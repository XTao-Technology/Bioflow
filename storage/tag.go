/* 
 Copyright (c) 2016-2017 XTAO technology <www.xtaotech.com>
 All rights reserved.

 Redistribution and use in source and binary forms, with or without
 modification, are permitted provided that the following conditions
 are met:
  1. Redistributions of source code must retain the above copyright
     notice, this list of conditions and the following disclaimer.
  2. Redistributions in binary form must reproduce the above copyright
     notice, this list of conditions and the following disclaimer in the
     documentation and/or other materials provided with the distribution.
 
  THIS SOFTWARE IS PROVIDED BY THE AUTHOR AND CONTRIBUTORS ``AS IS'' AND
  ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
  IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
  ARE DISCLAIMED.  IN NO EVENT SHALL THE AUTHOR OR CONTRIBUTORS BE LIABLE
  FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL
  DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS
  OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION)
  HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT
  LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY
  OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF
  SUCH DAMAGE.
*/
package storage

import (
    "syscall"
    "errors"
    "encoding/json"

    . "github.com/xtao/bioflow/common"
    )

const (
    TAG_ORIGIN_FROM = "originfrom"
    EA_GET_UUID = "xtao.uuid"
    EA_TAG_PREFIX = "xtao.tag."

    MAX_EA_SIZE = 1024
)

func TagUtilsAddTag(taggedFile string, tagName string, tagValue []byte) error {
    if tagName == "" {
        return errors.New("Invalid empty tag")
    }

    err := syscall.Setxattr(taggedFile, EA_TAG_PREFIX + tagName, tagValue, 0)
    if err != nil {
        StorageLogger.Errorf("Can't tag file %s with tag %s: %s\n",
            taggedFile, tagName, err.Error())
    }

    return err
}




/*
 * Tag a file (path name already mapped to scheduler volume mount)
 * with the source file list
 */
func TagUtilsSetFileOriginFromTag(realFilePath string, srcFiles []string,
    realSrcFiles []string, info *StageTagInfo) error {
    var srcFilePaths []string
    var srcFileUUIDs []string
    /*try to get file uuid, otherwise use its file path directly*/
    for i := 0; i < len(realSrcFiles); i ++ {
        uuid, err := TagUtilsGetFileUUID(realSrcFiles[i])
        if err != nil {
            StorageLogger.Infof("Fail to get uuid of file %s: %s\n",
                srcFiles[i], err.Error())
            srcFilePaths = append(srcFilePaths, srcFiles[i])
        } else {
            StorageLogger.Debugf("Succeed to get file %s uuid %s\n",
                srcFiles[i], uuid)
            srcFileUUIDs = append(srcFileUUIDs, uuid)
        }
    }

    /*encode source file list to JSON string*/
    tagJson := &FinalTagInfo {
        OriginFiles: srcFilePaths,
        OriginUUIDs: srcFileUUIDs,
        JobId: info.JobId,
        PipelineName: info.PipelineName,
        PipelineVersion: info.PipelineVersion,
        StageName: info.StageName,
    }

    tagBytes, err := json.Marshal(tagJson)
    if err != nil {
        StorageLogger.Errorf("Fail to encode tag to JSON: %s\n",
            err.Error())
        return err
    }

    return TagUtilsAddTag(realFilePath, TAG_ORIGIN_FROM, tagBytes)
}

func FSUtilsGetXattr(path, name string) ([]byte, error) {
    /*find size.*/
    size, err := syscall.Getxattr(path, name, nil)
    if err != nil {
        return nil, err
    }
    
    if size > 0 {
        data := make([]byte, size)
        /* Read into buffer of that size*/
        read, err := syscall.Getxattr(path, name, data)
        if err != nil {
            return nil, err
        }
        return data[:read], nil
    }
    return []byte{}, nil
}

/*
 * Get the file uuid from the xtao file systems. the return result is
 * in the format of "fsid:uuid". This is obtained by reading the extended
 * attribute "xt.uuid"
 */
func TagUtilsGetFileUUID(realFilePath string) (string, error) {
    eaBytes, err := FSUtilsGetXattr(realFilePath, EA_GET_UUID)
    if err != nil {
        StorageLogger.Errorf("Fail to get uuid for file %s: %s\n",
            realFilePath, err.Error())
        return "", err
    } else {
        StorageLogger.Debugf("file %s has sized uuid: %s\n",
            realFilePath, string(eaBytes))
        return string(eaBytes), nil
    }
}
