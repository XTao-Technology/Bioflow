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
package bl

import (
    . "github.com/xtao/bioflow/storage"
    . "github.com/xtao/bioflow/scheduler/common"
)

type storageMatcher struct {
    inputFiles []string
    fileNodes *NamedNodeMap
    dirNodes *NamedNodeMap
}

func NewStorageMatcher(inputFiles []string, fileNodes *NamedNodeMap,
    dirNodes *NamedNodeMap) *storageMatcher {
    return &storageMatcher {
        inputFiles: inputFiles,
        fileNodes: fileNodes,
        dirNodes: dirNodes,
    }
}

/*
 * Deduce the storage type from the input data
 */
func (matcher *storageMatcher)DeduceStorageTypeFromInput() (error, StorageType) {
    inputFiles := matcher.inputFiles
    fileNodes := matcher.fileNodes
    dirNodes := matcher.dirNodes

    /*
     * Currently only apply one rule:
     * 1) if any input is on HDFS, require output to be HDFS
     */
    hasHDFSInput := false
    if inputFiles != nil {
        for i := 0; i < len(inputFiles); i ++ {
            if FSUtilsIsHDFSPath(inputFiles[i]) {
                hasHDFSInput = true
            }
        }
    }

    if fileNodes != nil {
	    fileNodes.Visit(func(name string, node Node) bool {
            if node != nil {
		        stage := node.Stage()
                if stage != nil && stage.StorageType() == STORAGE_TYPE_HDFS {
                    hasHDFSInput = true
                    return true
                }
            }

		    return false
	    })
    }

    if dirNodes != nil {
	    dirNodes.Visit(func(name string, node Node) bool {
            if node != nil {
		        stage := node.Stage()
                if stage != nil && stage.StorageType() == STORAGE_TYPE_HDFS {
                    hasHDFSInput = true
                    return true
                }
            }

		    return false
	    })
    }

    if hasHDFSInput {
        return nil, STORAGE_TYPE_HDFS
    } else {
        return nil, STORAGE_TYPE_DEFAULT
    }
}
