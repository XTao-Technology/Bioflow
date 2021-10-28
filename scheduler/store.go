/* 
 Copyright (c) 2018 XTAO technology <www.xtaotech.com>
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
package scheduler

import (
    "strings"
    "errors"

    . "github.com/xtao/bioflow/storage"
    . "github.com/xtao/bioflow/common"
)

var (
    ErrPipelineSourceNotFound error = errors.New("no pipeine source found")
)

const (
    DEFAULT_PIPELINE_STORE_PATH string = "ssd-vol@xtao1:/bioflow/pstore"
)

type PipelineFileSet struct {
    baseDir string
    mainFile string
    inputFile string
}

type PipelineFileHandle struct {
    path string
}

type PipelineStore struct {
    mappedBaseDir string
    baseDir string
}

func (store *PipelineStore)IsReady() bool {
    return store.baseDir != "" && store.mappedBaseDir != ""
}

func (store *PipelineStore)GetPipelineFileSet(name string) *PipelineFileSet {
    return &PipelineFileSet{
        baseDir: store.mappedBaseDir + "/" + strings.ToLower(name),
        mainFile: strings.ToLower(name) + ".wdl",
        inputFile: strings.ToLower(name) + ".json",
    }
}

/*store pipeline source files temporarily*/
func (store *PipelineStore)CachePipeline(name string, data []byte) (*PipelineFileHandle, error) {
    if !store.IsReady() {
        return nil, errors.New("Pipeline store is not configured")
    }

    path := store.mappedBaseDir + "/tmp/" + name + ".tmpfile"
    err := FSUtilsWriteFile(path, data)
    if err != nil {
        return nil, err
    }

    return &PipelineFileHandle{path: path}, nil
}

func (store *PipelineStore)_BuildPipelineVersionPath(name string, version string,
    mapped bool) string {
    subStoreDir := name + "/version-" + version
    if version == "" {
        /*version empty means return pipeline path*/
        subStoreDir = name
    }

    if mapped {
        return store.mappedBaseDir + "/" + subStoreDir
    } else {
        return store.baseDir + "/" + subStoreDir
    }
}

func (store *PipelineStore)_SetBaseDir(baseDir string) error {
    if baseDir == "" {
        baseDir = DEFAULT_PIPELINE_STORE_PATH
    }
    err, mappedDir := GetStorageMgr().MapPathToSchedulerVolumeMount(baseDir)
    if err != nil {
        SchedulerLogger.Errorf("Failed to map dir %s on scheduler\n",
            baseDir)
        return err
    }

    /*Create the dir for pipeline store*/
    err = FSUtilsMkdir(mappedDir, true)
    if err != nil {
        return err
    }
    err = FSUtilsMkdir(mappedDir + "/tmp", true)
    if err != nil {
        return err
    }

    store.baseDir = baseDir
    store.mappedBaseDir = mappedDir

    return nil
}

/*Persist pipeline and source files with correct version information*/
func (store *PipelineStore)PersistPipeline(name string, version string,
    handle *PipelineFileHandle) (string, error) {
    if !store.IsReady() {
        return "", errors.New("Pipeline store is not configured")
    }

    if handle == nil {
        return "", errors.New("can't persist pipeline with nil handle")
    }

    if name == "" || version == "" {
        return "", errors.New("empty name or version")
    }

    targetDir := store._BuildPipelineVersionPath(name, version, true)
    err := FSUtilsMkdir(targetDir, true)
    if err != nil {
        return "", err
    }

    extractor := NewZipExtractor(targetDir, handle.path)
    SchedulerLogger.Infof("Will persist pipeline %s/%s to %s from %s\n",
        name, version, targetDir, handle.path)
    err = extractor.Extract()
    if err != nil {
        return "", err
    }

    /*delete the cache file*/
    FSUtilsDeleteFile(handle.path)

    SchedulerLogger.Infof("Succeed persist pipeline %s/%s to %s from %s\n",
        name, version, targetDir, handle.path)

    return store._BuildPipelineVersionPath(name, version, false), nil
}

/*Read pipeline source files with correct version information*/
func (store *PipelineStore)ReadPipeline(name string, version string) ([]byte, error) {
    if !store.IsReady() {
        return nil, errors.New("Pipeline store is not configured")
    }

    if name == "" || version == "" {
        return nil, errors.New("empty name or version")
    }

    targetDir := store._BuildPipelineVersionPath(name, version, true)
    tmpFilePath := store.mappedBaseDir + "/tmp/" + name + "-" + version + ".zipfile"
    writer := NewZipWriter(targetDir, tmpFilePath, true)
    SchedulerLogger.Infof("Will generate source zip file %s from %s for %s/%s\n",
        tmpFilePath, targetDir, name, version)
    err := writer.Write()
    if err != nil {
        SchedulerLogger.Errorf("Fail to zip wdl source code: %s\n",
            err.Error())
        return nil, err
    }
    /*delete the temp file*/
    defer FSUtilsDeleteFile(tmpFilePath)

    raw, err := FSUtilsReadFile(tmpFilePath)
    if err != nil {
        SchedulerLogger.Errorf("Fail to read archive file %s: %s\n", tmpFilePath, err.Error())
        return nil, err
    }
    SchedulerLogger.Infof("Succeed read pipeline %s/%s from %s\n",
        name, version, targetDir)

    return raw, nil
}

/*Delete pipeline source files from disk*/
func (store *PipelineStore)DeletePipeline(name string, version string) error {
    if !store.IsReady() {
        return errors.New("Pipeline store is not configured")
    }

    targetDir := store._BuildPipelineVersionPath(name, version, true)
    SchedulerLogger.Infof("Will cleanup pipeline %s/%s store dir %s\n",
        name, version, targetDir)
    err := FSUtilsDeleteDir(targetDir, true)
    if err != nil {
        SchedulerLogger.Errorf("Fail to delete pipeline %s/%s source dir %s: %s\n",
            name, version, targetDir, err.Error())
        return err
    }
    SchedulerLogger.Infof("Succeed to cleanup pipeline %s/%s store dir %s\n",
        name, version, targetDir)

    return nil
}

func (store *PipelineStore)ResetConfig(config *PipelineStoreConfig) error {
    return store._SetBaseDir(config.BaseDir)
}


var pipelineStore *PipelineStore = nil
func InitPipelineStore(config *PipelineStoreConfig) error {
    pipelineStore = &PipelineStore{}
    return pipelineStore._SetBaseDir(config.BaseDir)
}

func GetPipelineStore() *PipelineStore {
    return pipelineStore
}
