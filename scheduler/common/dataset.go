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
package common

import (
    "encoding/json"

    . "github.com/xtao/bioflow/common"
    . "github.com/xtao/bioflow/message"
)

type BioDataSet struct {
    files []string
    filesInOrder [][]string
    inputDir string
    vol string
    inputMap map[string]string
    workflowInput map[string]interface{}
    restorePath string
    restoreFilter []string
    logPath string
}

func NewBIODataSet(vol string, inputDir string,
    files []string, filesInOrder [][]string,
    restorePath string,
    restoreFilter []string,
    logPath string) *BioDataSet {
    return &BioDataSet {
        inputDir: inputDir,
        files: files,
        filesInOrder: filesInOrder,
        vol: vol,
        inputMap: nil,
        restorePath: restorePath,
        restoreFilter: restoreFilter,
        logPath: logPath,
    }
}

func (dataset *BioDataSet) SetInputMap(input map[string]string) {
    dataset.inputMap = input
}

func (dataset *BioDataSet) InputMap() map[string]string {
    return dataset.inputMap
}

func (dataset *BioDataSet) SetWorkflowInput(input map[string]interface{}) {
    dataset.workflowInput = input
}

func (dataset *BioDataSet) WorkflowInput() map[string]interface{} {
    return dataset.workflowInput
}

/*Merge all the key/values in input not in dataset to dataset*/
func (dataset *BioDataSet) MergeInputMap(input map[string]string) {
    if input == nil {
        return
    }

    if dataset.inputMap == nil {
        dataset.inputMap = input
    } else {
        for key, val := range input {
            if _, ok := dataset.inputMap[key]; !ok {
                dataset.inputMap[key] = val
            }
        }
    }
}

func (dataset *BioDataSet) Vol() string {
    return dataset.vol
}

func (dataset *BioDataSet) Files() []string {
    return dataset.files
}

func (dataset *BioDataSet) FilesInOrder() [][]string {
    return dataset.filesInOrder
}

func (dataset *BioDataSet) SetInputDir(dir string) {
    dataset.inputDir = dir
}

func (dataset *BioDataSet) InputDir() string {
    return dataset.inputDir
}

func (dataset *BioDataSet) CloneInputMap() map[string]string {
    inputMap := make(map[string]string)
    for key, val := range dataset.inputMap {
        inputMap[key] = val
    }

    return inputMap
}

func (dataset *BioDataSet) ToJSON() (error, string) {
    datasetJson := DataSetJSONData {
        Files: dataset.files,
        FilesInOrder: dataset.filesInOrder,
        InputDir: dataset.inputDir,
        Vol: dataset.vol,
        InputMap: dataset.inputMap,
        WorkflowInput: dataset.workflowInput,
        RestorePath: dataset.restorePath,
        RestoreFilter: dataset.restoreFilter,
        LogPath: dataset.logPath,
    }

    body, err := json.Marshal(&datasetJson)
    if err != nil {
        Logger.Println(err)
        return err, ""
    }

    return nil, string(body)
}

func (dataset *BioDataSet) FromJSON(data string) error {
    rawBytes := []byte(data)
    
    datasetJson := DataSetJSONData{}
    err := json.Unmarshal(rawBytes, &datasetJson)
    if err != nil {
        return err
    }

    dataset.files = datasetJson.Files
    dataset.filesInOrder = datasetJson.FilesInOrder
    dataset.inputDir = datasetJson.InputDir
    dataset.vol = datasetJson.Vol
    dataset.inputMap = datasetJson.InputMap
    dataset.workflowInput = datasetJson.WorkflowInput
    dataset.restorePath = datasetJson.RestorePath
    dataset.restoreFilter = datasetJson.RestoreFilter
    dataset.logPath = datasetJson.LogPath

    return nil
}

func (dataset *BioDataSet) RestorePath() string {
    return dataset.restorePath
}

func (dataset *BioDataSet) RestoreFilter() []string {
    return dataset.restoreFilter
}

func (dataset *BioDataSet) LogPath() string {
    return dataset.logPath
}