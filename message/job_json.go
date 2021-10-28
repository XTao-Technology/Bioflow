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
package message

const (
    JOB_MIN_PRI int = 0
    JOB_MAX_PRI int = 10
)


type DataSetJSONData struct {
    Files []string                          `json:"Files"`
    FilesInOrder[][]string                  `josn:"FilesInOrder"`
    InputDir string                         `json:"InputDir"`
    Vol string                              `json:"Vol"`
    InputMap map[string]string              `json:"InputMap"`
    WorkflowInput map[string]interface{}    `json:"WorkflowInput"`
    RestorePath string                      `json:"RestorePath"`
    RestoreFilter []string                  `json:"RestoreFilter"`
    LogPath string                          `json:"LogPath"`
}

type JobJSONData struct {
    Name   string                       `json:"Name"`
    Description string                  `json:"Description"`
    Pipeline string                     `json:"Pipeline"`
    WorkDir string                      `json:"WorkDir"`
    LogDir string                       `json:"LogDir"`
    InputDataSet DataSetJSONData        `json:"InputDataSet"`
    SMId string                         `json:"SMId"`
    Priority int
    DisableOOMCheck bool                
    ExecMode string
    HDFSWorkDir string                  `json:"HDFSWorkDir"`
    StageQuota *int                     `json:"StageQuota"`
    Constraints map[string]string       `json:"Constraints"`
}

type JobListOpt struct {
	ListType string    `json:"All"`
	Name string        `json:"Name"`
	Pipeline string    `json:"Pipeline"`
	After string       `json:"After"`
	Before string      `json:"Before"`
	Finished string    `json:"Finished"`
	Count int          `json:"Count"`
    SecID string       `json:"Secid"`
    Priority int       `json:"Priority"`
    State string       `json:"State"`
}
