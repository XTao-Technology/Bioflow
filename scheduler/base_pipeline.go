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
package scheduler

import (
    "strings"
    . "github.com/xtao/bioflow/scheduler/common"
)

type basePipeline struct {
    name string
    pipelineType string
    inputMap map[string]string
    workDir string
    hdfsWorkDir string
    state int
    description string
    secID string
    ignoreDir string
    /*
     * Track the pipeline update/clone relationship
     * and version information
     */
    version string
    lastVersion string
    parent string
}

func (pipeline *basePipeline) SecID() string {
    return pipeline.secID
}

func (pipeline *basePipeline) SetSecID(id string) {
    pipeline.secID = id
}

func (pipeline *bioPipeline) IgnoreDir() string {
    return pipeline.ignoreDir
}

func (pipeline *bioPipeline) SetIgnoreDir(dirs string) {
    pipeline.ignoreDir = dirs
}

func (pipeline *basePipeline) Parent() string {
    return pipeline.parent
}

func (pipeline *basePipeline) SetParent(parent string) {
    pipeline.parent = strings.ToUpper(parent)
}

func (pipeline *basePipeline) Version() string {
    return pipeline.version
}

func (pipeline *basePipeline) SetVersion(version string) {
    pipeline.version = version
}

func (pipeline *basePipeline) LastVersion() string {
    return pipeline.lastVersion
}

func (pipeline *basePipeline) SetLastVersion(version string) {
    pipeline.lastVersion = version
}

func (pipeline *basePipeline) SetState(state int) {
    pipeline.state = state
}

func (pipeline *basePipeline) State() int {
    return pipeline.state
}

func (pipeline *basePipeline) WorkDir() string {
    return pipeline.workDir
}

func (pipeline *basePipeline) HDFSWorkDir() string {
    return pipeline.hdfsWorkDir
}

func (pipeline *basePipeline) InputMap() map[string]string {
    return pipeline.inputMap
}

func (pipeline *basePipeline) CloneInputMap() map[string]string {
    if pipeline.inputMap == nil {
        return nil
    }

    retMap := make(map[string]string)
    for key, val := range pipeline.inputMap {
        retMap[key] = val
    }

    return retMap
}

func (pipeline *basePipeline) Name() string {
    return strings.ToUpper(pipeline.name)
}

func (pipeline *basePipeline) SetName(name string) {
    pipeline.name = strings.ToUpper(name)
}

func (pipeline *basePipeline) Type() string {
    return strings.ToUpper(pipeline.pipelineType)
}

func (pipeline *basePipeline) IsBIOType() bool {
    return strings.ToUpper(pipeline.pipelineType) == PIPELINE_TYPE_BIOBPIPE
}

