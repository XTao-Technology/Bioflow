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
    . "github.com/xtao/bioflow/message"
    . "github.com/xtao/bioflow/dbservice"
)

const (
    PIPELINE_TYPE_BIOBPIPE string = "BIOBPIPE"
    PIPELINE_TYPE_WDL string = "WDL"
)

const (
    GEN_PT_SIMPLE string = "SIMPLE"
    GEN_PT_SHARDFILES string = "SHARDFILES"
    GEN_PT_VARIABLEPARALLEL string = "VARIABLEPARALLEL"
    GEN_PT_CLONEPARALLEL string = "CLONEPARALLEL"
    GEN_PT_PIPELINE string = "PIPELINE"
)

type PipelineSourceInfo struct {
    WorkflowFile string
    StoreDir string
}

type Pipeline interface {
    Size() int
    Item(int) PipelineItem
    State() int
    SetState(int)
    SecID() string
    SetSecID(string)
    IgnoreDir() string
    SetIgnoreDir(string)
    Type() string
    IsBIOType() bool
    Name() string
    Items() map[int]PipelineItem
    SetItem(int, PipelineItem)
    FromJSON(*PipelineJSONData) error
    ToJSON() (error, *PipelineJSONData)
    ToBioflowPipelineInfo(bool) (error, *BioflowPipelineInfo)
    Clone() (error, Pipeline)
    SetName(string)
    WorkDir() string
    HDFSWorkDir() string
    InputMap() map[string]string
    CloneInputMap() map[string]string
    Parent() string
    SetParent(string)
    Version() string
    SetVersion(string)
    LastVersion() string
    SetLastVersion(string)
    PersistToStore() error
    DeleteFromStore() error
    ReadFromStore() ([]byte, error)
    SourceInfo() *PipelineSourceInfo
}

type PipelineItem interface {
    SetResourceSpec(float64, float64, float64, float64, float64, string)
    ResourceSpec() ResourceSpec
    SetResourceSpecExpression(string, string, string, string, string)
    ResourceSpecExpression() ResourceSpecExpression
    Cmd() string
    Name() string
    State() int
    SetState(int)
    SecID() string
    SetSecID(string)
    ItemType() string
    Image() string
    Filter() string
    GroupPattern() string
    MatchPattern() string
    CleanupPattern() string
    Items() map[int]PipelineItem
    ItemsCount() int
    Item(int) PipelineItem
    SetItem(int, PipelineItem)
    SetItems(map[int]PipelineItem)
    TagPrefix() string
    TagPrefixMap() map[string]string
    GetNamedBranchVariables(string) []string
    SetNamedBranchVariables(string, []string)
    GetAllBranchVarListMap() map[string][]string
    GetAllBranchVarFileMap() map[string]string
    GetAllBranchVarTagMap() map[string]string
    FromJSON(*BioPipelineItemJSONData) error
    ToJSON() (error, *BioPipelineItemJSONData)
    ToBioflowPipelineItemInfo(bool) (error, *BioflowPipelineItemInfo)
    InputDirAndTag() (string,string)
    OutputDirAndTag() (string,string)
    OutputFile() string
    OutputFileMap() map[string]string
    Constraints() map[string]string
    ExtensionMap() map[string]string
    ExtensionName() string
    Discard() []string
    Forward() []string
    DirMapTargets() (string, string)
    ShardInputsFileAndTag() (string, string)
    WorkDir() string
    ShardGroupSize() int
    BranchVarMapFileAndTag() (string, string)
    FailAbort() bool
    FailRetryLimit() int
    ResourceTuneRatio() (float64, float64)
    BranchSelector() (string, string)
    StorageType() string
    SortPattern() string
    Privileged() bool
    Env() map[string]string
    Volumes() map[string]string
    SetExecMode(string)
    ExecMode() string
    IORWPattern() (string, string)
    WorkingSet() string
    IsolationLevel() string
    EpheremalProperty() (string, string, map[string]string)
    LargeSmallFiles() bool
}

func PipelineStateToStr(state int) string {
    switch state {
        case PSTATE_HEAD:
            return "InUse"
        case PSTATE_HIST:
            return "History Version"
        default:
            return "N/A"
    }
}
