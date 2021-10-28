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

/*version info*/
const (
    PIPELINE_VERSION_HEAD string = "HEAD"
    PIPELINE_VERSION_PREV string = "PREV"
)

type BioflowPipelineItemInfo struct {
    Name    string                                  
    Cmd    string                                   
    State  string
    Comments string                                 
    Cleanup string
    Owner string
    ResourceSpec map[string]interface{}
    GroupPattern string                              
    MatchPattern string                              
    InputDir string
    InputDirTag string
    Filter string                                   
    OutputFile string
    OutputFileMap map[string]string
    OutputDir string
    OutputDirTag string
    ExtensionName string
    ExtensionMap map[string]string
    TagPrefix string
    TagPrefixMap map[string]string
    InputDirMapTarget string
    WorkDirMapTarget string
    Image string                                    
    Items []BioflowPipelineItemInfo
    Type  string                                    
    BranchVarList map[string][]string             
    BranchVarFiles map[string]string             
    BranchVarTags map[string]string
    BranchVarMapFile string
    BranchVarMapTag string
    InputFile string
    InputFileTag string
    WorkDir string
    ShardGroupSize int
    FailRetryLimit int
    FailIgnore bool
    CPUTuneRatio float64
    MemTuneRatio float64
    BranchSelectorFile string
    BranchSelectorTag string
    StorageType string
    ServerType string
    SortPattern string
    Privileged bool
    Env map[string]string
    Volumes map[string]string
    ExecMode string
    Constraints map[string]string
    Forward []string
    Discard []string
    IOPattern string
    RWPattern string
    LargeSmallFiles bool
    IsolationLevel string
    EpheremalLevel string
    WorkingSet string
    EpheremalFilePattern string
    EpheremalMap map[string]string
}

type BioflowPipelineInfo struct {
    Name  string
    State string
    Owner string
    Description string
    Type  string
    WorkDir string
    HDFSWorkDir string
    Parent string
    LastVersion string
    Version string
    IgnoreDir string
    InputMap map[string]string
    WorkflowFile string
    ItemCount int
    Items []BioflowPipelineItemInfo
}

type BioflowListPipelineResult struct {
    Status string
    Msg string
    Pipelines []BioflowPipelineInfo
}

type BioflowGetPipelineResult struct {
    Status string
    Msg string
    Pipeline BioflowPipelineInfo
}

type BioflowDumpPipelineResult struct {
    Status string
    Msg string
    PipelineJsonData PipelineJSONData
}

type BioflowListPipelineItemResult struct {
    Status string
    Msg string
    PipelineItems []BioflowPipelineItemInfo
}

type BioflowGetPipelineItemResult struct {
    Status string
    Msg string
    PipelineItem BioflowPipelineItemInfo
}

type BioflowDumpPipelineItemResult struct {
    Status string
    Msg string
    PipelineItemsJsonData PipelineItemsJSONData
}
