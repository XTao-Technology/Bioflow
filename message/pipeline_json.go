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
    STORAGE_HDFS = "HDFS"
    STORAGE_LATENCY = "LATENCY"
    STORAGE_THROUGHPUT = "THROUGHPUT"
    EXEC_MODE_DOCKER = "DOCKER"
    EXEC_MODE_HOST = "HOST"

    IO_PATTERN_SEQUENTIAL = "SEQUENTIAL"
    IO_PATTERN_RANDOM = "RANDOM"
    IO_PATTERN_HYBRID = "HYBRID"

    RW_PATTERN_READ = "READ"
    RW_PATTERN_WRITE = "WRITE"
    RW_PATTERN_HYBRID = "HYBRID"
    RW_PATTERN_REREAD = "REREAD"
    RW_PATTERN_REWRITE = "REWRITE"

    ISO_LEVEL_STAGE = "STAGE"
    ISO_LEVEL_IO = "IO"

    EPH_LEVEL_STAGE = "STAGE"
    EPH_LEVEL_JOB = "JOB"
    EPH_LEVEL_USER = "USER"
)

type ResourceSpecJSONData struct {
    Cpu float64                     `json:"Cpu"`
    Memory float64                  `json:"Memory"`
    Disk float64                    `json:"Disk"`
}

/*
 * The shard file list can be read from a json file as follows:
 * {
 *   "files" : [
 *        ["file1", "file2"],
 *        ["file3", "file4"],
 *        ...
 *    ]
 * }
 */
type BioShardFilesJSONData struct {
    Files [][]string
}

/*
 * The advanced JSON format for Shard Files Group:
 * {
 *   "tumor" : {
 *        "files" : ["file1", "file2"]
 *     }
 *   "normal" : {
 *        "files" : ["file3", "file4"]
 *     }
 *        ...
 * }
 */
type ShardGroupFiles struct {
    Files []string
}
type BioShardFilesJSONDataV2 map[string]ShardGroupFiles

/*
 * The branch vars can be read from a json file as follows: 
 * {
 *   "name1" : ["val1","val2","val3", ... ],
 *   "name2" : ["xx", ...],
 *   ...
 * }
 */
type BioBranchVarMapJSONData struct {
    Vars map[string][]string
}

/*
 * The branch selector JSON file format for clone parallel
 * item should be as follows:
 * {
 *   "Index" : ["1", "2", "3"],
 *   "Name" : ["name1", "name2"]
 * }
 *
 */
type BioBranchSelectorJSONData struct {
    Index []string      `json:"Index"`
    Name []string       `json:"Name"`
}

const (
    ITEM_RSC_CPU string = "Cpu"
    ITEM_RSC_MEM string = "Memory"
    ITEM_RSC_DISK string = "Disk"
    ITEM_RSC_SERVER string = "Server"
    ITEM_RSC_GPU string = "Gpu"
    ITEM_RSC_GPU_MEMORY string = "GpuMemory"
)

/*Server type consts*/
const (
    SERVER_TYPE_FAT string = "FAT"
)

type BioPipelineItemJSONData struct {
    Name    string                                  `json:"Name,omitempty"`
    State  int
    Cmd    string                                   `json:"Cmd,omitempty"`
    Comments string                                 `json:"Comments,omitempty"`
    ResourceSpec map[string]interface{}             `json:"ResourceSpec"`
    GroupPattern string                             `json:"GroupPattern,omitempty"`
    MatchPattern string                             `json:"MatchPattern,omitempty"`
    SortPattern string                              `json:"SortPattern,omitempty"`
    Filter string                                   `json:"Filter,omitempty"`
    Image string                                    `json:"Image,omitempty"`
    Items []BioPipelineItemJSONData                 `json:"Items,omitempty"`
    Type  string                                    `json:"Type,omitempty"`
    BranchVarList map[string][]string               `json:"BranchVarList,omitempty"`
    BranchVarFiles map[string]string                `json:"BranchVarFiles,omitempty"`
    BranchVarTags map[string]string                 `json:"BranchVarTags,omitempty"`
    BranchVarMapFile string                         `json:"BranchVarMapFile,omitempty"`
    BranchVarMapTag string                          `json:"BranchVarMapTag,omitempty"`
    BranchSelectorFile string                       `json:"BranchSelectorFile,omitempty"`
    BranchSelectorTag string                        `json:"BranchSelectorTag,omitempty"`
    Cleanup string                                  `json:"Cleanup,omitempty"`
    InputDir string                                 `json:"InputDir,omitempty"`
    InputDirTag string                              `json:"InputDirTag,omitempty"`
    OutputFile string                               `json:"OutputFile,omitempty"`
    OutputFileMap map[string]string                 `json:"OutputFileMap,omitempty"`
    OutputDir string                                `json:"OutputDir,omitempty"`
    OutputDirTag string                             `json:"OutputDirTag,omitempty"`
    ExtensionName string                            `json:"ExtensionName,omitempty"`
    ExtensionMap map[string]string                  `json:"ExtensionMap,omitempty"`
    Discard []string                                `json:"Discard,omitempty"`
    Forward []string                                `json:"Forward,omitempty"`
    InputDirMapTarget string                        `json:"InputDirMapTarget,omitempty"`
    WorkDirMapTarget string                         `json:"WorkDirMapTarget,omitempty"`
    InputFile string                                `json:"InputFile,omitempty"`
    InputFileTag string                             `json:"InputFileTag,omitempty"`
    WorkDir string                                  `json:"WorkDir,omitempty"`
    ShardGroupSize int                              `json:"ShardGroupSize,omitempty"`
    FailRetryLimit int                              `json:"FailRetryLimit,omitempty"`
    FailIgnore bool
    CPUTuneRatio float64                            `json:"CPUTuneRatio,omitempty"`
    MemTuneRatio float64                            `json:"MemTuneRatio,omitempty"`
    StorageType string                              `json:"StorageType,omitempty"`
    ServerType string                               `json:"ServerType,omitempty"`
    Privileged bool
    Env map[string]string                           `json:"Env,omitempty"`
    Volumes map[string]string                       `json:"Volumes,omitempty"`
    ExecMode string                                 `json:"ExecMode,omitempty"`
    TagPrefix string                                `json:"TagPrefix,omitempty"`
    TagPrefixMap map[string]string                  `json:"TagPrefixMap,omitempty"`
    Constraints map[string]string                   `json:"Constraints,omitempty"`
    IOPattern string                                `json:"IOPattern,omitempty"`
    RWPattern string                                `json:"RWPattern,omitempty"`
    LargeSmallFiles bool                            `json:"LargeSmallFiles,omitempty"`
    WorkingSet string                               `json:"WorkingSet,omitempty"`
    IsolationLevel string                           `json:"IsolationLevel,omitempty"`
    EpheremalLevel string                           `json:"EpheremalLevel,omitempty"`
    EpheremalFilePattern string                     `json:"EpheremalFilePattern,omitempty"`
    EpheremalMap map[string]string                  `json:"EpheremalMap,omitempty"`
}

type WdlJSONData struct {
    WorkflowFile string                             `json:"WorkflowFile,omitempty"`
    Blob []byte
    StoreDir string                                 `json:"StoreDir,omitempty"`
}

type PipelineJSONData struct {
    Name  string                                    `json:"Name,omitempty"`
    State int                                       `json:"State,omitempty"`
    UseExistingItem bool
    InputMap map[string]string                      `json:"InputMap,omitempty"`
    WorkDir string                                  `json:"WorkDir,omitempty"`
    IgnoreDir string                                `json:"IgnoreDir,omitempty"`
    HDFSWorkDir string                              `json:"HDFSWorkDir,omitempty"`
    Description string                              `json:"Description,omitempty"`
    Type  string                                    `json:"Type,omitempty"`
    Itemcount int                                   `json:"Itemcount,omitempty"`
    Items []BioPipelineItemJSONData                 `json:"Items,omitempty"`
    Wdl WdlJSONData
}

type PipelineItemsJSONData struct {
    Items []BioPipelineItemJSONData
}
