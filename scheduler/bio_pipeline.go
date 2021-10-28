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
    "errors"

    . "github.com/xtao/bioflow/common"
    . "github.com/xtao/bioflow/message"
    . "github.com/xtao/bioflow/scheduler/common"
)





type branchVariable struct {
    varMap map[string][]string
    varFiles map[string]string
    varTags map[string]string
}

func (br *branchVariable) GetVariables(name string) []string {
    if br.varMap == nil {
        return nil
    }

    return br.varMap[name]
}

func (br *branchVariable) SetVariables(name string, vals []string) {
    if br.varMap == nil {
        br.varMap = make(map[string][]string)
    }

    br.varMap[name] = vals
}

func (br *branchVariable) SetVarFile(name string, file string) {
    if br.varFiles == nil {
        br.varFiles = make(map[string]string)
    }

    br.varFiles[name] = file
}

func (br *branchVariable) GetVarFile(name string) string {
    if br.varFiles == nil {
        return ""
    }

    return br.varFiles[name]
}

func (br *branchVariable) SetVarFileTag(name string, fileTag string) {
    if br.varTags == nil {
        br.varTags = make(map[string]string)
    }

    br.varTags[name] = fileTag
}

func (br *branchVariable) GetVarFileTag(name string) string {
    if br.varTags == nil {
        return ""
    }

    return br.varTags[name]
}

/*
 * Data structure to represent a bio pipeline item
 * in memory.
 *
 */
type bioPipelineItem struct {
    /*
     * basic inforamtion are obtained directly from
     * the pipeline item definiton of user
     *
     */
    name string
    ptype string
    cmd string
    image string
    groupPattern string
    matchPattern string
    sortPattern string
    filter string
    resourceSpec ResourceSpec
    resourceSpecExpression ResourceSpecExpression
    items map[int]PipelineItem
    branchVar branchVariable
    branchVarMapFile string
    branchVarMapTag string
    cleanupPattern string
    outputFile string
    outputFileMap map[string]string
    extensionName string
    extensionMap map[string]string
    workDir string
    execMode string

    /*
     * outputDir is set to change output files
     * to a sub directory relative to current
     * work directory
     */
    outputDir string

    /*
     * outputDirTag is to name the output dir
     * created this stage and can be referenced
     * via the name in a later stage
     */
    outputDirTag string

    /*
     * inputDir can be set for shardfiles stage
     * to match files under this directory. it is 
     * vol@cluster:path format.
     */
    inputDir string

    /*
     * sometimes we want to use the named output dir
     * of some earlier stage as inputDir. so it specify
     * the tag here
     */
    inputDirTag string

    /*
     * The shard inputs may be specified by user in
     * a file or a tag
     */
    shardInputsFile string
    shardInputsTag string
    shardGroupSize int

    /*
     * sometimes we need map input directory or work directory
     * to a user specified location in container
     */
    inputDirMapTarget string
    workDirMapTarget string

    /*
     * How many times retry a failed item
     */
    failRetryLimit int

    /*
     * whether ignore the failed item or not, In contrast to the failignore
     */
    failAbort bool

    /*
     * how much ratio to increase the cpu or memory resource if
     * the item execute fail
     */
    cpuTuneRatio float64
    memTuneRatio float64

    /*
     * file to select target branches in the clone parallel
     * case
     */
    branchSelectorFile string
    branchSelectorTag string

    state int
    secID string


    /*
     * Storage type
     *
     */
    storageType string

    /*
     * Whether the stage should run in privileged mode
     */
    privileged bool
    env map[string]string
    volumes map[string]string

    /*
     * The policy only used for complex item, and default
     * shard item is forward, clone item is discard
     */
    discard []string
    forward []string

    tagPrefix string
    tagPrefixMap map[string]string

    constraints map[string]string

    ioPattern string
    rwPattern string
    largeSmallFiles bool
    workingSet string
    isolationLevel string
    epheremalLevel string
    epheremalFilePattern string
    epheremalMap map[string]string
}

func (item *bioPipelineItem) SecID() string {
    return item.secID
}

func (item *bioPipelineItem) SetSecID(id string) {
    item.secID = id
}

func (item *bioPipelineItem) ShardGroupSize() int {
    return item.shardGroupSize
}

func (item *bioPipelineItem) WorkDir() string {
    return item.workDir
}

func (item *bioPipelineItem) StorageType() string {
    return item.storageType
}

func (item *bioPipelineItem) Privileged() bool {
    return item.privileged
}

func (item *bioPipelineItem) Env() map[string]string {
    return item.env
}

func (item *bioPipelineItem) Volumes() map[string]string {
    return item.volumes
}

func (item *bioPipelineItem) Image() string {
    return item.image
}

func (item *bioPipelineItem) State() int {
    return item.state
}

func (item *bioPipelineItem) SetState(state int) {
    item.state = state
}

func (item *bioPipelineItem) CleanupPattern() string {
    return item.cleanupPattern
}

func (item *bioPipelineItem) GroupPattern() string {
    return item.groupPattern
}

func (item *bioPipelineItem) MatchPattern() string {
    return item.matchPattern
}

func (item *bioPipelineItem) SortPattern() string {
    return item.sortPattern
}

func (item *bioPipelineItem) Filter() string {
    return item.filter
}

func (item *bioPipelineItem) GetAllBranchVarListMap() map[string][]string {
    return item.branchVar.varMap
}

func (item *bioPipelineItem) GetAllBranchVarFileMap() map[string]string {
    return item.branchVar.varFiles
}

func (item *bioPipelineItem) GetAllBranchVarTagMap() map[string]string {
    return item.branchVar.varTags
}

func (item *bioPipelineItem) GetNamedBranchVariables(name string) []string {
    return item.branchVar.GetVariables(name)
}

func (item *bioPipelineItem) SetNamedBranchVariables(name string, vals []string){
    item.branchVar.SetVariables(name, vals)
}

func (item *bioPipelineItem) GetNamedBranchVarFile(name string) string {
    return item.branchVar.GetVarFile(name)
}

func (item *bioPipelineItem) SetNamedBranchVarFile(name string, file string){
    item.branchVar.SetVarFile(name, file)
}

func (item *bioPipelineItem) GetNamedBranchVarFileTag(name string) string {
    return item.branchVar.GetVarFileTag(name)
}

func (item *bioPipelineItem) SetNamedBranchVarFileTag(name string, fileTag string){
    item.branchVar.SetVarFileTag(name, fileTag)
}

func (item *bioPipelineItem) ShardInputsFileAndTag() (string, string) {
    return item.shardInputsFile, item.shardInputsTag
}

func (item *bioPipelineItem) BranchVarMapFileAndTag() (string, string) {
    return item.branchVarMapFile, item.branchVarMapTag
}

func (item *bioPipelineItem) Cmd() string {
    return item.cmd
}

func (item *bioPipelineItem) ExecMode() string {
    return item.execMode
}

func (item *bioPipelineItem) SetExecMode(mode string) {
    item.execMode = mode
}

func (item *bioPipelineItem) Name() string {
    return strings.ToUpper(item.name)
}

func (item *bioPipelineItem) Items() map[int]PipelineItem {
    return item.items
}

func (item *bioPipelineItem) ItemsCount() int {
    return len(item.items)
}

func (item *bioPipelineItem) Item(i int) PipelineItem {
    return item.items[i]
}

func (item *bioPipelineItem) SetItems(items map[int]PipelineItem) {
    item.items = items
}

func (item *bioPipelineItem) SetItem(i int, newItem PipelineItem) {
    item.items[i] = newItem
}

func (item *bioPipelineItem) SetResourceSpecExpression(cpu string, memory string,
    disk string, gpu string, gpu_memory string) {
    item.resourceSpecExpression.Cpu = cpu
    item.resourceSpecExpression.Memory = memory
    item.resourceSpecExpression.Disk = disk
    item.resourceSpecExpression.GPU = gpu
    item.resourceSpecExpression.GPUMemory = gpu_memory
}

func (item *bioPipelineItem) ResourceSpecExpression() ResourceSpecExpression {
    return item.resourceSpecExpression
}

func (item *bioPipelineItem) SetResourceSpec(cpu float64, memory float64,
    disk float64, gpu float64, gpu_memory float64, serverType string) {
    item.resourceSpec.SetCPU(cpu)
    item.resourceSpec.SetMemory(memory)
    item.resourceSpec.SetDisk(disk)
    item.resourceSpec.SetGPU(gpu)
    item.resourceSpec.SetGPUMemory(gpu_memory)
    item.resourceSpec.SetServerType(serverType)
}

func (item *bioPipelineItem) ResourceSpec() ResourceSpec {
    return item.resourceSpec
}

func (item *bioPipelineItem) GetCmd() string {
    return item.cmd
}

func (item *bioPipelineItem) ItemType() string {
    return strings.ToUpper(item.ptype)
}

func (item *bioPipelineItem) ExtensionName() string {
    return item.extensionName
}

func (item *bioPipelineItem) ExtensionMap() map[string]string {
    return item.extensionMap
}

func (item *bioPipelineItem) TagPrefix() string {
    return item.tagPrefix
}

func (item *bioPipelineItem) TagPrefixMap() map[string]string {
    return item.tagPrefixMap
}

func (item *bioPipelineItem) IORWPattern() (string, string) {
    return item.ioPattern, item.rwPattern
}

func (item *bioPipelineItem) WorkingSet() string {
    return item.workingSet
}

func (item *bioPipelineItem) IsolationLevel() string {
    return item.isolationLevel
}

func (item *bioPipelineItem) LargeSmallFiles() bool {
    return item.largeSmallFiles
}

func (item *bioPipelineItem) EpheremalProperty() (string, string, map[string]string) {
    return item.epheremalLevel, item.epheremalFilePattern, item.epheremalMap
}

func (item *bioPipelineItem) Discard() []string {
    return item.discard
}

func (item *bioPipelineItem) Constraints() map[string]string {
    return item.constraints
}

func (item *bioPipelineItem) Forward() []string {
    return item.forward
}

func (item *bioPipelineItem) OutputFile() string {
    return item.outputFile
}

func (item *bioPipelineItem) OutputFileMap() map[string]string {
    return item.outputFileMap
}


func (item *bioPipelineItem) InputDirAndTag() (string,string) {
    return item.inputDir, item.inputDirTag
}

func (item *bioPipelineItem) OutputDirAndTag() (string,string) {
    return item.outputDir, item.outputDirTag
}

func (item *bioPipelineItem) DirMapTargets() (string, string) {
    return item.inputDirMapTarget, item.workDirMapTarget
}

func (item *bioPipelineItem) FailAbort() bool {
    return item.failAbort
}

func (item *bioPipelineItem) FailRetryLimit() int {
    return item.failRetryLimit
}

func (item *bioPipelineItem) ResourceTuneRatio() (float64, float64) {
    return item.cpuTuneRatio, item.memTuneRatio
}

func (item *bioPipelineItem) BranchSelector() (string, string) {
    return item.branchSelectorFile, item.branchSelectorTag
}

func (item *bioPipelineItem) FromJSON(itemJson *BioPipelineItemJSONData) error {
    item.name = strings.ToUpper(itemJson.Name)
    item.cmd = itemJson.Cmd
    item.state = itemJson.State
    item.groupPattern = itemJson.GroupPattern
    item.sortPattern = itemJson.SortPattern
    item.matchPattern = itemJson.MatchPattern
    item.filter = itemJson.Filter
    item.image = itemJson.Image
    item.execMode = strings.ToUpper(itemJson.ExecMode)
    item.items = make(map[int]PipelineItem)
    item.cleanupPattern = itemJson.Cleanup
    item.ptype = GEN_PT_SIMPLE
    if itemJson.Type != "" {
        item.ptype = itemJson.Type
    }
    item.outputFile = itemJson.OutputFile
    item.outputFileMap = make(map[string]string)
    for k, v := range itemJson.OutputFileMap {
        item.outputFileMap[k] = v
    }
    item.outputDir = itemJson.OutputDir
    item.outputDirTag = itemJson.OutputDirTag
    item.extensionName = itemJson.ExtensionName
    item.extensionMap = make(map[string]string)
    for k, v := range itemJson.ExtensionMap {
        item.extensionMap[k] = v
    }
    item.tagPrefix = itemJson.TagPrefix
    item.tagPrefixMap = make(map[string]string)
    for k, v := range itemJson.TagPrefixMap {
        item.tagPrefixMap[k] = v
    }
    item.discard = make([]string, 0)
    item.discard = append(item.discard, itemJson.Discard ...)

    item.forward = make([]string, 0)
    item.forward = append(item.forward, itemJson.Forward ...)

    item.constraints = make(map[string]string, 0)
    for k, v := range itemJson.Constraints {
        item.constraints[k] = v
    }

    item.shardInputsFile = itemJson.InputFile
    item.shardInputsTag = itemJson.InputFileTag

    /* the default shard group size should be 2 (pair read),
     * the valid value should be 1, 2
     */
    item.shardGroupSize = itemJson.ShardGroupSize
    if item.shardGroupSize != 0 && item.shardGroupSize != 1 && item.shardGroupSize != 2 {
        return errors.New("Invalid shard group size")
    }

    item.inputDir = itemJson.InputDir
    item.inputDirTag = itemJson.InputDirTag
    item.inputDirMapTarget = itemJson.InputDirMapTarget
    item.workDirMapTarget = itemJson.WorkDirMapTarget
    item.workDir = itemJson.WorkDir
    item.storageType = itemJson.StorageType
    item.privileged = itemJson.Privileged
    item.env = itemJson.Env
    item.volumes = itemJson.Volumes

    if itemJson.Items != nil {
        for i := 0; i < len(itemJson.Items); i ++ {
            subItem := new(bioPipelineItem)
            err := subItem.FromJSON(&itemJson.Items[i])
            if err != nil {
                SchedulerLogger.Errorf("Parse pipeline item failure: %s\n",
                    err.Error())
                return err
            }
            item.items[i] = subItem
        }
    }

    if itemJson.BranchVarList != nil {
        for key, val := range itemJson.BranchVarList {
            item.SetNamedBranchVariables(key, val)
        }
    }

    if itemJson.BranchVarFiles != nil {
        for key, val := range itemJson.BranchVarFiles {
            item.SetNamedBranchVarFile(key, val)
        }
    }

    if itemJson.BranchVarTags != nil {
        for key, val := range itemJson.BranchVarTags {
            item.SetNamedBranchVarFileTag(key, val)
        }
    }

    item.branchVarMapFile = itemJson.BranchVarMapFile
    item.branchVarMapTag = itemJson.BranchVarMapTag

    item.failRetryLimit = itemJson.FailRetryLimit
    item.failAbort = itemJson.FailIgnore
    item.cpuTuneRatio = itemJson.CPUTuneRatio
    item.memTuneRatio = itemJson.MemTuneRatio
    item.branchSelectorFile = itemJson.BranchSelectorFile
    item.branchSelectorTag = itemJson.BranchSelectorTag

    var cpu_val float64 = 1.5
    var memory_val float64 = 5000
    var disk_val float64 = 0
    var gpu_val float64 = 0
    var gpu_memory_val float64 = 0

    var tmp_cup, tmp_memory, tmp_disk, tmp_gpu, tmp_gpu_memory string
    serverType := itemJson.ServerType
    if itemJson.ResourceSpec != nil {
        if kval, ok := itemJson.ResourceSpec[ITEM_RSC_CPU]; ok {
            switch kval.(type) {
            case string:
                tmp_cup = kval.(string)
            case float64:
                cpu_val = kval.(float64)
            }
        }
        if kval, ok := itemJson.ResourceSpec[ITEM_RSC_MEM]; ok {
            switch kval.(type) {
            case string:
                tmp_memory = kval.(string)
            case float64:
                memory_val = kval.(float64)
            }
        }
        if kval, ok := itemJson.ResourceSpec[ITEM_RSC_DISK]; ok {
            switch kval.(type) {
            case string:
                tmp_disk = kval.(string)
            case float64:
                disk_val = kval.(float64)
            }
        }
        if kval, ok := itemJson.ResourceSpec[ITEM_RSC_GPU]; ok {
            switch kval.(type) {
            case string:
                tmp_gpu = kval.(string)
            case float64:
                gpu_val = kval.(float64)
            }
        }
        if kval, ok := itemJson.ResourceSpec[ITEM_RSC_GPU_MEMORY]; ok {
            switch kval.(type) {
            case string:
                tmp_gpu_memory = kval.(string)
            case float64:
                gpu_memory_val = kval.(float64)
            }
        }
        item.SetResourceSpec(cpu_val, memory_val, disk_val,
            gpu_val, gpu_memory_val, serverType)
        item.SetResourceSpecExpression(tmp_cup, tmp_memory,
            tmp_disk, tmp_gpu, tmp_gpu_memory)
    } else {
        item.SetResourceSpec(cpu_val, memory_val, disk_val,
            gpu_val, gpu_memory_val, serverType)
    }

    /*handle storage access property*/
    item.ioPattern = itemJson.IOPattern
    item.rwPattern = itemJson.RWPattern
    item.largeSmallFiles = itemJson.LargeSmallFiles
    item.workingSet = itemJson.WorkingSet
    item.isolationLevel = itemJson.IsolationLevel
    item.epheremalLevel = itemJson.EpheremalLevel
    item.epheremalFilePattern = itemJson.EpheremalFilePattern
    item.epheremalMap = itemJson.EpheremalMap

    return nil
}

func (item *bioPipelineItem) ToJSON() (error, *BioPipelineItemJSONData) {
    itemJson := new(BioPipelineItemJSONData)
    itemJson.Name = item.name
    itemJson.State = item.state
    itemJson.Cmd = item.cmd
    itemJson.GroupPattern = item.groupPattern
    itemJson.SortPattern = item.sortPattern
    itemJson.MatchPattern = item.matchPattern
    itemJson.Filter = item.filter
    itemJson.Image = item.image
    itemJson.Type = item.ptype
    itemJson.Cleanup = item.cleanupPattern
    itemJson.OutputFile = item.outputFile
    itemJson.OutputFileMap = make(map[string]string)
    for k, v := range item.outputFileMap {
        itemJson.OutputFileMap[k] = v
    }
    itemJson.ExtensionName = item.extensionName
    itemJson.ExtensionMap = make(map[string]string)
    for k, v := range item.extensionMap {
        itemJson.ExtensionMap[k] = v
    }
    itemJson.TagPrefix = item.tagPrefix
    itemJson.TagPrefixMap = make(map[string]string)
    for k, v := range item.tagPrefixMap {
        itemJson.TagPrefixMap[k] = v
    }

    itemJson.Discard = make([]string, 0)
    itemJson.Discard = append(itemJson.Discard, item.discard ...)
    itemJson.Forward = make([]string, 0)
    itemJson.Forward = append(itemJson.Forward, item.forward ...)

    itemJson.Constraints = make(map[string]string)
    for k, v := range item.constraints {
        itemJson.Constraints[k] = v
    }

    itemJson.OutputDir = item.outputDir
    itemJson.OutputDirTag = item.outputDirTag
    itemJson.InputFile = item.shardInputsFile
    itemJson.InputFileTag = item.shardInputsTag
    itemJson.InputDir = item.inputDir
    itemJson.InputDirTag = item.inputDirTag
    itemJson.InputDirMapTarget = item.inputDirMapTarget
    itemJson.WorkDirMapTarget = item.workDirMapTarget
    itemJson.WorkDir = item.workDir
    itemJson.StorageType = item.storageType
    itemJson.Privileged = item.privileged
    itemJson.Env = item.env
    itemJson.Volumes = item.volumes
    itemJson.ExecMode = item.execMode

    itemJson.ShardGroupSize = item.shardGroupSize
    itemJson.Items = make([]BioPipelineItemJSONData, 0)
    for i := 0; i < len(item.items); i ++ {
        subItem := item.items[i]
        err, subItemJson := subItem.ToJSON()
        if err != nil {
            SchedulerLogger.Errorf("Parse pipeline item to JSON failure: %s\n",
                err.Error())
            return err, nil
        }
        itemJson.Items = append(itemJson.Items, *subItemJson)
    }

    itemJson.BranchVarList = item.branchVar.varMap
    itemJson.BranchVarFiles = item.branchVar.varFiles
    itemJson.BranchVarTags = item.branchVar.varTags
    itemJson.BranchVarMapFile = item.branchVarMapFile
    itemJson.BranchVarMapTag = item.branchVarMapTag
    itemJson.ResourceSpec = make(map[string]interface{})
    if item.resourceSpecExpression.Cpu != "" {
        itemJson.ResourceSpec[ITEM_RSC_CPU] = item.resourceSpecExpression.Cpu
    } else {
        itemJson.ResourceSpec[ITEM_RSC_CPU] = item.resourceSpec.GetCPU()
    }
    if item.resourceSpecExpression.Memory != "" {
        itemJson.ResourceSpec[ITEM_RSC_MEM] = item.resourceSpecExpression.Memory
    } else {
        itemJson.ResourceSpec[ITEM_RSC_MEM] = item.resourceSpec.GetMemory()
    }
    if item.resourceSpecExpression.Disk != "" {
        itemJson.ResourceSpec[ITEM_RSC_DISK] = item.resourceSpecExpression.Disk
    } else {
        itemJson.ResourceSpec[ITEM_RSC_DISK] = item.resourceSpec.GetDisk()
    }
    if item.resourceSpecExpression.GPU != "" {
        itemJson.ResourceSpec[ITEM_RSC_GPU] = item.resourceSpecExpression.GPU
    } else {
        itemJson.ResourceSpec[ITEM_RSC_GPU] = item.resourceSpec.GetGPU()
    }
    if item.resourceSpecExpression.GPUMemory != "" {
        itemJson.ResourceSpec[ITEM_RSC_GPU_MEMORY] = item.resourceSpecExpression.GPUMemory
    } else {
        itemJson.ResourceSpec[ITEM_RSC_GPU_MEMORY] = item.resourceSpec.GetGPUMemory()
    }

    itemJson.ServerType = item.resourceSpec.GetServerType()

    itemJson.FailRetryLimit = item.failRetryLimit
    itemJson.FailIgnore = item.failAbort
    itemJson.CPUTuneRatio = item.cpuTuneRatio
    itemJson.MemTuneRatio = item.memTuneRatio
    itemJson.BranchSelectorFile = item.branchSelectorFile
    itemJson.BranchSelectorTag = item.branchSelectorTag

    itemJson.IOPattern = item.ioPattern
    itemJson.RWPattern = item.rwPattern
    itemJson.LargeSmallFiles = item.largeSmallFiles
    itemJson.WorkingSet = item.workingSet
    itemJson.IsolationLevel = item.isolationLevel
    itemJson.EpheremalLevel = item.epheremalLevel
    itemJson.EpheremalFilePattern = item.epheremalFilePattern
    itemJson.EpheremalMap = item.epheremalMap

    return nil, itemJson
}

func (item *bioPipelineItem) ToBioflowPipelineItemInfo(all bool) (error, *BioflowPipelineItemInfo) {
    itemInfo := new(BioflowPipelineItemInfo)
    itemInfo.Name = item.name
    itemInfo.Owner = item.secID
    if !all {
        return nil, itemInfo
    }

    itemInfo.Cmd = item.cmd
    itemInfo.GroupPattern = item.groupPattern
    itemInfo.MatchPattern = item.matchPattern
    itemInfo.SortPattern = item.sortPattern
    itemInfo.StorageType = item.storageType
    itemInfo.Cleanup = item.cleanupPattern
    itemInfo.Filter = item.filter
    itemInfo.Image = item.image
    itemInfo.State = PipelineStateToStr(item.state)
    itemInfo.Type = item.ptype
    itemInfo.OutputFile = item.outputFile
    itemInfo.OutputFileMap = make(map[string]string)
    for k, v := range item.outputFileMap {
        itemInfo.OutputFileMap[k] = v
    }
    itemInfo.ExtensionName = item.extensionName
    itemInfo.ExtensionMap = make(map[string]string)
    for k, v := range item.extensionMap {
        itemInfo.ExtensionMap[k] = v
    }
    itemInfo.TagPrefix = item.tagPrefix
    itemInfo.TagPrefixMap = make(map[string]string)
    for k, v := range item.tagPrefixMap {
        itemInfo.TagPrefixMap[k] = v
    }
    itemInfo.OutputDir = item.outputDir
    itemInfo.OutputDirTag = item.outputDirTag
    itemInfo.InputDir = item.inputDir
    itemInfo.InputDirTag = item.inputDirTag
    itemInfo.InputDirMapTarget = item.inputDirMapTarget
    itemInfo.WorkDirMapTarget = item.workDirMapTarget
    itemInfo.WorkDir = item.workDir
    itemInfo.ExecMode = item.execMode
    itemInfo.InputFile = item.shardInputsFile
    itemInfo.InputFileTag = item.shardInputsTag
    itemInfo.ShardGroupSize = item.shardGroupSize
    itemInfo.Items = make([]BioflowPipelineItemInfo, 0)
    for i := 0; i < len(item.items); i ++ {
        subItem := item.items[i]
        err, subItemInfo := subItem.ToBioflowPipelineItemInfo(all)
        if err != nil {
            SchedulerLogger.Errorf("Parse pipeline item to bioflow info failure: %s\n",
                err.Error())
            return err, nil
        }
        itemInfo.Items = append(itemInfo.Items, *subItemInfo)
    }

    itemInfo.BranchVarList = item.branchVar.varMap
    itemInfo.BranchVarFiles = item.branchVar.varFiles
    itemInfo.BranchVarTags = item.branchVar.varTags
    itemInfo.BranchVarMapFile = item.branchVarMapFile
    itemInfo.BranchVarMapTag = item.branchVarMapTag
    itemInfo.ResourceSpec = make(map[string]interface{})
    if item.resourceSpecExpression.Cpu != "" {
        itemInfo.ResourceSpec[ITEM_RSC_CPU] = item.resourceSpecExpression.Cpu
    } else {
        itemInfo.ResourceSpec[ITEM_RSC_CPU] = item.resourceSpec.GetCPU()
    }
    if item.resourceSpecExpression.Memory != "" {
        itemInfo.ResourceSpec[ITEM_RSC_MEM] = item.resourceSpecExpression.Memory
    } else {
        itemInfo.ResourceSpec[ITEM_RSC_MEM] = item.resourceSpec.GetMemory()
    }
    if item.resourceSpecExpression.Disk != "" {
        itemInfo.ResourceSpec[ITEM_RSC_DISK] = item.resourceSpecExpression.Disk
    } else {
        itemInfo.ResourceSpec[ITEM_RSC_DISK] = item.resourceSpec.GetDisk()
    }
    if item.resourceSpecExpression.GPU != "" {
        itemInfo.ResourceSpec[ITEM_RSC_GPU] = item.resourceSpecExpression.GPU
    } else {
        itemInfo.ResourceSpec[ITEM_RSC_GPU] = item.resourceSpec.GetGPU()
    }
    if item.resourceSpecExpression.GPUMemory != "" {
        itemInfo.ResourceSpec[ITEM_RSC_GPU_MEMORY] = item.resourceSpecExpression.GPUMemory
    } else {
        itemInfo.ResourceSpec[ITEM_RSC_GPU_MEMORY] = item.resourceSpec.GetGPUMemory()
    }
    itemInfo.ServerType = item.resourceSpec.GetServerType()
    itemInfo.FailRetryLimit = item.failRetryLimit
    itemInfo.FailIgnore = item.failAbort
    itemInfo.CPUTuneRatio = item.cpuTuneRatio
    itemInfo.MemTuneRatio = item.memTuneRatio
    itemInfo.BranchSelectorFile = item.branchSelectorFile
    itemInfo.BranchSelectorTag = item.branchSelectorTag
    itemInfo.Privileged = item.privileged

    /*Don't clone here for simplity*/
    itemInfo.Env = item.env
    itemInfo.Volumes = item.volumes
    itemInfo.Constraints = item.constraints
    itemInfo.Forward = item.forward
    itemInfo.Discard = item.discard

    itemInfo.IOPattern = item.ioPattern
    itemInfo.RWPattern = item.rwPattern
    itemInfo.LargeSmallFiles = item.largeSmallFiles
    itemInfo.WorkingSet = item.workingSet
    itemInfo.IsolationLevel = item.isolationLevel
    itemInfo.EpheremalLevel = item.epheremalLevel
    itemInfo.EpheremalFilePattern = item.epheremalFilePattern
    itemInfo.EpheremalMap = item.epheremalMap

    return nil, itemInfo
}


type bioPipeline struct {
    basePipeline
    items map[int]PipelineItem
    storeDir string
    storeHandle *PipelineFileHandle
    workflowFile string
}

func (pipeline *bioPipeline) Size() int {
    return len(pipeline.items)
}

func (pipeline *bioPipeline) Item(i int) PipelineItem {
    return pipeline.items[i]
}

func (pipeline *bioPipeline) SetItem(i int, item PipelineItem) {
    pipeline.items[i] = item
}

func (pipeline *bioPipeline) Items() map[int]PipelineItem {
    return pipeline.items
}

func (pipeline *bioPipeline) SourceInfo() *PipelineSourceInfo {
    return &PipelineSourceInfo{
        WorkflowFile: pipeline.workflowFile,
        StoreDir: pipeline.storeDir,
    }
}

func (pipeline *bioPipeline) PersistToStore() error {
    if pipeline.Type() == PIPELINE_TYPE_BIOBPIPE {
        return nil
    }

    if pipeline.storeHandle == nil {
        SchedulerLogger.Debugf("no file handle attached for %s, don't persist",
            pipeline.Name())
        return nil
    }

    storePath, err := GetPipelineStore().PersistPipeline(pipeline.Name(),
        pipeline.Version(), pipeline.storeHandle)
    if err != nil {
        return err
    }
    pipeline.storeDir = storePath
    return nil
}

func (pipeline *bioPipeline) DeleteFromStore() error {
    if pipeline.Type() == PIPELINE_TYPE_BIOBPIPE {
        return nil
    }

    return GetPipelineStore().DeletePipeline(pipeline.Name(), "")
}

func (pipeline *bioPipeline) ReadFromStore() ([]byte, error) {
    if pipeline.Type() == PIPELINE_TYPE_BIOBPIPE {
        return nil, nil
    }

    return GetPipelineStore().ReadPipeline(pipeline.Name(), pipeline.Version())
}

func (pipeline *bioPipeline) FromJSON(pipelineJson *PipelineJSONData) error {
    pipeline.SetName(pipelineJson.Name)
    pipeline.state = pipelineJson.State
    pipeline.description = pipelineJson.Description
    pipeline.items = make(map[int]PipelineItem)
    pipeline.pipelineType = strings.ToUpper(pipelineJson.Type)
    if pipeline.pipelineType == "" {
        pipeline.pipelineType = PIPELINE_TYPE_BIOBPIPE
    }
    pipeline.workDir = pipelineJson.WorkDir
    pipeline.hdfsWorkDir = pipelineJson.HDFSWorkDir
    pipeline.inputMap = pipelineJson.InputMap
    pipeline.ignoreDir = pipelineJson.IgnoreDir

    /*Handle different pipeline type details*/
    switch pipeline.pipelineType {
        case PIPELINE_TYPE_BIOBPIPE:
            for i := 0; i < len(pipelineJson.Items); i ++ {
                item := new(bioPipelineItem)
                err := item.FromJSON(&pipelineJson.Items[i])
                if err != nil {
                    SchedulerLogger.Errorf("Parse pipeline item json failure: %s \n",
                        err.Error())
                    return err
                }

                pipeline.items[i] = item
            }
        case PIPELINE_TYPE_WDL:
            pipeline.workflowFile = pipelineJson.Wdl.WorkflowFile
            pipeline.storeDir = pipelineJson.Wdl.StoreDir
            if len(pipelineJson.Wdl.Blob) > 0 {
                pStore := GetPipelineStore()
                storeHandle, err := pStore.CachePipeline(pipeline.Name(),
                    pipelineJson.Wdl.Blob)
                if err != nil {
                    return err
                }
                pipeline.storeHandle = storeHandle
            }
    }

    return nil
}

func (pipeline *bioPipeline) ToJSON() (error, *PipelineJSONData){
    pipelineJson := new(PipelineJSONData)
    pipelineJson.Name = pipeline.Name()
    pipelineJson.State = pipeline.state
    pipelineJson.Description = pipeline.description
    pipelineJson.Type = pipeline.pipelineType
    pipelineJson.InputMap = pipeline.inputMap
    pipelineJson.WorkDir = pipeline.workDir
    pipelineJson.HDFSWorkDir = pipeline.hdfsWorkDir
    pipelineJson.IgnoreDir = pipeline.ignoreDir

    switch pipeline.pipelineType {
        case PIPELINE_TYPE_BIOBPIPE:
            pipelineJson.Items = make([]BioPipelineItemJSONData, 0)
            for i := 0; i < len(pipeline.items); i ++ {
                item := pipeline.items[i]
                err, itemJson := item.ToJSON()
                if err != nil {
                    SchedulerLogger.Errorf("Parse pipeline item json failure: %s \n",
                        err.Error())
                    return err, nil
                }
                pipelineJson.Items = append(pipelineJson.Items, *itemJson)
            }
        case PIPELINE_TYPE_WDL:
            pipelineJson.Wdl = WdlJSONData{
                    WorkflowFile: pipeline.workflowFile,
                    StoreDir: pipeline.storeDir,
            }
    }

    return nil, pipelineJson
}

func (pipeline *bioPipeline) Clone() (error, Pipeline) {
    err, jsonData := pipeline.ToJSON()
    if err != nil {
        SchedulerLogger.Errorf("Clone pipeline %s error: %s\n",
            pipeline.Name(), err.Error())
        return errors.New("Clone Pipeline " + pipeline.Name() + " serialize failure"),
            nil
    }

    clonedPipeline := &bioPipeline{}
    err = clonedPipeline.FromJSON(jsonData)
    if err != nil {
        SchedulerLogger.Errorf("Clone pipeline %s error: %s\n",
            pipeline.Name(), err.Error())
        return errors.New("Clone Pipeline " + pipeline.Name() + " deserialize failure"),
            nil
    }

    return nil, clonedPipeline
}

func (pipeline *bioPipeline) ToBioflowPipelineInfo(includeItem bool) (error, *BioflowPipelineInfo) {
    pipelineInfo := &BioflowPipelineInfo {
                Name: pipeline.Name(),
                Type: pipeline.pipelineType,
                WorkDir: pipeline.workDir,
                HDFSWorkDir: pipeline.hdfsWorkDir,
                InputMap: pipeline.inputMap,
                ItemCount: len(pipeline.items),
                State: PipelineStateToStr(pipeline.state),
                Owner: pipeline.secID,
                Parent: pipeline.Parent(),
                LastVersion: pipeline.LastVersion(),
                Version: pipeline.Version(),
                IgnoreDir: pipeline.IgnoreDir(),
    }

    switch pipeline.Type() {
        case PIPELINE_TYPE_BIOBPIPE:
            if includeItem {
                pipelineInfo.Items = make([]BioflowPipelineItemInfo, 0)
                for i := 0; i < len(pipeline.items); i ++ {
                    item := pipeline.items[i]
                    err, itemInfo := item.ToBioflowPipelineItemInfo(true)
                    if err != nil {
                        SchedulerLogger.Errorf("Can't serelize item info: %s\n",
                            err.Error())
                        return err, nil
                    }
                    pipelineInfo.Items = append(pipelineInfo.Items, *itemInfo)
                }
            }
        case PIPELINE_TYPE_WDL:
            pipelineInfo.WorkflowFile = pipeline.workflowFile
    }

    return nil, pipelineInfo
}
