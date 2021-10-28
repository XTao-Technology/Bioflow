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
    "time"
    . "github.com/xtao/bioflow/storage"
    . "github.com/xtao/bioflow/message"
    )

type StageExecEnv struct{
    WorkDir string
    ForceChdir bool
    NetMode ContainerNetMode
    Privileged bool
    Env map[string]string
    Volumes map[string]string
    OutputDirs []string
    OutputFiles []string
}

type Stage interface {
    Type() int
	GetID() string
    PipelineName() string
    PipelineVersion() string
	GetInprogressEvent() uint32
    SetState(uint32)
    State() uint32
    GetCommand() string
    RestoreExecutionState(*StageJSONInfo, bool) error
    GetImage() string
    GetCPU() float64
    GetMemory() float64
    GetServerType() string
    GetGPU() float64
    GetGPUMemory() float64
    GetDisk() float64
    SetResourceSpec(ResourceSpec)
    MergeResourceSpec(float64, float64, float64, float64, float64)
    NeedStoreAppOutput() bool
    Name() string
    SetName(name string)
    SetImage(image string)
    PrepareRetry()
    RetryCount() int
    SetRetryCount(int)
    WorkDir() string
    SetWorkDir(dir string)
    SetLogDir(dir string)
    LogDir() string
    SetSubmitTime(time.Time)
    SubmitTime() time.Time
    SetScheduledTime(time.Time)
    ScheduledTime() time.Time
    RunDurationTillNow() float64
    SetFinishTime(time.Time)
    FinishTime() time.Time
    QueueDuration() float64
    QueueDurationTillNow() float64
    TotalDuration() float64
    Cleanup() error
    CleanupPattern() string
    GetTargetOutputs() map[string]string
    GetDataVolsMap() (error, []*DataVol)
    OutputDir() string
    PrepareExecution(bool) (error, *BuildStageErrorInfo, *StageExecEnv)
    PostExecution(bool) error
    MapDirToContainer(dirPath string, dirTarget string) error
    IsRunning() bool
    FailReason() string
    SetFailReason(string)
    SetFailRetryLimit(int)
    FailRetryLimit() int
    SetFailAbort(bool)
    FailAbort() bool
    SetResourceTuneRatio(float64, float64)
    ToBioflowStageInfo() *BioflowStageInfo
    StorageType() StorageType
    SetStorageType(StorageType)
    SetPrivileged(bool)
    SetEnv(map[string]string)
    SetVolumes(map[string]string)
    SetExecMode(string)
    ExecMode() string
    HostName() string
    SetHostName(string)
    IP() string
    SetIP(string)
    ResourceStats() ResourceUsageSummary
    SetResourceStats(*ResourceUsageSummary)
    StartHandleEvent(uint32)bool
    FinishHandleEvent(uint32)
    AllowScheduleStage() bool
    FilesNeedPrepare() []string
    Constraints() map[string]string
    Recycle()
    IOAttr() *IOAttr
    SetIOAttr(*IOAttr)
}

/*
 stage state:
 0:  idle
 1:  running
 2:  fail
 3:  pending
*/

const (
    DATA_STAGE int = 0
    GRAPH_STAGE int = 1
    OFFLOAD_STAGE int = 2
)

func StageIsNotStarted(state uint32) bool {
    return state == STAGE_INITIAL || state == STAGE_SUBMITTED || state == STAGE_QUEUED || state == STAGE_SUBMIT_FAIL
}

func StageIsTerminal(state uint32) bool {
    return state == STAGE_FAIL || state == STAGE_DONE
}

func StageStateToStr(state uint32) string {
    switch state {
        case STAGE_INITIAL:
            return "STAGE_INITIAL"
        case STAGE_FAIL:
            return "STAGE_FAIL"
        case STAGE_DONE:
            return "STAGE_DONE"
        case STAGE_RUNNING:
            return "STAGE_RUNNING"
        case STAGE_LOST:
            return "STAGE_LOST"
        case STAGE_SUBMITTED:
            return "STAGE_SUBMITTED"
        case STAGE_QUEUED:
            return "STAGE_QUEUED"
        case STAGE_SUBMIT_FAIL:
            return "STAGE_SUBMIT_FAIL"
        default:
            return "INVALID"
    }
}

type ResourceUsageSummary struct {
    AvgCPURatio float64
    MaxCPURatio float64
    LowCPURatio float64
    MaxMem float64
    MaxIOMem float64
    AvgMem float64
    AvgIOMem float64
    MaxSwap float64
    AvgSwap float64
    TotalRead float64
    TotalWrite float64
    TotalSysCR float64
    TotalSysCW float64
    OOM bool
    MaxProfilingCost float64
}

func (usage ResourceUsageSummary)ToResourceUsageInfo() ResourceUsageInfo{
    return ResourceUsageInfo{
        MaxCPURatio: usage.MaxCPURatio,
        AvgCPURatio: usage.AvgCPURatio,
        LowCPURatio: usage.LowCPURatio,
        MaxMem: usage.MaxMem,
        AvgMem: usage.AvgMem,
        MaxIOMem: usage.MaxIOMem,
        AvgIOMem: usage.AvgIOMem,
        MaxSwap: usage.MaxSwap,
        AvgSwap: usage.AvgSwap,
        TotalRead: usage.TotalRead,
        TotalWrite: usage.TotalWrite,
        TotalSysCR: usage.TotalSysCR,
        TotalSysCW: usage.TotalSysCW,
        OOM: usage.OOM,
        MaxProfilingCost: usage.MaxProfilingCost,
    }
}

type StageJSONInfo struct {
    Id string
    State uint32
    Name string
    Command string
    CleanupPattern string
    CPU float64
    Memory float64
    GPU float64
    GPUMemory float64
    Disk float64
    ServerType string
    InputStages map[string]string
    Output map[string]string
    WorkDir string
    LogDir string
    RetryCount int
    SubmitTime string
    ScheduledTime string
    FinishTime string
    QueueDuration float64
    TotalDuration float64
    BackendID string
    TaskID string
    FailReason string
    FailRetryLimit string
    FailIgnore bool
    CPUTuneRatio float64
    MemTuneRatio float64
    ExecMode string
    HostName string
    HostIP string
    ResourceStats ResourceUsageSummary
}

func (stageJsonInfo *StageJSONInfo)ToBioflowStageInfo() *BioflowStageInfo{
    scheduleTime := stageJsonInfo.ScheduledTime
    if StageIsNotStarted(stageJsonInfo.State) {
        scheduleTime = "N/A"
    }

    stageInfo := &BioflowStageInfo {
        Id: stageJsonInfo.Id,
        Name: stageJsonInfo.Name,
        State: StageStateToStr(stageJsonInfo.State),
        Command: stageJsonInfo.Command,
        Output: stageJsonInfo.Output,
        RetryCount: stageJsonInfo.RetryCount,
        BackendId: stageJsonInfo.BackendID,
        TaskId: stageJsonInfo.TaskID,
        SubmitTime: stageJsonInfo.SubmitTime,
        FinishTime: stageJsonInfo.FinishTime,
        ScheduleTime: scheduleTime,
        TotalDuration: stageJsonInfo.TotalDuration,
        FailReason: stageJsonInfo.FailReason,
        CPU: stageJsonInfo.CPU,
        Memory: stageJsonInfo.Memory,
        GPU: stageJsonInfo.GPU,
        GPUMemory: stageJsonInfo.GPUMemory,
        Disk: stageJsonInfo.Disk,
        ServerType: stageJsonInfo.ServerType,
        ExecMode: stageJsonInfo.ExecMode,
        HostName: stageJsonInfo.HostName,
        HostIP: stageJsonInfo.HostIP,
        ResourceStats: stageJsonInfo.ResourceStats.ToResourceUsageInfo(),
    }

    return stageInfo
}
