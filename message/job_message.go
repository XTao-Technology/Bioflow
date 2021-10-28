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

type BioflowSubmitJobResult struct {
    Status string
    Msg string
    JobId string
}

type BioflowJobInfo struct {
    JobId string
    Name string
    Pipeline string
    Created string
    Finished string
    State string
    Description string
    Owner string
    Priority int
    ExecMode string

    /*info for hang stages*/
    HangStages []BioflowStageInfo
}

type BioflowListJobResult struct {
    Status string
    Msg string
    Count int
    Jobs []BioflowJobInfo
    HistJobs []BioflowJobInfo
}

type PeriodResourceUsageInfo struct {
    AvgSysRatio float64
    AvgUserRatio float64
    MaxMem float64
    AvgMem float64
    MaxCache float64
    AvgCache float64
    MaxSwap float64
    AvgSwap float64
    TotalRead float64
    TotalWrite float64
    TotalSysCR float64
    TotalSysCW float64
}

type ResourceUsageInfo struct {
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
    Periods []PeriodResourceUsageInfo
}

type BioflowGetResourceUsageResult struct {
	Status string
	Msg string
	Usage map[string]ResourceUsageInfo
}

type BioflowStageInfo struct {
    Id string
    Name string
    State string
    Command string
    Output map[string]string
    RetryCount int
    BackendId string
    TaskId string
    SubmitTime string
    ScheduleTime string
    FinishTime string
    TotalDuration float64
    FailReason string
    CPU float64
    Memory float64
    ServerType string
    GPU float64
    GPUMemory float64
    Disk float64
    ExecMode string
    HostName string
    HostIP string
    ResourceStats ResourceUsageInfo
}

type BioflowJobStatus struct {
    JobId string
    Name string
    Pipeline string
    Owner string
    WorkDir string
    HDFSWorkDir string
    Created string
    Finished string
    State string
    PausedState string
    RunCount int
    Priority int
    RetryLimit int
    StageCount int
    StageQuota int
    ExecMode string
    FailReason []string
    GraphCompleted bool
    PipelineBuildPos int
    DoneStages []BioflowStageInfo
    PendingStages []BioflowStageInfo
    WaitingStages []BioflowStageInfo
    ForbiddenStages []BioflowStageInfo
    JobOutput map[string]string
    Constraints map[string]string
}

type BioflowGetJobStatusResult struct {
    Status string
    Msg string
    JobStatus BioflowJobStatus
}

type BioflowStageLog struct {
	StageName string
	StageId   string
	Logs map[string]string
}

type BioflowGetLogResult struct {
	Status string
	Msg string
	JobLogs map[string]BioflowStageLog
}

type BioflowCleanupJobResult struct {
	Status string
	Msg string
	CleanupCount int
}
