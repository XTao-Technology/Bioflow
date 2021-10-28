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

type BioflowGetDashBoardResult struct {
    Status string
    Msg string
    DashBoardInfo BioflowDashBoardInfo
}

type BioflowJobStats struct {
    JobCount int64
    PendingScheduleJobs int64
    OutstandingTasks int64
    RunningTasks int64
    QueuedTasks int64
    TotalQueueTime float64
    MaxQueueTime float64
    ThroateJobs int64
    ThroateScheduleJobs int64
    DelayJobs int64
    SubmitTasks int64
    CompleteTasks int64
    CompleteJobs int64
    ScheduleRounds int64
    DelayRounds int64
    ThroateRounds int64
    AggDelayRounds int64
    TotalAggDelayPeriod float64
    MaxAggDelayPeriod float64
    MaxAggTaskNum int64
    MinAggTaskNum int64
    DelayingSchedule bool
    DelayTimeTillNow float64
}

type BioflowJobSchedulePerfStats struct {
    MaxScheduleTime float64
    MaxMarkScheduleTime float64
    MaxJobCheckTime float64
}

type BioflowSchedulePerfStats struct {
    MaxSchedJobTime float64
    AvgSchedJobTime float64
    SchedJobCount int64
    OutstandingSchedJobCount int64
    MaxSchedStageTime float64
    AvgSchedStageTime float64
    SchedStageCount int64
    OutstandingSchedStageCount int64
    MaxSyncDBTime float64
    AvgSyncDBTime float64
    SyncDBCount int64
    OutstandingSyncDBCount int64
    MaxJobStateMachineTime float64
    AvgJobStateMachineTime float64
    JobStateMachineCount int64
    OutstandingJobStateMachineCount int64
    MaxStagePostExecTime float64
    AvgStagePostExecTime float64
    StagePostExecCount int64
    OutstandingStagePostExecCount int64
    MaxStagePrepareTime float64
    AvgStagePrepareTime float64
    StagePrepareCount int64
    OutstandingStagePrepareCount int64
    MaxBackendScheduleTime float64
    AvgBackendScheduleTime float64
    BackendScheduleCount int64
    OutstandingBackendScheduleCount int64
}

type BioflowWorkerGroupStats struct {
    TotalQueueLen float64
    QueueLens []float64
    Mean float64
    Median float64
    StdVariance float64
    MergedCounts []uint64
    TotalMergedCount uint64
}

type BioflowStageBatcherStats struct {
    QuotaInPriority []int
    Allocated []int
    PendingJobs [][]string
}

type BioflowSchedulerStats struct {
    State string
    Backends []BioflowBackendInfo
    JobStats map[string]BioflowJobStats
    PerfStats BioflowJobSchedulePerfStats
    ScheduleStats BioflowSchedulePerfStats
    PendingAsyncEventCount int64
    ScheduleWorkerStats []BioflowWorkerGroupStats
    DBWorkerStats BioflowWorkerGroupStats
    StageBatcherStats BioflowStageBatcherStats
}

type BioflowJobMgrStats struct {
    State string
    RecoveryTime float64
    MaxJobSubmitTime float64
    TotalJobNum int64
    FailJobNum int64
    CompleteJobNum int64
    RunningJobNum int64
    CanceledJobNum int64
    PausedJobNum int64
    PsudoCompleteJobNum int64
}

type BioflowStorageMgrStats struct {
    MaxReadDir float64
    MinReadDir float64
    AvgReadDir float64
    MaxMkDir float64
    MinMkDir float64
    AvgMkDir float64
    MaxReadFile float64
    MinReadFile float64
    AvgReadFile float64
    MaxRmDir float64
    MinRmDir float64
    AvgRmDir float64
    MaxDeleteFile float64
    MinDeleteFile float64
    AvgDeleteFile float64
    MaxWriteFile float64
    MinWriteFile float64
    AvgWriteFile float64
    MaxMkDirPath float64
    AvgMkDirPath float64
    MaxChown float64
    AvgChown float64
    MaxChmod float64
    AvgChmod float64
    MinMkDirPath float64
    MinChown float64
    MinChmod float64
    MaxNFSMount float64
    MinNFSMount float64
    MaxGlusterMount float64
    MinGlusterMount float64
    MaxCephMount float64
    MinCephMount float64
    MaxRename float64
    MinRename float64
    AvgRename float64

    OutstandingMkDirCount int64
    TotalMkDirCount int64
    OutstandingMkDirPathCount int64
    TotalMkDirPathCount int64
    OutstandingRmDirCount int64
    TotalRmDirCount int64
    OutstandingReadDirCount int64
    TotalReadDirCount int64
    OutstandingReadFileCount int64
    TotalReadFileCount int64
    OutstandingWriteFileCount int64
    TotalWriteFileCount int64
    OutstandingDeleteFileCount int64
    TotalDeleteFileCount int64
    OutstandingChownCount int64
    TotalChownCount int64
    OutstandingChmodCount int64
    TotalChmodCount int64
    OutstandingNFSMountCount int64
    OutstandingGlusterMountCount int64
    OutstandingCephMountCount int64
    OutstandingRenameCount int64
    TotalRenameCount int64
    TotalRetryCount int64
}

type BioflowDashBoardInfo struct {
    OpStatKeys []string
    OpStatVals []int64
    SchedulerStats BioflowSchedulerStats
    JobMgrStats BioflowJobMgrStats
    StorageMgrStats BioflowStorageMgrStats
}

