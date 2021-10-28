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
package main

import (
    "fmt"
    . "github.com/xtao/bioflow/common"
    . "github.com/xtao/bioflow/message"
    . "github.com/xtao/bioflow/client"
)


type dashboardCommand struct {
    client *BioflowClient
}

func newDashBoardCommand(c *BioflowClient) *dashboardCommand {
    return &dashboardCommand{
        client: c,
    }
}

func ShowDashBoardInfo(info *BioflowDashBoardInfo, showDetails bool) {
    schedulerStatus := info.SchedulerStats
    fmt.Printf("Scheduler Status:\n")
    IndentPrint(true, "State:%s\n",
        schedulerStatus.State)
    IndentPrint(true, "Backends:\n")
    for i := 0; i < len(schedulerStatus.Backends); i ++ {
        backend := schedulerStatus.Backends[i]
        IndentPrint(true, "  Backend %d:\n", i + 1)
        IndentPrint(true, "     Type:   %s\n", backend.Type)
        IndentPrint(true, "     Server:  %s\n", backend.Server)
        IndentPrint(true, "     Status:  %s\n", backend.Status)
        IndentPrint(true, "     TaskCount:  %d\n", backend.TaskCount)
        IndentPrint(true, "     FailCount:  %d\n", backend.FailCount)
    }
    IndentPrint(true, "Job Stats:\n")
    for pri := JOB_MIN_PRI; pri <= JOB_MAX_PRI; pri ++ {
        jobStat, exist := schedulerStatus.JobStats[fmt.Sprintf("%d", pri)]
        if !exist {
            continue
        }

        IndentPrint(true, "  Pri %d:\n", pri)
        IndentPrint(true, "    Job Count:%d\n", jobStat.JobCount)
        IndentPrint(true, "    Pending Schedule Count:%d\n",
            jobStat.PendingScheduleJobs)
        IndentPrint(true, "    Queued Tasks:%d\n",
            jobStat.QueuedTasks)
        IndentPrint(true, "    Running Tasks:%d\n",
            jobStat.RunningTasks)
        IndentPrint(true, "    Total Queue Time:%f\n",
            jobStat.TotalQueueTime)
        IndentPrint(true, "    Max Queue Time:%f\n",
            jobStat.MaxQueueTime)
        IndentPrint(true, "    Throated Jobs:%v\n",
            jobStat.ThroateJobs)
        IndentPrint(true, "    Throate Schedule Jobs:%v\n",
            jobStat.ThroateScheduleJobs)
        IndentPrint(true, "    Delay Jobs:%v\n",
            jobStat.DelayJobs)
        IndentPrint(true, "    Submit Tasks:%v\n",
            jobStat.SubmitTasks)
        IndentPrint(true, "    Complete Tasks:%v\n",
            jobStat.CompleteTasks)
        IndentPrint(true, "    Complete Jobs:%v\n",
            jobStat.CompleteJobs)
        IndentPrint(true, "    Schedule Rounds:%v\n",
            jobStat.ScheduleRounds)
        IndentPrint(true, "    Throate Rounds:%v\n",
            jobStat.ThroateRounds)
        IndentPrint(true, "    Delay Rounds:%v\n",
            jobStat.DelayRounds)
        IndentPrint(true, "    AggregateDelayRounds:%d\n",
            jobStat.AggDelayRounds)
        IndentPrint(true, "    MaxAggregateDelayPeriod:%f\n",
            jobStat.MaxAggDelayPeriod)
        IndentPrint(true, "    TotalAggDelayPeriod:%f\n",
            jobStat.TotalAggDelayPeriod)
        IndentPrint(true, "    MaxAggregateTaskNum:%d\n",
            jobStat.MaxAggTaskNum)
        IndentPrint(true, "    MinAggregateTaskNum:%d\n",
            jobStat.MinAggTaskNum)
        IndentPrint(true, "    DelayingByAggregate:%v\n",
            jobStat.DelayingSchedule)
        IndentPrint(true, "    DelayTimeTillNow:%f\n",
            jobStat.DelayTimeTillNow)
    }
    schedulePerfStats := info.SchedulerStats.PerfStats
    IndentPrint(true, "Job Scheduler Stats:\n")
    IndentPrint(true, "  State: %s\n", info.SchedulerStats.State)
    IndentPrint(true, "  Schedule Perf Stats:\n")
    IndentPrint(true, "     MaxScheduleTime:%f\n",
        schedulePerfStats.MaxScheduleTime)
    IndentPrint(true, "     MaxMarkScheduleTime:%f\n",
        schedulePerfStats.MaxMarkScheduleTime)
    IndentPrint(true, "     MaxJobCheckTime:%f\n",
        schedulePerfStats.MaxJobCheckTime)
    IndentPrint(true, "     PendingAsyncEventCount:%d\n",
        info.SchedulerStats.PendingAsyncEventCount)
    if showDetails {
        scheduleStats := info.SchedulerStats.ScheduleStats
        workerGroupStats := info.SchedulerStats.ScheduleWorkerStats
        IndentPrint(true, "     MaxJobScheduleTime:%f\n",
            scheduleStats.MaxSchedJobTime)
        IndentPrint(true, "     AvgJobScheduleTime:%f\n",
            scheduleStats.AvgSchedJobTime)
        IndentPrint(true, "     JobScheduleCount:%d\n",
            scheduleStats.SchedJobCount)
        IndentPrint(true, "     OutstandingJobScheduleCount:%d\n",
            scheduleStats.OutstandingSchedJobCount)
        IndentPrint(true, "     MaxStageScheduleTime:%f\n",
            scheduleStats.MaxSchedStageTime)
        IndentPrint(true, "     AvgStageScheduleTime:%f\n",
            scheduleStats.AvgSchedStageTime)
        IndentPrint(true, "     StageScheduleCount:%d\n",
            scheduleStats.SchedStageCount)
        IndentPrint(true, "     OutstandingStageScheduleCount:%d\n",
            scheduleStats.OutstandingSchedStageCount)
        IndentPrint(true, "     MaxSyncDBTime:%f\n",
            scheduleStats.MaxSyncDBTime)
        IndentPrint(true, "     AvgSyncDBTime:%f\n",
            scheduleStats.AvgSyncDBTime)
        IndentPrint(true, "     SyncDBCount:%d\n",
            scheduleStats.SyncDBCount)
        IndentPrint(true, "     OutstandingSyncDBCount:%d\n",
            scheduleStats.OutstandingSyncDBCount)
        IndentPrint(true, "     MaxJobEventTime:%f\n",
            scheduleStats.MaxJobStateMachineTime)
        IndentPrint(true, "     AvgJobEventTime:%f\n",
            scheduleStats.AvgJobStateMachineTime)
        IndentPrint(true, "     JobEventCount:%d\n",
            scheduleStats.JobStateMachineCount)
        IndentPrint(true, "     OutstandingJobEventCount:%d\n",
            scheduleStats.OutstandingJobStateMachineCount)
        IndentPrint(true, "     MaxStagePrepareTime:%f\n",
            scheduleStats.MaxStagePrepareTime)
        IndentPrint(true, "     AvgStagePrepareTime:%f\n",
            scheduleStats.AvgStagePrepareTime)
        IndentPrint(true, "     StagePrepareCount:%d\n",
            scheduleStats.StagePrepareCount)
        IndentPrint(true, "     OutstandingStagePrepareCount:%d\n",
            scheduleStats.OutstandingStagePrepareCount)
        IndentPrint(true, "     MaxStagePostExecTime:%f\n",
            scheduleStats.MaxStagePostExecTime)
        IndentPrint(true, "     AvgStagePostExecTime:%f\n",
            scheduleStats.AvgStagePostExecTime)
        IndentPrint(true, "     StagePostExecCount:%d\n",
            scheduleStats.StagePostExecCount)
        IndentPrint(true, "     OutstandingStagePostExecCount:%d\n",
            scheduleStats.OutstandingStagePostExecCount)
        IndentPrint(true, "     MaxBackendScheduleTime:%f\n",
            scheduleStats.MaxBackendScheduleTime)
        IndentPrint(true, "     AvgBackendScheduleTime:%f\n",
            scheduleStats.AvgBackendScheduleTime)
        IndentPrint(true, "     BackendScheduleCount:%d\n",
            scheduleStats.BackendScheduleCount)
        IndentPrint(true, "     OutstandingBackendScheduleCount:%d\n",
            scheduleStats.OutstandingBackendScheduleCount)
        IndentPrint(true, "     ScheduleWorkersStats:\n")
        for index, workerStat := range workerGroupStats {
            IndentPrint(true, "       Group%dStats: %f,%f,%f,%f,%d\n", index,
                workerStat.TotalQueueLen, workerStat.Mean, workerStat.Median,
                workerStat.StdVariance, workerStat.TotalMergedCount)
        }
        dbSyncStats := info.SchedulerStats.DBWorkerStats
        IndentPrint(true, "     DBSyncerStats: %f,%f,%f,%f,%d\n",
            dbSyncStats.TotalQueueLen, dbSyncStats.Mean, dbSyncStats.Median,
            dbSyncStats.StdVariance, dbSyncStats.TotalMergedCount)

        stageBatcherStats := info.SchedulerStats.StageBatcherStats
        IndentPrint(true, "Stage Batcher Stats:\n")
        for i := 0; i < len(stageBatcherStats.QuotaInPriority); i++ {
            IndentPrint(true, "  Priority %d:\n", i)
            IndentPrint(true, "    Quota: %d\n", stageBatcherStats.QuotaInPriority[i])
            IndentPrint(true, "    Allocated: %d\n", stageBatcherStats.Allocated[i])
            IndentPrint(true, "    Pending Jobs:\n")
            for _, job := range stageBatcherStats.PendingJobs[i] {
                IndentPrint(true, "      %s\n", job)
            }
        }
    }

    jobMgrStats := info.JobMgrStats
    IndentPrint(true, "Job Mgr Stats:\n")
    IndentPrint(true, "    State:%v\n",
        jobMgrStats.State)
    IndentPrint(true, "    RecoveryTime:%v\n",
        jobMgrStats.RecoveryTime)
    IndentPrint(true, "    MaxJobSubmitTime:%v\n",
        jobMgrStats.MaxJobSubmitTime)
    IndentPrint(true, "    TotalJobNum:%d\n",
        jobMgrStats.TotalJobNum)
    IndentPrint(true, "    RunningJobNum:%d\n",
        jobMgrStats.RunningJobNum)
    IndentPrint(true, "    CompleteJobNum:%d\n",
        jobMgrStats.CompleteJobNum)
    IndentPrint(true, "    PartiallyCompleteJobNum:%d\n",
        jobMgrStats.PsudoCompleteJobNum)
    IndentPrint(true, "    FailJobNum:%d\n",
        jobMgrStats.FailJobNum)
    IndentPrint(true, "    CanceledJobNum:%d\n",
        jobMgrStats.CanceledJobNum)
    IndentPrint(true, "    PausedJobNum:%d\n",
        jobMgrStats.PausedJobNum)

    
    /*don't show storage stats by default*/
    if !showDetails {
        return
    }

    storageMgrStats := info.StorageMgrStats
    IndentPrint(true, "Storage Mgr Stats:\n")
    IndentPrint(true, "    MaxReadDirTime:%f\n",
        storageMgrStats.MaxReadDir)
    if storageMgrStats.MinReadDir == INITIAL_MIN_VALUE {
        IndentPrint(true, "    MinReadDirTime:N/A\n")
    } else {
        IndentPrint(true, "    MinReadDirTime:%f\n",
            storageMgrStats.MinReadDir)
    }
    IndentPrint(true, "    AvgReadDirTime:%f\n",
        storageMgrStats.AvgReadDir)

    IndentPrint(true, "    MaxRenameTime:%f\n",
        storageMgrStats.MaxRename)
    if storageMgrStats.MinRename == INITIAL_MIN_VALUE {
        IndentPrint(true, "    MinRenameTime:N/A\n")
    } else {
        IndentPrint(true, "    MinRenameTime:%f\n",
            storageMgrStats.MinRename)
    }
    IndentPrint(true, "    AvgRenameTime:%f\n",
        storageMgrStats.AvgRename)

    IndentPrint(true, "    MaxMkDirTime:%f\n",
        storageMgrStats.MaxMkDir)
    if storageMgrStats.MinMkDir == INITIAL_MIN_VALUE {
        IndentPrint(true, "    MinMkDirTime:N/A\n")
    } else {
        IndentPrint(true, "    MinMkDirTime:%f\n",
            storageMgrStats.MinMkDir)
    }
    IndentPrint(true, "    AvgMkDirTime:%f\n",
        storageMgrStats.AvgMkDir)

    IndentPrint(true, "    MaxReadFileTime:%f\n",
        storageMgrStats.MaxReadFile)
    if storageMgrStats.MinReadFile == INITIAL_MIN_VALUE {
        IndentPrint(true, "    MinReadFileTime:N/A\n")
    } else {
        IndentPrint(true, "    MinReadFileTime:%f\n",
            storageMgrStats.MinReadFile)
    }
    IndentPrint(true, "    AvgReadFileTime:%f\n",
        storageMgrStats.AvgReadFile)

    IndentPrint(true, "    MaxWriteFileTime:%f\n",
        storageMgrStats.MaxWriteFile)
    if storageMgrStats.MinWriteFile == INITIAL_MIN_VALUE {
        IndentPrint(true, "    MinWriteFileTime:N/A\n")
    } else {
        IndentPrint(true, "    MinWriteFileTime:%f\n",
            storageMgrStats.MinWriteFile)
    }
    IndentPrint(true, "    AvgWriteFileTime:%f\n",
        storageMgrStats.AvgWriteFile)

    IndentPrint(true, "    MaxRmDirTime:%f\n",
        storageMgrStats.MaxRmDir)
    if storageMgrStats.MinRmDir == INITIAL_MIN_VALUE {
        IndentPrint(true, "    MinRmDirTime:N/A\n")
    } else {
        IndentPrint(true, "    MinRmDirTime:%f\n",
            storageMgrStats.MinRmDir)
    }
    IndentPrint(true, "    AvgRmDirTime:%f\n",
        storageMgrStats.AvgRmDir)

    IndentPrint(true, "    MaxDeleteFileTime:%f\n",
        storageMgrStats.MaxDeleteFile)
    if storageMgrStats.MinDeleteFile == INITIAL_MIN_VALUE {
        IndentPrint(true, "    MinDeleteFileTime:N/A\n")
    } else {
        IndentPrint(true, "    MinDeleteFileTime:%f\n",
            storageMgrStats.MinDeleteFile)
    }
    IndentPrint(true, "    AvgDeleteFileTime:%f\n",
        storageMgrStats.AvgDeleteFile)

    IndentPrint(true, "    MaxMkDirPathTime:%f\n",
        storageMgrStats.MaxMkDirPath)
    if storageMgrStats.MinMkDirPath == INITIAL_MIN_VALUE {
        IndentPrint(true, "    MinMkDirPathTime:N/A\n")
    } else {
        IndentPrint(true, "    MinMkDirPathTime:%f\n",
            storageMgrStats.MinMkDirPath)
    }
    IndentPrint(true, "    AvgMkDirPathTime:%f\n",
        storageMgrStats.AvgMkDirPath)

    IndentPrint(true, "    MaxChownTime:%f\n",
        storageMgrStats.MaxChown)
    if storageMgrStats.MinChown == INITIAL_MIN_VALUE {
        IndentPrint(true, "    MinChownTime:N/A\n")
    } else {
        IndentPrint(true, "    MinChownTime:%f\n",
            storageMgrStats.MinChown)
    }
    IndentPrint(true, "    AvgChownTime:%f\n",
        storageMgrStats.AvgChown)

    IndentPrint(true, "    MaxChmodTime:%f\n",
        storageMgrStats.MaxChmod)
    if storageMgrStats.MinChmod == INITIAL_MIN_VALUE {
        IndentPrint(true, "    MinChmodTime:N/A\n")
    } else {
        IndentPrint(true, "    MinChmodTime:%f\n",
            storageMgrStats.MinChmod)
    }
    IndentPrint(true, "    AvgChmodTime:%f\n",
        storageMgrStats.AvgChmod)

    IndentPrint(true, "    OutstandingMkDir:%d\n",
        storageMgrStats.OutstandingMkDirCount)
    IndentPrint(true, "    OutstandingRename:%d\n",
        storageMgrStats.OutstandingRenameCount)
    IndentPrint(true, "    OutstandingMkDirPath:%d\n",
        storageMgrStats.OutstandingMkDirPathCount)
    IndentPrint(true, "    OutstandingRmDir:%d\n",
        storageMgrStats.OutstandingRmDirCount)
    IndentPrint(true, "    OutstandingReadDir:%d\n",
        storageMgrStats.OutstandingReadDirCount)
    IndentPrint(true, "    OutstandingReadFile:%d\n",
        storageMgrStats.OutstandingReadFileCount)
    IndentPrint(true, "    OutstandingWriteFile:%d\n",
        storageMgrStats.OutstandingWriteFileCount)
    IndentPrint(true, "    OutstandingDeleteFile:%d\n",
        storageMgrStats.OutstandingDeleteFileCount)
    IndentPrint(true, "    OutstandingChown:%d\n",
        storageMgrStats.OutstandingChownCount)
    IndentPrint(true, "    OutstandingChmod:%d\n",
        storageMgrStats.OutstandingChmodCount)
    IndentPrint(true, "    OutstandingNFSMount:%d\n",
        storageMgrStats.OutstandingNFSMountCount)
    IndentPrint(true, "    OutstandingAlamoMount:%d\n",
        storageMgrStats.OutstandingGlusterMountCount)
    IndentPrint(true, "    OutstandingAnnaMount:%d\n",
        storageMgrStats.OutstandingCephMountCount)
    IndentPrint(true, "    TotalRetryCount:%d\n",
        storageMgrStats.TotalRetryCount)

    fmt.Printf("Operation Stats:\n")
    opStatKeys := info.OpStatKeys
    opStatVals := info.OpStatVals
    for i := 0; i < len(opStatKeys); i ++ {
        IndentPrint(true, "%s:%d\n",
            opStatKeys[i], opStatVals[i])
    }
}

func (cmd *dashboardCommand) Show(showDetails bool) {
    err, dashboard := cmd.client.GetDashBoard()
    if err != nil {
        fmt.Printf("Fail to get dashboard: %s\n",
            err.Error())
        return
    }
    
    ShowDashBoardInfo(dashboard, showDetails)
}
