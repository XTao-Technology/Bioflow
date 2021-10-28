/* 
 Copyright (c) 2018 XTAO technology <www.xtaotech.com>
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
    "time"
    "sync/atomic"

    . "github.com/xtao/bioflow/common"
    . "github.com/xtao/bioflow/message"
)

type SchedulePerfStats struct {
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

func _StartProfile(preTime *time.Time, outstandingCount *int64) {
    *preTime = time.Now()
    atomic.AddInt64(outstandingCount, 1)
}

func _EndProfile(preTime time.Time, maxVal *float64, avgVal *float64,
    totalCount *int64, outstandingCount *int64) {
    latency := time.Now().Sub(preTime).Seconds()
    StatUtilsMax(maxVal, latency)
    StatUtilsAvg(avgVal, latency, totalCount)
    atomic.AddInt64(outstandingCount, -1)
}

func (stats *SchedulePerfStats)Reset() {
    stats.MaxSchedJobTime = 0
    stats.AvgSchedJobTime = 0
    stats.SchedJobCount = 0
    stats.MaxSchedStageTime = 0
    stats.AvgSchedStageTime = 0
    stats.SchedStageCount = 0
    stats.MaxSyncDBTime = 0
    stats.AvgSyncDBTime = 0
    stats.SyncDBCount = 0
    stats.MaxJobStateMachineTime = 0
    stats.AvgJobStateMachineTime = 0
    stats.JobStateMachineCount = 0
    stats.MaxStagePostExecTime = 0
    stats.AvgStagePostExecTime = 0
    stats.StagePostExecCount = 0
    stats.MaxStagePrepareTime = 0
    stats.AvgStagePrepareTime = 0
    stats.StagePrepareCount = 0
    stats.MaxBackendScheduleTime = 0
    stats.AvgBackendScheduleTime = 0
    stats.BackendScheduleCount = 0
}

func (stats *SchedulePerfStats)ToBioflowSchedulePerfStats() *BioflowSchedulePerfStats {
    return &BioflowSchedulePerfStats {
        MaxSchedJobTime: stats.MaxSchedJobTime,
        AvgSchedJobTime: stats.AvgSchedJobTime,
        SchedJobCount: stats.SchedJobCount,
        OutstandingSchedJobCount: stats.OutstandingSchedJobCount,
        MaxSchedStageTime: stats.MaxSchedStageTime,
        AvgSchedStageTime: stats.AvgSchedStageTime,
        SchedStageCount: stats.SchedStageCount,
        OutstandingSchedStageCount: stats.OutstandingSchedStageCount,
        MaxSyncDBTime: stats.MaxSyncDBTime,
        AvgSyncDBTime: stats.AvgSyncDBTime,
        SyncDBCount: stats.SyncDBCount,
        OutstandingSyncDBCount: stats.OutstandingSyncDBCount,
        MaxJobStateMachineTime: stats.MaxJobStateMachineTime,
        AvgJobStateMachineTime: stats.AvgJobStateMachineTime,
        JobStateMachineCount: stats.JobStateMachineCount,
        OutstandingJobStateMachineCount: stats.OutstandingJobStateMachineCount,
        MaxStagePostExecTime: stats.MaxStagePostExecTime,
        AvgStagePostExecTime: stats.AvgStagePostExecTime,
        StagePostExecCount: stats.StagePostExecCount,
        OutstandingStagePostExecCount: stats.OutstandingStagePostExecCount,
        MaxStagePrepareTime: stats.MaxStagePrepareTime,
        AvgStagePrepareTime: stats.AvgStagePrepareTime,
        StagePrepareCount: stats.StagePrepareCount,
        OutstandingStagePrepareCount: stats.OutstandingStagePrepareCount,
        MaxBackendScheduleTime: stats.MaxBackendScheduleTime,
        AvgBackendScheduleTime: stats.AvgBackendScheduleTime,
        BackendScheduleCount: stats.BackendScheduleCount,
        OutstandingBackendScheduleCount: stats.OutstandingBackendScheduleCount,
    }
}

func (stats *SchedulePerfStats)StartProfileSchedJob(preTime *time.Time) {
    _StartProfile(preTime, &stats.OutstandingSchedJobCount)
}

func (stats *SchedulePerfStats)EndProfileSchedJob(preTime time.Time) {
    _EndProfile(preTime, &stats.MaxSchedJobTime, &stats.AvgSchedJobTime,
        &stats.SchedJobCount, &stats.OutstandingSchedJobCount)
}

func (stats *SchedulePerfStats)StartProfileSchedStage(preTime *time.Time) {
    _StartProfile(preTime, &stats.OutstandingSchedStageCount)
}

func (stats *SchedulePerfStats)EndProfileSchedStage(preTime time.Time) {
    _EndProfile(preTime, &stats.MaxSchedStageTime, &stats.AvgSchedStageTime,
        &stats.SchedStageCount, &stats.OutstandingSchedStageCount)
}

func (stats *SchedulePerfStats)StartProfileSyncDB(preTime *time.Time) {
    _StartProfile(preTime, &stats.OutstandingSyncDBCount)
}

func (stats *SchedulePerfStats)EndProfileSyncDB(preTime time.Time) {
    _EndProfile(preTime, &stats.MaxSyncDBTime, &stats.AvgSyncDBTime,
        &stats.SyncDBCount, &stats.OutstandingSyncDBCount)
}

func (stats *SchedulePerfStats)StartProfileJobStateMachine(preTime *time.Time) {
    _StartProfile(preTime, &stats.OutstandingJobStateMachineCount)
}

func (stats *SchedulePerfStats)EndProfileJobStateMachine(preTime time.Time) {
    _EndProfile(preTime, &stats.MaxJobStateMachineTime, &stats.AvgJobStateMachineTime,
        &stats.JobStateMachineCount, &stats.OutstandingJobStateMachineCount)
}

func (stats *SchedulePerfStats)StartProfileStagePostExec(preTime *time.Time) {
    _StartProfile(preTime, &stats.OutstandingStagePostExecCount)
}

func (stats *SchedulePerfStats)EndProfileStagePostExec(preTime time.Time) {
    _EndProfile(preTime, &stats.MaxStagePostExecTime, &stats.AvgStagePostExecTime,
        &stats.StagePostExecCount, &stats.OutstandingStagePostExecCount)
}

func (stats *SchedulePerfStats)StartProfileStagePrepare(preTime *time.Time) {
    _StartProfile(preTime, &stats.OutstandingStagePrepareCount)
}

func (stats *SchedulePerfStats)EndProfileStagePrepare(preTime time.Time) {
    _EndProfile(preTime, &stats.MaxStagePrepareTime, &stats.AvgStagePrepareTime,
        &stats.StagePrepareCount, &stats.OutstandingStagePrepareCount)
}

func (stats *SchedulePerfStats)StartProfileBackendSchedule(preTime *time.Time) {
    _StartProfile(preTime, &stats.OutstandingBackendScheduleCount)
}

func (stats *SchedulePerfStats)EndProfileBackendSchedule(preTime time.Time) {
    _EndProfile(preTime, &stats.MaxBackendScheduleTime, &stats.AvgBackendScheduleTime,
        &stats.BackendScheduleCount, &stats.OutstandingBackendScheduleCount)
}
