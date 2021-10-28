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
    "io"
    "errors"
    "strings"

    . "github.com/xtao/bioflow/common"
    . "github.com/xtao/bioflow/message"
    "sync"
    "time"
    "fmt"
)

type Scheduler interface {
    Start() int
    Stop()
    Quiesce()
    Quiesced() bool
    Resume()
    SubmitJob(job Job) error
    ResumeJob(job Job) error
    CancelJob(job Job) error
    KillJobTasks(job Job, taskId string) error
    Enable() error
    Disable() error
    Disabled() bool
    RequeueJobStage(job Job, stageId string, resource *ResourceSpecJSONData) error
    GetJobStatus(job Job, jobStatus *BioflowJobStatus) error
    GetHistoryJobStatus(jobId string) (error, *BioflowJobStatus)
    GetJobLogs(job Job, jobId string, stageName string, stageId string) (error,map[string]BioflowStageLog)
    HandleTaskNotify(backendId string, body io.ReadCloser) error
    DestroyJobScheduleInfo(job Job) error
    PersistJobHistory(job Job, recover bool) error
    GetStats() (error, *BioflowSchedulerStats)
    UpdateConfig(config *JobSchedulerConfig)
    KickScheduler()
    KickScheduleJob(job Job, markOnly bool)
    TryKickScheduleJob(job Job, markOnly bool)
    UpdateJobPriority(job Job, oldPri int) error
    AutoScheduleHDFS() bool
    GetJobTaskResourceUsageInfo(job Job, jobId string,taskId string) (error, map[string]ResourceUsageInfo)
    DumpStageBatcherInfo()
    FailbackTasks(backendID string, taskIds []string)
}

type ScheduleAction int

func (action ScheduleAction)String() string{
    switch int(action) {
        case 0:
            return "INVALID"
        case 1:
            return "SCHEDULE_STAGE"
        case 2:
            return "RETRY_STAGE"
        case 3:
            return "PRUNE_STAGE"
        case 4:
            return "ABORT_STAGE"
        case 5:
            return "DO_NOTHING"
        case 6:
            return "FINISH_JOB"
        case 7:
            return "UPDATE_DB"
        case 8:
            return "PSEUDO_FINISH"
        case 9:
            return "CHECK_FAIL_STATUS"
        case 10:
            return "CHECK_LOST_STATUS"
        case 11:
            return "CLEANUP"
        case 12:
            return "BUILD_GRAPH"
        case 13:
            return "OFFLOAD_CHECK"
        default:
            return "INVALID"
    }
}

/*pre-difined schedule actions*/
const (
    S_ACTION_INVALID = ScheduleAction(-1)
    S_ACTION_SCHEDULE = ScheduleAction(1)
    S_ACTION_RETRY_STAGE = ScheduleAction(2)
    S_ACTION_PRUNE_STAGE = ScheduleAction(3)
    S_ACTION_ABORT_JOB = ScheduleAction(4)
    S_ACTION_NONE = ScheduleAction(5)
    S_ACTION_FINISH_JOB = ScheduleAction(6)
    S_ACTION_UPDATE_DB = ScheduleAction(7)
    S_ACTION_PSEUDO_FINISH_JOB = ScheduleAction(8)
    S_ACTION_CHECK_FAIL_STATUS = ScheduleAction(9)
    S_ACTION_CHECK_LOST_STATUS = ScheduleAction(10)
    S_ACTION_CLEANUP_JOB = ScheduleAction(11)
    S_ACTION_BUILD_JOB_GRAPH = ScheduleAction(12)
    S_ACTION_OFFLOAD_SKIP_TO_NEXT_STATE = ScheduleAction(13)
)

var (
    JOB_GRAPH_PENDING int = 0
    JOB_GRAPH_FINISHED int = 1
    JOB_GRAPH_PSEUDO_FINISH int = 2
    JOB_GRAPH_ERROR int = 3
    JOB_GRAPH_NOBUILD int = 4
)

type ResourceSpecExpression struct {
    Cpu string
    Memory string
    Disk string
    GPU string
    GPUMemory string
}

type ResourceSpec struct {
    cpu float64
    memory float64
    disk float64
    serverType string
    gpu float64
    gpuMemory float64
}

func (spec *ResourceSpec) GetCPU() float64 {
    return spec.cpu
}

func (spec *ResourceSpec) SetCPU(cpu float64) {
    spec.cpu = cpu
}

func (spec *ResourceSpec) GetMemory() float64 {
    return spec.memory
}

func (spec *ResourceSpec) SetMemory(memory float64) {
    spec.memory = memory
}

func (spec *ResourceSpec) GetDisk() float64 {
    return spec.disk
}

func (spec *ResourceSpec) SetDisk(disk float64) {
    spec.disk = disk
}

func (spec *ResourceSpec) GetServerType() string {
    return spec.serverType
}

func (spec *ResourceSpec) SetServerType(serverType string) {
    spec.serverType = serverType
}

func (spec *ResourceSpec) GetGPU() float64 {
    return spec.gpu
}

func (spec *ResourceSpec) SetGPU(gpu float64) {
    spec.gpu = gpu
}

func (spec *ResourceSpec) GetGPUMemory() float64 {
    return spec.gpuMemory
}

func (spec *ResourceSpec) SetGPUMemory(gpu float64) {
    spec.gpuMemory = gpu
}

/*
 * Scheduler related error definition
 */
var (
    SCHED_ERR_PREPARE_EXECUTION error = errors.New("Error when preparing stage execution")
    SCHED_ERR_QUIESCED error = errors.New("The scheduler is quiesced")
)

/*define errors for graph builder here*/
var (
    GB_ERR_SUSPEND_BUILD error = errors.New("suspend build graph for inputs ready")
)

type ContainerNetMode int

func (s ContainerNetMode) String() string {
    switch int(s) {
        case 0:
            return "CLUSTER_SIMPLEX"
        case 1:
            return "CLUSTER_DUPLEX"
        default:
            return "INVALID"
    }
}

const (
    /*have network to access other nodes, can't be accessed by other nodes*/
    CLUSTER_NET_SIMPLEX_MODE ContainerNetMode = 0

    /*the container can access and be accessed by other containers*/
    CLUSTER_NET_DUPLEX_MODE ContainerNetMode = 1
)

type StorageType string

func (s StorageType) String() string {
    return string(s)
}

const (
    STORAGE_TYPE_INVALID = StorageType("INVALID")
    STORAGE_TYPE_HDFS = StorageType("HDFS")
    STORAGE_TYPE_LATENCY = StorageType("LATENCY")
    STORAGE_TYPE_THROUGHPUT = StorageType("THROUGHPUT")
    STORAGE_TYPE_DEFAULT = StorageType("DEFAULT")
)

func StringToStorageType(s string) StorageType {
    switch strings.ToUpper(s) {
        case STORAGE_HDFS:
            return STORAGE_TYPE_HDFS
        case STORAGE_LATENCY:
            return STORAGE_TYPE_LATENCY
        case STORAGE_THROUGHPUT:
            return STORAGE_TYPE_THROUGHPUT
        default:
            return STORAGE_TYPE_INVALID
    }
}

type EventNotifyInfo struct {
    message string
    hostname string
    ip string
}

func (info *EventNotifyInfo)Hostname() string {
    return info.hostname
}

func (info *EventNotifyInfo)IP() string {
    return info.ip
}

func (info *EventNotifyInfo)Message() string {
    return info.message
}

func NewEventNotifyInfo(message string, hostname string, ip string) *EventNotifyInfo {
    return &EventNotifyInfo{
        message: message,
        hostname: hostname,
        ip: ip,
    }
}

type StageBatcher struct {
    priorityQuota map[int]int
    /*quota used in a job*/
    usedInJob map[string]int
    /*quota reserved for a job*/
    reserveForJob map[string]int
    /*all quota allocated in a priority*/
    allocatedInPriority map[int]int
    /*jobs ready to schedule, but reached quota limit*/
    waitForScheduling map[int]map[Job]bool
    /*skip update metric when a job on scheduling*/
    onSchedule map[string]bool
    scheduler Scheduler
    kick bool
    lock *sync.RWMutex
}

type StageBaterStats struct {
    PriorityQuota []int
    TotalAllocation []int
    PendingJobs [][]string

}

func NewStageBaterStats() *StageBaterStats {
    size := JOB_MAX_PRI-JOB_MIN_PRI + 1
    return &StageBaterStats{
        PriorityQuota: make([]int, size, size),
        TotalAllocation: make([]int, size, size),
        PendingJobs: make([][]string, size, size),
    }
}

func (batcher *StageBatcher) PrepareSchedule(job Job) {
    batcher.lock.Lock()
    batcher.onSchedule[job.GetID()] = true
    batcher.lock.Unlock()
}

func (batcher *StageBatcher) FinishSchedule(job Job) {
    batcher.lock.Lock()
    delete(batcher.onSchedule, job.GetID())
    batcher.lock.Unlock()
}

/*1.Get and record how many stages are scheduled from 'JobScheduleInfo', then update quota record.
*2.Wake up jobs wait to schedule as much as possible.
*/
func (batcher *StageBatcher) UpdateMetric(job Job) {
    schedInfo := job.GetScheduleInfo()
    current := 0
    if schedInfo != nil {
        current = schedInfo.PendingStageCount()
    }

    batcher.lock.Lock()
    if _, ok := batcher.onSchedule[job.GetID()]; ok {
        batcher.lock.Unlock()
        SchedulerLogger.Infof("STAGEBATCHER job %s on schedule, skip update", job.GetID())
        return
    }

    last := batcher.usedInJob[job.GetID()]
    batcher.usedInJob[job.GetID()] = current
    lastInPriority := batcher.allocatedInPriority[job.Priority()]
    currentInPriority := lastInPriority - last + current
    batcher.allocatedInPriority[job.Priority()] = currentInPriority
    priorityQuota := batcher.priorityQuota[job.Priority()]
    if batcher.usedInJob[job.GetID()] == 0 {
        delete(batcher.usedInJob, job.GetID())
    }
    batcher.lock.Unlock()

    SchedulerLogger.Infof("STAGEBATCHER %d stages scheduled for job %s, %d stages scheduled for priority %d",
        current, job.GetID(), currentInPriority, job.Priority())
    if priorityQuota > currentInPriority || priorityQuota == -1 {
        batcher.WakeUpWaittingJobs(job.Priority())
    }
}

func (batcher *StageBatcher) WakeUpWaittingJobs(priority int) {
    jobsToWakeUp := make(map[Job]bool)

    batcher.lock.Lock()
    for job, _ := range batcher.waitForScheduling[priority] {
        if batcher.stageQuota(job) != 0 {
            jobsToWakeUp[job] = true
            delete(batcher.waitForScheduling[priority], job)
        }
    }
    batcher.lock.Unlock()

    for job, _ := range jobsToWakeUp {
        SchedulerLogger.Infof("STAGEBATCHER kick schedule job %s", job.GetID())
        batcher.scheduler.KickScheduleJob(job, !batcher.kick)
    }
}

func (batcher *StageBatcher) GetStageQuota(job Job) int {
    batcher.lock.RLock()
    quota := batcher.stageQuota(job)
    batcher.lock.RUnlock()
    SchedulerLogger.Infof("STAGEBATCHER get stage quota %d for job %s", quota, job.GetID())
    return quota
}

func (batcher *StageBatcher) RequestStageQuota(job Job, need int) int {
    batcher.lock.Lock()
    quota := batcher.stageQuota(job)
    var reserve int
    if quota == -1 || quota > need {
        reserve = need
    } else {
        reserve = quota
    }
    batcher.reserveForJob[job.GetID()] = batcher.reserveForJob[job.GetID()] + reserve
    batcher.allocatedInPriority[job.Priority()] = batcher.allocatedInPriority[job.Priority()] + reserve
    batcher.lock.Unlock()

    SchedulerLogger.Infof("STAGEBATCHER request stage quota %d for job %s", reserve, job.GetID())
    return reserve
}

func (batcher *StageBatcher) stageQuota(job Job) int {
    quota := 0
    jobQuota := job.StageQuota()

    priorityQuota := batcher.priorityQuota[job.Priority()]
    usedInJob := batcher.usedInJob[job.GetID()]
    reserveForJob := batcher.reserveForJob[job.GetID()]
    allocatedInPriority := batcher.allocatedInPriority[job.Priority()]

    /*No limit for 'job' and 'priority'*/
    if priorityQuota == -1 && jobQuota == -1 {
        quota = -1
    } else if priorityQuota == -1 {
        quota = jobQuota - usedInJob - reserveForJob
        if quota < 0 {
            quota = 0
        }
    } else if jobQuota == -1 {
        quota = priorityQuota - allocatedInPriority
        if quota < 0 {
            quota = 0
        }
    } else {
        q1 := jobQuota - usedInJob - reserveForJob
        quota = priorityQuota - allocatedInPriority
        if q1 < quota {
            quota = q1
        }
        if quota < 0 {
            quota = 0
        }
    }
    return quota
}

func (batcher *StageBatcher) ReleaseQuota(job Job, release int) {
    batcher.removeReserve(job, release, false)
}

func (batcher *StageBatcher) UseQuota(job Job, use int) {
    batcher.removeReserve(job, use, true)
}

func (batcher *StageBatcher) removeReserve(job Job, remove int, used bool) {
    batcher.lock.Lock()
    batcher.reserveForJob[job.GetID()] = batcher.reserveForJob[job.GetID()] - remove
    batcher.usedInJob[job.GetID()] = batcher.usedInJob[job.GetID()] + remove
    if batcher.reserveForJob[job.GetID()] == 0 {
        delete(batcher.reserveForJob, job.GetID())
    }
    if !used {
        batcher.allocatedInPriority[job.Priority()] = batcher.allocatedInPriority[job.Priority()] - remove
    }
    batcher.lock.Unlock()
}

func (batcher *StageBatcher) TryScheduling(job Job, markOnly bool) {
    batcher.UpdateMetric(job)
    batcher.lock.Lock()
    if batcher.stageQuota(job) == 0 {
        batcher.waitSchduling(job)
        batcher.lock.Unlock()
        SchedulerLogger.Logger.Infof("STAGEBATCHER job %s wait for scheduling", job.GetID())
    } else {
        batcher.lock.Unlock()
        batcher.scheduler.KickScheduleJob(job, markOnly)
        SchedulerLogger.Logger.Infof("STAGEBATCHER job %s kick scheduling", job.GetID())
    }
}

func (batcher *StageBatcher) waitSchduling(job Job) {
    if jobs, ok := batcher.waitForScheduling[job.Priority()]; ok {
        jobs[job] = true
    } else {
        jobs := make(map[Job]bool)
        jobs[job] = true
        batcher.waitForScheduling[job.Priority()] = jobs
    }
}

func (batcher *StageBatcher) SetPriorityQuota(priority, quota int) {
    if quota < -1 {
        return
    }
    batcher.lock.Lock()
    batcher.priorityQuota[priority] = quota
    wakeup := quota == -1 || batcher.allocatedInPriority[priority] < quota
    batcher.lock.Unlock()
    if wakeup {
        batcher.WakeUpWaittingJobs(priority)
    }
    return
}

func (batcher *StageBatcher) keepWakingUp() {
    for {
        time.Sleep(5 * time.Minute)
        SchedulerLogger.Infof("STAGEBATCHER time to wake up waiting jobs")
        for i := JOB_MIN_PRI; i <= JOB_MAX_PRI; i++ {
            batcher.WakeUpWaittingJobs(i)
        }
    }
}

func (batcher *StageBatcher) UpdateJobPriority(job Job, oldPri int) {
    batcher.lock.Lock()
    allocated := batcher.usedInJob[job.GetID()] + batcher.reserveForJob[job.GetID()]
    batcher.allocatedInPriority[oldPri] = batcher.allocatedInPriority[oldPri] - allocated
    batcher.allocatedInPriority[job.Priority()] = batcher.allocatedInPriority[job.Priority()] + allocated
    wakup := batcher.priorityQuota[oldPri] == -1 || batcher.allocatedInPriority[oldPri] < batcher.priorityQuota[oldPri]
    batcher.lock.Unlock()
    if wakup {
        batcher.WakeUpWaittingJobs(oldPri)
    }
}

func (batcher *StageBatcher) DumpInfo() {
    totalAllocationInPriority := 0
    totalAllocationInJobs := 0
    batcher.lock.RLock()
    for _, count := range batcher.allocatedInPriority {
        totalAllocationInPriority += count
    }

    for _, count := range batcher.usedInJob {
        totalAllocationInJobs += count
    }

    for _, count := range batcher.reserveForJob {
        totalAllocationInJobs += count
    }
    batcher.lock.RUnlock()

    info := fmt.Sprintf("All quota allocated in priority: %d\nAll quota allocated in job: %d\n", totalAllocationInPriority, totalAllocationInJobs)
    SchedulerLogger.Infof("Start Dump StageBatcher Info: \n%sEnd Dump StageBatcher Info", info)
}

func (batcher *StageBatcher) Stats() *StageBaterStats {
    stats := NewStageBaterStats()
    batcher.lock.RLock()
    for priority, count := range batcher.allocatedInPriority {
        stats.TotalAllocation[priority] = count
    }
    for priority, quota := range batcher.priorityQuota {
        stats.PriorityQuota[priority] = quota
    }
    for priority, jobs := range batcher.waitForScheduling {
        var joblist []string
        for job, _ := range jobs {
            joblist = append(joblist, job.GetID())
        }
        stats.PendingJobs[priority] = joblist
    }
    batcher.lock.RUnlock()
    return stats
}

func (batcher *StageBatcher) Kick() {
    batcher.kick = true
}

func NewStageBatcher(scheduler Scheduler) *StageBatcher {
    batcher := &StageBatcher{
        priorityQuota: make(map[int]int),
        usedInJob: make(map[string]int),
        allocatedInPriority: make(map[int]int),
        reserveForJob: make(map[string]int),
        waitForScheduling: make(map[int]map[Job]bool),
        onSchedule: make(map[string]bool),
        scheduler: scheduler,
        kick: false,
        lock: new(sync.RWMutex),
    }
    for i := JOB_MIN_PRI; i <= JOB_MAX_PRI; i++ {
        batcher.priorityQuota[i] = -1
    }

    go batcher.keepWakingUp()
    return batcher
}
