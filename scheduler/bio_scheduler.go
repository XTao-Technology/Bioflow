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
    "fmt"
	"strings"
    "sync"
    "encoding/json"
    "errors"
    "io"
    "time"
    "sync/atomic"

    "github.com/xtao/bioflow/scheduler/backend"
    . "github.com/xtao/bioflow/dbservice"
    . "github.com/xtao/bioflow/storage"
    . "github.com/xtao/bioflow/common"
    . "github.com/xtao/bioflow/message"
    "github.com/xtao/bioflow/eventbus"
    . "github.com/xtao/bioflow/scheduler/common"
    . "github.com/xtao/bioflow/scheduler/physicalscheduler"
)


const (
    DEFAULT_CHECK_INTERVAL int = 3
    DELAY_CHECK_TIMEOUT int = 600
    KICK_SCHEDULE_PERIOD int = 600

    /*How many threads do cleanup work*/
    CLEANUP_WORKER_COUNT uint32 = 5
    ACCT_WORKER_COUNT uint32 = 5
    ASYNC_EVENT_WORKER_COUNT int = 16
    JOB_STALE_TIMEOUT float64 = 5
    MAX_JOBS_SCHEDULE_PER_ROUND int = 300
    MAX_STAGES_PER_ROUND int = 1024 * 1024
    MAX_STAGES_ISSUE_IN_PARALLEL int = 256
    MAX_BATCH_STAGES int = 20

    DEF_SOFT_LIMIT_RATIO float64 = 0
    DEF_CAP_RATIO float64 = 0.2
    VALID_CAP_RATIO_LIMIT float64 = 0.01 

    DEF_MAX_CPU_PER_TASK float64 = 128
    DEF_MAX_MEMORY_PER_TASK float64 = 256000
    VALID_RESOURCE_LIMIT float64 = 0.5

    MAX_NOTIFY_JOB_COUNT int = 20000
    JOB_SCHEDULE_WORKER_COUNT uint32 = 32

    MAX_EVENT_COUNT_PER_ROUND int = 25600
    JOB_SCHEDULE_EVENT_HANDLER_WORKER_COUNT uint32 = 128

    /*Labels for feature list*/
    FEATURE_LABEL_PROFILER_KEY = "EXECUTOR-FEATURE-LABEL-PROFILER"
    FEATURE_LABEL_PROFILER_VALUE = "ENABLE"
)

type bioScheduler struct {
    beScheduler      backendScheduler

    /*
     * Job tracker tracks all jobs and its
     * schedule information
     */
    jobTracker      *jobTracker
    callbackURI     string
    checkInterval   int

    resourceAcctWorkers *HashSyncWorker
    pendingAsyncEventCount int64

    scheduleLock   *sync.Mutex
    scheduleCond   *sync.Cond
    scheduleKickCount int
    kickSchedulePeriod int

    autoReclaimTask bool
    autoReScheduleHangTasks bool
    disableOOMKill bool
    autoScheduleHDFS bool
    enableStorageConstraint bool

    quiesced bool
    disabled bool

    /*track delay check jobs*/
    delayJobChecker *DelayMerger
    delayJobCheckTimeout int
    maxJobsSchedulePerRound int

    softLimitRatio float64

    jobScheduleStateSyncer *HashSyncWorker
    jobScheduleWorkers []*HashSyncWorker
    jobScheduleEventHandleWorkers *HashSyncWorker
    jobTerminateEventHandleWorkers *HashSyncWorker
    jobScheduleWorkerCount uint32

    stageScheduleWorker *SyncWorker
    enableStageWorker bool

    /*Qos Parameters*/
    qosEnabled bool
    cpuCapRatio float64
    memCapRatio float64

    /*Schedule resource limit parameters*/
    maxCPUPerTask float64
    maxMemoryPerTask float64

    /*whether enable the profiler feature*/
    profilerEnabled bool
    enforceServerTypeConstraint bool

    /*whether set the container workdir equal stage workdir*/
    setStageWorkdirToContainerWorkdir bool

    /*whether enable the cleanup feature*/
    cleanupPatternEnable bool

    enableRightControl bool
    useTaskOwner bool
    asyncTrackTaskInDB bool

    perfStats SchedulePerfStats

    batcher *StageBatcher
    stageRetryDelay int
}


func NewBIOScheduler(callbackURI string, jobTracker *jobTracker,
    config *JobSchedulerConfig) *bioScheduler {
    scheduler := &bioScheduler{
        jobTracker: jobTracker,
        callbackURI: callbackURI,
        checkInterval: DEFAULT_CHECK_INTERVAL,
        pendingAsyncEventCount: 0,
        resourceAcctWorkers: NewHashSyncWorker(ACCT_WORKER_COUNT, MAX_NOTIFY_JOB_COUNT, false),
        quiesced: false,
        disabled: false,
        autoReclaimTask: false,
        autoReScheduleHangTasks: false,
        disableOOMKill: false,
        enableStorageConstraint: false,
        enforceServerTypeConstraint: false,
        scheduleKickCount: 0,
        delayJobChecker: NewDelayMerger(300),
        kickSchedulePeriod: KICK_SCHEDULE_PERIOD,
        delayJobCheckTimeout: DELAY_CHECK_TIMEOUT,
        maxJobsSchedulePerRound: MAX_JOBS_SCHEDULE_PER_ROUND,
        softLimitRatio: DEF_SOFT_LIMIT_RATIO,
        jobScheduleStateSyncer: NewHashSyncWorker(JOB_SCHEDULE_WORKER_COUNT, MAX_NOTIFY_JOB_COUNT, true),
        jobScheduleWorkers: make([]*HashSyncWorker, 0),
        jobScheduleEventHandleWorkers: NewHashSyncWorker(JOB_SCHEDULE_EVENT_HANDLER_WORKER_COUNT, MAX_EVENT_COUNT_PER_ROUND, false),
        jobTerminateEventHandleWorkers: NewHashSyncWorker(CLEANUP_WORKER_COUNT, MAX_NOTIFY_JOB_COUNT, false),
        stageScheduleWorker: NewSyncWorker(MAX_STAGES_PER_ROUND, MAX_STAGES_ISSUE_IN_PARALLEL, false),
        autoScheduleHDFS: false,
        qosEnabled: false,
        profilerEnabled: false,
        cleanupPatternEnable: false,
        setStageWorkdirToContainerWorkdir: false,
        enableRightControl: false,
        enableStageWorker: false,
        useTaskOwner: false,
        cpuCapRatio: DEF_CAP_RATIO,
        memCapRatio: DEF_CAP_RATIO,
        maxCPUPerTask: DEF_MAX_CPU_PER_TASK,
        maxMemoryPerTask: DEF_MAX_MEMORY_PER_TASK,
        jobScheduleWorkerCount: JOB_SCHEDULE_WORKER_COUNT,
        stageRetryDelay: 30,
        asyncTrackTaskInDB: false,
    }
    scheduler.batcher = NewStageBatcher(scheduler)
    scheduler.UpdateConfig(config)
    /*init the job scheduler workers*/
    for pri := JOB_MIN_PRI; pri <= JOB_MAX_PRI; pri ++ {
        scheduler.jobScheduleWorkers = append(scheduler.jobScheduleWorkers,
            NewHashSyncWorker(scheduler.jobScheduleWorkerCount, MAX_JOBS_SCHEDULE_PER_ROUND, true))
    }

    scheduler.beScheduler = NewRRBeScheduler(scheduler, callbackURI)
    scheduler.scheduleLock = new(sync.Mutex)
    scheduler.scheduleCond = sync.NewCond(scheduler.scheduleLock)
    return scheduler
}

func (scheduler *bioScheduler) ResetStats() {
    scheduler.perfStats.Reset()
}

func (scheduler *bioScheduler) UpdateConfig(config *JobSchedulerConfig) {
    if config != nil {
        SchedulerLogger.Infof("update job scheduler config: %v\n",
            *config)
        if config.ScheduleCheckPeriod > 0 {
            scheduler.kickSchedulePeriod = config.ScheduleCheckPeriod
        }
        if config.JobCheckInterval > 0 {
            scheduler.checkInterval = config.JobCheckInterval
        }
        if config.JobDelayCheckTimeout > 0 {
            scheduler.delayJobCheckTimeout = config.JobDelayCheckTimeout
        }
        if config.MaxJobsSchedulePerRound > 0 {
            scheduler.maxJobsSchedulePerRound = config.MaxJobsSchedulePerRound
        }
        if config.JobScheduleWorkerCount > 0 {
            scheduler.jobScheduleWorkerCount = uint32(config.JobScheduleWorkerCount)
        }
        scheduler.softLimitRatio = config.SoftLimitRatio
        scheduler.autoReScheduleHangTasks = config.AutoReScheduleHangTasks
        scheduler.disableOOMKill = config.DisableOOMKill
        scheduler.autoScheduleHDFS = config.AutoScheduleHDFS
        scheduler.enableStorageConstraint = config.EnableStorageConstraint
        scheduler.enforceServerTypeConstraint = config.EnforceServerTypeConstraint
        scheduler.enableRightControl = config.EnableRightControl
        scheduler.enableStageWorker = config.EnableStageWorker
        scheduler.asyncTrackTaskInDB = config.AsyncTrackTaskInDB
        scheduler.useTaskOwner = config.UseTaskOwner

        scheduler.qosEnabled = config.QosEnabled
        scheduler.profilerEnabled = config.ProfilerEnabled
        scheduler.setStageWorkdirToContainerWorkdir = config.SetStageWorkdirToContainerWorkdir
        scheduler.cleanupPatternEnable = config.CleanupPatternEnable
        scheduler.stageRetryDelay = config.StageRetryDelay
        if config.CPUCapRatio > VALID_CAP_RATIO_LIMIT {
            scheduler.cpuCapRatio = config.CPUCapRatio
        }
        if config.MemCapRatio > VALID_CAP_RATIO_LIMIT {
            scheduler.memCapRatio = config.MemCapRatio
        }

        if config.MaxCPUPerTask > VALID_RESOURCE_LIMIT {
            scheduler.maxCPUPerTask = config.MaxCPUPerTask
        }
        if config.MaxMemoryPerTask > VALID_RESOURCE_LIMIT {
            scheduler.maxMemoryPerTask = config.MaxMemoryPerTask
        }

        scheduler.jobTracker.UpdateConfig(config)

        for priority, quota := range config.StageQuota {
            scheduler.batcher.SetPriorityQuota(priority, quota)
        }
    }
}

func (scheduler *bioScheduler) AutoReScheduleHangTasks() bool {
    return scheduler.autoReScheduleHangTasks
}

func (scheduler *bioScheduler) AutoReclaimTask() bool {
    return scheduler.autoReclaimTask
}

func (scheduler *bioScheduler) AutoScheduleHDFS() bool {
    return scheduler.autoScheduleHDFS
}

func (scheduler *bioScheduler) BackendScheduler() backendScheduler {
	return scheduler.beScheduler
}

func (scheduler *bioScheduler) Disabled() bool {
    return scheduler.disabled
}

func (scheduler *bioScheduler) Disable() error {
    scheduler.scheduleLock.Lock()
    defer scheduler.scheduleLock.Unlock()

    scheduler.disabled = true
    SchedulerLogger.Infof("The scheduler is disabled\n")
    return nil
}

func (scheduler *bioScheduler) Enable() error {
    scheduler.scheduleLock.Lock()
    defer scheduler.scheduleLock.Unlock()

    scheduler.disabled = false
    scheduler.scheduleCond.Signal()
    SchedulerLogger.Infof("The scheduler is re-enabled now\n")
    return nil
}

func (scheduler *bioScheduler) Quiesced() bool {
    return scheduler.quiesced
}

func (scheduler *bioScheduler) _Quiesce() {
    scheduler.quiesced = true
    SchedulerLogger.Infof("The scheduler is quiesced\n")
}

func (scheduler *bioScheduler) Quiesce() {
    scheduler.scheduleLock.Lock()
    defer scheduler.scheduleLock.Unlock()
    scheduler._Quiesce()
}

func (scheduler *bioScheduler) _Resume() {
    scheduler.quiesced = false
    scheduler.scheduleCond.Signal()
    SchedulerLogger.Infof("The scheduler is resumed now\n")
}

func (scheduler *bioScheduler) Resume() {
    scheduler.scheduleLock.Lock()
    defer scheduler.scheduleLock.Unlock()
    scheduler._Resume()
}

func (scheduler *bioScheduler) _callJobEventHandlerWithProfiling(job Job, stageId string,
    event uint32, info *EventNotifyInfo) (error, *BuildStageErrorInfo, ScheduleAction) {
    var preTime time.Time
    scheduler.perfStats.StartProfileJobStateMachine(&preTime)
    defer scheduler.perfStats.EndProfileJobStateMachine(preTime)

    enableRightControl := scheduler.enableRightControl
    job.SetEnableRightControl(enableRightControl)

    return job.HandleEvent(stageId, event, info)
}

func (scheduler *bioScheduler) SubmitJob(job Job) error {
    err, buildStageErrInfo, _ := scheduler._callJobEventHandlerWithProfiling(job, "", JOB_START, nil)
    if err != nil {
        SchedulerLogger.Errorf("Job %s failed to start: %s\n",
            job.GetID(), err.Error())
        if buildStageErrInfo != nil {
            bioJobFailInfo := NewBioJobFailInfo(buildStageErrInfo.ItemName, "",
                BIOJOB_FAILREASON_BUILDGRAPH, buildStageErrInfo.Err, buildStageErrInfo)
            failReason := bioJobFailInfo.BioJobFailInfoFormat()
            job.SetFailReason(failReason)
        } else {
            job.SetFailReason(err.Error())
        }
        scheduler.IssueJobTerminateEvent(job, JOB_EVENT_FAIL)
        GetDashBoard().UpdateStats(STAT_JOB_BUILD_FAIL, 1)
        return err
    } else {
        /*Init the job's schedule state in DB*/
        err = scheduler.SyncJobSchedInfoToDB(job, true, true, 0)
        if err != nil {
            SchedulerLogger.Errorf("Fail to init schedule info in DB for job %s: %s\n",
                job.GetID(), err.Error())
            return err
        }
    }

    scheduler.KickScheduleJob(job, false)

    return nil
}

func (scheduler *bioScheduler) TryKickScheduleJob(job Job, markOnly bool) {
    scheduler.batcher.TryScheduling(job, markOnly)
}

/*request scheduler to schedule a job*/
func (scheduler *bioScheduler) KickScheduleJob(job Job,
    markOnly bool) {
    err := scheduler.jobTracker.MarkJobNeedSchedule(job,
        false)
    if err != nil {
        SchedulerLogger.Errorf("Fail to mark job %s to schedule:%s\n",
            job.GetID(), err.Error())
    }

    if !markOnly {
        /*kick scheduler worker to start work*/
        scheduler.KickScheduler()
    }
}

type JobEventTask struct {
    job Job
    taskId string
    backendId string
    eventInfo *EventNotifyInfo
    event uint32

    /* In some complicated cases, the stage
     * bioflow fail before submitting task. So
     * there will be stage with empty backend
     * and task id. The stage id will be passed
     * this cases.
     */
    stageId string
}

/*
 * request check thread to check job state, it is always called by
 * caller when received an event indicating a stage maybe complete or fail
 */
func (scheduler *bioScheduler) HandleJobEventAsync(syncKey string, job Job, backendId string,
    taskId string, event uint32, info *EventNotifyInfo, async bool) {
    eventTask := &JobEventTask{
                    job: job,
                    taskId: taskId,
                    backendId: backendId,
                    eventInfo: info,
                    event: event,
    }

    atomic.AddInt64(&scheduler.pendingAsyncEventCount, 1)
    if async {
        /*Handle event async in worker threads*/
        scheduler.jobScheduleEventHandleWorkers.IssueOperation(syncKey, eventTask,
            func (data interface{}) error {
                scheduler.HandleAsyncJobEvent(eventTask)
                atomic.AddInt64(&scheduler.pendingAsyncEventCount, -1)
                return nil
            }, false, 0, "")
    } else {
        /*Handle event sync in worker threads*/
        scheduler.jobScheduleEventHandleWorkers.IssueOperation(syncKey, eventTask,
            func (data interface{}) error {
                scheduler.HandleAsyncJobEvent(eventTask)
                atomic.AddInt64(&scheduler.pendingAsyncEventCount, -1)
                return nil
            }, true, 600, "")
    }
}

/*
 * Kick the scheduler worker to starts work
 */
func (scheduler *bioScheduler) KickScheduler() {
    /*
     * Use a count instead of channel here is that
     * the buffered channel requires a size. This
     * make block caller un-necessarily
     */
    scheduler.scheduleLock.Lock()
    scheduler.scheduleKickCount ++
    scheduler.scheduleCond.Signal()
    scheduler.scheduleLock.Unlock()
    scheduler.batcher.Kick()
}

/*
 * It is called by backend scheduler proactively when it detects that a backend
 * is stuck in failure state and can't be recovered. This help fail back the tasks
 * sent to the backend and avoid deadlock in job level.
 */
func (scheduler *bioScheduler) FailbackTasks(backendID string, taskIds []string) {
    for _, taskID := range taskIds {
        info := NewEventNotifyInfo("backend can't be recovered", "", "")
        /*Mark the task lost*/
        scheduler.HandleJobEvent(backendID, taskID, STAGE_LOST, info)
        SchedulerLogger.Errorf("The task %s/%s be fail back\n",
            taskID, backendID)
    }
}

/*
 * Check job tasks's state with backend. It will be called
 * by job status check process to update state and handle the
 * callback request lost.
 */
func (scheduler *bioScheduler) CheckJobStageStatus(stageList []*StageExecInfo) {
    beScheduler := scheduler.BackendScheduler()
    for _, stage := range stageList {
        taskID := stage.TaskID()
        backendID := stage.BackendID()
        SchedulerLogger.Infof("Job checker check task %s state \n",
            taskID)
        err, taskStatus := beScheduler.CheckTask(backendID, taskID)
        if err == nil {
            taskState := taskStatus.State
            info := NewEventNotifyInfo(taskStatus.Info, taskStatus.HostName,
                        taskStatus.AgentIP)
            switch taskState {
                case backend.TASK_FINISHED:
                    scheduler.HandleJobEvent(backendID, taskID, STAGE_DONE, info)
                    SchedulerLogger.Infof("Job checker check task %s finished \n", taskID)
                case backend.TASK_FAIL:
                    scheduler.HandleJobEvent(backendID, taskID, STAGE_FAIL, info)
                    SchedulerLogger.Infof("Job checker check task %s failed \n", taskID)
                case backend.TASK_KILLED:
                    scheduler.HandleJobEvent(backendID, taskID, STAGE_FAIL, info)
                    SchedulerLogger.Infof("Job checker check task %s killed \n", taskID)
                case backend.TASK_GONE:
                    scheduler.HandleJobEvent(backendID, taskID, STAGE_FAIL, info)
                    SchedulerLogger.Infof("Job checker check task %s gone \n", taskID)
                case backend.TASK_DROPPED:
                    scheduler.HandleJobEvent(backendID, taskID, STAGE_FAIL, info)
                    SchedulerLogger.Infof("Job checker check task %s dropped \n", taskID)
                case backend.TASK_ERROR:
                    scheduler.HandleJobEvent(backendID, taskID, STAGE_FAIL, info)
                    SchedulerLogger.Infof("Job checker check task %s error \n", taskID)
                case backend.TASK_LOST:
                    scheduler.HandleJobEvent(backendID, taskID, STAGE_LOST, info)
                    SchedulerLogger.Infof("Job checker check task %s lost \n", taskID)
                case backend.TASK_RUNNING:
                    scheduler.HandleJobEvent(backendID, taskID, STAGE_RUNNING, info)
                    SchedulerLogger.Infof("Job checker check task %s running \n", taskID)
                case backend.TASK_QUEUED:
                    scheduler.HandleJobEvent(backendID, taskID, STAGE_QUEUED, info)
                    SchedulerLogger.Infof("Job checker check task %s queued \n", taskID)
                case backend.TASK_UNKNOWN:
                    scheduler.HandleJobEvent(backendID, taskID, STAGE_LOST, info)
                    SchedulerLogger.Infof("Job checker check task %s unknown \n", taskID)
                default:
                    SchedulerLogger.Infof("task %s of backend:%s state unknown %d \n",
                        taskID, backendID, taskState)
            }

        } else {
            SchedulerLogger.Errorf("Job checker check task %s operation fail \n", taskID)
        }
    }
}

/*
 * When a job's stage is queued too long or running too long,
 * it may be regarded hang. The scheduler can automatically kill
 * and re-schedule these tasks if enabled by user.
 */
func (scheduler *bioScheduler) ReScheduleHangJob(job Job) error {
    /*Get job's hang tasks*/
    schedInfo := job.GetScheduleInfo()
    if schedInfo == nil {
        SchedulerLogger.Errorf("Can't re-schedule hang job %s with no schedule info\n",
            job.GetID())
        return errors.New("Job with no schedule info")
    }

    hangStages := schedInfo.GetHangStageInfos()
    if hangStages == nil || len(hangStages) == 0 {
        SchedulerLogger.Infof("Job %s has no hang stages, don't re-schedule\n",
            job.GetID())
        return nil
    }

    beScheduler := scheduler.BackendScheduler()
    for i := 0; i < len(hangStages); i ++ {
        stageInfo := hangStages[i]
        stageId := stageInfo.StageID()
        taskId := stageInfo.TaskID()
        backendId := stageInfo.BackendID()

        SchedulerLogger.Warnf("Job %s stage %s task %s/%s hang, kill and reschedule it\n",
            job.GetID(), stageId, backendId, taskId)

        /*
         * The safe way to re-schedule a hang stage is as follows:
         * 1) simulate a stage lost event, this will set stage to correct state
         * 2) kill backend task
         * 3) delete backend task
         *
         * The step 1) is performed first because it should reset the schedule information
         * and don't let backend task killed events to confuse the scheduler. These events
         * will be ignored if the stage/task mapping is cleared.
         */
        SchedulerLogger.Warnf("Simulate a stage lost event for job %s stage %s\n",
            job.GetID(), stageId)
        scheduler.HandleJobEvent(backendId, taskId, STAGE_LOST,
            NewEventNotifyInfo("Simulate lost event", "", ""))

        SchedulerLogger.Debugf("Will kill task %s of %s ReScheduleHangJob\n", taskId, backendId)
        err := beScheduler.KillTask(backendId, taskId)
        if err != nil {
            errmsg := fmt.Sprintf("Kill task %s/%s failed\n",
                backendId, taskId)
            SchedulerLogger.Error(errmsg)
        }

        SchedulerLogger.Warnf("Job %s stage %s task %s/%s cleaned in backend\n",
            job.GetID(), stageId, backendId, taskId)

        GetDashBoard().UpdateStats(STAT_RESCHEDULE_HANG_TASK, 1)
    }

    return nil
}

/*
 * It will cleanup job's temp files or data.
 */
func (scheduler *bioScheduler) JobFilesCleanup(job Job) error {
    jobSchedInfo := job.GetScheduleInfo()
    if jobSchedInfo == nil {
        SchedulerLogger.Errorf("No schedule information for job %s, strange\n",
            job.GetID())
        return errors.New("No job schedule info for " + job.GetID())
    }

    /*cleanup the files specified by stage pattern. we need merge
      the cleanup patterns for same work to avoid too many readdir
      operations*/
    pendingStages := jobSchedInfo.GetPendingStages()
    stages := jobSchedInfo.GetDoneStages()
    if stages != nil && pendingStages != nil {
        stages = append(stages, pendingStages ...)
    }
    if stages != nil {
        for _, stage := range stages {
            if err := stage.Cleanup(); err != nil {
                SchedulerLogger.Errorf("Fail to cleanup stage %s: %s\n",
                    stage.GetID(), err.Error())
            }
        }
    }

    return nil
}

func (scheduler *bioScheduler) CleanupAndFixJobDirPrivilege(dir string,
    accountInfo *UserAccountInfo, ignoreDirsMap map[string]bool) error {
    SchedulerLogger.Debugf("Will cleanup and fix directory %s\n",
        dir)
    /*
     * privilege fix will be done by scheduler, so translates
     * the path relative to scheduler volumes mount
     */
    err, workDir := GetStorageMgr().MapPathToSchedulerVolumeMount(dir)
    if err != nil {
        SchedulerLogger.Errorf("Can't map directory to scheduler to fix privilege:%s\n",
            err.Error())
        return err
    }
    err = FSUtilsChangeDirUserPrivilege(workDir, accountInfo, true, ignoreDirsMap)
    if err != nil {
        SchedulerLogger.Errorf("Fail to change privilege for %s: %s\n",
            workDir, err.Error())
        return err
    } else {
        SchedulerLogger.Debugf("Successfully change directory %s privilege\n",
            workDir)
    }

    return err
}

func (scheduler *bioScheduler) SetUserDirectoryPrivilege(job Job, ignoreDirs string) error {
    err, workDir := GetStorageMgr().MapPathToSchedulerVolumeMount(job.WorkDir())
    if err != nil {
        SchedulerLogger.Errorf("Can't map directory to scheduler to fix privilege:%s\n",
            err.Error())
        return err
    }
    ignoreDirsMap := FSUtilsBuildIgnoreDirsTOMap(workDir, ignoreDirs)
    secCtxt := job.GetSecurityContext()
    accountInfo := secCtxt.GetUserInfo()
    jobSchedInfo := job.GetScheduleInfo()
    if jobSchedInfo == nil {
        err := scheduler.CleanupAndFixJobDirPrivilege(job.WorkDir(),
            accountInfo, ignoreDirsMap)
        if err != nil {
            SchedulerLogger.Errorf("Cleanup job %s work directory fail: %s\n",
                job.GetID(), err.Error())
        }

        SchedulerLogger.Errorf("No schedule information for job %s on cleanup\n",
            job.GetID())
        return errors.New("No job schedule info for " + job.GetID())
    }

    dirChecked := make(map[string]bool)
    /* Change all the files in stage's work directory to
     * correct mode.
     * In most cases, all stages' work directory is under the job's work dir.
     * traverse the stages' work directory here to handle the special cases that
     * stage's work directory is outside the job's work directory
     */
    pendingStages := jobSchedInfo.GetPendingStages()
    stages := jobSchedInfo.GetDoneStages()
    if pendingStages != nil {
        stages = append(stages, pendingStages ...)
    }

    /*
     * Notes!!!!!
     * A trick here: for wom graph scheduler, the done stages list must be empty.
     * But the process here still works because that all the files generated by
     * it are under the job's workdir.
     */

    /*in most cases, all files are under job's work directory*/
    prevWorkDir := job.WorkDir()
    for i := 0; i < len(stages); i ++ {
        stage := stages[i]
        outputDir := stage.WorkDir()
        if stage.OutputDir() != "" {
            outputDir = stage.OutputDir()
        }
        if prevWorkDir != "" && outputDir != prevWorkDir {
            if checked, ok := dirChecked[prevWorkDir]; !ok || !checked {
                dirChecked[prevWorkDir] = true
                err := scheduler.CleanupAndFixJobDirPrivilege(prevWorkDir,
                    accountInfo, ignoreDirsMap)
                if err != nil {
                    SchedulerLogger.Errorf("Cleanup job %s directory %s fail: %s\n",
                        job.GetID(), prevWorkDir, err.Error())
                }
            }
        }
        prevWorkDir = outputDir
    }

    if prevWorkDir != "" {
        err := scheduler.CleanupAndFixJobDirPrivilege(prevWorkDir,
            accountInfo, ignoreDirsMap)
        if err != nil {
            SchedulerLogger.Errorf("Cleanup job %s directory %s fail: %s\n",
                job.GetID(), prevWorkDir, err.Error())
        }
    }
    
    return nil
}

/*persist job state and history to database*/
func (scheduler *bioScheduler) PersistJobHistory(job Job,
    isRecover bool) error {
    db := GetDBService()
    jobSchedInfo := job.GetScheduleInfo()
    var schedDBInfo *JobScheduleDBInfo = nil
    var err error = nil

    if isRecover {
        /*
         * Partial execution history is in DB, read it
         */
        err, schedDBInfo = db.GetJobScheduleInfo(job.GetID())
        if err != nil {
            SchedulerLogger.Errorf("Can't get job history from database\n")
            schedDBInfo = nil
        }

        if schedDBInfo == nil {
            SchedulerLogger.Infof("No schedule info found for job %s\n",
                job.GetID())
            jobSchedInfo := NewJobScheduleInfo(job)
            schedDBInfo = jobSchedInfo.CreateJobScheduleDBInfo()
        }
    } else {
        if jobSchedInfo != nil {
            schedDBInfo = jobSchedInfo.CreateJobScheduleDBInfo()
        } else {
            SchedulerLogger.Infof("No schedule info found for job %s\n",
                job.GetID())
            /*
             * Need create empty schedule information here for format
             * validity
             */
            jobSchedInfo = NewJobScheduleInfo(job)
            schedDBInfo = jobSchedInfo.CreateJobScheduleDBInfo()
        }
    }

    jobInfo := job.CreateJobInfo()

    /*save job history to database*/
    if schedDBInfo != nil && jobInfo != nil {
        err := db.AddJobHistory(jobInfo, schedDBInfo)
        if err != nil {
            SchedulerLogger.Errorf("Add Job %s history to DB fail: %s \n",
                job.GetID(), err.Error())
        } else {
            SchedulerLogger.Infof("Job %s history is added to DB\n",
                job.GetID())
        }
    } else {
        SchedulerLogger.Errorf("Serialize failure, can't add job %s history to DB\n",
            job.GetID())
    }

    return nil
}


/*Cleanup Job schedule info from memory and database*/
func (scheduler *bioScheduler) DestroyJobScheduleInfo(job Job) error {
    db := GetDBService()

    /*Delete job schedule info via sync worker to ensure consistency*/
    scheduler.jobScheduleStateSyncer.IssueOperation(job.GetID(), job, 
        func(data interface{}) error {
            job := data.(Job)
            err := db.DeleteJobScheduleInfo(job.GetID())
            if err != nil {
                SchedulerLogger.Errorf("Fail to delete job schedule info for %s\n",
                    job.GetID())
            } else {
                SchedulerLogger.Infof("Succeed to delete job schedule info for %s\n",
                    job.GetID())
            }

            return err
        }, true, 30, "")

    scheduler.RecycleJobScheduleState(job)

    return nil
}

/*
 * It is called when job completed or failed. It will update job
 * state, persist database and cleanup all the temp files or data
 * created by the job.
 */
func (scheduler *bioScheduler) JobComplete(job Job, event int) {
    var err error
    var isRecover bool
    oldJobState := job.State()
    /*update job state*/
    switch event {
    case JOB_EVENT_COMPLETE:
        job.Complete(true, "")
        GetDashBoard().UpdateStats(STAT_JOB_COMPLETE, 1)
    case JOB_EVENT_FAIL:
        job.Complete(false, "")
        GetDashBoard().UpdateStats(STAT_JOB_FAIL, 1)
    case JOB_EVENT_CANCELED:
        job.Complete(false, "Canceled by user")
        GetDashBoard().UpdateStats(STAT_JOB_CANCEL, 1)
    case JOB_EVENT_PSEUDONE, JOB_EVENT_RECOVER_PSEUDONE:
        job.SetState(JOB_STATE_PSUDONE)
        job.Complete(true, "Some stage failed to forbid other stage continue to run")
        GetDashBoard().UpdateStats(STAT_JOB_PSEUDONE, 1)
    default:
        job.Complete(false, "Unknown event")
        GetDashBoard().UpdateStats(STAT_JOB_FAIL, 1)
    }

    if event == JOB_EVENT_RECOVER_PSEUDONE {
        isRecover = true
        event = JOB_EVENT_PSEUDONE
    }

    err = scheduler.PersistJobHistory(job, isRecover)
    if err != nil {
        SchedulerLogger.Errorf("Fail persist job %s history\n",
            job.GetID())
    }

    err = scheduler.DestroyJobScheduleInfo(job)
    if err != nil {
        SchedulerLogger.Errorf("Fail to destroy job %s schedule info\n",
            job.GetID())
    }

    /*
     * The job may hold some system resources. For example, the wom graph
     * scheduler of a job hold a databse lock during its execution. And this
     * should be released here each time the job is completed. Otherwise, when
     * the job is recovered, it may not be able to re-acquire the lock.
     * all the resource is expected to be released in the cleanup of job.
     */
    err = job.ReleaseResource()
    if err != nil {
        SchedulerLogger.Errorf("Fail to cleanup job %s: %s\n",
            job.GetID(), err.Error())
    }

    scheduler.delayJobChecker.Remove(job.GetID())
    GetUserMgr().ReleaseJobQuota(job.GetSecurityContext(), 1)

    scheduler.batcher.UpdateMetric(job)

    /*
     * No one is allowed to access job schedule info anymore
     */

    /*
     * Notify job manager and user manager for statistics
     *
     * for a canceled job, the old state should be obtained from its
     * last state.
     */
    if event == JOB_EVENT_CANCELED {
        oldJobState = job.LastState()
    }
    jobEvent := NewJobEvent(JOBEVENTID, job, oldJobState, event)
    eventbus.Publish(jobEvent)

    job.FinishScheduleOperation(JOB_OP_TERMINATE)
}

type JobTerminateTask struct {
    job Job
    event int
}

/*
 * Issue a job terminate event to worker threads
 */

func (scheduler *bioScheduler) _sendTerminateTaskToWorkers(task *JobTerminateTask) {
    scheduler.jobTerminateEventHandleWorkers.IssueOperation(task.job.GetID(), task,
        func (data interface{}) error {
            scheduler.HandleJobTerminateTask(task)
            return nil
        }, false, 0, "")
}

func (scheduler *bioScheduler) IssueJobTerminateEvent(job Job, event int) {
    task := &JobTerminateTask{
            job: job,
            event: event,
    }

    if job.RequestScheduleOperation(JOB_OP_TERMINATE, task) {

        scheduler._sendTerminateTaskToWorkers(task)
    }
}

/*
 * Fetch the stdout or stderr files from sandbox on remote servers. It may need
 * save a copy of cmd_stdout and cmd_stderr to the stage's workdir if it is a 
 * wom stage
 */
func (scheduler *bioScheduler) FetchJobSandboxFiles(stageInfo *StageExecInfo,
    file string) {
    stage := stageInfo.Stage()
    taskId := stageInfo.TaskID()
    backendId := stageInfo.BackendID()

    jfs := NewJobFileSet(stage.LogDir())
    
    workDir := ""
    saveToWorkdir := false
    if stage != nil {
        if stage.NeedStoreAppOutput() {
            saveToWorkdir = true
            var err error
            err, workDir = GetStorageMgr().MapPathToSchedulerVolumeMount(stage.WorkDir())
            if err != nil {
                SchedulerLogger.Errorf("Fail to map stage %s workdir %s: %s\n",
                    stage.GetID(), stage.WorkDir(), err.Error())
            }
        }
    }
    var sandboxFiles []string
    if file == "" {
        sandboxFiles = []string{backend.SANDBOX_STDERR, backend.SANDBOX_STDOUT,
            backend.SANDBOX_PROFILING, backend.SANDBOX_CMD_STDOUT, backend.SANDBOX_CMD_STDERR}
    } else {
        sandboxFiles = []string{file}
    }

    beScheduler := scheduler.BackendScheduler()
    for _, file := range sandboxFiles {
        SchedulerLogger.Debugf("Fetch sandbox file %s for task %s/%s\n",
            file, backendId, taskId)
        localSandboxFile, err := jfs.GenerateTaskSandboxFilePath(stage.Name(), taskId,
            file)
        if err != nil {
            SchedulerLogger.Errorf("Fail to generate local path for sandbox file %s task %s: %s\n",
                file, taskId, err.Error())
        } else {
            err := beScheduler.FetchSandboxFile(backendId, taskId, file, localSandboxFile)
            if err != nil {
                SchedulerLogger.Errorf("Fail to fetch sandbox file %s for task %s: %s\n",
                    file, taskId, err.Error())
            } else {
                if saveToWorkdir {
                    switch file {
                        case backend.SANDBOX_CMD_STDOUT:
                            err := FSUtilsCopyFile(localSandboxFile, workDir + "/stdout")
                            if err != nil {
                                SchedulerLogger.Errorf("Fail to save stdout to stage workdir: %s\n",
                                    err.Error())
                            }
                        case backend.SANDBOX_CMD_STDERR:
                            err := FSUtilsCopyFile(localSandboxFile, workDir + "/stderr")
                            if err != nil {
                                SchedulerLogger.Errorf("Fail to save stderr to stage workdir: %s\n",
                                    err.Error())
                            }
                    }
                }
            }
        }
    }
}

func (scheduler *bioScheduler) PostPsudoJobEvent(backendId string,
    taskId string, event uint32, message string, job Job, stageId string) {
    eventInfo := NewEventNotifyInfo(message, "", "")
    if backendId != "" && taskId != "" {
        scheduler.HandleJobEvent(backendId, taskId, event, eventInfo)
    } else {
        /*Issue a task by stageId*/
        SchedulerLogger.Infof("The job %s stage %s has empty backend/task, simulate a event by stage id\n",
            job.GetID(), stageId)

        checkTask := &JobEventTask{
                    job: job,
                    taskId: "",
                    backendId: "",
                    eventInfo: eventInfo,
                    event: event,
                    stageId: stageId,
        }

        atomic.AddInt64(&scheduler.pendingAsyncEventCount, 1)
        syncKey := job.GetID() + stageId
        scheduler.jobScheduleEventHandleWorkers.IssueOperation(syncKey, checkTask,
            func (data interface{}) error {
                scheduler.HandleAsyncJobEvent(checkTask)
                atomic.AddInt64(&scheduler.pendingAsyncEventCount, -1)
                return nil
            }, false, 0, "")
    }
}


/*
 * Job cleanup worker thread calls it to cleanup the jobs
 */
func (scheduler *bioScheduler) HandleJobTerminateTask(terminateTask *JobTerminateTask) {
    job := terminateTask.job
    event := terminateTask.event

    if job.JobTerminated() {
        SchedulerLogger.Debugf("Cancel the job termnate task because it is already terminated, job state: %d",
            job.State())
        return
    }

    SchedulerLogger.Debugf("Job Terminate Event Worker start handle job %s event %s \n",
        job.GetID(), JobMgmtEventToStr(event))
    scheduler.JobComplete(job, event)
    SchedulerLogger.Debugf("Job Terminate Event  Worker complete job %s event %s \n", 
        job.GetID(), JobMgmtEventToStr(event))
}

/*
 * try to requeue a job stage. Handle the following cases:
 * 1) stage is pending or running, update stage resource, kill the old
 *    task and start a new task
 * 2) stage is not pending yet, only update the stage resource
 *
 */
func (scheduler *bioScheduler) RequeueJobStage(job Job, stageId string,
    resource *ResourceSpecJSONData) error {

    /*update the resource spec any way*/
    if resource != nil {
        err := job.UpdateStageResourceSpec(stageId, resource)
        if err != nil {
            SchedulerLogger.Errorf("Update stage resource spec fail: %s\n",
                err.Error())
            return err
        }
        SchedulerLogger.Infof("Job %s stage %s resource spec updated by %v\n",
            job.GetID(), stageId, *resource)
    }


    /*
     * kill previous task, requeue the stage to backend
     */
    jobSchedInfo := job.GetScheduleInfo()
    if jobSchedInfo == nil {
        SchedulerLogger.Infof("No schedule information for job %s, strange\n",
            job.GetID())
        return errors.New("Job has no schedule info, only update resource")
    }

    var errmsg string
    beScheduler := scheduler.BackendScheduler()
    stageInfo := jobSchedInfo.GetPendingStageInfoById(stageId)
    if stageInfo == nil {
        SchedulerLogger.Errorf("The stage %s of job %s not pending or exist\n",
            stageId, job.GetID())
        return errors.New("Stage " + stageId + " not pending, only update resource")
    }

    taskId := stageInfo.TaskID()
    backendId := stageInfo.BackendID()

    if backendId == "" || taskId == "" {
        errmsg = fmt.Sprintf("Stage %s pending on invalid task %s/%s, only update resource\n",
            stageId, backendId, taskId)
        SchedulerLogger.Error(errmsg)
        return errors.New(errmsg)
    }

    err, taskStatus := beScheduler.CheckTask(backendId,
        taskId)
    if err != nil {
        errmsg = fmt.Sprintf("Failed to get status for task:%s\n",
            taskId)
        SchedulerLogger.Info(errmsg)
    }

    SchedulerLogger.Infof("Requeue job %s stage %s task %s/%s state %s\n",
        job.GetID(), stageId, backendId, taskId,
        backend.BackendTaskStateToStr(taskStatus.State))

    SchedulerLogger.Debugf("Will kill task %s of %s in func RequeueJobStage\n", taskId, backendId)
    err = beScheduler.KillTask(backendId, taskId)
    if err != nil {
        errmsg = fmt.Sprintf("Requeue op kill task %s failed %s, ignore\n",
            taskId, err.Error())
        SchedulerLogger.Error(errmsg)
    }


    SchedulerLogger.Infof("Proactively simulate job %s stage %s task %s/%s failure\n",
        job.GetID(), stageId, backendId, taskId)

    eventInfo := NewEventNotifyInfo("User requeue the stage proactively",
                    "", "")
    scheduler.HandleJobEvent(backendId, taskId, STAGE_LOST,
        eventInfo)

    SchedulerLogger.Infof("job %s stage %s requeue done\n",
        job.GetID(), stageId)

    return nil
}

type ResourceAcctTask struct {
    job Job
    stage Stage
}

/*
 * Kick an async worker to do resource usage accounting for
 * a stage
 */
func (scheduler *bioScheduler) KickResourceAccounting(job Job, stage Stage) error {
    acctTask := &ResourceAcctTask {
                job: job,
                stage: stage,
    }

    /*
     * The job resource accounting is done per user, so need sync all the accounting
     * operations of a user
     */
    ctxt := job.GetSecurityContext()
    if ctxt == nil {
        SchedulerLogger.Errorf("No user for job %s, can't do resource accounting\n",
            job.GetID())
        return errors.New("No user for job")
    }
	user := ctxt.ID()
    return scheduler.resourceAcctWorkers.IssueOperation(user, acctTask,
        func (data interface{}) error {
            scheduler.HandleStageResourceAccounting(acctTask)
            return nil
        }, false, 0, "")
}

/*
 * Do resource usage accounting for a stage asynchronously
 */
func (scheduler *bioScheduler) HandleStageResourceAccounting(acctTask *ResourceAcctTask) error {
    job := acctTask.job
    stage := acctTask.stage
    cpu := stage.GetCPU()
    mem := stage.GetMemory()
	jobId := job.GetID()
	stageId := stage.GetID()
	schTime := stage.ScheduledTime()
    duration := stage.TotalDuration()
    if duration < 0 {
        SchedulerLogger.Errorf("Fail to get correct runing time for stage %s\n",
            stage.GetID())
        return errors.New("invalid stage state")
    }
    ctxt := job.GetSecurityContext()
    if ctxt == nil {
        SchedulerLogger.Errorf("No user for job %s, can't do resource accounting\n",
            job.GetID())
        return errors.New("No user for job")
    }

	user := ctxt.ID()
    return  GetUserMgr().AuditResourceUsage(jobId, stageId, schTime, user,
		duration, cpu, mem)
}

/*
 * Do stage resource profiling. This will trigger profiler to analyze the trace
 * file of executing stages.
 */
func (scheduler *bioScheduler) HandleStageProfiling(stage Stage, taskId string) {
    usage, err := GetTaskProfilingSummary(stage.LogDir(), stage.Name(), taskId)
    if err != nil {
        SchedulerLogger.Errorf("Fail to do profiling for stage %s task %s: %s\n",
            stage.GetID(), taskId, err.Error())
    } else {
        stage.SetResourceStats(usage)
    }
}

/*
 * When a stage terminated (done, fail, lost or gone), there is a lot of work
 * to do. Some operations require access storage, which may be blocked for a
 * long time. So these operations are handled asynchronous to avoid block the
 * main service threads.
 */
func (scheduler *bioScheduler) HandleAsyncJobEvent(eventTask *JobEventTask) error {
    job := eventTask.job
    taskId := eventTask.taskId
    backendId := eventTask.backendId
    event := eventTask.event
    info := eventTask.eventInfo
    stageId := eventTask.stageId
    beScheduler := scheduler.BackendScheduler()

    if stageId == "" && (backendId == "" || taskId == "") {
        SchedulerLogger.Errorf("HandleAsyncJobEvent(job %s): event task with empty stage and task info\n",
            job.GetID())
        return errors.New("Invalid event")
    }
    
    var stageInfo *StageExecInfo = nil

    if backendId != "" && taskId != "" {
        schedInfo := scheduler.GetJobSchedInfoByTaskId(backendId, taskId)
        if schedInfo == nil {
            SchedulerLogger.Errorf("HandleAsyncJobEvent(job %s): fail to find sched info for %s/%s\n",
                job.GetID(), backendId, taskId)
            return errors.New("no sched info for backend/task")
        }

        stageId, stageInfo = schedInfo.GetStageOfTask(backendId, taskId)
    }

    stage := job.GetStage(stageId)
    if stage == nil {
        SchedulerLogger.Errorf("HandleAsyncJobEvent(job %s): ignore non-exist stage %s event\n",
            job.GetID(), stageId)
        return errors.New("Stage not exist")
    }

    /*
     * The eremetic and polling threads may received the task state change at the
     * the same time. So sync here to avoid two threads to handle the same terminal
     * events at the same time.
     */
    if stage != nil && !stage.StartHandleEvent(event) {
        SchedulerLogger.Infof("HandleAsyncJobEvent(job %s): stage %s are handling terminal events, so ignore the %s\n",
            job.GetID(), stageId, StageStateToStr(event))
        return nil
    }

    if stageId == "" {
        stageId = stage.GetID()
    }

    SchedulerLogger.Infof("HandleAsyncJobEvent(job %s): handle stage %s event %s\n",
        job.GetID(), stageId, StageStateToStr(event))

    switch event {
        case STAGE_DONE:
            var donePostTime time.Time
            scheduler.perfStats.StartProfileStagePostExec(&donePostTime)
            err := stage.PostExecution(true)
            scheduler.perfStats.EndProfileStagePostExec(donePostTime)
            if err != nil {
                SchedulerLogger.Errorf("Fail to do post execution for job %s stage %s: %s\n",
                    job.GetID(), stageId, err.Error())

                bioJobFailInfo := NewBioJobFailInfo(stage.Name(), stageId,
                    BIOJOB_FAILREASON_POSTEXECUTION, err, nil)
                failReason := bioJobFailInfo.BioJobFailInfoFormat()
                job.SetFailReason(failReason)

                scheduler.IssueJobTerminateEvent(job, JOB_EVENT_FAIL)
                /*
                 * Task failed and resource or quota released, need kick
                 * scheduler to work
                 */
                stage.FinishHandleEvent(event)
                scheduler.KickScheduler()

                return err
            } else {
                SchedulerLogger.Infof("Succeed to do post execution for job %s stage %s\n",
                    job.GetID(), stageId)
            }


            /*
             * The log files for done stage should be fetched before issue
             * STAGE_DONE event to job. This is because that the stages of 
             * WDL jobs need read the "stdout" and "stderr" files when handling
             * the STAGE_DONE EVENT
             */
            scheduler.FetchJobSandboxFiles(stageInfo, "")

            /*
            * Job handles its stage's event and evaluate its failure
            * state.
            */
            err, buildStageErrInfo, action := scheduler._callJobEventHandlerWithProfiling(job,
                stageId, event, info)
            if err != nil {
                SchedulerLogger.Errorf("Build Job %s stages failure: %s\n",
                    job.GetID(), err.Error())
                if buildStageErrInfo != nil {
                    bioJobFailInfo := NewBioJobFailInfo(buildStageErrInfo.ItemName, "",
                        BIOJOB_FAILREASON_BUILDGRAPH, buildStageErrInfo.Err, buildStageErrInfo)
                    failReason := bioJobFailInfo.BioJobFailInfoFormat()
                    job.SetFailReason(failReason)
                } else {
                    job.SetFailReason(err.Error())
                }
            }
            info := NewFrontEndNotcieInfo(job.GetID(), job.Name(), stage.ToBioflowStageInfo())
            e := NewNoticeEvent(FRONTENDNOTICEEVENT, info)
            eventbus.Publish(e)
            GetDashBoard().UpdateStats(STAT_TASK_COMPLETE, 1)

            scheduler.HandleStageProfiling(stage, taskId)

            scheduler.UnTrackJobTask(job, stageId, backendId, taskId,
                true)
            scheduler.jobTracker.EvaluateJobMetric(job, false)

            scheduler.SyncJobSchedInfoToDB(job, false, false, 0)
            
            /*
             * If a job is completed, handle it to cleanup worker
             */
            if action == S_ACTION_FINISH_JOB {
                SchedulerLogger.Infof("Job %s complete", job.GetID())
                scheduler.IssueJobTerminateEvent(job, JOB_EVENT_COMPLETE)
                /*
                 * Task failed and resource or quota released, need kick
                 * scheduler to work
                 */
                stage.FinishHandleEvent(event)
                scheduler.KickScheduler()
            } else if action == S_ACTION_SCHEDULE {
                SchedulerLogger.Debugf("Job %s continue to schedule the rest stages",
                    job.GetID())
                stage.FinishHandleEvent(event)
                scheduler.TryKickScheduleJob(job, false)
            } else if action == S_ACTION_PSEUDO_FINISH_JOB {
                SchedulerLogger.Debugf("Job %s finished with some stages forbidden",
                    job.GetID())
                scheduler.IssueJobTerminateEvent(job, JOB_EVENT_PSEUDONE)
                stage.FinishHandleEvent(event)
                scheduler.KickScheduler()
            } else if action == S_ACTION_ABORT_JOB {
                scheduler.IssueJobTerminateEvent(job, JOB_EVENT_FAIL)
                /*
                 * Task failed and resource or quota released, need kick
                 * scheduler to work
                 */
                stage.FinishHandleEvent(event)
                scheduler.KickScheduler()
            } else {
                /*
                 * Task failed and resource or quota released, need kick
                 * scheduler to work
                 */
                stage.FinishHandleEvent(event)
                scheduler.KickScheduler()
            }

            scheduler.KickResourceAccounting(job, stage)
            scheduler.batcher.UpdateMetric(job)
        case STAGE_FAIL:
            var failPostTime time.Time
            scheduler.perfStats.StartProfileStagePostExec(&failPostTime)
            err := stage.PostExecution(false)
            scheduler.perfStats.EndProfileStagePostExec(failPostTime)
            if err != nil {
                SchedulerLogger.Errorf("Fail to do post execution for job %s stage %s: %s\n",
                    job.GetID(), stageId, err.Error())
            } else {
                SchedulerLogger.Infof("Succeed to do post execution for job %s stage %s\n",
                    job.GetID(), stageId)
            }

            /*
            * Job handles its stage's event and evaluate its failure
            * state.
            */
            err, buildStageErrInfo, action := scheduler._callJobEventHandlerWithProfiling(job, stageId, event, info)
            if err != nil {
                SchedulerLogger.Errorf("Build Job %s stages failure: %s\n",
                    job.GetID(), err.Error())
                if buildStageErrInfo != nil {
                    bioJobFailInfo := NewBioJobFailInfo(buildStageErrInfo.ItemName, "", BIOJOB_FAILREASON_BUILDGRAPH,
                        buildStageErrInfo.Err, buildStageErrInfo)
                    failReason := bioJobFailInfo.BioJobFailInfoFormat()
                    job.SetFailReason(failReason)
                } else {
                    job.SetFailReason(err.Error())
                }
            }
            noticeInfo := NewFrontEndNotcieInfo(job.GetID(), job.Name(), stage.ToBioflowStageInfo())
            e := NewNoticeEvent(FRONTENDNOTICEEVENT, noticeInfo)
            eventbus.Publish(e)
            GetDashBoard().UpdateStats(STAT_TASK_FAIL, 1)
            /*   
             * get stage backend and stage name, to retrieve log of
             * failed task, save the task error log to file under
             * logDir with pattern $backendName-$stageName-$taskID
             */
            if backendId != "" && taskId != "" {
                scheduler.FetchJobSandboxFiles(stageInfo, "")
                scheduler.HandleStageProfiling(stage, taskId)
            }
            scheduler.UnTrackJobTask(job, stageId, backendId, taskId, false)
            scheduler.jobTracker.EvaluateJobMetric(job, false)

            scheduler.SyncJobSchedInfoToDB(job, false, false, 0)

            SchedulerLogger.Debugf("kill task %s of %s\n",
                taskId, backendId)
            err = beScheduler.KillTask(backendId, taskId)
            if err != nil {
                errmsgAboutKillTask := fmt.Sprintf("Handle stage fail event kill task %s failed %s, ignore\n",
                    taskId, err.Error())
                SchedulerLogger.Error(errmsgAboutKillTask)
            }

            noticeMsg := fmt.Sprintf("Task %s of job %s failed: %s. ", taskId, job.GetID(), info.Message())
            if action == S_ACTION_ABORT_JOB {
                SchedulerLogger.Infof("The job %s stage %s fail, fail by policy \n",
                    job.GetID(), stageId)

                /*
                 *  If the user specified stage fail in the stage to fail the entire job,
                 *  we are going to handle the fail event through the IssueJobTerminateEvent.
                 */
                SchedulerLogger.Infof("The job %s stage %s fail by policy \n",
                    job.GetID(), stageId)
                bioJobFailInfo := NewBioJobFailInfo(stage.Name(), stageId,
                    BIOJOB_FAILREASON_STAGEFAILANDFAILJOB, nil, nil)
                failReason := bioJobFailInfo.BioJobFailInfoFormat()
                job.SetFailReason(failReason)
                scheduler.IssueJobTerminateEvent(job, JOB_EVENT_FAIL)
                /*
                 * Task failed and resource or quota released, need kick
                 * scheduler to work
                 */
                stage.FinishHandleEvent(event)
                scheduler.KickScheduler()
                noticeMsg += fmt.Sprintf("Job %s fail.", job.GetID())
            } else if action == S_ACTION_PSEUDO_FINISH_JOB {
                SchedulerLogger.Infof("The job %s stage %s fail, and job pseudo finish by policy \n",
                    job.GetID(), stageId)
                bioJobFailInfo := NewBioJobFailInfo(stage.Name(), stageId,
                    BIOJOB_FAILREASON_STAGEFAILANDPSUDOFINISH, nil, nil)
                failReason := bioJobFailInfo.BioJobFailInfoFormat()
                job.SetFailReason(failReason)
                scheduler.IssueJobTerminateEvent(job, JOB_EVENT_PSEUDONE)
                /*
                 * Task failed and resource or quota released, need kick
                 * scheduler to work
                 */
                stage.FinishHandleEvent(event)
                scheduler.KickScheduler()
                noticeMsg += fmt.Sprintf("Job %s pseudo finish.", job.GetID())
            } else {
                SchedulerLogger.Infof("The job %s stage %s fail, retry by policy \n",
                    job.GetID(), stageId)

                if backendId != "" && taskId != "" {
                    SchedulerLogger.Debugf("kill task %s of %s\n",
                        taskId, backendId)
                    err = beScheduler.KillTask(backendId, taskId)
                    if err != nil {
                        errmsgAboutKillTask := fmt.Sprintf("Handle stage fail event kill task %s failed %s, ignore\n",
                            taskId, err.Error())
                        SchedulerLogger.Error(errmsgAboutKillTask)
                    }
                }
                /*will kick scheduler anyway*/
                stage.FinishHandleEvent(event)
                scheduler.TryKickScheduleJob(job, false)
                noticeMsg += fmt.Sprintf("Retry task %s.", taskId)
            }

            scheduler.KickResourceAccounting(job, stage)

            ctxt := job.GetSecurityContext()
            if ctxt != nil {
                userInfo := GetUserMgr().GetUserInfo(ctxt.ID())
                if userInfo != nil {
                    event := NewNoticeEvent(userInfo.ID(), noticeMsg)
                    eventbus.Publish(event)
                }
            }
            scheduler.batcher.UpdateMetric(job)
        case STAGE_LOST:
            var lostPostTime time.Time
            scheduler.perfStats.StartProfileStagePostExec(&lostPostTime)
            err := stage.PostExecution(false)
            scheduler.perfStats.EndProfileStagePostExec(lostPostTime)
            if err != nil {
                SchedulerLogger.Errorf("Fail to do post execution for job %s stage %s: %s\n",
                    job.GetID(), stageId, err.Error())
            } else {
                SchedulerLogger.Infof("Succeed to do post execution for job %s stage %s\n",
                    job.GetID(), stageId)
            }
                
            /*
            * Job handles its stage's event and evaluate its failure
            * state.
            */
            err, buildStageErrInfo, action := scheduler._callJobEventHandlerWithProfiling(job, stageId, event, info)
            if err != nil {
                SchedulerLogger.Errorf("Build Job %s stages failure: %s\n",
                    job.GetID(), err.Error())
                if buildStageErrInfo != nil {
                    bioJobFailInfo := NewBioJobFailInfo(buildStageErrInfo.ItemName, "", BIOJOB_FAILREASON_BUILDGRAPH,
                        buildStageErrInfo.Err, buildStageErrInfo)
                    failReason := bioJobFailInfo.BioJobFailInfoFormat()
                    job.SetFailReason(failReason)
                } else {
                    job.SetFailReason(err.Error())
                }
            }
            info := NewFrontEndNotcieInfo(job.GetID(), job.Name(), stage.ToBioflowStageInfo())
            e := NewNoticeEvent(FRONTENDNOTICEEVENT, info)
            eventbus.Publish(e)
            GetDashBoard().UpdateStats(STAT_TASK_LOST, 1)
            scheduler.UnTrackJobTask(job, stageId, backendId,
                taskId, false)

            scheduler.SyncJobSchedInfoToDB(job, false, false, 0)
            
            scheduler.jobTracker.EvaluateJobMetric(job, false)
            if backendId != "" && taskId != "" {
                SchedulerLogger.Debugf("kill task %s of %s\n",
                    taskId, backendId)
                err = beScheduler.KillTask(backendId, taskId)
                if err != nil {
                    errmsgAboutKillTask := fmt.Sprintf("Handle stage fail event kill task %s failed %s, ignore\n",
                        taskId, err.Error())
                    SchedulerLogger.Error(errmsgAboutKillTask)
                }
            }
            if action == S_ACTION_ABORT_JOB {
                SchedulerLogger.Infof("The job %s stage %s lost, fail by policy \n",
                    job.GetID(), stageId)

                /*
                 *  If the user specified stage fail in the stage to fail the entire job,
                 *  we are going to handle the fail event through the IssueJobTerminateEvent.
                 */
                SchedulerLogger.Infof("The job %s stage %s lost by policy \n",
                    job.GetID(), stageId)
                bioJobFailInfo := NewBioJobFailInfo(stage.Name(), stageId,
                    BIOJOB_FAILREASON_STAGEFAILANDFAILJOB, nil, nil)
                failReason := bioJobFailInfo.BioJobFailInfoFormat()
                job.SetFailReason(failReason)
                scheduler.IssueJobTerminateEvent(job, JOB_EVENT_FAIL)
                /*
                 * Task failed and resource or quota released, need kick
                 * scheduler to work
                 */
                stage.FinishHandleEvent(event)
                scheduler.KickScheduler()
            } else if action == S_ACTION_PSEUDO_FINISH_JOB {
                SchedulerLogger.Infof("The job %s stage %s lost, and job pseudo finish by policy \n",
                    job.GetID(), stageId)
                bioJobFailInfo := NewBioJobFailInfo(stage.Name(), stageId,
                    BIOJOB_FAILREASON_STAGEFAILANDPSUDOFINISH, nil, nil)
                failReason := bioJobFailInfo.BioJobFailInfoFormat()
                job.SetFailReason(failReason)
                scheduler.IssueJobTerminateEvent(job, JOB_EVENT_PSEUDONE)
                /*
                 * Task failed and resource or quota released, need kick
                 * scheduler to work
                 */
                stage.FinishHandleEvent(event)
                scheduler.KickScheduler()
            } else {
                SchedulerLogger.Infof("The job %s stage %s lost, retry by policy \n",
                    job.GetID(), stageId)

                /*will kick scheduler anyway*/
                stage.FinishHandleEvent(event)
                scheduler.TryKickScheduleJob(job, false)
            }

            scheduler.KickResourceAccounting(job, stage)
            scheduler.batcher.UpdateMetric(job)
        case STAGE_SUBMITTED, STAGE_RUNNING, STAGE_QUEUED:
            _, _, action := scheduler._callJobEventHandlerWithProfiling(job, stageId, event, info)
            stage := job.GetStage(stageId)
            scheduler.jobTracker.EvaluateJobMetric(job, false)
            /*update database asynchronously if required*/
            if action == S_ACTION_UPDATE_DB {
                scheduler.SyncJobSchedInfoToDB(job, false, false, 0)
            }
            stage.FinishHandleEvent(event)
            info := NewFrontEndNotcieInfo(job.GetID(), job.Name(), stage.ToBioflowStageInfo())
            e := NewNoticeEvent(FRONTENDNOTICEEVENT, info)
            eventbus.Publish(e)

        case STAGE_SUBMIT_FAIL:
            /*
             * Some graph scheduler may need handle the stage submit fail events.
             */
            job.GetScheduleInfo().DeletePendingStage(stageId)
            _, _, action := scheduler._callJobEventHandlerWithProfiling(job, stageId, event, info)
            stage.FinishHandleEvent(event)
            if action == S_ACTION_SCHEDULE {
                SchedulerLogger.Infof("HandleAsyncJobEvent(job %s): kick schedule job on stage %s submit failure\n",
                    job.GetID(), stageId)
                /*
                 * will kick scheduler anyway, but don't kick immediately,
                 * because it may fail again
                 */
                scheduler.TryKickScheduleJob(job, true)
            }
            scheduler.batcher.UpdateMetric(job)
        default:
            SchedulerLogger.Errorf("Handle async stage %s event %d, not expected\n",
                stageId, event)
            stage.FinishHandleEvent(event)
            return errors.New("Un-expected event")
    }

    SchedulerLogger.Infof("HandleAsyncJobEvent(job %s): handle stage %s event %s done\n",
        job.GetID(), stageId, StageStateToStr(event))
    return nil
}

/*
 * It is the main routine called by scheduler worker thread. It
 * will do the following:
 * 1) collect jobs required to schedule from scheduleQueue
 * 2) for each job, calls its schedule function to get all the
 *    stages which has no pending dependency from the flow graph.
 * 3) Submit all the stages to the backend
 *
 */
func (scheduler *bioScheduler) ReScheduleJob() {
    jobList := scheduler.jobTracker.SelectJobs(scheduler.maxJobsSchedulePerRound)
    if jobList != nil && len(jobList) > 0 {
        SchedulerLogger.Infof("Will re-schedule %d jobs\n",
            len(jobList))
    } else {
        /*
         * No jobs to schedule, need set the periodically interval
         * to a longer time
         */
        return
    }
    
    /*try to schedule the job*/
    for _, job := range jobList {
        /* 
         * Hash sync worker ensure that the same job will be handled by the same
         * worker thread
         */
        scheduler.IssueJobScheduleOperation(job)
    }

}

/*
 * Issue job scheduler operation to the per priority worker threads,
 * this will avoid the priority reverse problem caused by low priority
 * jobs which issue too many stages at one time
 */
func (scheduler *bioScheduler) IssueJobScheduleOperation(job Job) {
    pri := job.Priority()
    if pri < JOB_MIN_PRI || pri > JOB_MAX_PRI {
        pri = 0
    }

    syncWorker := scheduler.jobScheduleWorkers[pri]
    
    /* 
     * Hash sync worker ensure that the same job will be handled by the same
     * worker thread
     */
    syncWorker.IssueOperation(job.GetID(), job,
        func (data interface{}) error {
            job := data.(Job)
            if !job.RequestScheduleOperation(JOB_OP_SCHEDULE, nil) {
                SchedulerLogger.Infof("Job %s is not allowed schedule, skip it\n",
                    job.GetID())
                return nil
            }

            scheduler.ScheduleSingleJob(job)

            /*
             * Finish the requested schedule operation here, it may need
             * resume the pending cleanup operation
             */
            scheduler._FinishResumeJobScheduleOperation(job)

            return nil
        }, false, 0, job.GetID())
}

func (scheduler *bioScheduler) _FinishResumeJobScheduleOperation(job Job) {
    /*
     * Finish the requested schedule operation here, it may need
     * resume the pending cleanup operation
     */
    schedOp, schedOpData := job.FinishScheduleOperation(JOB_OP_SCHEDULE)
    if schedOp == JOB_OP_TERMINATE {
        /*need resume the pending cleanup operation*/
        SchedulerLogger.Infof("Resume the job %s terminate after schedule operation\n",
            job.GetID())
        scheduler._sendTerminateTaskToWorkers(schedOpData.(*JobTerminateTask))
    }
}

func (scheduler *bioScheduler) ScheduleSingleJob(job Job) {
    if job.Canceled() {
        SchedulerLogger.Infof("The job %s is canceled, so cleanup \n",
            job.GetID())
        scheduler.IssueJobTerminateEvent(job, JOB_EVENT_CANCELED)
        return
    }

    SchedulerLogger.Infof("Start to schedule job %s", job.GetID())
    stageQuota := scheduler.batcher.GetStageQuota(job)

    if stageQuota == 0 {
        scheduler.TryKickScheduleJob(job, false)
        return
    }

    /*Profiling the single job schedule process*/
    var preTime time.Time
    scheduler.perfStats.StartProfileSchedJob(&preTime)
    defer scheduler.perfStats.EndProfileSchedJob(preTime)

    stages, action, err := job.ScheduleStages()
    if err != nil {
        SchedulerLogger.Infof("ScheduleJob(job %s): schedule error %s, fail job\n",
            job.GetID(), err.Error())
        failReason := fmt.Sprintf("Schedule job fail: %s", err.Error())
        job.SetFailReason(failReason)
        scheduler.IssueJobTerminateEvent(job, JOB_EVENT_FAIL)
        scheduler.KickScheduler()
        return
    }

    switch action {
        case S_ACTION_CLEANUP_JOB:
            SchedulerLogger.Infof("ScheduleJob(job %s): cleanup job\n",
                job.GetID())
            scheduler.HandleJobEventAsync(job.GetID(), job, "", "",
                JOB_CLEANUP, nil, true)
            scheduler.KickScheduler()
            return
        case S_ACTION_NONE:
            SchedulerLogger.Infof("ScheduleJob(job %s): none action, skip\n",
                job.GetID())
            return
        case S_ACTION_FINISH_JOB:
            SchedulerLogger.Infof("ScheduleJob(job %s): finish job\n",
                job.GetID())
            scheduler.IssueJobTerminateEvent(job, JOB_EVENT_COMPLETE)
            scheduler.KickScheduler()
            return
        case S_ACTION_PSEUDO_FINISH_JOB:
            SchedulerLogger.Infof("ScheduleJob(job %s): psedo finish job\n",
                job.GetID())
            scheduler.IssueJobTerminateEvent(job, JOB_EVENT_PSEUDONE)
            scheduler.KickScheduler()
            return
        case S_ACTION_ABORT_JOB:
            SchedulerLogger.Infof("ScheduleJob(job %s): abort job\n",
                job.GetID())
            job.SetFailReason("Job aborted by scheduler")
            scheduler.IssueJobTerminateEvent(job, JOB_EVENT_FAIL)
            scheduler.KickScheduler()
            return
    }

    SchedulerLogger.Infof("Schedule job %s with stages %d action %s\n",
        job.GetID(), len(stages), action.String())

    var stageToSchedule []Stage
    var stageToRetry []Stage

    for stageID, stage := range stages {
        if !job.AllowSchedule() {
            /*
             * As much as possible ward off stages which would be scheduled to backend
             * Between completed flowGraph and cancel,fail,pause job.
             */
            SchedulerLogger.Infof("job(id %s, state %s) not allow scheduled, send submit fail event for stage %s",
                job.GetID(), JobStateToStr(job.State()), stageID)

            /*
             * Skip scheduling the stage but still need to post a stage submit cancel event
             * for the stage.
             */
            scheduler.PostPsudoJobEvent("", "",
                STAGE_SUBMIT_FAIL, "stage submit canceled", job, stageID)
            continue
        }

        SchedulerLogger.Infof("scheduler will schedule job %s stage %s \n",
            job.GetID(), stageID)
        /*
         * Check stage state and try schedule:
         */
        action := job.EvaluateStageScheduleAction(stage)

        /*Handle the special case first*/
        switch action {
        case S_ACTION_RETRY_STAGE:
            stageToRetry = append(stageToRetry, stage)
            continue
        case S_ACTION_ABORT_JOB:
            SchedulerLogger.Infof("ScheduleJob(job %s, stage %s): state %s, abort job \n",
                job.GetID(), stageID, StageStateToStr(stage.State()))
            /*fail the job*/
            GetDashBoard().UpdateStats(STAT_JOB_RUN_FAIL, 1)
            bioJobFailInfo := NewBioJobFailInfo(stage.Name(), stage.GetID(),
                BIOJOB_FAILREASON_REACHRETRYNUM, nil, nil)
            failReason := bioJobFailInfo.BioJobFailInfoFormat()
            job.SetFailReason(failReason)
            scheduler.IssueJobTerminateEvent(job, JOB_EVENT_FAIL)
            return
        case S_ACTION_PRUNE_STAGE:
            SchedulerLogger.Infof("ScheduleJob(job %s, stage %s): prune stage\n",
                job.GetID(), stageID)
            continue
        case S_ACTION_INVALID:
            SchedulerLogger.Errorf("ScheduleJob(job %s, stage %s): invalid action in state %s\n",
                job.GetID(), stageID, StageStateToStr(stage.State()))
            continue
        case S_ACTION_PSEUDO_FINISH_JOB:
            SchedulerLogger.Infof("ScheduleJob(job %s, stage %s): pseudo finish in state %s\n",
                job.GetID(), stageID, StageStateToStr(stage.State()))
            scheduler.IssueJobTerminateEvent(job, JOB_EVENT_PSEUDONE)
            scheduler.KickScheduler()
            return
        case S_ACTION_CHECK_FAIL_STATUS:
            SchedulerLogger.Infof("ScheduleJob(job %s, stage %s): %s, fail retry by limit\n",
                job.GetID(), stageID, StageStateToStr(stage.State()))
            /*Simulate the event to ensure the process is consistent.In fact, it's not really an event.*/
            scheduler.PostPsudoJobEvent("", "",
                STAGE_FAIL, "stage fail retry exceeded the limit", job, stageID)
            continue
        case S_ACTION_CHECK_LOST_STATUS:
            SchedulerLogger.Infof("ScheduleJob(job %s, stage %s): %s, fail retry by limit\n",
                job.GetID(), stageID, StageStateToStr(stage.State()))
            /*Simulate the event to ensure the process is consistent.In fact, it's not really an event.*/
            scheduler.PostPsudoJobEvent("", "",
                STAGE_LOST, "stage lost retry exceeded the limit", job, stageID)
            continue
        case S_ACTION_NONE:
            SchedulerLogger.Infof("ScheduleJob(job %s, stage %s): %s, none action, send submit fail event\n",
                job.GetID(), stageID, StageStateToStr(stage.State()))
            /*
                 * Skip scheduling the stage but still need to post a stage submit cancel event
                 * for the stage.
                 */
            scheduler.PostPsudoJobEvent("", "", STAGE_SUBMIT_FAIL,
                "stage submit canceled", job, stageID)
            continue
        default:
            stageToSchedule = append(stageToSchedule, stage)
        }
    }
    /*Handle the normal stage schedule operation*/
    scheduler.batcher.PrepareSchedule(job)

    need := len(stageToRetry) + len(stageToSchedule)
    get := scheduler.batcher.RequestStageQuota(job, need)

    var delaySchedule []Stage
    var used int
    if get < len(stageToRetry) {
        used = 0
        delaySchedule = append(delaySchedule, stageToRetry[get:]...)
        delaySchedule = append(delaySchedule, stageToSchedule...)
        stageToRetry = stageToRetry[:get]
        stageToSchedule = nil
    } else if get < need {
        used = get - len(stageToRetry)
        delaySchedule = stageToSchedule[used:]
        stageToSchedule = stageToSchedule[:used]
    } else {
        used = len(stageToSchedule)
    }

    for _, stage := range stageToRetry {
        GetDashBoard().UpdateStats(STAT_TASK_RETRY, 1)
        stage.PrepareRetry()
        SchedulerLogger.Infof("ScheduleJob(job %s, stage %s): retry %d time\n",
            job.GetID(), stage.GetID(), stage.RetryCount())
    }

    if len(stageToRetry) > 0 {
        scheduler.DelayScheduleStage(job, stageToRetry)
    }

    /*
     * If there are too many stages to schedule, leverage workers
     * to issue them in parallel
     */
    StageCount := len(stageToSchedule)
    if (StageCount > MAX_BATCH_STAGES) && scheduler.enableStageWorker {
        /*Issue via workers in parallel*/
        for start := 0; start < StageCount; start += MAX_BATCH_STAGES {
            end := start + MAX_BATCH_STAGES
            if end > StageCount {
                end = StageCount
            }
            stagesToIssue := stageToSchedule[start:end]
            err := scheduler.stageScheduleWorker.IssueOperation(stagesToIssue,
                func(data interface{}) error {
                    subStages := data.([]Stage)

                    /*The stages are scheduled asynchronously, so need sync with job terminate*/
                    if !job.RequestScheduleOperation(JOB_OP_SCHEDULE, nil) {
                        SchedulerLogger.Infof("Job %s is not allowed schedule, skip schedule stages\n",
                            job.GetID())
                        /*send stages submit fail event*/
                        for _, stage := range subStages {
                            scheduler.PostPsudoJobEvent("", "",
                                STAGE_SUBMIT_FAIL, "stage submit canceled", job, stage.GetID())
                        }
                        return nil
                    }

                    scheduler.ScheduleStages(job, subStages)
                    
                    /*finish and resume pending schedule operation*/
                    scheduler._FinishResumeJobScheduleOperation(job)

                    return nil
                }, false, 0, "")
            if err != nil {
                SchedulerLogger.Errorf("Fail to issue stages in parallel for job %s: %s\n",
                    job.GetID(), err.Error())
            }
        }
    } else {
        /*Issue in current thread*/
        scheduler.ScheduleStages(job, stageToSchedule)
    }
    scheduler.batcher.UseQuota(job, used)
    scheduler.batcher.FinishSchedule(job)
    scheduler.batcher.UpdateMetric(job)

    for _, stage := range delaySchedule {
        scheduler.PostPsudoJobEvent("", "", STAGE_SUBMIT_FAIL,
            "reached the scheduling limit", job, stage.GetID())
    }
}

func (scheduler *bioScheduler) DelayScheduleStage(job Job, stages []Stage) {
    delay := time.Duration(scheduler.stageRetryDelay) * time.Second
    var duration time.Duration
    for _, stage := range stages {
        duration = time.Duration(time.Now().Unix() - stage.FinishTime().Unix()) * time.Second
        retryStage := stage
        time.AfterFunc(delay - duration, func() {
            SchedulerLogger.Infof("Stage %s of job %s is retried schedule after delay\n",
                stage.GetID(), job.GetID())
            scheduler.stageScheduleWorker.IssueOperation(job,
                func(data interface{}) error {
                    job := data.(Job)
                    
                    /*The stage is scheduled asynchronously, so need sync with job terminate*/
                    if !job.RequestScheduleOperation(JOB_OP_SCHEDULE, nil) {
                        SchedulerLogger.Infof("Job %s is not allowed schedule, skip it\n",
                            job.GetID())
                        scheduler.batcher.ReleaseQuota(job, 1)
                        /*send stage submit fail event*/
                        scheduler.PostPsudoJobEvent("", "",
                            STAGE_SUBMIT_FAIL, "stage submit canceled", job, retryStage.GetID())
                        return nil
                    }

                    scheduler.batcher.PrepareSchedule(job)
                    scheduler.ScheduleStages(job, []Stage{retryStage})
                    scheduler.batcher.UseQuota(job, 1)
                    scheduler.batcher.FinishSchedule(job)
                    scheduler.batcher.UpdateMetric(job)

                    /*
                     * Finish the requested schedule operation here, it may need
                     * resume the pending cleanup operation
                     */
                    scheduler._FinishResumeJobScheduleOperation(job)

                    return nil
                }, false, 0, "")
        })
    }
}

func (scheduler *bioScheduler) ScheduleStages (job Job, stages []Stage) {
    stageQuiesceFailCount := 0
    stageOtherFailCount := 0
    var err error

    for _, stage := range stages {
        /*
         * When sechedule thousands of stages, it may take a long time. Should cancel
         * the schedule if the job is marked not allow schedule.
         */
        if !job.AllowSchedule() {
            /*
             * As much as possible ward off stages which would be scheduled to backend
             * Between completed flowGraph and cancel,fail,pause job.
             */
            SchedulerLogger.Infof("job(id %s, state %s) not allow scheduled, send submit fail event for stage %s",
                job.GetID(), JobStateToStr(job.State()), stage.GetID())

            /*
             * Skip scheduling the stage but still need to post a stage submit cancel event
             * for the stage.
             */
            scheduler.PostPsudoJobEvent("", "",
                STAGE_SUBMIT_FAIL, "stage submit canceled", job, stage.GetID())
            continue
        }

        stageID := stage.GetID()
        err = scheduler.ScheduleStage(job, stage)
        if err == SCHED_ERR_PREPARE_EXECUTION {
            SchedulerLogger.Errorf("ScheduleJob(job %s, stage %s): prepare fail: %s\n",
                job.GetID(), stage.GetID(), err.Error())
            break
        } else if err == SCHED_ERR_QUIESCED {
            SchedulerLogger.Warnf("ScheduleJob(job %s, stage %s): schedule stage fail by quiesce\n",
                job.GetID(), stage.GetID())
            stageQuiesceFailCount ++
        } else if err != nil {
            SchedulerLogger.Warnf("ScheduleJob(job %s, stage %s): schedule stage fail by: %s\n",
                job.GetID(), stage.GetID(), err.Error())
            stageOtherFailCount ++
        }

        if err == nil {
            SchedulerLogger.Infof("ScheduleJob(job %s, stage %s): stage submitted \n",
                job.GetID(), stageID)
        } else {
            /*
             * Stage submit failed, post an event.
             * Actually some graph schedulers will be interested in it, while others may ignore
             * it. It depends on the internal implementation.
             * Currently post an event to handle asynchronously. The graph scheduler should be
             * aware of it.
             */
            scheduler.PostPsudoJobEvent("", "",
                STAGE_SUBMIT_FAIL, "stage submit fail", job, stageID)
        }
    }


    if err == SCHED_ERR_PREPARE_EXECUTION {
        SchedulerLogger.Errorf("ScheduleJob(job %s): prepare execution fail\n",
            job.GetID())
        GetDashBoard().UpdateStats(STAT_JOB_FAIL, 1)
        scheduler.IssueJobTerminateEvent(job, JOB_EVENT_FAIL)
    }

    /*
     * If some stage scheduled fail by backend or schedule quiesce, we need
     * mark the job need schedule and let work threads to retry schedule it
     * in a later time.
     */
    if stageQuiesceFailCount > 0 || stageOtherFailCount > 0 {
        SchedulerLogger.Warnf("ScheduleJob(job %s): schedule has %d quiesce failures %d other failures\n",
            job.GetID(), stageQuiesceFailCount, stageOtherFailCount)
        /*
         * mark it need schedule but don't kick it, because retry it immediately may
         * fail in high probability
         */
        scheduler.TryKickScheduleJob(job, true)
    }
}

/*
 * Start routine will start the worker threads of the scheduler
 */
func (scheduler *bioScheduler) Start() int {
    /*the scheduler thread*/
    SchedulerLogger.Info("Starts scheduler thread")
    go func() {
        for ;; {
            scheduler.scheduleLock.Lock()
            if !scheduler.Quiesced() && !scheduler.Disabled() {
                scheduler.scheduleKickCount = 0
                scheduler.scheduleLock.Unlock()

                SchedulerLogger.Info("Scheduler try to schedule jobs")
                scheduler.ReScheduleJob()
                SchedulerLogger.Info("Scheduler done, wait new events")

                scheduler.scheduleLock.Lock()
            }
            if scheduler.Disabled() || scheduler.Quiesced() ||
                scheduler.scheduleKickCount == 0 {
                SchedulerLogger.Infof("Scheduler quiesced or kick count %d, don't schedule\n",
                    scheduler.scheduleKickCount)
                scheduler.scheduleCond.Wait()
            }

            SchedulerLogger.Info("Kicked, scheduler starts work")
            scheduler.scheduleLock.Unlock()
        }
    } ()

    /*thread to periodically kick off the schedule*/
    SchedulerLogger.Info("Starts Task check thread")
    go func() {
        round := 1
        for {
            if round % scheduler.checkInterval != 0 && !scheduler.jobTracker.NeedCheckAllPendingJobs() {
                jobMap := scheduler.delayJobChecker.TimedWait(scheduler.delayJobCheckTimeout)
                SchedulerLogger.Infof("Delay job checked check %d scheduled jobs",
                    len(jobMap))
                for _, val := range jobMap {
                    job := val.(Job)
                    if job != nil {
                        schedInfo := job.GetScheduleInfo()
                        if schedInfo != nil {
                            stages := schedInfo.GetPendingStageInfos()
                            if stages != nil && len(stages) > 0 {
                                SchedulerLogger.Infof("Delay job checker check %d tasks\n",
                                    len(stages))
                                scheduler.CheckJobStageStatus(stages)
                                /*sleep some seconds here to avoid overwhelm backends*/
                                time.Sleep(3 * time.Second)
                            }
                        }
                    }
                }
            } else {
                scheduler.delayJobChecker.TimedWait(DELAY_CHECK_TIMEOUT)
                staleJobs := scheduler.jobTracker.GetJobsNeedCheck(JOB_STALE_TIMEOUT)
                if staleJobs == nil {
                    continue
                }

                SchedulerLogger.Infof("Delay job checker check %d stale jobs\n",
                    len(staleJobs))
                for i := 0; i < len(staleJobs); i ++ {
                    job := staleJobs[i]
                    if job != nil {
                        schedInfo := job.GetScheduleInfo()
                        if schedInfo != nil {
                            stages := schedInfo.GetPendingStageInfos()
                            if stages != nil && len(stages) > 0 {
                                SchedulerLogger.Infof("Delay job checker check %d tasks\n",
                                    len(stages))
                                scheduler.CheckJobStageStatus(stages)
                                /*sleep some seconds not to overwhelm backends*/
                                time.Sleep(2 * time.Second)
                            }
                        }

                        if scheduler.AutoReScheduleHangTasks() {
                            scheduler.ReScheduleHangJob(job)
                        }
                    }
                }
            }
            SchedulerLogger.Info("Delay job checker done")
            round ++
        }
    } ()

    go func() {
        for {
            time.Sleep(time.Duration(scheduler.kickSchedulePeriod) * time.Second)
            /*need resume the quiesced scheduler here to avoid deadlock*/
            if scheduler.Quiesced() {
                SchedulerLogger.Info("The scheduler periodically resume and kick schedule job\n")
                scheduler.Resume()
            }
            scheduler.KickScheduler()
        }
    } ()

    scheduler.beScheduler.Start()
    if GetPhysicalScheduler().Enabled() {
        go func() {
            for {
                if scheduler.Quiesced() || scheduler.Disabled() {
                    SchedulerLogger.Infof("Scheduler is quiesced or disabled.\n")
                } else {
                    err, slaveIps := scheduler.beScheduler.GetRemovableSlaves()
                    if err != nil {
                        SchedulerLogger.Infof("Failed to get removable slaves, err: %s", err.Error())
                    }

                    if slaveIps != nil && len(slaveIps) > 0 {
                        phyEvent := PhysicalEvent{
                            EventType: EVENTDELETE,
                            EventData: slaveIps,
                            EventTime: time.Now(),
                        }

                        SchedulerLogger.Debugf("Notify physical machine scheduler event: %s\n",
                            PhysicalEventtoString(phyEvent))
                        GetPhysicalScheduler().SubmitEvent(&phyEvent)
                    }
                }

                time.Sleep(time.Second * time.Duration(GetPhysicalScheduler().GetConfig().DeleteInstancesInterval))
            }
        }()
    }


    return 0
}

func (scheduler *bioScheduler) GetJobSchedInfoByTaskId(backendId string,
    taskId string) *JobScheduleInfo{
    err, job := scheduler.BackendScheduler().GetJobFromTask(backendId,
        taskId)
    if err != nil || job == nil {
        SchedulerLogger.Errorf("No job id associated with task id %s\n",
            taskId)
        return nil
    }

    return job.GetScheduleInfo()
}

/*
 * When a task of the stage of a job submitted, completed, failed. it will
 * be called to handle it. It will trigger the scheduler worker thread to
 * re-schedule the job or fail the job directly.
 *
 */
func (scheduler *bioScheduler) HandleJobEvent(backendId string,
    taskId string, event uint32, info *EventNotifyInfo) {
    schedInfo := scheduler.GetJobSchedInfoByTaskId(backendId, taskId)
    if schedInfo != nil {
        stageId, _ := schedInfo.GetStageOfTask(backendId, taskId)
        job := schedInfo.Job()

        SchedulerLogger.Infof("HandleJobEvent(job %s, task %s, stage %s): event %s message %s\n",
            job.GetID(), taskId, stageId, StageStateToStr(event), info.Message())

        /*
         * Hash sync worker ensure that the same stage will be handled by the same
         * worker thread
         */
        syncKey := job.GetID() + stageId
        scheduler.HandleJobEventAsync(syncKey, job, backendId, taskId, event, info, true)
    } else {
        SchedulerLogger.Infof("HandleJobEvent(task %s): No schedule info found\n",
            taskId)
        GetDashBoard().UpdateStats(STAT_TASK_STALE_NOTIFY, 1)
    }
}

/*
 * Schedule a stage of the job, it will submit it via backend docker task and
 * track it in jobExecInfo. the changes will be persist to database as a result.
 */
func (scheduler *bioScheduler) _ScheduleStage(job Job, stage Stage) error {
    /*
     *Build stage's command to be executed, this will also collect
     *data volumes which need to be mapped to the container
     */
    var prepareTime time.Time
    scheduler.perfStats.StartProfileStagePrepare(&prepareTime)
    err, buildStageErrInfo, stageExecEnv := stage.PrepareExecution(false)
    scheduler.perfStats.EndProfileStagePrepare(prepareTime)
    if err != nil {
        SchedulerLogger.Errorf("Can't prepare execution for stage %s: %s\n",
            stage.GetID(), err.Error())
        bioJobFailInfo := NewBioJobFailInfo(stage.Name(), stage.GetID(),
            BIOJOB_FAILREASON_PREPARESTAGEFAIL, err, buildStageErrInfo)
        failReason := bioJobFailInfo.BioJobFailInfoFormat()
        job.SetFailReason(failReason)
        return SCHED_ERR_PREPARE_EXECUTION
    }

    /*
     * A task requiring too many CPU or Memory which is beyound any physical machine
     * in the cluster will block other tasks to be scheduled in bioflow or backend.
     * So check the requested memory and cpu resource value here.
     */
    if stage.GetCPU() > scheduler.maxCPUPerTask {
        SchedulerLogger.Errorf("The stage %s of job %s require too many cpus (%f > %f)\n",
            stage.GetID(), job.GetID(), stage.GetCPU(), scheduler.maxCPUPerTask)
        bioJobFailInfo := NewBioJobFailInfo(stage.Name(), stage.GetID(),
            BIOJOB_FAILREASON_EXCEEDINGCPULIMIT, nil, nil)
        failReason := bioJobFailInfo.BioJobFailInfoFormat()
        job.SetFailReason(failReason)
        return SCHED_ERR_PREPARE_EXECUTION
    }
    if stage.GetMemory() > scheduler.maxMemoryPerTask {
        SchedulerLogger.Errorf("The stage %s of job %s require too many memory (%f > %f)\n",
            stage.GetID(), job.GetID(), stage.GetMemory(), scheduler.maxMemoryPerTask)
        bioJobFailInfo := NewBioJobFailInfo(stage.Name(), stage.GetID(),
            BIOJOB_FAILREASON_EXCEEDINGMEMLIMIT, nil, nil)
        failReason := bioJobFailInfo.BioJobFailInfoFormat()
        job.SetFailReason(failReason)
        return SCHED_ERR_PREPARE_EXECUTION
    }

    storageMgr := GetStorageMgr()
    tempDir := storageMgr.GetTempVolumeMount()
    _, _, conTempDir := storageMgr.GetContainerExecDir()

    err, dataVols := stage.GetDataVolsMap()
    if err != nil {
        SchedulerLogger.Errorf("Can't build volumes map for stage %s: %s\n",
            stage.GetID(), err.Error())

        bioJobFailInfo := NewBioJobFailInfo(stage.Name(), stage.GetID(),
            BIOJOB_FAILREASON_NOMOUNTPOINT, err, nil)
        failReason := bioJobFailInfo.BioJobFailInfoFormat()
        job.SetFailReason(failReason)
        return SCHED_ERR_PREPARE_EXECUTION
    }

    volsMap := make(map[string]string)
    scheduleConstraints := make(map[string]string)
    /*Add the volumes map required by user in the pipeline*/
    if stageExecEnv.Volumes != nil {
        for key, val := range stageExecEnv.Volumes {
            volsMap[key] = val
        }
    }

    /*
     * build extra container volumes which is mapped by bioflow
     * automatically to help it access the storage specified in
     * the URI
     */
    for i := 0; i < len(dataVols); i ++ {
        volURI := dataVols[i].VolURI()
        if !dataVols[i].IsHDFSVol() {
            hostPath := storageMgr.GetDataVolumesMap(volURI)
            if hostPath == "" {
                SchedulerLogger.Errorf("Fail to find server mountpoint for %s\n",
                    volURI)
                bioJobFailInfo := NewBioJobFailInfo(stage.Name(), stage.GetID(),
                    BIOJOB_FAILREASON_NOMOUNTPOINT, nil, nil)
                failReason := bioJobFailInfo.BioJobFailInfoFormat()
                job.SetFailReason(failReason)
                return SCHED_ERR_PREPARE_EXECUTION
            }

            /*check whether we need map a dir*/
            if dataVols[i].Path() != "" {
                hostPath += "/" + dataVols[i].Path()
            }
            volsMap[dataVols[i].ContainerPath()] = hostPath
        }
        
        /*
         * Build schedule constraints according to the volumes required
         * for this task
         */
        err, constraintKey, constraintVal :=
            storageMgr.BuildStorageVolConstraints(volURI)
        if err != nil {
            SchedulerLogger.Errorf("Fail to build storage constraints for vol %s: %s\n",
                volURI, err.Error())
            return errors.New("Fail build constraints for " + volURI)
        } else if constraintKey != "" {
            /*
             * an empty constraint key means we place no constraint
             * on the nodes
             */
            scheduleConstraints[constraintKey] = constraintVal
        }
    }

    volsMap[conTempDir] = tempDir

    /*decide whether disable out of memory killer in backend*/
    disableOOMKill := false
    if job.OOMKillDisabled() {
        disableOOMKill = true
    } else if scheduler.disableOOMKill {
        disableOOMKill = true
    }

    /* don't use the soft memory limit if too small, so we can 
     * disable the feature by setting softLimitRatio to 0
     */
    softMemLimit := stage.GetMemory() * scheduler.softLimitRatio
    if softMemLimit <= 1 {
        /*pass negative value to disable soft memory limit*/
        softMemLimit = -1
    }

    /*
     * If a task requiring be accessible from outside of the
     * container, use the host networking.
     *
     */
    useHostNet := false
    if stageExecEnv.NetMode == CLUSTER_NET_DUPLEX_MODE {
        useHostNet = true
    }

    var overSubscribeCPU float64 = 0
    var overSubscribeMem float64 = 0
    isKillable := false
    if scheduler.qosEnabled {
        overSubscribeCPU = stage.GetCPU() * scheduler.cpuCapRatio
        overSubscribeMem = stage.GetMemory() * scheduler.memCapRatio
        isKillable = true
    }

    taskLabels := make(map[string]string)

    /*
     * Try to schedule cache according to io attributes of the stage.
     * This may improve the performance a lot, while the cost is pretty
     * high.
     */
    cacheScheduler := NewCacheScheduler(stageExecEnv.OutputDirs, stageExecEnv.OutputFiles,
        stage.IOAttr())
    if cacheScheduler != nil {
        for key, val := range cacheScheduler.GenerateIOLabels() {
            taskLabels[key] = val
        }
    }

    /*
     * The executor end resource usage profiling should be enabled
     * via label
     */
    if scheduler.profilerEnabled {
        taskLabels[FEATURE_LABEL_PROFILER_KEY] = FEATURE_LABEL_PROFILER_VALUE
    }

    /*
     * Bioflow try to leverage schedule nodes' constrait to select
     * nodes which be capable of accessing the data required by the
     * job. If we choose to configure all the nodes to access all the
     * the storage cluster, then we needn't use the storage constrait.
     * here:
     * 1) reset all the server constraints required by storage cluster
     *    if needed
     * 2) add constraints required by server type requirement. For example,
     *    the gene assembly tasks may need to be done on "Fat" nodes.
     *
     */
    if !scheduler.enableStorageConstraint {
        scheduleConstraints = make(map[string]string)
    }

    if stage.GetServerType() != "" {
        scheduleConstraints["perf_type"] = strings.ToLower(stage.GetServerType())
    } else {
        if scheduler.enforceServerTypeConstraint {
            scheduleConstraints["perf_type"] = "normal"
        }
    }

    /* merge storage constraints with stage's constraints set by users*/
    for key, value := range stage.Constraints() {
        scheduleConstraints[key] = value
    }

    /*
     * Merge the job's schedule constraints to it if have. Job should override
     * the stage's configuration.
     */
    for key, value := range job.Constraints() {
        scheduleConstraints[key] = value
    }

    taskUser := ""
    taskUid := ""
    taskGid := ""
    if stage.ExecMode() == EXEC_MODE_HOST {
        taskUser = job.SecID()
    } else {
        if scheduler.useTaskOwner {
            taskUser = job.SecID()
        }
    }

    /*
     * Get the user id and group id of the user. The mesos docker executor
     * will leverage it to run process via these IDs.
     */
    if taskUser != "" {
        taskUid, taskGid = GetUserMgr().GetUIDByName(taskUser)
    }

    taskName := fmt.Sprintf("%s.bioflow.%s.%s.%s", job.SecID(), job.GetID(),
        stage.Name(), stage.GetID())
    var workDir string
    if stageExecEnv.ForceChdir || scheduler.setStageWorkdirToContainerWorkdir {
        workDir = stageExecEnv.WorkDir
    } else {
        workDir = "/"
    }

    /*The gpus and gpu memory setting should be exclusive*/
    gpus := stage.GetGPU()
    gpuMemory := stage.GetGPUMemory()
    if gpus >= 0.001 {
        gpuMemory = 0
    }

    task := backend.ScheduleBackendTask {
        Cmd:     stage.GetCommand(),
        Image:   stage.GetImage(),
        CPU:    stage.GetCPU(),
        GPU: gpus,
        GPUMemory: gpuMemory,
        Disk: stage.GetDisk(),
        OversubCPU: overSubscribeCPU,
        OversubMemory: overSubscribeMem,
        Memory:     stage.GetMemory(),
        SoftMemLimit: softMemLimit,
        ForcePullImage: true,
        VolumesMap: volsMap,
        DisableOOMKill: disableOOMKill,
        ScheduleConstraints: scheduleConstraints,
        WorkDir: workDir,
        UseHostNet: useHostNet,
        Priority: job.Priority(),
        IsKillable: isKillable,
        Privileged: stageExecEnv.Privileged,
        Env: stageExecEnv.Env,
        ExecMode: stage.ExecMode(),
        User: taskUser,
        UID: taskUid,
        GID: taskGid,
        TaskName: taskName,
        Labels: taskLabels,
    }

	if scheduler.Quiesced() || scheduler.Disabled() {
		errmsg := fmt.Sprintf("Scheduler is quiesced or disabled, don't submit stages.\n")
        SchedulerLogger.Infof(errmsg)
		return SCHED_ERR_QUIESCED
	}

    /*
     * For safety, we need update job schedule state in database before submit
     * the job to the backend. If we persist database after submit request, and
     * then panic before the persist operation, the launched stage will not be
     * recorded in the database. Then it may cause two tasks run at the same time
     * in the cluster, which is a danger of data consistency. It is ugly, but
     * have to do.
     */
    scheduler.PreTrackJobTask(job, "", "",
        stage)
    var syncTime time.Time
    scheduler.perfStats.StartProfileSyncDB(&syncTime)
    if scheduler.asyncTrackTaskInDB {
        /*
         * Trade safety for performance when scheduling tasks in parallel
         */
        scheduler.SyncJobSchedInfoToDB(job, false, false, 0)
    } else {
        scheduler.SyncJobSchedInfoToDB(job, false, true, 0)
    }
    scheduler.perfStats.EndProfileSyncDB(syncTime)

    beScheduler := scheduler.BackendScheduler()

    /*
     * Backend scheduler will try to select a backend to schedule
     * task and return a task ID
     */
    var backendTime time.Time
    scheduler.perfStats.StartProfileBackendSchedule(&backendTime)
    err, backendId, taskId := beScheduler.ScheduleTask(&task)
    scheduler.perfStats.EndProfileBackendSchedule(backendTime)
    if err != nil {
        if err == BE_ERR_NO_BACKEND {
            SchedulerLogger.Infof("No available backend to schedule job %s stage %s\n",
                job.GetID(), stage.GetID())
            scheduler.Quiesce()
            SchedulerLogger.Errorf("Quiesced scheduler for minitues\n")
            return SCHED_ERR_QUIESCED
        } else {
            SchedulerLogger.Errorf("schedule job %s stage %s failure: %s\n",
                job.GetID(), stage.GetID(), err.Error())
            GetDashBoard().UpdateStats(STAT_TASK_SUBMIT_FAIL, 1)
            return err
        }
    } else {
        GetDashBoard().UpdateStats(STAT_TASK_SUBMIT, 1)
        GetDashBoard().UpdateStats(STAT_TASK_TOTAL, 1)
        scheduler.TrackJobTask(job, stage, backendId, taskId)
        scheduler.HandleJobEvent(backendId, taskId, STAGE_SUBMITTED,
            NewEventNotifyInfo("Task Submitted", "", ""))

        scheduler.perfStats.StartProfileSyncDB(&syncTime)
        if scheduler.asyncTrackTaskInDB {
            scheduler.SyncJobSchedInfoToDB(job, false, false, 0)
        } else {
            scheduler.SyncJobSchedInfoToDB(job, false, true, 5)
        }
        scheduler.perfStats.EndProfileSyncDB(syncTime)

        /*logging the schedule information here*/
        volInfo := "vols map: "
        for hostPath, conPath := range volsMap {
            volInfo += fmt.Sprintf("%s:%s ", hostPath, conPath)
        }
        constraintInfo := "constraints: "
        for key, val := range scheduleConstraints {
            constraintInfo += fmt.Sprintf("%s:%s ", key, val)
        }
        labelInfo := "labels: "
        for key, val := range taskLabels {
            labelInfo += fmt.Sprintf("%s:%s ", key, val)
        }
        strMsgFormat := "ScheduleStage(Stage %s/%s): stage name %s, task %s,"
        strMsgFormat += "command %s, cpu %f memory %f gpu %f gpu mem %f disk %f serverType %s, user %s(%s,%s),"
        strMsgFormat += " workdir %s, mode %s, %s, %s, %s\n"
        SchedulerLogger.Infof(strMsgFormat, stage.GetID(), job.GetID(), stage.Name(), taskId, stage.GetCommand(),
            stage.GetCPU(), stage.GetMemory(), gpus, gpuMemory, stage.GetDisk(), stage.GetServerType(),
            taskUser, taskUid, taskGid, workDir, stage.ExecMode(), volInfo, constraintInfo, labelInfo)

        oldState := job.State()
        if job.MarkScheduled() {
            /* the job scheduled first time, notify job manager and user manager to
             * do necessary accounting
             */
            jobEvent := NewJobEvent(JOBEVENTID, job, oldState, JOB_EVENT_SCHEDULED)
            eventbus.Publish(jobEvent)
        }
        scheduler.delayJobChecker.Add(job.GetID(), job)

        return nil
    }
}

func (scheduler *bioScheduler) ScheduleStage(job Job, stage Stage) error {

	if scheduler.Quiesced() || scheduler.Disabled() {
		SchedulerLogger.Infof("Scheduler is quiesced or disabled.\n")
		return SCHED_ERR_QUIESCED
	}

    var err error
    for i := 0; i <= backend.BE_SCHED_RETRY_COUNT; i++ {

        /*Profiling the single stage schedule process*/
        var preTime time.Time
        scheduler.perfStats.StartProfileSchedStage(&preTime)
        err = scheduler._ScheduleStage(job, stage)
        scheduler.perfStats.EndProfileSchedStage(preTime)

        if err == nil || err == SCHED_ERR_PREPARE_EXECUTION {
            break
        } else if scheduler.BackendScheduler().BackendCount() == 1 {
            /*
             * only 1 backend, no need reschedule the job stage
             */
            break
        } else if err == backend.BE_INTERNAL_ERR {
            /*
             * internal error is dangerous, we're not sure if
             * the task is launched or not, just add retry
             * stage count anwyay.
             */
			stage.PrepareRetry()
		}
        /*
         * other backend error worth to retry reschedule
         */
    }

    if err != nil {
        SchedulerLogger.Errorf("ScheduleStage(job %s, stage %s) error: %s",
            job.GetID(), stage.GetID(), err.Error())
    }

    return err
}

/*
 * The schedule state sync operation writes latest state from memory to database. So
 * the same job's db operation could be merged.
 */
func (scheduler *bioScheduler) SyncJobSchedInfoToDB(job Job, insert bool, sync bool,
    timeout int) error {
    if sync {
        /*
         * Noted that the sync operation should not be merged. So provide empty string
         * as merge key.
         */
        return scheduler.jobScheduleStateSyncer.IssueOperation(job.GetID(), job, 
            func(data interface{}) error {
                job := data.(Job)
                scheduler.PersistSchedInfo(job, insert)
                return nil
            }, 
            sync, timeout, "")
    } else {
        /*
         * The async DB sync operation can be merged, so the job id is provided
         * as merge key to reduce all the operations which sync same job's state
         * to database.
         */
        return scheduler.jobScheduleStateSyncer.IssueOperation(job.GetID(), job, 
            func(data interface{}) error {
                job := data.(Job)
                scheduler.PersistSchedInfo(job, insert)
                return nil
            }, 
            sync, timeout, job.GetID())
    }
}

/*save the schedule information to the database*/
func (scheduler *bioScheduler) PersistSchedInfo(job Job, add bool) {
    schedInfo := job.GetScheduleInfo()
    if schedInfo == nil {
        SchedulerLogger.Infof("Job %s doesn't have schedule info, can't persist it\n",
            job.GetID())
        return
    }

    db := GetDBService()
    schedDBInfo := schedInfo.CreateJobScheduleDBInfo()
    var err error = nil
    if add {
        err = db.AddJobScheduleInfo(schedDBInfo)
    } else {
        err = db.UpdateJobScheduleInfo(schedDBInfo)
    }

    if err != nil {
        SchedulerLogger.Errorf("Fail to persist job %s schedule Info to database\n",
            job.GetID())
    } else {
        SchedulerLogger.Debugf("Succeed to persist job %s schedule info to database\n",
            job.GetID())
    }
}

func (scheduler *bioScheduler) UpdateJobFlowGraphStatus(job Job) {
    job.UpdateJobFlowGraphInfo()
}

/*
 * A new task will be submited, task id not known yet, but record the pending stage
 * information.
 */
func (scheduler *bioScheduler) PreTrackJobTask(job Job, backendId string, 
    taskId string, stage Stage) {
    scheduler.jobTracker.TrackBackendTask(job, stage,
        backendId, taskId)
}

/*A new task is started by backend for a stage, just track it in the jobExecInfo*/
func (scheduler *bioScheduler) TrackJobTask(job Job, stage Stage, backendId string,
    taskId string) error {
    scheduler.jobTracker.TrackBackendTask(job, stage,
        backendId, taskId)
    
    /*track task job map*/
    err := scheduler.BackendScheduler().TrackJobTask(backendId, taskId, job)
    if err != nil {
        SchedulerLogger.Errorf("Fail to track job %s backend %s task %s in backend\n",
            job.GetID(), backendId, taskId)
        return err
    }

    return nil
}

/* a task (corresponding to a stage) completed success or failure, remove the
 * task from the schedule information
 */
func (scheduler *bioScheduler) UnTrackJobTask(job Job, stageId string,
    backendId string, taskId string, success bool) error {
    scheduler.jobTracker.UnTrackBackendTask(job, stageId, success)    
    scheduler.BackendScheduler().UnTrackJobTask(backendId, taskId)
    return nil
}

/*remove the schedule information when job complete or failed*/
func (scheduler *bioScheduler) RecycleJobScheduleState(job Job) {
    beScheduler := scheduler.BackendScheduler()
    schedInfo := job.GetScheduleInfo()
    if schedInfo != nil {
        /*
         * Kill all tasks for the pending stages
         */
        stages := schedInfo.GetPendingStageInfos()
        for _, stage := range stages {
            bkId := stage.BackendID()
            err := scheduler.UnTrackJobTask(job, stage.StageID(),
                bkId, stage.TaskID(), false)
            if err != nil {
                SchedulerLogger.Errorf("Can't kill task %s of %s, untrack failure %s\n",
                    stage.TaskID(), bkId, err.Error())
                continue
            }

            err = beScheduler.KillTask(bkId, stage.TaskID())
            if err != nil {
                SchedulerLogger.Errorf("Fail to kill task %s of %s: %s\n",
                    stage.TaskID(), bkId, err.Error())
                GetDashBoard().UpdateStats(STAT_TASK_KILL_FAIL, 1)
            } else {
                SchedulerLogger.Infof("Succeed to kill task %s of %s\n",
                    stage.TaskID(), bkId)
                if stage.TaskID() != "" {
                    GetDashBoard().UpdateStats(STAT_TASK_KILL, 1)
                }
            }
        }
    }
    
    /*
     * After un-track the job tasks, we need re-evaluate the metric of
     * schedule queue to update the task statistics.
     */
    scheduler.jobTracker.EvaluateJobMetric(job, false)
}

func (scheduler *bioScheduler) Stop() {
}

/*resume a paused job*/
func (scheduler *bioScheduler) ResumeJob(job Job) error {
    SchedulerLogger.Infof("The job %s is resumed schedule\n",
        job.GetID())
    scheduler.TryKickScheduleJob(job, false)

    return nil
}

/*
 * Get status of a running job. All the information can be obtained from the 
 * jobExecInfo.
 */
func (scheduler *bioScheduler) GetJobStatus(job Job,
    jobStatus *BioflowJobStatus) error {
    schedInfo := job.GetScheduleInfo()

    if schedInfo == nil {
        return nil
    }

    doneStages, pendingStages := schedInfo.CreateStageSummaryInfo()
    jobStatus.DoneStages = doneStages
    jobStatus.PendingStages = pendingStages
    jobOutput := job.GetOutput()
    if jobOutput != nil {
        jobStatus.JobOutput = jobOutput.Outputs()
    }
    jobStatus.Constraints = job.Constraints()

    waitingJobStages, forbiddenStages := job.GetWaitingStages()
    if waitingJobStages != nil {
        waitingStageInfo := make([]BioflowStageInfo, 0)
        for i := 0; i < len(waitingJobStages); i ++ {
            stage := waitingJobStages[i]
            stageInfo := stage.ToBioflowStageInfo()
            waitingStageInfo = append(waitingStageInfo, *stageInfo)
        }
        jobStatus.WaitingStages = waitingStageInfo
    }

    if forbiddenStages != nil {
        forbiddenStageInfo := make([]BioflowStageInfo, 0)
        for i := 0; i < len(forbiddenStages); i ++ {
            stage := forbiddenStages[i]
            stageInfo := stage.ToBioflowStageInfo()
            forbiddenStageInfo = append(forbiddenStageInfo, *stageInfo)
        }
        jobStatus.ForbiddenStages = forbiddenStageInfo
    }

    return nil
}

func (scheduler *bioScheduler)GetJobLogs(job Job, jobId string,
    stageName string, stageId string) (error, map[string]BioflowStageLog) {

    var schedInfo *JobScheduleInfo = nil
    if job != nil {
        schedInfo = job.GetScheduleInfo()
    }
    jobLogs := make(map[string]BioflowStageLog)

    if schedInfo != nil {
        /*
         * It is a running job, so get all information from in-memory
         * data structures.
         */
        logDir := schedInfo.Job().LogDir()
        jfs := NewJobFileSet(logDir)
        var err error
        if stageId != "*" {
            /* If the stage is still running and we try to fetch its files
             * and then show logs. It intends to let user view the online
             * logs of its long running tasks.
             */
            pendingStageInfoList := schedInfo.GetPendingStageInfos()
            if pendingStageInfoList != nil {
                for _, stageInfo := range pendingStageInfoList {
                    if stageInfo.StageID() == stageId {
                        /*
                         * Fetch sandbox files may cost a long time and user need to wait
                         * fot the download done.

                         * Another risk is that during the download, the task is done and
                         * event handling threads starting downloading the same set of files.
                         * The log files may be corrupt in this situation. Currently we ignore
                         * this risk.
                         * TODO: sync the download with event handling threads
                         */
                        SchedulerLogger.Infof("GetJobLogs(Job %s): start download pending tasks %s/%s logs\n",
                            job.GetID(), stageId, stageInfo.TaskID())
                        scheduler.FetchJobSandboxFiles(stageInfo, "")
                        SchedulerLogger.Infof("GetJobLogs(Job %s): complete download pending tasks %s/%s logs\n",
                            job.GetID(), stageId, stageInfo.TaskID())
                    }
                }
            }

            name, stageTaskIDs := schedInfo.GetStageNameAndTaskInfoById(stageId)
            if name == "" {
                return errors.New("The stage id not exist"), nil
            }
            stageLogs := BioflowStageLog {
                    StageName: name,
            }
            /*Get all log files of the tasks*/
            err, stageLogs.Logs = jfs.ReadTaskLogs(name, stageTaskIDs)
            if err != nil {
                SchedulerLogger.Errorf("ReadTaskLogs for %s/%s/%s failed: %s\n",
                        logDir, name, stageId, err.Error())
                return err, nil
            }

            jobLogs[name] = stageLogs
        } else {
            var allStageNames []string
            if stageName != "*" {
                allStageNames = append(allStageNames, stageName)
            } else {
                allStageNames = schedInfo.GetAllNonWaitingStageNames()
            }

            for _, name := range allStageNames {
                /*check whether already have the logs of the stage name*/
                if _, ok := jobLogs[name]; ok {
                    continue
                }

                stageLogs := BioflowStageLog {
                    StageName: name,
                }
                /*Get all log files of stages with the same name*/
                err, stageLogs.Logs = jfs.ReadStageLogs(name)
                if err != nil {
                    SchedulerLogger.Errorf("ReadStageLogs for %s/%s failed: %s\n",
                        logDir, name, err.Error())
                    return err, nil
                }
            
                jobLogs[name] = stageLogs
            }
        }
    } else {
        db := GetDBService()
        err, jobInfo, schedInfo := db.GetJobHistoryById(jobId)
        if err != nil {
            SchedulerLogger.Errorf("Fail to get job %s history\n",
                jobId)
            return err, nil
        }

        if jobInfo != nil && schedInfo != nil {
            logDir := jobInfo.LogDir
            jfs := NewJobFileSet(logDir)

            execJson := JobExecJSONInfo{}
            err = json.Unmarshal([]byte(schedInfo.ExecJson), &execJson)
            if err != nil {
                SchedulerLogger.Errorf("Failed to decode JSON for schedInfo!\n")
                return err, nil
            }

            for _, stageJsonInfo := range execJson.DoneStages {
                name := stageJsonInfo.Name
                taskIDs := make([]string, 0)
                id := stageJsonInfo.Id
                if stageName != "*" {
                    if name != stageName {
                        continue
                    }
                }
                if stageId != "*" {
                    if stageId != id {
                        continue
                    }
                    taskIDs = append(taskIDs, stageJsonInfo.TaskID)
                }
                /*
                 * get log for failed stage according logdir and stagename
                 */
                stageLogs := BioflowStageLog {
                    StageName: name,
                }

                if _, ok := jobLogs[name]; ok {
                    continue
                }

                if stageId != "*" {
                    SchedulerLogger.Infof("Get log for stagaId: %s\n", id)
                    err, stageLogs.Logs = jfs.ReadTaskLogs(name, taskIDs)
                    if err != nil {
                        SchedulerLogger.Errorf("Fail get stage logs for %s: %s\n",
                            id, err.Error())
                        return err, nil
                    }

                    jobLogs[name] = stageLogs
                } else {
                    SchedulerLogger.Infof("Get log for stage: %s\n", stageName)
                    err, stageLogs.Logs = jfs.ReadStageLogs(name)
                    if err != nil {
                        SchedulerLogger.Errorf("Fail get stage logs for %s: %s \n",
                            name, err.Error())
                        return err, nil
                    }

                    jobLogs[name] = stageLogs
                }

                if stageName != "*" {
                    break
                }
            }
        }
    }

    return nil, jobLogs
}

/* Get status of a job which is already completed. So the inforamtion
 * will be obtained from database.
 */
func (scheduler *bioScheduler)GetHistoryJobStatus(jobId string) (error,
    *BioflowJobStatus) {
    db := GetDBService()

    err, jobInfo, schedInfo := db.GetJobHistoryById(jobId)
    if err != nil {
        SchedulerLogger.Errorf("Fail to get job %s history\n",
            jobId)
        return err, nil
    }
    if jobInfo == nil || schedInfo == nil {
        return errors.New("No history job " + jobId + " found"),
            nil
    }

    execJson := JobExecJSONInfo{}
    err = json.Unmarshal([]byte(schedInfo.ExecJson), &execJson)
    if err != nil {
        return err, nil
    }
    
    pendingStages := make([]BioflowStageInfo, 0)
    for _, stageJsonInfo := range execJson.PendingStages {
        stageInfo := stageJsonInfo.ToBioflowStageInfo()
        pendingStages = append(pendingStages, *stageInfo)
    }

    doneStages := make([]BioflowStageInfo, 0)
    for i := 0; i < len(execJson.DoneStages); i ++ {
        stageJsonInfo := execJson.DoneStages[i]
        stageInfo := stageJsonInfo.ToBioflowStageInfo()
        doneStages = append(doneStages, *stageInfo)
    }

    auxInfo := &jobAuxInfo{}
    auxInfo.FromJSON(jobInfo.AuxInfo)
    jobStatus := &BioflowJobStatus{
        JobId: jobId,
        Name: jobInfo.Name,
        Owner: jobInfo.SecID,
        Pipeline: jobInfo.Pipeline,
        Created: jobInfo.Created,
        WorkDir: jobInfo.WorkDir,
        HDFSWorkDir: auxInfo.HDFSWorkDir,
        JobOutput: auxInfo.JobOutput,
        Finished: jobInfo.Finished,
        State: jobInfo.State,
        PausedState: jobInfo.PausedState,
        ExecMode: auxInfo.ExecMode,
        RetryLimit: jobInfo.RetryLimit,
        StageCount: len(pendingStages) + len(doneStages),
        RunCount: jobInfo.Run,
        FailReason: strings.Split(jobInfo.FailReason, "\n"),
        StageQuota: auxInfo.StageQuota,
    }
    jobStatus.PendingStages = pendingStages
    jobStatus.DoneStages = doneStages

    return nil, jobStatus
}

/*To cancel a running job*/
func (scheduler *bioScheduler) CancelJob(job Job) error {
    scheduler.IssueJobTerminateEvent(job, JOB_EVENT_CANCELED)
    SchedulerLogger.Infof("The job %s is canceled schedule\n",
        job.GetID())
    return nil
}

/*To kill all running tasks of job*/
func (scheduler *bioScheduler) KillJobTasks(job Job, taskId string) error {
    if job == nil {
        return errors.New("Can't kill non-running job tasks")
    }

    beScheduler := scheduler.BackendScheduler()
    schedInfo := job.GetScheduleInfo()
    if schedInfo != nil {
        /*
         * Kill all tasks for the pending stages
         */
        stages := schedInfo.GetPendingStageInfos()
        for _, stage := range stages {
            bkId := stage.BackendID()
            task := stage.TaskID()
            if taskId == "" || taskId == "*" || taskId == task {
                err := beScheduler.KillTask(bkId, task)
                if err != nil {
                    SchedulerLogger.Errorf("KillJobTasks(job %s): Fail to kill task %s/%s: %s\n",
                        job.GetID(), task, bkId, err.Error())
                    GetDashBoard().UpdateStats(STAT_TASK_KILL_FAIL, 1)
                } else {
                    SchedulerLogger.Infof("KillJobTasks(job %s): Succeed to kill task %s/%s\n",
                        job.GetID(), task, bkId)
                    if task != "" {
                        GetDashBoard().UpdateStats(STAT_TASK_KILL, 1)
                    }
                }
            }
        }
        return nil
    } else {
        return errors.New("Can't kill non-running job tasks")
    }
}

/*
 * If a task in backend completed, the docker scheduler (e.g, eremetic) will
 * notify bioflow via REST callback. Then the REST server will call scheduler's
 * Task notify handler to handle it.
 */
func(scheduler *bioScheduler) HandleTaskNotify(backendID string,
    callbackMsg io.ReadCloser) error {
    err, taskEvent := scheduler.BackendScheduler().ParseTaskNotify(backendID,
        callbackMsg)
    if err != nil {
        SchedulerLogger.Errorf("Backend can't parse notify: %s\n",
            err.Error())
        return err
    }
    taskID := taskEvent.Id
    event := taskEvent.Event
    info := NewEventNotifyInfo(taskEvent.Info, taskEvent.HostName,
            taskEvent.AgentIP)

    switch event {
        case backend.TASK_FINISHED:
            scheduler.HandleJobEvent(backendID, taskID, STAGE_DONE, info)
            SchedulerLogger.Debugf("Notified task %s finished \n", taskID)
        case backend.TASK_FAIL, backend.TASK_GONE, backend.TASK_DROPPED:
            scheduler.HandleJobEvent(backendID, taskID, STAGE_FAIL, info)
            SchedulerLogger.Debugf("Notified task %s failed \n", taskID)
        case backend.TASK_KILLED:
            scheduler.HandleJobEvent(backendID, taskID, STAGE_FAIL, info)
            SchedulerLogger.Debugf("Notified task %s killed \n", taskID)
        case backend.TASK_ERROR:
            scheduler.HandleJobEvent(backendID, taskID, STAGE_FAIL, info)
            SchedulerLogger.Debugf("Notified task %s error \n", taskID)
        case backend.TASK_LOST:
            scheduler.HandleJobEvent(backendID, taskID, STAGE_LOST, info)
            SchedulerLogger.Debugf("Notified task %s lost \n", taskID)
        case backend.TASK_UNKNOWN:
            scheduler.HandleJobEvent(backendID, taskID, STAGE_LOST, info)
            SchedulerLogger.Debugf("Notified task %s unknown, handled as lost \n",
                taskID)
        case backend.TASK_RUNNING:
            scheduler.HandleJobEvent(backendID, taskID, STAGE_RUNNING, info)
            SchedulerLogger.Debugf("Notified task %s running \n", taskID)
        default:
            SchedulerLogger.Errorf("Notified task %s state unknown %d \n",
                taskID, event)
    }

    return nil
}

func (scheduler *bioScheduler) GetStats() (error, *BioflowSchedulerStats) {
    /*Get job scheduler status*/
    state := "Normal"
    if scheduler.Disabled() {
        state = "Disabled"
    } else if scheduler.Quiesced() {
        state = "Quiesced"
    }

    /*Get backend list and status*/
    err, backends := scheduler.BackendScheduler().GetBackendListInfo()
    if err != nil {
        SchedulerLogger.Errorf("Fail to list backend info: %s\n",
            err.Error())
        return err, nil
    }

    /*Get job schedule statistics from tracker*/
    jobStats := scheduler.jobTracker.GetJobStats()
    jobSchedulePerfStats := scheduler.jobTracker.GetSchedulePerfStats()
    schedulePerfStats := scheduler.perfStats.ToBioflowSchedulePerfStats()

    /*Collect stats for schedule workers group*/
    var scheduleWorkerStats []BioflowWorkerGroupStats
    var totalQueueLen float64 = 0
    var totalMergedCount uint64 = 0
    for _, worker := range scheduler.jobScheduleWorkers {
        workerStats := worker.Stats()
        totalQueueLen = 0
        for _, len := range workerStats.QueueLens {
            totalQueueLen += len
        }
        totalMergedCount = 0
        for _, count := range workerStats.MergedCounts {
            totalMergedCount += count
        }
        scheduleWorkerStats = append(scheduleWorkerStats,
            BioflowWorkerGroupStats {
                TotalQueueLen: totalQueueLen,
                QueueLens: workerStats.QueueLens,
                Mean: workerStats.Mean,
                Median: workerStats.Median,
                StdVariance: workerStats.StdVariance,
                MergedCounts: workerStats.MergedCounts,
                TotalMergedCount: totalMergedCount,
            })
    }

    dbSyncStats := scheduler.jobScheduleStateSyncer.Stats()
    totalQueueLen = 0
    for _, len := range dbSyncStats.QueueLens {
        totalQueueLen += len
    }
    totalMergedCount = 0
    for _, count := range dbSyncStats.MergedCounts {
        totalMergedCount += count
    }
    dbWorkerStats := BioflowWorkerGroupStats {
        TotalQueueLen: totalQueueLen,
        QueueLens: dbSyncStats.QueueLens,
        Mean: dbSyncStats.Mean,
        Median: dbSyncStats.Median,
        StdVariance: dbSyncStats.StdVariance,
        MergedCounts: dbSyncStats.MergedCounts,
        TotalMergedCount: totalMergedCount,
    }

    stageBatcherStats := scheduler.batcher.Stats()
    sBatcherStats := BioflowStageBatcherStats{
        QuotaInPriority: stageBatcherStats.PriorityQuota,
        Allocated: stageBatcherStats.TotalAllocation,
        PendingJobs: stageBatcherStats.PendingJobs,
    }

    stats := &BioflowSchedulerStats {
        State: state,
        Backends: backends,
        JobStats: jobStats,
        PerfStats: *jobSchedulePerfStats,
        ScheduleStats: *schedulePerfStats,
        PendingAsyncEventCount: scheduler.pendingAsyncEventCount,
        ScheduleWorkerStats: scheduleWorkerStats,
        DBWorkerStats: dbWorkerStats,
        StageBatcherStats: sBatcherStats,
    }

    return nil, stats
}

func (scheduler *bioScheduler) UpdateJobPriority(job Job, oldPri int) error {
    scheduler.batcher.UpdateJobPriority(job, oldPri)
    return scheduler.jobTracker.UpdateJobPriority(job, oldPri)
}

/*
 * Do profiling and analyze the stats result for a running task
 * of a stage
 */
func (scheduler *bioScheduler) GetJobTaskResourceUsageInfo(job Job, jobId string,
    taskId string) (error, map[string]ResourceUsageInfo) {
    /*
     * If job is running and the task is a running task, we need fetch
     * the latest profiling files first
     */
    logDir := ""
    if job != nil {
        schedInfo := job.GetScheduleInfo()
        if schedInfo != nil {
            /*
             * It is a running job, so get all information from in-memory
             * data structures.
             */
            stageInfoList := schedInfo.GetPendingStageInfos()
            if stageInfoList != nil {
                for _, stageInfo := range stageInfoList {
                    if stageInfo.TaskID() == taskId {
                        scheduler.FetchJobSandboxFiles(stageInfo, backend.SANDBOX_PROFILING)
                    }
                }
            }
        }
        logDir = job.LogDir()
    } else {
        db := GetDBService()
        err, jobInfo, _ := db.GetJobHistoryById(jobId)
        if err != nil {
            SchedulerLogger.Errorf("Fail to get job %s history\n",
                jobId)
            return err, nil
        }

        logDir = jobInfo.LogDir
    }
    
    rscUsageInfo, err := GetDetailTaskResourceUsageInfo(logDir, taskId)

    return err, rscUsageInfo
}

func (scheduler *bioScheduler) DumpStageBatcherInfo() {
    if scheduler.batcher != nil {
        scheduler.batcher.DumpInfo()
    }
}

var bioMainScheduler *bioScheduler = nil

func GetScheduler() *bioScheduler {
    return bioMainScheduler
}

func CreateScheduler(callback string, jobTracker *jobTracker,
    config *JobSchedulerConfig) Scheduler {
    bioMainScheduler = NewBIOScheduler(callback, jobTracker,
        config)

    return bioMainScheduler
}
