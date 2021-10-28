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
    "sync"
    "time"
    "encoding/json"
    "errors"
    . "github.com/xtao/bioflow/dbservice"
    . "github.com/xtao/bioflow/common"
    . "github.com/xtao/bioflow/message"
)

const (
    INIT int = 0
    PREPARING int = 1
    SCHEDULING int = 2
    CLEANUPING int = 3
    RESTORING int = 4
    RIGHTCONTROL int = 5
)

func JobExecStateToString(state int) string {
    switch state {
        case INIT:
            return "INIT"
        case PREPARING:
            return "PREPARING"
        case SCHEDULING:
            return "SCHEDULING"
        case CLEANUPING:
            return "CLEANUPING"
        case RESTORING:
            return "RESTORING"
        case RIGHTCONTROL:
            return "RIGHTCONTROL"
        default:
            return "INVALID"
    }
}


/*
 * data structure to track backend and task
 * information for a job stage
 */
type StageExecInfo struct {
    /*
     * In normal case, each StageExecInfo should be associated
     * with a stage. But for some graph scheduler (e.g, wom),
     * we may not find a built stage for it. Then we need save
     * the stageId here.
     */
    stage Stage
    stageId string

    /*runtime task information*/
    backendId string
    taskId string

    /*
     * when stage completed (e.g, fail, lost, done)
     * save the stage json info here as a snapshot
     * of that time. So the exec info can save the
     * complete history of stages, even when some stage
     * are retried many times with different parameters.
     */
    snapshotStageInfo *StageJSONInfo

    /*
     * Hang detection information:
     * if a task running or queued too long,
     * it will be marked hang. The scheduler
     * may query this information to handle it
     */
    taskHang bool
}

func (stageInfo *StageExecInfo) Stage() Stage {
    return stageInfo.stage
}

/*Retrieve the stage id of the exec info*/
func (stageInfo *StageExecInfo) StageID() string {
    if stageInfo.stage != nil {
        /* For pending stages which has the stage or can
         * find the stage by the stored id  during recovery
         */
        return stageInfo.stage.GetID()
    }
    
    if stageInfo.snapshotStageInfo != nil {
        /*
         * For done stages
         */
        return stageInfo.snapshotStageInfo.Id
    }
    
    /*
     * For the pending stages which can't be recovered
     * the stage from wom graph scheduler during recovery
     */
    return stageInfo.stageId
}

/*Retrieve the stage name from the exec info*/
func (stageInfo *StageExecInfo) StageName() string {
    /*all the pending stages have stage pointer*/
    if stage := stageInfo.stage; stage != nil {
        return stage.Name()
    }

    /*
     * the done stages' name can be fetched from snapshot info
     */
    if stageInfo.snapshotStageInfo != nil {
        return stageInfo.snapshotStageInfo.Name
    }

    return ""
}

func (stageInfo *StageExecInfo) TaskID() string {
    return stageInfo.taskId
}

func (stageInfo *StageExecInfo) BackendID() string {
    return stageInfo.backendId
}

func (stageInfo *StageExecInfo) SetTaskHang(hang bool) {
    stageInfo.taskHang = hang
}

func (stageInfo *StageExecInfo) SnapshotStageInfo() *StageJSONInfo {
    return stageInfo.snapshotStageInfo
}

func (stageInfo *StageExecInfo) IsTaskHang() bool {
    return stageInfo.taskHang
}

func (stageInfo *StageExecInfo) BuildStageSnapshotInfo() *StageJSONInfo{
    stage := stageInfo.stage
    if stage == nil {
        return &StageJSONInfo{}
    }

    scheduleTime := stage.ScheduledTime().Format(time.UnixDate)
    if StageIsNotStarted(stage.State()) {
        scheduleTime = "N/A"
    }
    finishTime := "N/A"
    var totalDuration float64 = -1
    if StageIsTerminal(stage.State()) {
        finishTime = stage.FinishTime().Format(time.UnixDate)
        totalDuration = stage.TotalDuration()
    }
    return &StageJSONInfo {
            Id: stage.GetID(),
            Name: stage.Name(),
            State: stage.State(),
            Command: stage.GetCommand(),
            Output: stage.GetTargetOutputs(),
            RetryCount: stage.RetryCount(),
            CPU: stage.GetCPU(),
            Memory: stage.GetMemory(),
            GPU: stage.GetGPU(),
            GPUMemory: stage.GetGPUMemory(),
            Disk: stage.GetDisk(),
            ServerType: stage.GetServerType(),
            SubmitTime: stage.SubmitTime().Format(time.UnixDate),
            ScheduledTime: scheduleTime,
            FinishTime: finishTime,
            QueueDuration: stage.QueueDuration(),
            TotalDuration: totalDuration,
            BackendID: stageInfo.backendId,
            TaskID: stageInfo.taskId,
            FailReason: stage.FailReason(),
            ExecMode: stage.ExecMode(),
            HostName: stage.HostName(),
            HostIP: stage.IP(),
            ResourceStats: stage.ResourceStats(),
    }
}

func (stageInfo *StageExecInfo) SaveStageSnapshotInfo() {
    stageInfo.snapshotStageInfo = stageInfo.BuildStageSnapshotInfo()
}

func (stageInfo *StageExecInfo) ToJSONInfo() *StageJSONInfo {
    if stageInfo.snapshotStageInfo != nil {
        return stageInfo.snapshotStageInfo
    } else {
        return stageInfo.BuildStageSnapshotInfo()
    }
}

func (stageInfo *StageExecInfo) ToBioflowStageInfo() *BioflowStageInfo{
    stage := stageInfo.stage
    snapshotInfo := stageInfo.snapshotStageInfo
    if snapshotInfo != nil {
        return snapshotInfo.ToBioflowStageInfo()
    } else {
        bioflowStageInfo := stage.ToBioflowStageInfo()
        bioflowStageInfo.BackendId = stageInfo.backendId
        bioflowStageInfo.TaskId = stageInfo.taskId
        return bioflowStageInfo
    }
}

/* 
 * JobExecInfo tracks the execution state information of 
 * a job. Scheduler tracks job's runtime state in two places:
 * 1) job.flowGraph: tracks all stages and their dependency
 *    information. It can be re-generated by job dataset and
 *    pipeline. If bioflow panic, it can re-build the flow graph
 *    from job input dataset and pipeline. so it is not saved to
 *    the database.
 * 2) JobExecInfo: tracks the done stages and running stages
 *    of the job. for each stage, it also tracks the schedule
 *    backend task inforamtion. It can be used to check job
 *    task state.  The JobExecInfo will be persist to database
 *    as snapshot. scheduler will recover it from database after
 *    panic.
 *
 */
type JobExecInfo struct {
   execPlan string
   execState int
   doneStages []*StageExecInfo
   pendingStages map[string]*StageExecInfo
}

type JobExecJSONInfo struct {
    ExecPlan string
    ExecState int
    DoneStages []StageJSONInfo
    PendingStages map[string]StageJSONInfo
}

func newJobExecInfo() *JobExecInfo {
    execInfo := &JobExecInfo {
        doneStages: make([]*StageExecInfo, 0),
        pendingStages: make(map[string]*StageExecInfo),
    }

    return execInfo
}

func (execInfo *JobExecInfo) DoneStages() []*StageExecInfo {
    return execInfo.doneStages
}

func (execInfo *JobExecInfo) PendingStages() map[string]*StageExecInfo {
    return execInfo.pendingStages
}

func (execInfo *JobExecInfo) DeletePendingStageInfo(stageId string) {
    delete(execInfo.pendingStages, stageId)
}

func (execInfo *JobExecInfo) GetExecState() int {
    return execInfo.execState
}

func (execInfo *JobExecInfo) SetExecState(state int) {
    execInfo.execState = state
}

func (execInfo *JobExecInfo) ToJSON() (error, string){
    execJson := &JobExecJSONInfo {
        DoneStages: make([]StageJSONInfo, 0),
        PendingStages: make(map[string]StageJSONInfo),
        ExecState: execInfo.execState,
    }

    for stageId, stageInfo := range execInfo.pendingStages {
        jsonInfo := stageInfo.ToJSONInfo()
        if jsonInfo == nil {
            return errors.New("Fail to create stage JSON"), ""
        }
        execJson.PendingStages[stageId] = *jsonInfo
    }

    for i := 0; i < len(execInfo.doneStages); i ++ {
        stageInfo := execInfo.doneStages[i]
        execJson.DoneStages = append(execJson.DoneStages, *stageInfo.ToJSONInfo())
    }

    body, err := json.Marshal(&execJson)
    if err != nil {
        Logger.Println(err)
        return err, ""
    }
    return nil, string(body)
}

/*
 * Exec Info should support recover from database step by step.
 * Because some stages may not be built yet when restoring the
 * graph.
 */
func (execInfo *JobExecInfo) FromJSON(job Job, jsonData string,
    partialRecover bool, allowMissStage bool) error {
    execJson := JobExecJSONInfo{}
    err := json.Unmarshal([]byte(jsonData), &execJson)
    if err != nil {
        return err
    }

    execInfo.execState = execJson.ExecState

    for stageId, stageJsonInfo := range execJson.PendingStages {
        /*check whether it is already restored*/
        if _, ok := execInfo.pendingStages[stageId]; ok {
            continue
        }

        stage := job.GetStage(stageId)
        if stage == nil {
            if partialRecover {
                continue
            } else if !allowMissStage {
                SchedulerLogger.Errorf("restore stage %s of job %s from json %s fail\n",
                    stageId, job.GetID(), jsonData)
                return errors.New("Invalid stageID")
            }
        }

        stageInfo := &StageExecInfo {
            stage: stage,
            stageId: stageId,
            backendId: stageJsonInfo.BackendID,
            taskId: stageJsonInfo.TaskID,
            snapshotStageInfo: nil,
        }

        /*resotre execution information for pending stage*/
        if stage != nil {
            stage.RestoreExecutionState(&stageJsonInfo, true)
        } else {
            SchedulerLogger.Errorf("The pending stage %s of job %s miss sync with graph scheduler, ignore it\n",
                stageId, job.GetID())
        }

        execInfo.pendingStages[stageId] = stageInfo
    }

ResotreDoneStages:
    for i := 0; i < len(execJson.DoneStages); i ++ {
        stageJsonInfo := execJson.DoneStages[i]
        /* 
         * Check whether the stage is already restored, need consider the
         * following complicated cases:
         * 1) a stage may be lost or fail for many times, so it has many
         *    snapshot info in the done stages list. They are only distinguished
         *    by retry count.
         */
        for j := 0; j < len(execInfo.doneStages); j ++ {
            if stageJsonInfo.Id == execInfo.doneStages[j].StageID() && 
                stageJsonInfo.RetryCount == execInfo.doneStages[j].snapshotStageInfo.RetryCount {
                continue ResotreDoneStages
            }
        }

        stage := job.GetStage(stageJsonInfo.Id)
        if stage == nil {
            if partialRecover {
                continue
            } else if !allowMissStage {
                SchedulerLogger.Errorf("Can't restore stage %s of job %s from json %s\n",
                    stageJsonInfo.Id, job.GetID(), jsonData)
                return errors.New("Invalid stageID")
            }
        }

        stageInfo := &StageExecInfo {
            stage: stage,
            backendId: stageJsonInfo.BackendID,
            taskId: stageJsonInfo.TaskID,
            snapshotStageInfo: &stageJsonInfo,
        }

        /*resotre execution information for done stage*/
        if stage != nil {
            stage.RestoreExecutionState(&stageJsonInfo, false)
        } else {
            SchedulerLogger.Errorf("The done stage %s of job %s miss sync with graph scheduler, ignore it\n",
                stageJsonInfo.Id, job.GetID())
        }
        execInfo.doneStages = append(execInfo.doneStages, stageInfo)
    }

    return nil
}

/*
 * track job schedule metric
 */
type JobScheduleMetric struct {
    MaxTaskQueueTime float64
    MaxTaskRunningTime float64
    QueuedTasks int64
    RunningTasks int64
    TotalQueueTime float64
    TotalRunningTime float64
}

type JobScheduleInfo struct {
    lock sync.Mutex
    job Job

    /*
     * schedule state information, need be persist
     * to database
     */
    flowGraphInfo FlowGraphInfo
    execInfo *JobExecInfo

    /*
     * Job schedule metric data, needn't
     * be persist to database. will be 
     * re-built when job recovered.
     */
    metric *JobScheduleMetric
}

func NewJobScheduleInfo(job Job) *JobScheduleInfo {
    jobFlowGraphInfo, _ := job.CreateFlowGraphInfo()
    schedInfo := &JobScheduleInfo{
        job: job,
        flowGraphInfo: jobFlowGraphInfo,
        metric: &JobScheduleMetric{
            MaxTaskQueueTime: 0,
            QueuedTasks: 0,
            RunningTasks: 0,
            TotalQueueTime: 0,
        },
    }

    schedInfo.execInfo = newJobExecInfo()
    return schedInfo
}

func (schedInfo *JobScheduleInfo) ExecInfo() *JobExecInfo {
    return schedInfo.execInfo
}

func (schedInfo *JobScheduleInfo) Job() Job {
    return schedInfo.job
}

func (schedInfo *JobScheduleInfo) Metric() *JobScheduleMetric {
    return schedInfo.metric
}

func (schedInfo *JobScheduleInfo) SetMetric(metric *JobScheduleMetric) {
    schedInfo.metric = metric
}

func (schedInfo *JobScheduleInfo) AddPendingStage(stage Stage, backendId string, taskId string) {
    schedInfo.lock.Lock()
    defer schedInfo.lock.Unlock()

    stageExecInfo := &StageExecInfo {
        stage: stage,
        backendId: backendId,
        taskId: taskId,
        taskHang: false,
        snapshotStageInfo: nil,
    }

    schedInfo.execInfo.pendingStages[stage.GetID()] = stageExecInfo
}

func (schedInfo *JobScheduleInfo) CompletePendingStage(stageId string, success bool) {
    schedInfo.lock.Lock()
    defer schedInfo.lock.Unlock()

    if stageInfo, ok := schedInfo.execInfo.pendingStages[stageId]; ok {
        stageInfo.SetTaskHang(false)
        /*
         * task complete (no matter success or fail), save this time
         * execution info of the stage to the done stages as history
         */
        stageInfo.SaveStageSnapshotInfo()
        schedInfo.execInfo.doneStages = append(schedInfo.execInfo.doneStages,
            stageInfo)
        delete(schedInfo.execInfo.pendingStages, stageId)
    }
}

func (schedInfo *JobScheduleInfo) DeletePendingStage(stageId string) {
    schedInfo.lock.Lock()
    defer schedInfo.lock.Unlock()
    delete(schedInfo.execInfo.pendingStages, stageId)
}

func (schedInfo *JobScheduleInfo) GetStageOfTask(backendId string, taskId string) (string, *StageExecInfo) {
    schedInfo.lock.Lock()
    defer schedInfo.lock.Unlock()

    for stageId, stageInfo := range schedInfo.execInfo.pendingStages {
        if stageInfo.backendId == backendId && stageInfo.taskId == taskId {
            return stageId, stageInfo
        }
    }

    return "", nil
}

func (schedInfo *JobScheduleInfo) GetPendingStageInfos() []*StageExecInfo {
    schedInfo.lock.Lock()
    defer schedInfo.lock.Unlock()

    stages := make([]*StageExecInfo, 0)
    for _, stage := range schedInfo.execInfo.pendingStages {
        stages = append(stages, stage)
    }
    return stages
}

func (schedInfo *JobScheduleInfo) PendingStageCount() int {
    return len(schedInfo.execInfo.pendingStages)
}

func (schedInfo *JobScheduleInfo) GetPendingStageInfoById(stageId string) *StageExecInfo {
    schedInfo.lock.Lock()
    defer schedInfo.lock.Unlock()

    if stage, found := schedInfo.execInfo.pendingStages[stageId]; found {
        return stage
    }

    return nil
}


func (schedInfo *JobScheduleInfo) GetDoneStageInfoById(stageId string) []*StageExecInfo {
    schedInfo.lock.Lock()
    defer schedInfo.lock.Unlock()

    stages := make([]*StageExecInfo, 0)
    for  _, stageJsonInfo := range schedInfo.execInfo.doneStages {
        if stageJsonInfo.StageID() == stageId {
            stages = append(stages, stageJsonInfo)
        }
    }
    return stages
}

func (schedInfo *JobScheduleInfo) GetHangStageInfos() []*StageExecInfo {
    schedInfo.lock.Lock()
    defer schedInfo.lock.Unlock()

    stages := make([]*StageExecInfo, 0)
    for _, stage := range schedInfo.execInfo.pendingStages {
        if stage.IsTaskHang() {
            stages = append(stages, stage)
        }
    }
    return stages
}


func (schedInfo *JobScheduleInfo) GetPendingStages() []Stage {
    schedInfo.lock.Lock()
    defer schedInfo.lock.Unlock()

    stages := make([]Stage, 0)
    for _ , stageInfo := range schedInfo.execInfo.pendingStages {
        if stageInfo.stage != nil {
            stages = append(stages, stageInfo.stage)
        }
    }

    return stages
}

func (schedInfo *JobScheduleInfo) GetPendingTasks() []string {
    schedInfo.lock.Lock()
    defer schedInfo.lock.Unlock()

    tasks := make([]string, 0, len(schedInfo.execInfo.pendingStages))
    for _, execInfo := range schedInfo.execInfo.pendingStages {
        tasks = append(tasks, execInfo.taskId)
    }

    return tasks
}

func (schedInfo *JobScheduleInfo) GetDoneStages() []Stage {
    schedInfo.lock.Lock()
    defer schedInfo.lock.Unlock()

    stages := make([]Stage, 0)
    for _, stageInfo := range schedInfo.execInfo.doneStages {
        if stageInfo.stage != nil {
            stages = append(stages, stageInfo.stage)
        }
    }

    return stages
}

func (schedInfo *JobScheduleInfo) GetDoneStageInfos() []*StageExecInfo {
    schedInfo.lock.Lock()
    defer schedInfo.lock.Unlock()

    stageInfos := make([]*StageExecInfo, 0)
    stageInfos = append(stageInfos, schedInfo.execInfo.doneStages ...)
    return stageInfos
}


/*
 * Get all the done or pending stages' name of a executing job.
 */
func (schedInfo *JobScheduleInfo) GetAllNonWaitingStageNames() []string {
    var allStageNames []string
    pendingStages := schedInfo.GetPendingStageInfos()
    for _, stageInfo := range pendingStages {
        allStageNames = append(allStageNames, stageInfo.StageName())
    }
    doneStages := schedInfo.GetDoneStageInfos()
    for _, stageInfo := range doneStages {
        allStageNames = append(allStageNames, stageInfo.StageName())
    }

    return allStageNames
}

func (schedInfo *JobScheduleInfo) GetHangStageSummaryInfo() ([]BioflowStageInfo) {
    schedInfo.lock.Lock()
    defer schedInfo.lock.Unlock()

    hangStages := make([]BioflowStageInfo, 0)
    for _, stageExecInfo := range schedInfo.execInfo.pendingStages {
        if !stageExecInfo.IsTaskHang() {
            continue
        }

        stage := stageExecInfo.stage
        stageInfo := stage.ToBioflowStageInfo()
        if stageInfo != nil {
            stageInfo.BackendId = stageExecInfo.backendId
            stageInfo.TaskId = stageExecInfo.taskId
            hangStages = append(hangStages, *stageInfo)
        } else {
            /*simply ignore the allocation failure*/
            SchedulerLogger.Error("Can't allocate meomory when create hang stages summary\n")
        }
    }

    return hangStages
}

func (schedInfo *JobScheduleInfo) CreateStageSummaryInfo() ([]BioflowStageInfo, []BioflowStageInfo) {
    schedInfo.lock.Lock()
    defer schedInfo.lock.Unlock()

    doneStages := make([]BioflowStageInfo, 0)
    for i := 0; i < len(schedInfo.execInfo.doneStages); i ++ {
        stageExecInfo := schedInfo.execInfo.doneStages[i]
        stageInfo := stageExecInfo.ToBioflowStageInfo()
        doneStages = append(doneStages, *stageInfo)
    }

    pendingStages := make([]BioflowStageInfo, 0)
    for _, stageExecInfo := range schedInfo.execInfo.pendingStages {
        stageInfo := stageExecInfo.ToBioflowStageInfo()
        pendingStages = append(pendingStages, *stageInfo)
    }

    return doneStages, pendingStages
}

func (schedInfo *JobScheduleInfo) CreateJobScheduleDBInfo() *JobScheduleDBInfo {
    schedInfo.lock.Lock()
    defer schedInfo.lock.Unlock()

    jobInfo := &JobScheduleDBInfo{
            Id: schedInfo.Job().GetID(),
    }

    err, jsonInfo := schedInfo.execInfo.ToJSON()
    if err != nil {
        SchedulerLogger.Errorf("Fail to create exec JSON: %s\n",
            err.Error())
        return nil
    }
    jobInfo.ExecJson = jsonInfo

    if schedInfo.flowGraphInfo == nil {
        flowGraphInfo, err:= schedInfo.Job().CreateFlowGraphInfo()
        if err != nil {
            SchedulerLogger.Errorf("Fail to create flowGraphInfo: %s\n",
                err.Error())
            return nil
        }
        if flowGraphInfo == nil {
            SchedulerLogger.Debugf("No pipeline init to job!")
            return jobInfo
        }
        schedInfo.flowGraphInfo = flowGraphInfo
    }

    err, graphInfo := schedInfo.flowGraphInfo.FlowGraphInfoToJSON()
    if err != nil {
        SchedulerLogger.Errorf("Fail to create flow graph JSON:%s\n",
            err.Error())
        return nil
    }
    jobInfo.GraphInfo = graphInfo

    return jobInfo
}

func (schedInfo *JobScheduleInfo) RecoverFromDB(jobInfo *JobScheduleDBInfo,
    partialRecover bool, allowMissStage bool) error {
    if schedInfo.flowGraphInfo == nil {
        flowGraphInfo, err:= schedInfo.Job().CreateFlowGraphInfo()
        if err != nil {
            SchedulerLogger.Errorf("Fail to create flowGraphInfo: %s\n",
                err.Error())
            return nil
        }
        schedInfo.flowGraphInfo = flowGraphInfo
    }
    schedInfo.flowGraphInfo.FlowGraphInfoFromJSON(jobInfo.GraphInfo)
    execInfo := schedInfo.execInfo
    if execInfo == nil {
        execInfo = newJobExecInfo()
    }
    err := execInfo.FromJSON(schedInfo.Job(),
        jobInfo.ExecJson, partialRecover, allowMissStage)
    if err != nil {
        SchedulerLogger.Errorf("Recover exec info failure: %s\n",
            err.Error())
        return err
    }
    schedInfo.execInfo = execInfo

    return nil
}


func (schedInfo *JobScheduleInfo) EvaluateMetric(metric *JobScheduleMetric,
    maxAllowTaskTimeout float64) {
    pendingStages := schedInfo.GetPendingStageInfos()
    metric.MaxTaskQueueTime = 0
    metric.RunningTasks = 0
    metric.QueuedTasks = 0
    metric.TotalQueueTime = 0
    if pendingStages == nil || len(pendingStages) == 0 {
        return 
    }

    for i := 0; i < len(pendingStages); i ++ {
        stageInfo := pendingStages[i]
        stage := stageInfo.stage
        state := stage.State()

        switch state {
            case STAGE_QUEUED, STAGE_SUBMITTED:
                queueTime := stage.QueueDurationTillNow()
                if queueTime > maxAllowTaskTimeout {
                    stageInfo.SetTaskHang(true)
                }
                metric.QueuedTasks ++
                metric.TotalQueueTime += queueTime
                if queueTime > metric.MaxTaskQueueTime {
                    metric.MaxTaskQueueTime = queueTime
                }
            case STAGE_RUNNING:
                metric.RunningTasks ++
                runTime := stage.RunDurationTillNow()
                if runTime > maxAllowTaskTimeout {
                    stageInfo.SetTaskHang(true)
                }
        }
    }
}

func (schedInfo *JobScheduleInfo) CheckPendingOrHangTasks() (bool, bool) {
    schedInfo.lock.Lock()
    defer schedInfo.lock.Unlock()

    hasPending := false
    hasHang := false
    for _, stage := range schedInfo.execInfo.pendingStages {
        hasPending = true
        if stage.IsTaskHang() {
            hasHang = true
            break
        }
    }

    return hasPending, hasHang
}

func (schedInfo *JobScheduleInfo) GetStageNameAndTaskInfoById(id string) (string, []string) {
    var taskIDs []string
    stageName := ""
    doneStageInfos := schedInfo.GetDoneStageInfoById(id)
    if len(doneStageInfos) != 0 {
        for _, v := range doneStageInfos {
            taskIDs = append(taskIDs, v.taskId)
            if stageName == "" {
                stageName = v.StageName()
            }
        }
	}

    pendingStageInfo := schedInfo.GetPendingStageInfoById(id)
    if pendingStageInfo != nil {
        taskIDs = append(taskIDs, pendingStageInfo.taskId)
        if stageName == "" {
            stageName = pendingStageInfo.StageName()
        }
    }

    return stageName, taskIDs
}

func (schedInfo *JobScheduleInfo)SetGraphInfo(info FlowGraphInfo) {
    schedInfo.flowGraphInfo = info
}
