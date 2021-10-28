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
    "time"
    "errors"
    "sync"
    "fmt"
    "strings"
    "encoding/json"
    
    "github.com/xtao/bioflow/dbservice"
    . "github.com/xtao/bioflow/common"
    . "github.com/xtao/bioflow/message"
    . "github.com/xtao/bioflow/scheduler/common"
    "github.com/xtao/bioflow/scheduler/graphscheduler"
    "github.com/xtao/bioflow/scheduler/dataprovider"
    "github.com/xtao/bioflow/scheduler/asyncOffloadMgr"
    xcommon "github.com/xtao/xstone/common"
    "github.com/xtao/bioflow/storage"
    . "github.com/xtao/bioflow/scheduler/graphscheduler/bl"
    . "github.com/xtao/bioflow/scheduler/graphscheduler/wom"
    . "github.com/xtao/bioflow/debug/faultinject"
)

type JobExecEvent struct {
    stage Stage
    event uint32
    lastStageState uint32
}


func (execEvent *JobExecEvent)String() string {
    strEvent := "Event("
    strEvent += JobExecEventToString(execEvent.event)
    if execEvent.stage != nil {
        strEvent += "," + execEvent.stage.GetID()
    } else {
        strEvent += ",NoStage"
    }
    strEvent += "," + StageStateToStr(execEvent.lastStageState)
    strEvent += ")"
    return strEvent
}

type PendingWork func()(error, ScheduleAction)

type bioJob struct {
    lock sync.Mutex
    id   *JobID
    state int
    priority int
    lastState int
    stageRetryLimit int
    name string
    description string
    pipelineName string
    pipeline Pipeline
    stageIndex int
    workDir string
    hdfsWorkDir string
    logDir string
    inputDataSet *BioDataSet
    smId string
    disableOOMKill bool
    failReason []string
    
    createTime time.Time
    finishTime time.Time
    run int

    /*security information*/
    secID string
    secCtxt *SecurityContext

    /*runtime schedule information*/
    schedInfo *JobScheduleInfo

    /*the time last update job's schedule info*/
    lastUpdateTime time.Time

    /*
     * Graph scheduler to build and schedule the job flow graph
     */
    graphScheduler GraphScheduler

    /*
     * tracking information to sync schedule operations:
     * 1) schedule
     * 2) cleanup
     */
    inprogressScheduleOps int
    pendingTerminateOp bool
    scheduleOpData JobScheduleOpData

    /*
     * How to run the job in backend:
     * 1) DOCKER: run the command in a docker container
     * 2) HOST: run the command as a host process
     */
    execMode string

    dataProvider DataProvider
    dataDir string

    /*
     * Only allow one thread to build job graph anytime. So when
     * many threads decide to build the graph, only one thread win,
     * the others should wait
     */
    buildWaiter *xcommon.TimedWaiter
    buildAction ScheduleAction
    buildErr error
    buildErrInfo *BuildStageErrorInfo

    /*
     * Save the job output
     */
    output *JobOutput
    constraints map[string]string

    stageQuota int
    /*
     * AsyncOffload management
     */
    asyncOffloadMgr AsyncOffloadMgr
    enableRightControl bool
    /*
     * auxliary information for scheduler. It saves
     * the position of the job in priority heap
     */
    queueIndex int
}

/*
 * Auxliary information for job. it will be stored
 * into database as JSON string. It is a method to
 * expand database persist information without change
 * database schema
 */
type jobAuxInfo struct {
    AccountInfo UserAccountInfo
    DisableOOMKill bool
    ExecMode string
    HDFSWorkDir string
    JobOutput map[string]string
    StageQuota int
    Constraints map[string]string
}

func (aux *jobAuxInfo)ToJSON() (error, string) {
    body, err := json.Marshal(aux)
    if err != nil {
        Logger.Errorf("Fail to marshal job aux info: %s\n",
            err.Error())
        return err, ""
    }

    return nil, string(body)
}

func (aux *jobAuxInfo)FromJSON(jsonData string) error {
    auxInfo := &jobAuxInfo{}
    decoder := json.NewDecoder(strings.NewReader(jsonData))
    decoder.UseNumber()
    err := decoder.Decode(auxInfo)
    if err != nil {
        return err
    }

    aux.AccountInfo = auxInfo.AccountInfo
    aux.DisableOOMKill = auxInfo.DisableOOMKill
    aux.HDFSWorkDir = auxInfo.HDFSWorkDir
    aux.ExecMode = auxInfo.ExecMode
    aux.JobOutput = auxInfo.JobOutput
    aux.StageQuota = auxInfo.StageQuota
    aux.Constraints = auxInfo.Constraints

    return nil
}

func NewBIOJob(name string, description string, input *BioDataSet, work_dir string,
    log_dir string, pipelineName string) *bioJob {
    job := &bioJob{
        name: name,
        description: description,
        workDir: work_dir,
        logDir: log_dir,
        inputDataSet: input,
        stageIndex: 0,
        pipelineName: pipelineName,
        smId: name,
        stageRetryLimit: 3,
        run: 0,
        lastState: -1,
        graphScheduler: nil,
        failReason: make([]string, 0),
        disableOOMKill: false,
        execMode: "",
        pendingTerminateOp: false,
        scheduleOpData: nil,
        inprogressScheduleOps: 0,
        stageQuota: -1,
        queueIndex: -1,
    }
    jobSchedInfo := NewJobScheduleInfo(job)
    job.SetScheduleInfo(jobSchedInfo)
    job.createTime = time.Now()
    job.dataProvider = dataprovider.NewOsDataProvider(job)
    job.asyncOffloadMgr = asyncOffloadMgr.NewAsyncOffloadContainer(job)
    return job
}

func (job *bioJob) GetEnableRightControl() bool {
    return job.enableRightControl
}

func (job *bioJob) SetEnableRightControl(enableRightControl bool) {
    job.enableRightControl = enableRightControl
}

func (job *bioJob) GetScheduleInfo() *JobScheduleInfo {
    return job.schedInfo
}

func (job *bioJob) SetScheduleInfo(info *JobScheduleInfo) {
    job.schedInfo = info
}

func (job *bioJob) GetOutput() *JobOutput {
    return job.output
}

func (job *bioJob) SetConstraints(constraints map[string]string) {
    job.constraints = constraints
}

func (job *bioJob) Constraints() map[string]string {
    return job.constraints
}

func (job *bioJob) GetStage(stageId string) Stage {
    var stage Stage = nil
    if job.graphScheduler != nil {
        stage = job.graphScheduler.GetStageByID(stageId)
    }
    if stage == nil && job.dataProvider != nil {
        stage = job.dataProvider.GetStageByID(stageId)
    }
    /*offload stage must by be handle the last*/
    if stage == nil && job.asyncOffloadMgr != nil {
        stage = job.asyncOffloadMgr.GetStageByStageId(stageId)
    }

    return stage
}

func (job *bioJob) SetState(state int) {
    job.state = state
}

func (job *bioJob) State() int {
    return job.state
}

func (job *bioJob) SetExecMode(mode string) {
    job.execMode = strings.ToUpper(mode)
}

func (job *bioJob) ExecMode() string {
    return job.execMode
}

func (job *bioJob) SecID() string {
    return job.secID
}

func (job *bioJob) SetSecID(id string) {
    job.secID = id
}

func (job *bioJob) LastState() int {
    return job.lastState
}

func (job *bioJob) Name() string {
    return job.name
}

func (job *bioJob) CreateTime() time.Time {
    return job.createTime
}

func (job *bioJob) FinishTime() time.Time {
    return job.finishTime
}

func (job *bioJob) SetUpdateTime(tm time.Time) {
    job.lock.Lock()
    defer job.lock.Unlock()
    
    job.lastUpdateTime = tm
}

func (job *bioJob) UpdateTime() time.Time {
    job.lock.Lock()
    defer job.lock.Unlock()

    return job.lastUpdateTime
}

func (job *bioJob) SetOOMKillDisabled(disable bool) {
    job.disableOOMKill = disable
}

func (job *bioJob) OOMKillDisabled() bool {
    return job.disableOOMKill
}

func (job *bioJob) CreateJobAuxInfo() (error, string) {
    auxInfo := jobAuxInfo {
                AccountInfo: *job.secCtxt.GetUserInfo(),
                DisableOOMKill: job.OOMKillDisabled(),
                HDFSWorkDir: job.HDFSWorkDir(),
                ExecMode: job.ExecMode(),
                StageQuota: job.StageQuota(),
                Constraints: job.Constraints(),
    }

    /* Save a copy of job output if have */
    if job.output != nil {
        auxInfo.JobOutput = job.output.Outputs()
    }

    return auxInfo.ToJSON()
}

func (job *bioJob) CreateJobInfo() *dbservice.JobDBInfo {
    jobInfo := &dbservice.JobDBInfo{
            Name: job.name,
            Created: job.createTime.Format(BIOFLOW_TIME_LAYOUT),
            State: JobStateToStr(job.state),
            Id: job.GetID(),
            Pipeline: job.pipelineName,
            WorkDir: job.workDir,
	        LogDir: job.logDir,
            Run: job.run,
            Description: job.description,
            Finished: job.finishTime.Format(BIOFLOW_TIME_LAYOUT),
            SMID: job.smId,
            RetryLimit: job.stageRetryLimit,
            FailReason: "",
            SecID: job.SecID(),
            Priority: job.Priority(),
        }

    /*serialize the fail reason*/
    reasons := job.FailReason()
    for i := 0; i < len(reasons); i ++ {
        if reasons[i] == "" {
            continue
        }

        if jobInfo.FailReason == "" {
            jobInfo.FailReason = reasons[i]
        } else {
            jobInfo.FailReason += "\n" + reasons[i]
        }
    }

    if jobInfo.Finished == "" {
        jobInfo.Finished = jobInfo.Created
    }
    err, jsonData := job.InputDataSet().ToJSON()
    if err != nil {
        SchedulerLogger.Errorf("Can't encode job dataset to JSON: %s",
            err.Error())
        return nil
    }
    jobInfo.Json = jsonData

    /*
     * package the auxliary info as JSON
     */
    err, jsonData = job.CreateJobAuxInfo()
    if err != nil {
        SchedulerLogger.Errorf("Can't encode job aux info to JSON:%s\n",
            err.Error())
        return nil
    }
    jobInfo.AuxInfo = jsonData

    return jobInfo
}

func (job *bioJob) RecoverFromJobInfo(jobInfo *dbservice.JobDBInfo, isRecovery bool) error {
    var err error = nil
    job.name = jobInfo.Name
    job.createTime, err = time.Parse(BIOFLOW_TIME_LAYOUT, jobInfo.Created)
    if err != nil {
        SchedulerLogger.Errorf("Fail to parse job database time %s: %s\n",
            jobInfo.Created, err.Error())
        return err
    }

    if !isRecovery {
        job.state = JobStrToState(jobInfo.State)
    } else {
        job.state = JOB_STATE_CREATED
    }
    job.lastState = JobStrToState(jobInfo.PausedState)
    job.id = String2JobID(jobInfo.Id)
    job.pipelineName = jobInfo.Pipeline
    job.workDir = jobInfo.WorkDir
    job.logDir = jobInfo.LogDir
    job.description = jobInfo.Description
    job.run = jobInfo.Run
    job.finishTime, err = time.Parse(BIOFLOW_TIME_LAYOUT, jobInfo.Finished)
    if err != nil {
        SchedulerLogger.Errorf("Fail to parse job database time %s: %s\n",
            jobInfo.Finished, err.Error())
        return err
    }
    job.stageRetryLimit = jobInfo.RetryLimit
    job.smId = jobInfo.SMID
    job.secID = jobInfo.SecID
    job.priority = jobInfo.Priority

    job.failReason = make([]string, 0)
    if !isRecovery {
        reasons := strings.Split(jobInfo.FailReason, "\n")
        job.failReason = append(job.failReason, reasons ...)
    }

    job.inputDataSet = new(BioDataSet)
    err = job.inputDataSet.FromJSON(jobInfo.Json)
    if err != nil {
        return err
    }

    jobSchedInfo := NewJobScheduleInfo(job)
    job.SetScheduleInfo(jobSchedInfo)

    job.dataProvider = dataprovider.NewOsDataProvider(job)

    /*reconstruct auxliary information*/
    auxInfo := &jobAuxInfo{}
    err = auxInfo.FromJSON(jobInfo.AuxInfo)
    if err == nil {
        job.secCtxt = NewSecurityContext(nil)
        job.secCtxt.SetUserInfo(&auxInfo.AccountInfo)
        job.SetOOMKillDisabled(auxInfo.DisableOOMKill)
        job.SetHDFSWorkDir(auxInfo.HDFSWorkDir)
        job.SetExecMode(auxInfo.ExecMode)
        job.stageQuota = auxInfo.StageQuota
    }
    job.asyncOffloadMgr = asyncOffloadMgr.NewAsyncOffloadContainer(job)
    job.SetQueueIndex(-1)

    return err
}

func (job *bioJob) SetID(id *JobID) {
    job.id = id
}

func (job *bioJob) InputDataSet() *BioDataSet {
    return job.inputDataSet
}

func (job *bioJob) SetSampleID(sm string) {
    job.smId = sm
}

func (job *bioJob) SMID() string {
    return job.smId
}

func (job *bioJob) GetID() string {
    return job.id.String()
}

func (job *bioJob) ID() *JobID {
    return job.id
}

func (job *bioJob) WorkDir() string {
    return job.workDir
}

func (job *bioJob) SetWorkDir(dir string) {
    job.workDir = dir
}

func (job *bioJob) HDFSWorkDir() string {
    return job.hdfsWorkDir
}

func (job *bioJob) SetHDFSWorkDir(dir string) {
    job.hdfsWorkDir = dir
}

func (job *bioJob) LogDir() string {
    return job.logDir
}

func (job *bioJob) SetLogDir(dir string) {
    job.logDir = dir
}

func (job *bioJob) GenerateStageIndex() int {
    index := job.stageIndex
    job.stageIndex ++
    return index
}

func cloneInputData(out map[string]string, in map[string]string) {
    for id, item := range in {
        out[id] = item
    }
}

func (job *bioJob) PipelineName() string {
    return strings.ToUpper(job.pipelineName)
}

func (job *bioJob) Pipeline() Pipeline {
    return job.pipeline
}

func (job *bioJob) SetPipeline(pipeline Pipeline) {
    job.pipeline = pipeline
}

/*build flow graph for job*/
func (job *bioJob) BuildFlowGraph() (error, *BuildStageErrorInfo) {
    var buildStageErrInfo *BuildStageErrorInfo
    pipeline := job.Pipeline()
    if job.graphScheduler == nil {
        graphScheduler, err := graphscheduler.CreateGraphScheduler(pipeline.Type(),
            job, pipeline)
        if err != nil {
            SchedulerLogger.Errorf("Fail to create graph scheduler: %s\n",
                err.Error())
            return err, nil
        }
        job.graphScheduler = graphScheduler
    }
    
    err, buildStageErrInfo := job.graphScheduler.BuildGraph()

    job.UpdateJobFlowGraphInfo()

    return err, buildStageErrInfo
}

/*
 * Just restore the flow graph one step.
 * The reason that can't restored to latest graph is that some
 * stage may have some execution state (e.g, retry-count). We need
 * restore the execution state before restoring to a later state.
 */
func (job *bioJob) StepRestoreFlowGraph(info FlowGraphInfo) (error, bool, *BuildStageErrorInfo) {
    return job.graphScheduler.RestoreGraph(info)
}

/*
 * This interface is used to release and cleanup all the system resource acquired by the
 * job. For example, if graph scheduler hold some lock from database, it will release the
 * lock here. otherwise, it may not be able to recover.
 */
func (job *bioJob) ReleaseResource() error {
    if job.graphScheduler != nil {
        err := job.graphScheduler.Fini()
        job.graphScheduler = nil
        return err
    }

    return nil
}

/*
 * This interface is used to release memory or pointers tracked by the job.
 * It is to help GC work well.
 */
func (job *bioJob) Fini() {
    job.schedInfo = nil
    job.dataProvider = nil
    job.asyncOffloadMgr = nil
}

/*
 * Whether job's flow graph is built complete
 */
func (job *bioJob) FlowGraphCompleted() bool {
    return job.graphScheduler.GraphCompleted()
}

func (job *bioJob) ScheduleJobGraph() (map[string]Stage, int, error) {
     return job.graphScheduler.Schedule()
}

func (job *bioJob) CheckJobGraph() int {
    return job.graphScheduler.CheckGraphState()
}

func(job *bioJob) UpdateStage(stage Stage, event uint32, info *EventNotifyInfo) {
    if info.Hostname() != "" {
        stage.SetHostName(info.Hostname())
    }
    if info.IP() != "" {
        stage.SetIP(info.IP())
    }

    switch event {
        case STAGE_DONE:
            stage.SetState(STAGE_DONE)
            stage.SetFinishTime(time.Now())
        case STAGE_FAIL:
            stage.SetState(STAGE_FAIL)
            stage.SetFinishTime(time.Now())
            stage.SetFailReason(info.Message())
        case STAGE_RUNNING:
            stage.SetState(STAGE_RUNNING)
            stage.SetScheduledTime(time.Now())
        case STAGE_SUBMITTED:
            stage.SetState(STAGE_SUBMITTED)
            stage.SetSubmitTime(time.Now())
        case STAGE_LOST:
            stage.SetFinishTime(time.Now())
            stage.SetFailReason(info.Message())
            stage.SetState(STAGE_LOST)
        case STAGE_QUEUED:
            stage.SetState(STAGE_QUEUED)
        case STAGE_SUBMIT_FAIL:
            /*Do nothing*/
        default:
            SchedulerLogger.Infof("UpdateStage(job %s, stage %s): ignore unknown event %s\n",
                job.GetID(), stage.GetID(), StageStateToStr(event))
    }
}

func (job *bioJob) HandleOffloadStageEvent(stage Stage, event uint32) error {
    SchedulerLogger.Infof("HandleoffloadStageEvent(stage %s): event %s",
        stage.GetID(), StageStateToStr(event))
    switch event {
    case STAGE_DONE:
        job.asyncOffloadMgr.HandleStageEvent(stage.GetID(), DATA_DONE)
    case STAGE_LOST, STAGE_FAIL:
        retry := job._AllowStageRetry(stage)
        if !retry {
            job.asyncOffloadMgr.HandleStageEvent(stage.GetID(), DATA_ABORT)
        } else {
            /*stage lost or stage fail is the same to offload stage*/
            job.asyncOffloadMgr.HandleStageEvent(stage.GetID(), DATA_LOST)
        }
    case STAGE_SUBMIT_FAIL:
        job.asyncOffloadMgr.HandleStageEvent(stage.GetID(), DATA_SUBMIT_FAIL)
    default:
        SchedulerLogger.Infof("HandleDataStageEvent(stage %s): ignore event %s\n",
            stage.GetID(), StageStateToStr(event))
    }
    return nil
}

func (job *bioJob) HandleDataStageEvent(stage Stage, event uint32) error {
    SchedulerLogger.Infof("HandleDataStageEvent(stage %s): event %s",
        stage.GetID(), StageStateToStr(event))
    switch event {
        case STAGE_DONE:
            job.dataProvider.HandleStageEvent(stage.GetID(), DATA_DONE)
        case STAGE_LOST, STAGE_FAIL:
            retry := job._AllowStageRetry(stage)
            if !retry {
                stages := job.dataProvider.HandleStageEvent(stage.GetID(), DATA_ABORT)
                if stages != nil {
                    SchedulerLogger.Infof("Stage %s failed, fail stages %+v depend on it.",
                        stage.GetID(), stages)
                    /*If a data stage fail, we forbid all the graph stages depend on it. */
                    for _, stageId := range stages {
                        job.graphScheduler.HandleGraphEvent(stageId, GRAPH_EVENT_FORBIDDEN)
                    }
                }
            } else {
                if event == STAGE_LOST {
                    job.dataProvider.HandleStageEvent(stage.GetID(), DATA_LOST)
                } else {
                    job.dataProvider.HandleStageEvent(stage.GetID(), DATA_FAIL)
                }
            }
        case STAGE_SUBMIT_FAIL:
            job.dataProvider.HandleStageEvent(stage.GetID(), DATA_SUBMIT_FAIL)
        default:
            SchedulerLogger.Infof("HandleDataStageEvent(stage %s): ignore event %s\n",
                stage.GetID(), StageStateToStr(event))
    }

    return nil
}

func (job *bioJob) HandleGraphStageEvent(stage Stage, event uint32) error {
    SchedulerLogger.Infof("HandleGraphStageEvent(stage %s) handle event %s",
        stage.GetID(), StageStateToStr(event))
    graphScheduler := job.graphScheduler
    stageId := stage.GetID()
    var err error = nil
    switch event {
    case STAGE_DONE:
        err = graphScheduler.HandleGraphEvent(stage.GetID(), GRAPH_EVENT_DONE)
        if err != nil {
            SchedulerLogger.Errorf("Fail to handle graph event: %s\n",
                err.Error())
        }
        /*release the resource of stages executing done*/
        stage.Recycle()
    case STAGE_LOST, STAGE_FAIL:
        retry := job._AllowStageRetry(stage)
        if !retry {
            err = graphScheduler.HandleGraphEvent(stageId, GRAPH_EVENT_FORBIDDEN)
            /*release resource for stages fail but never retry*/
            stage.Recycle()
        } else {
            err = graphScheduler.HandleGraphEvent(stageId, GRAPH_EVENT_FAIL)
        }
    case STAGE_RUNNING:
        err = graphScheduler.HandleGraphEvent(stageId, GRAPH_EVENT_RUNNING)
    case STAGE_SUBMITTED:
        err = graphScheduler.HandleGraphEvent(stageId, GRAPH_EVENT_SUBMITTED)
    case STAGE_SUBMIT_FAIL:
        err = graphScheduler.HandleGraphEvent(stageId, GRAPH_EVENT_SUBMIT_FAIL)
    case STAGE_QUEUED:
        err = graphScheduler.HandleGraphEvent(stageId, GRAPH_EVENT_QUEUED)
    default:
        SchedulerLogger.Infof("HandleGraphStageEvent(stage %s): ignore event %s",
            stage.GetID(), StageStateToStr(event))
    }
    return err
}

func (job *bioJob) ExecState() int {
    execInfo := job.schedInfo.ExecInfo()
    return execInfo.GetExecState()
}

func (job *bioJob) SetExecState(state int) {
    execInfo := job.schedInfo.ExecInfo()
    execInfo.SetExecState(state)
}

func (job *bioJob) HandleEvent(stageId string, event uint32,
    info *EventNotifyInfo) (error, *BuildStageErrorInfo, ScheduleAction) {
    return job._HandleEvent(stageId, event, info, false)
}

func (job *bioJob) HandleStageEventInRecover(stageId string, event uint32,
    info *EventNotifyInfo) ScheduleAction {
    _, _, action := job._HandleEvent(stageId, event, info, true)
    return action
}

func (job *bioJob) _HandleEvent(stageId string, event uint32,
    info *EventNotifyInfo, isRecover bool) (error, *BuildStageErrorInfo, ScheduleAction) {
    /*
    * In recover process, we ignore the stages created by dataprovider, so we
    * do nothing here when handle this stages.
     */
    SchedulerLogger.Infof("HandleEvent(job %s): handle stage %s event %s",
        job.GetID(), stageId, StageStateToStr(event))
    var err error = nil
    var buildStageErrInfo *BuildStageErrorInfo = nil
    action := S_ACTION_NONE
    var stage Stage
    var lastStageState uint32
    if stageId != "" {
        job.lock.Lock()
        stage = job.GetStage(stageId)
        if stage != nil {
            lastStageState = stage.State()
            job.UpdateStage(stage, event, info)
            switch stage.Type() {
            case GRAPH_STAGE:
                err = job.HandleGraphStageEvent(stage, event)
            case DATA_STAGE:
                err = job.HandleDataStageEvent(stage, event)
            case OFFLOAD_STAGE:
                err = job.HandleOffloadStageEvent(stage, event)
            default:
                SchedulerLogger.Errorf("HandleEvent(job %s): ignore non-exist stage type: %d\n",
                    job.GetID(), stage.Type())
            }
        }
        job.lock.Unlock()
        job.SetUpdateTime(time.Now())

        if stage == nil {
            SchedulerLogger.Infof("HandleEvent(job %s): ignore non-exist stage %s event %s",
                job.GetID(), stageId, StageStateToStr(event))
            return nil, nil, S_ACTION_NONE
        }
        if err != nil {
            return err, nil, S_ACTION_ABORT_JOB
        }
    }

    if !isRecover {
        execEvent := &JobExecEvent{
            stage: stage,
            event: event,
            lastStageState: lastStageState,
        }
        SchedulerLogger.Infof("HandleEvent(job %s): check state machine stage %s, event %s",
            job.GetID(), stageId, StageStateToStr(event))
        err, buildStageErrInfo, action = job.RunExecStateMachine(execEvent)
    }

    return err, buildStageErrInfo, action
}

func (job *bioJob) ReConstructJobScheduleState(stageStateInfo map[string]uint32) ScheduleAction {
    action := S_ACTION_NONE
    for stageId, state := range stageStateInfo {
        stageAction := job.HandleStageEventInRecover(stageId, state,
            NewEventNotifyInfo("", "", ""))
        if stageAction == S_ACTION_ABORT_JOB {
            action = S_ACTION_ABORT_JOB
        }
        SchedulerLogger.Infof("Recover job %s stage %s state to %s\n",
            job.GetID(), stageId, StageStateToStr(state))
    }

    return action
}

/*
 * For each stage, evaluate state and schedule policy to get the 
 * target schedule action
 */
func (job *bioJob) EvaluateStageScheduleAction(stage Stage) ScheduleAction {
    switch stage.State() {
    case STAGE_INITIAL:
        /*
         * This is an initial stage and schedule it directly
         */
        return S_ACTION_SCHEDULE
    case STAGE_LOST, STAGE_FAIL:
        if !stage.AllowScheduleStage() {
            /*
             * At this time, a thread is in the handle stage lost event,
             * no need to handle this stage, and we will kick it when the
             * handle stage lost event complete.
             */
            return S_ACTION_NONE
        }

        retry := job._AllowStageRetry(stage)
        if retry {
            return S_ACTION_RETRY_STAGE
        }

        if stage.FailAbort() {
            SchedulerLogger.Infof("The stage %s not fail ignore, so fail the job %s directly\n",
                stage.GetID(),job.GetID())
            return S_ACTION_ABORT_JOB
        }
        if stage.Type() == GRAPH_STAGE {
            ret := job.CheckJobGraph()
            if ret == JOB_GRAPH_PSEUDO_FINISH {
                return S_ACTION_PSEUDO_FINISH_JOB
            } else if ret == JOB_GRAPH_ERROR {
                return S_ACTION_ABORT_JOB
            } else if ret == JOB_GRAPH_FINISHED {
                return S_ACTION_NONE
            } else {
                if stage.State() == STAGE_LOST {
                    /*The stage retry exceeded the limit, need send to HandleAsyncJobEvent processing*/
                    return S_ACTION_CHECK_LOST_STATUS
                } else if stage.State() == STAGE_FAIL {
                    return S_ACTION_CHECK_FAIL_STATUS
                }
            }
        } else {
            /*DATA STAGE and OFFLOAD STAGE have the same process*/
            if stage.State() == STAGE_LOST {
                /*The stage retry exceeded the limit, need send to HandleAsyncJobEvent processing*/
                return S_ACTION_CHECK_LOST_STATUS
            } else if stage.State() == STAGE_FAIL {
                return S_ACTION_CHECK_FAIL_STATUS
            }
        }

    default:
        return S_ACTION_NONE
    }

    return S_ACTION_INVALID
}


func (job *bioJob) _AllowStageRetry(stage Stage) bool {
    switch stage.State() {
    case STAGE_LOST:
        retryCount := stage.RetryCount()
        retryLimit := job.stageRetryLimit
        if stage.FailRetryLimit() != 0 {
            retryLimit = stage.FailRetryLimit()
        }

        /*
         * The run count indicate how many times user initiate the
         * manual job recovery, so need account it to the stage's
         * retry limit
         */
        retryLimit += job.RunCount()

        if retryCount < retryLimit {
            return true
        } else {
            return false
        }

    case STAGE_FAIL:
        /*
         * The run count indicate how many times user initiate the
         * manual job recovery, so need account it to the stage's
         * retry limit
         */
        retryLimit := stage.FailRetryLimit() + job.RunCount()
        if retryLimit == 0 {
            return false
        }
        retryCount := stage.RetryCount()
        if retryCount < retryLimit {
            return true
        } else {
            return false
        }

    default:
        return true
    }
}

/*
 * For each stage, evaluate state and failure events to get the 
 * target job action
 */
func (job *bioJob) _EvaluateGraphStageFailureState(stage Stage) ScheduleAction {
    switch stage.State() {
        case STAGE_LOST, STAGE_FAIL:
            if stage.FailAbort() {
                SchedulerLogger.Infof("The stage %s not fail ignore, so fail the job %s directly\n",
                    stage.GetID(),job.GetID())
                return S_ACTION_ABORT_JOB
            }
            ret := job.CheckJobGraph()
            if ret == JOB_GRAPH_PSEUDO_FINISH {
                return S_ACTION_PSEUDO_FINISH_JOB
            } else if ret == JOB_GRAPH_ERROR {
                return S_ACTION_ABORT_JOB
            } else {
                return S_ACTION_NONE
            }
        default:
            return S_ACTION_NONE
    }

    return S_ACTION_NONE
}

func (job *bioJob) QueueIndex() int {
    return job.queueIndex
}

func (job *bioJob) SetQueueIndex(index int) {
    job.queueIndex = index
}

/*
 * A job only needs to be terminated once, but since multiple tasks of a job done(fail/psudone/finish) at
 * the same time, causing this job to terminated multiple times, caller the func avoid
 * terminated processing multiple times
 */
func (job *bioJob) JobTerminated() bool {
    if job.state == JOB_STATE_PAUSED || job.state == JOB_STATE_FAIL ||
        job.state == JOB_STATE_PSUDONE || job.state == JOB_STATE_FINISHED {
        return true
    }

    return false
}

/*
 * Caller requests a schedule operation for the job. There are two kinds of operations:
 * 1) terminate the job
 * 2) schedule the job
 * 
 * the request will be permit or deny depends on the situation.
 */
func (job *bioJob) RequestScheduleOperation(op JobScheduleOpType, data JobScheduleOpData) bool {
    job.lock.Lock()
    defer job.lock.Unlock()

    if op == JOB_OP_TERMINATE {
        if !job.pendingTerminateOp {
            /* If no one do cleanup, do it. otherwise
             * some others are doing it, don't do it again
             */
            job.pendingTerminateOp = true
            job.scheduleOpData = data
            if job.inprogressScheduleOps <= 0 {
                SchedulerLogger.Infof("Check job %s cleanup request with %d schedule ops, permit it\n",
                    job.id, job.inprogressScheduleOps)
                return true
            } else {
                SchedulerLogger.Infof("Check job %s cleanup request with %d schedule ops, deny it\n",
                    job.id, job.inprogressScheduleOps)
                return false
            }
        } else {
            SchedulerLogger.Infof("Check job %s cleanup request duplicated with %d schedule ops, deny it\n",
                    job.id, job.inprogressScheduleOps)
            return false
        }
    } else if op == JOB_OP_SCHEDULE {
        if job.pendingTerminateOp {
            SchedulerLogger.Infof("Check job %s schedule request with 1 schedule ops, deny it\n",
                    job.id)
            return false
        }
        if job.state == JOB_STATE_PAUSED || job.state == JOB_STATE_CANCELED ||
            job.state == JOB_STATE_FAIL || job.state == JOB_STATE_PSUDONE ||
            job.state == JOB_STATE_FINISHED {
            return false
        }

        job.inprogressScheduleOps ++
        return true
    }

    return true
}

/*
 * Finish the requested operation. It will return the following data:
 * 1) the pending schedule operation
 * 2) the data associated with the operation
 */
func (job *bioJob) FinishScheduleOperation(op JobScheduleOpType) (JobScheduleOpType, JobScheduleOpData) {
    job.lock.Lock()
    defer job.lock.Unlock()

    if op == JOB_OP_SCHEDULE {
        job.inprogressScheduleOps --
        if job.inprogressScheduleOps <= 0 && job.pendingTerminateOp {
            SchedulerLogger.Infof("Finish job %s schedule operations(%d) and resume pending cleanup\n",
               job.id, job.inprogressScheduleOps) 
            return JOB_OP_TERMINATE, job.scheduleOpData
        }
    }

    if op == JOB_OP_TERMINATE {
        job.pendingTerminateOp = false
        job.scheduleOpData = nil
    }

    return JOB_OP_NOOP, nil
}

func (job *bioJob) Pause() error {
    job.lock.Lock()
    defer job.lock.Unlock()

    if job.state != JOB_STATE_PAUSED {
        job.lastState = job.state
        job.state = JOB_STATE_PAUSED
    }

    return nil
}

func (job *bioJob) Resume() error {
    job.lock.Lock()
    defer job.lock.Unlock()

    if job.state == JOB_STATE_PAUSED {
        job.state = job.lastState
        job.lastState = -1
    } else {
        errStr := fmt.Sprintf("Job state is %s, not paused. Can't resume.",
            JobStateToStr(job.state))
        return errors.New(errStr)
    }

    return nil
}

func (job *bioJob) UpdateStageResourceSpec(stageId string,
    resource *ResourceSpecJSONData) error {
    if resource == nil {
        return nil
    }

    stage := job.GetStage(stageId)
    if stage == nil {
        SchedulerLogger.Infof("The Node or Stage with ID %s for job %s not exist\n",
            stageId, job.GetID())
        return errors.New("No stage exist with name " + stageId)
    }

    /*validate the resource spec*/ 
    spec := ResourceSpec{}
    spec.SetCPU(stage.GetCPU())
    spec.SetMemory(stage.GetMemory())
    if resource.Cpu > 0 {
        spec.SetCPU(resource.Cpu)
    }
    if resource.Memory > 0 {
        spec.SetMemory(resource.Memory)
    }

    stage.SetResourceSpec(spec)

	return nil
}

/*
 * Mark job scheduled, return whether it is
 * marked success. return true only job marked
 * scheduled for the first time
 */
func (job *bioJob) MarkScheduled() bool{
    job.lock.Lock()
    defer job.lock.Unlock()

    /*
     * Should not mark a canceled job running
     */
    if job.state == JOB_STATE_CANCELED {
        return false
    }

    marked := false
    if job.state != JOB_STATE_PAUSED {
        if job.state != JOB_STATE_RUNNING {
            job.state = JOB_STATE_RUNNING
            marked = true
        }
    } else {
        job.lastState = JOB_STATE_RUNNING
    }

    return marked
}

func (job *bioJob) IsRunning() bool {
    job.lock.Lock()
    defer job.lock.Unlock()

    return job.state == JOB_STATE_RUNNING
}

func (job *bioJob) Canceled() bool {
    job.lock.Lock()
    defer job.lock.Unlock()

    return job.state == JOB_STATE_CANCELED
}

func (job *bioJob) Cancel() {
    job.lock.Lock()
    defer job.lock.Unlock()

    job.lastState = job.state
    job.state = JOB_STATE_CANCELED
}

func (job *bioJob) AllowSchedule() bool {
    if job.state == JOB_STATE_PAUSED || job.state == JOB_STATE_CANCELED ||
    job.state == JOB_STATE_FAIL {
        return false
    } else {
        return true
    }
}

func (job *bioJob) Complete(success bool,
    reason string) {
    job.lock.Lock()
    defer job.lock.Unlock()

    job.finishTime = time.Now()
    if success {
        if job.state != JOB_STATE_PSUDONE {
            job.state = JOB_STATE_FINISHED
        }
    } else {
        /*a job canceled should not be set to fail*/
        if job.state != JOB_STATE_CANCELED {
            job.state = JOB_STATE_FAIL
        }
        if reason != "" {
            job.failReason = append(job.failReason, reason)
        }
    }
}


func (job *bioJob) RetryLimit() int {
    return job.stageRetryLimit
}

func (job *bioJob) StageCount() int {
    if job.graphScheduler == nil {
        return -1
    }

    return job.graphScheduler.StageCount()
}

func (job *bioJob) RunCount() int {
    return job.run
}

func (job *bioJob) Retry() {
    job.lock.Lock()
    defer job.lock.Unlock()

    job.run ++
}

func (job *bioJob) SetFailReason(reason string) {
    job.lock.Lock()
    defer job.lock.Unlock()

    job.failReason = append(job.failReason, reason)
}

func (job *bioJob) FailReason() []string {
    return job.failReason
}

func (job *bioJob) GetWaitingStages() ([]Stage, []Stage) {
     if job.graphScheduler == nil {
         return nil, nil
     }

     return job.graphScheduler.GetWaitingStages()
}

func (job *bioJob) Priority() int {
    return job.priority
}

func (job *bioJob) SetPriority(pri int) {
    job.priority = pri
}

func (job *bioJob) GetSecurityContext() *SecurityContext {
    return job.secCtxt
}

func (job *bioJob) SetSecurityContext(ctxt *SecurityContext) {
    job.secCtxt = ctxt
}

func (job *bioJob) UpdateJobFlowGraphInfo() error {
    schedInfo := job.GetScheduleInfo()
    err, info := job.graphScheduler.GetGraphInfo()
    if err != nil {
        return err
    }
    schedInfo.SetGraphInfo(info)
    return nil
}

func (job *bioJob) GetStatus() (error, *BioflowJobStatus) {
    jobStatus := &BioflowJobStatus{
        JobId: job.GetID(),
        Name: job.Name(),
        Pipeline: job.PipelineName(),
        WorkDir: job.WorkDir(),
        HDFSWorkDir: job.HDFSWorkDir(),
        Created: job.createTime.Format(BIOFLOW_TIME_LAYOUT),
        Finished: "N/A",
        State: JobStateToStr(job.State()),
        PausedState: JobStateToStr(job.LastState()),
        RetryLimit: job.RetryLimit(),
        StageCount: job.StageCount(),
        RunCount: job.RunCount(),
        FailReason: job.FailReason(),
        Owner: job.SecID(),
        Priority: job.Priority(),
        ExecMode: job.ExecMode(),
        StageQuota: job.StageQuota(),
    }

    if job.graphScheduler != nil {
        err, graphInfo := job.graphScheduler.GetGraphInfo()
        if err != nil {
            return err, nil
        }
        jobStatus.GraphCompleted = graphInfo.FlowGraphInfoCompleted()
    } else {
        jobStatus.GraphCompleted = false
    }

    return nil, jobStatus
}

/*
 * Determine the thread's role in building graph:
 * 1) owner: build the graph
 * 2) waiter: wait for the owner to build the graph
 *
 * must be called inside the job lock critical section
 */
func (job *bioJob) _DetermineGraphBuildOwnerLocked() (bool, *xcommon.TimedWaiter) {
    waiter := job.buildWaiter
    if waiter == nil {
        waiter = xcommon.NewTimedWaiter()

        /*Set it inside the critical section to make another builder thread to wait*/
        job.buildWaiter = waiter
        job.buildAction = S_ACTION_NONE
        job.buildErr = nil
        job.buildErrInfo = nil

        /*I am the winner to build the graph*/
        return true, waiter
    } else {
        /*should wait for others to build the graph*/
        return false, waiter
    }
}

const (
    BUILD_JOB_GRAPH_TIMEOUT int = 60 * 30    /*default build graph timeout 30 minutes*/
)

func (job *bioJob) _ThreadedBuildGraph(isOwner bool, waiter *xcommon.TimedWaiter) (error,
    *BuildStageErrorInfo, ScheduleAction) {
    var err error
    var buildStageErrInfo *BuildStageErrorInfo
    action := S_ACTION_NONE
    if isOwner {
        SchedulerLogger.Infof("ThreadedBuildGraph(job %s): starts as owner\n",
            job.GetID())
        err, buildStageErrInfo = job.BuildFlowGraph()
        if err != nil {
            if err == GB_ERR_SUSPEND_BUILD {
                SchedulerLogger.Infof("ThreadedBuildGraph(job %s): suspend build and run",
                    job.GetID())
                err = nil
                action = S_ACTION_SCHEDULE
            } else {
                SchedulerLogger.Errorf("ThreadedBuildGraph(job %s): failed to build graph: %s",
                    job.GetID(), err.Error())
                action = S_ACTION_PSEUDO_FINISH_JOB
            }
        } else {
            action = S_ACTION_SCHEDULE
        }

        /*
         * Set the result and clear the waiter inside the critical section. This is critical
         * to ensure the correctness.
         */
        job.lock.Lock()

        /*set the build result*/
        job.buildAction = action
        job.buildErr = err
        job.buildErrInfo = buildStageErrInfo

        /* Clear the waiter
         * From now on, only two cases should occur:
         * 1) Some thread decided to build the graph in an earlier time, but the thread
         *    already hold the waiter pointer in the critical secion
         * 2) All the threads enter critical secion after the point should not decide to
         *    build the graph. This should be ensured by the CheckJobGraph function.
         */
        job.buildWaiter = nil
        job.lock.Unlock()

        /*notify the waiters that the build process is done*/
        waiter.Signal()
    } else {
        SchedulerLogger.Infof("ThreadedBuildGraph(job %s): waits for the owner to build graph\n",
            job.GetID())
        /*should wait for indefinitely*/
        err = waiter.Wait(BUILD_JOB_GRAPH_TIMEOUT)
        if err != nil {
            SchedulerLogger.Errorf("ThreadedBuildGraph(job %s): fail to wait:%s\n",
                job.GetID(), err.Error())
            action = S_ACTION_NONE
        } else {
            SchedulerLogger.Errorf("ThreadedBuildGraph(job %s): wait for build graph done\n",
                job.GetID())

            /* The thread should return the result of the thread building the graph
             * get result in critical section to ensure consistency
             */
            job.lock.Lock()
            action = job.buildAction
            err = job.buildErr
            buildStageErrInfo = job.buildErrInfo
            job.lock.Unlock()
        }
    }

    return err, buildStageErrInfo, action
}


/*
 * The job state machine is core function for the job management and schedule. All
 * the job execution state change should be synced through this function.
 */
func (job *bioJob) RunExecStateMachine(execEvent *JobExecEvent) (error, *BuildStageErrorInfo, ScheduleAction) {
    var err error
    var buildStageErrInfo *BuildStageErrorInfo
    action := S_ACTION_NONE
    stage := execEvent.stage

    SchedulerLogger.Infof("StateMachine(job %s) start: job exec state %s, event %s\n",
        job.GetID(), JobExecStateToString(job.ExecState()), execEvent.String())

    if stage == nil {
        switch execEvent.event {
            case JOB_START, JOB_CLEANUPED, JOB_CLEANUP, JOB_CHECK_STATE:
                /*Job Events allow stage to be nil*/
            default:
                /*Stage events should have stage pointer*/
                SchedulerLogger.Errorf("StateMachine(job %s): handle event %s with nil stage\n",
                    job.GetID(), JobExecEventToString(execEvent.event))
                return nil, nil, S_ACTION_NONE
        }
    }

    /*
     * The state check and transition should be done inside the critical section
     */
    job.lock.Lock()
RUNSTATEMACHINE:
    state := job.ExecState()
    switch state {
        case INIT:
            err, buildStageErrInfo, action = job.RunExecStateMachineInInit(execEvent)
        case PREPARING:
            err, buildStageErrInfo, action = job.RunExecStateMachineInPreparing(execEvent)
        case SCHEDULING:
            err, buildStageErrInfo, action = job.RunExecStateMachineInScheduling(execEvent)
        case CLEANUPING:
            err, buildStageErrInfo, action = job.RunExecStateMachineInCleanuping(execEvent)
            XtDebugAssert(job.GetID(), "cleanuping")
        case RESTORING:
            err, buildStageErrInfo, action = job.RunExecStateMachineInRestoring(execEvent)
            XtDebugAssert(job.GetID(), "restoring")
        case RIGHTCONTROL:
            err, buildStageErrInfo, action = job.RunExecStateMachineInChowning(execEvent)
            XtDebugAssert(job.GetID(), "rightcontrol")
        default:
            SchedulerLogger.Errorf("StateMachine(job %s): exec state is invalid, ignore the event\n",
                job.GetID())
    }

    /*
     * offload has two step:
     * 1.cleanup;
     * 2.rightcontrol.
     * when cleanup is S_ACTION_OFFLOAD_SKIP_TO_NEXT_STATE
     * we need run state machine to restoring, but rightcontrol
     * we only finish the job is ok.
     */
    if action == S_ACTION_OFFLOAD_SKIP_TO_NEXT_STATE {
        SchedulerLogger.Debugf("Offload cleanup no stage, go to next state machine!")
        goto RUNSTATEMACHINE
    }

    /*
     * There may be many threads decide to build the job graph. But only one thread is allowed
     * to do it, others should wait for it
     */
    var waiter *xcommon.TimedWaiter = nil
    isBuildOwner := false
    if action == S_ACTION_BUILD_JOB_GRAPH {
        /*must be called with lock held*/
        isBuildOwner, waiter = job._DetermineGraphBuildOwnerLocked()
    }
    job.lock.Unlock()

    /*
     * Do some heavy pending work outside the critical section now
     */

    /*
     * The job graph build may involve many IO operations, so it should be done
     * outside of the critical section. The timed waiter based sync is to ensure
     * that only one thread will actually build the graph.
     */
    if action == S_ACTION_BUILD_JOB_GRAPH {
        SchedulerLogger.Infof("StateMachine(job %s): starts to build graph\n",
            job.GetID())

        /*Should be called outside the critical section*/
        err, buildStageErrInfo, action = job._ThreadedBuildGraph(isBuildOwner, waiter)
    }

    SchedulerLogger.Infof("StateMachine(job %s): end, job exec state %s, event %s, action %s\n",
        job.GetID(), JobExecStateToString(job.ExecState()), execEvent.String(), action.String())

    return err, buildStageErrInfo, action
}

func (job *bioJob) RunExecStateMachineInInit(execEvent *JobExecEvent) (error,
    *BuildStageErrorInfo, ScheduleAction) {
    var err error
    var buildStageErrInfo *BuildStageErrorInfo
    action := S_ACTION_NONE
    SchedulerLogger.Infof("StateMachineInInit(job %s): starts", job.GetID())
    if execEvent.event == JOB_START {
        job.SetExecState(PREPARING)
        SchedulerLogger.Infof("StateMachineInInit(job %s): receive job start, transition to PREPARING",
            job.GetID())

        SchedulerLogger.Infof("StateMachineInInit(job %s): starts prepare data from input",
            job.GetID())
        err, action = job.PrepareData()
        if err != nil {
            SchedulerLogger.Infof("Job %s failed to prepare data: %s",
                job.GetID(), err.Error())
        }

        /*Transition to scheduling if don't need prepare data*/
        if action == S_ACTION_NONE {
            job.SetExecState(SCHEDULING)
            SchedulerLogger.Infof("StateMachineInInit(job %s): don't need prepare data, transition to SCHEDULING",
                job.GetID())
            action = S_ACTION_BUILD_JOB_GRAPH
        }
    } else {
        stageId := ""
        if execEvent.stage != nil {
            stageId = execEvent.stage.GetID()
        }
        SchedulerLogger.Infof("StateMachineInInit(job %s): ignore the event (stage %s, event %s)",
            job.GetID(), stageId, JobExecEventToString(execEvent.event))
        action = S_ACTION_NONE
    }
    SchedulerLogger.Infof("StateMachineInInit(job %s): ends", job.GetID())

    return err, buildStageErrInfo, action
}

/*
 * Job StateMachine in Preparing State
 */
func (job *bioJob) RunExecStateMachineInPreparing(execEvent *JobExecEvent) (error,
    *BuildStageErrorInfo, ScheduleAction) {
    SchedulerLogger.Infof("StateMachineInPreparing(job %s): starts",
        job.GetID())
    stage := execEvent.stage
    if stage == nil {
        switch execEvent.event {
            case JOB_CHECK_STATE:
            default:
                SchedulerLogger.Infof("StateMachineInPreparing(job %s): ignore nil stage event",
                    job.GetID())
                return nil, nil, S_ACTION_NONE
        }
    } else if stage.Type() != DATA_STAGE {
        SchedulerLogger.Infof("StateMachineInPreparing(job %s): ignore the non-data stage event",
            job.GetID())
        return nil, nil, S_ACTION_NONE
    }

    var err error
    var buildStageErrInfo *BuildStageErrorInfo
    action := S_ACTION_NONE

    switch execEvent.event {
        case STAGE_DONE, JOB_CHECK_STATE:
            SchedulerLogger.Infof("StateMachineInPreparing(job %s): check data provision state",
                job.GetID())
            act := job.CheckDataProvision()
            switch act {
                case DATA_PROVISON_ACTION_FINISH:
                    job.SetExecState(SCHEDULING)
                    SchedulerLogger.Infof("StateMachineInPreparing(job %s): data prepare done, transition to SCHEDULING",
                        job.GetID())
                    action = S_ACTION_BUILD_JOB_GRAPH
                case DATA_PROVISON_ACTION_SCHEDULE:
                    action = S_ACTION_SCHEDULE
                case DATA_PROVISON_ACTION_NONE:
                        action = S_ACTION_NONE
                default:
                    err = fmt.Errorf("StateMachineInPreparing(job %s): get unexpected data provision action",
                        job.GetID())
                    action = S_ACTION_ABORT_JOB
            }
        case STAGE_FAIL, STAGE_LOST:
            allowRetry := job._AllowStageRetry(stage)
            if allowRetry {
                action = S_ACTION_RETRY_STAGE
            } else {
                SchedulerLogger.Infof("StateMachineInPreparing(job %s): have fatal stage, fail the job",
                    job.GetID())
                action = S_ACTION_ABORT_JOB
            }
        case STAGE_RUNNING:
            if execEvent.lastStageState != STAGE_RUNNING {
                action = S_ACTION_UPDATE_DB
            }
        case STAGE_SUBMIT_FAIL:
            action = S_ACTION_SCHEDULE
        default:
            stageId := ""
            if execEvent.stage != nil {
                stageId = execEvent.stage.GetID()
            }
            SchedulerLogger.Infof("StateMachineInPreparing(job %s): ignore event (stage %s, event %s)",
                job.GetID(), stageId, JobExecEventToString(execEvent.event))
            action = S_ACTION_NONE
    }
    SchedulerLogger.Infof("StateMachineInPreparing(job %s): ends",
        job.GetID())

    return err, buildStageErrInfo, action
}

func (job *bioJob) _CalculateActionByJobGraphState(isCheckState bool) (ScheduleAction, error) {
    ret := job.CheckJobGraph()
    action := S_ACTION_NONE
    var err error = nil
    switch ret {
        case JOB_GRAPH_FINISHED:
            if job.FlowGraphCompleted() {
                err, action = job.GenereateCleanupOffloadStage()
                job.SetExecState(CLEANUPING)
                /*
                 * Job is scheduled done, save a copy of job output of graph scheduler
                 */
                if job.graphScheduler != nil {
                    SchedulerLogger.Infof("Retrieving job %s output from graph scheduler\n ",
                        job.GetID())
                    job.output, err = job.graphScheduler.GetOutput()
                    if err != nil {
                        SchedulerLogger.Errorf("Retrieve job %s output from graph scheduler fail: %s\n ",
                            job.GetID(), err.Error())
                        return S_ACTION_NONE, errors.New("Evaluate job output error: " + err.Error())
                    } else {
                        SchedulerLogger.Infof("Succeed to retrieve job %s output from graph scheduler\n ",
                            job.GetID())
                    }
                }

                SchedulerLogger.Infof("Job %s complete schedule graph, start to run in CLEANUPING state",
                    job.GetID())

                SchedulerLogger.Infof("Job %s wait for cleanuping", job.GetID())
            } else {
                SchedulerLogger.Infof("Job %s complete schedule partial graph, continue to build graph",
                    job.GetID())
                action = S_ACTION_BUILD_JOB_GRAPH
            }
        case JOB_GRAPH_PSEUDO_FINISH:
            action = S_ACTION_PSEUDO_FINISH_JOB
        case JOB_GRAPH_ERROR:
            action = S_ACTION_ABORT_JOB
        default:
            if !isCheckState {
                action = S_ACTION_SCHEDULE
            } else {
                action = S_ACTION_NONE
            }
    }

    return action, err
}

/*
 * Job StateMachine in Scheduling State
 */
func (job *bioJob) RunExecStateMachineInScheduling(execEvent *JobExecEvent) (error,
    *BuildStageErrorInfo, ScheduleAction) {
    SchedulerLogger.Infof("StateMachineInScheduling(job %s): starts",
        job.GetID())
    stage := execEvent.stage
    if stage == nil {
        switch execEvent.event {
            case JOB_CHECK_STATE:
            default:
                SchedulerLogger.Errorf("StateMachineInScheduling(job %s): ignore event %s with nil stage\n",
                   job.GetID(), JobExecEventToString(execEvent.event)) 
                return nil, nil, S_ACTION_NONE
        }
    }

    var err error
    var buildStageErrInfo *BuildStageErrorInfo
    action := S_ACTION_NONE
    switch execEvent.event {
        case STAGE_DONE:
            stageType := stage.Type()
            if stageType == DATA_STAGE {
                SchedulerLogger.Infof("StateMachineInScheduling(job %s): check data provision state",
                    job.GetID())
                act := job.CheckDataProvision()
                if act == DATA_PROVISON_ACTION_SCHEDULE {
                    action = S_ACTION_SCHEDULE
                } else if act == DATA_PROVISON_ACTION_NONE {
                    action = S_ACTION_NONE
                } else {
                    err = fmt.Errorf("Unexpected data provision action in state SCHEDULING")
                    action = S_ACTION_ABORT_JOB
                }
            } else if stageType == GRAPH_STAGE {
                SchedulerLogger.Infof("StateMachineInScheduling(job %s): check graph state",
                    job.GetID())
                action, err = job._CalculateActionByJobGraphState(false)
            } else {
                SchedulerLogger.Infof("StateMachineInScheduling(job %s): check offload state",
                    job.GetID())
            }
        case JOB_CHECK_STATE:
            action, err = job._CalculateActionByJobGraphState(true)
        case STAGE_FAIL, STAGE_LOST:
            allowRetry := job._AllowStageRetry(stage)
            if allowRetry {
                action = S_ACTION_RETRY_STAGE
            } else {
                action = job._EvaluateGraphStageFailureState(stage)
            }
        case STAGE_RUNNING:
            if execEvent.lastStageState != STAGE_RUNNING {
                action = S_ACTION_UPDATE_DB
            }
        case STAGE_SUBMIT_FAIL:
            action = S_ACTION_SCHEDULE
        default:
            stageId := ""
            if execEvent.stage != nil {
                stageId = execEvent.stage.GetID()
            }
            SchedulerLogger.Infof("StateMachineInScheduling(job %s): ignore event (stage %s, event %s)",
                job.GetID(), stageId, JobExecEventToString(execEvent.event))
            action = S_ACTION_NONE
    }
    SchedulerLogger.Infof("StateMachineInScheduling(job %s): ends",
        job.GetID())

    return err, buildStageErrInfo, action
}

func (job *bioJob) RunExecStateMachineInCleanuping(execEvent *JobExecEvent) (error,
    *BuildStageErrorInfo, ScheduleAction) {
    SchedulerLogger.Infof("StateMachineInCleanuping(job %s): starts",
        job.GetID())
    defer SchedulerLogger.Infof("StateMachineInCleanuping(job %s): ends", job.GetID())
    var action ScheduleAction
    var err error
    stage := execEvent.stage
    switch execEvent.event {
    case STAGE_DONE, JOB_CHECK_STATE:
        err, action = job.RestoreData()
        job.SetExecState(RESTORING)
        if action == S_ACTION_FINISH_JOB {
            action = S_ACTION_SCHEDULE
        }
    case STAGE_FAIL, STAGE_LOST:
        allowRetry := job._AllowStageRetry(stage)
        if allowRetry {
            action = S_ACTION_RETRY_STAGE
        } else {
            action = S_ACTION_PSEUDO_FINISH_JOB
        }
    case STAGE_SUBMIT_FAIL:
        action = S_ACTION_SCHEDULE
    case STAGE_RUNNING:
        if execEvent.lastStageState != STAGE_RUNNING {
            action = S_ACTION_UPDATE_DB
        }
    default:
        stageId := ""
        if execEvent.stage != nil {
            stageId = execEvent.stage.GetID()
        }
        SchedulerLogger.Infof("StateMachineInCleanuping(job %s): ignore event (stage %s, event %s)",
            job.GetID(), stageId, JobExecEventToString(execEvent.event))
        action = S_ACTION_NONE
    }

    return err, nil, action
}

func (job *bioJob)RunExecStateMachineInChowning(execEvent *JobExecEvent) (error,
*BuildStageErrorInfo, ScheduleAction) {
    SchedulerLogger.Infof("StateMachineInChowning(job %s): starts",
        job.GetID())
    defer SchedulerLogger.Infof("StateMachineInChowning(job %s): ends", job.GetID())
    var action ScheduleAction
    var err error
    stage := execEvent.stage
    switch execEvent.event {
    case STAGE_DONE:
        action = S_ACTION_FINISH_JOB
    case STAGE_FAIL, STAGE_LOST:
        allowRetry := job._AllowStageRetry(stage)
        if allowRetry {
            action = S_ACTION_RETRY_STAGE
        } else {
            action = S_ACTION_PSEUDO_FINISH_JOB
        }
    case STAGE_SUBMIT_FAIL:
        action = S_ACTION_SCHEDULE
    case STAGE_RUNNING:
        if execEvent.lastStageState != STAGE_RUNNING {
            action = S_ACTION_UPDATE_DB
        }
    default:
        stageId := ""
        if execEvent.stage != nil {
            stageId = execEvent.stage.GetID()
        }
        SchedulerLogger.Infof("StateMachineInChowning(job %s): ignore event (stage %s, event %s)",
            job.GetID(), stageId, JobExecEventToString(execEvent.event))
        action = S_ACTION_NONE
    }

    return err, nil, action
}

func (job *bioJob) RunExecStateMachineInRestoring(execEvent *JobExecEvent) (error,
    *BuildStageErrorInfo, ScheduleAction) {
    SchedulerLogger.Infof("StateMachineInRestoring(job %s): starts",
        job.GetID())
    stage := execEvent.stage
    if stage == nil {
        switch execEvent.event {
            case JOB_CHECK_STATE:
            default:
                SchedulerLogger.Errorf("Job %s handle event %s in scheduling with nil stage\n",
                   job.GetID(), JobExecEventToString(execEvent.event))
                return nil, nil, S_ACTION_NONE
        }
    } else if stage.Type() != DATA_STAGE {
        return nil, nil, S_ACTION_NONE
    }

    var err error
    var buildStageErrInfo *BuildStageErrorInfo
    action := S_ACTION_NONE

    switch execEvent.event {
        case STAGE_DONE, JOB_CHECK_STATE:
            SchedulerLogger.Infof("StateMachineInRestoring(job %s): check data provision state",
                job.GetID())
            act := job.CheckDataProvision()
            switch act {
                case DATA_PROVISON_ACTION_FINISH:
                    SchedulerLogger.Infof("StateMachineInRestoring(job %s): complete restore data, rightcontrol job",
                        job.GetID())
                    err, action = job.GenereateRightControlOffloadData(job.NeedPrepareData())
                    job.SetExecState(RIGHTCONTROL)
                case DATA_PROVISON_ACTION_PSEUDO_FINISH:
                    SchedulerLogger.Infof("StateMachineInRestoring(job %s): have fatal stage, pseudo finish",
                        job.GetID())
                    action = S_ACTION_PSEUDO_FINISH_JOB
                case DATA_PROVISON_ACTION_SCHEDULE:
                    action = S_ACTION_SCHEDULE
                case DATA_PROVISON_ACTION_NONE:
                    action = S_ACTION_NONE
                default:
                    err = fmt.Errorf("StateMachineInRestoring(job %s): unexpected data provison action, abort job",
                        job.GetID())
                    action = S_ACTION_ABORT_JOB
            }
        case STAGE_FAIL, STAGE_LOST:
            allowRetry := job._AllowStageRetry(stage)
            if allowRetry {
                action = S_ACTION_RETRY_STAGE
            } else {
                act := job.CheckDataProvision()
                if act == DATA_PROVISON_ACTION_PSEUDO_FINISH {
                    SchedulerLogger.Infof("StateMachineInRestoring(job %s): have fatal stage, pseudo finish",
                        job.GetID())
                    action = S_ACTION_PSEUDO_FINISH_JOB
                } else {
                    action = S_ACTION_NONE
                }

            }
        case STAGE_RUNNING:
            if execEvent.lastStageState != STAGE_RUNNING {
                action = S_ACTION_UPDATE_DB
            }
        case STAGE_SUBMIT_FAIL:
            action = S_ACTION_SCHEDULE
        default:
            stageId := ""
            if execEvent.stage != nil {
                stageId = execEvent.stage.GetID()
            }
            SchedulerLogger.Infof("StateMachineInRestoring(job %s): ignore event (stage %s, event %s)",
                job.GetID(), stageId, JobExecEventToString(execEvent.event))
            action = S_ACTION_NONE
    }
    SchedulerLogger.Infof("StateMachineInRestoring(job %s): ends",
        job.GetID())

    return err, buildStageErrInfo, action
}

func (job *bioJob) CheckDataProvision() ACTION {
    return job.dataProvider.CheckDataProvision()
}

func (job *bioJob) PrepareData() (error, ScheduleAction) {
    err, act := job.dataProvider.PreHandleInput()
    if err != nil {
        return err, S_ACTION_ABORT_JOB
    }
    if act == DATA_PROVISON_ACTION_NONE {
        return nil, S_ACTION_NONE
    }else {
        return nil, S_ACTION_SCHEDULE
    }
}

func (job *bioJob) GenereateCleanupOffloadStage() (error, ScheduleAction) {
    _, action := job.asyncOffloadMgr.GenereateCleanupOffloadStage()
    if action == DATA_PROVISON_ACTION_OFFLOAD_CHECK {
        return nil, S_ACTION_OFFLOAD_SKIP_TO_NEXT_STATE
    } else if action == DATA_PROVISON_ACTION_SCHEDULE {
        return nil, S_ACTION_SCHEDULE
    } else if action == DATA_PROVISON_ACTION_NONE {
        return nil, S_ACTION_NONE
    } else {
        return errors.New("No such aciton"), S_ACTION_PSEUDO_FINISH_JOB
    }
}

func (job *bioJob) GenereateRightControlOffloadData(needPrepareData bool) (error, ScheduleAction) {
    _, action := job.asyncOffloadMgr.GenereateRightControlOffloadStage(needPrepareData)
    if action == DATA_PROVISON_ACTION_OFFLOAD_CHECK {
        return nil, S_ACTION_FINISH_JOB
    } else if action == DATA_PROVISON_ACTION_SCHEDULE {
        return nil, S_ACTION_SCHEDULE
    } else if action == DATA_PROVISON_ACTION_NONE {
        return nil, S_ACTION_NONE
    } else {
        return errors.New("No such aciton"), S_ACTION_PSEUDO_FINISH_JOB
    }
}

func (job *bioJob) RestoreData() (error, ScheduleAction) {
    SchedulerLogger.Infof("Try restore data of job %s", job.GetID())
    err, act := job.dataProvider.RestoreData()
    if err != nil {
        return err, S_ACTION_PSEUDO_FINISH_JOB
    }
    if act == DATA_PROVISON_ACTION_SCHEDULE {
        return nil, S_ACTION_SCHEDULE
    }else if act == DATA_PROVISON_ACTION_NONE {
        SchedulerLogger.Infof("No need restoring data for job %s", job.GetID())
        return nil, S_ACTION_FINISH_JOB
    }
    return errors.New("Invail state in job restoring"), S_ACTION_PSEUDO_FINISH_JOB
}

/*
 * Try to schedule job and get the ready stages.
 */
func (job *bioJob) ScheduleStages() (map[string]Stage, ScheduleAction, error){
    var graphStages map[string]Stage
    var err error
    var jobGraphState int
    var offloadStages map[string]Stage
    var needOffload bool = false

ReSchedule:
    SchedulerLogger.Infof("ScheduleStages(job %s): try scheduling", job.GetID())
    job.lock.Lock()
    state := job.ExecState()
    job.lock.Unlock()

    switch state {
        case INIT:
            SchedulerLogger.Infof("ScheduleStages(job %s): Exec State %s, run state machine first\n",
                job.GetID(), JobExecStateToString(state))
            execEvent := &JobExecEvent{
                stage: nil,
                event: JOB_START,
                lastStageState: 0,
            }
            err, _, action := job.RunExecStateMachine(execEvent)
            if err != nil {
                return nil, action, err
            }
            goto ReSchedule
        case RIGHTCONTROL, CLEANUPING:
            SchedulerLogger.Infof("ScheduleStages(job %s): Exec State %s, need cleanup or chown\n",
                job.GetID(), JobExecStateToString(state))
            offloadStages, needOffload, err = job.asyncOffloadMgr.GetReadyStage()
            if !needOffload {
                SchedulerLogger.Debugf("ScheduleStages(job %s): Don't need offload right control though it is in the state ",
                    job.GetID())
                if err != nil {
                    if state == RIGHTCONTROL {
                        SchedulerLogger.Debugf("ScheduleStages(job %s): state in RIGHTCONTROL, job complete",
                            job.GetID())
                        return nil, S_ACTION_FINISH_JOB, nil
                    } else if state == CLEANUPING {
                        return nil, S_ACTION_NONE, nil
                    } else {
                        return nil, S_ACTION_ABORT_JOB, errors.New("ScheduleStages(job): No such job state")
                    }
                }
                return nil, S_ACTION_NONE, nil
            }
        case SCHEDULING:
            graphStages, jobGraphState, err = job.graphScheduler.Schedule()
            if err != nil {
                return nil, S_ACTION_ABORT_JOB, err
            }
        default:
            SchedulerLogger.Infof("ScheduleStages(job %s): Exec State %s, no graph stage\n",
                job.GetID(), JobExecStateToString(state))
            graphStages = nil
    }

    readyStages, err := job.dataProvider.GetReadyStages(graphStages)
    if err != nil {
        return nil, S_ACTION_ABORT_JOB, err
    }

    if needOffload {
        for offloadStageId, offloadStage := range offloadStages {
            SchedulerLogger.Debugf("ScheduleStages(job %s): offload stageId: %s",
                job.GetID(), offloadStageId)
            readyStages[offloadStageId] = offloadStage
        }
    }

    if len(readyStages) == 0 || jobGraphState == JOB_GRAPH_NOBUILD {
        /*Run job state machine here to obtain its exact state*/
        execEvent := &JobExecEvent{
            stage: nil,
            event: JOB_CHECK_STATE,
            lastStageState: 0,
        }
        SchedulerLogger.Debugf("ScheduleStages(job %s): check job state in state machine",
            job.GetID())
        err, _, action := job.RunExecStateMachine(execEvent)
        if action == S_ACTION_SCHEDULE && len(graphStages) == 0 {
            SchedulerLogger.Debugf("ScheduleStages(job %s): job state is S_ACTION_SCHEDULE but no stages",
                job.GetID())
            goto ReSchedule
        }
        return nil, action, err
    } else {
        return readyStages, S_ACTION_SCHEDULE, nil
    }
}

func (job *bioJob) NeedPrepareData() bool {
    return job.dataProvider.NeedPrepare()
}

func (job *bioJob) NeedRestoreData() bool {
    return job.dataProvider.NeedPrepare()
}

func (job *bioJob) SetDataDir(dir string) {
    job.dataDir = dir
}

func (job *bioJob) DataDir() string {
    return job.dataDir
}

func (job *bioJob) TryRecoverData(stage Stage) {
    job.dataProvider.RecoverFromStage(stage)
}

func (job *bioJob) DeleteFiles() {
    if !job.dataProvider.NeedPrepare() {
        return
    }

    _, path := storage.GetStorageMgr().JobRootDir(job.GetID())
    err, fsPath := storage.GetStorageMgr().MapPathToSchedulerVolumeMount(path)
    if err != nil {
        SchedulerLogger.Errorf("Failed to remove directory for job %s: %s", job.GetID(), err.Error())
        return
    }
    err = storage.FSUtilsDeleteDir(fsPath, true)
    if err != nil {
        SchedulerLogger.Errorf("Failed to remove directory for job %s: %s", job.GetID(), err.Error())
    } else {
        SchedulerLogger.Infof("Remove directory for job %s", job.GetID())
    }
}

func (job *bioJob) CreateFlowGraphInfo() (FlowGraphInfo, error) {
    if job.Pipeline() == nil {
        return nil, nil
    }

    switch job.Pipeline().Type() {
    case PIPELINE_TYPE_BIOBPIPE:
        return NewBlFlowGraphInfo(), nil
    case PIPELINE_TYPE_WDL:
        return NewWomFlowGraphInfo(), nil
    default:
        return nil, errors.New("unknown pipeline type " + job.Pipeline().Type())
    }

}

func (job *bioJob) StageQuota() int {
    return job.stageQuota
}

func (job *bioJob) SetStageQuota(quota int) {
    job.lock.Lock()
    defer job.lock.Unlock()
    job.stageQuota = quota
}
