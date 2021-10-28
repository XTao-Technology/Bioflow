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
    "github.com/xtao/bioflow/scheduler/backend"
    "container/list"
    "sync/atomic"
    "sync"
    "time"
    "fmt"
    "errors"
    "io"

    . "github.com/xtao/bioflow/common"
    . "github.com/xtao/bioflow/message"
    . "github.com/xtao/bioflow/scheduler/common"
)

/*errors*/
var (
    BE_ERR_NO_BACKEND error = errors.New("No available backend to schedule task")
)

const (
    BE_HEALTH int = 0
    BE_WARNING int = 1
    BE_QUIESCED int = 2
    BE_DISABLED int = 3

    BE_HEALTH_CHECK_TIMEOUT time.Duration = 2
    BE_FAILBACK_TIMEOUT time.Duration = 60
)

type backendScheduler interface {
    EnableBackend(backendId string) error
    QuiesceBackend(backendId string, disable bool) error
    SelectBackend() string
    AddBackend(backend backend.ScheduleBackend)
    DeleteBackend(id string) error
    GetBackendListInfo() (error, []BioflowBackendInfo)
    GetBackendInfo(id string) (error, *BioflowBackendInfo)
    ReclaimTask(backendId string, taskId string) error
    RecoverBackendTask(backendId string, taskId string) (error, int)
    ScheduleTask(task *backend.ScheduleBackendTask) (error, string, string)
    KillTask(backendId string, taskId string) error
    CheckTask(backendId string, taskId string) (error, *backend.TaskStatus)
    GetSandboxFile(backendId string, taskId string, fileName string) ([]byte, error)
    FetchSandboxFile(backendId string, taskId string, fileName string, localFile string) error
    ParseTaskNotify(backendId string, msg io.ReadCloser) (error, *backend.TaskEvent)
    TrackJobTask(backendId string, taskId string, job Job) error
    UnTrackJobTask(backendId string, taskId string) error
    GetJobFromTask(backendId string, taskId string) (error, Job)
    GetRemovableSlaves() (error, []string)
    BackendCount() int
    Start()
    DumpBackendInfo()
}

/*
 * Backend descriptor
 */
type backendDesc struct {
    lock sync.Mutex
    callbackURI string
    backend backend.ScheduleBackend
    taskToJob map[string]Job
    taskCount uint64

    status int
    failCount uint32
    lastFailAt time.Time

    /*indicate whether the backend always fail since last fail*/
    stickInFail bool
    contigFailCount uint32
    startFailTime time.Time
    failbackDone bool
}

const BE_MAX_FAIL_COUNT uint32 = 5

func (be *backendDesc) _MarkBackendErr() {
    atomic.AddUint32(&be.failCount, 1)
    be.SetStatus(BE_WARNING)
    be.lastFailAt = time.Now()

    if be.stickInFail {
        atomic.AddUint32(&be.contigFailCount, 1)
    } else {
        be.startFailTime = be.lastFailAt
        be.stickInFail = true
    }

    SchedulerLogger.Infof("backend :%s failCount:%d",
        be.backend.GetID(), be.failCount)
    if be.failCount >  BE_MAX_FAIL_COUNT {
        beSched := GetBeScheduler()
        if beSched != nil {
            SchedulerLogger.Infof("!!!Quesce backend :%s",
                be.backend.GetID())
            beSched.QuiesceBackend(be.backend.GetID(), false)
        }
    }
}

func (be *backendDesc) _MarkBackendHealth() {
    be.stickInFail = false
    be.contigFailCount = 0
    be.failbackDone = false
}

func (be *backendDesc) _Quiesced() bool {
    return be.status == BE_QUIESCED
}

func (be *backendDesc) _Disabled() bool {
    return be.status == BE_DISABLED
}

func (be *backendDesc) _Health() bool {
    return be.status == BE_HEALTH
}

func (be *backendDesc) _ClearBackendErr() {
    be.failCount = 0
    if be._Quiesced() {
        beSched := GetBeScheduler()
        if beSched != nil {
            SchedulerLogger.Infof("!!!Renable backend :%s",
                be.backend.GetID())
            beSched.EnableBackend(be.backend.GetID())
        }
    }
}

func (be *backendDesc) _NeedFailbackTasks() bool {
    if be.stickInFail && !be.failbackDone {
        if time.Since(be.startFailTime) > time.Minute * BE_FAILBACK_TIMEOUT {
            if be.taskCount > 0 {
                return true
            }
        }
    }

    return false
}

func (be *backendDesc) _NeedCheckHealth() bool {
    if time.Since(be.lastFailAt) > time.Minute * BE_HEALTH_CHECK_TIMEOUT {
        return true
    }

    return false
}

func (be *backendDesc) _MarkFailbackDone() {
    be.failbackDone = true
}

func NewBackendDesc(backend backend.ScheduleBackend, callbackURI string) *backendDesc{
    bkDesc := &backendDesc{
        backend: backend,
        taskCount: 0,
        taskToJob: make(map[string]Job),
        status: BE_HEALTH,
        failCount: 0,
        lastFailAt: time.Now(),
        contigFailCount: 0,
        stickInFail: false,
    }
    bkDesc.callbackURI = fmt.Sprintf("%s/%s", callbackURI, backend.GetID())

    return bkDesc
}

func (be *backendDesc) SetStatus(status int) {
    be.status = status
}

func (be *backendDesc) GetStatus() int {
    return be.status
}

func (be *backendDesc) GetStatusStr() string {
    var str string
    switch be.status {
    case BE_HEALTH:
        str = "Health"
    case BE_WARNING:
        str = "Warning"
    case BE_QUIESCED:
        str = "Quiesced"
    case BE_DISABLED:
        str = "Disabled"
    default:
        str = "Unknown"
    }
    return str
}

func (be *backendDesc) GetTaskCount() uint64 {
    return be.taskCount
}

func (be *backendDesc) GetFailCount() uint32 {
    return be.failCount
}

func (be *backendDesc) GetLastFailAt() string {
    return be.lastFailAt.Format(BIOFLOW_TIME_LAYOUT)
}

func (be *backendDesc) MapTaskToJob(taskId string, job Job) {
    be.lock.Lock()
    defer be.lock.Unlock()

    if _, found := be.taskToJob[taskId]; !found {
        be.taskToJob[taskId] = job
        be.taskCount ++
    } else {
        SchedulerLogger.Errorf("Map an exist task %s/%s to backend\n",
            taskId, job.GetID())
    }
}

func (be *backendDesc) GetMappedTasks() []string {
    var tasks []string = nil

    be.lock.Lock()
    defer be.lock.Unlock()
    for id, _ := range be.taskToJob {
        tasks = append(tasks, id)
    }

    return tasks
}

func (be *backendDesc) UnMapTask(taskId string) {
    be.lock.Lock()
    defer be.lock.Unlock()

    if _, found := be.taskToJob[taskId]; found {
        delete(be.taskToJob, taskId)
        be.taskCount --
    }
}

func (be *backendDesc) GetJobOfTask(taskId string) Job {
    be.lock.Lock()
    defer be.lock.Unlock()

    if job, ok := be.taskToJob[taskId]; ok {
        return job
    } else {
        return nil
    }
}

func (be *backendDesc) GetTaskSummary() string {
    be.lock.Lock()
    defer be.lock.Unlock()

    summary := ""
    for taskId, job := range be.taskToJob {
        if summary != "" {
            summary += ","
        }
        summary += taskId + ":" + job.GetID()
    }

    return summary
}

type baseBeScheduler struct {
    lock sync.RWMutex
    backends         map[string]*backendDesc
    callbackURI string
}

func NewBaseBeScheduler(callback string) *baseBeScheduler {
    return &baseBeScheduler {
        backends: make(map[string]*backendDesc),
        callbackURI: callback,
    }
}

func (baseScheduler *baseBeScheduler) CallbackURI() string {
    return baseScheduler.callbackURI
}

func (baseScheduler *baseBeScheduler) BackendCount() int {
    baseScheduler.lock.RLock()
    defer baseScheduler.lock.RUnlock()

    return len(baseScheduler.backends)
}

func (baseScheduler *baseBeScheduler) AddBackend(backend backend.ScheduleBackend) {
    bkDesc := NewBackendDesc(backend, baseScheduler.callbackURI)
    baseScheduler.lock.Lock()
    baseScheduler.backends[backend.GetID()] = bkDesc
    baseScheduler.lock.Unlock()
}

func (baseScheduler *baseBeScheduler) DeleteBackend(id string) error {
    baseScheduler.lock.Lock()
    defer baseScheduler.lock.Unlock()

	be := baseScheduler.backends[id]
	if be.taskCount != 0 {
		errmsg := fmt.Sprintf("backend %s is quiescing.\n", id)
		SchedulerLogger.Error(errmsg)
		return errors.New(errmsg)
	}
	delete(baseScheduler.backends, id)
	return nil
}

func (baseScheduler *baseBeScheduler) GetBackend(id string) backend.ScheduleBackend {
    backendDesc := baseScheduler.GetBackendDesc(id)
    if backendDesc != nil {
        return backendDesc.backend
    } else {
        return nil
    }
}

func (baseScheduler *baseBeScheduler) GetBackendDesc(id string) *backendDesc {
    baseScheduler.lock.RLock()
    defer baseScheduler.lock.RUnlock()

    if backendDesc, ok := baseScheduler.backends[id]; ok {
        return backendDesc
    } else {
        return nil
    }
}

func (baseScheduler *baseBeScheduler) DumpBackendInfo() {
    baseScheduler.lock.RLock()
    defer baseScheduler.lock.RUnlock()

    taskInfo := ""
    for id, backendDesc := range baseScheduler.backends {
        taskSummary := backendDesc.GetTaskSummary()
        taskInfo += "backend " + id + " tasks: " + taskSummary + "\n"
    }

    SchedulerLogger.Infof("Start Dump Backend TaskInfo: \n %s End Dump Backend TaskInfo", taskInfo)
}

func (baseScheduler *baseBeScheduler) GetBackendListInfo() (error, []BioflowBackendInfo) {
    baseScheduler.lock.RLock()
    defer baseScheduler.lock.RUnlock()

	beDescs := make([]BioflowBackendInfo, 0)
	for id, be := range baseScheduler.backends {
		b := BioflowBackendInfo {
			Id: id,
			Server: be.backend.GetServer(),
			Type: be.backend.GetType(),
			TaskCount: be.GetTaskCount(),
			FailCount: be.GetFailCount(),
			LastFailAt:be.GetLastFailAt(),
			Status: be.GetStatusStr(),
		}
		beDescs = append(beDescs, b)
	}

	return nil, beDescs
}

func (baseScheduler *baseBeScheduler) Start() {
    scheduler := GetScheduler()
    go func() {
        for {
            time.Sleep(time.Minute * 1)
            var failbackBackends []*backendDesc = nil
            var healthCheckBackends []*backendDesc = nil
            baseScheduler.lock.RLock()
            for _, be := range baseScheduler.backends {
                if be._Health() {
                    if scheduler.Quiesced() {
                        SchedulerLogger.Info("backend checker resume backend %s\n",
                            be.backend.GetID())
                        scheduler.Resume()
                    }
                    continue
                }
                /*Don't check backends disabled by user*/
                if be._Disabled() {
                    continue
                }

                if be._NeedCheckHealth() {
                    healthCheckBackends = append(healthCheckBackends, be)
                }

                /*check whether fail back tasks to avoid deadlock*/
                if be._NeedFailbackTasks() {
                    failbackBackends = append(failbackBackends, be)
                }
            }
            baseScheduler.lock.RUnlock()

            /*Check health of quiesced backends*/
            for _, be := range healthCheckBackends {
                if be != nil && be.backend != nil {
                    if err := be.backend.Ping(); err == nil {
                        SchedulerLogger.Infof("Succeed to ping backend %s, re-enable it\n",
                            be.backend.GetID())
                        /*ping success means we needn't fail-back tasks*/
                        be._MarkBackendHealth()
                        be._ClearBackendErr()
                        if scheduler.Quiesced() {
                            SchedulerLogger.Info("Backend health checker resume backend %s\n",
                                be.backend.GetID())
                            scheduler.Resume()
                        }
                    } else {
                        SchedulerLogger.Errorf("Fail to ping backend %s, stuck in fail state\n",
                            be.backend.GetID())
                    }
                }
            }
            
            /*Fail back tasks outside the critical section*/
            for _, be := range failbackBackends {
                if be != nil && be._NeedFailbackTasks() {
                    tasks := be.GetMappedTasks()
                    SchedulerLogger.Infof("Fail back %d tasks for backend %s\n",
                        len(tasks), be.backend.GetID())
                    scheduler.FailbackTasks(be.backend.GetID(), tasks)
                    be._MarkFailbackDone()
                }
            }
        }
    } ()
}

func (baseScheduler *baseBeScheduler) GetBackendInfo(id string) (error, *BioflowBackendInfo) {
    baseScheduler.lock.RLock()
    defer baseScheduler.lock.RUnlock()

	if be, ok := baseScheduler.backends[id]; ok {
		b := &BioflowBackendInfo {
			Id: id,
			Server: be.backend.GetServer(),
			Type: be.backend.GetType(),
			TaskCount: be.GetTaskCount(),
			FailCount: be.GetFailCount(),
			LastFailAt:be.GetLastFailAt(),
			Status: be.GetStatusStr(),
		}

        return nil, b
	} else {
        return errors.New("Invalid id"), nil
    }
}

func (baseScheduler *baseBeScheduler) TrackJobTask(backendId string,
    taskId string, job Job) error {
    backend := baseScheduler.GetBackendDesc(backendId)
    if backend == nil {
        return errors.New("Backend " + backendId + " not exist")
    }
    backend.MapTaskToJob(taskId, job)
    return nil
}

func (baseScheduler *baseBeScheduler) UnTrackJobTask(backendId string,
    taskId string) error {
    backend := baseScheduler.GetBackendDesc(backendId)
    if backend == nil {
        SchedulerLogger.Errorf("Invalid backend id %s for task %s\n",
            backendId, taskId)
        return errors.New("Invalid backend id " + backendId)
    }
    backend.UnMapTask(taskId)
    return nil
}

func (baseScheduler *baseBeScheduler) GetJobFromTask(backendId string,
    taskId string) (error, Job) {
    backendDesc := baseScheduler.GetBackendDesc(backendId)
    if backendDesc == nil {
        SchedulerLogger.Errorf("Invalid backend id %s \n",
            backendId)
        return errors.New("Invalid backend id " + backendId),
            nil
    }

    job := backendDesc.GetJobOfTask(taskId)
    return nil, job
}

const (
    RECOVER_TASK_OPERR int = -1
    RECOVER_TASK_RUNNING int = 0
    RECOVER_TASK_FINISHED int = 1
    RECOVER_TASK_QUEUED int = 2
    RECOVER_TASK_FAIL int = 3
    RECOVER_TASK_LOST int = 4
    RECOVER_TASK_UNKNOWN int = 5
)

/*
 * Recover a pending task's state by check with the backend. it will
 * be called by job recovery process.
 */
func (baseScheduler *baseBeScheduler) RecoverBackendTask(backendId string,
    taskId string) (error, int) {

    /*
     * Check whether the task id is valid or not
     */
    if taskId == "" {
        SchedulerLogger.Infof("Recover anonymous task, mark lost \n")
        return errors.New("recover anonymous task"), -1
    }

    /*validate the backend*/
    backendIns := baseScheduler.GetBackend(backendId)
    if backendIns == nil {
        SchedulerLogger.Errorf("The backend id %s not exist\n",
            backendId)
        return errors.New("Backend " + backendId + " not exist"),
            RECOVER_TASK_OPERR
    }

    GetDashBoard().UpdateStats(STAT_OP_CHECK_TASK, 1)
    err, taskStatus := backendIns.CheckTask(taskId)
    if err != nil {
        GetDashBoard().UpdateStats(STAT_OP_CHECK_TASK_FAIL, 1)
        SchedulerLogger.Errorf("Check task %s fail, mark it lost\n",
            err.Error())
        return err, RECOVER_TASK_OPERR
    }

    switch taskStatus.State {
        case backend.TASK_RUNNING:
            SchedulerLogger.Infof("Check task %s state running, use it\n",
                taskId)
            return nil, RECOVER_TASK_RUNNING
        case backend.TASK_STAGING,backend.TASK_STARTING,
            backend.TASK_TERMINATING,backend.TASK_QUEUED:
            SchedulerLogger.Infof("Check task %s preparing or terminating, use it\n",
                taskId)
            return nil, RECOVER_TASK_QUEUED
        case backend.TASK_FINISHED:
            SchedulerLogger.Infof("Check task %s finished, use it\n",
                taskId)
            return nil, RECOVER_TASK_FINISHED
        case backend.TASK_FAIL, backend.TASK_KILLED, backend.TASK_ERROR, backend.TASK_GONE,
            backend.TASK_DROPPED:
            SchedulerLogger.Infof("Check task %s failed or killed, use it\n",
                taskId)
            return nil, RECOVER_TASK_FAIL
        case backend.TASK_UNKNOWN:
            SchedulerLogger.Infof("Check task %s unknown, regard it lost\n",
                taskId)
            return nil, RECOVER_TASK_UNKNOWN
        default:
            SchedulerLogger.Infof("Check task %s unknown state %d, kill it\n",
                taskId, taskStatus.State)
            err := backendIns.KillTask(taskId)
            if err != nil {
                SchedulerLogger.Errorf("Recover kill task %s fail: %s\n",
                    taskId, err.Error())
            } else {
                SchedulerLogger.Infof("Recover kill task %s success\n",
                    taskId)
            }
            return nil, RECOVER_TASK_OPERR
    }
}

/*
 * Delete a task in backends. It is to reclaim the resources
 * associated with the task in backends. 
 */
func (baseScheduler *baseBeScheduler) ReclaimTask(backendId string,
    taskId string) error {

    if taskId == "" {
        SchedulerLogger.Infof("Delete anonymous task, no nothing\n")
        return nil
    }

    /*validate the backend*/
    backendIns := baseScheduler.GetBackend(backendId)
    if backendIns == nil {
        SchedulerLogger.Errorf("The backend id %s not exist\n",
            backendId)
        return errors.New("Backend " + backendId + " not exist")
    }

    GetDashBoard().UpdateStats(STAT_OP_DELETE_TASK, 1)
    err := backendIns.DeleteTask(taskId)
    if err != nil {
        GetDashBoard().UpdateStats(STAT_OP_DELETE_TASK_FAIL, 1)
        SchedulerLogger.Errorf("Delete the task %s in %s fail: %s\n",
            taskId, backendId, err.Error())
        return err
    }

    return nil
}

func (baseScheduler *baseBeScheduler) KillTask(backendId string,
    taskId string) error {

    if taskId == "" {
        SchedulerLogger.Infof("Kill anonymous task, no nothing\n")
        return nil
    }

    /*validate the backend*/
    backendIns := baseScheduler.GetBackend(backendId)
    if backendIns == nil {
        SchedulerLogger.Errorf("The backend id %s not exist\n",
            backendId)
        return errors.New("Backend " + backendId + " not exist")
    }

    GetDashBoard().UpdateStats(STAT_OP_KILL_TASK, 1)
    err := backendIns.KillTask(taskId)
    if err != nil {
    GetDashBoard().UpdateStats(STAT_OP_KILL_TASK_FAIL, 1)
        SchedulerLogger.Errorf("Kill the task %s in %s fail: %s\n",
            taskId, backendId, err.Error())
        return err
    }

    return nil
}

func (baseScheduler *baseBeScheduler) CheckTask(backendId string,
    taskId string) (error, *backend.TaskStatus) {
    if backendId == "" || taskId == "" {
        SchedulerLogger.Infof("Check anonymous task, no nothing\n")
        return errors.New("Empty backend task id"),
            nil
    }

    /*validate the backend*/
    backend := baseScheduler.GetBackend(backendId)
    if backend == nil {
        SchedulerLogger.Errorf("The backend id %s not exist\n",
            backendId)
        return errors.New("Backend " + backendId + " not exist"),
            nil
    }

    GetDashBoard().UpdateStats(STAT_OP_CHECK_TASK, 1)
    err, taskState := backend.CheckTask(taskId)
    if err != nil {
        GetDashBoard().UpdateStats(STAT_OP_CHECK_TASK_FAIL, 1)
        SchedulerLogger.Errorf("Check the task %s in %s fail: %s\n",
            taskId, backendId, err.Error())
        return err, nil
    }

    return nil, taskState
}

func (baseScheduler *baseBeScheduler) FetchSandboxFile(backendId string,
    taskId string, fileName string, localFile string) error {
    /*validate the backend*/
    backend := baseScheduler.GetBackend(backendId)
    if backend == nil {
        SchedulerLogger.Errorf("The backend id %s not exist\n",
            backendId)
        return errors.New("Backend " + backendId + " not exist")
    }

    GetDashBoard().UpdateStats(STAT_OP_GET_LOG, 1)
    err := backend.FetchSandboxFile(taskId, fileName, localFile)
    if err != nil {
        GetDashBoard().UpdateStats(STAT_OP_GET_LOG_FAIL, 1)
        SchedulerLogger.Errorf("Check the task %s in %s fail: %s\n",
            taskId, backendId, err.Error())
        return err
    }

    return nil
}

func (baseScheduler *baseBeScheduler) GetSandboxFile(backendId string,
    taskId string, fileName string) ([]byte, error) {
    /*validate the backend*/
    backend := baseScheduler.GetBackend(backendId)
    if backend == nil {
        SchedulerLogger.Errorf("The backend id %s not exist\n",
            backendId)
        return nil, errors.New("Backend " + backendId + " not exist")
    }

    GetDashBoard().UpdateStats(STAT_OP_GET_LOG, 1)
    buf, err := backend.GetSandboxFile(taskId, fileName)
    if err != nil {
        GetDashBoard().UpdateStats(STAT_OP_GET_LOG_FAIL, 1)
        SchedulerLogger.Errorf("Check the task %s in %s fail: %s\n",
            taskId, backendId, err.Error())
        return nil, err
    }

    return buf, nil
}

/*
 * When this function is called, it means that a notify from the backend
 * is received. So set the stickInFail to false to avoid fail back the
 * backend's tasks by mistake.
 */
func (baseScheduler *baseBeScheduler) ParseTaskNotify(backendId string,
    msg io.ReadCloser) (error, *backend.TaskEvent) {
    be := baseScheduler.GetBackendDesc(backendId)
    if be == nil || be.backend == nil {
        SchedulerLogger.Errorf("Can't get backend by invaid backendId %s\n",
            backendId)
        return errors.New("Invalid backend id " + backendId),
            nil
    }

    GetDashBoard().UpdateStats(STAT_OP_TASK_NOTIFY, 1)

    /*A notification is received, so mark it not stickInFail*/
    be._MarkBackendHealth()

    return be.backend.ParseTaskNotify(msg)
}

var globalBeScheduler backendScheduler = nil

/*
 * RoundRobin backend scheduler
 */
type RRBeScheduler struct {
    baseBeScheduler
    beLock *sync.RWMutex
    backendList *list.List
    cursor *list.Element

    scheduler Scheduler
}

func NewRRBeScheduler(scheduler Scheduler, callbackURI string) *RRBeScheduler {
    beScheduler := &RRBeScheduler {
        baseBeScheduler: *NewBaseBeScheduler(callbackURI),
        scheduler: scheduler,
    }
    beScheduler.beLock = new(sync.RWMutex)
    beScheduler.backendList = list.New()
    beScheduler.cursor = nil

    globalBeScheduler = beScheduler

    return beScheduler
}

func GetBeScheduler() backendScheduler {
    return globalBeScheduler
}

func (beScheduler *RRBeScheduler) EnableBackend(backendId string) error{
    beScheduler.beLock.Lock()
    defer beScheduler.beLock.Unlock()

    if backend := beScheduler.GetBackendDesc(backendId); backend != nil {
	    backend.SetStatus(BE_HEALTH)
    } else {
        SchedulerLogger.Errorf("Enable a invalid backend\n")
        return errors.New("Enable invalid backend")
    }

    beScheduler.backendList.PushBack(backendId)
    if beScheduler.cursor == nil {
        beScheduler.cursor = beScheduler.backendList.Front()
    }

    SchedulerLogger.Infof("Enabled the backend %s\n",
        backendId)

    return nil
}

func (beScheduler *RRBeScheduler) QuiesceBackend(backendId string, disable bool) error {
    beScheduler.beLock.Lock()
    defer beScheduler.beLock.Unlock()
	
    if backend := beScheduler.GetBackendDesc(backendId); backend != nil {
        SchedulerLogger.Infof("The backend %s status set to disabled\n",
            backendId)
        if disable {
            backend.SetStatus(BE_DISABLED)
        } else {
            backend.SetStatus(BE_QUIESCED)
        }
    } else {
        SchedulerLogger.Errorf("The backend %s not exist, can't quiesce\n",
            backendId)
        return errors.New("Backend not exist")
    }

    var e *list.Element
    for e = beScheduler.backendList.Front(); e != nil; e = e.Next() {
        if e.Value == backendId {
            break
        }
    }

    if e != nil {
        beScheduler.backendList.Remove(e)
        SchedulerLogger.Infof("The backend %s is quiesced\n",
            backendId)
    } else {
        SchedulerLogger.Errorf("Quiesce backend %s not exist or quiesced already\n",
            backendId)
        return errors.New("Backend already quiesced")
    }

    return nil
}

func (beScheduler *RRBeScheduler) SelectBackend() string {
    var backendId interface{}

    beScheduler.beLock.RLock()
    defer beScheduler.beLock.RUnlock()

    if beScheduler.backendList.Len() < 1 {
        return ""
    }

    if beScheduler.backendList.Len() == 1 {
        backendId = beScheduler.cursor.Value
        return backendId.(string)
    }

    backendId = beScheduler.cursor.Value

    if beScheduler.cursor.Next() == nil {
        beScheduler.cursor = beScheduler.backendList.Front()
    } else {
        beScheduler.cursor = beScheduler.cursor.Next()
    }

    return backendId.(string)
}

func (beScheduler *RRBeScheduler) ScheduleTask(task *backend.ScheduleBackendTask) (error, string, string) {
    backendId := beScheduler.SelectBackend()
    if backendId == "" {
        SchedulerLogger.Errorf("No available backend to schedule\n")
        return BE_ERR_NO_BACKEND, "", ""
    }

    be := beScheduler.GetBackendDesc(backendId)
    if be == nil {
        SchedulerLogger.Errorf("Backend scheduler select an invalid backend %s\n",
            backendId)
        return errors.New("Inconsistent backend id"), "", ""
    }

    /*task notify handler is per-backend*/
    task.CallBackURI = be.callbackURI

    err, taskId := be.backend.SubmitTask(task)
    if err != nil {
        be._MarkBackendErr()
        SchedulerLogger.Errorf("Submit task to backend %s failure\n",
            backendId)
    } else {
        be._MarkBackendHealth()
    }

    return err, backendId, taskId
}

func (beScheduler *RRBeScheduler) GetRemovableSlaves() (error, []string) {
    backendId := beScheduler.SelectBackend()
    if backendId == "" {
        SchedulerLogger.Errorf("No available backend to schedule\n")
        return BE_ERR_NO_BACKEND, nil
    }

    be := beScheduler.GetBackendDesc(backendId)
    if be == nil {
        SchedulerLogger.Errorf("Backend scheduler select an invalid backend %s\n",
            backendId)
        return errors.New("Inconsistent backend id"), nil
    }

    err, slaves := be.backend.GetRemovableSlaves()
    if err != nil {
        be._MarkBackendErr()
        SchedulerLogger.Errorf("Failed to get removable mesos slaves from %s\n",
            backendId)
    }

    return err, slaves
}
