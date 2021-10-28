package scheduler

import (
    "errors"
    "encoding/json"
    ."github.com/xtao/bioflow/common"
    ."github.com/xtao/bioflow/scheduler/common"
    ."github.com/xtao/bioflow/message"
    "github.com/xtao/bioflow/eventbus"
    "time"
)

var JOBEVENTID string = "JOBEVENTID"
var FRONTENDNOTICEEVENT string = "FRONTENDNOTICEEVENTID"
var JOB string = "Job"
var STAGE string = "Stage"
var RETRYNUM int = 3
var RETRYDELAY int = 30000000

type JobInfo struct {
    job Job
    state int
    event int
}

func NewJobInfo(j Job, s, e int) *JobInfo {
    info := &JobInfo{
        job: j,
        state: s,
        event: e,
    }

    return info
}

type JobEvent struct{
    id string
    data *JobInfo
}

func (event *JobEvent) SetId(id string) {
    event.id = id
}

func(event *JobEvent) GetId() string {
    return event.id
}

func(event *JobEvent) SetData(a interface{}) error{
    if info, ok := a.(*JobInfo); ok {
        event.data = info
        return nil
    }

    return errors.New("The type of parameter must be *JobInfo.")
}

func(event *JobEvent) GetData() interface{} {
    return event.data
}

func NewJobEvent(i string, j Job, s, e int) *JobEvent {
    event := &JobEvent{
        id: i,
        data: nil,
    }

    event.data = NewJobInfo(j, s, e)
    return event
}

type NoticeEvent struct {
    id string
    data interface{}
}

func(e *NoticeEvent) SetId(id string) {
    e.id = id
}

func(e *NoticeEvent) GetId() string {
    return e.id
}

func(e *NoticeEvent) SetData(a interface{}) error {
    e.data = a
    return nil
}

func(e *NoticeEvent) GetData() interface{} {
    return e.data
}

func NewNoticeEvent(id string, data interface{}) *NoticeEvent {
    return &NoticeEvent{
        id: id,
        data: data,
    }
}

type MailNoticeHandler struct {
    to string
    subject string
    toMsg func(interface{})(string, error)
}

func (h *MailNoticeHandler) Handle(a interface{}){
    if h.toMsg == nil || h.to == ""{
        SchedulerLogger.Errorf("Fail to send e-mail: no message or receiver address set\n")
        return
    }

    msg, err := h.toMsg(a)
    if err != nil {
        SchedulerLogger.Errorf("Fail to send e-mail: parse message err\n")
        return
    }

    receiver, _ := eventbus.NewMailReceiver(h.to, h.subject, msg)
    err = receiver.TryReceive(a)
    if err != nil{
        SchedulerLogger.Errorf("Fail to send e-mail to user %s:   %s\n", h.to, err.Error())
    } else {
        SchedulerLogger.Infof("Successfully send e-mail to user %s\n", h.to)
    }
    return
}

func NewMailNoticeHandler(to, subject string, fnc func(interface{})(string, error)) *MailNoticeHandler{
    return &MailNoticeHandler{
        to: to,
        subject: subject,
        toMsg: fnc,
    }
}

func JobStatusToMsg (a interface{}) (string, error) {
    msg, ok := a.(string)
    if !ok {
        return "", errors.New("Fail to convert job status.")
    }
    return msg, nil
}

type JobNoticeInfo struct {
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
}

type StageNoticeInfo struct {
    JobId string
    JobName string
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
    ExecMode string
    HostName string
    HostIP string
    ResourceStats ResourceUsageInfo
}

type FrontEndNotcieInfo struct {
    Type string
    JobInfo *JobNoticeInfo
    StageInfo *StageNoticeInfo
}

func NewFrontEndNotcieInfo (args ...interface{}) *FrontEndNotcieInfo {
    if len(args) == 1 {
        if status, ok := args[0].(*BioflowJobStatus); ok {
            jobInfo := &JobNoticeInfo{
                JobId: status.JobId,
                Name: status.Name,
                Pipeline: status.Pipeline,
                Owner: status.Owner,
                WorkDir: status.WorkDir,
                HDFSWorkDir: status.HDFSWorkDir,
                Created: status.Created,
                Finished: status.Finished,
                State: status.State,
                PausedState: status.PausedState,
                RunCount: status.RunCount,
                Priority: status.Priority,
                RetryLimit: status.RetryLimit,
                StageCount: status.StageCount,
                StageQuota: status.StageQuota,
                ExecMode: status.ExecMode,
                FailReason: status.FailReason,
                GraphCompleted: status.GraphCompleted,
                PipelineBuildPos: status.PipelineBuildPos,
            }
            data := &FrontEndNotcieInfo{
                Type: JOB,
                JobInfo: jobInfo,
            }
            return data
        }else{
            return nil
        }
    } else if len(args) == 3 {
        jobId, ok1 := args[0].(string)
        jobName, ok2 := args[1].(string)
        stageInfo, ok3 := args[2].(*BioflowStageInfo)
        if !ok1 || !ok2 || !ok3{
            return nil
        }
        stageNoticeInfo := &StageNoticeInfo{
            JobId: jobId,
            JobName: jobName,
            Id: stageInfo.Id,
            Name: stageInfo.Name,
            State: stageInfo.State,
            Command: stageInfo.Command,
            Output: stageInfo.Output,
            RetryCount: stageInfo.RetryCount,
            BackendId: stageInfo.BackendId,
            TaskId: stageInfo.TaskId,
            SubmitTime: stageInfo.SubmitTime,
            ScheduleTime: stageInfo.ScheduleTime,
            FinishTime: stageInfo.FinishTime,
            TotalDuration: stageInfo.TotalDuration,
            FailReason: stageInfo.FailReason,
            CPU: stageInfo.CPU,
            Memory: stageInfo.Memory,
            ServerType: stageInfo.ServerType,
            ExecMode: stageInfo.ExecMode,
            HostName: stageInfo.HostName,
            HostIP: stageInfo.HostIP,
            ResourceStats: stageInfo.ResourceStats,
        }
        data := &FrontEndNotcieInfo{
            Type: STAGE,
            StageInfo: stageNoticeInfo,
        }
        return data
    } else {
        return nil
    }
}

type FrontEndHandler struct {
    url string
    toBody func(interface{})([]byte, error)
}

func (h *FrontEndHandler) Handle(a interface{}){
    if h.url == "" || h.toBody == nil {
        return
    }
    body, err := h.toBody(a)
    if err != nil {
        return
    }

    notice, _ := eventbus.NewRestAPI(h.url, body)
    for  i := 0; i < RETRYNUM ; i++ {
        _, err = notice.TryCall(a)
        if err != nil {
            SchedulerLogger.Error("Fail to post information to %s:   %s\n", h.url, err.Error())
            time.Sleep(time.Duration(RETRYDELAY))
            continue
        } else {
            break
        }
    }

    return
}

func NewRestAPINoticeHandler(url string, fnc func(interface{})([]byte, error)) *FrontEndHandler{
    return &FrontEndHandler{
        url: url,
        toBody: fnc,
    }
}

func ToBody (a interface{}) ([]byte, error) {
    info, ok := a.(*FrontEndNotcieInfo)
    if !ok {
        return nil, errors.New("Fail to convert notice information.")
    }
    if info == nil {
        return nil, errors.New("Empty information.")
    }
    body, err := json.Marshal(info)
    if err != nil {
        return nil, err
    }
    return body, nil
}

type FrontEndMgr struct {
    subscribers map[string]*eventbus.Subscriber
}

func(watcher *FrontEndMgr)Subscribe(url string) {
    if _, ok := watcher.subscribers[url]; ok {
        return
    }
    subscriber := eventbus.NewSubscriber()
    handler := NewRestAPINoticeHandler(url, ToBody)
    subscriber.Subscribe(FRONTENDNOTICEEVENT, handler.Handle, true)
    watcher.subscribers[url] = subscriber
}

func(watcher *FrontEndMgr)Unsubscribe(url string) {
    subscriber, ok := watcher.subscribers[url]
    if !ok {
        return
    }
    subscriber.Unsubscribe(FRONTENDNOTICEEVENT)
    delete(watcher.subscribers, url)
}

var frontEndMgr *FrontEndMgr

func GetRestApiMgr() *FrontEndMgr {
    if frontEndMgr == nil {
        frontEndMgr = &FrontEndMgr{
            subscribers: make(map[string]*eventbus.Subscriber),
        }
    }
    return frontEndMgr
}
