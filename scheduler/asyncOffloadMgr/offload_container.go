package asyncOffloadMgr

import (
    "sync"
    "github.com/xtao/bioflow/common"
    "github.com/xtao/bioflow/storage"
    . "github.com/xtao/bioflow/scheduler/common"
    "errors"
)

const (
    INITIAL int = 0
    RUNNING int = 1
    COMPLETE int = 2
)

type AsyncOffloadStage struct {
    stages map[string]Stage
    status map[string]int
    lock *sync.RWMutex
}

func NewOffloadStage() *AsyncOffloadStage {
    return &AsyncOffloadStage {
        lock: new(sync.RWMutex),
        stages: make(map[string]Stage),
        status: make(map[string]int),
    }
}

func (OffloadStage *AsyncOffloadStage)NewStageId(id OFFLOADMETADATAACTION) string {
    var stageId string
    if id == CLEANUP {
        stageId = cleanup + OFFLOAD
    } else {
        stageId = chown + OFFLOAD
    }
    return stageId
}

func (OffloadStage *AsyncOffloadStage)AddStage(stage Stage, stageId string) {
    OffloadStage.lock.Lock()
    defer OffloadStage.lock.Unlock()
    OffloadStage.stages[stageId] = stage
    OffloadStage.status[stageId] = INITIAL
}

func (OffloadStage *AsyncOffloadStage)DeleteStage(stageId string) {
    OffloadStage.lock.Lock()
    defer OffloadStage.lock.Unlock()
    delete(OffloadStage.stages, stageId)
    delete(OffloadStage.status, stageId)
}

type AsyncOffload struct {
    job Job
    stage *AsyncOffloadStage
    isBusy bool
    lock *sync.Mutex
}

func NewAsyncOffloadContainer(job Job) *AsyncOffload {
    return &AsyncOffload {
        job: job,
        stage: NewOffloadStage(),
        lock: new(sync.Mutex),
    }
}


func (asyncOffload *AsyncOffload)GetReadyStage() (map[string]Stage,
    bool, error) {
    asyncOffload.lock.Lock()
    defer asyncOffload.lock.Unlock()

    readyStages := make(map[string]Stage)

    if len(asyncOffload.stage.stages) == 0 {
        return nil, false, errors.New("no need offload stage")
    }

    for stageId, status := range asyncOffload.stage.status {
        if status == INITIAL {
            common.SchedulerLogger.Infof("GetReadyStage: stage %s will be running!",
                stageId)
            readyStages[stageId] = asyncOffload.stage.stages[stageId]
        } else {
            common.SchedulerLogger.Infof("GetReadyStage: stage %s is running!",
                stageId)
            return nil, false, nil
        }
    }

    return readyStages, true, nil
}

func (asyncOffload *AsyncOffload)HandleStageEvent(stageId string, event STAGEEVENT) {
    common.SchedulerLogger.Infof("HandleStageEvent offload stage: %s\n", stageId)

    switch event {
    case DATA_DONE, DATA_ABORT:
        asyncOffload.stage.DeleteStage(stageId)
    case DATA_LOST, DATA_FAIL, DATA_SUBMIT_FAIL:
        asyncOffload.stage.status[stageId] = INITIAL
    }
}

func (asyncOffload *AsyncOffload)GetStageByStageId(stageId string) Stage {
    return asyncOffload.stage.stages[stageId]
}

func (asyncOffload *AsyncOffload)GenereateCleanupOffloadStage() (error, ACTION) {
    asyncOffload.lock.Lock()
    defer asyncOffload.lock.Unlock()

    if asyncOffload.isBusy == true {
        common.SchedulerLogger.Debugf("The offload metadata is busy!")
        return nil, DATA_PROVISON_ACTION_NONE
    } else {
        asyncOffload.StartOffloadMetadata()
    }

    var pattern string = ""
    jobSchedInfo := asyncOffload.job.GetScheduleInfo()
    if jobSchedInfo == nil {
        pattern = ""
    } else {
        pendingStages := jobSchedInfo.GetPendingStages()
        stages := jobSchedInfo.GetDoneStages()
        if stages != nil && pendingStages != nil {
            stages = append(stages, pendingStages ...)
        }
        if stages != nil {
            for _, blstage := range stages {
                if blstage.CleanupPattern() != "" {
                    tmpPattern := blstage.CleanupPattern() + ","
                    pattern += tmpPattern
                }
            }
        }
    }
    if pattern == "" {
        asyncOffload.EndOffloadMetadata()
        return nil, DATA_PROVISON_ACTION_OFFLOAD_CHECK
    }

    asyncOffload.OffLoadNewStage(CLEANUP, pattern, "")
    asyncOffload.EndOffloadMetadata()
    return nil, DATA_PROVISON_ACTION_SCHEDULE
}

func (asyncOffload *AsyncOffload)GenereateRightControlOffloadStage(needPrepareData bool) (error, ACTION) {
    asyncOffload.lock.Lock()
    defer asyncOffload.lock.Unlock()

    job := asyncOffload.job
    enableRightControl := job.GetEnableRightControl()

    if asyncOffload.isBusy == true {
        common.SchedulerLogger.Debugf("The offload metadata is pushing!")
        return nil, DATA_PROVISON_ACTION_NONE
    } else {
        asyncOffload.StartOffloadMetadata()
    }

    _, path := storage.GetStorageMgr().JobRootDir(job.GetID())

    if len(asyncOffload.stage.stages) != 0 {
        asyncOffload.EndOffloadMetadata()
        return nil, DATA_PROVISON_ACTION_NONE
    } else {
        if !enableRightControl && !needPrepareData {
            asyncOffload.OffLoadNewStage(CHOWN, "", "")
        } else if needPrepareData {
            asyncOffload.OffLoadNewStage(DELETEDIR, "", path)
        } else {
            return nil, DATA_PROVISON_ACTION_OFFLOAD_CHECK
        }
    }
    asyncOffload.EndOffloadMetadata()
    return nil, DATA_PROVISON_ACTION_SCHEDULE
}

func (asyncOffload *AsyncOffload)StartOffloadMetadata() {
    asyncOffload.isBusy = true
}

func (asyncOffload *AsyncOffload)EndOffloadMetadata() {
    asyncOffload.isBusy = false
}

func (asyncOffload *AsyncOffload)OffLoadNewStage(action OFFLOADMETADATAACTION, pattern string, deletePath string) error {
    job := asyncOffload.job
    dir := job.WorkDir()
    logDir := job.LogDir()
    secCtxt := job.GetSecurityContext()
    accountInfo := secCtxt.GetUserInfo()

    stageId := asyncOffload.stage.NewStageId(action)
    stage := NewMetaDataCmdStage(job, stageId, action, dir, logDir, accountInfo, pattern, deletePath)

    asyncOffload.stage.AddStage(stage, stageId)

    return nil
}
