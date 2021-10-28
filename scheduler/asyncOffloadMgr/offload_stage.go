package asyncOffloadMgr


import (
    . "github.com/xtao/bioflow/scheduler/common"
    . "github.com/xtao/bioflow/common"
    "github.com/xtao/bioflow/storage"
    . "github.com/xtao/bioflow/message"
    "time"
    "errors"
)

type OFFLOADMETADATAACTION int

const(
    CLEANUP OFFLOADMETADATAACTION = 0
    CHOWN OFFLOADMETADATAACTION = 1
    DELETEDIR OFFLOADMETADATAACTION = 2
    OFFLOAD string = "OFFLOAD"
    cleanup string = "CLEANUP-"
    chown string  = "CHOWN-"
    OFFLOADCLEANCMD = "xtfscli remove "
    OFFLOADCHOWNCMD = "xtfscli chown "
    OFFLOADIMAGE = "offloadmt:latest"
    CPU float64 = 1.0
    MEMERY float64 = 10000
    RETRYLIMIT int = 3
)


type AsyncOffloadCmdStage struct {
    BaseStageInfo
    pattern string
    workdir string
    deletePath string
    cmd string
    action OFFLOADMETADATAACTION
    conVolMgr *storage.ContainerVolMgr
    accountInfo *UserAccountInfo
}

func NewMetaDataCmdStage(job Job, id string, action OFFLOADMETADATAACTION, workdir string,
    logdir string, account *UserAccountInfo, pattern string, deletePath string) *AsyncOffloadCmdStage {
    baseDir, _, _ := storage.GetStorageMgr().GetContainerExecDir()

    return &AsyncOffloadCmdStage {
        BaseStageInfo: CreateBaseStageInfo(job, id, workdir, logdir, OFFLOAD_STAGE),
        conVolMgr: storage.NewContainerVolMgr(baseDir, workdir),
        accountInfo: account,
        pattern: pattern,
        action: action,
        deletePath: deletePath,
    }
}

func (stage *AsyncOffloadCmdStage) Cleanup() error {
    return nil
}

func (stage *AsyncOffloadCmdStage) GetTargetOutputs() map[string]string {
    return nil
}

func (stage *AsyncOffloadCmdStage) OutputDir() string {
    return ""
}

func (stage *AsyncOffloadCmdStage) MapDirToContainer(dirPath string, dirTarget string) error {
    return nil
}

func(stage *AsyncOffloadCmdStage) FilesNeedPrepare() []string {
    return nil
}

func (stage *AsyncOffloadCmdStage) GetCommand() string {
    return stage.cmd
}

func (stage *AsyncOffloadCmdStage) RestoreExecutionState(stageJsonInfo *StageJSONInfo, pending bool) error {
    return nil
}

func (stage *AsyncOffloadCmdStage) MapDataPathToContainer(dir string) string {
    err, path := stage.conVolMgr.MapDataPathToContainer("", dir)
    if err != nil {
        SchedulerLogger.Errorf("Can't map file %s: %s\n", dir,
            err.Error())
        return ""
    }

    return path
}

func (stage *AsyncOffloadCmdStage) BuildCommand() *BuildStageErrorInfo {
    user := stage.accountInfo.Username
    group := stage.accountInfo.Groupname
    uid := stage.accountInfo.Uid
    gid := stage.accountInfo.Gid
    umask := stage.accountInfo.Umask
    pattern := stage.pattern
    if user == "" {
        user = `""`
    }
    if group == "" {
        group = `""`
    }
    if uid == "" {
        uid = `""`
    }
    if gid == "" {
        gid = `""`
    }
    if umask == "" {
        umask = `""`
    }
    if pattern == "" {
        pattern = `""`
    }

    realWorkDir := stage.MapDataPathToContainer(stage.WorkDir())
    SchedulerLogger.Debugf("workdir: %s, realWorkdir: %s\n",
        stage.workdir, realWorkDir)

    if stage.action == CLEANUP {
        stage.cmd = OFFLOADCLEANCMD + " " + realWorkDir + " " + pattern
    } else if stage.action == CHOWN {
        stage.cmd = OFFLOADCHOWNCMD + " " + realWorkDir + " " + user + " " +
            group + " " + uid + " " + gid + " " + umask + " " + `""`
    } else if stage.action == DELETEDIR {
        stage.cmd = OFFLOADCHOWNCMD + " "
    } else{
        err := errors.New("No support action")
        buildStageErrorInfo := NewBuildStageErrorInfo(0, BIOSTAGE_FAILREASON_OTHERMISTAKE,
            "", err)
        SchedulerLogger.Errorf("Can't marshal accountinfo %s\n",
            err.Error())
        return buildStageErrorInfo
    }
    return nil
}

func (stage *AsyncOffloadCmdStage) PrepareExecution(isRecovery bool) (error,
    *BuildStageErrorInfo, *StageExecEnv) {
    execEnv := &StageExecEnv {
        Privileged: stage.Privileged(),
        Env: stage.Env(),
        Volumes: stage.Volumes(),
    }

    source := new(ResourceSpec)
    cpu := CPU
    mem := MEMERY
    source.SetCPU(cpu)
    source.SetMemory(mem)
    stage.SetResourceSpec(*source)
    stage.SetImage(OFFLOADIMAGE)
    stage.SetFailRetryLimit(RETRYLIMIT)

    buildStageErrorInfo := stage.BuildCommand()
    if buildStageErrorInfo != nil {
        SchedulerLogger.Errorf("Build cmd fail: %s\n", buildStageErrorInfo.Err.Error())
        return nil, buildStageErrorInfo, execEnv
    }

    return nil, nil, execEnv
}

func (stage *AsyncOffloadCmdStage) GetDataVolsMap() (error, []*storage.DataVol) {
    return stage.conVolMgr.GetMappedVols()
}

func (stage *AsyncOffloadCmdStage) PostExecution(success bool) error {
    return nil
}


func (stage *AsyncOffloadCmdStage) ToBioflowStageInfo() *BioflowStageInfo {
    stageInfo := &BioflowStageInfo {
        Id: stage.GetID(),
        Name: stage.Name(),
        State: StageStateToStr(stage.State()),
        Command: stage.GetCommand(),
        Output: stage.GetTargetOutputs(),
        BackendId: "N/A",
        TaskId: "N/A",
        ScheduleTime: stage.ScheduledTime().Format(time.UnixDate),
        SubmitTime: stage.SubmitTime().Format(time.UnixDate),
        FinishTime: stage.FinishTime().Format(time.UnixDate),
        TotalDuration: stage.TotalDuration(),
        RetryCount: stage.RetryCount(),
        CPU: stage.GetCPU(),
        Memory: stage.GetMemory(),
        ExecMode: stage.ExecMode(),
        HostName: stage.HostName(),
        HostIP: stage.IP(),
    }

    switch stage.State() {
    case STAGE_INITIAL:
        stageInfo.SubmitTime = "N/A"
        stageInfo.ScheduleTime = "N/A"
        stageInfo.FinishTime = "N/A"
        stageInfo.TotalDuration = -1
    case STAGE_SUBMITTED, STAGE_QUEUED:
        stageInfo.ScheduleTime = "N/A"
        stageInfo.FinishTime = "N/A"
        stageInfo.TotalDuration = -1
    case STAGE_RUNNING:
        stageInfo.FinishTime = "N/A"
        stageInfo.TotalDuration = -1
    }

    return stageInfo
}
