package dataprovider

import (
    . "github.com/xtao/bioflow/scheduler/common"
    . "github.com/xtao/bioflow/common"
    "github.com/xtao/bioflow/storage"
    "github.com/xtao/bioflow/message"
    "time"
    "fmt"
    "errors"
    "strings"
    "os"
    "github.com/xtao/bioflow/common"
    "sync"
)

type FILESTAGEACTION int

const(
    PREPARE FILESTAGEACTION = 0
    RESTORE FILESTAGEACTION = 1
    PUT string = "put"
    GET string = "get"
    COMMANDTEMPLATE string = `ossadm --action=%s --bucket=%s --objects=%s --endpoints=%s`
    PAIRSEP string = ":"
    TRANSMINTSEP string = "#"
    ETCDENDPOINTSEP string = ","
    TMPDIR string = "BIOFLOW_DIR_USE_TO_HACK_MEGER_WHEN_PARENT_DIR_NOT_EMPTY"
    IMAGE string = "ossmgmt"
    RETRYLIMIT int = 5
    SERIALFLAG string = "-s"
)

var CPU float64 = 0.2
var MEMERY float64 = 1000
var lock *sync.RWMutex = new(sync.RWMutex)

func SetDownloadWorkResource(cpu, mem float64) {
    lock.Lock()
    defer  lock.Unlock()
    CPU = cpu
    MEMERY = mem
}

type DataCmdStage struct {
    BaseStageInfo
    conVolMgr *storage.ContainerVolMgr
    cmd string
    action FILESTAGEACTION
    transmit map[string]string
    bucket string
    shadowWorkDir string
    serial bool
}

func (stage *DataCmdStage) Cleanup() error {
    return nil
}

func (stage *DataCmdStage) GetTargetOutputs() map[string]string {
    return nil
}

func (stage *DataCmdStage) OutputDir() string {
    return ""
}

func (stage *DataCmdStage) MapDirToContainer(dirPath string, dirTarget string) error {
    return nil
}

func(stage *DataCmdStage) FilesNeedPrepare() []string {
    return nil
}

func (stage *DataCmdStage) GetCommand() string {
    return stage.cmd
}

func (stage *DataCmdStage) RestoreExecutionState(stageJsonInfo *StageJSONInfo, pending bool) error {
    /*Merge the retry count*/
    if stage.RetryCount() < stageJsonInfo.RetryCount {
        stage.SetRetryCount(stageJsonInfo.RetryCount)
    }

    /*
     * All the done stage execution information can be obtained directly
     * from the done stages history. so only need restore pending stages'
     * information.
     */
    if pending {
        submitTime, err := time.Parse(time.UnixDate,
            stageJsonInfo.SubmitTime)
        if err != nil {
            SchedulerLogger.Errorf("Fail restore stage's submit time %s: %s\n",
                stageJsonInfo.SubmitTime, err.Error())
        } else {
            stage.SetSubmitTime(submitTime)
        }

        scheduleTime, err := time.Parse(time.UnixDate,
            stageJsonInfo.ScheduledTime)
        if err != nil {
            SchedulerLogger.Errorf("Fail restore stage's schedule time %s: %s\n",
                stageJsonInfo.ScheduledTime, err.Error())
        } else {
            stage.SetScheduledTime(scheduleTime)
        }

        finishTime, err := time.Parse(time.UnixDate,
            stageJsonInfo.FinishTime)
        if err != nil {
            SchedulerLogger.Errorf("Fail restore stage's finish time %s: %s\n",
                stageJsonInfo.FinishTime, err.Error())
        } else {
            stage.SetFinishTime(finishTime)
        }

        /*restore the resource requirement*/
        rsc := ResourceSpec{}
        rsc.SetCPU(stageJsonInfo.CPU)
        rsc.SetMemory(stageJsonInfo.Memory)
        stage.SetResourceSpec(rsc)

        if stageJsonInfo.HostName != "" {
            stage.SetHostName(stageJsonInfo.HostName)
        }
        if stageJsonInfo.HostIP != "" {
            stage.SetIP(stageJsonInfo.HostIP)
        }

        /*restore the stage execution info*/
        err, _, _ = stage.PrepareExecution(true)
        if err != nil {
            return err
        }
    }

    return nil
}

func (stage *DataCmdStage) GetDataVolsMap() (error, []*storage.DataVol) {
    return stage.conVolMgr.GetMappedVols()
}

func (stage *DataCmdStage) PrepareExecution(isRecovery bool) (error, *message.BuildStageErrorInfo, *StageExecEnv) {
    var workDir string
    if stage.action == PREPARE {
        err := stage.PrepareShadowDir(!isRecovery)
        if err != nil {
            SchedulerLogger.Errorf("Fail to create shadow work directory: %s\n",
                err.Error())
            return err, nil, nil
        }

        workDir = stage.MapDataPathToContainer(stage.shadowWorkDir)
        if workDir == "" {
            SchedulerLogger.Errorf("Fail to build stage work directory\n")
            return errors.New("Fail to build stage work directory"), nil, nil
        }
    } else {
        workDir = stage.MapDataPathToContainer(stage.WorkDir())
    }

    if storage.FSUtilsIsHDFSPath(workDir) {
        /* The hdfs work direcotry can't be used by container
         * as work directory
         */
        SchedulerLogger.Infof("Change stage work directory from hdfs path %s to /\n",
            workDir)
        workDir = "/"
    }

    execEnv := &StageExecEnv {
        WorkDir: workDir,
        NetMode: CLUSTER_NET_DUPLEX_MODE,
        Privileged: stage.Privileged(),
        Env: stage.Env(),
        Volumes: stage.Volumes(),
    }

    source := new(ResourceSpec)
    lock.RLock()
    cpu := CPU
    mem := MEMERY
    lock.RUnlock()
    source.SetCPU(cpu)
    source.SetMemory(mem)
    stage.SetResourceSpec(*source)
    stage.SetImage(IMAGE)
    stage.SetFailRetryLimit(RETRYLIMIT)

    buildStageErrInfo := stage.BuildCommand()
    if buildStageErrInfo != nil {
        return nil, buildStageErrInfo, execEnv
    }

    return nil, nil, execEnv
}

func (stage *DataCmdStage) MapDataPathToContainer(file string) string {
    err, path := stage.conVolMgr.MapDataPathToContainer("", file)
    if err != nil {
        SchedulerLogger.Errorf("Can't map file %s: %s\n", file,
            err.Error())
        return ""
    }

    return path
}

func (stage *DataCmdStage) BuildCommand() *message.BuildStageErrorInfo {
    var transmitPairs []string
    for obj, file := range stage.transmit {
        if stage.action == PREPARE {
            file = storage.FSUtilsBuildFsPath(stage.shadowWorkDir, file)
        }else {
            file = storage.FSUtilsBuildFsPath(stage.WorkDir(), file)
        }
        realPath := stage.MapDataPathToContainer(file)
        pair := fmt.Sprintf("%s%s%s", obj, PAIRSEP, realPath)
        transmitPairs = append(transmitPairs, pair)
    }

    transmit := strings.Join(transmitPairs, TRANSMINTSEP)
    var action string
    if stage.action == PREPARE {
        action = GET
    }else {
        action = PUT
    }
    etcdEndPoint := strings.Join(EtcdEndPoints, ETCDENDPOINTSEP)
    stage.cmd = fmt.Sprintf(COMMANDTEMPLATE, action, stage.bucket, transmit, etcdEndPoint)
    if stage.serial {
        stage.cmd += " " + SERIALFLAG
    }
    common.SchedulerLogger.Infof("Data stage generate cmd %s", stage.cmd)

    return nil
}

func (stage *DataCmdStage) PrepareShadowDir(create bool) error {
    shadowDirPath := stage.WorkDir() + fmt.Sprintf("/shadow-%s-run-%d",
        stage.ID(), stage.RetryCount())
    secCtxt := stage.Job().GetSecurityContext()
    accountInfo := secCtxt.GetUserInfo()
    if create {
        err := storage.GetStorageMgr().MkdirOnScheduler(shadowDirPath, true,
            accountInfo)
        if err != nil {
            return err
        }
    }
    stage.shadowWorkDir = shadowDirPath

    SchedulerLogger.Debugf("Stage %s works on shadow work dir %s\n",
        stage.GetID(), stage.shadowWorkDir)
    return nil
}


func (stage *DataCmdStage) ShadowWorkDir() string {
    return stage.shadowWorkDir
}

func (stage *DataCmdStage) ClearShadowDir() {
    stage.shadowWorkDir = ""
}

func (stage *DataCmdStage) PostExecution(success bool) error {
    storageMgr := storage.GetStorageMgr()
    if success && stage.action == PREPARE {
        SchedulerLogger.Debugf("Do post work for success stage %s\n",
            stage.GetID())

        err := stage.MergeWorkdir()
        if err != nil {
            SchedulerLogger.Infof("Fail to merge shadow work directory %s to target %s: %s\n",
                stage.ShadowWorkDir(), stage.WorkDir(), err.Error())
            return nil
        }

    }

    if !success && stage.ShadowWorkDir() != "" {
        err := storageMgr.DeleteDirOnScheduler(stage.ShadowWorkDir(), true)
        if err != nil {
            SchedulerLogger.Errorf("Fail to delete shadow work dir %s: %s\n",
                stage.ShadowWorkDir(), err.Error())
            /*ignore the error*/
        }
    }

    stage.ClearShadowDir()
    return nil
}

func (stage *DataCmdStage) MergeWorkdir() error {
    var retryCount int = 0
    storageMgr := storage.GetStorageMgr()
retryMergeShadowWorkDir:
    if stage.ShadowWorkDir() != "" {
        err := storageMgr.MergeDirFilesOnScheduler(stage.ShadowWorkDir(),
            stage.WorkDir(), "")
        if err != nil {
            if os.IsNotExist(err) {
                /*
                 * When the ShadowWorkDir is alread not exsit, it must be deleted some time
                 * so the following repeat operations do not need to continue to Delete
                 * the ShadowWorkDir.
                 */
                return nil
            } else {
                return err
            }
        } else {
            SchedulerLogger.Infof("Succeed to merge shadow work directory %s to target %s\n",
                stage.ShadowWorkDir(), stage.WorkDir())
            err := storageMgr.DeleteDirOnScheduler(stage.ShadowWorkDir(), true)
            if err != nil {
                SchedulerLogger.Errorf("Fail to delete shadow work dir %s: %s\n",
                    stage.ShadowWorkDir(), err.Error())
                retryCount ++
                storage.FSUtilsCountRetryNum()
                /*Hack the situation which rm system call return directory not empty*/
                SchedulerLogger.Infof("Delete shadow optput dir:%s,err:%s, so modify the parent dir dentry and retry, retryCount: %d!!",
                    stage.ShadowWorkDir(), err.Error(), retryCount)
                subDir := stage.ShadowWorkDir() + "/" + TMPDIR
                storage.FSUtilsMkdir(subDir, false)
                storage.FSUtilsDeleteDir(subDir, false)
                goto retryMergeShadowWorkDir
            }
        }
    }

    return nil
}

func (stage *DataCmdStage) ToBioflowStageInfo() *message.BioflowStageInfo {
    stageInfo := &message.BioflowStageInfo {
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

func (stage *DataCmdStage) AddTransmitWork (obj string, path string) {
    stage.transmit[obj] = path
}

func(stage *DataCmdStage) SetBucket(bucket string) {
    stage.bucket = bucket
}

func(stage *DataCmdStage) SetWorkSerially() {
    stage.serial = true
}

func NewDataStage(id string, action FILESTAGEACTION, job Job, dir, logdir string) *DataCmdStage {
    baseDir, _, _ := storage.GetStorageMgr().GetContainerExecDir()
    return &DataCmdStage{
        BaseStageInfo: CreateBaseStageInfo(job, id, dir, logdir, DATA_STAGE),
        action: action,
        transmit: make(map[string]string),
        conVolMgr: storage.NewContainerVolMgr(baseDir, dir),
        serial: false,
    }
}
