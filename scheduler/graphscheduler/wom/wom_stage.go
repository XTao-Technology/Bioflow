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
package wom

import (
    "time"
    "errors"
    "fmt"
    
    . "github.com/xtao/bioflow/common"
    . "github.com/xtao/bioflow/message"
    . "github.com/xtao/bioflow/storage"
    . "github.com/xtao/bioflow/scheduler/common"
)


/*
 * Wom stage represents a task or workflow call which need to be
 * executed by backend
 */
type womCmdStage struct {
    BaseStageInfo

    /*
     *  The command to be executed
     */
    cmd       string

    /*
     * Track the volumes need to be mapped to container
     * for this stage
     */
    conVolMgr *ContainerVolMgr

    outputValues map[string]string

    shadowWorkDir string

    constraints map[string]string
}

func NewWomCmdStage(job Job, id string, cmd string, workdir string, 
    logdir string, volMgr *ContainerVolMgr, restoreDataDir string) *womCmdStage {
	stage := &womCmdStage{
        BaseStageInfo: CreateBaseStageInfo(job, id, workdir, logdir, GRAPH_STAGE),
        cmd: cmd,
        conVolMgr: volMgr,
        constraints: make(map[string]string),
	}

    if volMgr == nil {
        dataDir, _, _ := GetStorageMgr().GetContainerExecDir()
        stage.conVolMgr = NewContainerVolMgr(dataDir, restoreDataDir)
    }

    return stage
}


func (stage *womCmdStage) GetCommand() string {
    return stage.cmd
}

func (stage *womCmdStage) SetCommand(cmd string) {
    stage.cmd = cmd
}

func (stage *womCmdStage) Constraints() map[string]string {
    return stage.constraints
}

func (stage *womCmdStage) AddConstraints(constraints map[string]string) {
    for k, v := range constraints {
        stage.constraints[k] = v
    }
}

func (stage *womCmdStage) GetDataVolsMap() (error, []*DataVol) {
    return stage.conVolMgr.GetMappedVols()
}

func (stage *womCmdStage) OutputDir() string {
    return ""
}

/*
 * Maps vol@cluster:filepath format to container based path, it will
 * collect the volume mapping information during this process.
 */
func (stage *womCmdStage) MapDataPathToContainer(file string) string {
    err, path := stage.conVolMgr.MapDataPathToContainer("", file)
    if err != nil {
        SchedulerLogger.Errorf("Can't map file %s: %s\n", file,
            err.Error())
        return ""
    }

    return path
}

func (stage *womCmdStage) MapDirToContainer(dirPath string,
    dirTarget string) error {
    return stage.conVolMgr.MapDirToContainer(dirPath, dirTarget)
}

func (stage *womCmdStage) ShadowWorkDir() string {
    if stage.shadowWorkDir == "" {
        shadowDir := fmt.Sprintf("shadow-%s-run%d", stage.GetID(), stage.RetryCount())
        stage.shadowWorkDir = stage.Job().WorkDir() + "/" + shadowDir
    }

    return stage.shadowWorkDir
}

func (stage *womCmdStage) ResetShadowWorkDir() {
    stage.shadowWorkDir = ""
}

/*Build container relative work dir*/
func (stage *womCmdStage) BuildWorkDir() string {
    return stage.MapDataPathToContainer(stage.ShadowWorkDir())
}

func (stage *womCmdStage) GetTargetOutputs() map[string]string {
    return stage.outputValues
}

func (stage *womCmdStage) SetTargetOutputs(outputValues map[string]string) {
    stage.outputValues = outputValues
}


/*
 * Prepare execution for the stage
 */
func (stage *womCmdStage) PrepareExecution(isRecovery bool) (error, *BuildStageErrorInfo, *StageExecEnv) {
    secCtxt := stage.Job().GetSecurityContext()
    accountInfo := secCtxt.GetUserInfo()
    err := GetStorageMgr().MkdirOnScheduler(stage.ShadowWorkDir(), true,
        accountInfo)
    if err != nil {
        return err, nil, nil
    }

    workDir := stage.BuildWorkDir()
    if workDir == "" {
        SchedulerLogger.Errorf("The workdir for stage is empty\n")
        return errors.New("Fail to get stage work directory"),
            nil, nil
    }

    if FSUtilsIsHDFSPath(workDir) {
        /* The hdfs work direcotry can't be used by container
         * as work directory
         */
        SchedulerLogger.Infof("Change stage work directory from hdfs path %s to /\n",
            workDir)
        workDir = "/"
    }

    /*
     * WDL requires the binary runs in the  workdir. So should tell
     * the caller use the generated workdir by set ForceChdir to true.
     */
    execEnv := &StageExecEnv {
        WorkDir: workDir,
        ForceChdir: true,
        NetMode: CLUSTER_NET_DUPLEX_MODE,
        Privileged: stage.Privileged(),
        Env: stage.Env(),
        Volumes: stage.Volumes(),
        OutputDirs: []string{workDir},
    }

    return nil, nil, execEnv
}

/*
 * Called when the stage complete, it will do post process for a task call
 * or workflow call
 */
func (stage *womCmdStage) PostExecution(success bool) error {
    storageMgr := GetStorageMgr()
    if success {
        /* If the stage fail during move path, then there is a possiblity that the
         * target work directory of stage already exists. So delete it here.
         */
        err := storageMgr.DeleteDirOnScheduler(stage.WorkDir(), true)
        if err != nil {
            SchedulerLogger.Errorf("Fail to delete target work dir %s before merge: %s\n",
                stage.ShadowWorkDir(), err.Error())
            /*ignore the error*/
        }

        err = storageMgr.MovePathOnScheduler(stage.ShadowWorkDir(), stage.WorkDir())
        if err != nil {
            SchedulerLogger.Errorf("Fail to move shadow workdir %s to workdir %s: %s\n",
                stage.ShadowWorkDir(), stage.WorkDir(), err.Error())
            return err
        }
    }

    err := storageMgr.DeleteDirOnScheduler(stage.ShadowWorkDir(), true)
    if err != nil {
        SchedulerLogger.Errorf("Fail to delete shadow work dir %s: %s\n",
            stage.ShadowWorkDir(), err.Error())
        /*ignore the error*/
    }

    stage.ResetShadowWorkDir()

    return nil
}

func (stage *womCmdStage)ToBioflowStageInfo() *BioflowStageInfo{
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
        GPU: stage.GetGPU(),
        GPUMemory: stage.GetGPUMemory(),
        Disk: stage.GetDisk(),
        ServerType: stage.GetServerType(),
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

func (stage *womCmdStage)RestoreExecutionState(stageJsonInfo *StageJSONInfo,
    pending bool) error{
    SchedulerLogger.Infof("Restore stage %s execution state: retry count(%d/%d)\n",
        stage.GetID(), stage.RetryCount(), stageJsonInfo.RetryCount)
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
    }

    return nil
}

func (stage *womCmdStage)Cleanup() error {
    return nil
}

func (stage *womCmdStage)NeedStoreAppOutput() bool {
    return true
}

func (stage *womCmdStage) FilesNeedPrepare() []string {
    return stage.conVolMgr.FilesNeedPrepare()
}

/*
 * Release the unused stage resources to save memory
 */
func (stage *womCmdStage) Recycle() {
    /*The Vol Mgr can be recycled now*/
    if stage.conVolMgr != nil {
        stage.conVolMgr.Destroy()
        stage.conVolMgr = nil
    }

    /*Recycle all the input tracking data structures*/
    stage.SetIOAttr(nil)

    SchedulerLogger.Infof("Stage %s/%s resources are recycled\n",
        stage.GetID(), stage.Job().GetID())
}
