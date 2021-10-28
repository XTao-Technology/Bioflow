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
    "strings"
    "time"
    "sync"
    
    . "github.com/xtao/bioflow/common"
    . "github.com/xtao/bioflow/storage"
)

const BIOTK_IMAGE string = "biotk:latest"

/*
 * BaseStageInfo data structure holds the basic context information
 * for a stage. It also provides common utility interfaces.
 */
type BaseStageInfo struct {
    /*per-job unique identifier*/
    id        string
    stageType int

    /* following execution information
     * are from pipeline item
     */
    name       string
    image     string
    workDir  string
    hdfsWorkDir string


    logDir   string
    resourceSpec ResourceSpec
    outputFile string
    outputFileMap map[string]string

    state     uint32
    retryCount int
    submitTime time.Time
    submitTimeSet bool
    scheduledTime time.Time
    scheduledTimeSet bool
    finishTime time.Time
    finishTimeSet bool
    failTime time.Time
    failReason string

    failRetryLimit int

    /*fail abort*/
    failAbort bool
    cpuTuneRatio float64
    memTuneRatio float64

    /*track the input files for the stage*/
    trackedInputFiles []string

    /*job owning the stage*/
    job       Job

    /*storage requirement*/
    storageType StorageType

    privileged bool
    env map[string]string
    volumes map[string]string

    /*whether map volumes with same path*/
    execMode string

    /*the host info stage running on*/
    hostName string
    hostIP string

    /*stats on resource usage*/
    resourceStats *ResourceUsageSummary

    /*
     * Track the event in-progress to sync many threads
     * which may handle same event because of notify and
     * poll threads receive same event
     */
    inprogressEvent uint32
    lock sync.Mutex

    pipelineName string
    pipelineVersion string
    /*
     * the pattern specified by user to cleanup files
     */
    cleanupPattern string

    ioAttr *IOAttr
}

func (stage *BaseStageInfo) IOAttr() *IOAttr {
    return stage.ioAttr
}

func (stage *BaseStageInfo) SetIOAttr(attr *IOAttr) {
    stage.ioAttr = attr
}

func (stage *BaseStageInfo) SetCleanupPattern(pattern string) {
    stage.cleanupPattern = pattern
}

func (stage *BaseStageInfo) CleanupPattern() string {
    return stage.cleanupPattern
}

func (stage *BaseStageInfo) GetID() string {
    return stage.id
}

func (stage *BaseStageInfo) State() uint32 {
    return stage.state
}

func (stage *BaseStageInfo) SetState(state uint32) {
    stage.state = state
}

func (stage *BaseStageInfo) ExecMode() string {
    return stage.execMode
}

func (stage *BaseStageInfo) SetExecMode(mode string) {
    stage.execMode = strings.ToUpper(mode)
}

func (stage *BaseStageInfo) HostName() string {
    return stage.hostName
}

func (stage *BaseStageInfo) SetHostName(name string) {
    if stage.hostName != name {
        SchedulerLogger.Infof("The Stage %s host name is %s, and reset it %s!",
            stage.id, stage.hostName, name)
    }
    stage.hostName = name

}

func (stage *BaseStageInfo) SetResourceStats(usage *ResourceUsageSummary) {
    stage.resourceStats = usage
}

func (stage *BaseStageInfo) ResourceStats() ResourceUsageSummary {
    if stage.resourceStats != nil {
        return *stage.resourceStats
    } else {
        return ResourceUsageSummary{}
    }
}

func (stage *BaseStageInfo) IP() string {
    return stage.hostIP
}

func (stage *BaseStageInfo) SetIP(ip string) {
    stage.hostIP = ip
}

func (stage *BaseStageInfo) StorageType() StorageType {
    return stage.storageType
}

func (stage *BaseStageInfo) SetStorageType(stype StorageType) {
    stage.storageType = stype
}

func (stage *BaseStageInfo) ClearInputFileList() {
    stage.trackedInputFiles = nil
}

func (stage *BaseStageInfo) TrackInputFile(file string) {
    stage.trackedInputFiles = append(stage.trackedInputFiles, file)
}

func (stage *BaseStageInfo) GetInprogressEvent() uint32 {
    return stage.inprogressEvent
}

func (stage *BaseStageInfo) StartHandleEvent(event uint32) bool {
    stage.lock.Lock()
    defer stage.lock.Unlock()

    SchedulerLogger.Debugf("Stage %s start handle event: %s",
        stage.GetID(), StageStateToStr(event))

    /*Sync with other threads handling the stage terminal events*/
    if event == STAGE_FAIL || event == STAGE_LOST || event == STAGE_DONE ||
        event == STAGE_SUBMITTED || event == STAGE_QUEUED ||
        event == STAGE_RUNNING {
        if stage.inprogressEvent == STAGE_INVALID_STATE {
            stage.inprogressEvent = event
            return true
        } else {
            /*
             * A terminal event is already handled in-progress, so shouldn't
             * handle this event
             */
            SchedulerLogger.Infof("Stage %s already handling event %s when receiving %s\n",
                stage.GetID(), StageStateToStr(stage.inprogressEvent), StageStateToStr(event))
            return false
        }
    }

    return true
}

func (stage *BaseStageInfo) AllowScheduleStage() bool {
    return stage.GetInprogressEvent() == STAGE_INVALID_STATE
}

func (stage *BaseStageInfo) FinishHandleEvent(event uint32) {
    stage.lock.Lock()
    defer stage.lock.Unlock()

    SchedulerLogger.Debugf("Stage %s finish handle event: %s",
        stage.GetID(), StageStateToStr(event))

    stage.inprogressEvent = STAGE_INVALID_STATE
}

func (stage *BaseStageInfo) GetInputFileList() []string {
    return stage.trackedInputFiles
}

func (stage *BaseStageInfo) NeedStoreAppOutput() bool {
    return false
}

/*
 * Make sure that it will be called only by one thread
 * at the same time
 */
func (stage *BaseStageInfo) PrepareRetry()  {
    stage.retryCount ++

    /*
     * If retry a failed stage, need auto tune the
     * resource requirement if specified
     */
    if stage.state == STAGE_FAIL {
       resSpec := ResourceSpec{
                cpu: stage.GetCPU() * (1 + stage.cpuTuneRatio),
                memory: stage.GetMemory() * (1 + stage.memTuneRatio),
                serverType: stage.GetServerType(),
                gpu: stage.GetGPU(),
                disk: stage.GetDisk(),
                gpuMemory: stage.GetGPUMemory(),
       }
       stage.SetResourceSpec(resSpec)
    }

    /*clear the fail reason for a new run*/
    stage.SetFailReason("")
}

func (stage *BaseStageInfo) RetryCount() int {
    return stage.retryCount
}

func (stage *BaseStageInfo) SetRetryCount(count int) {
    stage.retryCount = count
}

func (stage *BaseStageInfo) FailRetryLimit() int {
    return stage.failRetryLimit
}

func (stage *BaseStageInfo) SetFailRetryLimit(limit int) {
    stage.failRetryLimit = limit
}

func (stage *BaseStageInfo) SetFailAbort(abort bool) {
    stage.failAbort = abort
}

func (stage *BaseStageInfo) FailAbort() bool {
    return stage.failAbort
}

func (stage *BaseStageInfo) SetResourceTuneRatio(cpuRatio float64,
    memRatio float64) {
    stage.cpuTuneRatio = cpuRatio
    stage.memTuneRatio = memRatio
}

func (stage *BaseStageInfo) WorkDir() string {
    return stage.workDir
}

func (stage *BaseStageInfo) SetWorkDir(dir string) {
    stage.workDir = dir
}

func (stage *BaseStageInfo) LogDir() string {
    return stage.logDir
}

func (stage *BaseStageInfo) SetLogDir(dir string) {
    stage.logDir = dir
}

func (stage *BaseStageInfo) ID() string {
    return stage.id
}

func (stage *BaseStageInfo) FailReason() string {
    return stage.failReason
}

func (stage *BaseStageInfo) SetFailReason(reason string) {
    stage.failReason = reason
}


func (stage *BaseStageInfo) Job() Job {
    return stage.job
}

func (stage *BaseStageInfo) IsRunning() bool {
    return stage.state == STAGE_RUNNING
}


func (stage *BaseStageInfo) OutputFile() string {
    return stage.outputFile
}

func (stage *BaseStageInfo) SetOutputFile(file string) {
    stage.outputFile = file
}

func (stage *BaseStageInfo) OutputFileMap() map[string]string {
    return stage.outputFileMap
}

func (stage *BaseStageInfo) SetOutputFileMap(outputFileMap map[string]string) {
    for k, v := range outputFileMap {
        stage.outputFileMap[k] = v
    }
}

func (stage *BaseStageInfo) SetResourceSpec(resource ResourceSpec) {
    stage.resourceSpec = resource
}

func (stage *BaseStageInfo) MergeResourceSpec(cpu float64, memory float64,
    disk float64, gpu float64, gpuMemory float64){
    if cpu != -1 {
        stage.resourceSpec.cpu = cpu
    }
    if memory != -1 {
        stage.resourceSpec.memory = memory
    }
    if disk != -1 {
        stage.resourceSpec.disk = disk
    }
    if gpu != -1 {
        stage.resourceSpec.gpu = gpu
    }
    if gpuMemory != -1 {
        stage.resourceSpec.gpuMemory = gpuMemory
    }
}

func (stage *BaseStageInfo) GetImage() string {
    if stage.image == "" {
        return BIOTK_IMAGE
    } else {
        return stage.image
    }
}

func (stage *BaseStageInfo) SetImage(image string) {
    stage.image = image
}

func (stage *BaseStageInfo) Name() string {
    return stage.name
}

func (stage *BaseStageInfo) SetName(name string) {
    stage.name = name
}

func (stage *BaseStageInfo) GetCPU() float64 {
    return stage.resourceSpec.GetCPU()
}

func (stage *BaseStageInfo) GetGPU() float64 {
    return stage.resourceSpec.GetGPU()
}

func (stage *BaseStageInfo) GetGPUMemory() float64 {
    return stage.resourceSpec.GetGPUMemory()
}

func (stage *BaseStageInfo) GetDisk() float64 {
    return stage.resourceSpec.GetDisk()
}

func (stage *BaseStageInfo) GetMemory() float64 {
    return stage.resourceSpec.GetMemory()
}

func (stage *BaseStageInfo) GetServerType() string {
    return stage.resourceSpec.GetServerType()
}

func (stage *BaseStageInfo) SetSubmitTime(tm time.Time) {
    if !stage.submitTimeSet {
        stage.submitTimeSet = true
        stage.submitTime = tm
    }
}

func (stage *BaseStageInfo) SubmitTime() time.Time {
    return stage.submitTime
}

func (stage *BaseStageInfo) Constraints() map[string]string {
    return nil
}

func (stage *BaseStageInfo) SetScheduledTime(tm time.Time) {
    if !stage.scheduledTimeSet {
        stage.scheduledTimeSet = true
        stage.scheduledTime = tm
    }
}

func (stage *BaseStageInfo) ScheduledTime() time.Time {
    return stage.scheduledTime
}

func (stage *BaseStageInfo) RunDurationTillNow() float64 {
    return time.Now().Sub(stage.scheduledTime).Minutes()
}

func (stage *BaseStageInfo) SetFinishTime(tm time.Time) {
    if stage.state == STAGE_FAIL {
        stage.finishTimeSet = false
    }
    if !stage.finishTimeSet {
        stage.finishTimeSet = true
        stage.finishTime = tm
    }
}

func (stage *BaseStageInfo) FinishTime() time.Time {
    return stage.finishTime
}

func (stage *BaseStageInfo) QueueDuration() float64 {
    return stage.scheduledTime.Sub(stage.submitTime).Minutes()
}

func (stage *BaseStageInfo) QueueDurationTillNow() float64 {
    return time.Now().Sub(stage.submitTime).Minutes()
}

func (stage *BaseStageInfo) TotalDuration() float64 {
    return stage.finishTime.Sub(stage.submitTime).Minutes()
}

func (stage *BaseStageInfo) SetPrivileged(pri bool) {
    stage.privileged = pri
}

func (stage *BaseStageInfo) Privileged() bool {
    return stage.privileged
}

func (stage *BaseStageInfo) SetEnv(env map[string]string) {
    /*clone for safety*/
    if env != nil {
        stage.env = make(map[string]string)
        for key, val := range env {
            stage.env[key] = val
        }
    }
}

func (stage *BaseStageInfo) Env() map[string]string {
    return stage.env
}

func (stage *BaseStageInfo) SetVolumes(vols map[string]string) {
    if vols != nil {
        stage.volumes = make(map[string]string)
        for key, val := range vols {
            stage.volumes[key] = val
        }
    }
}

func (stage *BaseStageInfo) Volumes() map[string]string {
    return stage.volumes
}

func (stage *BaseStageInfo) PipelineName() string {
    return stage.pipelineName
}

func (stage *BaseStageInfo) SetPipelineName(name string) {
    stage.pipelineName = name
}

func (stage *BaseStageInfo) PipelineVersion() string {
    return stage.pipelineVersion
}

func (stage *BaseStageInfo) SetPipelineVersion(version string) {
    stage.pipelineVersion = version
}

func (stage *BaseStageInfo) Type() int {
    return stage.stageType
}

func (stage *BaseStageInfo) Recycle() {
}

func CreateBaseStageInfo(job Job, id string, workdir string,
    logdir string, stageType int) BaseStageInfo {
    return BaseStageInfo{
            job: job, 
            id: id, 
            workDir: workdir, 
	        logDir: logdir,
            outputFile: "",
            outputFileMap: make(map[string]string),
            retryCount: 0,
            submitTimeSet: false,
            scheduledTimeSet: false,
            finishTimeSet: false,
            state: STAGE_INITIAL,
            stageType: stageType,
            cleanupPattern: "",
    }
}
