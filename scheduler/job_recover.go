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
    "errors"

    . "github.com/xtao/bioflow/dbservice"
    . "github.com/xtao/bioflow/common"
    . "github.com/xtao/bioflow/message"
    "github.com/xtao/bioflow/eventbus"
    . "github.com/xtao/bioflow/scheduler/common"
)

type JobRecover interface {
    RecoverJob(Job, bool) error
}

func RecoverJob(job Job, startScheduleJob bool) error {
    switch job.Pipeline().Type() {
        case PIPELINE_TYPE_BIOBPIPE:
            recover := BLJobRecover{}
            return recover.RecoverJob(job, startScheduleJob)
        case PIPELINE_TYPE_WDL:
            recover := WomJobRecover{}
            return recover.RecoverJob(job, startScheduleJob)
        default:
            return errors.New("unknown pipeline type to recover job")
    }
}

type BaseJobRecover struct {

}

func (recover *BaseJobRecover)RecoverPrepare(job Job, jobSchedDBInfo *JobScheduleDBInfo) error {
    SchedulerLogger.Infof("Start recover job %s stages from preparing work",
        job.GetID())
    jobSchedInfo := job.GetScheduleInfo()

    err, _ := job.PrepareData()
    if err != nil {
        return handleBuildStageErr(err, job, nil)
    }
    err = jobSchedInfo.RecoverFromDB(jobSchedDBInfo,
        true, true)
    if err != nil {
        return handleRecoverScheInfoErr(err, job)
    }
    SchedulerLogger.Infof("Complete recover job %s stages from preparing work",
        job.GetID())

    return nil
}

func (recover *BaseJobRecover)RecoverChownOffload(job Job, jobSchedDBInfo *JobScheduleDBInfo) error {
    SchedulerLogger.Infof("Start recover job %s stages from offload work",
        job.GetID())
    jobSchedInfo := job.GetScheduleInfo()
    scheduler := GetScheduler()

    enableRightControl := scheduler.enableRightControl
    job.SetEnableRightControl(enableRightControl)
    needPrepareData := job.NeedPrepareData()

    err, _ := job.GenereateRightControlOffloadData(needPrepareData)
    if err != nil {
        return handleBuildStageErr(err, job, nil)
    }

    err = jobSchedInfo.RecoverFromDB(jobSchedDBInfo,
        true, true)
    if err != nil {
        return handleRecoverScheInfoErr(err, job)
    }
    SchedulerLogger.Infof("Complete recover job %s stages from cleanup offload work",
        job.GetID())
    return nil
}

func (recover *BaseJobRecover)RecoverCleanupOffload(job Job, jobSchedDBInfo *JobScheduleDBInfo) error {
    SchedulerLogger.Infof("Start recover job %s stages from offload work",
        job.GetID())
    jobSchedInfo := job.GetScheduleInfo()

    err, _ := job.GenereateCleanupOffloadStage()
    if err != nil {
        return handleBuildStageErr(err, job, nil)
    }

    err = jobSchedInfo.RecoverFromDB(jobSchedDBInfo,
        true, true)
    if err != nil {
        return handleRecoverScheInfoErr(err, job)
    }
    SchedulerLogger.Infof("Complete recover job %s stages from cleanup offload work",
        job.GetID())
    return nil
}

func (recover *BaseJobRecover)RecoverRestore(job Job, jobSchedDBInfo *JobScheduleDBInfo) error {
    SchedulerLogger.Infof("Start recover job %s stages from restoring work",
        job.GetID())
    jobSchedInfo := job.GetScheduleInfo()

    err, _ := job.RestoreData()
    if err != nil {
        return handleBuildStageErr(err, job, nil)
    }

    err = jobSchedInfo.RecoverFromDB(jobSchedDBInfo,
        true, true)
    if err != nil {
        return handleRecoverScheInfoErr(err, job)
    }
    SchedulerLogger.Infof("Complete recover job %s stages from restoring work",
        job.GetID())
    return nil
}

func (recover *BaseJobRecover)ReConstructJobState(job Job) error {
    SchedulerLogger.Infof("Start reconstruct job %s state from current stages",
        job.GetID())
    scheduler := GetScheduler()

    enableRightControl := scheduler.enableRightControl
    job.SetEnableRightControl(enableRightControl)

    jobSchedInfo := job.GetScheduleInfo()
    execInfo := jobSchedInfo.ExecInfo()
    stageStateInfo := make(map[string]uint32)

    /*
     * Because a stage may fail or lost, and retried for many times,
     * so a stage may exist in the done list for many times. it may
     * also exist in multiple list. We need handle this complicated
     * cases and get the last correct state.
     */
    for _, stageInfo := range execInfo.DoneStages() {
        stage := stageInfo.Stage()
        if stage == nil {
            continue
        }
        snapshotInfo := stageInfo.SnapshotStageInfo()
        if snapshotInfo == nil {
            SchedulerLogger.Infof("Recover Job %s stage %s name %s fail, no snapshot info\n",
                job.GetID(), stage.GetID(), stage.Name())
            bioJobFailInfo := NewBioJobFailInfo("", "",
                BIOJOB_FAILREASON_RECOVERYJOBNOSNAPSHOT, nil, nil)
            failReason := bioJobFailInfo.BioJobFailInfoFormat()
            job.SetFailReason(failReason)
            scheduler.IssueJobTerminateEvent(job, JOB_EVENT_FAIL)
            GetDashBoard().UpdateStats(STAT_JOB_RECOVER_FAIL, 1)
            GetDashBoard().UpdateStats(STAT_JOB_GRAPH_RESTORE_FAIL, 1)
            return errors.New("Miss the stage snapshot info")
        }
        if snapshotInfo.State == STAGE_FAIL {
            SchedulerLogger.Infof("Check done stage %s of job %s state STAGE_FAIL\n",
                stage.GetID(), job.GetID())
            stageStateInfo[stage.GetID()] = STAGE_FAIL
        } else if snapshotInfo.State == STAGE_DONE {
            SchedulerLogger.Infof("Check done stage %s of job %s state STAGE_DONE\n",
                stage.GetID(), job.GetID())
            stageStateInfo[stage.GetID()] = STAGE_DONE
        } else if snapshotInfo.State == STAGE_LOST {
            SchedulerLogger.Infof("Check done stage %s of job %s state STAGE_LOST\n",
                stage.GetID(), job.GetID())
            stageStateInfo[stage.GetID()] = STAGE_LOST
        } else if snapshotInfo.State == STAGE_RUNNING {
            /* Recover as much as possible */
            SchedulerLogger.Infof("Check done stage %s of job %s state STAGE_RUNNING, mark it fail\n",
                stage.GetID(), job.GetID())
            stageStateInfo[stage.GetID()] = STAGE_FAIL
        } else {
            SchedulerLogger.Infof("Recover Job %s stage %s name %s fail, invalid done state %d\n",
                job.GetID(), stage.GetID(), stage.Name(), snapshotInfo.State)
            bioJobFailInfo := NewBioJobFailInfo("", "",
                BIOJOB_FAILREASON_RECOVERYJOBIVALIDDONESTATE, nil, nil)
            failReason := bioJobFailInfo.BioJobFailInfoFormat()
            job.SetFailReason(failReason)
            scheduler.IssueJobTerminateEvent(job, JOB_EVENT_FAIL)
            GetDashBoard().UpdateStats(STAT_JOB_RECOVER_FAIL, 1)
            GetDashBoard().UpdateStats(STAT_JOB_GRAPH_RESTORE_FAIL, 1)
            return errors.New("Invalid done state")
        }
        SchedulerLogger.Infof("Recover Job %s stage %s name %s done\n",
            job.GetID(), stage.GetID(), stage.Name())
    }

    for stageId, _ := range execInfo.PendingStages() {
        stageStateInfo[stageId] = STAGE_SUBMITTED
    }

    /*recover the state of done stages*/
    SchedulerLogger.Infof("Recover job %s %d done stages %d pending stages\n", job.GetID(),
        len(execInfo.DoneStages()), len(execInfo.PendingStages()))
    job.ReConstructJobScheduleState(stageStateInfo)
    for stageId, _ := range stageStateInfo {
        stage := job.GetStage(stageId)
        if stage != nil {
            info := NewFrontEndNotcieInfo(job.GetID(), job.Name(), stage.ToBioflowStageInfo())
            e := NewNoticeEvent(FRONTENDNOTICEEVENT, info)
            eventbus.Publish(e)
        }
    }
    SchedulerLogger.Infof("Complete reconstruct job %s state from current stages", job.GetID())
    return nil
}

func (recover *BaseJobRecover)RecoverJobDataFromStage(job Job) {
    SchedulerLogger.Infof("Start recover data of job %s from current stages",
        job.GetID())
    jobSchedInfo := job.GetScheduleInfo()
    execInfo := jobSchedInfo.ExecInfo()

    /*
     * Don't recover data dependency for done stages. It may cause
     * some files to be downloaded twice when cross reboot, but it
     * help save memory because done stages already recycled resources
     * it required. So we don't call TryRecoverData for any done stage.
     */
    for stageId, _ := range execInfo.PendingStages() {
        stage := job.GetStage(stageId)
        job.TryRecoverData(stage)
    }
    SchedulerLogger.Infof("Complete recover data of job %s from current stages",
        job.GetID())
}

type WomJobRecover struct {
    *BaseJobRecover
}

func (recover *WomJobRecover)RecoverJob(job Job, startScheduleJob bool) error {
    SchedulerLogger.Infof("WomJobRecover(job %s): starts recover job",
        job.GetID())
    scheduler := GetScheduler()
    db := GetDBService()
    err, jobSchedDBInfo := db.GetJobScheduleInfo(job.GetID())
    if err != nil {
        handleGetScheDBInfoErr(err, job)
        return err
    } else if jobSchedDBInfo == nil {
        SchedulerLogger.Infof("No schedule info for %s DB, submit as new Job\n",
            job.GetID())
        /*Mark job need schedule but not kick scheduler*/
        scheduler.TryKickScheduleJob(job, true)
        return nil
    }

    jobSchedInfo := job.GetScheduleInfo()
    err = jobSchedInfo.RecoverFromDB(jobSchedDBInfo,
        true, true)
    if err != nil {
        return handleRecoverScheInfoErr(err, job)
    }

    execInfo := jobSchedInfo.ExecInfo()
    runState := execInfo.GetExecState()
    SchedulerLogger.Infof("WomJobRecover(job %s): will recover to exec state %s",
        job.GetID(), JobExecStateToString(runState))
    var recoverLevel int
    switch runState {
    case PREPARING:
        recoverLevel = 1
    case SCHEDULING:
        recoverLevel = 2
    case CLEANUPING:
        recoverLevel = 3
    case RESTORING:
        recoverLevel = 4
    case RIGHTCONTROL:
        recoverLevel = 5
    }
    /*step 1. recover stages*/
    recoverWorkList := []func(Job, *JobScheduleDBInfo)error {
        recover.RecoverPrepare,
        recover.RecoverGraph,
        recover.RecoverCleanupOffload,
        recover.RecoverRestore,
        recover.RecoverChownOffload,
    }

    for i := 0; i < recoverLevel; i++ {
        recoverWork := recoverWorkList[i]
        err := recoverWork(job, jobSchedDBInfo)
        if err != nil {
            SchedulerLogger.Errorf("WomJobRecover(job %s): fail on recoverWork %s\n",
                job.GetID(), err.Error())
            return err
        }
    }

    SchedulerLogger.Infof("WomJobRecover(job %s): will reconstruct job stage state",
        job.GetID())
    /*step 2. reconstrcut job from stage event*/
    err = recover.ReConstructJobState(job)
    if err != nil {
        SchedulerLogger.Errorf("WomJobRecover(job %s): fail on reconstruct job state %s\n",
            job.GetID(), err.Error())
        return err
    }

    /*step 3. recover data record form scheduled stages*/
    SchedulerLogger.Infof("WomJobRecover(job %s): will recover data provision state",
        job.GetID())
    recover.RecoverJobDataFromStage(job)

    /*
     * re-build job's state after recovery. The statistics for job manager and
     * user manager also need to be re-constructed.
     */
    if len(execInfo.DoneStages()) + len(execInfo.PendingStages()) > 0 {
        if JobStateIsPaused(job.State()) {
            SchedulerLogger.Infof("Job %s will be recovered to paused\n",
                job.GetID())
            /*just rebuild the stats, so pass invalid old state*/
            jobEvent := NewJobEvent(JOBEVENTID, job, JOB_STATE_INVALID, JOB_EVENT_PAUSED)
            eventbus.Publish(jobEvent)
        } else {
            SchedulerLogger.Infof("Job %s will be recovered to scheduled\n",
                job.GetID())
            oldState := job.State()
            if job.MarkScheduled() {
                jobEvent := NewJobEvent(JOBEVENTID, job, oldState, JOB_EVENT_SCHEDULED)
                eventbus.Publish(jobEvent)
            }
        }
    }

    /*step 4. track new state of pending stages */
    recover.TrackPendingStages(job)
    /*
     * evaluate the job schedule metric to reflect the reconstruction
     * of job schedule state
     */
    scheduler.jobTracker.EvaluateJobMetric(job, false)

    /* submit the job to scheduler but not kick schedule*/
    scheduler.TryKickScheduleJob(job, !startScheduleJob)
    SchedulerLogger.Infof("Job %s is submit to schedule queue for schedule\n",
        job.GetID())
    SchedulerLogger.Infof("WomJobRecover(job %s): complete recover \n",
        job.GetID())

    return nil
}

func (recover *WomJobRecover) RecoverGraph(job Job, jobSchedDBInfo *JobScheduleDBInfo) error {
    SchedulerLogger.Infof("Start recover job %s stages from building graph",
        job.GetID())
    err, buildStageErrInfo := job.BuildFlowGraph()
    if err != nil && err != GB_ERR_SUSPEND_BUILD {
        return handleBuildStageErr(err, job, buildStageErrInfo)
    } else if err == GB_ERR_SUSPEND_BUILD {
        SchedulerLogger.Infof("Recover execute the partial graph of job %s \n",
            job.GetID())
    }

    /*
     * Restore the flow graph of job to the latest state
     */
    graphInfo, _ := job.CreateFlowGraphInfo()
    err = graphInfo.FlowGraphInfoFromJSON(jobSchedDBInfo.GraphInfo)
    if err != nil {
        return handleRecoverGraphInfoErr(err, job)
    }
    jobSchedInfo := job.GetScheduleInfo()

    /*
     * Restore wom graph and try to collect all the stages pending before last reboot.
     * For wom graph scheduler, this should be done before recovering from DB.
     */
    err, restored, buildStageErrInfo := job.StepRestoreFlowGraph(graphInfo)

    /*The Wom Graph is expected to be restored completely*/
    if err != nil || !restored {
        return handleRestoreGraphErr(err, job, buildStageErrInfo)
    }

    /*
     * Start recovering the job schedule info, don't allow partial recovery but
     * allow missing stages. This may be due to the in-consistent state between
     * job schedule info and WOM VM engine. This is possible, but need adjust the
     * state by following wom graph's state.
     */
    err = jobSchedInfo.RecoverFromDB(jobSchedDBInfo,
        false, true)
    if err != nil {
        return handleRecoverScheInfoErr(err, job)
    }
    SchedulerLogger.Infof("Complete recover job %s stages from building graph",
        job.GetID())

    return nil
}

func (recover *WomJobRecover) TrackPendingStages(job Job) {
    SchedulerLogger.Infof("Start track job %s new state of pending stages", job.GetID())
    scheduler := GetScheduler()
    beScheduler := scheduler.BackendScheduler()

    jobSchedInfo := job.GetScheduleInfo()
    execInfo := jobSchedInfo.ExecInfo()
    /*
     * complete the pending task here by simulating the job event
     */
    SchedulerLogger.Infof("Recover job %s %d pending stages\n", job.GetID(),
        len(execInfo.PendingStages()))
    stageStateInfo := make(map[string]uint32)
    missingStages := make(map[string]*StageExecInfo)
    /*
     * start recover the pending stages. Because during the downtime of bioflow,
     * some stages may have done, fail or lost. So just pulls their state and simulate
     * a notification to handle the event.
     */
    for stageId, stageInfo := range execInfo.PendingStages() {
        SchedulerLogger.Infof("Wom Job Recoverer Recover Job %s pending stage %s\n",
            job.GetID(), stageId)
        backendId := stageInfo.BackendID()
        taskId := stageInfo.TaskID()

        stage := job.GetStage(stageId)
        if stage == nil {
            SchedulerLogger.Errorf("The stage %s of job %s can't recover with  wom graph state, mark lost\n",
                stageId, job.GetID())
            stageStateInfo[stageId] = STAGE_LOST
            missingStages[stageId] = stageInfo
        } else {
            err := scheduler.TrackJobTask(job, stage, backendId, taskId)
            if err != nil {
                SchedulerLogger.Errorf("Fail track the task %s of backend %s for job %s stage %s, mark lost\n",
                    taskId, backendId, job.GetID(), stageId)
                stageStateInfo[stageId] = STAGE_LOST
                GetDashBoard().UpdateStats(STAT_JOB_RECOVER_FAIL, 1)
                GetDashBoard().UpdateStats(STAT_TASK_TRACK_FAIL, 1)
                continue
            }
        }

        SchedulerLogger.Infof("Recover from backend %s task %s state\n",
            backendId, taskId)
        err, ret := beScheduler.RecoverBackendTask(backendId, taskId)
        if err != nil || ret == RECOVER_TASK_OPERR {
            SchedulerLogger.Infof("stage %s marked lost and retry it safely\n",
                stageId)
            stageStateInfo[stageId] = STAGE_LOST
            GetDashBoard().UpdateStats(STAT_RECOVER_TASK_FAIL, 1)
        } else if ret == RECOVER_TASK_RUNNING {
            SchedulerLogger.Infof("stage %s marked running\n",
                stageId)
            stageStateInfo[stageId] = STAGE_RUNNING
            GetDashBoard().UpdateStats(STAT_RECOVERED_TASKS, 1)
        } else if ret == RECOVER_TASK_FINISHED {
            SchedulerLogger.Infof("stage %s marked finished\n",
                stageId)
            stageStateInfo[stageId] = STAGE_DONE
            GetDashBoard().UpdateStats(STAT_RECOVERED_TASKS, 1)
        } else if ret == RECOVER_TASK_QUEUED {
            SchedulerLogger.Infof("stage %s marked queued\n",
                stageId)
            stageStateInfo[stageId] = STAGE_QUEUED
            GetDashBoard().UpdateStats(STAT_RECOVERED_TASKS, 1)
        } else if ret == RECOVER_TASK_FAIL {
            SchedulerLogger.Infof("stage %s marked fail\n",
                stageId)
            stageStateInfo[stageId] = STAGE_FAIL
            GetDashBoard().UpdateStats(STAT_RECOVERED_TASKS, 1)
        } else if ret == RECOVER_TASK_UNKNOWN{
            SchedulerLogger.Infof("stage %s state unknown, marked lost\n",
                stageId)
            stageStateInfo[stageId] = STAGE_LOST
            GetDashBoard().UpdateStats(STAT_RECOVER_TASK_FAIL, 1)
        } else {
            SchedulerLogger.Errorf("Un-handled state for stage %s ret %d\n",
                stageId, ret)
            GetDashBoard().UpdateStats(STAT_RECOVER_TASK_FAIL, 1)
            stageStateInfo[stageId] = STAGE_LOST
        }
    }

    /*
     * Handle the missing stages
     */
    SchedulerLogger.Infof("Wom Job Recoverer start handle %d missing stages\n",
        len(missingStages))
    for stageId, stageInfo := range missingStages {
        execInfo.DeletePendingStageInfo(stageId)
        err := beScheduler.KillTask(stageInfo.BackendID(),
            stageInfo.TaskID())
        if err != nil {
            SchedulerLogger.Errorf("Fail to kill the missing stage backend task %s/%s: %s\n",
                stageInfo.BackendID(), stageInfo.TaskID(), err.Error())
        }
    }
    SchedulerLogger.Infof("Wom Job Recoverer complete handle %d missing stages\n",
        len(missingStages))

    /*
     * complete the pending task here by simulating the job event
     */
    for stageId, stageInfo := range execInfo.PendingStages() {
        if stageStateInfo[stageId] == STAGE_DONE {
            SchedulerLogger.Infof("Recovery complete Job %s pending stage %s STAGE_DONE\n",
                job.GetID(), stageId)
            scheduler.PostPsudoJobEvent(stageInfo.BackendID(), stageInfo.TaskID(),
                STAGE_DONE, "recovery pull stage state done", job, stageId)
        } else if stageStateInfo[stageId] == STAGE_LOST {
            SchedulerLogger.Infof("Recovery complete Job %s pending stage %s STAGE_LOST\n",
                job.GetID(), stageId)
            scheduler.PostPsudoJobEvent(stageInfo.BackendID(), stageInfo.TaskID(),
                STAGE_LOST, "recovery pull stage state lost", job, stageId)
        } else if stageStateInfo[stageId] == STAGE_FAIL {
            SchedulerLogger.Infof("Recovery complete Job %s pending stage %s STAGE_FAIL\n",
                job.GetID(), stageId)
            scheduler.PostPsudoJobEvent(stageInfo.BackendID(), stageInfo.TaskID(),
                STAGE_FAIL, "recovery pull stage state fail", job, stageId)
        } else if stageStateInfo[stageId] == STAGE_RUNNING {
            scheduler.PostPsudoJobEvent(stageInfo.BackendID(), stageInfo.TaskID(),
                STAGE_RUNNING, "recovery pull stage state running", job, stageId)
        }
    }

    SchedulerLogger.Infof("Complete track job %s new state of pending stages", job.GetID())
}

type BLJobRecover struct {
    *BaseJobRecover
}

/*
 * It is the main rountine called by job recovery process when
 * bioflow restart from a crash or exit:
 * 1) recover the jobExecInfo from the database
 * 2) Mark all the done stages as completed in the job's flow
 *    graph (which is already re-generated by job mgr).
 * 3) Check backend task state of all the pending stages, update
 *    its state: completed, running or lost.
 * 4) submit the job to schedule queue after all state is update to
 *    date.
 */
func (recover *BLJobRecover) RecoverJob(job Job, startScheduleJob bool) error {
    SchedulerLogger.Infof("BLJobRecover(job %s): starts recover job",
        job.GetID())
    scheduler := GetScheduler()
    db := GetDBService()
    err, jobSchedDBInfo := db.GetJobScheduleInfo(job.GetID())
    if err != nil {
        handleGetScheDBInfoErr(err, job)
        return err
    } else if jobSchedDBInfo == nil {
        SchedulerLogger.Infof("No schedule info for %s DB, submit as new Job\n",
            job.GetID())
        /*Mark job need schedule but not kick scheduler*/
        scheduler.TryKickScheduleJob(job, true)
        return nil
    }

    jobSchedInfo := job.GetScheduleInfo()
    err = jobSchedInfo.RecoverFromDB(jobSchedDBInfo,
        true, true)
    if err != nil {
        return handleRecoverScheInfoErr(err, job)
    }

    execInfo := jobSchedInfo.ExecInfo()
    runState := execInfo.GetExecState()
    SchedulerLogger.Infof("BlJobRecover(job %s): will recover to exec state %s",
        job.GetID(), JobExecStateToString(runState))
    var recoverLevel int
    switch runState {
    case PREPARING:
        recoverLevel = 1
    case SCHEDULING:
        recoverLevel = 2
    case CLEANUPING:
        recoverLevel = 3
    case RESTORING:
        recoverLevel = 4
    case RIGHTCONTROL:
        recoverLevel = 5
    }
    /*step 1. recover stages*/
    recoverWorkList := []func(Job, *JobScheduleDBInfo)error {
        recover.RecoverPrepare,
        recover.RecoverGraph,
        recover.RecoverCleanupOffload,
        recover.RecoverRestore,
        recover.RecoverChownOffload,
    }

    for i := 0; i < recoverLevel; i++ {
        recoverWork := recoverWorkList[i]
        err := recoverWork(job, jobSchedDBInfo)
        if err != nil {
            SchedulerLogger.Errorf("BLJobRecover(job %s): fail on recoverWork %s\n",
                job.GetID(), err.Error())
            return err
        }
    }

    SchedulerLogger.Infof("BlJobRecover(job %s): will reconstruct job stage state",
        job.GetID())
    /*step 2. reconstrcut job from stage event*/
    err = recover.ReConstructJobState(job)
    if err != nil {
        SchedulerLogger.Errorf("BLJobRecover(job %s): fail on reconstruct job state %s\n",
            job.GetID(), err.Error())
        return err
    }

    /*step 3. recover data record form scheduled stages*/
    SchedulerLogger.Infof("BlJobRecover(job %s): will recover data provision state",
        job.GetID())
    recover.RecoverJobDataFromStage(job)

    /*
     * re-build job's state after recovery. The statistics for job manager and
     * user manager also need to be re-constructed.
     */
    if len(execInfo.DoneStages()) + len(execInfo.PendingStages()) > 0 {
        if JobStateIsPaused(job.State()) {
            SchedulerLogger.Infof("Job %s will be recovered to paused\n",
                job.GetID())
            /*just rebuild the stats, so pass invalid old state*/
            jobEvent := NewJobEvent(JOBEVENTID, job, JOB_STATE_INVALID, JOB_EVENT_PAUSED)
            eventbus.Publish(jobEvent)
        } else {
            SchedulerLogger.Infof("Job %s will be recovered to scheduled\n",
                job.GetID())
            oldState := job.State()
            if job.MarkScheduled() {
                jobEvent := NewJobEvent(JOBEVENTID, job, oldState, JOB_EVENT_SCHEDULED)
                eventbus.Publish(jobEvent)
            }
        }
    }

    /*step 4. track new state of pending stages */
    recover.TrackPendingStages(job)
    /*
     * evaluate the job schedule metric to reflect the reconstruction
     * of job schedule state
     */
    scheduler.jobTracker.EvaluateJobMetric(job, false)

    /* submit the job to scheduler but not kick schedule*/
    scheduler.TryKickScheduleJob(job, !startScheduleJob)
    SchedulerLogger.Infof("Job %s is submit to schedule queue for schedule\n",
        job.GetID())
    SchedulerLogger.Infof("BlJobRecover(job %s): complete recover \n",
        job.GetID())

    return nil
}

func (recover *BLJobRecover) RecoverGraph(job Job, jobSchedDBInfo *JobScheduleDBInfo) error {
    SchedulerLogger.Infof("Start recover job %s stages from building graph",
        job.GetID())
    err, buildStageErrInfo := job.BuildFlowGraph()
    if err != nil && err != GB_ERR_SUSPEND_BUILD {
        return handleBuildStageErr(err, job, buildStageErrInfo)
    } else if err == GB_ERR_SUSPEND_BUILD {
        SchedulerLogger.Infof("Recover execute the partial graph of job %s \n",
            job.GetID())
    }

    /*
     * Restore the flow graph of job to the latest state
     */
    graphInfo, _ := job.CreateFlowGraphInfo()
    err = graphInfo.FlowGraphInfoFromJSON(jobSchedDBInfo.GraphInfo)
    if err != nil {
        return handleRecoverGraphInfoErr(err, job)
    }
    jobSchedInfo := job.GetScheduleInfo()
    /*
     * Because the flow graph is executed by a series of "build and execute",
     * so we need recover it step by step:
     * 1) build initial flow graph
     * 2) restore the execute state of the parital flow graph
     * 3) continue build flow graph until latest ...
     * 4) ...
     */
RestoreAndBuildGraph:
    for {
        /*
         * try to recover execution state of partial graph already
         * built currently. Only all the execution state is restored,
         * the graph builder can continue build flow graph.
         */
        err = jobSchedInfo.RecoverFromDB(jobSchedDBInfo,
            true, false)
        if err != nil {
            return handleRecoverScheInfoErr(err, job)
        }

        /*
         * Each time it will try to build flow graph, until the last state
         * is identical to current graph state and does nothing.
         */
        err, restored, buildStageErrInfo := job.StepRestoreFlowGraph(graphInfo)
        if err != nil {
            return handleRestoreGraphErr(err, job, buildStageErrInfo)
        }

        if restored {
            break RestoreAndBuildGraph
        }
    }
    SchedulerLogger.Infof("Complete recover job %s stages from building graph",
        job.GetID())

    return nil
}

func (recover *BLJobRecover) TrackPendingStages(job Job) {
    SchedulerLogger.Infof("Start track job %s new state of pending stages", job.GetID())
    scheduler := GetScheduler()
    beScheduler := scheduler.BackendScheduler()

    jobSchedInfo := job.GetScheduleInfo()
    execInfo := jobSchedInfo.ExecInfo()
    /*
     * complete the pending task here by simulating the job event
     */
    SchedulerLogger.Infof("Recover job %s %d pending stages\n", job.GetID(),
        len(execInfo.PendingStages()))
    stageStateInfo := make(map[string]uint32)
    /*
     * start recover the pending stages. Because during the downtime of bioflow,
     * some stages may have done, fail or lost. So just pulls their state and simulate
     * a notification to handle the event.
     */
    for stageId, stageInfo := range execInfo.PendingStages() {
        SchedulerLogger.Infof("Recover Job %s pending stage %s\n",
            job.GetID(), stageId)
        stage := job.GetStage(stageId)

        backendId := stageInfo.BackendID()
        taskId := stageInfo.TaskID()

        err := scheduler.TrackJobTask(job, stage, backendId, taskId)
        if err != nil {
            SchedulerLogger.Errorf("Fail track the task %s of backend %s for job %s stage %s, mark lost\n",
                taskId, backendId, job.GetID(), stageId)
            stageStateInfo[stageId] = STAGE_LOST
            GetDashBoard().UpdateStats(STAT_JOB_RECOVER_FAIL, 1)
            GetDashBoard().UpdateStats(STAT_TASK_TRACK_FAIL, 1)
            continue
        }

        SchedulerLogger.Infof("Recover from backend %s task %s state\n",
            backendId, taskId)
        err, ret := beScheduler.RecoverBackendTask(backendId, taskId)
        if err != nil || ret == RECOVER_TASK_OPERR {
            SchedulerLogger.Infof("stage %s name %s marked lost and retry it safely\n",
                stageId, stage.Name())
            stageStateInfo[stageId] = STAGE_LOST
            GetDashBoard().UpdateStats(STAT_RECOVER_TASK_FAIL, 1)
        } else if ret == RECOVER_TASK_RUNNING {
            SchedulerLogger.Infof("stage %s name %s marked running\n",
                stageId, stage.Name())
            stageStateInfo[stageId] = STAGE_RUNNING
            GetDashBoard().UpdateStats(STAT_RECOVERED_TASKS, 1)
        } else if ret == RECOVER_TASK_FINISHED {
            SchedulerLogger.Infof("stage %s name %s marked finished\n",
                stageId, stage.Name())
            stageStateInfo[stageId] = STAGE_DONE
            GetDashBoard().UpdateStats(STAT_RECOVERED_TASKS, 1)
        } else if ret == RECOVER_TASK_QUEUED {
            SchedulerLogger.Infof("stage %s name %s marked queued\n",
                stageId, stage.Name())
            stageStateInfo[stageId] = STAGE_QUEUED
            GetDashBoard().UpdateStats(STAT_RECOVERED_TASKS, 1)
        } else if ret == RECOVER_TASK_FAIL {
            SchedulerLogger.Infof("stage %s name %s marked fail\n",
                stageId, stage.Name())
            stageStateInfo[stageId] = STAGE_FAIL
            GetDashBoard().UpdateStats(STAT_RECOVERED_TASKS, 1)
        } else if ret == RECOVER_TASK_UNKNOWN{
            SchedulerLogger.Infof("stage %s name %s state unknown, marked lost\n",
                stageId, stage.Name())
            stageStateInfo[stageId] = STAGE_LOST
            GetDashBoard().UpdateStats(STAT_RECOVER_TASK_FAIL, 1)
        } else {
            SchedulerLogger.Errorf("Un-handled state for stage %s ret %d\n",
                stageId, ret)
            GetDashBoard().UpdateStats(STAT_RECOVER_TASK_FAIL, 1)
            stageStateInfo[stageId] = STAGE_LOST
        }
    }

    for stageId, stageInfo := range execInfo.PendingStages() {
        if stageStateInfo[stageId] == STAGE_DONE {
            SchedulerLogger.Infof("Recovery complete Job %s pending stage %s STAGE_DONE\n",
            job.GetID(), stageId)
            scheduler.PostPsudoJobEvent(stageInfo.BackendID(), stageInfo.TaskID(),
            STAGE_DONE, "recovery pull stage state done", job, stageId)
        } else if stageStateInfo[stageId] == STAGE_LOST {
            SchedulerLogger.Infof("Recovery complete Job %s pending stage %s STAGE_LOST\n",
            job.GetID(), stageId)
            scheduler.PostPsudoJobEvent(stageInfo.BackendID(), stageInfo.TaskID(),
            STAGE_LOST, "recovery pull stage state lost", job, stageId)
        } else if stageStateInfo[stageId] == STAGE_FAIL {
            SchedulerLogger.Infof("Recovery complete Job %s pending stage %s STAGE_FAIL\n",
            job.GetID(), stageId)
            scheduler.PostPsudoJobEvent(stageInfo.BackendID(), stageInfo.TaskID(),
            STAGE_FAIL, "recovery pull stage state fail", job, stageId)
        } else if stageStateInfo[stageId] == STAGE_RUNNING {
            scheduler.PostPsudoJobEvent(stageInfo.BackendID(), stageInfo.TaskID(),
            STAGE_RUNNING, "recovery pull stage state running", job, stageId)
        }
    }

    SchedulerLogger.Infof("Complete track job %s new state of pending stages", job.GetID())
}

func handleGetScheDBInfoErr (err error, job Job) {
    scheduler := GetScheduler()
    SchedulerLogger.Infof("Can't recover jobs from database\n")
    bioJobFailInfo := NewBioJobFailInfo("", "",
        BIOJOB_FAILREASON_RECOVERYFROMDATABASE, err, nil)
    failReason := bioJobFailInfo.BioJobFailInfoFormat()
    job.SetFailReason(failReason)
    scheduler.IssueJobTerminateEvent(job, JOB_EVENT_RECOVER_PSEUDONE)
    GetDashBoard().UpdateStats(STAT_JOB_RECOVER_FAIL, 1)
    GetDashBoard().UpdateStats(STAT_DB_READ_FAIL, 1)
}

func handleRecoverScheInfoErr (err error, job Job) error {
    scheduler := GetScheduler()
    SchedulerLogger.Errorf("Can't restore job %s exec info from database: %s\n",
        job.GetID(), err.Error())
    bioJobFailInfo := NewBioJobFailInfo("", "",
        BIOJOB_FAILREASON_RESTOREJOBEXEC, err, nil)
    failReason := bioJobFailInfo.BioJobFailInfoFormat()
    job.SetFailReason(failReason)
    scheduler.IssueJobTerminateEvent(job, JOB_EVENT_RECOVER_PSEUDONE)
    GetDashBoard().UpdateStats(STAT_JOB_RECOVER_FAIL, 1)
    GetDashBoard().UpdateStats(STAT_DB_RESTORE_FAIL, 1)
    return errors.New("Recover job exec from DB fail: " + err.Error())
}

func handleBuildStageErr (err error, job Job, buildStageErrInfo *BuildStageErrorInfo) error {
    scheduler := GetScheduler()
    SchedulerLogger.Errorf("Build Job %s Flow Graph failure: %s\n",
        job.GetID(), err.Error())
    bioJobFailInfo := NewBioJobFailInfo("", "", BIOJOB_FAILREASON_BUILDGRAPH, err, buildStageErrInfo)
    failReason := bioJobFailInfo.BioJobFailInfoFormat()
    job.SetFailReason(failReason)
    scheduler.IssueJobTerminateEvent(job, JOB_EVENT_RECOVER_PSEUDONE)
    GetDashBoard().UpdateStats(STAT_JOB_RECOVER_FAIL, 1)
    GetDashBoard().UpdateStats(STAT_JOB_BUILD_FAIL, 1)
    return err
}

func handleRecoverGraphInfoErr(err error, job Job) error {
    scheduler := GetScheduler()
    SchedulerLogger.Errorf("Can't restore flow graph when recover job %s: %s\n",
        job.GetID(), err.Error())
    bioJobFailInfo := NewBioJobFailInfo("", "",
        BIOJOB_FAILREASON_RESTOREJOBGRAPH, err, nil)
    failReason := bioJobFailInfo.BioJobFailInfoFormat()
    job.SetFailReason(failReason)
    scheduler.IssueJobTerminateEvent(job, JOB_EVENT_RECOVER_PSEUDONE)
    GetDashBoard().UpdateStats(STAT_JOB_RECOVER_FAIL, 1)
    GetDashBoard().UpdateStats(STAT_DB_RESTORE_FAIL, 1)
    return err
}

func handleRestoreGraphErr(err error, job Job, buildStageErrInfo *BuildStageErrorInfo) error {
    scheduler := GetScheduler()
    SchedulerLogger.Errorf("Can't restore flow graph for job %s\n",
        job.GetID())
    bioJobFailInfo := NewBioJobFailInfo("", "",
        BIOJOB_FAILREASON_RESTOREJOBGRAPH, err, buildStageErrInfo)
    failReason := bioJobFailInfo.BioJobFailInfoFormat()
    job.SetFailReason(failReason)
    scheduler.IssueJobTerminateEvent(job, JOB_EVENT_RECOVER_PSEUDONE)
    GetDashBoard().UpdateStats(STAT_JOB_RECOVER_FAIL, 1)
    GetDashBoard().UpdateStats(STAT_JOB_GRAPH_RESTORE_FAIL, 1)
    return err
}
