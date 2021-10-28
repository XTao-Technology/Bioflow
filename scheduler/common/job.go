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
    "time"
    "github.com/nu7hatch/gouuid"
    "github.com/xtao/bioflow/dbservice"
    . "github.com/xtao/bioflow/message"
    . "github.com/xtao/bioflow/common"
    )

/*Job state definition*/
const (
    JOB_STATE_INVALID int = -1
    JOB_STATE_CREATED int = 0
    JOB_STATE_RUNNING int = 1
    JOB_STATE_FAIL int = 2
    JOB_STATE_FINISHED int = 3
    JOB_STATE_LOST int = 4
    JOB_STATE_RECOVERY int = 5
    JOB_STATE_PAUSED int = 6
    JOB_STATE_CANCELED int = 7
    JOB_STATE_FINISHING int = 8
    JOB_STATE_FAILING int = 9
    JOB_STATE_PSUDONE int = 10
    JOB_STATE_PSUDOING int = 11
)

/*Job event definition*/
const (
    JOB_EVENT_INVALID int = 0
    JOB_EVENT_SCHEDULED int = 1
    JOB_EVENT_ERROR int = 2
    JOB_EVENT_FAIL int = 3
    JOB_EVENT_LOST int = 4
    JOB_EVENT_COMPLETE int = 5
    JOB_EVENT_CANCELED int = 6
    JOB_EVENT_PAUSED int = 7
    JOB_EVENT_RESUMED int = 8
    JOB_EVENT_PSEUDONE int = 9
    JOB_EVENT_RECOVER_PSEUDONE int = 10
)

func JobMgmtEventToStr(event int) string {
    switch event {
        case JOB_EVENT_SCHEDULED:
            return "JOB_EVENT_SCHEDULED"
        case JOB_EVENT_ERROR:
            return "JOB_EVENT_ERROR"
        case JOB_EVENT_FAIL:
            return "JOB_EVENT_FAIL"
        case JOB_EVENT_LOST:
            return "JOB_EVENT_LOST"
        case JOB_EVENT_COMPLETE:
            return "JOB_EVENT_COMPLETE"
        case JOB_EVENT_CANCELED:
            return "JOB_EVENT_CANCELED"
        case JOB_EVENT_PAUSED:
            return "JOB_EVENT_PAUSED"
        case JOB_EVENT_RESUMED:
            return "JOB_EVENT_RESUMED"
        case JOB_EVENT_PSEUDONE:
            return "JOB_EVENT_PSEUDONE"
        default:
            return "JOB_EVENT_INVALID"
    }
}

/*
 * Define the stage and job events here. All the number
 * should be unique
 */
const (
    /*Stage Execution State and Events*/
    STAGE_INVALID_STATE uint32 = 0
    STAGE_INITIAL uint32 = 1
    STAGE_SUBMITTED uint32 = 2
    STAGE_QUEUED uint32 = 3
    STAGE_RUNNING uint32 = 4
    STAGE_FAIL uint32 = 5
    STAGE_DONE uint32 = 6
    STAGE_LOST uint32 = 7
    STAGE_SUBMIT_FAIL uint32 = 8

    /*Job Execution Event*/
    JOB_START uint32 = 10
    JOB_CLEANUP uint32 = 11
    JOB_CLEANUPED uint32 = 12
    JOB_CHECK_STATE uint32 = 13
)

func JobExecEventToString(event uint32) string {
    switch event {
        case STAGE_SUBMITTED:
            return "STAGE_SUBMITTED"
        case STAGE_QUEUED:
            return "STAGE_QUEUED"
        case STAGE_RUNNING:
            return "STAGE_RUNNING"
        case STAGE_FAIL:
            return "STAGE_FAIL"
        case STAGE_DONE:
            return "STAGE_DONE"
        case STAGE_LOST:
            return "STAGE_LOST"
        case JOB_START:
            return "JOB_START"
        case JOB_CLEANUP:
            return "JOB_CLEANUP"
        case JOB_CLEANUPED:
            return "JOB_CLEANUPED"
        case JOB_CHECK_STATE:
            return "JOB_CHECK_STATE"
        default:
            return "INVALID"
    }
}

func JobStateIsRunning(state int) bool {
    return state == JOB_STATE_RUNNING
}

func JobStateIsCreated(state int) bool {
    return state == JOB_STATE_CREATED
}

func JobStateIsPaused(state int) bool {
    return state == JOB_STATE_PAUSED
}

func JobStateToStr(state int) string {
    switch state {
        case JOB_STATE_CREATED:
            return "CREATED"
        case JOB_STATE_RUNNING:
            return "RUNNING"
        case JOB_STATE_FAIL:
            return "FAIL"
        case JOB_STATE_FINISHED:
            return "FINISHED"
        case JOB_STATE_LOST:
            return "LOST"
        case JOB_STATE_RECOVERY:
            return "RECOVERY"
        case JOB_STATE_PAUSED:
            return "PAUSED"
        case JOB_STATE_CANCELED:
            return "CANCELED"
        case JOB_STATE_FINISHING:
            return "FINISHING"
        case JOB_STATE_FAILING:
            return "FAILING"
        case JOB_STATE_PSUDOING:
            return "PSUDOING"
        case JOB_STATE_PSUDONE:
            return "PSUDONE"
        default:
            return "N/A"
    }
}

func JobStrToState(state string) int {
    switch state {
        case "CREATED":
            return JOB_STATE_CREATED
        case "RUNNING":
            return JOB_STATE_RUNNING
        case "FAIL":
            return JOB_STATE_FAIL
        case "FINISHED":
            return JOB_STATE_FINISHED
        case "LOST":
            return JOB_STATE_LOST
        case "RECOVERY":
            return JOB_STATE_RECOVERY
        case "PAUSED":
            return JOB_STATE_PAUSED
        case "CANCELED":
            return JOB_STATE_CANCELED
        default:
            return -1
    }
}


type JobID struct {
	ID *uuid.UUID
}

func NewJobID(x *uuid.UUID) *JobID {
	jobId := &JobID {
		ID: x,
	}
	return jobId
}

func (id *JobID) String() string {
	return id.ID.String()
}

func String2JobID(s string) *JobID {
	id, err := uuid.ParseHex(s)
	if err != nil {
		return nil
	} else {
		return NewJobID(id)
	}
}

func GenerateJobID() *JobID {
    id, err := uuid.NewV4()
	if err != nil {
		return nil
	} else {
		return NewJobID(id)
	}
}

type JobScheduleOpType int
const (
    JOB_OP_NOOP JobScheduleOpType = -1
    JOB_OP_SCHEDULE JobScheduleOpType = 1
    JOB_OP_TERMINATE JobScheduleOpType = 2
)

type JobScheduleOpData interface{
}

type Job interface {
    GetID() string
    ID() *JobID
    SetID(id *JobID)
    SetState(state int)
    State() int
    SetSecID(string)
    SecID() string
    Priority() int
    SetPriority(int)
    LastState() int
    WorkDir() string
    SetWorkDir(string)
    HDFSWorkDir() string
    SetHDFSWorkDir(string)
    LogDir() string
    SetLogDir(string)
    SMID() string
    GetScheduleInfo() *JobScheduleInfo
    GetEnableRightControl() bool
    SetEnableRightControl(bool)
    SetSampleID(sm string)
    GenerateStageIndex() int
    InputDataSet() *BioDataSet
    Name() string
    CreateTime() time.Time
    FinishTime() time.Time
    SetUpdateTime(time.Time)
    UpdateTime() time.Time
    RetryLimit() int
    StageCount() int
    RunCount() int
    SetFailReason(string)
    FailReason() []string
    BuildFlowGraph() (error, *BuildStageErrorInfo)
    PipelineName() string
    Pipeline() Pipeline
    SetPipeline(Pipeline)
    GetWaitingStages() ([]Stage, []Stage)
    HandleEvent(stageId string, event uint32, info *EventNotifyInfo) (error, *BuildStageErrorInfo, ScheduleAction)
    CreateJobInfo() *dbservice.JobDBInfo
    RecoverFromJobInfo(*dbservice.JobDBInfo, bool) error
    GetStage(string) Stage
    ReConstructJobScheduleState(stageStates map[string]uint32) ScheduleAction
    EvaluateStageScheduleAction(stage Stage) ScheduleAction
    Canceled() bool
    Pause() error
    Resume() error
    Cancel()
    Retry()
	UpdateStageResourceSpec(string, *ResourceSpecJSONData) error
    MarkScheduled() bool
    Complete(bool, string)
    AllowSchedule() bool
    ReleaseResource() error
    Fini()

    RequestScheduleOperation(JobScheduleOpType, JobScheduleOpData) bool
    FinishScheduleOperation(JobScheduleOpType) (JobScheduleOpType, JobScheduleOpData)

    StepRestoreFlowGraph(info FlowGraphInfo) (error, bool, *BuildStageErrorInfo)
    GetSecurityContext() *SecurityContext
    SetSecurityContext(*SecurityContext)
    UpdateJobFlowGraphInfo() error
    GetStatus() (error, *BioflowJobStatus)
    IsRunning() bool
    SetOOMKillDisabled(bool)
    OOMKillDisabled() bool
    SetExecMode(string)
    ExecMode() string
    RestoreData() (error, ScheduleAction)
    GenereateRightControlOffloadData(bool) (error, ScheduleAction)
    GenereateCleanupOffloadStage() (error, ScheduleAction)
    PrepareData() (error, ScheduleAction)
    ScheduleStages() (map[string]Stage, ScheduleAction, error)
    NeedPrepareData() bool
    NeedRestoreData() bool
    SetDataDir(string)
    DataDir() string
    TryRecoverData(stage Stage)
    GetOutput() *JobOutput
    DeleteFiles()
    CreateFlowGraphInfo() (FlowGraphInfo, error)
    StageQuota() int
    SetStageQuota(int)
    JobTerminated() bool
    QueueIndex() int
    SetQueueIndex(index int)
    Constraints() map[string]string
    SetConstraints(map[string]string)
}
