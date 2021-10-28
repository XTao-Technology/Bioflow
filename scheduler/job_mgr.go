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
    "sync"
    "sync/atomic"
    "errors"
    "fmt"
	"time"
    "strings"

    . "github.com/xtao/bioflow/dbservice"
    . "github.com/xtao/bioflow/common"
    . "github.com/xtao/bioflow/message"
    . "github.com/xtao/bioflow/storage"
    . "github.com/xtao/bioflow/scheduler/common"
    "github.com/xtao/bioflow/eventbus"
	"github.com/renstrom/fuzzysearch/fuzzy"
    )

type JobMgr interface {
    Start() error
    RecoverFromDB() error
    SubmitJob(*SecurityContext, Job) (error, *JobID)
    HandleJobScheduleEvent(Job, int, int)
    ResumeJob(*SecurityContext, string) error
    PauseJob(*SecurityContext, string) error
    ListJobs(*SecurityContext, bool) (error, []BioflowJobInfo, []BioflowJobInfo)
    CancelJob(*SecurityContext, string) error
    GetJobIdsByPipeline(*SecurityContext, string) []string
	GetJobLogs(*SecurityContext, string, string) (error, map[string]BioflowStageLog)
    CleanupJobHistory(*SecurityContext, *JobListOpt) (error, int)
    GetStats() *BioflowJobMgrStats
    UpdateJob(ctxt *SecurityContext, jobId string, jsonData *JobJSONData) error
    KillJobTasks(ctxt *SecurityContext, jobId string, taskId string) error
}

const (
    JOBMGR_STATE_RECOVERING int = 0
    JOBMGR_STATE_READY int = 1

    JOBMGR_BATCH_REQ_COUNT int = 64
    JOBMGR_RECOVER_WORKERS uint32 = 20
    JOBMGR_RECOVER_QUEUE_SIZE int = 50
)

type jobMgr struct {
    lock sync.Mutex
    submitQueue map[string]Job
    submitLock *sync.RWMutex
    submitCond *sync.Cond
    jobTracker *jobTracker

    /*statistics*/
    recoveryTime float64
    maxJobSubmitTime float64
    totalJobNum int64
    failJobNum int64
    pausedJobNum int64
    canceledJobNum int64
    runningJobNum int64
    completeJobNum int64
    psudoCompleteNum int64

    state int
}

var globalJobMgr *jobMgr = nil

func GetJobMgr() *jobMgr {
    return globalJobMgr
}

func NewJobMgr(config *JobMgrConfig, jobTracker *jobTracker) *jobMgr {
    if globalJobMgr == nil {
        globalJobMgr = &jobMgr{
            submitQueue: make(map[string]Job),
            jobTracker: jobTracker,
            state: JOBMGR_STATE_RECOVERING,
        }
        globalJobMgr.submitLock = new(sync.RWMutex)
        globalJobMgr.submitCond = sync.NewCond(globalJobMgr.submitLock)
    }

    return globalJobMgr
}

func (jobMgr *jobMgr)IsReady() bool {
    return jobMgr.state == JOBMGR_STATE_READY
}

func (jobMgr *jobMgr)Start() error {
    subscriber := eventbus.NewSubscriber()
    subscriber.Subscribe(JOBEVENTID, jobMgr._HandleJobEvent, true)

    go func() {
        for ;; {
            SchedulerLogger.Debugf("JobMgr handles new Job\n")
            jobMgr.HandleNewJob()
            SchedulerLogger.Debugf("JobMgr handles new Job done, wait for new jobs\n")

            jobMgr.submitLock.Lock()
            if len(jobMgr.submitQueue) == 0 {
                jobMgr.submitCond.Wait()
            }
            jobMgr.submitLock.Unlock()
        }
    }()

    return nil
}


func (jobMgr *jobMgr) GetJobId (id string, vtype int) (string, error) {

	if len(id) > 36 {
		return "", errors.New("Invalid job ID.")
	}

	if len(id) == 36 {
		return id, nil
	}

	var matchIds []string

	if vtype & GET_JOBID_FROM_MEM != 0 {
		matchIds = jobMgr.SearchJobIdInMem(id)
	}

	if vtype & GET_JOBID_FROM_DB != 0 {
		matchIds = append(jobMgr.SearchJobIdInDB(id), matchIds...)
	}

	if len(matchIds) == 0 {
		return "", errors.New("No job ID match input.")
	} else if len(matchIds) == 1 {
		return matchIds[0], nil
	} else {
		errmsg := fmt.Sprintln("Multiple JobID matches:", matchIds)
		SchedulerLogger.Errorf(errmsg)
		return "", errors.New(errmsg)
	}
}

func (jobMgr *jobMgr)RecoverJob(job Job, startScheduleJob bool) error {
    jobMgr.jobTracker.AddJob(job)
    GetUserMgr().RequestJobQuota(job.GetSecurityContext(), true)
    err := RecoverJob(job, startScheduleJob)
    if err != nil {
        SchedulerLogger.Errorf("Job scheduler recover job %s fail %s\n",
            job.GetID(), err.Error())
        GetDashBoard().UpdateStats(STAT_JOB_RECOVER_FAIL, 1)
    } else {
        GetDashBoard().UpdateStats(STAT_JOB_RECOVER, 1)
        GetDashBoard().UpdateStats(STAT_JOB_TOTAL, 1)
    }

    return err
}

func (jobMgr *jobMgr)RecoverFromDB() error{
    dbService := GetDBService()

    startTime := time.Now()
    SchedulerLogger.Infof("Starting Recover Jobs from Database\n")
    err, jobIds := dbService.GetAllJobIds()
    if err != nil {
        SchedulerLogger.Errorf("Can't get job id list from DB: %s \n",
            err.Error())
        return err
    }

    SchedulerLogger.Infof("Create parallel recover workers\n")

    recoverWorkers := NewHashSyncWorker(JOBMGR_RECOVER_WORKERS,
        JOBMGR_RECOVER_QUEUE_SIZE, false)

    SchedulerLogger.Infof("Will recover %d jobs from Database\n",
        len(jobIds))
    for i := 0; i < len(jobIds); i ++ {
        err, jobInfos := dbService.GetJobById(jobIds[i])
        if err != nil {
            SchedulerLogger.Errorf("Fail to get job %s info from Database\n",
                jobIds[i])
        } else {
            job := new(bioJob)
            if len(jobInfos) != 1 {
                SchedulerLogger.Errorf("%d jobs with id %s exist in Database\n",
                    len(jobInfos), jobIds[i])
            } else {
                SchedulerLogger.Infof("Start recovering job %s from DB \n", jobIds[i])
                err := job.RecoverFromJobInfo(jobInfos[0], false)
                if err != nil {
                    SchedulerLogger.Errorf("Recover job %s from job info fail: %s\n",
                        jobIds[i], err.Error())
                } else {
                    if job.NeedPrepareData() {
                        _, _, dataDir, _ := GetStorageMgr().GenerateDirsForJob(job.GetID())
                        job.SetDataDir(dataDir)
                    }
                    err := jobMgr.InitJobPipeline(job)
                    if err != nil {
                        SchedulerLogger.Errorf("Fail to init job %s pipeline: %s\n",
                            job.GetID(), err.Error())
                    } else {
                        recoverWorkers.IssueOperation(job.GetID(), job, 
                            func(data interface{}) error {
                                job := data.(Job)
                                SchedulerLogger.Infof("Start recovering job %s schedule state \n",
                                    job.GetID())
                                /*
                                 * The job will be recovered but not scheduled immediately. Because
                                 * we can't ensure jobs are recovered in priority order, so kicking
                                 * scheduler early may cause lower priority jobs are scheduled first and
                                 * used up all resources.
                                 */
                                err := jobMgr.RecoverJob(job, false)
                                if err != nil {
                                    SchedulerLogger.Errorf("JobMgr fail to recover the job %s, err: %s \n",
                                        job.GetID(), err.Error())
                                } else {
                                    atomic.AddInt64(&jobMgr.totalJobNum, 1)
                                }

                                return err
                            }, false, 0, "")
                    }
                }
            }
        }
    }

    SchedulerLogger.Infof("JobMgr sync with all recovery workers\n")
    recoverWorkers.SyncAllOperations(0)
    SchedulerLogger.Infof("JobMgr complete sync with recovery workers\n")

    /*In the end, kick scheduler to work*/
    GetScheduler().KickScheduler()

    /*stat the recovery time*/
    StatUtilsCalcDiffSeconds(&jobMgr.recoveryTime, startTime)
    jobMgr.state = JOBMGR_STATE_READY

    return nil
}

/*
 * Recover a failed job by ID
 */
func (jobMgr *jobMgr)RecoverFailedJob(ctxt *SecurityContext, jobId string) error{
    if !jobMgr.IsReady() {
        SchedulerLogger.Error("Job mgr not ready, refuse job management\n")
        return errors.New("Job service not ready")
    }

    db := GetDBService()

    err, jobInfo, schedInfo := db.GetJobHistoryById(jobId)
    if err != nil {
        SchedulerLogger.Errorf("Fail to get job %s history\n",
            jobId)
        return err
    }
    if jobInfo == nil {
        return errors.New("No history job " + jobId + " found")
    }

    /*Security check*/
    if !ctxt.CheckSecID(jobInfo.SecID) {
        SchedulerLogger.Errorf("security check fail: %s can't recover %s's job %s\n",
            ctxt.ID(), jobInfo.SecID, jobId)
        return errors.New("Security ID doesn't match")
    }

	SchedulerLogger.Infof("Start recovering job %s \n", jobId)

	job := new(bioJob)
	var errmsg string

	err = job.RecoverFromJobInfo(jobInfo, true)
	if err != nil {
		errmsg = fmt.Sprintf("Recover job %s from job info fail: %s\n",
			jobId, err.Error())
		SchedulerLogger.Errorf(errmsg)

		return errors.New(errmsg)
	}
    
    err = jobMgr.InitJobPipeline(job)
    if err != nil {
        SchedulerLogger.Errorf("Fail to init job %s pipeline: %s\n",
            job.GetID(), err.Error())
        return err
    }

    err = db.MovJobSchedHisToJobScheduleDB(schedInfo)
    if err != nil {
        errmsg = fmt.Sprintf("Failed to move %s from JobSchedHis to JobScheduleDB: %s\n",
            jobId, err.Error())
        SchedulerLogger.Errorf(errmsg)
        return errors.New(errmsg)
    }

	err = db.MovJobHisToJob(jobInfo)
	if err != nil {
		errmsg = fmt.Sprintf("Failed to move %s from JobHistory to JobInfo: %s\n",
			jobId, err.Error())
		SchedulerLogger.Errorf(errmsg)
		return errors.New(errmsg)
	}

    /*Update the job run count*/
    job.Retry()
    err = db.UpdateJobRun(job.GetID(), job.RunCount())
    if err != nil {
        SchedulerLogger.Errorf("Can't persist job %s run count to DB: %s\n",
            job.GetID(), err.Error())
        GetDashBoard().UpdateStats(STAT_DB_UPDATE_FAIL, 1)
        return errors.New("fail to update job recovery count: " + err.Error())
    }

    /*
     * Job should be scheduled immediately.
     */
	err = jobMgr.RecoverJob(job, true)
	if err != nil {
		errmsg = fmt.Sprintf("JobMgr fail to recover the job %s, err: %s \n",
			jobId, err.Error())
		SchedulerLogger.Errorf(errmsg)
		return errors.New(errmsg)
	}

    atomic.AddInt64(&jobMgr.totalJobNum, 1)

    return nil
}

/*
 * If a stage of job queued for long time, requeue the job stage
 */
func (jobMgr *jobMgr)RequeueJobStage(ctxt *SecurityContext,
    jobId string, stageId string, resourceSpec *ResourceSpecJSONData) error {

    if !jobMgr.IsReady() {
        SchedulerLogger.Error("Job mgr not ready, refuse job management\n")
        return errors.New("Job service not ready")
    }

    err, job := jobMgr.jobTracker.FindJobByID(jobId)
    if err != nil {
        SchedulerLogger.Errorf("The job %s is not found: %s\n",
            jobId, err.Error())
        return errors.New("The job with id %s not found")
    }

    /*security check*/
    if !ctxt.CheckSecID(job.SecID()) {
        SchedulerLogger.Errorf("Security check fail: The ID %s and %s not match for job %s\n",
            ctxt.ID(), job.SecID(), jobId)
        return errors.New("Security ID doesn't match")
    }

    err = GetScheduler().RequeueJobStage(job, stageId, resourceSpec)
    if err != nil {
        SchedulerLogger.Errorf("The scheduler can't requeue job %s stage %s: %s\n",
            job.GetID(), stageId, err.Error())
        return err
    }

    return nil
}

func (jobMgr *jobMgr)HandleNewJob() {
    var jobList []Job
    jobCount := 0
    jobMgr.submitLock.Lock()
    for _, job := range jobMgr.submitQueue {
        if jobCount < JOBMGR_BATCH_REQ_COUNT {
            jobCount ++
            jobList = append(jobList, job)
            delete(jobMgr.submitQueue, job.GetID())     
            jobMgr.jobTracker.AddJob(job)
        } else {
            break
        }
    }
    jobMgr.submitLock.Unlock()

    dbService := GetDBService()
    scheduler := GetScheduler()
    for i := 0; i < len(jobList); i ++ {
        job := jobList[i]

        /*
         * init job pipeline here is very impossable, because create job graph
         * info use job's pipeline,
         * Important !!!!
         * Don't modify the init pipeline order unless truly understand the
         * pipeline when and where with job flow graph info.
         */
        err := jobMgr.InitJobPipeline(job)
        if err != nil {
            /*shouldn't reach here*/
            SchedulerLogger.Errorf("Fail to init job %s pipeline: %s\n",
                job.GetID(), err.Error())
            continue
        }

        /*
         * Preparing job work directories is a very expensive
         * and slow operation. We do it asynchronously here.
         */
        err = jobMgr.PrepareJobDirs(job)
        if err != nil {
            SchedulerLogger.Errorf("Fail to prepare work directories for job %s: %s\n",
                job.GetID(), err.Error())
            jobMgr.FailNonScheduledJob(job, "Preapre work directories fail: " + err.Error())
            continue
        }

        /*persist job to database*/
        jobInfo := job.CreateJobInfo()
        if jobInfo == nil {
            SchedulerLogger.Errorf("Create job %s info fail\n",
                job.GetID())
        } else {
            err := dbService.AddJob(jobInfo)
            if err != nil {
                SchedulerLogger.Errorf("Fail to persist job info %s \n",
                    err.Error())
            }
        }

        scheduler.SubmitJob(job)
    }
}

/*
 * Each user has information (e.g, credit) to evaluate the user
 * level priority, while each job is set per-sample priority.
 * So these priority setting should be merged to get the truly
 * appropriate job priority.
 */
func (jobMgr *jobMgr)EvaluateJobPriority(ctxt *SecurityContext,
    job Job) {
    err, credit := GetUserMgr().GetUserCredit(ctxt, ctxt.ID())
    if err != nil {
        SchedulerLogger.Debugf("The user %s have no user config, keep job priority\n",
            ctxt.ID())
        return 
    }

    /*
     * TODO: use the correct formula to calculate priority from
     * the credit
     */
     userPri := 0
     if credit > int64(JOB_MAX_PRI) {
        userPri = JOB_MAX_PRI
     } else {
        userPri = int(credit)
     }
     jobPri := job.Priority()
     if jobPri < userPri {
        jobPri = userPri
     }

     job.SetPriority(jobPri)
     SchedulerLogger.Debugf("The user %s job %s evaluated priority %d from (%d %d)\n",
        ctxt.ID(), job.GetID(), jobPri, credit, userPri)
}

func PrepareJobLogDir(logDir string) error {
    err := GetStorageMgr().MkdirOnScheduler(logDir, true, nil)
    if err != nil {
        SchedulerLogger.Errorf("Can't create Log directory %s for error %s: %s\n",
            logDir, err.Error())
        return err
    }
    return nil
}

func PrepareJobDir(ctxt *SecurityContext, job Job, workDir string) error {
    err := GetStorageMgr().MkdirOnScheduler(workDir, true, ctxt.GetUserInfo())
    if err != nil {
        SchedulerLogger.Errorf("Can't create work directory %s for job %s: %s\n",
            workDir, job.GetID(), err.Error())
        GetDashBoard().UpdateStats(STAT_JOB_FAIL, 1)
        GetDashBoard().UpdateStats(STAT_JOB_PREPARE_FAIL, 1)
        return err
    }

    return nil
}

/*Policy to determine whether to accept job or not*/
func (jobMgr *jobMgr)_CheckJobServiceState() (bool, error) {
    /*Don't accept jobs during recovery*/
    if !jobMgr.IsReady() {
        SchedulerLogger.Error("Job mgr not ready, refuse submit job\n")
        return false, errors.New("Job service not ready")
    }
    /*Check whether the service is disabled by cluster controller or not*/
	scheduler := GetScheduler()
	if scheduler.Disabled() {
		errmsg := fmt.Sprintf("schedule service is disabled for safety by cluster controller")
		SchedulerLogger.Errorf("Fail submit job: %s\n", errmsg)
        GetDashBoard().UpdateStats(STAT_JOB_SUBMIT_QUIESCE, 1)
		return false, errors.New(errmsg)
	}

    return true, nil
}

/*submit a job*/
func (jobMgr *jobMgr)SubmitJob(ctxt *SecurityContext,
    job Job) (error, *JobID) {

    /*Check service state to determine whether accept jobs or not*/
    if allow, err := jobMgr._CheckJobServiceState(); !allow {
        if err != nil {
            return err, nil
        } else {
            return errors.New("job service disabled"), nil
        }
    }

    /*performance statistics*/
    startTime := time.Now()

    GetDashBoard().UpdateStats(STAT_JOB_SUBMIT, 1)

    jobId := GenerateJobID()
    job.SetID(jobId)
    job.SetSecurityContext(ctxt)

    /* Step 0: job validation
     * 1) check job pipeline exist
     * 2) no two jobs work on same output directory: data corruption
     * 3) check job workdir and input dataset
     */
    ns := NSMapFromContext(ctxt)
    pipeline := GetPipelineMgr().GetPipeline(ns, job.PipelineName())
    if pipeline == nil {
        SchedulerLogger.Errorf("Can't find pipeline for job %s pipeline %s \n",
            job.GetID(), job.PipelineName())
        err := errors.New("The refered pipeline " + job.PipelineName() + " not exist")
        GetDashBoard().UpdateStats(STAT_JOB_FAIL, 1)
        GetDashBoard().UpdateStats(STAT_JOB_PARSE_FAIL, 1)
        return err, nil
    }

    dataset := job.InputDataSet()
    /*
     * Merge the input map of pipeline definition to job. If:
     * 1) job has the key-value pair, use job's
     * 2) otherwise, use pipeline's
     */
    dataset.MergeInputMap(pipeline.CloneInputMap())

    if job.NeedPrepareData() {
        err, workDir, dataDir, logDir := GetStorageMgr().GenerateDirsForJob(job.GetID())
        if err != nil {
            SchedulerLogger.Errorf("Can't generate directories for job %s: %s\n", job.GetID(), err.Error())
            err := errors.New("Fail to generate directories for job")
            GetDashBoard().UpdateStats(STAT_JOB_FAIL, 1)
            GetDashBoard().UpdateStats(STAT_JOB_PARSE_FAIL, 1)
            return err, nil
        }
        SchedulerLogger.Debugf("Generate job %s directories: %s %s %s\n", job.GetID(), workDir, dataDir, logDir)
        job.SetWorkDir(workDir)
        job.SetLogDir(logDir)
        job.SetDataDir(dataDir)
    } else if job.WorkDir() == "" {
        if pipeline.WorkDir() != "" {
            workDir := pipeline.WorkDir() + "/job-" + job.GetID()
            job.SetWorkDir(workDir)
            if job.LogDir() == "" {
                logDir := fmt.Sprintf("%s/%s", workDir, "logs")
                job.SetLogDir(logDir)
            }
            SchedulerLogger.Debugf("Set job's work dir to pipeline's: %s\n",
                workDir)
        } else {
            job.SetWorkDir(dataset.InputDir())
            if job.LogDir() == "" {
                logDir := fmt.Sprintf("%s/%s", dataset.InputDir(), "logs")
                job.SetLogDir(logDir)
            }
            SchedulerLogger.Debugf("Set job's work dir to input dir: %s\n",
                job.WorkDir())
        }
    } else {
        /*
         * check whether the job work directory is used by another job
         * two jobs use same work directory may corrupt data
         */
        jobIds := jobMgr.GetJobIdsByWorkDir(job.WorkDir())
        if len(jobIds) > 0 {
            GetDashBoard().UpdateStats(STAT_JOB_FAIL, 1)
            GetDashBoard().UpdateStats(STAT_JOB_PARSE_FAIL, 1)
            return errors.New("Use same work directory with " + jobIds[0]),
                nil
        }

        if job.LogDir() == "" {
            logDir := fmt.Sprintf("%s/%s", job.WorkDir(), "logs")
            job.SetLogDir(logDir)
        }
    }


    /*Set security information*/
    job.SetSecID(ctxt.ID())

    if !GetUserMgr().RequestJobQuota(ctxt, false) {
        errMsg := fmt.Sprintf("User %s have no quota to submit new job",
            ctxt.ID())
        SchedulerLogger.Errorf("Fail submit job: %s\n",
            errMsg)
        return errors.New(errMsg), nil
    }

    jobMgr.EvaluateJobPriority(ctxt, job)

    /*step 1: add job to queue, notify worker threads to handle it*/
    jobMgr._SubmitJob(job)

    atomic.AddInt64(&jobMgr.totalJobNum, 1)

    GetDashBoard().UpdateStats(STAT_JOB_TOTAL, 1)

    StatUtilsCalcMaxDiffSeconds(&jobMgr.maxJobSubmitTime, startTime)

    return nil, jobId
}

func (jobMgr *jobMgr) PrepareJobDirs(job Job) error {
    ctxt := job.GetSecurityContext()
    ns := NSMapFromContext(ctxt)
    pipeline := GetPipelineMgr().GetPipeline(ns, job.PipelineName())
    if job.NeedPrepareData() {
        SchedulerLogger.Debugf("Check and create job %s data dir %s \n",
            job.GetID(), job.DataDir())
        err := PrepareJobDir(ctxt, job, job.DataDir())
        if err != nil {
            SchedulerLogger.Errorf("Fail to prepare job %s data dir directory %s: %s\n",
                job.GetID(), job.DataDir(), err.Error())
            return err
        }
    }
    SchedulerLogger.Debugf("Check and create job %s work dir %s \n",
        job.GetID(), job.WorkDir())
    err := PrepareJobDir(ctxt, job, job.WorkDir())
    if err != nil {
        SchedulerLogger.Errorf("Fail to prepare job %s work directory %s: %s\n",
            job.GetID(), job.WorkDir(), err.Error())
        return err
    }

    SchedulerLogger.Debugf("Successfully created job %s work dir %s \n",
        job.GetID(), job.WorkDir())
    
    SchedulerLogger.Debugf("Check and create job %s log dir %s \n",
        job.GetID(), job.LogDir())
    err = PrepareJobLogDir(job.LogDir())
    if err != nil {
        SchedulerLogger.Errorf("Fail to prepare Job %s log dir %s: %s\n",
            job.GetID(), job.LogDir(), err.Error())
        return err
    }
    SchedulerLogger.Debugf("Successfully created job %s log dir %s\n",
        job.GetID(), job.LogDir())

    SchedulerLogger.Debugf("Check and create job %s HDFS work dir %s \n",
        job.GetID(), job.HDFSWorkDir())

    if job.HDFSWorkDir() == "" {
        if FSUtilsIsHDFSPath(job.WorkDir()) {
            job.SetHDFSWorkDir(job.WorkDir())
        } else if pipeline.HDFSWorkDir() != "" {
            hdfsWorkDir := pipeline.HDFSWorkDir() + "/job-" + job.GetID()
            job.SetHDFSWorkDir(hdfsWorkDir)
            SchedulerLogger.Debugf("Set job's HDFS work dir to pipeline's: %s\n",
                hdfsWorkDir)
        } else if GetScheduler().AutoScheduleHDFS(){
            /*try to assing a work directory on HDFS*/
            err, hdfsCluster := GetStorageScheduler().SelectHDFSCluster()
            if err != nil {
                SchedulerLogger.Debugf("No default HDFS cluster exist for work directory\n")
            } else {
                err, mirrorPath := FSUtilsMirrorPathToCluster(job.WorkDir(), "hdfs", hdfsCluster)
                if err != nil {
                    SchedulerLogger.Errorf("Fail to mirror work directory %s to HDFS cluster %s: %s\n",
                        job.WorkDir(), hdfsCluster, err.Error())
                } else {
                    job.SetHDFSWorkDir(mirrorPath)
                    SchedulerLogger.Debugf("Set job %s HDFS work directory to %s\n",
                        job.GetID(), mirrorPath)
                }
            }
        }
    }

    if job.HDFSWorkDir() != "" {
        err := PrepareJobDir(ctxt, job, job.HDFSWorkDir())
        if err != nil {
            SchedulerLogger.Errorf("Fail to prepare job %s HDFS work directory %s: %s\n",
                job.GetID(), job.HDFSWorkDir(), err.Error())
        }
        SchedulerLogger.Debugf("Successfully created job %s HDFS work dir %s \n",
            job.GetID(), job.HDFSWorkDir())
    }

    return nil
}

func (jobMgr *jobMgr)_SubmitJob(job Job) {
    jobMgr.submitLock.Lock()
    defer jobMgr.submitLock.Unlock()

    jobMgr.submitQueue[job.GetID()] = job
    jobMgr.submitCond.Signal()
}

func (jobMgr *jobMgr) _HandleJobEvent(a interface{}) {
    if jobinfo, ok := a.(*JobInfo);ok {
        job := jobinfo.job
        state := jobinfo.state
        event := jobinfo.event
        jobMgr.HandleJobScheduleEvent(job, state, event)
    }
}

func (jobMgr *jobMgr)HandleJobScheduleEvent(job Job, state int, event int) {
    dbService := GetDBService()
    switch event {
        case JOB_EVENT_COMPLETE:
            SchedulerLogger.Debugf("JobMgr handle job %s schedule complete event\n",
                job.GetID())
            err := dbService.DeleteJob(job.GetID())
            if err != nil {
                SchedulerLogger.Errorf("fail to delete job %s from DB\n", job.GetID())
            }
            jobMgr.jobTracker.RemoveJob(job)
            if JobStateIsRunning(state) {
                atomic.AddInt64(&jobMgr.runningJobNum, -1)
            }
            atomic.AddInt64(&jobMgr.totalJobNum, -1)
            atomic.AddInt64(&jobMgr.completeJobNum, 1)
        case JOB_EVENT_SCHEDULED:
            SchedulerLogger.Debugf("JobMgr handle job %s scheduled event\n",
                job.GetID())
            atomic.AddInt64(&jobMgr.runningJobNum, 1)
        case JOB_EVENT_ERROR:
            SchedulerLogger.Debugf("JobMgr Handle job %s schedule error event\n",
                job.GetID())
            err := dbService.DeleteJob(job.GetID())
            if err != nil {
                SchedulerLogger.Errorf("fail to delete job %s from DB\n", job.GetID())
            }
            jobMgr.jobTracker.RemoveJob(job)
            if JobStateIsRunning(state) {
                atomic.AddInt64(&jobMgr.runningJobNum, -1)
            }
            atomic.AddInt64(&jobMgr.totalJobNum, -1)
            atomic.AddInt64(&jobMgr.failJobNum, 1)
        case JOB_EVENT_FAIL:
            SchedulerLogger.Debugf("JobMgr Handle job %s schedule fail event\n",
                job.GetID())
            err := dbService.DeleteJob(job.GetID())
            if err != nil {
                SchedulerLogger.Errorf("fail to delete job %s from DB\n", job.GetID())
            }
            jobMgr.jobTracker.RemoveJob(job)
            if JobStateIsRunning(state) {
                atomic.AddInt64(&jobMgr.runningJobNum, -1)
            }
            atomic.AddInt64(&jobMgr.totalJobNum, -1)
            atomic.AddInt64(&jobMgr.failJobNum, 1)
        case JOB_EVENT_LOST:
            SchedulerLogger.Debugf("JobMgr Handle job %s schedule lost event\n",
                job.GetID())
        case JOB_EVENT_CANCELED:
            SchedulerLogger.Debugf("JobMgr Handle job %s schedule canceled event\n",
                job.GetID())
            err := dbService.DeleteJob(job.GetID())
            if err != nil {
                SchedulerLogger.Errorf("fail to delete job %s from DB\n", job.GetID())
            }
            jobMgr.jobTracker.RemoveJob(job)
            if JobStateIsRunning(state) {
                atomic.AddInt64(&jobMgr.runningJobNum, -1)
            } else if JobStateIsPaused(state) {
                atomic.AddInt64(&jobMgr.pausedJobNum, -1)
            }
            atomic.AddInt64(&jobMgr.totalJobNum, -1)
            atomic.AddInt64(&jobMgr.canceledJobNum, 1)
        case JOB_EVENT_PAUSED:
            if JobStateIsRunning(state) {
                atomic.AddInt64(&jobMgr.runningJobNum, -1)
            }
            atomic.AddInt64(&jobMgr.pausedJobNum, 1)
        case JOB_EVENT_RESUMED:
            if JobStateIsRunning(job.State()) {
                atomic.AddInt64(&jobMgr.runningJobNum, 1)
            }
            atomic.AddInt64(&jobMgr.pausedJobNum, -1)
        case JOB_EVENT_PSEUDONE:
            SchedulerLogger.Debugf("JobMgr handle job %s schedule pseudo finish event\n",
                job.GetID())
            err := dbService.DeleteJob(job.GetID())
            if err != nil {
                SchedulerLogger.Errorf("fail to delete job %s from DB\n", job.GetID())
            }
            jobMgr.jobTracker.RemoveJob(job)
            if JobStateIsRunning(state) {
                atomic.AddInt64(&jobMgr.runningJobNum, -1)
            }
            atomic.AddInt64(&jobMgr.totalJobNum, -1)
            atomic.AddInt64(&jobMgr.psudoCompleteNum, 1)
        default:
            SchedulerLogger.Debugf("Ignore the event %d for job %s\n", event,
                job.GetID())
    }

    /*Notify handler to send job information to frontend.*/
    err, jobStatus := job.GetStatus()
    if err != nil {
        SchedulerLogger.Errorf("fail to get job %s status\n", job.GetID())
        return
    }
    info := NewFrontEndNotcieInfo(jobStatus)
    if info != nil {
        e := NewNoticeEvent(FRONTENDNOTICEEVENT,info)
        eventbus.Publish(e)
    }
}

func (jobMgr *jobMgr)PauseJob(ctxt *SecurityContext,
    jobId string) error{

    if !jobMgr.IsReady() {
        SchedulerLogger.Error("Job mgr not ready, refuse job management\n")
        return errors.New("Job service not ready")
    }
    
    runJobs, err := jobMgr._FindJobsById(ctxt, jobId)
    if err != nil {
        return err
    }

    if runJobs == nil || len(runJobs) == 0 {
        return nil
    }

    db := GetDBService()
    /*to pause the jobs*/
    for i := 0; i < len(runJobs); i ++ {
        job := runJobs[i]
        oldState := job.State()
        err := job.Pause()
        if err != nil {
            SchedulerLogger.Errorf("The job %s pause error %s\n",
                    job.GetID(), err.Error())
            return err
        }
        SchedulerLogger.Infof("The job %s is paused, start persist to database\n",
            job.GetID())

        err = db.UpdateJobState(job.GetID(), JobStateToStr(job.State()),
            JobStateToStr(job.LastState()))
        if err != nil {
            SchedulerLogger.Errorf("Can't set job %s paused state DB %s, but ignore it\n",
                job.GetID(), err.Error())
        } else {
            SchedulerLogger.Infof("Job %s is paused and persist to database\n",
                job.GetID())
        }
        jobEvent := NewJobEvent(JOBEVENTID, job, oldState, JOB_EVENT_PAUSED)
        eventbus.Publish(jobEvent)
    }

    return nil
}

func (jobMgr *jobMgr)ResumeJob(ctxt *SecurityContext,
    jobId string) error{
    
    if !jobMgr.IsReady() {
        SchedulerLogger.Error("Job mgr not ready, refuse job management\n")
        return errors.New("Job service not ready")
    }

    runJobs, err := jobMgr._FindJobsById(ctxt, jobId)
    if err != nil {
        return err
    }

    if runJobs == nil || len(runJobs) == 0 {
        return nil
    }

    db := GetDBService()
    for i := 0; i < len(runJobs); i ++ {
        job := runJobs[i]
        oldState := job.State()
        err := job.Resume()
        if err != nil {
            SchedulerLogger.Errorf("The job %s resume error %s\n",
                job.GetID(), err.Error())
            return err
        } 
    
        err = GetScheduler().ResumeJob(job)
        if err != nil {
            SchedulerLogger.Errorf("The scheduler can't resume job %s: %s\n",
                job.GetID(), err.Error())
            return err
        }

        SchedulerLogger.Infof("The job %s is resumed, start persist DB\n",
            job.GetID())

        err = db.UpdateJobState(job.GetID(), JobStateToStr(job.State()),
            JobStateToStr(job.LastState()))
        if err != nil {
            SchedulerLogger.Errorf("Can't persist job %s state to DB: %s \n",
                job.GetID(), err.Error())
        } else {
            SchedulerLogger.Infof("Job %s is resumed and persist to DB\n",
                job.GetID())
        }
        jobEvent := NewJobEvent(JOBEVENTID, job, oldState, JOB_EVENT_RESUMED)
        eventbus.Publish(jobEvent)
    }

    return nil
}

func (jobMgr *jobMgr)KillJobTasks(ctxt *SecurityContext,
    jobId string, taskId string) error {
    if !jobMgr.IsReady() {
        SchedulerLogger.Error("Job mgr not ready, refuse job management\n")
        return errors.New("Job service not ready")
    }
    jobList, err := jobMgr._FindJobsById(ctxt, jobId)
    if err != nil {
        return err
    }
    scheduler := GetScheduler()
    for _, job := range jobList {
        err := scheduler.KillJobTasks(job, taskId)
        if err != nil {
            return err
        } else {
            SchedulerLogger.Infof("KillJobTasks: succeed to kill job %s tasks\n",
                job.GetID())
        }
    }

    return nil
}

func (jobMgr *jobMgr)_FindJobsById(ctxt *SecurityContext, jobId string) ([]Job, error) {
    runJobs := make([]Job, 0)
    if jobId == "*" {
        /*pause all jobs*/
        runJobs = jobMgr.jobTracker.GetAllJobsByPrivilege(ctxt)
    } else {
        /*set job state to paused*/
        err, job := jobMgr.jobTracker.FindJobByID(jobId)

        if err != nil {
		    errmsg := fmt.Sprintf("The job %s is not found\n",
                jobId)
            SchedulerLogger.Errorf(errmsg)
            return runJobs, errors.New(errmsg)
        }

        /*Security check*/
        if !ctxt.CheckSecID(job.SecID()) {
            SchedulerLogger.Errorf("Security check fail: id %s and %s doesn't match for job %s\n",
                ctxt.ID(), job.SecID(), jobId)
            return runJobs, errors.New("Security ID doesn't match")
        }

        runJobs = append(runJobs, job)
    }

    return runJobs, nil
}

func (jobMgr *jobMgr)TraverseJobQueues(fn func(Job)bool) {
    jobMgr.submitLock.RLock()
    defer jobMgr.submitLock.RUnlock()

    /*
     * Because in production enviornment, there may be hundreds of thousands
     * of jobs. So here grab read lock and only return first found job
     */
    for _, job := range jobMgr.submitQueue {
        if fn(job) {
            return
        }
    }

    jobMgr.jobTracker.TraverseJobList(fn)
}

func (jobMgr *jobMgr) SearchJobIdInMem(id string) []string {
	jobIds := make([]string, 0)
	jobMgr.TraverseJobQueues(func(job Job) bool {
		x := job.GetID()
		if fuzzy.MatchFold(id, x) == true {
			jobIds = append(jobIds, x)
		}
		return false
	})
	return jobIds
}

func (jobMgr *jobMgr)SearchJobIdInDB(id string) []string {
	db := GetDBService()
	err, jobIds := db.GetJobIdsFromHistory(id)
	if err != nil {
		SchedulerLogger.Infof("SearchJobIdInDB failed:%s", err.Error())
		return nil
	} else {
		return jobIds
	}
}

func (jobMgr *jobMgr)GetJobIdsByPipeline(ctxt *SecurityContext,
    pipelineName string) []string {
    jobIds := make([]string, 0)
    jobMgr.TraverseJobQueues(func(job Job) bool {
                if job.PipelineName() == pipelineName {
                    jobIds = append(jobIds, job.GetID())
                    return true
                }
                return false
            })

    return jobIds
}

func (jobMgr *jobMgr)GetJobIdsByWorkDir(workDir string) []string {
    jobIds := make([]string, 0)
    jobMgr.TraverseJobQueues(func(job Job) bool {
                if job.WorkDir() == workDir {
                    jobIds = append(jobIds, job.GetID())
                    return true
                }
                return false
            })

    return jobIds
}

/*REST API handlers query job list*/
func (jobMgr *jobMgr)ListJobs(ctxt *SecurityContext,
    listOpt *JobListOpt) (error, []BioflowJobInfo,
    []BioflowJobInfo) {

    if !jobMgr.IsReady() {
        SchedulerLogger.Error("Job mgr not ready, refuse job management\n")
        return errors.New("Job service not ready"), nil, nil
    }

	sum := 0

	var start time.Time
	var end time.Time
	var err error

	if listOpt.After != "" {
		start, err = TimeUtilsUserInputToBioflowTime(listOpt.After)
        if err != nil {
            SchedulerLogger.Errorf("Parse user input time %s error: %s\n",
                listOpt.After, err.Error())
            return err, nil, nil
        }
	}

	if listOpt.Before != "" {
		end, err = TimeUtilsUserInputToBioflowTime(listOpt.Before)
        if err != nil {
            SchedulerLogger.Errorf("Parse user input time %s error: %s\n",
                listOpt.Before, err.Error())
            return err, nil, nil
        }
	}


    jobList := make([]BioflowJobInfo, 0)

    if listOpt.ListType != "old" {

        jobMgr.submitLock.RLock()

        for _, job := range jobMgr.submitQueue {
		    if listOpt.Count != 0 && sum == listOpt.Count {
			    break
		    }

		    if listOpt.Name != "" && job.Name() != listOpt.Name {
			    continue
		    }

		    if listOpt.Pipeline != "" && job.PipelineName() != listOpt.Pipeline {
			    continue
		    }

            /*if priority is -1, don't use it*/
            if listOpt.Priority != -1 && job.Priority() != listOpt.Priority {
                continue
            }

            if listOpt.State != "" && JobStateToStr(job.State()) != strings.ToUpper(listOpt.State) {
                continue
            }

            /*security check*/
            if !ctxt.CheckSecID(job.SecID()) {
                continue
            }

		    var t time.Time

		    if listOpt.Finished == "" {
			    t = job.CreateTime()

			    if err == nil && listOpt.After != "" &&  !t.After(start) {
				    continue
			    }

			    if err == nil && listOpt.Before != "" && !t.Before(end) {
				    continue
			    }
		    }

            jobInfo := BioflowJobInfo {
                JobId: job.GetID(),
                Name: job.Name(),
                Pipeline: job.PipelineName(),
                Created: job.CreateTime().Format(BIOFLOW_TIME_LAYOUT),
                Finished: job.FinishTime().Format(BIOFLOW_TIME_LAYOUT),
                State: JobStateToStr(job.State()),
                Owner: job.SecID(),
                ExecMode: job.ExecMode(),
                Priority: job.Priority(),
            }
            jobList = append(jobList, jobInfo)
		    sum += 1
        }

        err, runJobs := jobMgr.jobTracker.SearchJobList(ctxt,
            listOpt, &sum)

        jobMgr.submitLock.RUnlock()

        if err == nil {
            jobList = append(jobList, runJobs ...)
        } else {
            SchedulerLogger.Errorf("Search job list in tracker fail: %s\n",
                err.Error())
            return err, nil, nil
        }
    }

	if listOpt.Count != 0 {
		if sum == listOpt.Count {
			return nil, jobList, nil
		} else {
			listOpt.Count -= sum
		}
	}

	if listOpt.ListType == "mem" {
		return nil, jobList, nil
	}

	db := GetDBService()
	err, histJobs := db.GetHistoryJobList(ctxt, listOpt)
	if err != nil {
		SchedulerLogger.Errorf("Get history jobs from DB fail: %s\n",
			err.Error())
	} else {
		return nil, jobList, histJobs
	}

    return nil, jobList, nil
}


/*REST API handlers query job status*/
func (jobMgr *jobMgr)GetJobStatus(ctxt *SecurityContext,
    jobId string) (error, *BioflowJobStatus) {

    if !jobMgr.IsReady() {
        SchedulerLogger.Error("Job mgr not ready, refuse job management\n")
        return errors.New("Job service not ready"), nil
    }

    var job Job = nil

    jobMgr.submitLock.RLock()
    job, ok := jobMgr.submitQueue[jobId]
    if !ok {
        _, job = jobMgr.jobTracker.FindJobByID(jobId)
    }
    jobMgr.submitLock.RUnlock()

    scheduler := GetScheduler()
    if job == nil {
        SchedulerLogger.Debugf("The job %s not found running, get history\n", 
            jobId)
        err, jobStatus := scheduler.GetHistoryJobStatus(jobId)
        if err != nil {
            SchedulerLogger.Errorf("Get History Job %s error: %s\n",
                jobId, err.Error())
            return err, nil
        }

        return nil, jobStatus
    } 

    /*Security check*/
    if !ctxt.CheckSecID(job.SecID()) {
        SchedulerLogger.Errorf("Security check fail: %s and %s not match for job %s\n",
            ctxt.ID(), job.SecID(), jobId)
        return errors.New("Security ID doesn't match"), nil
    }

    err, jobStatus := job.GetStatus()
    if err != nil {
        SchedulerLogger.Errorf("Fail to get job %s status: %s\n",
            jobId, err.Error())
        return err, nil
    }

    /*Get runtime stage information from scheduler*/
    err = scheduler.GetJobStatus(job, jobStatus)
    if err != nil {
        SchedulerLogger.Errorf("Fail to get job %s schedule status: %s\n",
            jobId, err.Error())
    }

    return nil, jobStatus
}

func (jobMgr *jobMgr) CancelJob(ctxt *SecurityContext,
    jobId string) error {
    submitJobs := make([]Job, 0)
    runJobs := make([]Job, 0)

    if !jobMgr.IsReady() {
        SchedulerLogger.Error("Job mgr not ready, refuse job management\n")
        return errors.New("Job service not ready")
    }

    jobMgr.submitLock.Lock()
    if jobId == "*" {
        /*cancel all jobs*/
        for _, job := range jobMgr.submitQueue {
            /*Security check*/
            if !ctxt.CheckSecID(job.SecID()) {
                continue
            }

            delete(jobMgr.submitQueue, jobId)
            submitJobs = append(submitJobs, job)
        }
        runJobs = jobMgr.jobTracker.GetAllJobsByPrivilege(ctxt)
    } else {
        job, ok := jobMgr.submitQueue[jobId]
        if !ok {
            _, job = jobMgr.jobTracker.FindJobByID(jobId)
            if job != nil && ctxt.CheckSecID(job.SecID()) {
                runJobs = append(runJobs, job)
            }
        } else if ctxt.CheckSecID(job.SecID()){
            /*job still in submit queue, remove it directly*/
            delete(jobMgr.submitQueue, jobId)
            submitJobs = append(submitJobs, job)
        }
    }
    jobMgr.submitLock.Unlock()

    if len(submitJobs) == 0 && len(runJobs) == 0 {
        SchedulerLogger.Errorf("The job %s not found running\n", jobId)
        errMsg := fmt.Sprintf("Job %s not found running", jobId)
        return errors.New(errMsg)
    } 

    for i := 0; i < len(submitJobs); i++ {
        job := submitJobs[i]
        oldJobState := job.State()
        job.Cancel()
        jobEvent := NewJobEvent(JOBEVENTID, job, oldJobState, JOB_EVENT_CANCELED)
        eventbus.Publish(jobEvent)
        SchedulerLogger.Infof("Job %s canceled directly by Job Mgr\n",
            job.GetID())
    }

    for i := 0; i < len(runJobs); i ++ {
        job := runJobs[i]
        job.Cancel()
        err := GetScheduler().CancelJob(job)
        if err != nil {
            SchedulerLogger.Errorf("Job %s can't be canceled by scheduler: %s\n",
                job.GetID(), err.Error())
            return err
        }

        SchedulerLogger.Infof("Job %s is canceled successfully\n",
            job.GetID())
    }

    return nil
}

func(jobMgr *jobMgr) GetJobLogs(ctxt *SecurityContext,
    jobId string, stageName string, stageId string) (error, 
	map[string]BioflowStageLog) {
	
    SchedulerLogger.Debugf("Get Job Logs for Job %s, Stage:%s\n",
        jobId, stageName)
    
    jobMgr.submitLock.RLock()
    job, ok := jobMgr.submitQueue[jobId]
    if !ok {
        _, job = jobMgr.jobTracker.FindJobByID(jobId)
    }
    jobMgr.submitLock.RUnlock()
    scheduler := GetScheduler()
	return scheduler.GetJobLogs(job, jobId, stageName, stageId)
}

/*Fail a job after submitted and before scheduled*/
func(jobMgr *jobMgr) FailNonScheduledJob(job Job, reason string) error {
    oldState := job.State()
    /*update job state*/
    job.Complete(false, reason)
    
    scheduler := GetScheduler()
    err := scheduler.PersistJobHistory(job, false)
    if err != nil {
        SchedulerLogger.Errorf("Fail to persist job %s history:%s\n",
            job.GetID(), err.Error())
    }

    err = scheduler.DestroyJobScheduleInfo(job)
    if err != nil {
        SchedulerLogger.Errorf("Fail to destroy job %s schedule info\n",
            job.GetID())
    }
    
    GetUserMgr().ReleaseJobQuota(job.GetSecurityContext(), 1)

    jobEvent := NewJobEvent(JOBEVENTID, job, oldState, JOB_EVENT_FAIL)
    eventbus.Publish(jobEvent)

    GetDashBoard().UpdateStats(STAT_JOB_FAIL, 1)

    return nil
}

func (jobMgr *jobMgr) CleanupJobHistory(ctxt *SecurityContext,
    listOpt *JobListOpt) (error, int) {
    if !jobMgr.IsReady() {
        SchedulerLogger.Error("Job mgr not ready, refuse job management\n")
        return errors.New("Job service not ready"), -1
    }

	db := GetDBService()
	err, histJobs := db.GetHistoryJobList(ctxt, listOpt)
	if err != nil {
		SchedulerLogger.Errorf("Get history jobs from DB fail: %s\n",
			err.Error())
        return err, 0
	}

    cleanupCount := 0
    for i := 0; i < len(histJobs); i ++ {
        jobId := histJobs[i].JobId
        err = db.DeleteJobHistory(jobId)
        if err != nil {
            SchedulerLogger.Errorf("User %s fail delete job %s: %s\n",
                ctxt.ID(), jobId, err.Error())
        } else {
            cleanupCount ++
            SchedulerLogger.Infof("User %s delete job id %s, pipeline %s history from DB\n",
                ctxt.ID(), jobId, histJobs[i].Pipeline)
        }
    }

    return nil, cleanupCount
}

func (jobMgr *jobMgr) GetState() string {
    state := "Unknown"
    switch jobMgr.state {
        case JOBMGR_STATE_RECOVERING:
            return "Recovering"
        case JOBMGR_STATE_READY:
            return "Ready"
    }

    return state
}

func (jobMgr *jobMgr) GetStats() *BioflowJobMgrStats {

    return &BioflowJobMgrStats {
            State: jobMgr.GetState(),
            RecoveryTime: jobMgr.recoveryTime,
            MaxJobSubmitTime: jobMgr.maxJobSubmitTime,
            TotalJobNum: jobMgr.totalJobNum,
            CompleteJobNum: jobMgr.completeJobNum,
            FailJobNum: jobMgr.failJobNum,
            CanceledJobNum: jobMgr.canceledJobNum,
            PausedJobNum: jobMgr.pausedJobNum,
            RunningJobNum: jobMgr.runningJobNum,
            PsudoCompleteJobNum: jobMgr.psudoCompleteNum,
    }
}

func (jobMgr *jobMgr)UpdateJob(ctxt *SecurityContext,
    jobId string, jsonData *JobJSONData) error{
    
    if !jobMgr.IsReady() {
        SchedulerLogger.Error("Job mgr not ready, refuse job management\n")
        return errors.New("Job service not ready")
    }

    err, job := jobMgr.jobTracker.FindJobByID(jobId)
    if err != nil {
        SchedulerLogger.Errorf("The job %s is not found\n",
            jobId)
        return errors.New("The job with id %s not found")
    }

    /*Security check*/
    if !ctxt.CheckSecID(job.SecID()) {
        SchedulerLogger.Errorf("Security check fail: id %s and %s doesn't match for job %s\n",
            ctxt.ID(), job.SecID(), jobId)
        return errors.New("Security ID doesn't match")
    }
    
    /*
     * Update job property, currently only support update the priority
     */
    if jsonData.Priority == job.Priority() {
        SchedulerLogger.Debugf("Job %s priority not changed, ignore the update\n",
            job.GetID())
        return errors.New("No property changed")
    }

    if jsonData.Priority < JOB_MIN_PRI || jsonData.Priority > JOB_MAX_PRI {
        SchedulerLogger.Errorf("Job %s set invalid priority %d\n",
            job.GetID(), jsonData.Priority)
        return errors.New(fmt.Sprintf("Priority should in [%d, %d]",
            JOB_MIN_PRI, JOB_MAX_PRI))
    }

    oldPri := job.Priority()
    job.SetPriority(jsonData.Priority)

    err = GetScheduler().UpdateJobPriority(job, oldPri)
    if err != nil {
        SchedulerLogger.Errorf("The scheduler can't update job %s priority %d(%d): %s\n",
            job.GetID(), jsonData.Priority, oldPri, err.Error())
        return err
    }

	db := GetDBService()
	err = db.UpdateJobPriority(job.GetID(), job.Priority())
	if err != nil {
		SchedulerLogger.Errorf("Fail update job %s priority to DB: %s\n",
			job.GetID(), err.Error())
        return err
	}

    SchedulerLogger.Infof("Successfully set job %s priority from %d to %d\n",
        job.GetID(), oldPri, job.Priority())

    return nil
}

func (jobMgr *jobMgr)ListHangJobs(ctxt *SecurityContext) (error, []BioflowJobInfo) {
    if !jobMgr.IsReady() {
        SchedulerLogger.Error("Job mgr not ready, refuse job management\n")
        return errors.New("Job service not ready"), nil
    }

    return nil, jobMgr.jobTracker.GetHangJobs(ctxt)
}

func(jobMgr *jobMgr) GetJobTaskResourceUsageInfo(ctxt *SecurityContext,
    jobId string, taskId string) (error, 
	map[string]ResourceUsageInfo) {
	
    SchedulerLogger.Debugf("Do profiling for resource usage on Job %s and Task %s\n",
        jobId, taskId)
    
    jobMgr.submitLock.RLock()
    job, ok := jobMgr.submitQueue[jobId]
    if !ok {
        _, job = jobMgr.jobTracker.FindJobByID(jobId)
    }
    jobMgr.submitLock.RUnlock()
    scheduler := GetScheduler()
	return scheduler.GetJobTaskResourceUsageInfo(job, jobId, taskId)
}

func(jobMgr *jobMgr)InitJobPipeline(job Job) error {
    ns := NSMapFromSecID(job.SecID())
    pipeline := GetPipelineMgr().GetPipeline(ns,
        job.PipelineName())
    if pipeline == nil {
        SchedulerLogger.Errorf("Job's pipeline %s not exist\n",
            job.PipelineName())
        return errors.New("Job Pipeline not exist")
    }

    job.SetPipeline(pipeline)

    return nil
}
