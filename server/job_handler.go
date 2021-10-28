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
package server

import (
    "net/http"

    "encoding/json"
    "github.com/gorilla/mux"
    "github.com/xtao/bioflow/scheduler"
    . "github.com/xtao/bioflow/common"
    . "github.com/xtao/bioflow/message"
    )

func ListJob(w http.ResponseWriter, req *http.Request) {
    decoder := json.NewDecoder(req.Body)
    defer req.Body.Close()

    var result BioflowListJobResult

    var listOpt JobListOpt
    err := decoder.Decode(&listOpt)
    if err != nil {
        Logger.Println("Can't parse the request " + err.Error())
        result.Status = BIOFLOW_API_RET_FAIL
        result.Msg = err.Error()
        writeJSON(500, result, w)
        return
    }

    jobMgr := scheduler.GetJobMgr()

    err, secCtxt := BuildSecurityContext(req)
    if err != nil {
        result.Status = BIOFLOW_API_RET_FAIL
        result.Msg = err.Error()
        writeJSON(500, result, w)
        return
    }

    err = CheckClientAccount(secCtxt)
    if err != nil {
        result.Status = BIOFLOW_API_RET_FAIL
        result.Msg = err.Error()
        writeJSON(500, result, w)
        return
    }

    err, jobInfo, histJobs := jobMgr.ListJobs(secCtxt, &listOpt)
    if err != nil {
        ServerLogger.Infof("Can't list jobs: %s\n",
            err.Error())
        result.Status = BIOFLOW_API_RET_FAIL
        result.Msg = err.Error()
        writeJSON(500, result, w)
    } else {
        ServerLogger.Debugf("Successfully list jobs \n")
        result.Status = BIOFLOW_API_RET_OK
        result.Msg = "successfully list jobs"
        result.Jobs = jobInfo
        result.HistJobs = histJobs
        result.Count = len(jobInfo)
        writeJSON(http.StatusAccepted, result, w)
    }
}

func GetJobStatus(w http.ResponseWriter, req *http.Request) {
    defer req.Body.Close()
    vars := mux.Vars(req)
    id := vars["jobId"]

    var result BioflowGetJobStatusResult
    jobMgr := scheduler.GetJobMgr()

    jobId, err := jobMgr.GetJobId(id, GET_JOBID_FROM_ALL)
    if err != nil {
        result.Status = BIOFLOW_API_RET_FAIL
        result.Msg = err.Error()
        writeJSON(500, result, w)
        return
    }

    err, secCtxt := BuildSecurityContext(req)
    if err != nil {
        result.Status = BIOFLOW_API_RET_FAIL
        result.Msg = err.Error()
        writeJSON(500, result, w)
        return
    }

    err = CheckClientAccount(secCtxt)
    if err != nil {
        result.Status = BIOFLOW_API_RET_FAIL
        result.Msg = err.Error()
        writeJSON(500, result, w)
        return
    }

    err, jobStatus := jobMgr.GetJobStatus(secCtxt, jobId)
    if err != nil {
        ServerLogger.Infof("Can't get job %s status: %s\n",
            jobId, err.Error())
        result.Status = BIOFLOW_API_RET_FAIL
        result.Msg = err.Error()
        writeJSON(500, result, w)
    } else {
        ServerLogger.Debugf("Successfully get job %s status \n", jobId)
        result.Status = BIOFLOW_API_RET_OK
        result.Msg = "successfully list jobs"
        result.JobStatus = *jobStatus
        writeJSON(http.StatusAccepted, result, w)
    }
}


func SubmitJob(w http.ResponseWriter, req *http.Request) {
    defer req.Body.Close()
    var result BioflowSubmitJobResult

    err, secCtxt := BuildSecurityContext(req)
    if err != nil {
        result.Status = BIOFLOW_API_RET_FAIL
        result.Msg = err.Error()
        writeJSON(500, result, w)
        return
    }

    err = CheckClientAccount(secCtxt)
    if err != nil {
        result.Status = BIOFLOW_API_RET_FAIL
        result.Msg = err.Error()
        writeJSON(500, result, w)
        return
    }

    err, job := scheduler.NewJobFromJSONStream(req.Body)
    if err != nil {
        ServerLogger.Error("Received invalid format for job, reject it")
        result.Status = BIOFLOW_API_RET_FAIL
        result.Msg = "Server reject wrong format job, msg: " + err.Error()
        writeJSON(500, result, w)
    } else {
        jobMgr := scheduler.GetJobMgr()
        err, jobId := jobMgr.SubmitJob(secCtxt, job)
        if err == nil {
            ServerLogger.Infof("Job added: %s", jobId.String())
            result.Status = BIOFLOW_API_RET_OK
            result.Msg = "The job is added success"
            result.JobId = jobId.String()
            writeJSON(http.StatusAccepted, result, w)
        } else {
            ServerLogger.Errorf("Job added fail: %s \n",
                err.Error())
            result.Status = BIOFLOW_API_RET_FAIL
            result.Msg = err.Error()
            writeJSON(500, result, w)
        }
    }
}

func PauseJob(w http.ResponseWriter, req *http.Request) {
    defer req.Body.Close()
    var result BioflowAPIResult
    vars := mux.Vars(req)
    id := vars["jobId"]

    err, secCtxt := BuildSecurityContext(req)
    if err != nil {
        result.Status = BIOFLOW_API_RET_FAIL
        result.Msg = err.Error()
        writeJSON(500, result, w)
        return
    }

    err = CheckClientAccount(secCtxt)
    if err != nil {
        result.Status = BIOFLOW_API_RET_FAIL
        result.Msg = err.Error()
        writeJSON(500, result, w)
        return
    }

    jobMgr := scheduler.GetJobMgr()
    jobId := id
    if id != "*" {
        jobId, err = jobMgr.GetJobId(id, GET_JOBID_FROM_MEM)
        if err != nil {
            result.Status = BIOFLOW_API_RET_FAIL
            result.Msg = err.Error()
            writeJSON(500, result, w)
            return
        }
    }

    err = jobMgr.PauseJob(secCtxt, jobId)
    if err != nil {
        ServerLogger.Infof("Can't pause job %s: %s\n",
            jobId, err.Error())
        result.Status = BIOFLOW_API_RET_FAIL
        result.Msg = err.Error()
        writeJSON(500, result, w)
    } else {
        ServerLogger.Infof("Successfully pause the job %s \n", jobId)
        result.Status = BIOFLOW_API_RET_OK
        result.Msg = "successfully pause the job"
        writeJSON(http.StatusAccepted, result, w)
    }
}

func KillJobTasks(w http.ResponseWriter, req *http.Request) {
    defer req.Body.Close()
    var result BioflowAPIResult
    vars := mux.Vars(req)
    id := vars["jobId"]
    taskId := vars["taskId"]

    err, secCtxt := BuildSecurityContext(req)
    if err != nil {
        result.Status = BIOFLOW_API_RET_FAIL
        result.Msg = err.Error()
        writeJSON(500, result, w)
        return
    }

    err = CheckClientAccount(secCtxt)
    if err != nil {
        result.Status = BIOFLOW_API_RET_FAIL
        result.Msg = err.Error()
        writeJSON(500, result, w)
        return
    }

    jobMgr := scheduler.GetJobMgr()
    jobId := id
    if id != "*" {
        jobId, err = jobMgr.GetJobId(id, GET_JOBID_FROM_MEM)
        if err != nil {
            result.Status = BIOFLOW_API_RET_FAIL
            result.Msg = err.Error()
            writeJSON(500, result, w)
            return
        }
    }

    err = jobMgr.KillJobTasks(secCtxt, jobId, taskId)
    if err != nil {
        ServerLogger.Infof("Can't kill job %s tasks: %s\n",
            jobId, err.Error())
        result.Status = BIOFLOW_API_RET_FAIL
        result.Msg = err.Error()
        writeJSON(500, result, w)
    } else {
        ServerLogger.Infof("Successfully kill the job %s tasks %s\n",
            jobId, taskId)
        result.Status = BIOFLOW_API_RET_OK
        result.Msg = "successfully kill the job tasks"
        writeJSON(http.StatusAccepted, result, w)
    }
}

func RequeueJobStage(w http.ResponseWriter, req *http.Request) {
    decoder := json.NewDecoder(req.Body)
    defer req.Body.Close()

    vars := mux.Vars(req)
    id := vars["jobId"]
    stageId := vars["stageId"]

    var result BioflowAPIResult

    var resourceSpec ResourceSpecJSONData
    err := decoder.Decode(&resourceSpec)
    if err != nil {
        Logger.Println("Can't parse the request " + err.Error())
        result.Status = BIOFLOW_API_RET_FAIL
        result.Msg = err.Error()
        writeJSON(500, result, w)
        return
    }

    err, secCtxt := BuildSecurityContext(req)
    if err != nil {
        result.Status = BIOFLOW_API_RET_FAIL
        result.Msg = err.Error()
        writeJSON(500, result, w)
        return
    }

    err = CheckClientAccount(secCtxt)
    if err != nil {
        result.Status = BIOFLOW_API_RET_FAIL
        result.Msg = err.Error()
        writeJSON(500, result, w)
        return
    }

    jobMgr := scheduler.GetJobMgr()
    jobId, err := jobMgr.GetJobId(id, GET_JOBID_FROM_MEM)
    if err != nil {
        result.Status = BIOFLOW_API_RET_FAIL
        result.Msg = err.Error()
        writeJSON(500, result, w)
        return
    }

    err = jobMgr.RequeueJobStage(secCtxt, jobId, stageId,
        &resourceSpec)
    if err != nil {
        ServerLogger.Infof("Can't requeue job %s stage %s: %s\n",
            jobId, stageId, err.Error())
        result.Status = BIOFLOW_API_RET_FAIL
        result.Msg = err.Error()
        writeJSON(500, result, w)
    } else {
        ServerLogger.Infof("Successfully requeue the job %s stage %s \n",
            jobId, stageId)
        result.Status = BIOFLOW_API_RET_OK
        result.Msg = "successfully resume the job"
        writeJSON(http.StatusAccepted, result, w)
    }
}

func RecoverFailedJob(w http.ResponseWriter, req *http.Request) {
    defer req.Body.Close()
    vars := mux.Vars(req)
    id := vars["jobId"]

    var result BioflowAPIResult
    err, secCtxt := BuildSecurityContext(req)
    if err != nil {
        result.Status = BIOFLOW_API_RET_FAIL
        result.Msg = err.Error()
        writeJSON(500, result, w)
        return
    }

    err = CheckClientAccount(secCtxt)
    if err != nil {
        result.Status = BIOFLOW_API_RET_FAIL
        result.Msg = err.Error()
        writeJSON(500, result, w)
        return
    }

    jobMgr := scheduler.GetJobMgr()
    jobId, err := jobMgr.GetJobId(id, GET_JOBID_FROM_DB)
    if err != nil {
        result.Status = BIOFLOW_API_RET_FAIL
        result.Msg = err.Error()
        writeJSON(500, result, w)
        return
    }

    err = jobMgr.RecoverFailedJob(secCtxt, jobId)
    if err != nil {
        ServerLogger.Infof("Can't recover filed job %s: %s\n",
            jobId, err.Error())
        result.Status = BIOFLOW_API_RET_FAIL
        result.Msg = err.Error()
        writeJSON(500, result, w)
    } else {
        ServerLogger.Infof("Successfully recover the job %s \n", jobId)
        result.Status = BIOFLOW_API_RET_OK
        result.Msg = "successfully recover the job"
        writeJSON(http.StatusAccepted, result, w)
    }
}

func ResumeJob(w http.ResponseWriter, req *http.Request) {
    defer req.Body.Close()
    vars := mux.Vars(req)
    id := vars["jobId"]

    var result BioflowAPIResult
    err, secCtxt := BuildSecurityContext(req)
    if err != nil {
        result.Status = BIOFLOW_API_RET_FAIL
        result.Msg = err.Error()
        writeJSON(500, result, w)
        return
    }

    err = CheckClientAccount(secCtxt)
    if err != nil {
        result.Status = BIOFLOW_API_RET_FAIL
        result.Msg = err.Error()
        writeJSON(500, result, w)
        return
    }

    jobMgr := scheduler.GetJobMgr()
    jobId := id
    if id != "*" {
        jobId, err = jobMgr.GetJobId(id, GET_JOBID_FROM_MEM)
        if err != nil {
            result.Status = BIOFLOW_API_RET_FAIL
            result.Msg = err.Error()
            writeJSON(500, result, w)
            return
        }
    }

    err = jobMgr.ResumeJob(secCtxt, jobId)
    if err != nil {
        ServerLogger.Infof("Can't resume job %s: %s\n",
            jobId, err.Error())
        result.Status = BIOFLOW_API_RET_FAIL
        result.Msg = err.Error()
        writeJSON(500, result, w)
    } else {
        ServerLogger.Infof("Successfully resume the job %s \n", jobId)
        result.Status = BIOFLOW_API_RET_OK
        result.Msg = "successfully resume the job"
        writeJSON(http.StatusAccepted, result, w)
    }
}

func CancelJob(w http.ResponseWriter, req *http.Request) {
    defer req.Body.Close()
    vars := mux.Vars(req)
    id := vars["jobId"]

    var result BioflowAPIResult
    var err error = nil
    err, secCtxt := BuildSecurityContext(req)
    if err != nil {
        result.Status = BIOFLOW_API_RET_FAIL
        result.Msg = err.Error()
        writeJSON(500, result, w)
        return
    }

    err = CheckClientAccount(secCtxt)
    if err != nil {
        result.Status = BIOFLOW_API_RET_FAIL
        result.Msg = err.Error()
        writeJSON(500, result, w)
        return
    }

    jobMgr := scheduler.GetJobMgr()

    jobId := ""
    /*cancel * means cancel all the jobs*/
    if id == "*" {
        jobId = "*"
    } else {
        jobId, err = jobMgr.GetJobId(id, GET_JOBID_FROM_MEM)
        if err != nil {
            result.Status = BIOFLOW_API_RET_FAIL
            result.Msg = err.Error()
            writeJSON(500, result, w)
            return
        }
    }

    err = jobMgr.CancelJob(secCtxt, jobId)
    if err != nil {
        ServerLogger.Infof("Can't cancel job %s: %s\n",
            jobId, err.Error())
        result.Status = BIOFLOW_API_RET_FAIL
        result.Msg = err.Error()
        writeJSON(500, result, w)
    } else {
        ServerLogger.Infof("Successfully cancel the job %s \n", jobId)
        result.Status = BIOFLOW_API_RET_OK
        result.Msg = "successfully cancel the job"
        writeJSON(http.StatusAccepted, result, w)
    }
}

func GetJobLog(w http.ResponseWriter, req *http.Request) {
    defer req.Body.Close()
    vars := mux.Vars(req)
    id := vars["jobId"]
    stageName := vars["stageName"]
    stageId := vars["stageId"]

    ServerLogger.Debugf("stageid:%s stagename:%s id:%s \n", stageId, stageName, id)
    var result BioflowGetLogResult
    err, secCtxt := BuildSecurityContext(req)
    if err != nil {
        result.Status = BIOFLOW_API_RET_FAIL
        result.Msg = err.Error()
        writeJSON(500, result, w)
        return
    }

    err = CheckClientAccount(secCtxt)
    if err != nil {
        result.Status = BIOFLOW_API_RET_FAIL
        result.Msg = err.Error()
        writeJSON(500, result, w)
        return
    }
    
    jobMgr := scheduler.GetJobMgr()
    jobId, err := jobMgr.GetJobId(id, GET_JOBID_FROM_ALL)
    if err != nil {
        result.Status = BIOFLOW_API_RET_FAIL
        result.Msg = err.Error()
        writeJSON(500, result, w)
        return
    }

    ServerLogger.Debugf("Get log for :%s stage:%s stageId:%s", jobId, stageName, stageId)

    err, errLogs := jobMgr.GetJobLogs(secCtxt, jobId, stageName, stageId)
    if err != nil {
        ServerLogger.Infof("Can't get job logs: %s\n",
            err.Error())
        result.Status = BIOFLOW_API_RET_FAIL
        result.Msg = err.Error()
        writeJSON(500, result, w)
    } else {
        result.Status = BIOFLOW_API_RET_OK
        result.Msg = "successfully get job log"
        result.JobLogs = errLogs
        writeJSON(http.StatusAccepted, result, w)
    }
}

func CleanupJobs(w http.ResponseWriter, req *http.Request) {
    decoder := json.NewDecoder(req.Body)
    defer req.Body.Close()

    var listOpt JobListOpt
    err := decoder.Decode(&listOpt)
    if err != nil {
        Logger.Println("Can't parse the request " + err.Error())
        return
    }

    var result BioflowCleanupJobResult
    jobMgr := scheduler.GetJobMgr()

    /*use global user by default*/
    err, secCtxt := BuildSecurityContext(req)
    if err != nil {
        result.Status = BIOFLOW_API_RET_FAIL
        result.Msg = err.Error()
        writeJSON(500, result, w)
        return
    }

    err = CheckClientAccount(secCtxt)
    if err != nil {
        result.Status = BIOFLOW_API_RET_FAIL
        result.Msg = err.Error()
        writeJSON(500, result, w)
        return
    }

    err, count := jobMgr.CleanupJobHistory(secCtxt, &listOpt)
    if err != nil {
        ServerLogger.Errorf("Can't cleanup job history: %s\n",
            err.Error())
        result.Status = BIOFLOW_API_RET_FAIL
        result.Msg = err.Error()
        result.CleanupCount = 0
        writeJSON(500, result, w)
    } else {
        ServerLogger.Infof("Successfully cleanup jobs \n")
        result.Status = BIOFLOW_API_RET_OK
        result.Msg = "successfully cleanup jobs"
        result.CleanupCount = count
        writeJSON(http.StatusAccepted, result, w)
    }
}

func UpdateJob(w http.ResponseWriter, req *http.Request) {
    defer req.Body.Close()
    vars := mux.Vars(req)
    id := vars["jobId"]

    var result BioflowAPIResult
    err, secCtxt := BuildSecurityContext(req)
    if err != nil {
        result.Status = BIOFLOW_API_RET_FAIL
        result.Msg = err.Error()
        writeJSON(500, result, w)
        return
    }

    err = CheckClientAccount(secCtxt)
    if err != nil {
        result.Status = BIOFLOW_API_RET_FAIL
        result.Msg = err.Error()
        writeJSON(500, result, w)
        return
    }

    jobMgr := scheduler.GetJobMgr()
    jobId, err := jobMgr.GetJobId(id, GET_JOBID_FROM_MEM)
    if err != nil {
        result.Status = BIOFLOW_API_RET_FAIL
        result.Msg = err.Error()
        writeJSON(500, result, w)
        return
    }

    decoder := json.NewDecoder(req.Body)
    var jobJson JobJSONData
    err = decoder.Decode(&jobJson)
    if err != nil {
        SchedulerLogger.Errorf("Can't parse job %s update request: %s\n",
            jobId, err.Error())
        result.Status = BIOFLOW_API_RET_FAIL
        result.Msg = err.Error()
        writeJSON(500, result, w)
        return
    }

    err = jobMgr.UpdateJob(secCtxt, jobId, &jobJson)
    if err != nil {
        ServerLogger.Infof("Can't update job %s: %s\n",
            jobId, err.Error())
        result.Status = BIOFLOW_API_RET_FAIL
        result.Msg = err.Error()
        writeJSON(500, result, w)
    } else {
        ServerLogger.Infof("Successfully update the job %s \n", jobId)
        result.Status = BIOFLOW_API_RET_OK
        result.Msg = "successfully update the job"
        writeJSON(http.StatusAccepted, result, w)
    }
}

func ListHangJobs(w http.ResponseWriter, req *http.Request) {
    defer req.Body.Close()

    var result BioflowListJobResult
    jobMgr := scheduler.GetJobMgr()

    err, secCtxt := BuildSecurityContext(req)
    if err != nil {
        result.Status = BIOFLOW_API_RET_FAIL
        result.Msg = err.Error()
        writeJSON(500, result, w)
        return
    }

    err = CheckClientAccount(secCtxt)
    if err != nil {
        result.Status = BIOFLOW_API_RET_FAIL
        result.Msg = err.Error()
        writeJSON(500, result, w)
        return
    }

    err, hangJobs := jobMgr.ListHangJobs(secCtxt)
    if err != nil {
        ServerLogger.Infof("Can't list hang jobs: %s\n",
            err.Error())
        result.Status = BIOFLOW_API_RET_FAIL
        result.Msg = err.Error()
        writeJSON(500, result, w)
    } else {
        ServerLogger.Debugf("Successfully list hang jobs \n")
        result.Status = BIOFLOW_API_RET_OK
        result.Msg = "successfully list hang jobs"
        result.Jobs = hangJobs
        result.Count = len(hangJobs)
        writeJSON(http.StatusAccepted, result, w)
    }
}

func GetJobTaskResourceUsageInfo(w http.ResponseWriter, req *http.Request) {
    defer req.Body.Close()
    vars := mux.Vars(req)
    id := vars["jobId"]
    taskId := vars["taskId"]

    ServerLogger.Debugf("Get resource usage of job %s and task %s \n",
        id, taskId)
    var result BioflowGetResourceUsageResult
    err, secCtxt := BuildSecurityContext(req)
    if err != nil {
        result.Status = BIOFLOW_API_RET_FAIL
        result.Msg = err.Error()
        writeJSON(500, result, w)
        return
    }

    err = CheckClientAccount(secCtxt)
    if err != nil {
        result.Status = BIOFLOW_API_RET_FAIL
        result.Msg = err.Error()
        writeJSON(500, result, w)
        return
    }
    
    jobMgr := scheduler.GetJobMgr()
    jobId, err := jobMgr.GetJobId(id, GET_JOBID_FROM_ALL)
    if err != nil {
        result.Status = BIOFLOW_API_RET_FAIL
        result.Msg = err.Error()
        writeJSON(500, result, w)
        return
    }

    ServerLogger.Debugf("Get resource usage info for job %s and task %s", 
        jobId, taskId)

    err, usageInfo := jobMgr.GetJobTaskResourceUsageInfo(secCtxt, jobId, taskId)
    if err != nil {
        ServerLogger.Infof("Can't get job resource usage info: %s\n",
            err.Error())
        result.Status = BIOFLOW_API_RET_FAIL
        result.Msg = err.Error()
        writeJSON(500, result, w)
    } else {
        result.Status = BIOFLOW_API_RET_OK
        result.Msg = "successfully get resource usage"
        result.Usage = usageInfo
        writeJSON(http.StatusAccepted, result, w)
    }
}
