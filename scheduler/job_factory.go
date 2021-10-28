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
    "encoding/json"
    "io/ioutil"
    "io"
    "fmt"
    "errors"
    . "github.com/xtao/bioflow/message"
    . "github.com/xtao/bioflow/common"
    . "github.com/xtao/bioflow/scheduler/common"
)

func ParseJobJSON(jobJson *JobJSONData) Job {
    datasetJson := jobJson.InputDataSet
    dataset := NewBIODataSet(datasetJson.Vol, datasetJson.InputDir,
        datasetJson.Files, datasetJson.FilesInOrder,
        datasetJson.RestorePath,
        datasetJson.RestoreFilter,
        datasetJson.LogPath)
    if datasetJson.InputMap != nil {
        dataset.SetInputMap(datasetJson.InputMap)
    }
    dataset.SetWorkflowInput(datasetJson.WorkflowInput)

    job := NewBIOJob(jobJson.Name, jobJson.Description, dataset, jobJson.WorkDir,
        jobJson.LogDir, jobJson.Pipeline)
    if jobJson.SMId == "" {
        jobJson.SMId = jobJson.Name
    }
    job.SetSampleID(jobJson.SMId)
    job.SetPriority(jobJson.Priority)
    job.SetOOMKillDisabled(jobJson.DisableOOMCheck)
    job.SetHDFSWorkDir(jobJson.HDFSWorkDir)
    job.SetExecMode(jobJson.ExecMode)
    if jobJson.StageQuota != nil {
        job.SetStageQuota(*jobJson.StageQuota)
    }
    job.SetConstraints(jobJson.Constraints)

    return job
}

func ValidateJobJSON(jobJson *JobJSONData) error {
    /*validate the job JSON input*/
    if jobJson.Name == "" {
        return errors.New("Job Name is NULL")
    }

    if jobJson.Priority < JOB_MIN_PRI || jobJson.Priority > JOB_MAX_PRI {
        errMsg := fmt.Sprintf("Job Priority should be between %d and %d",
            JOB_MIN_PRI, JOB_MAX_PRI)
        return errors.New(errMsg)
    }

    return nil
}

func NewJobFromJSONFile(file string) Job {
    raw, err := ioutil.ReadFile(file)
    if err != nil {
        Logger.Println(err.Error())
        return nil
    }

    var jobJson JobJSONData
    err = json.Unmarshal(raw, &jobJson)
    if err != nil {
        Logger.Println(err.Error())
        return nil
    }

    if err := ValidateJobJSON(&jobJson); err != nil {
        SchedulerLogger.Errorf("Job %s is not valid \n", jobJson.Name)
        return nil
    }

    return ParseJobJSON(&jobJson)
}

func NewJobFromJSONStream(body io.ReadCloser) (error, Job) {
    decoder := json.NewDecoder(body)
    decoder.UseNumber()
    var jobJson JobJSONData
    err := decoder.Decode(&jobJson)
    if err != nil {
        SchedulerLogger.Errorf("Can't parse the request " + err.Error())
        return err, nil
    }

    if err := ValidateJobJSON(&jobJson); err != nil {
        SchedulerLogger.Errorf("Job %s is not valid \n", jobJson.Name)
        return err, nil
    }

    return nil, ParseJobJSON(&jobJson)
}
