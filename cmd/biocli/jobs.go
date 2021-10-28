package main

import (
    "fmt"
    "os"

    . "github.com/xtao/bioflow/message"
    . "github.com/xtao/bioflow/client"
)



type jobsCommand struct {
    client *BioflowClient
}

func newJobsCommand(c *BioflowClient) *jobsCommand {
    return &jobsCommand{
        client: c,
    }
}

func ShowJobsRunningStages(jobStatus *BioflowJobStatus) {
    if jobStatus == nil {
        fmt.Printf("Error, can't parse the job status\n")
        return
    }
    pendingStages := jobStatus.PendingStages

    failedStages := make([]BioflowStageInfo, 0)

    var stageInfo BioflowStageInfo

    if len(pendingStages) == 0 {
        fmt.Printf(" RunningStages: No stage running\n")
    } else {
        for _, stageInfo = range pendingStages {
            if stageInfo.State == "STAGE_FAIL" {
                failedStages = append(failedStages, stageInfo)
                continue
            }
        }
        fmt.Printf(" RunningStages: %d\n", len(pendingStages) - len(failedStages))
        for _, stageInfo = range pendingStages {
            if stageInfo.State == "STAGE_FAIL" {
                continue
            }
            fmt.Printf("    Stage %s: \n", stageInfo.Id)
            fmt.Printf("        Name: %s\n", stageInfo.Name)
            fmt.Printf("        State: %s\n", stageInfo.State)
            fmt.Printf("        Output: \n")
            for name, file := range stageInfo.Output {
                fmt.Printf("          %s:%s \n", name, file)
            }
            fmt.Printf("        Backend: %s\n", stageInfo.BackendId)
            fmt.Printf("        Task: %s\n", stageInfo.TaskId)
            fmt.Printf("        RetryCount: %d\n", stageInfo.RetryCount)
            fmt.Printf("        Submited: %s\n", stageInfo.SubmitTime)
            fmt.Printf("        Scheduled: %s\n", stageInfo.ScheduleTime)
            fmt.Printf("        Finished: %s\n", stageInfo.FinishTime)
            if stageInfo.TotalDuration > 0 {
                fmt.Printf("        Duration: %f\n", stageInfo.TotalDuration)
            } else {
                fmt.Printf("        Duration: N/A\n")
            }
            if stageInfo.HostName == "" {
                fmt.Printf("        ExecutionHost: N/A\n")
            } else {
                fmt.Printf("        ExecutionHost: %s(%s)\n",
                    stageInfo.HostName, stageInfo.HostIP)
            }
            fmt.Printf("        CPU: %f\n", stageInfo.CPU)
            fmt.Printf("        Memory: %f\n", stageInfo.Memory)
            if stageInfo.ServerType != "" {
                fmt.Printf("        ServerType: %s\n", stageInfo.ServerType)
            }
            if stageInfo.GPU > 0 {
                fmt.Printf("        GPU: %f\n", stageInfo.GPU)
            }
            if stageInfo.GPUMemory > 0 {
                fmt.Printf("        GPUMemory: %f\n", stageInfo.GPUMemory)
            }
            if stageInfo.Disk > 0 {
                fmt.Printf("        Disk: %f\n", stageInfo.Disk)
            }
            if stageInfo.ExecMode != "" {
                fmt.Printf("        ExecMode: %s\n", stageInfo.ExecMode)
            }
        }
    }

}

func (cmd *jobsCommand) List(args map[string]interface{}) {
    err, jobList, _ := cmd.client.ListJobs(args)
    if err != nil {
        fmt.Printf("Fail to get all jobs: %s\n", err.Error())
        os.Exit(1)
    }

    if jobList == nil || len(jobList)  == 0 {
        fmt.Printf("No jobs found\n")
        os.Exit(1)
    } else {
        cmd.Status(jobList)
    }
}

func (cmd *jobsCommand) Status(jobList []BioflowJobInfo) {
    for i := 0; i < len(jobList); i ++ {
        jobInfo := jobList[i]
        err, jobStatus := cmd.client.GetJobStatus(jobInfo.JobId)
        if err != nil {
            fmt.Printf("Fail to get job %s status: %s\n",
                jobInfo.JobId, err.Error())
            os.Exit(1)
        } else {
            if i != 0 {
                fmt.Printf("\n")
                fmt.Printf("============================================================\n")
            }
            fmt.Printf("%d. Job %s", i, jobInfo.JobId)
            ShowJobsRunningStages(jobStatus)
        }
    }
}
