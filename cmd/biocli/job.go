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
package main

import (
	"fmt"
    "os"
    "strings"
    "strconv"

    . "github.com/xtao/bioflow/message"
    . "github.com/xtao/bioflow/client"
    "time"
    "sort"
)

type jobCommand struct {
	client *BioflowClient
}

func newJobCommand(c *BioflowClient) *jobCommand {
	return &jobCommand{
		client: c,
	}
}

func ShowJobs(jobList []BioflowJobInfo, histJobs []BioflowJobInfo) {
    if jobList == nil || len(jobList)  == 0 {
        if histJobs == nil || len(histJobs) == 0 {
            fmt.Printf("No jobs found\n")
            return
        }
    } else {
        fmt.Printf("%d running jobs: \n", len(jobList))
        for i := 0; i < len(jobList); i ++ {
            jobInfo := jobList[i]
            fmt.Printf(" Job %d: \n", i + 1)
            fmt.Printf("    ID: %s\n", jobInfo.JobId)
            fmt.Printf("    Name: %s\n", jobInfo.Name)
            fmt.Printf("    Pipeline: %s\n", jobInfo.Pipeline)
            fmt.Printf("    Created: %s\n", jobInfo.Created)
            fmt.Printf("    Finished: N/A\n")
            fmt.Printf("    State: %s\n", jobInfo.State)
            fmt.Printf("    Owner: %s\n", jobInfo.Owner)
            if jobInfo.ExecMode != "" {
                fmt.Printf("    RunningMode: %s\n", jobInfo.ExecMode)
            }
            fmt.Printf("    Priority: %d\n", jobInfo.Priority)
            fmt.Println(" ")
        }
    }

	if histJobs == nil || len(histJobs)  == 0 {
		return
	}

	fmt.Printf("%d history jobs: \n", len(histJobs))
	for i := 0; i < len(histJobs); i ++ {
		jobInfo := histJobs[i]
		fmt.Printf(" Job %d: \n", i + 1)
		fmt.Printf("    ID: %s\n", jobInfo.JobId)
		fmt.Printf("    Name: %s\n", jobInfo.Name)
		fmt.Printf("    Pipeline: %s\n", jobInfo.Pipeline)
		fmt.Printf("    Created: %s\n", jobInfo.Created)
		fmt.Printf("    Finished: %s\n", jobInfo.Finished)
		fmt.Printf("    State: %s\n", jobInfo.State)
		fmt.Printf("    Owner: %s\n", jobInfo.Owner)
        if jobInfo.ExecMode != "" {
            fmt.Printf("    RunningMode: %s\n", jobInfo.ExecMode)
        }
		fmt.Printf("    Priority: %d\n", jobInfo.Priority)
		fmt.Println(" ")
	}
}

func ShowJobStatus(showDetail bool, showWait bool, showPerf bool,
    showOutput bool, jobStatus *BioflowJobStatus) {
    if jobStatus == nil {
        fmt.Printf("Error, can't parse the job status\n")
        return
    }

    fmt.Printf("Status of Job %s: \n", jobStatus.JobId)
    fmt.Printf(" Name: %s\n", jobStatus.Name)
    fmt.Printf(" Pipeline: %s\n", jobStatus.Pipeline)
    fmt.Printf(" State: %s\n", jobStatus.State)
    fmt.Printf(" Owner: %s\n", jobStatus.Owner)
    fmt.Printf(" WorkDir: %s\n", jobStatus.WorkDir)
    if jobStatus.HDFSWorkDir != "" {
        fmt.Printf(" HDFSWorkDir: %s\n", jobStatus.HDFSWorkDir)
    }
    fmt.Printf(" PausedState: %s\n", jobStatus.PausedState)
    fmt.Printf(" Created: %s\n", jobStatus.Created)
    fmt.Printf(" Finished: %s\n", jobStatus.Finished)
    fmt.Printf(" RetryLimit: %d\n", jobStatus.RetryLimit)
    fmt.Printf(" RunCount: %d\n", jobStatus.RunCount)
    fmt.Printf(" StageCount: %d\n", jobStatus.StageCount)
    fmt.Printf(" StageQuota: %d\n", jobStatus.StageQuota)
    fmt.Printf(" Priority: %d\n", jobStatus.Priority)
    if jobStatus.ExecMode != "" {
        fmt.Printf(" RunningMode: %s\n", jobStatus.ExecMode)
    }
    fmt.Printf(" FailReason: \n")
    for i := 0; i < len(jobStatus.FailReason); i ++ {
        if strings.TrimSpace(jobStatus.FailReason[i]) != "" {
            fmt.Printf("    reason %d: %s\n",
                i + 1, jobStatus.FailReason[i])
        }
    }

    if jobStatus.GraphCompleted {
        fmt.Printf(" GraphBuildStatus: Completed\n")
    } else {
        fmt.Printf(" GraphBuildPipelinePos: %d\n",
            jobStatus.PipelineBuildPos)
    }
    doneStages := jobStatus.DoneStages
    pendingStages := jobStatus.PendingStages
    waitingStages := jobStatus.WaitingStages
    forbiddenStages := jobStatus.ForbiddenStages
    if len(doneStages) == 0 {
        fmt.Printf(" DoneStages: No stage done\n")
    } else {
        fmt.Printf(" DoneStages: %d\n", len(doneStages))
        for i := 0; i < len(doneStages); i ++ {
            stageInfo := doneStages[i]
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
            if stageInfo.FailReason != "" {
                fmt.Printf("        FailReason: %s\n", stageInfo.FailReason)
            }
            if showDetail {
                fmt.Printf("        Command: %s\n", stageInfo.Command)
            }
            if showPerf {
                rscStats := stageInfo.ResourceStats
                fmt.Printf("        Resource Stats(10m profiling): \n")
                fmt.Printf("         MaxCPURatio: %f\n", rscStats.MaxCPURatio)
                fmt.Printf("         AvgCPURatio: %f\n", rscStats.AvgCPURatio)
                fmt.Printf("         LowCPURatio: %f\n", rscStats.LowCPURatio)
                fmt.Printf("         MaxMem: %f\n", rscStats.MaxMem)
                fmt.Printf("         AvgMem: %f\n", rscStats.AvgMem)
                fmt.Printf("         MaxIOMem: %f\n", rscStats.MaxIOMem)
                fmt.Printf("         AvgIOMem: %f\n", rscStats.AvgIOMem)
                fmt.Printf("         TotalRead: %fM\n", rscStats.TotalRead)
                fmt.Printf("         TotalWrite: %fM\n", rscStats.TotalWrite)
                fmt.Printf("         TotalReadOps: %f\n", rscStats.TotalSysCR)
                fmt.Printf("         TotalWriteOps: %f\n", rscStats.TotalSysCW)
                fmt.Printf("         MaxProfilingCost: %f\n", rscStats.MaxProfilingCost)
                if rscStats.OOM {
                    fmt.Printf("         OutOfMemory: YES\n")
                } else {
                    fmt.Printf("         OutOfMemory: NO\n")
                }
            }
        }
    }

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
            if showDetail {
                fmt.Printf("        Command: %s\n", stageInfo.Command)
            }
        }
    }
    if len(waitingStages) == 0 {
        fmt.Printf(" WaitingStages: No stage waiting\n")
    } else {
        fmt.Printf(" WaitingStages: %d\n", len(waitingStages))
        if showWait {
            for _, stageInfo = range waitingStages {
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
                if showDetail {
                    fmt.Printf("        Command: %s\n", stageInfo.Command)
                }
            }
        }
    }

    if len(forbiddenStages) == 0 {
        fmt.Printf(" ForbiddenStages: No stage forbidden\n")
    } else {
        fmt.Printf(" ForbiddenStages: %d\n", len(forbiddenStages))
        if showWait {
            for _, stageInfo = range forbiddenStages {
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
                fmt.Printf("        Finished: %s\n", stageInfo.FinishTime)
                fmt.Printf("        Duration: %f\n", stageInfo.TotalDuration)
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
                if showDetail {
                    fmt.Printf("        Command: %s\n", stageInfo.Command)
                }
            }
        }
    }

	if len(failedStages) != 0 {
        fmt.Printf(" FailedStages: %d\n", len(failedStages))
        for _, stageInfo = range failedStages {
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

            if stageInfo.FailReason != "" {
                fmt.Printf("        FailReason: %s\n", stageInfo.FailReason)
            }
            if showDetail {
                fmt.Printf("        Command: %s\n", stageInfo.Command)
            }
        }
	}

    if showOutput {
        if len(jobStatus.JobOutput) == 0 {
            fmt.Printf(" Output: N/A\n")
        } else {
            fmt.Printf(" Output: \n")
            for key, value := range jobStatus.JobOutput {
                fmt.Printf("    %s: %s\n", key, value)
            }
        }
    }

    fmt.Printf("\n")
}

func ShowJobLog(jobId string, logs map[string]BioflowStageLog) {
	fmt.Printf("========================\n")
	fmt.Printf("| Job %s Log:\n", jobId)
	fmt.Printf("========================\n")
	for stageName, log:= range logs {
		fmt.Printf("\n")
		fmt.Printf("++++++++++++++++++++++++\n")
		fmt.Printf("+ Stage %s:\n", stageName)
		fmt.Printf("++++++++++++++++++++++++\n")
		for taskid, le := range log.Logs {
			fmt.Printf("\n")
			fmt.Printf("------------------------\n")
			fmt.Printf("| TaskID: %s\n", taskid)
			fmt.Printf("------------------------\n")
            fmt.Printf("%s\n", le)
        }
	}
}

func (cmd *jobCommand) Submit(file string, isYaml bool) {
    jsonFile := file
    var err error
    if isYaml {
        jsonFile, err = YamlFileToJSON(file)
        if err != nil {
            fmt.Printf("Fail to convert from yaml to json: %s\n",
                err.Error())
            os.Exit(1)
        }
    }
	err, jobId := cmd.client.AddJobFile(jsonFile)
	if err != nil {
		fmt.Printf("The job added failure: %s\n", err.Error())
        if isYaml {
            os.Remove(jsonFile)
        }
		os.Exit(1)
	} else {
        if isYaml {
            os.Remove(jsonFile)
        }
		fmt.Printf("The job added success, job ID is: %s\n",
			jobId)
	}
}

func (cmd *jobCommand) Cancel(id string) {
	if id == "" {
		fmt.Printf("Please specify a valid job id: %s\n", id)
		os.Exit(1)
	}
	
	err := cmd.client.CancelJob(id)
	if err != nil {
		fmt.Printf("Fail to cancel job %s: %s\n", 
			id, err.Error())
		os.Exit(1)
	} else {
		fmt.Printf("Successfully cancel job %s\n", id)
	}
}

func (cmd *jobCommand) Status(id string, showDetail bool, showWait bool,
    showPerf bool, showOutput bool) {
    if id == "" {
		fmt.Printf("Please specify a valid job id: %s\n", id)
		os.Exit(1)
	}
	
	err, jobStatus := cmd.client.GetJobStatus(id)
	if err != nil {
		fmt.Printf("Fail to get job %s status: %s\n", 
			id, err.Error())
		os.Exit(1)
	} else {
		ShowJobStatus(showDetail, showWait, showPerf, showOutput,
            jobStatus)
	}
}

type Sorter struct{
    src []BioflowJobInfo
    record []int64
}

func (s *Sorter) Len() int {
    return len(s.src)
}

func (s *Sorter) Less(i, j int) bool {
    left := s.getParam(i)
    right := s.getParam(j)
    return left < right
}

func (s *Sorter) Swap (i, j int) {
    s.src[i], s.src[j] = s.src[j], s.src[i]
    s.record[i], s.record[j] = s.record[j], s.record[i]
}

func (s *Sorter) getParam (i int) int64 {
    var param int64
    if s.record[i] == 0 {
        time, err := time.Parse(time.RFC3339, s.src[i].Created)
        if err != nil {
            param = -1
        } else {
            param = time.Unix()
        }
        s.record[i] = param
    } else {
        param = s.record[i]
    }

    return param
}

func NewSorter (src []BioflowJobInfo) *Sorter {
    return &Sorter{
        src: src,
        record: make([]int64, len(src), len(src)),
    }
}

func (cmd *jobCommand) Log(jobId string, stageName string, stageId string) {
	if jobId == "" {
		fmt.Printf("Please specify a valid job id: %s\n", jobId)
		os.Exit(1)
	}

	err, logs := cmd.client.GetJobLog(jobId, stageName, stageId)
	if err != nil {
		fmt.Printf("Fail to get log for job %s:%s\n",
            jobId, err.Error())
        fmt.Printf("You may try with other options: e.g -T\n")
		os.Exit(1)
	} else {
		ShowJobLog(jobId, logs)		
	}
}

func (cmd *jobCommand) KillTasks(id string, taskId string) {
	if id == "" {
		fmt.Printf("Please specify a valid job id: %s\n", id)
		os.Exit(1)
	}

	err := cmd.client.KillTasks(id, taskId)
	if err != nil {
		fmt.Printf("Fail to kill job %s tasks %s: %s\n", 
			id, taskId, err.Error())
		os.Exit(1)
	} else {
		fmt.Printf("Successfully kill job %s tasks %s\n",
            id, taskId)
	}
}

func (cmd *jobCommand) Pause(id string) {
	if id == "" {
		fmt.Printf("Please specify a valid job id: %s\n", id)
		os.Exit(1)
	}

	err := cmd.client.PauseJob(id)
	if err != nil {
		fmt.Printf("Fail to pause job %s: %s\n", 
			id, err.Error())
		os.Exit(1)
	} else {
		fmt.Printf("Successfully pause job %s\n", id)
	}
}

func (cmd *jobCommand) Resume(id string) {
	if id == "" {
		fmt.Printf("Please specify a valid job id: %s\n", id)
		os.Exit(1)
	}

	err := cmd.client.ResumeJob(id)
	if err != nil {
		fmt.Printf("Fail to resume job %s: %s\n", 
			id, err.Error())
		os.Exit(1)
	} else {
		fmt.Printf("Successfully resume job %s\n", id)
	}
}

func (cmd *jobCommand) List(args map[string]interface{}) {
	err, jobList, histJobs := cmd.client.ListJobs(args)
	if err != nil {
		fmt.Printf("Fail to list jobs: %s\n", err.Error())
		os.Exit(1)
	} else {
	    sort.Sort(NewSorter(jobList))
	    sort.Sort(NewSorter(histJobs))
		ShowJobs(jobList, histJobs)
	}
}

func (cmd *jobCommand) Cleanup(args map[string]interface{}) {
    /*It is dangerous to cleanup all the history jobs*/
    filtered := false
    for key, _ := range args {
        if key != "Finish" && key != "Count" {
            if key == "Priority" {
                priVal := args[key].(int)
                if priVal == -1 {
                    continue
                }
            }
            filtered = true
        }
    }
    if !filtered {
        fmt.Printf("Please specify a filter, or -a to cleanup all\n")
        os.Exit(1)
    }

	err, count := cmd.client.CleanupJobs(args)
	if err != nil {
		fmt.Printf("Fail to cleanup jobs: %s\n", err.Error())
		os.Exit(1)
	} else {
        fmt.Printf("Matched and Cleanup %d jobs\n",
            count)
    }
}

func (cmd *jobCommand) Requeue(jobId string, stageId string,
    cpu string, memory string) {

    var cpuVal float64 = -1
    var memoryVal float64 = -1
    if cpu != "" {
        val, err := strconv.ParseFloat(cpu, 64)
        if err != nil {
            fmt.Printf("CPU Value %s for not valid\n",
                    cpu)
            os.Exit(-1)
        }
        cpuVal = val
    }
    if memory != "" {
        val, err := strconv.ParseFloat(memory, 64)
        if err != nil {
            fmt.Printf("Memory Value %s for not valid\n",
                    memory)
            os.Exit(-1)
        }
        memoryVal = val
    }
	err := cmd.client.RequeueJobStage(jobId, stageId, cpuVal,
        memoryVal)
	if err != nil {
		fmt.Printf("Fail to requeue the job %s, stage %s: %s\n", 
			jobId, stageId, err.Error())
		os.Exit(1)
	} else {
		fmt.Printf("Successfully requeue job %s stage %s, update cpu %s and memory %s\n",
		    jobId, stageId, cpu, memory)
	}
}

func (cmd *jobCommand) UpdateJobPriority(jobId string, pri int) {
	err := cmd.client.UpdateJobPriority(jobId, pri)
	if err != nil {
		fmt.Printf("Fail to update the job %s priority to %d: %s\n", 
			jobId, pri, err.Error())
		os.Exit(1)
	} else {
		fmt.Printf("Successfully update job %s priority to %d \n",
		    jobId, pri)
	}
}

func (cmd *jobCommand) Recover(jobId string) {
	err := cmd.client.RecoverJob(jobId)
	if err != nil {
		fmt.Printf("Fail to recover the job %s: %s\n", 
			jobId, err.Error())
		os.Exit(1)
	} else {
		fmt.Printf("Successfully recovery job %s\n",
		    jobId)
	}
}

func (cmd *jobCommand) ListHang() {
	err, jobList := cmd.client.ListHangJobs()
	if err != nil {
		fmt.Printf("Fail to list hang jobs: %s\n",
            err.Error())
		os.Exit(1)
	} else {
		ShowHangJobsInfo(jobList)
	}
}

func ShowHangJobsInfo(jobList []BioflowJobInfo) {
    if jobList == nil || len(jobList)  == 0 {
        fmt.Printf("No hang jobs\n")
    } else {
        fmt.Printf("%d hang jobs: \n", len(jobList))
        for i := 0; i < len(jobList); i ++ {
            jobInfo := jobList[i]
            fmt.Printf(" Job %d: \n", i + 1)
            fmt.Printf("    ID: %s\n", jobInfo.JobId)
            fmt.Printf("    Name: %s\n", jobInfo.Name)
            fmt.Printf("    Pipeline: %s\n", jobInfo.Pipeline)
            fmt.Printf("    Created: %s\n", jobInfo.Created)
            fmt.Printf("    Finished: N/A\n")
            fmt.Printf("    State: %s\n", jobInfo.State)
            fmt.Printf("    Owner: %s\n", jobInfo.Owner)
            fmt.Printf("    Priority: %d\n", jobInfo.Priority)
            if jobInfo.ExecMode != "" {
                fmt.Printf("    RunningMode: %s\n", jobInfo.ExecMode)
            }
            fmt.Println(" ")
            
            hangStages := jobInfo.HangStages
            fmt.Printf("    Hang Stages: %d\n", len(hangStages))
            for j := 0; j < len(hangStages); j ++ {
                stageInfo := hangStages[j]
                fmt.Printf("      Stage %s: \n", stageInfo.Id)
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
                fmt.Printf("        CPU: %f\n", stageInfo.CPU)
                fmt.Printf("        Memory: %f\n", stageInfo.Memory)
                if stageInfo.ServerType != "" {
                    fmt.Printf("        ServerType: %s\n", stageInfo.ServerType)
                }
                if stageInfo.ServerType != "" {
                    fmt.Printf("        ServerType: %f\n", stageInfo.ServerType)
                }
                if stageInfo.ExecMode != "" {
                    fmt.Printf("        ExecMode: %s\n", stageInfo.ExecMode)
                }
            }
        }
    }
}

func (cmd *jobCommand) Resource(jobId string, taskId string) {
	if jobId == "" || taskId == "" {
		fmt.Printf("Please specify a valid job id and task id\n")
		os.Exit(1)
	}

	err, resourceUsage := cmd.client.GetJobResourceUsage(jobId, taskId)
	if err != nil {
		fmt.Printf("Fail to get resource usage for job %s and task %s: %s\n",
            jobId, taskId, err.Error())
		os.Exit(1)
	} else {
		ShowJobResourceUsage(jobId, taskId, resourceUsage)
    }
}

func ShowJobResourceUsage(jobId string, taskId string, rscUsage map[string]ResourceUsageInfo) {
    fmt.Printf("Resource Usage for job %s:\n", jobId)
    for task, usageInfo := range rscUsage {
        fmt.Printf(" Task %s Resource Summary:\n", task)
        fmt.Printf("   MaxCPURatio: %f\n", usageInfo.MaxCPURatio)
        fmt.Printf("   AvgCPURatio: %f\n", usageInfo.AvgCPURatio)
        fmt.Printf("   LowCPURatio: %f\n", usageInfo.LowCPURatio)
        fmt.Printf("   MaxMem: %f\n", usageInfo.MaxMem)
        fmt.Printf("   AvgMem: %f\n", usageInfo.AvgMem)
        fmt.Printf("   MaxIOMem: %f\n", usageInfo.MaxIOMem)
        fmt.Printf("   AvgIOMem: %f\n", usageInfo.AvgIOMem)
        fmt.Printf("   MaxSwap: %f\n", usageInfo.MaxSwap)
        fmt.Printf("   AvgSwap: %f\n", usageInfo.AvgSwap)
        fmt.Printf("   TotalRead: %fM\n", usageInfo.TotalRead)
        fmt.Printf("   TotalWrite: %fM\n", usageInfo.TotalWrite)
        fmt.Printf("   TotalReadOps: %f\n", usageInfo.TotalSysCR)
        fmt.Printf("   TotalWriteOps: %f\n", usageInfo.TotalSysCW)
        fmt.Printf("   MaxProfilingCost: %f\n", usageInfo.MaxProfilingCost)
        if usageInfo.OOM {
            fmt.Printf("   OutOfMemory: YES\n")
        } else {
            fmt.Printf("   OutOfMemory: NO\n")
        }
        if usageInfo.Periods != nil && len(usageInfo.Periods) > 0 {
            for index, period := range usageInfo.Periods {
                fmt.Printf("   Period %d Usage:\n", index)
                fmt.Printf("    AvgSysRatio: %f\n", period.AvgSysRatio)
                fmt.Printf("    AvgUserRatio: %f\n", period.AvgUserRatio)
                fmt.Printf("    MaxMem: %f\n", period.MaxMem)
                fmt.Printf("    AvgMem: %f\n", period.AvgMem)
                fmt.Printf("    MaxIOMem: %f\n", period.MaxCache)
                fmt.Printf("    AvgIOMem: %f\n", period.AvgCache)
                fmt.Printf("    MaxSwap: %f\n", period.MaxSwap)
                fmt.Printf("    AvgSwap: %f\n", period.AvgSwap)
                fmt.Printf("    TotalRead: %f\n", period.TotalRead)
                fmt.Printf("    TotalWrite: %f\n", period.TotalWrite)
                fmt.Printf("    TotalReadOps: %f\n", period.TotalSysCR)
                fmt.Printf("    TotalWriteOps: %f\n", period.TotalSysCW)
            }
        }
        fmt.Printf("\n")
    }
}
