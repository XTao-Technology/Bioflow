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
package backend

import (
    "errors"
    "net/http"
    "os"
    "crypto/tls"
    "encoding/json"
    "io"
    "time"

    "github.com/klarna/eremetic"
    . "github.com/xtao/bioflow/common"
    "github.com/xtao/bioflow/message"
)


type eremeticBackend struct {
    eremeticServer string
    id             string
    backendType    string
    httpClient      *http.Client
    ermClient       *eremeticClient
}

func NewEremeticBackend(id string, server string) ScheduleBackend {
    backend := &eremeticBackend{
        eremeticServer: server,
        backendType: "paladin",
    }
    backend.id = id
    return backend
}

func (backend *eremeticBackend) Start() int {
    backend.httpClient = &http.Client{}
    if os.Getenv("BIOFLOW_INSECURE") != "" {
        tr := &http.Transport{
            TLSClientConfig: &tls.Config{InsecureSkipVerify: true},
        }
        backend.httpClient.Transport = tr
    }
    ec, err := NewEremeticClient(backend.eremeticServer,
        backend.httpClient)
    if err != nil {
        return -1
    }
    backend.ermClient = ec

    return 0
}

/*Eremetic error document*/
type errorDocument struct {
    Error   string `json:"error"`
    Message string `json:"message"`
}

func (backend *eremeticBackend) SubmitTask(task *ScheduleBackendTask) (error, string) {
    volumes := make([]eremetic.Volume, 0)
    for conPath, hostPath := range task.VolumesMap {
        vol := eremetic.Volume {
                ContainerPath: conPath,
                HostPath: hostPath,
            }
        volumes = append(volumes, vol)
    }
    constraints := make([]eremetic.SlaveConstraint, 0)
    for key, val := range task.ScheduleConstraints {
        constraint := eremetic.SlaveConstraint {
                        AttributeName: key,
                        AttributeValue: val,
        }
        constraints = append(constraints, constraint)
    }

    execMode := eremetic.EXEC_MODE_DOCKER
    if task.ExecMode == message.EXEC_MODE_HOST {
        execMode = eremetic.EXEC_MODE_MESOS
    }

    r := eremetic.Request{
        Command:     task.Cmd,
        DockerImage: task.Image,
        TaskCPUs:    task.CPU,
        OversubTaskCPUs: task.OversubCPU,
        TaskMem:     task.Memory,
        OversubTaskMem: task.OversubMemory,
        TaskGPUs: task.GPU,
        TaskGPUMem: task.GPUMemory,
        TaskDisk: task.Disk,
        SoftTaskMem: task.SoftMemLimit,
        ForcePullImage: task.ForcePullImage,
        DisableOOMKill: task.DisableOOMKill,
        CallbackURI: task.CallBackURI,
        Volumes: volumes,
        SlaveConstraints: constraints,
        WorkDir: task.WorkDir,
        UseHostNet: task.UseHostNet,
        Priority: task.Priority,
        IsKillable: task.IsKillable,
        Privileged: task.Privileged,
        Environment: task.Env,
        ExecMode: execMode,
        User: task.User,
        UserUID: task.UID,
        UserGID: task.GID,
        TaskName: task.TaskName,
        Labels: task.Labels,
    }

    var err error

    taskId := ""
    for i:= 0; i < BE_RETRY_COUNT; i++ {
        taskId, err = backend.ermClient.AddTask(r)
        if err != nil {
            BackendLogger.Errorf("Fail to add task to eremetic: %s\n",
                err.Error())
			if IsConnectionError(err) {
				BackendLogger.Errorf("Backend server problem?, retry after seconds...")
				time.Sleep(BE_RETRY_PERIOD)
				continue
			}
            return err, ""
        } else {
            return nil, taskId
        }
    }

    return err, ""
}

func (backend *eremeticBackend) GetSandboxFile(taskID string, fileName string) ([]byte, error) {
    return nil, errors.New("Not support now")
}

func (backend *eremeticBackend) FetchSandboxFile(taskID string, targetFileName string,
    localFileName string) error {
    var err error

    if targetFileName != SANDBOX_STDOUT && targetFileName != SANDBOX_STDERR && targetFileName != SANDBOX_PROFILING &&
        targetFileName != SANDBOX_CMD_STDOUT && targetFileName != SANDBOX_CMD_STDERR {
        BackendLogger.Errorf("Only support stdout, stderr, cmd_stdout, cmd_stderr and profiling file now")
        return errors.New("Only support stdout, stderr, cmd_stdout, cmd_stderr and profiling file now")
    }

    err = backend.ermClient.Sandbox(taskID, targetFileName, localFileName)
    if err != nil {
        BackendLogger.Errorf("Get sandbox file %s failed",
            targetFileName)
        return err
    }

    return nil
}

func (backend *eremeticBackend) CheckTask(taskID string) (error, *TaskStatus) {
    taskStatus := &TaskStatus{
                    State: -1,
                    Info: "",
    }

    if taskID == "" {
        return errors.New("Empty task id"), nil
    }

    task, err := backend.ermClient.Task(taskID)
    if err != nil {
        if err == BE_TASK_NOT_FOUND {
            BackendLogger.Infof("task %s lost \n", taskID)
            taskStatus.State = TASK_LOST
            return nil, taskStatus
        }
        BackendLogger.Infof("Check task %s http error: %s\n",
            taskID, err.Error())
        return err, nil
    }

    if err == nil && len(task.Status) >= 1 {
        taskStatus.SlaveID = task.SlaveID
        taskStatus.HostName = task.Hostname
        taskStatus.AgentIP = task.AgentIP
        status := task.Status[len(task.Status) - 1]
        taskStatus.Info = status.Message
        switch status.Status {
            case eremetic.TaskFinished:
                BackendLogger.Infof("HostName %s task %s finished \n", taskID, taskStatus.HostName)
                taskStatus.State = TASK_FINISHED
                return nil, taskStatus
            case eremetic.TaskFailed:
                BackendLogger.Infof("HostName %s task %s failed \n", taskID, taskStatus.HostName)
                taskStatus.State = TASK_FAIL
                return nil, taskStatus
            case eremetic.TaskKilled, eremetic.TaskTerminated:
                BackendLogger.Infof("HostName %s task %s killed \n", taskID, taskStatus.HostName)
                taskStatus.State = TASK_KILLED
                return nil, taskStatus
            case eremetic.TaskError:
                BackendLogger.Infof("HostName %s task %s error \n", taskID, taskStatus.HostName)
                taskStatus.State = TASK_ERROR
                return nil, taskStatus
            case eremetic.TaskLost:
                BackendLogger.Infof("HostName %s task %s lost \n", taskID, taskStatus.HostName)
                taskStatus.State = TASK_LOST
                return nil, taskStatus
            case eremetic.TaskRunning:
                BackendLogger.Infof("HostName %s task %s running \n", taskID, taskStatus.HostName)
                taskStatus.State = TASK_RUNNING
                return nil, taskStatus
            case eremetic.TaskQueued:
                BackendLogger.Infof("HostName %s task %s queued \n", taskID, taskStatus.HostName)
                taskStatus.State = TASK_QUEUED
                return nil, taskStatus
            case eremetic.TaskStaging:
                BackendLogger.Infof("HostName %s task %s staging \n", taskID, taskStatus.HostName)
                taskStatus.State = TASK_STAGING
                return nil, taskStatus
            case eremetic.TaskStarting:
                BackendLogger.Infof("HostName %s task %s starting \n", taskID, taskStatus.HostName)
                taskStatus.State = TASK_STARTING
                return nil, taskStatus
            case eremetic.TaskTerminating:
                BackendLogger.Infof("HostName %s task %s terminating \n", taskID, taskStatus.HostName)
                taskStatus.State = TASK_TERMINATING
                return nil, taskStatus
            case eremetic.TaskGone:
                BackendLogger.Infof("HostName %s task %s gone \n", taskID, taskStatus.HostName)
                taskStatus.State = TASK_GONE
                return nil, taskStatus
            case eremetic.TaskDropped:
                BackendLogger.Infof("HostName %s task %s dropped \n", taskID, taskStatus.HostName)
                taskStatus.State = TASK_DROPPED
                return nil, taskStatus
            case eremetic.TaskUnknown:
                BackendLogger.Infof("HostName %s task %s unknown \n", taskID, taskStatus.HostName)
                taskStatus.State = TASK_UNKNOWN
                return nil, taskStatus
            default:
                BackendLogger.Infof("task %s state unknown %s \n",
                    taskID, task.Status[len(task.Status) - 1].Status) 
        }
    } else if err == BE_TASK_NOT_FOUND || (err == nil && task.ID != taskID) {
        /*
         * This is not the best approach to check whether the task lost,
         * a better approach is to check eremetic's http response, it should
         * be "http.StatusNotFound". But the eremetic client doestn't check it.
         */
        BackendLogger.Infof("Check task %s gets empty information, may be lost\n",
            taskID)
        taskStatus.State = TASK_LOST
        return nil, taskStatus
    } else {
        BackendLogger.Errorf("Check task %s fail, unknown state %v \n",
            taskID, task)
        return errors.New("Check task fail"), nil
    }
    
    return nil, taskStatus
}

func (backend *eremeticBackend) KillTask(taskID string) error {
    var err error

    for i:= 0; i < BE_RETRY_COUNT; i++ {
		err = backend.ermClient.Kill(taskID)
		if err != nil {
			BackendLogger.Errorf("eremetic backend kill task %s fail: %s\n",
				taskID, err.Error())
			if IsConnectionError(err) {
				BackendLogger.Errorf("Backend server problem?, retry after seconds...")
				time.Sleep(BE_RETRY_PERIOD)
				continue
			} else {
				break
			}
		} else {
			break
		}
	}

    return err
}

func (backend *eremeticBackend) GetID() string {
    return backend.id
}

func (backend *eremeticBackend) GetType() string {
    return backend.backendType
}

func (backend *eremeticBackend) GetServer() string {
	return backend.eremeticServer
}

type CallbackData struct {
    Time   int64        `json:"time"`
    Status string       `json:"status"`
    TaskID string       `json:"task_id"`
    Message string      `json:"message"`
    SlaveID string      `json:"slave_id"`
    HostName string     `json:"host_name"`
    AgentIP string      `json:"agent_ip"`
}

func(backend *eremeticBackend) ParseTaskNotify(callbackMsg io.ReadCloser) (error, *TaskEvent) {
    decoder := json.NewDecoder(callbackMsg)
    var t CallbackData   
    err := decoder.Decode(&t)
    if err != nil {
        BackendLogger.Errorf("Can't parse the request\n")
        return err, nil
    }
    
    BackendLogger.Infof("Received Eremetic Notify %s %s %s\n",
        t.TaskID, t.Status, t.Message)

    taskEvent := &TaskEvent{
                    Id: t.TaskID,
                    Info: t.Message,
                    SlaveID: t.SlaveID,
                    HostName: t.HostName,
                    AgentIP: t.AgentIP,
            }

    switch t.Status {
        case eremetic.TaskFailed.String():
            taskEvent.Event = TASK_FAIL
        case eremetic.TaskFinished.String():
            taskEvent.Event = TASK_FINISHED
        case eremetic.TaskLost.String():
            taskEvent.Event = TASK_LOST
        case eremetic.TaskError.String():
            taskEvent.Event = TASK_ERROR
        case eremetic.TaskRunning.String():
            taskEvent.Event = TASK_RUNNING
        case eremetic.TaskGone.String():
            taskEvent.Event = TASK_GONE
        case eremetic.TaskDropped.String():
            taskEvent.Event = TASK_DROPPED
        case eremetic.TaskUnknown.String():
            taskEvent.Event = TASK_UNKNOWN
        case eremetic.TaskKilled.String(), eremetic.TaskTerminated.String():
            taskEvent.Event = TASK_KILLED
        default:
            BackendLogger.Infof("Callback event %s change to %s on time %d \n", 
                t.TaskID, t.Status, t.Time)
            taskEvent.Event = -1
    }

    return nil, taskEvent
}

func (backend *eremeticBackend) DeleteTask(taskID string) error {
    var err error

    for i:= 0; i < BE_RETRY_COUNT; i++ {
		err = backend.ermClient.Delete(taskID)
		if err != nil {
			BackendLogger.Errorf("eremetic backend delete task %s fail: %s\n",
				taskID, err.Error())
			if IsConnectionError(err) {
				BackendLogger.Errorf("Backend server problem?, retry after seconds...")
				time.Sleep(BE_RETRY_PERIOD)
				continue
			} else {
				break
			}
		} else {
			break
		}
	}

    return err
}

func (backend *eremeticBackend) GetRemovableSlaves() (error, []string) {
    return backend.ermClient.RemovableSlaves()
}

func (backend *eremeticBackend) Ping() error {
    return backend.ermClient.Ping()
}
