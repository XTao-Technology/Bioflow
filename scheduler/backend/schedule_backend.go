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
    "io"
    "time"
    "errors"
)

type ScheduleBackendTask struct {
    Cmd string
    Image string
    CPU float64
    GPU float64
    GPUMemory float64
    OversubCPU float64
    Memory float64
    OversubMemory float64
    SoftMemLimit float64
    Disk float64
    ForcePullImage bool
    DisableOOMKill bool
    UseHostNet bool
    CallBackURI string
    VolumesMap map[string]string
    ScheduleConstraints map[string]string
    WorkDir string
    Priority int
    IsKillable bool
    Privileged bool
    Env map[string]string
    ExecMode string
    User string
    UID string
    GID string
    TaskName string
    Labels map[string]string
}

/*Task state definition*/
const (
    TASK_FINISHED int = 1
    TASK_FAIL int = 2
    TASK_ERROR int = 3
    TASK_LOST int = 4
    TASK_KILLED int = 5
    TASK_RUNNING int = 6
    TASK_QUEUED int = 7
    TASK_STAGING int = 8
    TASK_STARTING int = 9
    TASK_TERMINATING int = 10
    TASK_GONE int = 11
    TASK_UNKNOWN int = 13
    TASK_DROPPED int = 14
)

func BackendTaskStateToStr(state int) string {
    switch state {
        case TASK_FINISHED:
            return "TASK_FINISHED"
        case TASK_FAIL:
            return "TASK_FAIL"
        case TASK_ERROR:
            return "TASK_ERROR"
        case TASK_LOST:
            return "TASK_LOST"
        case TASK_KILLED:
            return "TASK_KILLED"
        case TASK_RUNNING:
            return "TASK_RUNNING"
        case TASK_QUEUED:
            return "TASK_QUEUED"
        case TASK_STAGING:
            return "TASK_STAGING"
        case TASK_STARTING:
            return "TASK_STARTING"
        case TASK_TERMINATING:
            return "TASK_TERMINATING"
        case TASK_UNKNOWN:
            return "TASK_UNKNOWN"
        case TASK_GONE:
            return "TASK_GONE"
        case TASK_DROPPED:
            return "TASK_DROPPED"
        default:
            return "UNKNOWN"
    }
}

var (
    BE_QFULL_ERR error = errors.New("backend queue full")
    BE_UNKNOWN_ERR error = errors.New("backend unknow error")
    BE_INTERNAL_ERR error = errors.New("backend internal error")
    BE_CONN_ERR error = errors.New("network error")



    BE_TASK_NOT_FOUND error = errors.New("task not found")
)

var (
    SANDBOX_STDOUT          = "stdout"
    SANDBOX_STDERR          = "stderr"
    SANDBOX_PROFILING       = "profiling"
    SANDBOX_CMD_STDOUT      = "cmd_stdout"
    SANDBOX_CMD_STDERR      = "cmd_stderr"
)

const BE_SCHED_RETRY_COUNT int = 2
const BE_RETRY_PERIOD time.Duration = 5 * time.Second
const BE_RETRY_COUNT int = 3

type TaskStatus struct {
    State int
    Info string
    SlaveID string
    HostName string
    AgentIP string
}

type TaskEvent struct {
    Id string
    Event int
    Info string
    SlaveID string
    HostName string
    AgentIP string
}

type ScheduleBackend interface {
    Start() int
    SubmitTask(*ScheduleBackendTask) (error, string)
    CheckTask(string) (error, *TaskStatus)
    KillTask(string) error
    DeleteTask(string) error
    GetID() string
    GetServer() string
    GetType() string
    GetSandboxFile(string, string) ([]byte, error)
    FetchSandboxFile(string, string, string) error
    ParseTaskNotify(io.ReadCloser) (error, *TaskEvent)
    GetRemovableSlaves() (error, []string)
    Ping() error
}
