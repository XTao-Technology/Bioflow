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
	"fmt"
    "time"
    "net/http"
    "encoding/json"
    "bytes"
    "sync"
    "io"
    "errors"

    . "github.com/xtao/bioflow/common"
)

type simulatorBackend struct {
    taskCount int
    tasks map[string]string
    taskPeriod int
    lock sync.Mutex
    backendType string
}

func NewSimulatorBackend(taskPeriod int) ScheduleBackend {
    backend := &simulatorBackend{
        tasks: make(map[string]string),
        taskCount: 0,
        taskPeriod: taskPeriod,
        backendType: "simulator",
    }
    return backend
}

func (backend *simulatorBackend) Start() int {
    go func() {
        for ; ; {
            time.Sleep(time.Duration(backend.taskPeriod) * time.Second)
            BackendLogger.Infof("backend will simulate callback \n")
            backend.SimulateCallback()
            BackendLogger.Infof("backend simulate callback done\n")
        }
    }()

    return 0
}

func (backend *simulatorBackend) SimulateCallback() {
    backend.lock.Lock()
    defer backend.lock.Unlock()

    client := &http.Client{}
    for taskId, callbackUrl := range backend.tasks {
        BackendLogger.Infof("simulate callback %s on task %s \n", callbackUrl,
            taskId)
        data := CallbackData{
            Time:   12345673,
            Status: "TASK_FINISHED",
            TaskID: taskId,
        }
        body, err := json.Marshal(data)
        if err != nil {
            BackendLogger.Infof("simulate callback for %s fail %s \n",
                taskId, err.Error())
        } else {
            req, err := http.NewRequest("POST", callbackUrl, bytes.NewBuffer(body))
            req.Header.Set("Content-Type", "application/json")
            _, err = client.Do(req)
            if err != nil {
                Logger.Println("Submit Job to " + callbackUrl + " fail: " + err.Error())
            }
        }

        delete(backend.tasks, taskId)
    }
}

func (backend *simulatorBackend) SubmitTask(task *ScheduleBackendTask) (error, string) {
    backend.lock.Lock()
    defer backend.lock.Unlock()

    taskId := fmt.Sprintf("simulator-task-%d", backend.taskCount)
    backend.taskCount ++
    backend.tasks[taskId] = task.CallBackURI
    return nil, taskId
}

func (backend *simulatorBackend) CheckTask(taskID string) (error, *TaskStatus) {
    backend.lock.Lock()
    defer backend.lock.Unlock()

	if taskID == "" {
        return errors.New("Empty task id"), nil
	}

    if _, ok := backend.tasks[taskID]; ok {
        return nil, &TaskStatus{
                        State: TASK_RUNNING,
                    }
    }

    return nil, &TaskStatus{
                    State: TASK_FINISHED,
    }
}

func (backend *simulatorBackend) KillTask(taskID string) error {
    return nil
}

func (backend *simulatorBackend) GetID() string {
    return "simulator"
}

func (backend *simulatorBackend) GetType() string {
    return "simulator"
}

func (backend *simulatorBackend) GetServer() string {
    return "localhost"
}

func (backend *simulatorBackend) GetSandboxFile(taskID string, fileName string) ([]byte, error) {
	return nil, errors.New("Not Support")
}

func (backend *simulatorBackend) FetchSandboxFile(taskID string, targetFileName string,
    localFileName string) error {
    return errors.New("Not supported")
}

func(backend *simulatorBackend) ParseTaskNotify(callbackMsg io.ReadCloser) (error, *TaskEvent) {
    decoder := json.NewDecoder(callbackMsg)
    var t CallbackData   
    err := decoder.Decode(&t)
    if err != nil {
        BackendLogger.Infof("Can't parse the request\n")
        return err, nil
    }
    
    BackendLogger.Infof("Received Eremetic Notify %s %s \n", t.TaskID, t.Status)
    Logger.Println(t)
    taskEvent := &TaskEvent{
                    Id: t.TaskID,
                    Info: "",
    }

    switch t.Status {
        case "TASK_FAILED":
            taskEvent.Event = TASK_FAIL
            return nil, taskEvent
        case "TASK_FINISHED":
            taskEvent.Event = TASK_FINISHED
            return nil, taskEvent
        case "TASK_LOST":
            taskEvent.Event = TASK_LOST
            return nil, taskEvent
        case "TASK_ERROR":
            taskEvent.Event = TASK_ERROR
            return nil, taskEvent
        default:
            BackendLogger.Infof("Callback event %s change to %s on time %d \n", 
                t.TaskID, t.Status, t.Time)
            taskEvent.Event = -1
            return nil, taskEvent
    }
}

func (backend *simulatorBackend) DeleteTask(taskID string) error {
    return nil
}

func (backend *simulatorBackend) GetRemovableSlaves() (error, []string) {
    return errors.New("Not Support"), nil
}

func (backend *simulatorBackend) Ping() error {
    return nil
}
