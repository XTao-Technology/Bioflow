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
	"bytes"
	"fmt"
    . "github.com/xtao/bioflow/common"
)

type k8sClient struct {
    k8sServer string
    httpClient *http.Client
}

func (client *k8sClient) AddTask (request *k8sRequest) (string, error) {
	r, _ := json.Marshal(request)

	url := fmt.Sprintf("%s/t/st", client.k8sServer)
	req, err := http.NewRequest("POST", url, bytes.NewBuffer(r))
	if err != nil {
		BackendLogger.Errorf("New http request for AddTask failed!\n")
		Logger.Println(err)
		return "", err
	}

    req.Header.Set("Content-Type", "application/json; charset=UTF-8")
	resp, err := client.httpClient.Do(req)
	if err != nil {
		BackendLogger.Errorf("Failed to do http request for AddTask!\n")
		Logger.Println(err)
		return "", err
	}
    defer resp.Body.Close()

    decoder := json.NewDecoder(resp.Body)

	var dat map[string]string

	err = decoder.Decode(&dat)
	if err != nil {
		BackendLogger.Errorf("Failed to parse JSON response for AddTask!\n")
		Logger.Println(err)
		return "", err
	}

	Logger.Println(dat)

	return dat["taskId"], nil
}

func (client *k8sClient) KillTask(taskId string) error {

	args := make(map[string]string)
	args["taskId"] = taskId
	r, _ := json.Marshal(args)

	url := fmt.Sprintf("%s/t/kt", client.k8sServer)

	req, err := http.NewRequest("POST", url, bytes.NewBuffer(r))
	if err != nil {
		BackendLogger.Errorf("New http request for KillTask failed!\n")
		Logger.Println(err)
		return err
	}
    req.Header.Set("Content-Type", "application/json; charset=UTF-8")
	resp, err := client.httpClient.Do(req)
	if err != nil {
		BackendLogger.Errorf("Failed to do http request for KillTask!\n")
		Logger.Println(err)
		return err
	}
    defer resp.Body.Close()

    decoder := json.NewDecoder(resp.Body)

	var dat map[string]bool

	err = decoder.Decode(&dat)

	if err != nil {
		BackendLogger.Errorf("Failed to parse JSON response for KillTask!\n")
		Logger.Println(err)
		return err
	}

	if dat["result"] != true {
		errmsg := fmt.Sprintf("Failed to kill task %s !\n", taskId)
		BackendLogger.Errorf(errmsg)
		return errors.New(errmsg)
	} else {
		return nil
	}
}

func (client *k8sClient) GetErrLog(taskId string) ([]byte, error) {
	url := fmt.Sprintf("%s/t/logs/%s", client.k8sServer, taskId)
	req, err := http.NewRequest("GET", url, nil)
	if err != nil {
		BackendLogger.Errorf("New http request for GetErrLog failed!\n")
		Logger.Println(err)
		return nil, err
	}
    req.Header.Set("Content-Type", "application/json; charset=UTF-8")
	resp, err := client.httpClient.Do(req)
	if err != nil {
		BackendLogger.Errorf("Failed to do http request!\n")
		Logger.Println(err)
		return nil, err
	}
    defer resp.Body.Close()

    decoder := json.NewDecoder(resp.Body)

	var dat map[string]interface{}

	err = decoder.Decode(&dat)

	if err != nil {
		BackendLogger.Errorf("Failed to parse JSON response for GetErrLog!\n")
		Logger.Println(err)
		return nil, err
	}

	Logger.Println(dat)

	logs := []byte(dat["logs"].(string))

	return logs, nil
}

func (client *k8sClient) GetTaskStatus(taskId string) (int, error) {
	url := fmt.Sprintf("%s/t/ct/%s", client.k8sServer, taskId)
	req, err := http.NewRequest("GET", url, nil)
	if err != nil {
		BackendLogger.Errorf("New http request for GetTaskStatus failed!\n")
		Logger.Println(err)
		return -1, err
	}
    req.Header.Set("Content-Type", "application/json; charset=UTF-8")
	resp, err := client.httpClient.Do(req)
	if err != nil {
		BackendLogger.Errorf("Failed to do http request for GetTaskStatus %s!\n", taskId)
		Logger.Println(err)
		return -1, err
	}
    defer resp.Body.Close()

    decoder := json.NewDecoder(resp.Body)

	var dat map[string]int

	err = decoder.Decode(&dat)

	if err != nil {
		BackendLogger.Errorf("Failed to parse JSON response for GetTaskStatus %s!\n", taskId)
		Logger.Println(err)
		return -1, err
	}

	status := dat["status"]
	return status, nil
}

type k8sBackend struct {
	id string
    backendType    string
	client *k8sClient
}

type k8sVolume struct {
	VolName string             `json:"name"`
	ContainerPath string       `json:"containerPath"`
	HostPath string            `json:"hostPath"`
}

type k8sRequest struct {
	Command string             `json:"command"`
	DockerImage string         `json:"imageName"`
	TaskCPUs string            `json:"taskCpu"`
	TaskMem string             `json:"taskMem"`
	NameSpace string           `json:"namespace"`
	CallbackURI string         `json:"callbackURI"`
	Volumes []k8sVolume        `json:"volumes"`
	NodeSelector map[string]string `json:"nodeSelector"`
}

func NewK8sBackend(id string, server string) ScheduleBackend {
	client := &k8sClient {
		k8sServer: server,
	}

    backend := &k8sBackend{
        client: client,
        backendType: "kubernetes",
    }

    backend.id = id
    return backend
}

func (backend *k8sBackend) Start() int {
	BackendLogger.Infof("Start Kubernetes backend!\n")

    backend.client.httpClient = &http.Client{}
	if os.Getenv("BIOFLOW_INSECURE") != "" {
		tr := &http.Transport{
			TLSClientConfig: &tls.Config{InsecureSkipVerify: true},
		}
		backend.client.httpClient.Transport = tr
	}

    return 0
}

func (backend *k8sBackend) SubmitTask(task *ScheduleBackendTask) (error, string) {
    volumes := make([]k8sVolume, 0, len(task.VolumesMap))
    for conPath, hostPath := range task.VolumesMap {
        vol := k8sVolume {
                ContainerPath: conPath,
                HostPath: hostPath,
		}
        volumes = append(volumes, vol)
    }

	cpu := fmt.Sprintf("%f",task.CPU)
	mem := fmt.Sprintf("%f", task.Memory / 1000)

	// for debug only

	nodeSelector := make(map[string]string)
	nodeSelector["zone"] = "tenant"

	r := &k8sRequest{
		Command:     task.Cmd,
		DockerImage: task.Image,
		TaskCPUs:    cpu,
		TaskMem:     mem,
		NameSpace:   "default",
		CallbackURI: task.CallBackURI,
		Volumes: volumes,
//		NodeSelector: task.ScheduleConstraints,
		NodeSelector: nodeSelector,
	}

    taskId, err := backend.client.AddTask(r)
	if err != nil {
        return errors.New("fail to add Task"), ""
	} 

	return nil, taskId
}

func (backend *k8sBackend) GetSandboxFile(taskID string, fileName string) ([]byte, error) {
    /*TODO: Get the real log stdout*/
    if fileName != SANDBOX_STDERR {
        return nil, errors.New("Now the kubernettes backend not support get others logs except error")
    }
	
    b, err := backend.client.GetErrLog(taskID)
	if err != nil {
		return nil, errors.New("Fail to get error log")
	}
	return b, err
}

func (backend *k8sBackend) FetchSandboxFile(taskID string, targetFileName string,
    localFileName string) error {
    return errors.New("Not supported")
}

func (backend *k8sBackend) CheckTask(taskID string) (error, *TaskStatus) {
    taskStatus := &TaskStatus{
                    State: -1,
    }

	if taskID == "" {
        return errors.New("empty task id"),
            taskStatus
	}

	status, err := backend.client.GetTaskStatus(taskID)
	if err != nil {
        return err, taskStatus
	}

	if status == 0 {
		errmsg := fmt.Sprintf("task %s does not exist", taskID)
		BackendLogger.Errorf(errmsg)
		return errors.New(errmsg), taskStatus
	}

    if err == nil {
        switch status {
            case TASK_FINISHED:
                BackendLogger.Infof("task %s finished \n", taskID)
            case TASK_FAIL:
                BackendLogger.Infof("task %s failed \n", taskID)
            case TASK_KILLED:
                BackendLogger.Infof("task %s killed \n", taskID)
            case TASK_ERROR:
                BackendLogger.Infof("task %s error \n", taskID)
            case TASK_LOST:
                BackendLogger.Infof("task %s lost \n", taskID)
            case TASK_RUNNING:
                BackendLogger.Infof("task %s running \n", taskID)
            case TASK_QUEUED:
                BackendLogger.Infof("task %s queued \n", taskID)
            default:
                BackendLogger.Infof("task %s state unknown %d \n",
                    taskID, status) 
        }
        taskStatus.State = status
    } else {
            BackendLogger.Errorf("Check task %s fail \n", taskID)
            return errors.New("Check task fail"), taskStatus
    }
    
    return nil, taskStatus
}

func (backend *k8sBackend) KillTask(taskID string) error {
    err := backend.client.KillTask(taskID)
    if err != nil {
        BackendLogger.Errorf("k8s backend kill task %s fail: %s\n",
            taskID, err.Error())
    }

    return nil
}

func (backend *k8sBackend) GetID() string {
    return backend.id
}

func (backend *k8sBackend) GetType() string {
    return backend.backendType
}

func (backend *k8sBackend) GetServer() string {
    return backend.client.k8sServer
}

type k8sCallbackData struct {
    Time   int64  `json:"time"`
    Status int `json:"status"`
    TaskID string `json:"taskId"`
}

func(backend *k8sBackend) ParseTaskNotify(callbackMsg io.ReadCloser) (error, *TaskEvent) {
    decoder := json.NewDecoder(callbackMsg)
    var t k8sCallbackData
    err := decoder.Decode(&t)
    if err != nil {
        BackendLogger.Errorf("Can't parse the request\n")
        return err, nil
    }
    
    BackendLogger.Infof("Received K8s Notify %s %d \n", t.TaskID, t.Status)
    Logger.Println(t)
    taskEvent := &TaskEvent{
                    Id: t.TaskID,
                    Event: -1,
                    Info: "",
    }

    switch t.Status {
        case TASK_FAIL:
            taskEvent.Event = TASK_FAIL
            return nil, taskEvent
        case TASK_FINISHED:
            taskEvent.Event = TASK_FINISHED
            return nil, taskEvent
        case TASK_LOST:
            taskEvent.Event = TASK_LOST
            return nil, taskEvent
        case TASK_ERROR:
            taskEvent.Event = TASK_ERROR
            return nil, taskEvent
        default:
            BackendLogger.Infof("Callback event %s change to %d on time %d \n", 
                t.TaskID, t.Status, t.Time)
            return nil, taskEvent
    }
}

func (backend *k8sBackend) DeleteTask(taskID string) error {
    return errors.New("kubernettes don't support delete op")
}

func (backend *k8sBackend) GetRemovableSlaves() (error, []string) {
	return errors.New("Not Support"), nil
}

func (backend *k8sBackend) Ping() error {
    return errors.New("Not Support")
}
