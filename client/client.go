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
package client

import (
    "path/filepath"
    "crypto/tls"
    "errors"
    "fmt"
    "net/http"
    "os"
    "strings"
    "io/ioutil"
    "bytes"
    . "github.com/xtao/bioflow/common"
    . "github.com/xtao/bioflow/message"
    "encoding/json"
    "github.com/ghodss/yaml"
)

var defaultBioflowServer = "http://localhost:9090"


type BioflowClient struct {
    httpClient *http.Client
    server string
    account *UserAccountInfo
}

func NewBioflowClient(server string, account *UserAccountInfo) *BioflowClient {
    httpClient := &http.Client{}

    if os.Getenv("BIOFLOW_INSECURE") != "" {
        tr := &http.Transport{
            TLSClientConfig: &tls.Config{InsecureSkipVerify: true},
        }
        httpClient.Transport = tr
    }

    return &BioflowClient {
        httpClient: httpClient,
        server: server,
        account: account,
    }
}

func (client *BioflowClient)SetAccount(account *UserAccountInfo) {
    client.account = account
}

func (client *BioflowClient)AddJobFile(fileName string) (error, string) {
    server := client.server

    raw, err := ioutil.ReadFile(fileName)
    if err != nil {
        return err, ""
    }

    url := fmt.Sprintf("http://%s/v1/job/submit", server)
    req, err := http.NewRequest("POST", url, bytes.NewBuffer(raw))
    req.Header.Set("Content-Type", "application/json")

    err = SignClientReqHeader(req, client.account)
    if err != nil {
        return err, ""
    }

    resp, err := client.httpClient.Do(req)
    if err != nil {
        return err, ""
    }
    defer resp.Body.Close()

    decoder := json.NewDecoder(resp.Body)
    var result BioflowSubmitJobResult
    err = decoder.Decode(&result)
    if err != nil {
        return err, ""
    }

    if result.Status == BIOFLOW_API_RET_OK {
        return nil, result.JobId
    } else {
        return errors.New(result.Msg), ""
    }
}

func (client *BioflowClient)AddPipelineFile(fileName string) error {
    server := client.server

    raw, err := ioutil.ReadFile(fileName)
    if err != nil {
        return err
    }
    url := fmt.Sprintf("http://%s/v1/pipeline/add", server)
    req, err := http.NewRequest("POST", url, bytes.NewBuffer(raw))
    req.Header.Set("Content-Type", "application/json")

    err = SignClientReqHeader(req, client.account)
    if err != nil {
        return err
    }

    resp, err := client.httpClient.Do(req)
    if err != nil {
        return err
    }
    defer resp.Body.Close()

    decoder := json.NewDecoder(resp.Body)
    var result BioflowAPIResult
    err = decoder.Decode(&result)
    if err != nil {
        return err
    }
    
    if result.Status == BIOFLOW_API_RET_OK {
        return nil
    } else {
        return errors.New(result.Msg)
    }
}

func (client *BioflowClient)UpdatePipelineFile(fileName string, isForce bool) error {
    var url string
    server := client.server

    raw, err := ioutil.ReadFile(fileName)
    if err != nil {
        return err
    }
    if isForce {
        url = fmt.Sprintf("http://%s/v1/pipeline/forceupdate", server)
    } else {
        url = fmt.Sprintf("http://%s/v1/pipeline/update", server)
    }
    req, err := http.NewRequest("POST", url, bytes.NewBuffer(raw))
    req.Header.Set("Content-Type", "application/json")

    err = SignClientReqHeader(req, client.account)
    if err != nil {
        return err
    }

    resp, err := client.httpClient.Do(req)
    if err != nil {
        return err
    }
    defer resp.Body.Close()

    decoder := json.NewDecoder(resp.Body)
    var result BioflowAPIResult
    err = decoder.Decode(&result)
    if err != nil {
        return err
    }
    
    if result.Status == BIOFLOW_API_RET_OK {
        return nil
    } else {
        return errors.New(result.Msg)
    }
}

func (client *BioflowClient)ClonePipeline(src string, dst string) error {
    server := client.server

    url := fmt.Sprintf("http://%s/v1/pipeline/clone/%s/%s",
        server, src, dst)
    req, err := http.NewRequest("POST", url, nil)

    err = SignClientReqHeader(req, client.account)
    if err != nil {
        return err
    }

    resp, err := client.httpClient.Do(req)
    if err != nil {
        return err
    }
    defer resp.Body.Close()

    decoder := json.NewDecoder(resp.Body)
    var result BioflowAPIResult
    err = decoder.Decode(&result)
    if err != nil {
        return err
    }
    
    if result.Status == BIOFLOW_API_RET_OK {
        return nil
    } else {
        return errors.New(result.Msg)
    }
}

func (client *BioflowClient)DumpPipeline(name string, version string, file string,
    isYaml bool) error {
    server := client.server

    url := fmt.Sprintf("http://%s/v1/pipeline/dump/%s/%s",
        server, name, version)
    req, err := http.NewRequest("GET", url, nil)

    err = SignClientReqHeader(req, client.account)
    if err != nil {
        return err
    }

    resp, err := client.httpClient.Do(req)
    if err != nil {
        return err
    }
    defer resp.Body.Close()

    decoder := json.NewDecoder(resp.Body)
    var result BioflowDumpPipelineResult
    err = decoder.Decode(&result)
    if err != nil {
        return err
    }

    if result.Status == BIOFLOW_API_RET_OK {
        dumpJsonData := result.PipelineJsonData
        /*extract the source code to same directory if have*/
        if len(dumpJsonData.Wdl.Blob) > 0 {
            zipFile := file + ".zipsrc"
            err := ioutil.WriteFile(zipFile, dumpJsonData.Wdl.Blob, os.ModePerm)
            if err != nil {
                return err
            }
            srcDir := filepath.Dir(zipFile) + "/" + name
            if version != "" {
                srcDir += "-" + version
            }
            os.MkdirAll(srcDir, os.ModePerm)
            extractor := NewZipExtractor(srcDir, zipFile)
            err = extractor.Extract()
            if err != nil {
                return err
            }
            os.Remove(zipFile)
            dumpJsonData.Wdl.Blob = nil
        }
        var dumpData []byte
        if isYaml {
            dumpData, err = yaml.Marshal(dumpJsonData)
            if err != nil {
                fmt.Printf("Fail to convert from json to yaml\n")
                return err
            }
        } else {
            dumpData, err = json.MarshalIndent(dumpJsonData, "", "\t")
            if err != nil {
                fmt.Printf("indent  failure\n")
                return err
            }
        }

        f, err := UtilsOpenFile(file)
        if err != nil {
            return err
        }
        err = UtilsWriteFile(f, string(dumpData))
        if err != nil {
            return err
        }
        return nil
    } else {
        return errors.New(result.Msg)
    }
}

func (client *BioflowClient)ListJobs(args map[string]interface{}) (error,
    []BioflowJobInfo, []BioflowJobInfo) {
    server := client.server

    url := fmt.Sprintf("http://%s/v1/job/list", server)

    js, err := json.Marshal(args)
    if err != nil {
        return err, nil, nil
    }

    req, err := http.NewRequest("GET", url, strings.NewReader(string(js)))
    req.Header.Set("Content-Type", "application/json")

    err = SignClientReqHeader(req, client.account)
    if err != nil {
        return err, nil, nil
    }

    resp, err := client.httpClient.Do(req)
    if err != nil {
        return err, nil, nil
    }
    defer resp.Body.Close()

    decoder := json.NewDecoder(resp.Body)
    var result BioflowListJobResult
    err = decoder.Decode(&result)
    if err != nil {
        return err, nil, nil
    }

    if result.Status == BIOFLOW_API_RET_OK {
        return nil, result.Jobs, result.HistJobs
    } else {
        return errors.New(result.Msg), nil, nil
    }
}

func (client *BioflowClient)CleanupJobs(args map[string]interface{}) (error, int) {
    server := client.server

    url := fmt.Sprintf("http://%s/v1/job/cleanup", server)

    js, err := json.Marshal(args)
    if err != nil {
        return err, 0
    }

    req, err := http.NewRequest("POST", url, strings.NewReader(string(js)))
    req.Header.Set("Content-Type", "application/json")

    err = SignClientReqHeader(req, client.account)
    if err != nil {
        return err, 0
    }

    resp, err := client.httpClient.Do(req)
    if err != nil {
        return err, 0
    }
    defer resp.Body.Close()

    decoder := json.NewDecoder(resp.Body)
    var result BioflowCleanupJobResult
    err = decoder.Decode(&result)
    if err != nil {
        return err, 0
    }

    if result.Status == BIOFLOW_API_RET_OK {
        return nil, result.CleanupCount
    } else {
        return errors.New(result.Msg), 0
    }
}

func (client *BioflowClient) GetJobLog(jobId string, stageName string, stageId string) (error,
    map[string]BioflowStageLog) {
    server := client.server

    url := fmt.Sprintf("http://%s/v1/job/logs/%s/%s/%s",
        server, jobId, stageName, stageId)

    req, err := http.NewRequest("GET", url, nil)

    err = SignClientReqHeader(req, client.account)
    if err != nil {
        return err, nil
    }
    resp, err := client.httpClient.Do(req)
    if err != nil {
        return err, nil
    }
    defer resp.Body.Close()

    decoder := json.NewDecoder(resp.Body)
    var result BioflowGetLogResult

    err = decoder.Decode(&result)
    if err != nil {
        return err, nil
    }

    if result.Status == BIOFLOW_API_RET_OK {
        return nil, result.JobLogs
    } else {
        return errors.New(result.Msg), nil
    }
}

func (client *BioflowClient) GetJobResourceUsage(jobId string, taskId string) (error,
    map[string]ResourceUsageInfo) {
    server := client.server

    url := fmt.Sprintf("http://%s/v1/job/resource/%s/%s",
        server, jobId, taskId)

    req, err := http.NewRequest("GET", url, nil)

    err = SignClientReqHeader(req, client.account)
    if err != nil {
        return err, nil
    }
    resp, err := client.httpClient.Do(req)
    if err != nil {
        return err, nil
    }
    defer resp.Body.Close()

    decoder := json.NewDecoder(resp.Body)
    var result BioflowGetResourceUsageResult

    err = decoder.Decode(&result)
    if err != nil {
        return err, nil
    }

    if result.Status == BIOFLOW_API_RET_OK {
        return nil, result.Usage
    } else {
        return errors.New(result.Msg), nil
    }
}

func (client *BioflowClient)GetJobStatus(jobId string) (error, *BioflowJobStatus) {
    server := client.server

    url := fmt.Sprintf("http://%s/v1/job/status/%s", server, jobId)
    req, err := http.NewRequest("GET", url, nil)

    err = SignClientReqHeader(req, client.account)
    if err != nil {
        return err, nil
    }

    resp, err := client.httpClient.Do(req)
    if err != nil {
        return err, nil
    }
    defer resp.Body.Close()

    decoder := json.NewDecoder(resp.Body)
    var result BioflowGetJobStatusResult
    err = decoder.Decode(&result)
    if err != nil {
        return err, nil
    }
    
    if result.Status == BIOFLOW_API_RET_OK {
        return nil, &result.JobStatus
    } else {
        return errors.New(result.Msg), nil
    }
}

func (client *BioflowClient)PauseJob(jobId string) error {
    server := client.server

    url := fmt.Sprintf("http://%s/v1/job/pause/%s",
        server, jobId)
    req, err := http.NewRequest("POST", url, nil)

    err = SignClientReqHeader(req, client.account)
    if err != nil {
        return err
    }

    resp, err := client.httpClient.Do(req)
    if err != nil {
        return err
    }
    defer resp.Body.Close()

    decoder := json.NewDecoder(resp.Body)
    var result BioflowAPIResult
    err = decoder.Decode(&result)
    if err != nil {
        return err
    }
    
    if result.Status == BIOFLOW_API_RET_OK {
        return nil
    } else {
        return errors.New(result.Msg)
    }
}

func (client *BioflowClient)KillTasks(jobId string, taskId string) error {
    server := client.server

    url := fmt.Sprintf("http://%s/v1/job/killtasks/%s/%s",
        server, jobId, taskId)
    req, err := http.NewRequest("POST", url, nil)

    err = SignClientReqHeader(req, client.account)
    if err != nil {
        return err
    }

    resp, err := client.httpClient.Do(req)
    if err != nil {
        return err
    }
    defer resp.Body.Close()

    decoder := json.NewDecoder(resp.Body)
    var result BioflowAPIResult
    err = decoder.Decode(&result)
    if err != nil {
        return err
    }

    if result.Status == BIOFLOW_API_RET_OK {
        return nil
    } else {
        return errors.New(result.Msg)
    }
}

func (client *BioflowClient)ResumeJob(jobId string) error {
    server := client.server

    url := fmt.Sprintf("http://%s/v1/job/resume/%s",
        server, jobId)
    req, err := http.NewRequest("POST", url, nil)

    err = SignClientReqHeader(req, client.account)
    if err != nil {
        return err
    }

    resp, err := client.httpClient.Do(req)
    if err != nil {
        return err
    }
    defer resp.Body.Close()

    decoder := json.NewDecoder(resp.Body)
    var result BioflowAPIResult
    err = decoder.Decode(&result)
    if err != nil {
        return err
    }
    
    if result.Status == BIOFLOW_API_RET_OK {
        return nil
    } else {
        return errors.New(result.Msg)
    }
}

func (client *BioflowClient)RequeueJobStage(jobId string, stageId string,
    newCpu float64, newMemory float64) error {
    server := client.server

    resource := ResourceSpecJSONData{
                Cpu: newCpu,
                Memory: newMemory,
    }

    url := fmt.Sprintf("http://%s/v1/job/requeue/%s/%s",
        server, jobId, stageId)

    js, err := json.Marshal(&resource)
    if err != nil {
        return err
    }

    req, err := http.NewRequest("POST", url, strings.NewReader(string(js)))

    err = SignClientReqHeader(req, client.account)
    if err != nil {
        return err
    }

    resp, err := client.httpClient.Do(req)
    if err != nil {
        return err
    }
    defer resp.Body.Close()

    decoder := json.NewDecoder(resp.Body)
    var result BioflowAPIResult
    err = decoder.Decode(&result)
    if err != nil {
        return err
    }
    
    if result.Status == BIOFLOW_API_RET_OK {
        return nil
    } else {
        return errors.New(result.Msg)
    }
}

func (client *BioflowClient)RecoverJob(jobId string) error {
    server := client.server

    url := fmt.Sprintf("http://%s/v1/job/recover/%s",
        server, jobId)
    req, err := http.NewRequest("POST", url, nil)

    err = SignClientReqHeader(req, client.account)
    if err != nil {
        return err
    }

    resp, err := client.httpClient.Do(req)
    if err != nil {
        return err
    }
    defer resp.Body.Close()

    decoder := json.NewDecoder(resp.Body)
    var result BioflowAPIResult
    err = decoder.Decode(&result)
    if err != nil {
        return err
    }
    
    if result.Status == BIOFLOW_API_RET_OK {
        return nil
    } else {
        return errors.New(result.Msg)
    }
}

func (client *BioflowClient)CancelJob(jobId string) error {
    server := client.server

    url := fmt.Sprintf("http://%s/v1/job/cancel/%s", server, jobId)
    req, err := http.NewRequest("POST", url, nil)

    err = SignClientReqHeader(req, client.account)
    if err != nil {
        return err
    }

    resp, err := client.httpClient.Do(req)
    if err != nil {
        return err
    }
    defer resp.Body.Close()

    decoder := json.NewDecoder(resp.Body)
    var result BioflowAPIResult
    err = decoder.Decode(&result)
    if err != nil {
        return err
    }
    
    if result.Status == BIOFLOW_API_RET_OK {
        return nil
    } else {
        return errors.New(result.Msg)
    }
}

func (client *BioflowClient)ListPipelineAllVersions(pipelineId string) (error, []BioflowPipelineInfo) {
    server := client.server

    url := fmt.Sprintf("http://%s/v1/pipeline/version/%s", server, pipelineId)
    req, err := http.NewRequest("GET", url, nil)

    err = SignClientReqHeader(req, client.account)
    if err != nil {
        return err, nil
    }

    resp, err := client.httpClient.Do(req)
    if err != nil {
        return err, nil
    }
    defer resp.Body.Close()

    decoder := json.NewDecoder(resp.Body)
    var result BioflowListPipelineResult
    err = decoder.Decode(&result)
    if err != nil {
        return err, nil
    }

    if result.Status == BIOFLOW_API_RET_OK {
        return nil, result.Pipelines
    } else {
        return errors.New(result.Msg), nil
    }
}

func (client *BioflowClient)ListPipelines() (error, []BioflowPipelineInfo) {
    server := client.server

    url := fmt.Sprintf("http://%s/v1/pipeline/list", server)
    req, err := http.NewRequest("GET", url, nil)

    err = SignClientReqHeader(req, client.account)
    if err != nil {
        return err, nil
    }

    resp, err := client.httpClient.Do(req)
    if err != nil {
        return err, nil
    }
    defer resp.Body.Close()

    decoder := json.NewDecoder(resp.Body)
    var result BioflowListPipelineResult
    err = decoder.Decode(&result)
    if err != nil {
        return err, nil
    }
    
    if result.Status == BIOFLOW_API_RET_OK {
        return nil, result.Pipelines
    } else {
        return errors.New(result.Msg), nil
    }
}

func (client *BioflowClient)GetPipelineInfo(pipelineId string, version string) (error, *BioflowPipelineInfo) {
    server := client.server

    url := fmt.Sprintf("http://%s/v1/pipeline/info/%s/%s",
        server, pipelineId, version)
    req, err := http.NewRequest("GET", url, nil)

    err = SignClientReqHeader(req, client.account)
    if err != nil {
        return err, nil
    }

    resp, err := client.httpClient.Do(req)
    if err != nil {
        return err, nil
    }
    defer resp.Body.Close()

    decoder := json.NewDecoder(resp.Body)
    var result BioflowGetPipelineResult
    err = decoder.Decode(&result)
    if err != nil {
        return err, nil
    }
    
    if result.Status == BIOFLOW_API_RET_OK {
        return nil, &result.Pipeline
    } else {
        return errors.New(result.Msg), nil
    }
}

func (client *BioflowClient)ListPipelineItems() (error, []BioflowPipelineItemInfo) {
    server := client.server
    url := fmt.Sprintf("http://%s/v1/item/list", server)
    req, err := http.NewRequest("GET", url, nil)

    err = SignClientReqHeader(req, client.account)
    if err != nil {
        return err, nil
    }

    resp, err := client.httpClient.Do(req)
    if err != nil {
        return err, nil
    }
    defer resp.Body.Close()

    decoder := json.NewDecoder(resp.Body)
    var result BioflowListPipelineItemResult
    err = decoder.Decode(&result)
    if err != nil {
        return err, nil
    }
    
    if result.Status == BIOFLOW_API_RET_OK {
        return nil, result.PipelineItems
    } else {
        return errors.New(result.Msg), nil
    }
}

func (client *BioflowClient)GetPipelineItemInfo(pipelineId string) (error, *BioflowPipelineItemInfo) {
    server := client.server

    url := fmt.Sprintf("http://%s/v1/item/info/%s",
        server, pipelineId)
    req, err := http.NewRequest("GET", url, nil)

    err = SignClientReqHeader(req, client.account)
    if err != nil {
        return err, nil
    }

    resp, err := client.httpClient.Do(req)
    if err != nil {
        return err, nil
    }
    defer resp.Body.Close()

    decoder := json.NewDecoder(resp.Body)
    var result BioflowGetPipelineItemResult
    err = decoder.Decode(&result)
    if err != nil {
        return err, nil
    }
    
    if result.Status == BIOFLOW_API_RET_OK {
        return nil, &result.PipelineItem
    } else {
        return errors.New(result.Msg), nil
    }
}

func (client *BioflowClient)AddPipelineItemFile(fileName string) error {
    server := client.server

    raw, err := ioutil.ReadFile(fileName)
    if err != nil {
        return err
    }
    url := fmt.Sprintf("http://%s/v1/item/add", server)
    req, err := http.NewRequest("POST", url, bytes.NewBuffer(raw))
    req.Header.Set("Content-Type", "application/json")

    err = SignClientReqHeader(req, client.account)
    if err != nil {
        return err
    }

    resp, err := client.httpClient.Do(req)
    if err != nil {
        return err
    }
    defer resp.Body.Close()

    decoder := json.NewDecoder(resp.Body)
    var result BioflowAPIResult
    err = decoder.Decode(&result)
    if err != nil {
        return err
    }
    
    if result.Status == BIOFLOW_API_RET_OK {
        return nil
    } else {
        return errors.New(result.Msg)
    }
}

func (client *BioflowClient)UpdatePipelineItemFile(fileName string) error {
    server := client.server

    raw, err := ioutil.ReadFile(fileName)
    if err != nil {
        return err
    }
    url := fmt.Sprintf("http://%s/v1/item/update", server)
    req, err := http.NewRequest("POST", url, bytes.NewBuffer(raw))
    req.Header.Set("Content-Type", "application/json")

    err = SignClientReqHeader(req, client.account)
    if err != nil {
        return err
    }

    resp, err := client.httpClient.Do(req)
    if err != nil {
        return err
    }
    defer resp.Body.Close()

    decoder := json.NewDecoder(resp.Body)
    var result BioflowAPIResult
    err = decoder.Decode(&result)
    if err != nil {
        return err
    }
    
    if result.Status == BIOFLOW_API_RET_OK {
        return nil
    } else {
        return errors.New(result.Msg)
    }
}

func (client *BioflowClient)DeletePipeline(pipelineId string) error {
    server := client.server
    url := fmt.Sprintf("http://%s/v1/pipeline/delete/%s", server, pipelineId)
    req, err := http.NewRequest("POST", url, nil)

    err = SignClientReqHeader(req, client.account)
    if err != nil {
        return err
    }

    resp, err := client.httpClient.Do(req)
    if err != nil {
        return err
    }
    defer resp.Body.Close()

    decoder := json.NewDecoder(resp.Body)
    var result BioflowAPIResult
    err = decoder.Decode(&result)
    if err != nil {
        return err
    }
    
    if result.Status == BIOFLOW_API_RET_OK {
        return nil
    } else {
        return errors.New(result.Msg)
    }
}

func (client *BioflowClient)DeletePipelineItem(pipelineId string) error {
    server := client.server
    url := fmt.Sprintf("http://%s/v1/item/delete/%s", server, pipelineId)
    req, err := http.NewRequest("POST", url, nil)

    err = SignClientReqHeader(req, client.account)
    if err != nil {
        return err
    }

    resp, err := client.httpClient.Do(req)
    if err != nil {
        return err
    }
    defer resp.Body.Close()

    decoder := json.NewDecoder(resp.Body)
    var result BioflowAPIResult
    err = decoder.Decode(&result)
    if err != nil {
        return err
    }
    
    if result.Status == BIOFLOW_API_RET_OK {
        return nil
    } else {
        return errors.New(result.Msg)
    }
}

func (client *BioflowClient)ListBackend() (error, []BioflowBackendInfo) {
    server := client.server
    url := fmt.Sprintf("http://%s/v1/backend/list", server)
    req, err := http.NewRequest("GET", url, nil)

    err = SignClientReqHeader(req, client.account)
    if err != nil {
        return err, nil
    }

    resp, err := client.httpClient.Do(req)
    if err != nil {
        return err, nil
    }
    defer resp.Body.Close()

    decoder := json.NewDecoder(resp.Body)
    var result BioflowListBackendResult
    err = decoder.Decode(&result)
    if err != nil {
        return err, nil
    }

    if result.Status == BIOFLOW_API_RET_OK {
        return nil, result.Backends
    } else {
        return errors.New(result.Msg), nil
    }
}

func (client *BioflowClient)DisableBackend(backendId string) error {
    server := client.server
    url := fmt.Sprintf("http://%s/v1/backend/disable/%s",
        server, backendId)
    req, err := http.NewRequest("POST", url, nil)

    err = SignClientReqHeader(req, client.account)
    if err != nil {
        return err
    }

    resp, err := client.httpClient.Do(req)
    if err != nil {
        return err
    }
    defer resp.Body.Close()

    decoder := json.NewDecoder(resp.Body)
    var result BioflowAPIResult
    err = decoder.Decode(&result)
    if err != nil {
        return err
    }

    if result.Status == BIOFLOW_API_RET_OK {
        return nil
    } else {
        return errors.New(result.Msg)
    }
}

func (client *BioflowClient)EnableBackend(backendId string) error {
    server := client.server
    url := fmt.Sprintf("http://%s/v1/backend/enable/%s",
        server, backendId)
    req, err := http.NewRequest("POST", url, nil)

    err = SignClientReqHeader(req, client.account)
    if err != nil {
        return err
    }

    resp, err := client.httpClient.Do(req)
    if err != nil {
        return err
    }
    defer resp.Body.Close()

    decoder := json.NewDecoder(resp.Body)
    var result BioflowAPIResult
    err = decoder.Decode(&result)
    if err != nil {
        return err
    }

    if result.Status == BIOFLOW_API_RET_OK {
        return nil
    } else {
        return errors.New(result.Msg)
    }
}

func (client *BioflowClient)FaultInjectControl(ctlOpt FaultInjectOpt) error {
    server := client.server

    url := fmt.Sprintf("http://%s/v1/debug/faultinjectcontrol", server)

    js, err := json.Marshal(ctlOpt)
    if err != nil {
        return err
    }

    req, err := http.NewRequest("POST", url, strings.NewReader(string(js)))
    req.Header.Set("Content-Type", "application/json")

    err = SignClientReqHeader(req, client.account)
    if err != nil {
        return err
    }
    resp, err := client.httpClient.Do(req)
    if err != nil {
        return err
    }
    defer resp.Body.Close()

    decoder := json.NewDecoder(resp.Body)
    var result BioflowAPIResult
    err = decoder.Decode(&result)
    if err != nil {
        return err
    }

    if result.Status == BIOFLOW_API_RET_OK {
        return nil
    } else {
        return errors.New(result.Msg)
    }
}

func (client *BioflowClient)GetInjectedFaults() (error, bool, []FaultEvent) {
    server := client.server
    url := fmt.Sprintf("http://%s/v1/debug/showfaultinject", server)
    req, err := http.NewRequest("GET", url, nil)

    err = SignClientReqHeader(req, client.account)
    if err != nil {
        return err, false, nil
    }

    resp, err := client.httpClient.Do(req)
    if err != nil {
        return err, false, nil
    }
    defer resp.Body.Close()

    decoder := json.NewDecoder(resp.Body)

    var result BioflowShowFaultInjectResult
    err = decoder.Decode(&result)
    if err != nil {
        return err, false, nil
    }

    if result.Status == BIOFLOW_API_RET_OK {
        return nil, result.Enabled, result.Events
    } else {
        return errors.New(result.Msg), false, nil
    }
}

func (client *BioflowClient)GetDashBoard() (error, *BioflowDashBoardInfo) {
    server := client.server
    url := fmt.Sprintf("http://%s/v1/dashboard/get", server)
    req, err := http.NewRequest("GET", url, nil)

    err = SignClientReqHeader(req, client.account)
    if err != nil {
        return err, nil
    }

    resp, err := client.httpClient.Do(req)
    if err != nil {
        return err, nil
    }
    defer resp.Body.Close()

    decoder := json.NewDecoder(resp.Body)

    var result BioflowGetDashBoardResult
    err = decoder.Decode(&result)
    if err != nil {
        return err, nil
    }

    if result.Status == BIOFLOW_API_RET_OK {
        return nil, &result.DashBoardInfo
    } else {
        return errors.New(result.Msg), nil
    }
}

func (client *BioflowClient)LoadUsers() error {
    server := client.server
    url := fmt.Sprintf("http://%s/v1/user/load", server)
    req, err := http.NewRequest("POST", url, nil)

    err = SignClientReqHeader(req, client.account)
    if err != nil {
        return err
    }

    resp, err := client.httpClient.Do(req)
    if err != nil {
        return err
    }
    defer resp.Body.Close()

    decoder := json.NewDecoder(resp.Body)
    var result BioflowAPIResult
    err = decoder.Decode(&result)
    if err != nil {
        return err
    }

    if result.Status == BIOFLOW_API_RET_OK {
        return nil
    } else {
        return errors.New(result.Msg)
    }
}

func (client *BioflowClient)GetUserInfo(userId string) (error, *BioflowUserInfo) {
    server := client.server
    url := fmt.Sprintf("http://%s/v1/user/info/%s",
        server, userId)
    req, err := http.NewRequest("GET", url, nil)

    err = SignClientReqHeader(req, client.account)
    if err != nil {
        return err, nil
    }

    resp, err := client.httpClient.Do(req)
    if err != nil {
        return err, nil
    }
    defer resp.Body.Close()

    decoder := json.NewDecoder(resp.Body)
    var result BioflowGetUserInfoResult
    err = decoder.Decode(&result)
    if err != nil {
        return err, nil
    }

    if result.Status == BIOFLOW_API_RET_OK {
        return nil, &result.UserInfo
    } else {
        return errors.New(result.Msg), nil
    }
}

func (client *BioflowClient)ListUserInfo() (error, []BioflowUserInfo) {
    server := client.server
    url := fmt.Sprintf("http://%s/v1/user/list", server)
    req, err := http.NewRequest("GET", url, nil)

    err = SignClientReqHeader(req, client.account)
    if err != nil {
        return err, nil
    }

    resp, err := client.httpClient.Do(req)
    if err != nil {
        return err, nil
    }
    defer resp.Body.Close()

    decoder := json.NewDecoder(resp.Body)

    var result BioflowListUserInfoResult
    err = decoder.Decode(&result)
    if err != nil {
        return err, nil
    }

    if result.Status == BIOFLOW_API_RET_OK {
        return nil, result.UserList
    } else {
        return errors.New(result.Msg), nil
    }
}

func (client *BioflowClient)UpdateJobPriority(jobId string, pri int) error {
    server := client.server

    jobJson := JobJSONData{
            Priority: pri,
    }
    raw, err := json.Marshal(&jobJson)
    if err != nil {
        return err
    }

    url := fmt.Sprintf("http://%s/v1/job/update/%s", server, jobId)
    req, err := http.NewRequest("POST", url, bytes.NewBuffer(raw))
    req.Header.Set("Content-Type", "application/json")

    err = SignClientReqHeader(req, client.account)
    if err != nil {
        return err
    }

    resp, err := client.httpClient.Do(req)
    if err != nil {
        return err
    }
    defer resp.Body.Close()

    decoder := json.NewDecoder(resp.Body)
    var result BioflowAPIResult
    err = decoder.Decode(&result)
    if err != nil {
        return err
    }

    if result.Status == BIOFLOW_API_RET_OK {
        return nil
    } else {
        return errors.New(result.Msg)
    }
}

func (client *BioflowClient)ListHangJobs() (error,
    []BioflowJobInfo) {
    server := client.server

    url := fmt.Sprintf("http://%s/v1/job/listhang", server)

    req, err := http.NewRequest("GET", url, nil)
    req.Header.Set("Content-Type", "application/json")

    err = SignClientReqHeader(req, client.account)
    if err != nil {
        return err, nil
    }

    resp, err := client.httpClient.Do(req)
    if err != nil {
        return err, nil
    }
    defer resp.Body.Close()

    decoder := json.NewDecoder(resp.Body)
    var result BioflowListJobResult
    err = decoder.Decode(&result)
    if err != nil {
        return err, nil
    }

    if result.Status == BIOFLOW_API_RET_OK {
        return nil, result.Jobs
    } else {
        return errors.New(result.Msg), nil
    }
}

func (client *BioflowClient)UpdateUserConfig(config *BioflowUserConfig) error {
    server := client.server
    url := fmt.Sprintf("http://%s/v1/user/updateconfig", server)

    js, err := json.Marshal(config)
    if err != nil {
        return err
    }

    req, err := http.NewRequest("POST", url, strings.NewReader(string(js)))
    if err != nil {
        return err
    }

    err = SignClientReqHeader(req, client.account)
    if err != nil {
        return err
    }

    resp, err := client.httpClient.Do(req)
    if err != nil {
        return err
    }
    defer resp.Body.Close()

    decoder := json.NewDecoder(resp.Body)
    var result BioflowAPIResult
    err = decoder.Decode(&result)
    if err != nil {
        return err
    }

    if result.Status == BIOFLOW_API_RET_OK {
        return nil
    } else {
        return errors.New(result.Msg)
    }
}

func (client *BioflowClient)ListUserRscStats(args map[string]interface{}) (error,
    []UserRscStats) {
    server := client.server

    url := fmt.Sprintf("http://%s/v1/user/listrscstats", server)

    js, err := json.Marshal(args)
    if err != nil {
        return err, nil
    }

    req, err := http.NewRequest("GET", url, strings.NewReader(string(js)))
    req.Header.Set("Content-Type", "application/json")

    err = SignClientReqHeader(req, client.account)
    if err != nil {
        return err, nil
    }

    resp, err := client.httpClient.Do(req)
    if err != nil {
        return err, nil
    }
    defer resp.Body.Close()

    decoder := json.NewDecoder(resp.Body)
    var result BioflowListUserRscStatsResult
    err = decoder.Decode(&result)
    if err != nil {
        return err, nil
    }

    if result.Status == BIOFLOW_API_RET_OK {
        return nil, result.StatsList
    } else {
        return errors.New(result.Msg), nil
    }
}

func (client *BioflowClient)ResetRscStats(args map[string]interface{}) error {
    server := client.server
    url := fmt.Sprintf("http://%s/v1/user/resetrscstats", server)

    js, err := json.Marshal(args)
    if err != nil {
        return err
    }

    req, err := http.NewRequest("POST", url, strings.NewReader(string(js)))
    req.Header.Set("Content-Type", "application/json")

    err = SignClientReqHeader(req, client.account)
    if err != nil {
        return err
    }

    resp, err := client.httpClient.Do(req)
    if err != nil {
        return err
    }
    defer resp.Body.Close()

    decoder := json.NewDecoder(resp.Body)
    var result BioflowAPIResult
    err = decoder.Decode(&result)
    if err != nil {
        return err
    }

    if result.Status == BIOFLOW_API_RET_OK {
        return nil
    } else {
        return errors.New(result.Msg)
    }
}

func (client *BioflowClient)DumpPipelineItem(name string, file string,
    isYaml bool) error {
    server := client.server

    url := fmt.Sprintf("http://%s/v1/item/dump/%s",
        server, name)
    req, err := http.NewRequest("GET", url, nil)

    err = SignClientReqHeader(req, client.account)
    if err != nil {
        return err
    }

    resp, err := client.httpClient.Do(req)
    if err != nil {
        return err
    }
    defer resp.Body.Close()

    decoder := json.NewDecoder(resp.Body)
    var result BioflowDumpPipelineItemResult
    err = decoder.Decode(&result)
    if err != nil {
        return err
    }

    if result.Status == BIOFLOW_API_RET_OK {
        dumpJsonData := result.PipelineItemsJsonData
        var dumpData []byte
        if isYaml {
            dumpData, err = yaml.Marshal(dumpJsonData)
            if err != nil {
                fmt.Printf("Fail to convert from json to yaml\n")
                return err
            }
        } else {
            dumpData, err = json.MarshalIndent(dumpJsonData, "", "\t")
            if err != nil {
                fmt.Printf("indent  failure\n")
                return err
            }
        }

        f, err := UtilsOpenFile(file)
        if err != nil {
            return err
        }
        err = UtilsWriteFile(f, string(dumpData))
        if err != nil {
            return err
        }
        return nil
    } else {
        return errors.New(result.Msg)
    }
}

func (client *BioflowClient)DoDebugOperation(op string) error {
    server := client.server

    url := fmt.Sprintf("http://%s/v1/debug/ops/%s", server, op)
    req, err := http.NewRequest("POST", url, nil)
    req.Header.Set("Content-Type", "application/json")

    err = SignClientReqHeader(req, client.account)
    if err != nil {
        return err
    }

    resp, err := client.httpClient.Do(req)
    if err != nil {
        return err
    }
    defer resp.Body.Close()

    decoder := json.NewDecoder(resp.Body)
    var result BioflowAPIResult
    err = decoder.Decode(&result)
    if err != nil {
        return err
    }

    if result.Status == BIOFLOW_API_RET_OK {
        return nil
    } else {
        return errors.New(result.Msg)
    }
}

