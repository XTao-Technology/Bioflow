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
    "fmt"

    "github.com/gorilla/mux"
    "github.com/xtao/bioflow/scheduler"
    . "github.com/xtao/bioflow/common"
    . "github.com/xtao/bioflow/message"
)

func ListPipeline(w http.ResponseWriter, req *http.Request) {
    defer req.Body.Close()

    var result BioflowListPipelineResult
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

    pipelineMgr := scheduler.GetPipelineMgr()
    err, pipelineInfo := pipelineMgr.ListPipelines(secCtxt)
    if err != nil {
        ServerLogger.Infof("Can't list pipelines: %s\n",
            err.Error())
        result.Status = BIOFLOW_API_RET_FAIL
        result.Msg = err.Error()
        writeJSON(500, result, w)
    } else {
        ServerLogger.Debugf("Successfully list pipelines \n")
        result.Status = BIOFLOW_API_RET_OK
        result.Msg = "successfully list pipelines"
        result.Pipelines = pipelineInfo
        writeJSON(http.StatusAccepted, result, w)
    }
}

func GetPipelineAllVersions(w http.ResponseWriter, req *http.Request) {
    defer req.Body.Close()
    vars := mux.Vars(req)
    pipelineId := vars["pipelineId"]

    var result BioflowListPipelineResult
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

    pipelineMgr := scheduler.GetPipelineMgr()
    err, pipelineVersionsInfo := pipelineMgr.ListPipelineAllVersions(secCtxt, pipelineId)
    if err != nil {
        ServerLogger.Infof("Can't list pipelineVersions: %s\n",
            err.Error())
        result.Status = BIOFLOW_API_RET_FAIL
        result.Msg = err.Error()
        writeJSON(500, result, w)
    } else {
        ServerLogger.Debugf("Successfully list pipelineVersions \n")
        result.Status = BIOFLOW_API_RET_OK
        result.Msg = "successfully list pipelineVersions"
        result.Pipelines = pipelineVersionsInfo
        writeJSON(http.StatusAccepted, result, w)
    }
}

func GetPipelineInfo(w http.ResponseWriter, req *http.Request) {
    defer req.Body.Close()
    vars := mux.Vars(req)
    pipelineId := vars["pipelineId"]
    version := vars["version"]

    var result BioflowGetPipelineResult
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

    pipelineMgr := scheduler.GetPipelineMgr()
    err, pipelineInfo := pipelineMgr.GetPipelineInfo(secCtxt, pipelineId,
        version)
    if err != nil {
        ServerLogger.Infof("Can't get pipeline %s/%s info: %s\n",
            pipelineId, version, err.Error())
        result.Status = BIOFLOW_API_RET_FAIL
        result.Msg = err.Error()
        writeJSON(500, result, w)
    } else {
        ServerLogger.Debugf("Successfully get pipeline %s/%s info \n",
            pipelineId, version)
        result.Status = BIOFLOW_API_RET_OK
        result.Msg = "successfully get pipeline info"
        result.Pipeline = *pipelineInfo
        writeJSON(http.StatusAccepted, result, w)
    }
}

func AddPipeline(w http.ResponseWriter, req *http.Request) {
    defer req.Body.Close()

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

    mgr := scheduler.GetPipelineMgr()
    err, pipeline := mgr.NewPipelineFromJSONStream(secCtxt, req.Body)
    if err != nil {
        ServerLogger.Infof("Received invalid pipeline, reject it: %s ",
            err.Error())
        result.Status = BIOFLOW_API_RET_FAIL
        result.Msg = fmt.Sprintf("The format of pipeline is not correct: %s",
            err.Error())
        writeJSON(500, result, w)
    } else {
        err := mgr.AddPipeline(secCtxt, pipeline)
        if err != nil {
            result.Status = BIOFLOW_API_RET_FAIL
            result.Msg = err.Error()
            writeJSON(500, result, w)
        } else {
            ServerLogger.Debugf("pipeline %s added success \n", pipeline.Name())
            result.Status = BIOFLOW_API_RET_OK
            result.Msg = "The pipeline is added success"
            writeJSON(http.StatusAccepted, result, w)
        }
    }
}

func UpdatePipelineForce(w http.ResponseWriter, req *http.Request) {
    defer req.Body.Close()
    var result BioflowAPIResult
    err := _UpdatePipeline(w, req, true)
    if err != nil {
        result.Status = BIOFLOW_API_RET_FAIL
        result.Msg = err.Error()
        writeJSON(500, result, w)
        return
    }
    result.Status = BIOFLOW_API_RET_OK
    result.Msg = "The pipeline is updated success"
    writeJSON(http.StatusAccepted, result, w)
    return
}

func UpdatePipeline(w http.ResponseWriter, req *http.Request) {
    defer req.Body.Close()
    var result BioflowAPIResult
    err := _UpdatePipeline(w, req, false)
    if err != nil {
        result.Status = BIOFLOW_API_RET_FAIL
        result.Msg = err.Error()
        writeJSON(500, result, w)
        return
    }
    result.Status = BIOFLOW_API_RET_OK
    result.Msg = "The pipeline is updated success"
    writeJSON(http.StatusAccepted, result, w)
    return
}

func _UpdatePipeline(w http.ResponseWriter, req *http.Request, isForce bool) error {
    defer req.Body.Close()

    err, secCtxt := BuildSecurityContext(req)
	if err != nil {
        ServerLogger.Errorf("Build security context failed, error: %s",
            err.Error())
	    return err
	}

    err = CheckClientAccount(secCtxt)
    if err != nil {
        ServerLogger.Errorf("Check client account failed, error: %s",
            err.Error())
        return err
    }

    mgr := scheduler.GetPipelineMgr()
    err, pipeline := mgr.NewPipelineFromJSONStream(secCtxt, req.Body)
    if err != nil {
        ServerLogger.Infof("Received invalid pipeline, reject it: %s ",
            err.Error())
        return err
    } else {
        err := mgr.UpdatePipeline(secCtxt, pipeline, isForce)
        if err != nil {
            ServerLogger.Errorf("pipeline %s updated failed",
                pipeline.Name())
            return err
        } else {
            ServerLogger.Infof("pipeline %s updated success",
                pipeline.Name())
            return nil
        }
    }
}

func ClonePipeline(w http.ResponseWriter, req *http.Request) {
    defer req.Body.Close()
    vars := mux.Vars(req)
    srcPipelineId := vars["srcPipelineId"]
    dstPipelineId := vars["dstPipelineId"]

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

    pipelineMgr := scheduler.GetPipelineMgr()
    err = pipelineMgr.ClonePipeline(secCtxt, srcPipelineId,
        dstPipelineId)
    if err != nil {
        ServerLogger.Infof("Can't clone pipeline %s to %s : %s\n",
            srcPipelineId, dstPipelineId, err.Error())
        result.Status = BIOFLOW_API_RET_FAIL
        result.Msg = err.Error()
        writeJSON(500, result, w)
    } else {
        ServerLogger.Infof("Successfully clone pipeline %s to %s \n",
            srcPipelineId, dstPipelineId)
        result.Status = BIOFLOW_API_RET_OK
        result.Msg = "successfully cloned pipeline"
        writeJSON(http.StatusAccepted, result, w)
    }
}

func DumpPipeline(w http.ResponseWriter, req *http.Request) {
    defer req.Body.Close()
    vars := mux.Vars(req)
    pipelineName := vars["pipelineName"]
    pipelineVersion := vars["version"]

    SchedulerLogger.Debugf("Start dump pipeline %s/%s\n", pipelineName,
        pipelineVersion)
    var result BioflowDumpPipelineResult
    err, secCtxt := BuildSecurityContext(req)
    if err != nil {
        ServerLogger.Errorf("Build security context failed, error: %s\n",
            err.Error())
        result.Status = BIOFLOW_API_RET_FAIL
        result.Msg = err.Error()
        writeJSON(500, result, w)
        return
    }

    err = CheckClientAccount(secCtxt)
    if err != nil {
        ServerLogger.Errorf("Check client account failed, error: %s\n",
            err.Error())
        result.Status = BIOFLOW_API_RET_FAIL
        result.Msg = err.Error()
        writeJSON(500, result, w)
        return
    }

    mgr := scheduler.GetPipelineMgr()
    err, PipelineJsonData := mgr.DumpBIOPipelineToJSONFile(secCtxt, pipelineName,
        pipelineVersion)
    if err != nil {
        ServerLogger.Errorf("pipeline %s dumped failed: %s\n",
            pipelineName, err.Error())
        result.Status = BIOFLOW_API_RET_FAIL
        result.Msg = err.Error()
        writeJSON(500, result, w)
        return
    } else {
        ServerLogger.Infof("pipeline %s dumped success\n",
            pipelineName)
        result.Status = BIOFLOW_API_RET_OK
        result.Msg = "Successfully dumped pipeline"
        result.PipelineJsonData = *PipelineJsonData
        writeJSON(http.StatusAccepted, result, w)
        return
    }
}

func DeletePipeline(w http.ResponseWriter, req *http.Request) {
    defer req.Body.Close()
    vars := mux.Vars(req)
    pipelineId := vars["pipelineId"]

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

    pipelineMgr := scheduler.GetPipelineMgr()
    err = pipelineMgr.DeletePipeline(secCtxt, pipelineId)
    if err != nil {
        ServerLogger.Infof("Can't delete pipeline %s : %s\n",
            pipelineId, err.Error())
        result.Status = BIOFLOW_API_RET_FAIL
        result.Msg = err.Error()
        writeJSON(500, result, w)
    } else {
        ServerLogger.Infof("Successfully delete pipeline %s \n", pipelineId)
        result.Status = BIOFLOW_API_RET_OK
        result.Msg = "successfully delete pipeline"
        writeJSON(http.StatusAccepted, result, w)
    }
}

func AddPipelineItems(w http.ResponseWriter, req *http.Request) {
    defer req.Body.Close()
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

    mgr := scheduler.GetPipelineMgr()
    err, pipelineItems := mgr.NewPipelineItemsFromJSONStream(secCtxt, req.Body)
    if err != nil {
        ServerLogger.Infof("Received invalid pipeline item:%s\n",
            err.Error())
        result.Status = BIOFLOW_API_RET_FAIL
        result.Msg = fmt.Sprintf("The format of pipeline item is not correct: %s",
            err.Error())
        writeJSON(500, result, w)
    } else {
        if len(pipelineItems) == 0 {
            ServerLogger.Infof("There is no valid pipeline items found\n")
            result.Status = BIOFLOW_API_RET_FAIL
            result.Msg = "There is no valid pipeline items be found, please check your items"
            writeJSON(http.StatusAccepted, result, w)
            return
        }
        err := mgr.AddPipelineItems(secCtxt, pipelineItems)
        if err != nil {
            result.Status = BIOFLOW_API_RET_FAIL
            result.Msg = err.Error()
            writeJSON(500, result, w)
        } else {
            ServerLogger.Infof("%d pipeline items added success\n",
                len(pipelineItems))
            result.Status = BIOFLOW_API_RET_OK
            result.Msg = "The pipeline item is added success"
            writeJSON(http.StatusAccepted, result, w)
        }
    }
    return
}

func ListPipelineItem(w http.ResponseWriter, req *http.Request) {
    defer req.Body.Close()
    var result BioflowListPipelineItemResult
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

    pipelineMgr := scheduler.GetPipelineMgr()
    err, pipelineItemInfo := pipelineMgr.ListPipelineItems(secCtxt)
    if err != nil {
        ServerLogger.Infof("Can't list pipeline items: %s\n",
            err.Error())
        result.Status = BIOFLOW_API_RET_FAIL
        result.Msg = err.Error()
        writeJSON(500, result, w)
    } else {
        ServerLogger.Debugf("Successfully list pipeline items \n")
        result.Status = BIOFLOW_API_RET_OK
        result.Msg = "successfully list pipeline items"
        result.PipelineItems = pipelineItemInfo
        writeJSON(http.StatusAccepted, result, w)
    }
}

func GetPipelineItemInfo(w http.ResponseWriter, req *http.Request) {
    defer req.Body.Close()
    vars := mux.Vars(req)
    pipelineId := vars["pipelineItemId"]
    var result BioflowGetPipelineItemResult
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

    pipelineMgr := scheduler.GetPipelineMgr()
    err, pipelineInfo := pipelineMgr.GetPipelineItemInfo(secCtxt, pipelineId)
    if err != nil {
        ServerLogger.Infof("Can't get pipeline item %s info: %s\n",
            pipelineId, err.Error())
        result.Status = BIOFLOW_API_RET_FAIL
        result.Msg = err.Error()
        writeJSON(500, result, w)
    } else {
        ServerLogger.Debugf("Successfully get pipeline item %s info \n",
            pipelineId)
        result.Status = BIOFLOW_API_RET_OK
        result.Msg = "successfully get pipeline item info"
        result.PipelineItem = *pipelineInfo
        writeJSON(http.StatusAccepted, result, w)
    }
}

func DeletePipelineItem(w http.ResponseWriter, req *http.Request) {
    defer req.Body.Close()
    vars := mux.Vars(req)
    pipelineId := vars["pipelineItemId"]
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

    pipelineMgr := scheduler.GetPipelineMgr()
    err = pipelineMgr.DeletePipelineItem(secCtxt, pipelineId)
    if err != nil {
        ServerLogger.Infof("Can't delete pipeline item %s : %s\n",
            pipelineId, err.Error())
        result.Status = BIOFLOW_API_RET_FAIL
        result.Msg = err.Error()
        writeJSON(500, result, w)
    } else {
        ServerLogger.Infof("Successfully delete pipeline item %s \n", pipelineId)
        result.Status = BIOFLOW_API_RET_OK
        result.Msg = "successfully delete pipeline item"
        writeJSON(http.StatusAccepted, result, w)
    }
}

func UpdatePipelineItems(w http.ResponseWriter, req *http.Request) {
    defer req.Body.Close()
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

    mgr := scheduler.GetPipelineMgr()
    err, pipelineItems := mgr.NewPipelineItemsFromJSONStream(secCtxt, req.Body)
    if err != nil {
        ServerLogger.Infof("Received invalid pipeline item:%s\n",
            err.Error())
        result.Status = BIOFLOW_API_RET_FAIL
        result.Msg = fmt.Sprintf("The format of pipeline item is not correct: %s",
            err.Error())
        writeJSON(500, result, w)
    } else {
        err := mgr.UpdatePipelineItems(secCtxt, pipelineItems)
        if err != nil {
            result.Status = BIOFLOW_API_RET_FAIL
            result.Msg = err.Error()
            writeJSON(500, result, w)
        } else {
            ServerLogger.Infof("%d pipeline items updated success\n",
                len(pipelineItems))
            result.Status = BIOFLOW_API_RET_OK
            result.Msg = "The pipeline item updated success"
            writeJSON(http.StatusAccepted, result, w)
        }
    }
}

func DumpPipelineItem(w http.ResponseWriter, req *http.Request) {
    defer req.Body.Close()
    vars := mux.Vars(req)
    itemName := vars["itemName"]

    SchedulerLogger.Debugf("Start dump pipeline item %s\n", itemName)
    var result BioflowDumpPipelineItemResult
    err, secCtxt := BuildSecurityContext(req)
    if err != nil {
        ServerLogger.Errorf("Build security context failed, error: %s\n",
            err.Error())
        result.Status = BIOFLOW_API_RET_FAIL
        result.Msg = err.Error()
        writeJSON(500, result, w)
        return
    }

    err = CheckClientAccount(secCtxt)
    if err != nil {
        ServerLogger.Errorf("Check client account failed, error: %s\n",
            err.Error())
        result.Status = BIOFLOW_API_RET_FAIL
        result.Msg = err.Error()
        writeJSON(500, result, w)
        return
    }

    mgr := scheduler.GetPipelineMgr()
    err, PipelineItemsJsonData := mgr.DumpPipelineItemToJSON(secCtxt, itemName)
    if err != nil {
        ServerLogger.Errorf("pipeline item %s dumped failed: %s\n",
            itemName, err.Error())
        result.Status = BIOFLOW_API_RET_FAIL
        result.Msg = err.Error()
        writeJSON(500, result, w)
        return
    } else {
        ServerLogger.Infof("pipeline item %s dumped success\n",
            itemName)
        result.Status = BIOFLOW_API_RET_OK
        result.Msg = "Successfully dumped pipeline item"
        result.PipelineItemsJsonData = *PipelineItemsJsonData
        writeJSON(http.StatusAccepted, result, w)
        return
    }
}
