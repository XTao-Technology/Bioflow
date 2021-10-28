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
    "github.com/gorilla/mux"
    "github.com/xtao/bioflow/scheduler"
    . "github.com/xtao/bioflow/common"
    . "github.com/xtao/bioflow/message"
    )

func ListBackend(w http.ResponseWriter, req *http.Request) {
    defer req.Body.Close()

    var result BioflowListBackendResult
    backendMgr := scheduler.GetBackendMgr()

    err, secCtxt := BuildSecurityContext(req)
    if err != nil {
        result.Status = BIOFLOW_API_RET_FAIL
        result.Msg = err.Error()
        writeJSON(500, result, w)
        return
    }

    err = CheckClientAccount(secCtxt)
    if err != nil && !secCtxt.IsPrivileged() {
        result.Status = BIOFLOW_API_RET_FAIL
        result.Msg = err.Error()
        writeJSON(500, result, w)
        return
    }

    err, backends := backendMgr.ListBackend()
    if err != nil {
        ServerLogger.Infof("Can't list backends: %s\n",
            err.Error())
        result.Status = BIOFLOW_API_RET_FAIL
        result.Msg = err.Error()
        writeJSON(500, result, w)
    } else {
        ServerLogger.Debugf("Successfully list backends \n")
        result.Status = BIOFLOW_API_RET_OK
        result.Msg = "successfully list backends"
        result.Backends = backends
        writeJSON(http.StatusAccepted, result, w)
    }
}

func DisableBackend(w http.ResponseWriter, req *http.Request) {
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
    if err != nil && !secCtxt.IsPrivileged() {
        result.Status = BIOFLOW_API_RET_FAIL
        result.Msg = err.Error()
        writeJSON(500, result, w)
        return
    }

	vars := mux.Vars(req)
	backendId := vars["backendId"]

    backendMgr := scheduler.GetBackendMgr()

    err = backendMgr.DisableBackend(backendId)
    if err != nil {
        ServerLogger.Infof("Can't disable backend %s: %s\n",
            backendId, err.Error())
        result.Status = BIOFLOW_API_RET_FAIL
        result.Msg = err.Error()
        writeJSON(500, result, w)
    } else {
        ServerLogger.Infof("Successfully disable backends \n")
        result.Status = BIOFLOW_API_RET_OK
        result.Msg = "successfully disable backends"
        writeJSON(http.StatusAccepted, result, w)
    }
}

func EnableBackend(w http.ResponseWriter, req *http.Request) {
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
    if err != nil && !secCtxt.IsPrivileged() {
        result.Status = BIOFLOW_API_RET_FAIL
        result.Msg = err.Error()
        writeJSON(500, result, w)
        return
    }

	vars := mux.Vars(req)
	backendId := vars["backendId"]

    backendMgr := scheduler.GetBackendMgr()

    err = backendMgr.EnableBackend(backendId)
    if err != nil {
        ServerLogger.Infof("Can't enable backend %s: %s\n",
            backendId, err.Error())
        result.Status = BIOFLOW_API_RET_FAIL
        result.Msg = err.Error()
        writeJSON(500, result, w)
    } else {
        ServerLogger.Infof("Successfully enable backend %s. \n", backendId)
        result.Status = BIOFLOW_API_RET_OK
        result.Msg = "successfully enable backends"
        writeJSON(http.StatusAccepted, result, w)
    }
}


