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
    "encoding/json"

    "github.com/gorilla/mux"
    "github.com/xtao/bioflow/scheduler"
    . "github.com/xtao/bioflow/common"
    . "github.com/xtao/bioflow/message"
    )

func LoadUsers(w http.ResponseWriter, req *http.Request) {
    defer req.Body.Close()

    var result BioflowAPIResult
    userMgr := scheduler.GetUserMgr()

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

    /*by default, load users from same ETCD server*/
    err = userMgr.LoadExternalUsers(1)
    if err != nil {
        ServerLogger.Infof("Can't load external users: %s\n",
            err.Error())
        result.Status = BIOFLOW_API_RET_FAIL
        result.Msg = err.Error()
        writeJSON(500, result, w)
    } else {
        ServerLogger.Infof("Successfully load users \n")
        result.Status = BIOFLOW_API_RET_OK
        result.Msg = "successfully load users"
        writeJSON(http.StatusAccepted, result, w)
    }
}

func GetUserInfo(w http.ResponseWriter, req *http.Request) {
    defer req.Body.Close()
	vars := mux.Vars(req)
	user := vars["userId"]

    var result BioflowGetUserInfoResult
    userMgr := scheduler.GetUserMgr()

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

    err, userInfo := userMgr.GetBioflowUserInfo(secCtxt, user)
    if err != nil {
        ServerLogger.Infof("Can't get user %s info: %s\n",
            user, err.Error())
        result.Status = BIOFLOW_API_RET_FAIL
        result.Msg = err.Error()
        writeJSON(500, result, w)
    } else {
        result.Status = BIOFLOW_API_RET_OK
        result.Msg = "successfully get user info"
        result.UserInfo = *userInfo
        writeJSON(http.StatusAccepted, result, w)
    }
}

func ListUserInfo(w http.ResponseWriter, req *http.Request) {
    defer req.Body.Close()

    var result BioflowListUserInfoResult
    userMgr := scheduler.GetUserMgr()

    err, secCtxt := BuildSecurityContext(req)
	if err != nil {
		ServerLogger.Infof("Cannot build security context :%s\n", err.Error())
        result.Status = BIOFLOW_API_RET_FAIL
        result.Msg = err.Error()
        writeJSON(500, result, w)
        return
	}

    err = CheckClientAccount(secCtxt)
    if err != nil {
        result.Status = BIOFLOW_API_RET_FAIL
        result.Msg = err.Error()
		ServerLogger.Infof("Check client accoutn failure :%s\n", err.Error())
        writeJSON(500, result, w)
        return
    }

    err, userList := userMgr.ListBioflowUsers(secCtxt)
    if err != nil {
        ServerLogger.Infof("Can't list user info: %s\n",
            err.Error())
        result.Status = BIOFLOW_API_RET_FAIL
        result.Msg = err.Error()
		ServerLogger.Infof("Failed to list user :%s\n", err.Error())
        writeJSON(500, result, w)
    } else {
        result.Status = BIOFLOW_API_RET_OK
        result.Msg = "successfully list user info"
        result.UserList = userList
        writeJSON(http.StatusAccepted, result, w)
    }
}

func UpdateUserConfig(w http.ResponseWriter, req *http.Request) {
    decoder := json.NewDecoder(req.Body)
    defer req.Body.Close()

    var result BioflowAPIResult
    userMgr := scheduler.GetUserMgr()

    var config BioflowUserConfig
    err := decoder.Decode(&config)
    if err != nil {
        Logger.Println("Can't parse the request " + err.Error())
        result.Status = BIOFLOW_API_RET_FAIL
        result.Msg = err.Error()
        writeJSON(500, result, w)
        return
    }

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

    err = userMgr.UpdateUserCreditQuota(secCtxt, &config)
    if err != nil {
        ServerLogger.Infof("Can't update user config: %s\n",
            err.Error())
        result.Status = BIOFLOW_API_RET_FAIL
        result.Msg = err.Error()
        writeJSON(500, result, w)
    } else {
        ServerLogger.Infof("Successfully update user config\n")
        result.Status = BIOFLOW_API_RET_OK
        result.Msg = "successfully update user config"
        writeJSON(http.StatusAccepted, result, w)
    }
}

func ListUserRscStats(w http.ResponseWriter, req *http.Request) {
    decoder := json.NewDecoder(req.Body)
    defer req.Body.Close()

    var result BioflowListUserRscStatsResult
    userMgr := scheduler.GetUserMgr()

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

    var listOpt UserRscStatsOpt
    err = decoder.Decode(&listOpt)
    if err != nil {
        Logger.Println("Can't parse the request " + err.Error())
        result.Status = BIOFLOW_API_RET_FAIL
        result.Msg = err.Error()
        writeJSON(500, result, w)
        return
    }

    err, userRscStats := userMgr.GetRscStats(secCtxt, &listOpt)
    if err != nil {
        ServerLogger.Infof("Can't list user rsc stats: %s\n",
            err.Error())
        result.Status = BIOFLOW_API_RET_FAIL
        result.Msg = err.Error()
        writeJSON(500, result, w)
    } else {
        result.Status = BIOFLOW_API_RET_OK
        result.Msg = "successfully list user rsc stats"
        result.StatsList = userRscStats
        writeJSON(http.StatusAccepted, result, w)
    }
}

func ResetUserRscStats(w http.ResponseWriter, req *http.Request) {
    decoder := json.NewDecoder(req.Body)
    defer req.Body.Close()

    var result BioflowAPIResult
    userMgr := scheduler.GetUserMgr()

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

    var resetOpt UserRscStatsOpt
    err = decoder.Decode(&resetOpt)
    if err != nil {
        Logger.Println("Can't parse the request " + err.Error())
        result.Status = BIOFLOW_API_RET_FAIL
        result.Msg = err.Error()
        writeJSON(500, result, w)
        return
    }

    err = userMgr.ResetRscStats(secCtxt, &resetOpt)
    if err != nil {
        ServerLogger.Infof("Can't rest user rsc stats: %s\n",
            err.Error())
        result.Status = BIOFLOW_API_RET_FAIL
        result.Msg = err.Error()
        writeJSON(500, result, w)
    } else {
        result.Status = BIOFLOW_API_RET_OK
        result.Msg = "successfully rest user rsc stats"
        writeJSON(http.StatusAccepted, result, w)
    }
}
