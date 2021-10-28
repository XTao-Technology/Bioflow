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
    )

type Route struct {
    Name        string
    Method      string
    Pattern     string
    HandlerFunc http.HandlerFunc
}

type Routes []Route

func NewRouter() *mux.Router {

    router := mux.NewRouter().StrictSlash(true)
    for _, route := range routes {
        router.Methods(route.Method).
        Path(route.Pattern).
        Name(route.Name).
        Handler(route.HandlerFunc)
    }

    return router
}

var routes = Routes{
    Route{
        "ListJob",
        "GET",
        "/v1/job/list",
        ListJob,
    },
    Route{
        "ListHangJob",
        "GET",
        "/v1/job/listhang",
        ListHangJobs,
    },
    Route{
        "ListPipeline",
        "GET",
        "/v1/pipeline/list",
        ListPipeline,
    },
    Route{
        "ListPipelineItem",
        "GET",
        "/v1/item/list",
        ListPipelineItem,
    },
    Route{
        "GetPipeline",
        "GET",
        "/v1/pipeline/info/{pipelineId}/{version}",
        GetPipelineInfo,
    },
    Route{
        "GetPipeline",
        "GET",
        "/v1/pipeline/version/{pipelineId}",
        GetPipelineAllVersions,
    },
    Route{
        "GetPipelineItem",
        "GET",
        "/v1/item/info/{pipelineItemId}",
        GetPipelineItemInfo,
    },
    Route{
        "DeletePipeline",
        "POST",
        "/v1/pipeline/delete/{pipelineId}",
        DeletePipeline,
    },
    Route{
        "DeletePipelineItem",
        "POST",
        "/v1/item/delete/{pipelineItemId}",
        DeletePipelineItem,
    },
    Route{
        "TaskNotify",
        "POST",
        "/v1/tasknotify/{backendId}",
        TaskNotify,
    },
    Route{
        "AddPipeline",
        "POST",
        "/v1/pipeline/add",
        AddPipeline,
    },
    Route{
        "UpdatePipeline",
        "POST",
        "/v1/pipeline/forceupdate",
        UpdatePipelineForce,
    },
    Route{
        "UpdatePipeline",
        "POST",
        "/v1/pipeline/update",
        UpdatePipeline,
    },
    Route{
        "ClonePipeline",
        "POST",
        "/v1/pipeline/clone/{srcPipelineId}/{dstPipelineId}",
        ClonePipeline,
    },
    Route{
        "DumpPipeline",
        "GET",
        "/v1/pipeline/dump/{pipelineName}/{version}",
        DumpPipeline,
    },
    Route{
        "AddPipelineItems",
        "POST",
        "/v1/item/add",
        AddPipelineItems,
    },
    Route{
        "UpdatePipelineItems",
        "POST",
        "/v1/item/update",
        UpdatePipelineItems,
    },
    Route{
        "DumpPipelineItem",
        "GET",
        "/v1/item/dump/{itemName}",
        DumpPipelineItem,
    },
    Route{
        "SubmitJob",
        "POST",
        "/v1/job/submit",
        SubmitJob,
    },
    Route{
        "GetJobStatus",
        "GET",
        "/v1/job/status/{jobId}",
        GetJobStatus,
    },
    Route{
        "PauseJob",
        "POST",
        "/v1/job/pause/{jobId}",
        PauseJob,
    },
    Route{
        "KillJobTasks",
        "POST",
        "/v1/job/killtasks/{jobId}/{taskId}",
        KillJobTasks,
    },
    Route{
        "UpdateJob",
        "POST",
        "/v1/job/update/{jobId}",
        UpdateJob,
    },
    Route{
        "ResumeJob",
        "POST",
        "/v1/job/resume/{jobId}",
        ResumeJob,
    },
    Route{
        "RecoverJob",
        "POST",
        "/v1/job/recover/{jobId}",
        RecoverFailedJob,
    },
    Route{
        "RequeueJobStage",
        "POST",
        "/v1/job/requeue/{jobId}/{stageId}",
        RequeueJobStage,
    },
    Route{
        "CancelJob",
        "POST",
        "/v1/job/cancel/{jobId}",
        CancelJob,
    },
    Route{
        "GetJobLog",
        "GET",
        "/v1/job/logs/{jobId}/{stageName}/{stageId}",
        GetJobLog,
    },
    Route{
        "GetJobResourceUsage",
        "GET",
        "/v1/job/resource/{jobId}/{taskId}",
        GetJobTaskResourceUsageInfo,
    },
    Route{
        "CleanupJob",
        "POST",
        "/v1/job/cleanup",
        CleanupJobs,
    },
    Route{
        "ListBackend",
        "GET",
        "/v1/backend/list",
        ListBackend,
    },
    Route{
        "DisableBackend",
        "POST",
        "/v1/backend/disable/{backendId}",
        DisableBackend,
    },
    Route{
        "EnableBackend",
        "POST",
        "/v1/backend/enable/{backendId}",
        EnableBackend,
    },
    Route{
        "GetDashBoard",
        "GET",
        "/v1/dashboard/get",
        GetDashBoardInfo,
    },
    Route{
        "LoadUsers",
        "POST",
        "/v1/user/load",
        LoadUsers,
    },
    Route{
        "GetUserInfo",
        "GET",
        "/v1/user/info/{userId}",
        GetUserInfo,
    },
    Route{
        "ListUsers",
        "GET",
        "/v1/user/list",
        ListUserInfo,
    },
    Route{
        "UpdateUserConfig",
        "POST",
        "/v1/user/updateconfig",
        UpdateUserConfig,
    },
	Route{
		"ListUserRscStats",
		"GET",
		"/v1/user/listrscstats",
		ListUserRscStats,
	},
	Route{
		"ListUserRscStats",
		"POST",
		"/v1/user/resetrscstats",
		ResetUserRscStats,
	},
	Route{
		"DoDebugOperation",
		"POST",
		"/v1/debug/ops/{operation}",
		HandleDebugOperation,
	},
    Route{
        "FaultInjectControl",
        "POST",
        "/v1/debug/faultinjectcontrol",
        FaultInjectControl,
    },
    Route{
        "ShowFaultInject",
        "GET",
        "/v1/debug/showfaultinject",
        ShowFaultInject,
    },
}
