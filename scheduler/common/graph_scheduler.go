/* 
 Copyright (c) 2018 XTAO technology <www.xtaotech.com>
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
package common

import (
    . "github.com/xtao/bioflow/message"
)

type GraphEvent int

func (event GraphEvent)String() string {
    switch event {
        case GRAPH_EVENT_SUBMITTED:
            return "SUBMITTED"
        case GRAPH_EVENT_FAIL:
            return "FAIL"
        case GRAPH_EVENT_LOST:
            return "LOST"
        case GRAPH_EVENT_FORBIDDEN:
            return "FORBIDDEN"
        case GRAPH_EVENT_DONE:
            return "DONE"
        case GRAPH_EVENT_RUNNING:
            return "RUNNING"
        case GRAPH_EVENT_QUEUED:
            return "QUEUED"
        case GRAPH_EVENT_SUBMIT_FAIL:
            return "SUBMIT_FAIL"
        default:
            return "INVALID"
    }
}

const (
    GRAPH_EVENT_INVALID GraphEvent = 0
    GRAPH_EVENT_SUBMITTED GraphEvent = 1
    GRAPH_EVENT_FAIL GraphEvent = 2
    GRAPH_EVENT_LOST GraphEvent = 3
    GRAPH_EVENT_FORBIDDEN GraphEvent = 4
    GRAPH_EVENT_DONE GraphEvent = 5
    GRAPH_EVENT_RUNNING GraphEvent = 6
    GRAPH_EVENT_QUEUED GraphEvent = 7
    GRAPH_EVENT_SUBMIT_FAIL GraphEvent = 8
)

type GraphScheduler interface {
    GetStageByID(stageId string) Stage
    BuildGraph() (error, *BuildStageErrorInfo)
    GetGraphInfo() (error, FlowGraphInfo)
    RestoreGraph(info FlowGraphInfo) (error, bool, *BuildStageErrorInfo)
    GraphCompleted() bool
    Schedule() (map[string]Stage, int, error)
    CheckGraphState() int
    HandleGraphEvent(stageId string, event GraphEvent) error
    StageCount() int
    GetWaitingStages() ([]Stage, []Stage)
    Fini() error
    GetOutput() (*JobOutput, error)
}
