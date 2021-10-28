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

package message

import (
    "fmt"
)

var STRPOSITIONPATTERN int = 10


const (
    BIOSTAGE_FAILREASON_GRAMISTAKE int = 0  /*Grammar mistakes*/
    BIOSTAGE_FAILREASON_INPUTMISTAKE int = 1
    BIOSTAGE_FAILREASON_OUTPUTMISTAKE int = 2
    BIOSTAGE_FAILREASON_SHARDFILEMISTAKE int = 3
    BIOSTAGE_FAILREASON_INPUTDIRTAGMISTAKE int = 4
    BIOSTAGE_FAILREASON_VARIABLEPARALLELMISTAKE int = 5
    BIOSTAGE_FAILREASON_CLONEPARALLELMISTAKE int = 6
    BIOSTAGE_FAILREASON_SUBPIPELINEMISTAKE int = 7
    BIOSTAGE_FAILREASON_EXTENSIONNAMEMISTAKE int = 8
    BIOSTAGE_FAILREASON_OTHERMISTAKE int = 9
    BIOSTAGE_FAILREASON_NOMISTAKE int = -1

)

type BuildStageErrorInfo struct {
    CmdStrErrorPosition int
    ErrType int
    ErrInfo string
    ErrValue string
    ItemName string
    ItemCmdStr string
    IsCMDErr bool
    /*build cmd error*/
    Err error
}

func NewBuildStageErrorInfo(position int, errType int, itemName string, err error) *BuildStageErrorInfo {
    failInfo := ErrTypeToErrInfo(errType, err)
    return &BuildStageErrorInfo {
        CmdStrErrorPosition: position,
        ErrType: errType,
        ErrInfo: failInfo,
        ItemName: itemName,
        IsCMDErr: false,
        Err: err,
    }
}

func ErrTypeToErrInfo(ErrType int, err error) string {
    var failInfo string
    switch ErrType {
    case BIOSTAGE_FAILREASON_GRAMISTAKE:
        failInfo = fmt.Sprintf("syntax error: %s", err.Error())
    case BIOSTAGE_FAILREASON_INPUTMISTAKE:
        failInfo = fmt.Sprintf("input error: %s", err.Error())
    case BIOSTAGE_FAILREASON_OUTPUTMISTAKE:
        failInfo = fmt.Sprintf("output error: %s", err.Error())
    case BIOSTAGE_FAILREASON_SHARDFILEMISTAKE:
        failInfo = fmt.Sprintf("shardfile error: %s", err.Error())
    case BIOSTAGE_FAILREASON_INPUTDIRTAGMISTAKE:
        failInfo = fmt.Sprintf("input dir tag error: %s", err.Error())
    case BIOSTAGE_FAILREASON_VARIABLEPARALLELMISTAKE:
        failInfo = fmt.Sprintf("variable parallel error: %s", err.Error())
    case BIOSTAGE_FAILREASON_CLONEPARALLELMISTAKE:
        failInfo = fmt.Sprintf("clone parallel error: %s", err.Error())
    case BIOSTAGE_FAILREASON_EXTENSIONNAMEMISTAKE:
        failInfo = fmt.Sprintf("Extension name error: %s", err.Error())
    case BIOSTAGE_FAILREASON_SUBPIPELINEMISTAKE:
        failInfo = fmt.Sprintf("pipeline error: %s", err.Error())

    case BIOSTAGE_FAILREASON_NOMISTAKE:
        failInfo = fmt.Sprintf("No error")
    }
    return failInfo
}

type BioJobFailInfo struct {
    StageName string
    StageId string
    FailType int
    FailInfo string
    BuildCMDError *BuildStageErrorInfo
}

/*biojob fail reason*/
const (
    BIOJOB_FAILREASON_INSUFFICIENTRESOURCES int = 0       /*Insufficient resources*/
    BIOJOB_FAILREASON_BUILDGRAPH int = 1                  /*Failed to build graph*/
    BIOJOB_FAILREASON_RESTOREJOBGRAPH int = 2
    BIOJOB_FAILREASON_RESTOREJOBEXEC int = 3
    BIOJOB_FAILREASON_RECOVERYJOBNOSNAPSHOT int = 4
    BIOJOB_FAILREASON_RECOVERYJOBIVALIDDONESTATE int = 5
    BIOJOB_FAILREASON_POSTEXECUTION int = 6
    BIOJOB_FAILREASON_STAGEFAILANDFAILJOB int = 7
    BIOJOB_FAILREASON_STAGEFAILANDPSUDOFINISH int = 8
    BIOJOB_FAILREASON_REACHRETRYNUM int = 9
    BIOJOB_FAILREASON_PREPARESTAGEFAIL int = 10
    BIOJOB_FAILREASON_EXCEEDINGCPULIMIT int = 11        /*Exceeding the CPU limit*/
    BIOJOB_FAILREASON_EXCEEDINGMEMLIMIT int = 12
    BIOJOB_FAILREASON_BUILDSTAGEVOL int = 13
    BIOJOB_FAILREASON_NOMOUNTPOINT int = 14
    BIOJOB_FAILREASON_RECOVERYFROMDATABASE int = 15
)

func NewBioJobFailInfo(stageName string, stageId string, failType int, err error,
    buildStageErrInfo *BuildStageErrorInfo) *BioJobFailInfo {
    failInfo := FailTypeToFailInfo(failType, err)
    return &BioJobFailInfo {
        StageName: stageName,
        StageId: stageId,
        FailType: failType,
        FailInfo: failInfo,
        BuildCMDError: buildStageErrInfo,
    }
}

func FailTypeToFailInfo(failType int, err error) string {
    var failInfo string
    switch failType {
    case BIOJOB_FAILREASON_INSUFFICIENTRESOURCES:
        failInfo = fmt.Sprintf("Insufficient resources: %s", err.Error())
    case BIOJOB_FAILREASON_BUILDGRAPH:
        failInfo = fmt.Sprintf("%s", err.Error())
    case BIOJOB_FAILREASON_RESTOREJOBGRAPH:
        failInfo = fmt.Sprintf("Fail restore job graph on recover: %s", err.Error())
    case BIOJOB_FAILREASON_RESTOREJOBEXEC:
        failInfo = fmt.Sprintf("Fail restore job exec info:: %s", err.Error())
    case BIOJOB_FAILREASON_RECOVERYJOBNOSNAPSHOT:
        failInfo = fmt.Sprintf("Fail to recover job: no snapshot info")
    case BIOJOB_FAILREASON_RECOVERYJOBIVALIDDONESTATE:
        failInfo = fmt.Sprintf("Fail to recover job: Invalid done state")
    case BIOJOB_FAILREASON_RECOVERYFROMDATABASE:
        failInfo = fmt.Sprintf("Fail recover job from database: %s", err.Error())
    case BIOJOB_FAILREASON_POSTEXECUTION:
        failInfo = fmt.Sprintf("stage fail by post execution error: %s", err.Error())
    case BIOJOB_FAILREASON_STAGEFAILANDFAILJOB:
        failInfo = fmt.Sprintf("the job fail, fail by policy")
    case BIOJOB_FAILREASON_STAGEFAILANDPSUDOFINISH:
        failInfo = fmt.Sprintf("stage fail and pseudo finish")
    case BIOJOB_FAILREASON_REACHRETRYNUM:
        failInfo = fmt.Sprintf("stage reached maximum fail retry count")
    case BIOJOB_FAILREASON_PREPARESTAGEFAIL:
        failInfo = fmt.Sprintf("%s", err.Error())
    case BIOJOB_FAILREASON_EXCEEDINGCPULIMIT:
        failInfo = fmt.Sprintf("Stage require CPU > limit")
    case BIOJOB_FAILREASON_EXCEEDINGMEMLIMIT:
        failInfo = fmt.Sprintf("Stage require Memory > limit")
    case BIOJOB_FAILREASON_BUILDSTAGEVOL:
        failInfo = fmt.Sprintf("Build stage volumes: %s", err.Error())
    case BIOJOB_FAILREASON_NOMOUNTPOINT:
        failInfo = fmt.Sprintf("No mountpoint for volume of stage")
    default:
        failInfo = fmt.Sprintf("No such fail type: %d", failType)
    }
    return failInfo
}

func (bioJobFailInfo *BioJobFailInfo)BioJobFailInfoFormat() string {
    var rs string

    if bioJobFailInfo.BuildCMDError != nil {
        if bioJobFailInfo.BuildCMDError.IsCMDErr {
            rs = fmt.Sprintf("stageName: %s, stageId: %s, %s in '%s'",
                bioJobFailInfo.StageName, bioJobFailInfo.StageId, bioJobFailInfo.BuildCMDError.ErrInfo, bioJobFailInfo.BuildCMDError.ErrValue)
            return rs
        }
    }
    rs = fmt.Sprintf("stageName: %s, stageId: %s, %s ",
            bioJobFailInfo.StageName, bioJobFailInfo.StageId, bioJobFailInfo.FailInfo)

    return rs
}