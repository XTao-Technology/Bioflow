package server

import (
    "strings"
    "net/http"
    . "github.com/xtao/bioflow/common"
    . "github.com/xtao/bioflow/message"
    . "github.com/xtao/bioflow/debug/faultinject"
    "encoding/json"
)

func FaultInjectControl(w http.ResponseWriter, req *http.Request) {
    decoder := json.NewDecoder(req.Body)
    defer req.Body.Close()

    var result BioflowAPIResult

    faultInjectMgr := GetFaultInjectMgr()

    /*Validate user privilege*/
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

    var faultInjectOpt FaultInjectOpt
    err = decoder.Decode(&faultInjectOpt)
    if err != nil {
        Logger.Println("Can't parse the request " + err.Error())
        result.Status = BIOFLOW_API_RET_FAIL
        result.Msg = err.Error()
        writeJSON(500, result, w)
        return
    }

    result.Status = BIOFLOW_API_RET_OK
    result.Msg = "successfully control fault inject"
    switch strings.ToUpper(faultInjectOpt.Cmd) {
        case CMD_ENABLE_FAULT_INJECT:
            faultInjectMgr.SetAssertControlEnable(faultInjectOpt.Enable)
        case CMD_ADD_FAULT_EVENT:
            faultInjectMgr.AddEvent(faultInjectOpt.Id, faultInjectOpt.Type,
                faultInjectOpt.Action)
        case CMD_DELETE_FAULT_EVENT:
            faultInjectMgr.RemoveEvent(faultInjectOpt.Id, faultInjectOpt.Type)
        case CMD_RESUME_FAULT_EVENT:
            faultInjectMgr.ResumeEvent(faultInjectOpt.Id, faultInjectOpt.Type,
                faultInjectOpt.Msg)
        default:
            Logger.Errorf("unknown fault control cmd\n")
            result.Status = BIOFLOW_API_RET_FAIL
            result.Msg = "unknown fault control cmd"
    }
    writeJSON(http.StatusAccepted, result, w)
    return
}

func ShowFaultInject(w http.ResponseWriter, req *http.Request) {
    defer req.Body.Close()

    var result BioflowShowFaultInjectResult
    faultInjectMgr := GetFaultInjectMgr()

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
    result.Status = BIOFLOW_API_RET_OK
    result.Enabled = faultInjectMgr.GetAssertControlEnable()
    result.Events = faultInjectMgr.ListEvents()
    writeJSON(http.StatusAccepted, result, w)
}
