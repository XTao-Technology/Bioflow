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
package main

import (
    "fmt"
    "os"
    "strings"

    . "github.com/xtao/bioflow/client"
    . "github.com/xtao/bioflow/debug/faultinject"
    . "github.com/xtao/bioflow/message"
)


type debugCommand struct {
    client *BioflowClient
}

func newDebugCommand(c *BioflowClient) *debugCommand {
    return &debugCommand{
        client: c,
    }
}

func (cmd *debugCommand) DoDebugOperation(op string) {
    err := cmd.client.DoDebugOperation(op)
    if err != nil {
        fmt.Printf("Fail to do debug operation: %s\n",
            err.Error())
    } else {
        fmt.Printf("Successfully do the debug operation\n")
    }
}

func (cmd *debugCommand) EnableFaultInject(enable bool) {
    opt := FaultInjectOpt{
        Enable: enable,
    }
    opt.Cmd = CMD_ENABLE_FAULT_INJECT
    err := cmd.client.FaultInjectControl(opt)
    if err != nil {
        fmt.Printf("Fail to control fault inject: %s\n",
            err.Error())
        return
    }
}

func (cmd *debugCommand) DeleteFault(id string, ownType string) {
    opt := FaultInjectOpt{
        Id: id,
        Type: ownType,
    }
    opt.Cmd = CMD_DELETE_FAULT_EVENT
    err := cmd.client.FaultInjectControl(opt)
    if err != nil {
        fmt.Printf("Fail to control fault inject: %s\n",
            err.Error())
        return
    }
}

func (cmd *debugCommand) ResumeFault(id string, ownType string, msg string) {
    opt := FaultInjectOpt{
        Id: id,
        Type: ownType,
        Msg: msg,
    }
    opt.Cmd = CMD_RESUME_FAULT_EVENT
    err := cmd.client.FaultInjectControl(opt)
    if err != nil {
        fmt.Printf("Fail to control fault inject: %s\n",
            err.Error())
        return
    }
}

func (cmd *debugCommand) InjectFault(id string, ownType string, action string) {
    opt := FaultInjectOpt{
        Id: id,
        Type: ownType,
    }
    switch strings.ToUpper(action) {
        case FAULT_INJECT_PANIC, FAULT_INJECT_ERR, FAULT_INJECT_SUSPEND:
            opt.Action = action
        default:
            fmt.Printf("The action is invalid\n")
            os.Exit(-1)
    }

    opt.Cmd = CMD_ADD_FAULT_EVENT
    err := cmd.client.FaultInjectControl(opt)
    if err != nil {
        fmt.Printf("Fail to control fault inject: %s\n",
            err.Error())
        return
    }
}

func (cmd *debugCommand) ShowInjectedFaults() {
    err, enabled, injectedFaults := cmd.client.GetInjectedFaults()
    if err != nil {
        fmt.Printf("Fail to get injected faults: %s\n",
            err.Error())
        return
    }

    fmt.Printf("EnableAssertControl: %v\n", enabled)
    for index, event := range injectedFaults {
        fmt.Printf("Fault %d: \n", index + 1)
        fmt.Printf("  Id: %s\n", event.Id)
        fmt.Printf("  Type: %s\n", event.Type)
        fmt.Printf("  Action: %s\n", event.Action)
    }
}
