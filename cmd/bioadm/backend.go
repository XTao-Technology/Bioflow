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
package main

import (
    "fmt"
    "os"
    . "github.com/xtao/bioflow/confbase"
    . "github.com/xtao/bioflow/common"
    . "github.com/xtao/bioflow/message"
    . "github.com/xtao/bioflow/client"
)


type backendCommand struct {
    client *BioflowClient
}

func newBackendCommand(c *BioflowClient) *backendCommand {
    return &backendCommand{
        client: c,
    }
}

func ShowBackend(info *BioflowBackendInfo, indent bool) {
    IndentPrint(indent, "ID: %s\n", info.Id)
    IndentPrint(indent, "Server: %s\n", info.Server)
    IndentPrint(indent, "Type: %s\n", info.Type)
    IndentPrint(indent, "Status: %s\n", info.Status)
    IndentPrint(indent, "TaskCount: %d\n", info.TaskCount)
    IndentPrint(indent, "FailCount: %d\n", info.FailCount)
    IndentPrint(indent, "LastFailAt: %s\n", info.LastFailAt)
}

func ShowBackendShort(info *BackendConfig, indent bool) {
    id := GenBackendId(info.Type, info.Server, info.Port)
    IndentPrint(indent, "ID: %s\n", id)
    IndentPrint(indent, "Server: %s\n", info.Server)
    IndentPrint(indent, "Type: %s\n", info.Type)
    IndentPrint(indent, "Port: %s\n", info.Port)
}

func (cmd *backendCommand) List(epoints string) {
    err, backends := cmd.client.ListBackend()
    if err != nil {
        fmt.Printf("Listing backends from config database...\n")
        servers := ParseEndpoints(epoints)
        db := NewBioConfDB(servers)
        err, list := db.LoadBackendConfig()
        if err != nil {
            return
        }

        for i, b := range list {
            fmt.Printf( "Backend %d \n", i + 1)
            ShowBackendShort(&b, true)
        }

        return
    }

    if len(backends) == 0 {
        fmt.Printf("No any backend.\n")
        return
    }

    for i, b := range backends {
        fmt.Printf(" Backend %d: \n", i + 1)
        ShowBackend(&b, true)
    }
}

func (cmd *backendCommand) Add(epoints string, t string, svr string, port string) {
    if svr == "" || port == "" {
        fmt.Printf("You must specify a valid server or port.\n")
        os.Exit(1)
    }

    servers := ParseEndpoints(epoints)
    db := NewBioConfDB(servers)

    err := db.CompareAdminPassword()
    if err != nil {
        fmt.Printf("%s\n", err.Error())
        os.Exit(1)
    }

    backend := &BackendConfig {
        Type: t,
        Server: svr,
        Port: port,
    }

    err = db.AddSingleBackendConfig(backend)
    if err != nil {
        fmt.Printf("Fail to add backend http://%s:%s %s\n",
            svr, port, err.Error())
        os.Exit(1)
    } else {
        fmt.Printf("Succeed to add backend http://%s:%s\n",
            svr, port)
    }
}

func (cmd *backendCommand) Delete(epoints string, id string) {
    if id == "" {
        fmt.Printf("You must specify a valid backend ID\n")
        os.Exit(1)
    }

    servers := ParseEndpoints(epoints)
    db := NewBioConfDB(servers)

    err := db.CompareAdminPassword()
    if err != nil {
        fmt.Printf("%s\n", err.Error())
        os.Exit(1)
    }

    err = db.DelSingleBackendConfig(id)
    if err != nil {
        fmt.Printf("Fail to delete backend %s:%s.\n",
            id, err.Error())
        os.Exit(1)
    } else {
        fmt.Printf("Succeed to delete backend %s.\n",
            id)
    }
}

func (cmd *backendCommand) Disable(id string) {
    if id == "" {
        fmt.Printf("You must specify a valid backend ID\n")
        os.Exit(1)
    }

    err := cmd.client.DisableBackend(id)
    if err != nil {
        fmt.Printf("Fail to add disable backend %s:%s.\n",
            id, err.Error())
        os.Exit(1)
    } else {
        fmt.Printf("Succeed to disable backend %s.\n",
            id)
    }
}

func (cmd *backendCommand) Enable(id string) {
    if id == "" {
        fmt.Printf("You must specify a valid backend ID\n")
        os.Exit(1)
    }

    err := cmd.client.EnableBackend(id)
    if err != nil {
        fmt.Printf("Fail to enable backend %s:%s.\n",
            id, err.Error())
        os.Exit(1)
    } else {
        fmt.Printf("Succeed to enable backend %s.\n",
            id)
    }
}
