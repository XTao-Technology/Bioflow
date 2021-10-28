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
    "os"
    "fmt"
    "github.com/bndr/gotabulate"
    . "github.com/xtao/bioflow/client"
    . "github.com/xtao/bioflow/message"
)


type userCommand struct {
    client *BioflowClient
}

func newUserCommand(c *BioflowClient) *userCommand {
    return &userCommand{
        client: c,
    }
}

func (cmd *userCommand) LoadExternalUsers() {
    err := cmd.client.LoadUsers()
    if err != nil {
        fmt.Printf("Fail to load users: %s\n",
            err.Error())
        return
    } else {
        fmt.Printf("Successfully loaded users\n")
    }
}

func ShowUserInfo(userInfo *BioflowUserInfo) {
    fmt.Printf("User: %s\n", userInfo.Username)
    fmt.Printf("  Groupname: %s\n", userInfo.Groupname)
    fmt.Printf("  Uid: %s\n", userInfo.Uid)
    fmt.Printf("  Gid: %s\n", userInfo.Gid)
    fmt.Printf("  TaskNum: %d\n", userInfo.TaskNum)
    fmt.Printf("  JobNum: %d\n", userInfo.JobNum)
    fmt.Printf("  RunningJobNum: %d\n", userInfo.RunningJobNum)
    fmt.Printf("  CompletedJobNum: %d\n", userInfo.CompletedJobNum)
    fmt.Printf("  FailJobNum: %d\n", userInfo.FailJobNum)
    fmt.Printf("  CanceledJobNum: %d\n", userInfo.CanceledJobNum)
    fmt.Printf("  PausedJobNum: %d\n", userInfo.PausedJobNum)
    fmt.Printf("  LostTaskNum: %d\n", userInfo.LostTaskNum)
    fmt.Printf("  Credit: %d\n", userInfo.Credit)
    fmt.Printf("  JobQuota: %d\n", userInfo.JobQuota)
    fmt.Printf("  TaskQuota: %d\n", userInfo.TaskQuota)
    fmt.Printf("  MailAddress: %s\n", userInfo.Mail)
    fmt.Printf("  Groups:")
    for group, _ := range userInfo.Groups {
        fmt.Printf(" %s", group)
    }
    fmt.Printf("\n")
}

func ShowAllUsers(userList []BioflowUserInfo) {
    fmt.Printf("%d Users: \n", len(userList))
    for i := 0; i < len(userList); i ++ {
        ShowUserInfo(&userList[i])
    }
}

func (cmd *userCommand) ShowUserInfo(userId string) {
    err, userInfo := cmd.client.GetUserInfo(userId)
    if err != nil {
        fmt.Printf("Fail to get user info: %s\n",
            err.Error())
        os.Exit(-1)
    } else {
        ShowUserInfo(userInfo)
    }
}

func (cmd *userCommand) ListAllUsers() {
    err, userInfo := cmd.client.ListUserInfo()
    if err != nil {
        fmt.Printf("Fail to list users: %s\n",
            err.Error())
        os.Exit(-1)
    } else {
        ShowAllUsers(userInfo)
    }
}

func (cmd *userCommand) UpdateUserConfig(usr string, credit int64,
    jobQuota int64, taskQuota int64, mail string) {
    config := &BioflowUserConfig{
                ID: usr,
                Credit: credit,
                JobQuota: jobQuota,
                TaskQuota: taskQuota,
                Mail: mail,
    }
    err := cmd.client.UpdateUserConfig(config)
    if err != nil {
        fmt.Printf("Fail to update user config: %s\n",
            err.Error())
        os.Exit(-1)
    } else {
        fmt.Println("Successfully updated user config")
    }
}

func ShowUserRscStats(stats []UserRscStats) {
    var table [][]string
    fmt.Printf("%d Users Resource State \n", len(stats))
    for _, e := range stats {
        table = append(table, []string{e.ID,
            fmt.Sprintf("%d", e.JobCnt),
            fmt.Sprintf("%d",e.StageCnt),
            fmt.Sprintf("%f", e.CpuMinutes),
            fmt.Sprintf("%f", e.MemMinutes),})
    }

    tabulate := gotabulate.Create(table)
    tabulate.SetHeaders([]string{
        "UserID",
        "JobCount",
        "StageCount",
        "CPUMinutes",
        "MemMinutes",
    })
    tabulate.SetEmptyString("None")
    tabulate.SetAlign("left")
    fmt.Println(tabulate.Render("grid"))
}

func (cmd *userCommand) ListUserRscStats(args map[string]interface{}) {
    err, statsList := cmd.client.ListUserRscStats(args)
    if err != nil {
        fmt.Printf("Fail to list user rsc stats list: %s\n",
            err.Error())
        os.Exit(-1)        
    } else {
        ShowUserRscStats(statsList)
    }
}

func (cmd *userCommand) ResetRscStats(args map[string]interface{}) {
    err := cmd.client.ResetRscStats(args)
    if err != nil {
        fmt.Printf("Fail to reset rsc stats: %s\n", err.Error())
        os.Exit(-1)        
    } else {
        fmt.Printf("Succeed to reset rsc stats.\n")
    }
}
