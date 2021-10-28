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
package common

import (
    "encoding/json"
    "strings"
)

const (
    SECURITY_MODE_ISOLATION int = 1
    SECURITY_MODE_STRICT int = 2
)

const (
    UID_GLOBAL string = "Anoynmous"
)

type UserAccountInfo struct {
    Username string       `json:"user"`
    Groupname string      `json:"group"`
    Uid string            `json:"uid"`
    Gid string            `json:"gid"`
    Umask string          `json:"umask"`
}

type SecurityContext struct {
    secID string
    privileged bool
    account *UserAccountInfo
}

func NewSecurityContext(account *UserAccountInfo) *SecurityContext {
    if account == nil {
        return &SecurityContext{
                secID: UID_GLOBAL,
            }
    }

    ctxt := &SecurityContext {
            secID: account.Username,
            account: account,
    }

    if strings.ToUpper(account.Username) == "ROOT" {
        ctxt.SetPrivileged(true)
    }

    return ctxt
}

func (ctxt *SecurityContext)SetUserInfo(account *UserAccountInfo) {
    if account == nil {
        return
    }

    ctxt.account = account
    ctxt.secID = account.Username
    if strings.ToUpper(account.Username) == "ROOT" {
        ctxt.SetPrivileged(true)
    }
}

func (ctxt *SecurityContext)GetUserInfo() *UserAccountInfo{
    return ctxt.account
}

func (ctxt *SecurityContext)IsPrivileged() bool {
    return ctxt.privileged
}

func (ctxt *SecurityContext)SetPrivileged(pri bool) {
    ctxt.privileged = pri
}

func (ctxt *SecurityContext)ID() string {
    return ctxt.secID
}

func (ctxt *SecurityContext)CheckGroup(userGroups map[string]bool, pipelineOwnerGroups map[string]bool) bool {
    if ctxt.IsPrivileged() {
        return true
    }

    for pipelineOwnerGid,_ := range pipelineOwnerGroups {
        if _, ok := userGroups[pipelineOwnerGid]; ok {
                return true
        } else {
            continue
        }
    }

    return false
}

func (ctxt *SecurityContext)CheckSecID(id string) bool {
    if ctxt.IsPrivileged() {
        return true
    }

    return ctxt.secID == id
}

func (ctxt *SecurityContext)ToJSON() (error, string) {
    body, err := json.Marshal(ctxt.account)
    if err != nil {
        Logger.Errorf("Fail to marshal security context\n")
        return err, ""
    }

    return nil, string(body)
}

func (ctxt *SecurityContext)FromJSON(jsonData string) error {
    account := &UserAccountInfo{}
    err := json.Unmarshal([]byte(jsonData), account)
    if err != nil {
        return err
    }

    ctxt.SetUserInfo(account)
    return nil
}


