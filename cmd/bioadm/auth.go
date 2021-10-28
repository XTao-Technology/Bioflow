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
	"strings"
    . "github.com/xtao/bioflow/common"
    . "github.com/xtao/bioflow/confbase"
)

type authCommand struct {
    endpoints []string
}

func newAuthCommand(epoints string) *authCommand {
    servers := ParseEndpoints(epoints)
    return &authCommand {
        endpoints: servers,
    }
}

func (cmd *authCommand) Passwd(oldpass string, newpass string) {

    db := NewBioConfDB(cmd.endpoints)

    rc := db.SetAdminPassword(oldpass, newpass)
    if rc != 0 {
        fmt.Println("Error: Failed to set administrator's password.")
        switch rc {
        case 1:
            fmt.Println("Password is incorrect, last password is needed.")
        case 2:
            fmt.Println("Failed to encrypt old password.")
        case 3:
            fmt.Println("Last password is incorrect!")
        }
    } else {
        err := SaveAdminPassword(newpass)
        if err != nil {
            fmt.Println("Error: Failed to save password to local: ", err.Error())
        } else {
            fmt.Println("Succeed to set administrator's password.")
        }
    }
    return
}

func (cmd *authCommand) Key(user string) {
	if strings.ToUpper(user) == "ROOT" {
		fmt.Println("Error: It's not allowed to generate key for privileged account!")
		return
	}

	AesAkey := NewAesEncrypt(BIOFLOW_API_KEY_SEED)
    AesSkey := NewAesEncrypt(BIOFLOW_SECURITY_KEY_SEED)
    aKey, err := AesAkey.Encrypt(user)
    if err != nil {
        return
    }

   sKey, err := AesSkey.Encrypt(user)
    if err != nil {
        return
    }

	fmt.Println("Succeed to generate API-Key and Security-Key for: ", user)
	fmt.Println("API-Key      : ", aKey)
	fmt.Println("Security-Key : ", sKey)
	return
}
