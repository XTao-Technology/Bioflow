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


const (
    EX_ETCD_KEY_USER string = "/sys/account/users"
    EX_ETCD_KEY_USER_SHADOW string = "/sys/account/shadows"
    EX_ETCD_KEY_GROUP string = "/sys/account/groups"
    EX_ETCD_KEY_GROUP_SHADOW string = "/sys/account/gshadows"
)

/*An Entry contains all the fields for a specific user*/
type UserEntry struct {
    Pass  string `json:"Pass"`
    Uid   string `json:"Uid"`
    Gid   string `json:"Gid"`
    Gecos string `json:"Gecos"`
    Home  string `json:"Home"`
    Shell string `json:"Shell"`
}

/*An Entry contains all the fields for a specific group*/
type GroupEntry struct {
    Pass  string   `json:"Pass"`
    Gid   string   `json:"Gid"`
    Users []string `json:"Users"`
}

type ShadowEntry struct {
    Pass      string `json:"Pass"`
    LstChg    string   `json:"LstChg"`
    Min       string   `json:"Min"`
    Max       string   `json:"Max"`
    Warn      string   `json:"Warn"`
    Inact     string   `json:"Inact"`
    Expire    string   `json:"Expire"`
    Reserved  string   `json:"Reserved"`
}

type GshadowEntry struct {
    Pass    string   `json:"Pass"`
    Admins  []string `json:"Admins"`
    Members []string `json:"Members"`
}


func ExConfigUserEntryToAccount(userEntry map[string]UserEntry) []UserAccountInfo {
    accounts := make([]UserAccountInfo, 0)
    for userName, entry := range userEntry {
        account := UserAccountInfo {
            Username: userName,
            Uid: entry.Uid,
            Gid: entry.Gid,
        }
        accounts = append(accounts, account)
    }

    return accounts
}

func ExConfigGroupEntryToGroup (groupEntry map[string] GroupEntry) map[string][]string {
    groups := make(map[string] []string)
    for _, entry := range groupEntry {
        group := make([]string, 0)
        for _, v := range entry.Users {
            group = append(group, v)
        }
        groups[entry.Gid] = group
    }

    return groups
}
