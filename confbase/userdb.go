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
package confbase

import (
    "encoding/json"

    . "github.com/xtao/bioflow/common"
    )

type UserDB struct {
    BioConfDB
}

func NewUserDB(endpoints []string) *UserDB {
    db := &UserDB {
            BioConfDB: *NewBioConfDB(endpoints),
    }
    return db
}

func (db *UserDB)GetAllUsers() (error, map[string]UserEntry) {
    userKey := EX_ETCD_KEY_USER
    err, rawData := db.GetSingleKey(userKey)
    if err != nil {
        ConfLogger.Errorf("Fail to get user info from %s:%s\n",
            userKey, err.Error())
        return err, nil
    }

    userMap := make(map[string]UserEntry)
    err = json.Unmarshal(rawData, &userMap)
    if err != nil {
        ConfLogger.Errorf("Can't decode user %s config: %s\n",
            userKey, err.Error())
        return err, nil
    }

    return nil, userMap
}

func (db *UserDB) GetAllGroups() (error, map[string]GroupEntry) {
    groupKey := EX_ETCD_KEY_GROUP
    err, rawData := db.GetSingleKey(groupKey)
    if err != nil {
        ConfLogger.Errorf("Fail to get group info from %s:%s\n",
            groupKey, err.Error())
        return err, nil
    }

    groupMap := make(map[string]GroupEntry)
    err = json.Unmarshal(rawData, &groupMap)
    if err != nil {
        ConfLogger.Errorf("Can't decode user %s config: %s\n",
            groupMap, err.Error())
        return err, nil
    }
    return nil, groupMap
}
