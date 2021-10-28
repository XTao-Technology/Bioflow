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
    "fmt"
    "strconv"
    "time"

    . "github.com/xtao/bioflow/common"
    )

type InstanceDB struct {
    BioConfDB
}

func NewInstanceDB(endpoints []string) *InstanceDB {
    db := &InstanceDB {
            BioConfDB: *NewBioConfDB(endpoints),
    }
    return db
}

func (db *InstanceDB)GetQueueSeq(queue int) (error, uint64) {
    seqKey := ConfBuildQueueSeqKey(queue)
    err, rawData := db.GetSingleKey(seqKey)
    if err != nil {
        ConfLogger.Errorf("Fail to get seq for queue %d:%s\n",
            queue, err.Error())
        return err, 0
    }

    seq, err := strconv.ParseUint(string(rawData), 10, 64)
    if err != nil {
        ConfLogger.Errorf("Key %s value %s not integer: %s\n",
            seqKey, string(rawData), err.Error())
        return err, 0
    }

    return nil, seq
}

/*Set queue's sequence number, do the compare and set if oldSeq not 0*/
func (db *InstanceDB)SetQueueSeq(queue int, newSeq uint64, oldSeq uint64) error {
    seqKey := ConfBuildQueueSeqKey(queue)
    newValue := fmt.Sprintf("%d", newSeq)
    if oldSeq != 0 {
        oldValue := fmt.Sprintf("%d", oldSeq)
        err := db.CompareAndSetKey(seqKey, newValue, oldValue)
        if err != nil {
            ConfLogger.Errorf("Can't compare set queue seq %s %s %s: %s\n",
                seqKey, newValue, oldValue, err.Error())
            return err
        }
    } else {
        err := db.SetSingleKey(seqKey, newValue,
            time.Duration(0)) 
        if err != nil {
            ConfLogger.Errorf("Can't set queue seq %s %s: %s\n",
                seqKey, newValue, err.Error())
            return err
        }
    }

    return nil
}

func (db *InstanceDB)CreateInstanceState(queue int, seq uint64, 
    config *BioflowInstanceConfig, ttl time.Duration) error {
    instKey := ConfBuildQueueInstanceKey(queue, seq)
    rawData, err := json.Marshal(config)
    if err != nil {
        ConfLogger.Errorf("Can't encode bioflow instance config: %s\n",
            err.Error())
        return err
    }
    err = db.SetSingleKey(instKey, string(rawData), ttl)
    if err != nil {
        ConfLogger.Errorf("Fail create bioflow instance %s: %s\n",
            instKey, err.Error())
        return err
    }

    return nil
}

func (db *InstanceDB)DeleteInstanceState(queue int, seq uint64) error {
    instKey := ConfBuildQueueInstanceKey(queue, seq)
    err := db.DeleteSingleKey(instKey)
    if err != nil {
        ConfLogger.Errorf("Fail to delete key %s: %s\n",
            instKey, err.Error())
        return err
    }

    return nil
}

func (db *InstanceDB)RefreshInstanceState(queue int, seq uint64, 
    ttl time.Duration) error {
    instKey := ConfBuildQueueInstanceKey(queue, seq)
    err := db.RefreshKey(instKey, ttl)
    if err != nil {
        ConfLogger.Errorf("Fail refresh bioflow instance %s: %s\n",
            instKey, err.Error())
        return err
    }

    return nil
}

func (db *InstanceDB)GetAllInstanceState(queue int) (error,
    []BioflowInstanceConfig) {
    memberKey := ConfBuildQueueMemberKey(queue)
    err, rawMap := db.GetKeysOfDirectory(memberKey) 
    if err != nil {
        ConfLogger.Errorf("Can't get instance state list %s: %s\n",
            memberKey, err.Error())
        return err, nil
    }

    if rawMap == nil {
        return nil, nil
    }

    instances := make([]BioflowInstanceConfig, 0, len(rawMap))
    for id, rawConfig := range rawMap {
        instanceConfig := BioflowInstanceConfig{}
        err = json.Unmarshal(rawConfig, &instanceConfig)
        if err != nil {
            ConfLogger.Errorf("Can't decode instance %s config: %s\n",
                id, err.Error())
            return err, nil
        }
        instances = append(instances, instanceConfig)
    }

    return nil, instances
}

func (db *InstanceDB)NewQueueMemberWatcher(queue int) *ClusterQueueMemberWatcher{
    memberKey := ConfBuildQueueMemberKey(queue)
	return &ClusterQueueMemberWatcher{
			BaseConfWatcher: *db.NewRecursiveWatcher(memberKey),
	}
}

