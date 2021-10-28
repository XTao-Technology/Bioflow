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
    "time"
    "errors"

    . "github.com/xtao/bioflow/common"
    xconfig "github.com/xtao/xstone/config"
    )

type SchedulerDB struct {
    BioConfDB
}

func NewSchedulerDB(endpoints []string) *SchedulerDB {
    db := &SchedulerDB {
            BioConfDB: *NewBioConfDB(endpoints),
    }
    return db
}

func (db *SchedulerDB)LoadJobSchedulerConfig(config *JobSchedulerConfig) error {
    err, rawData := db.GetSingleKey(ETCD_KEY_SCHEDULER_CONFIG) 
    if err != nil {
        ConfLogger.Errorf("Can't get scheduler config %s",
            ETCD_KEY_SCHEDULER_CONFIG)
        return err
    }
    err = json.Unmarshal(rawData, config)
    if err != nil {
        ConfLogger.Errorf("Can't decode scheduler config: %s\n",
            err.Error())
        return err
    }

    return nil
}

func (db *BioConfDB)SaveJobSchedulerConfig(config *JobSchedulerConfig) error {
    rawData, err := json.Marshal(config)
    if err != nil {
        ConfLogger.Errorf("Can't encode scheduler etcd config: %s\n",
            err.Error())
        return err
    }
    err = db.SetSingleKey(ETCD_KEY_SCHEDULER_CONFIG, string(rawData),
        time.Duration(0)) 
    if err != nil {
        ConfLogger.Errorf("Can't set scheduler config %s: %s",
            ETCD_KEY_SCHEDULER_CONFIG, err.Error())
        return err
    }
    ConfLogger.Infof("Successfully save scheduler config %v to ETCD\n",
        *config)

    return nil
}

func (db *BioConfDB)NewJobSchedulerConfigWatcher() *JobSchedulerConfigWatcher {
	return &JobSchedulerConfigWatcher {
		BaseConfWatcher: *db.NewRecursiveWatcher(ETCD_KEY_SCHEDULER),
	}
}

type JobSchedulerConfigWatcher struct {
	xconfig.BaseConfWatcher
}

/*
 * Watcher waits for job scheduler config changes, it will return caller the:
 * 1) error
 */
func (watcher *JobSchedulerConfigWatcher) WatchChanges() (error, 
    *JobSchedulerConfig) {
	err, res := watcher.WatchNext()
	if err != nil {
		ConfLogger.Errorf("job scheduler config watch error: %s\n",
			err.Error())
		return err, nil
	}
    config := &JobSchedulerConfig{}
    switch res.Action {
        case "set", "create":
            err = json.Unmarshal([]byte(res.Node.Value), config)
            if err != nil {
                ConfLogger.Errorf("Can't unmarshal job scheduler config %s: %s\n",
                    res.Node.Key, err.Error())
                return err, nil
            }
            return nil, config
        default:
            return errors.New("Unknown job scheduler config action " + res.Action),
                nil
    }
}
