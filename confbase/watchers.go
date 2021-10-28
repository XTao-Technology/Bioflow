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
	"errors"

    . "github.com/xtao/bioflow/common"
    xconfig "github.com/xtao/xstone/config"
    )

type ClusterQueueMemberWatcher struct {
	xconfig.BaseConfWatcher
}

/*
 * Watcher waits for member changes, it will return caller the:
 * 1) error
 * 2) updated or created instance, nil if no
 * 3) deleted instance, nil if no
 * 4) expired instance, nil if no
 */
func (watcher *ClusterQueueMemberWatcher) WatchChanges() (error, 
    *BioflowInstanceConfig, *BioflowInstanceConfig, 
    *BioflowInstanceConfig) {
	err, res := watcher.WatchNext()
	if err != nil {
		ConfLogger.Errorf("bioflow instance members watch error: %s\n",
			err.Error())
		return err, nil, nil, nil
	}
    instState := &BioflowInstanceConfig{}
    switch res.Action {
        case "set", "create":
            err = json.Unmarshal([]byte(res.Node.Value), instState)
            if err != nil {
                ConfLogger.Errorf("Can't unmarshal set instance config %s: %s\n",
                    res.Node.Key, err.Error())
                return err, nil, nil, nil
            }
            return nil, instState, nil, nil
        case "delete":
            err, instState.Seq = ConfParseQueueInstanceKeyToSeq(res.Node.Key)
            if err != nil {
                ConfLogger.Errorf("Can't parse deleted instance config key %s: %s\n",
                    res.Node.Key, err.Error())
                return err, nil, nil, nil
            }
            return nil, nil, instState, nil
        case "expire":
            err, instState.Seq = ConfParseQueueInstanceKeyToSeq(res.Node.Key)
            if err != nil {
                ConfLogger.Errorf("can't parse expire instance config key %s: %s\n",
                    res.Node.Key, err.Error())
                return err, nil, nil, nil
            }
            return nil, nil, nil, instState
        default:
            return errors.New("Unknown action " + res.Action), nil,
                nil, nil
    }
}

type BackendsWatcher struct {
	xconfig.BaseConfWatcher
}

func (watcher *BackendsWatcher) WatchChanges() (error, string, *BackendConfig, string) {
	err, res := watcher.WatchNext()
	if err != nil {
		ConfLogger.Errorf("bioflow backends watch error: %s\n",
			err.Error())
		return err, "", nil, ""
	}

    switch res.Action {
	case "set", "create":
		var config BackendConfig
		err = json.Unmarshal([]byte(res.Node.Value), &config)
		if err != nil {
			ConfLogger.Errorf("Can't unmarshal backend config %s: %s\n",
				res.Node.Key, err.Error())
			return err, "add", nil, ""
		}
		return nil, "add", &config, ""
	case "delete":
		err, backendId := ConfParseBackendKeyToId(res.Node.Key)
		if err != nil {
			return err, "delete", nil, ""
		}
		return nil, "delete", nil, backendId
	}
	return errors.New("Unknown action " + res.Action), "unknown", nil, ""
}

type SecurityConfigWatcher struct {
	xconfig.BaseConfWatcher
}

/*
 * Watcher waits for security config changes, it will return caller the:
 * 1) error
 */
func (watcher *SecurityConfigWatcher) WatchChanges() (error, 
    *SecurityConfig) {
	err, res := watcher.WatchNext()
	if err != nil {
		ConfLogger.Errorf("bioflow security config watch error: %s\n",
			err.Error())
		return err, nil
	}
    config := &SecurityConfig{}
    switch res.Action {
        case "set", "create":
            err = json.Unmarshal([]byte(res.Node.Value), config)
            if err != nil {
                ConfLogger.Errorf("Can't unmarshal security config %s: %s\n",
                    res.Node.Key, err.Error())
                return err, nil
            }
            return nil, config
        default:
            return errors.New("Unknown action " + res.Action), nil
    }
}

type AdminPasswordWatcher struct {
	xconfig.BaseConfWatcher
}

func (watcher *AdminPasswordWatcher) WatchChanges() (error, string) {
	var pass string
	err, res := watcher.WatchNext()
	if err != nil {
		ConfLogger.Errorf("bioflow admin password watch error: %s\n",
			err.Error())
		return err, ""
	}
	switch res.Action {
	case "set", "create":
		pass = res.Node.Value
		return nil, pass
	default:
		return errors.New("Unknown action " + res.Action), ""
	}
}

type QueueConfigWatcher struct {
    xconfig.BaseConfWatcher
}

func (watcher *QueueConfigWatcher)WatchChanges(queue int) (error,
    *BioflowQueueConfig) {
    err, res := watcher.WatchNext()
    if err != nil {
        ConfLogger.Errorf("Queue loglevel config watch error: %s\n",
            err.Error())
        return err, nil
    }
    if res.Node != nil {
        queueConfigKey := ConfBuildQueueConfigKey(queue)
        switch res.Node.Key {
            case queueConfigKey:
                queueConfig := &BioflowQueueConfig{}
                err = json.Unmarshal([]byte(res.Node.Value), queueConfig)
                if err != nil {
                    ConfLogger.Errorf("Can't decode server storage config: %s\n",
                        err.Error())
                    return err, nil
                }
                return nil, queueConfig
            default:
                ConfLogger.Errorf("Watched a unknown key: %s \n",
                    res.Node.Key)
                return errors.New("Unknown key " + res.Node.Key), nil
        }
    } else {
        return errors.New("Strange error empty watcher result"), nil
    }
}

type GlobalConfigWatcher struct {
	xconfig.BaseConfWatcher
}

func (watcher *GlobalConfigWatcher)WatchChanges() (error,
	*GlobalConfig) {
	err, res := watcher.WatchNext()
	if err != nil {
		ConfLogger.Errorf("StoreMgr config watch error: %s\n",
			err.Error())
		return err, nil
	}

	if res.Node != nil {
		switch res.Node.Key {
			case ETCD_KEY_CONFIG_GLOBAL:
				globalConfig := &GlobalConfig{}
				err = json.Unmarshal([]byte(res.Node.Value), globalConfig)
				if err != nil {
					ConfLogger.Errorf("Can't decode server storage config: %s\n",
						err.Error())
					return err, nil
				}
				return nil, globalConfig
			default:
				ConfLogger.Errorf("Watched a unknown key: %s \n",
					res.Node.Key)
				return errors.New("Unknown key " + res.Node.Key), nil
		}
	} else {
		return errors.New("Strange error empty watcher result"), nil
	}
}

type MailConfigWatcher struct {
	xconfig.BaseConfWatcher
}

func (watcher *MailConfigWatcher) WatchChanges() (error, *MailConfig) {
	err, res := watcher.WatchNext()
	if err != nil {
		ConfLogger.Errorf("Mail config watch error: %s\n",
			err.Error())
		return err, nil
	}

	config := &MailConfig{}
	switch res.Action {
	case "set", "create":
		err = json.Unmarshal([]byte(res.Node.Value), config)
		if err != nil {
			ConfLogger.Errorf("Can't unmarshal mail config %s: %s\n",
				res.Node.Key, err.Error())
			return err, nil
		}
		return nil, config
	default:
		return errors.New("Unknown action " + res.Action), nil
	}
}

type FileDownloadConfigWatcher struct {
	xconfig.BaseConfWatcher
}

func (watcher *FileDownloadConfigWatcher) WatchChanges() (error, *FileDownloadConfig) {
	err, res := watcher.WatchNext()
	if err != nil {
		ConfLogger.Errorf("File download config watch error: %s\n",
			err.Error())
		return err, nil
	}

	config := &FileDownloadConfig{}
	switch res.Action {
	case "set", "create":
		err = json.Unmarshal([]byte(res.Node.Value), config)
		if err != nil {
			ConfLogger.Errorf("Can't unmarshal file download config %s: %s\n",
				res.Node.Key, err.Error())
			return err, nil
		}
		return nil, config
	default:
		return errors.New("Unknown action " + res.Action), nil
	}
}

type FrontEndWatcher struct {
	xconfig.BaseConfWatcher
}

func (watcher *FrontEndWatcher) WatchChanges() (error, *FrontEndConfig, string) {
	err, res := watcher.WatchNext()
	if err != nil {
		ConfLogger.Errorf("Front-end config watch error: %s\n",
			err.Error())
		return err, nil, ""
	}

	config := &FrontEndConfig{}
	err = json.Unmarshal([]byte(res.Node.Value), config)
	if err != nil {
		ConfLogger.Errorf("Can't unmarshal Front-end config %s: %s\n",
			res.Node.Key, err.Error())
		return err, nil, ""
	}
	var opt string
	switch res.Action {
	case "set", "create":
		opt = "ADD"
	case "delete":
		opt = "DEL"
	default:
		return errors.New("Unknown action " + res.Action), nil, ""
	}
	return nil, config, opt
}

type PipelineStoreConfigWatcher struct {
	xconfig.BaseConfWatcher
}

func (watcher *PipelineStoreConfigWatcher) WatchChanges() (error, *PipelineStoreConfig) {
	err, res := watcher.WatchNext()
	if err != nil {
		ConfLogger.Errorf("Pipeline store config watch error: %s\n",
			err.Error())
		return err, nil
	}

	config := &PipelineStoreConfig{}
	switch res.Action {
	case "set", "create":
		err = json.Unmarshal([]byte(res.Node.Value), config)
		if err != nil {
			ConfLogger.Errorf("Can't unmarshal pipeline store config %s: %s\n",
				res.Node.Key, err.Error())
			return err, nil
		}
		return nil, config
	default:
		return errors.New("Unknown action " + res.Action), nil
	}
}

type PhysicalConfigWatcher struct {
    xconfig.BaseConfWatcher
}

func (watcher *PhysicalConfigWatcher) WatchChanges() (error, *PhysicalConfig) {
    err, res := watcher.WatchNext()
    if err != nil {
        ConfLogger.Errorf("Physical config watch error: %s\n",
            err.Error())
        return err, nil
    }

    config := &PhysicalConfig{}
    switch res.Action {
        case "set", "create":
            err = json.Unmarshal([]byte(res.Node.Value), config)
            if err != nil {
                ConfLogger.Errorf("Can't unmarshal physical config %s: %s\n",
                    res.Node.Key, err.Error())
                return err, nil
            }
            return nil, config
        default:
            return errors.New("Unknown action " + res.Action), nil
    }
}

type SingleKeyWatcher struct {
	xconfig.BaseConfWatcher
}

/*
 * Watcher waits for ldap user config changes
 */
func (watcher *SingleKeyWatcher) WatchChanges() (error, bool) {
	err, res := watcher.WatchNext()
	if err != nil {
		ConfLogger.Errorf("LDAP watch error: %s\n",
			err.Error())
		return err, false
	}
    switch res.Action {
        case "set", "create":
            return nil, true
        default:
            return errors.New("Unknown action " + res.Action), false
    }
}
