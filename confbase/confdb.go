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
    "time"
    "strconv"
    "errors"
    "strings"

    . "github.com/xtao/bioflow/common"
    xconfig "github.com/xtao/xstone/config"
    )

type BioConfDB struct {
    xconfig.StorageMgrConfDB
}

func NewBioConfDB(endpoints []string) *BioConfDB {
    client := &BioConfDB{
                StorageMgrConfDB: *xconfig.NewStorageMgrConfDB(endpoints),
            }

    return client
}

func (db *BioConfDB)SaveDBConfig(dbConfig *DBServiceConfig) error {
    rawData, err := json.Marshal(dbConfig)
    if err != nil {
        ConfLogger.Errorf("Can't encode DB etcd config: %s\n",
            err.Error())
        return err
    }
    err = db.SetSingleKey(ETCD_KEY_CONFIG_DB, string(rawData),
        time.Duration(0)) 
    if err != nil {
        ConfLogger.Errorf("Can't set DBConfig %s: %s",
            ETCD_KEY_CONFIG_DB, err.Error())
        return err
    }
    ConfLogger.Infof("Successfully save DB config to ETCD\n")
    Logger.Println(*dbConfig)

    return nil
}

func (db *BioConfDB)LoadDBConfig(dbConfig *DBServiceConfig) error {
    err, rawData := db.GetSingleKey(ETCD_KEY_CONFIG_DB) 
    if err != nil {
        ConfLogger.Errorf("Can't get DBConfig %s", 
            ETCD_KEY_CONFIG_DB)
        return err
    }    
    err = json.Unmarshal(rawData, dbConfig)
    if err != nil {
        ConfLogger.Errorf("Can't decode DB etcd config: %s\n",
            err.Error())
        return err
    }    

    return nil
}


func (db *BioConfDB)LoadGlobalConfig(config *GlobalConfig) error {
    err, rawData := db.GetSingleKey(ETCD_KEY_CONFIG_GLOBAL) 
    if err != nil {
        ConfLogger.Errorf("Can't get global config %s",
            ETCD_KEY_CONFIG_GLOBAL)
        return err
    }
    err = json.Unmarshal(rawData, config)
    if err != nil {
        ConfLogger.Errorf("Can't decode global etcd config: %s\n",
            err.Error())
        return err
    }

    return nil
}

func (db *BioConfDB)SaveGlobalConfig(config *GlobalConfig) error {
    rawData, err := json.Marshal(config)
    if err != nil {
        ConfLogger.Errorf("Can't encode global etcd config: %s\n",
            err.Error())
        return err
    }
    err = db.SetSingleKey(ETCD_KEY_CONFIG_GLOBAL, string(rawData),
        time.Duration(0)) 
    if err != nil {
        ConfLogger.Errorf("Can't set global config %s: %s",
            ETCD_KEY_CONFIG_GLOBAL, err.Error())
        return err
    }
    ConfLogger.Infof("Successfully save global config to ETCD\n")
    Logger.Println(*config)

    return nil
}

func (db *BioConfDB)LoadBackendConfig() (error, []BackendConfig) {
    err, rawMap := db.GetKeysOfDirectory(ETCD_KEY_CONFIG_BACKENDS) 
    if err != nil {
        ConfLogger.Errorf("Can't get backend config %s",
            ETCD_KEY_CONFIG_BACKENDS)
        return err, nil
    }

    if rawMap == nil {
        return nil, nil
    }

    backends := make([]BackendConfig, 0, len(rawMap))
    for id, rawConfig := range rawMap {
        backendConfig := BackendConfig{}
        err = json.Unmarshal(rawConfig, &backendConfig)
        if err != nil {
            ConfLogger.Errorf("Can't decode backend %s config: %s\n",
                id, err.Error())
            return err, nil
        }
        backends = append(backends, backendConfig)
    }

    return nil, backends
}

func (db *BioConfDB) AddSingleBackendConfig(b *BackendConfig) error {
    key := fmt.Sprintf("%s/%s:%s:%s", ETCD_KEY_CONFIG_BACKENDS,
        b.Type, b.Server, b.Port)

    rawData, err := json.Marshal(b)
    err = db.SetSingleKey(key, string(rawData),
        time.Duration(0))
    if err != nil {
        ConfLogger.Errorf("Can't set backend config %s: %s",
            key, err.Error())
        return err
    }
    return nil
}

func (db *BioConfDB) DelSingleBackendConfig(id string) error {
    key := fmt.Sprintf("%s/%s", ETCD_KEY_CONFIG_BACKENDS, id)

    err := db.DeleteSingleKey(key)
    if err != nil {
        ConfLogger.Errorf("Failed to delete backend %s from confdb", id)
        return err
    }
    return nil
}

func (db *BioConfDB)SaveBackendConfig(backends []BackendConfig) error {
    for i := 0; i < len(backends); i ++ {
        backend := backends[i]
        err := db.AddSingleBackendConfig(&backend)
        if err != nil {
            return err
        }
    }

    return nil
}

func (db *BioConfDB)LoadRestConfig(restConfig *RestServerConfig) error {
    err, rawData := db.GetSingleKey(ETCD_KEY_CONFIG_REST) 
    if err != nil {
        ConfLogger.Errorf("Can't get rest config %s",
            ETCD_KEY_CONFIG_REST)
        return err
    }
    err = json.Unmarshal(rawData, restConfig)
    if err != nil {
        ConfLogger.Errorf("Can't decode rest etcd config: %s\n",
            err.Error())
        return err
    }

    return nil
}

func (db *BioConfDB)SaveRestConfig(restConfig *RestServerConfig) error {
    rawData, err := json.Marshal(restConfig)
    if err != nil {
        ConfLogger.Errorf("Can't encode DB etcd config: %s\n",
            err.Error())
        return err
    }
    err = db.SetSingleKey(ETCD_KEY_CONFIG_REST, string(rawData),
        time.Duration(0)) 
    if err != nil {
        ConfLogger.Errorf("Can't set rest Config %s: %s",
            ETCD_KEY_CONFIG_REST, err.Error())
        return err
    }
    ConfLogger.Infof("Successfully save REST config to ETCD\n")

    return nil
}

func (db *BioConfDB)LoadPhysicalConfig(physicalConfig *PhysicalConfig) error {
    err, rawData := db.GetSingleKey(ETCD_KEY_CONFIG_PHYSICAL)
    if err != nil {
        ConfLogger.Errorf("Can't get physical config %s",
            ETCD_KEY_CONFIG_PHYSICAL)
        return err
    }

    err = json.Unmarshal(rawData, physicalConfig)
    if err != nil {
        ConfLogger.Errorf("Can't decode physical etcd config: %s\n",
            err.Error())
        return err
    }

    return nil
}

func (db *BioConfDB)SavePhysicalConfig(physicalConfig *PhysicalConfig) error {
    rawData, err := json.Marshal(physicalConfig)
    if err != nil {
        ConfLogger.Errorf("Can't encode DB physical config: %s\n",
            err.Error())
        return err
    }

    err = db.SetSingleKey(ETCD_KEY_CONFIG_PHYSICAL, string(rawData),
        time.Duration(0))
    if err != nil {
        ConfLogger.Errorf("Can't set physical Config %s: %s",
            ETCD_KEY_CONFIG_PHYSICAL, err.Error())
        return err
    }

    ConfLogger.Infof("Successfully save physical config to ETCD\n")

    return nil
}

func (db *BioConfDB)LoadBioflowConfig() (error, *BioflowConfig) {
    config := &BioflowConfig{}

    err := db.LoadDBConfig(&config.DBConfig)
    if err != nil {
        ConfLogger.Errorf("Can't load DB config from ETCD: %s\n",
            err.Error())
        return err, nil
    }

    err = db.LoadRestConfig(&config.RestConfig)
    if err != nil {
        ConfLogger.Errorf("Can't load rest config from ETCD: %s\n",
            err.Error())
        return err, nil
    }

    err = db.LoadStorageMgrConfig(&config.StoreConfig)
    if err != nil {
        ConfLogger.Errorf("Can't load store config from ETCD: %s\n",
            err.Error())
        return err, nil
    }

    err, backends := db.LoadBackendConfig()
    if err != nil {
        ConfLogger.Errorf("Can't load backend config: %s\n",
            err.Error())
    }
    if backends != nil {
        config.BackendConfig = backends
    } else {
        config.BackendConfig = make([]BackendConfig, 0, 0)
    }

    err = db.LoadClusterConfig(&config.ClusterConfig)
    if err != nil {
        ConfLogger.Errorf("Can't load cluster config from ETCD: %s\n",
            err.Error())
        return err, nil
    }

    err = db.LoadSecurityConfig(&config.SecurityConfig)
    if err != nil {
        ConfLogger.Errorf("Can't load security config from ETCD: %s\n",
            err.Error())
        return err, nil
    }

    err = db.LoadPhysicalConfig(&config.PhysicalConfig)
    if err != nil {
         ConfLogger.Errorf("Can't load physical config from ETCD: %s\n",
            err.Error())
    }

    db.LoadMailConfig(&config.MailConfig)
    db.LoadFileDownloadConfig(&config.FileDownloadConfig)
    db.LoadPipelineStoreConfig(&config.PipelineStoreConfig)

    _, configs := db.LoadFrontEndConfig()
    if configs != nil {
        config.FrontEndConfig = configs
    } else {
        config.FrontEndConfig = make([]FrontEndConfig, 0)
    }

    return nil, config
}

func (db *BioConfDB)SaveBioflowConfig(config *BioflowConfig) error {
    err := db.SaveDBConfig(&config.DBConfig)
    if err != nil {
        ConfLogger.Errorf("Can't save DB config to ETCD: %s\n",
            err.Error())
        return err
    }

    err = db.SaveRestConfig(&config.RestConfig)
    if err != nil {
        ConfLogger.Errorf("Can't save rest config to ETCD: %s\n",
            err.Error())
        return err
    }

    err = db.SaveStorageMgrConfig(&config.StoreConfig)
    if err != nil {
        ConfLogger.Errorf("Can't save store config to ETCD: %s\n",
            err.Error())
        return err
    }

    err = db.SaveBackendConfig(config.BackendConfig)
    if err != nil {
        ConfLogger.Errorf("Can't save backend config to ETCD: %s\n",
            err.Error())
        return err
    }

    err = db.SaveClusterConfig(&config.ClusterConfig, false)
    if err != nil {
        ConfLogger.Errorf("Can't save cluster config to ETCD: %s\n",
            err.Error())
        return err
    }

    err = db.SaveSecurityConfig(&config.SecurityConfig)
    if err != nil {
        ConfLogger.Errorf("Can't save security config to ETCD: %s\n",
            err.Error())
        return err
    }

    err = db.SavePhysicalConfig(&config.PhysicalConfig)
    if err != nil {
        ConfLogger.Errorf("Can't save physical config to ETCD: %s\n",
            err.Error())
        return err
    }
    
    return nil
}

    
func (db *BioConfDB)LoadQueueConfig(queue int, config *BioflowQueueConfig) error {
    configKey := ConfBuildQueueConfigKey(queue)
    err, rawData := db.GetSingleKey(configKey) 
    if err != nil {
        ConfLogger.Errorf("Can't get cluster queue config %s",
            configKey)
        return err
    }
    err = json.Unmarshal(rawData, config)
    if err != nil {
        ConfLogger.Errorf("Can't decode cluster queue config: %s\n",
            err.Error())
        return err
    }

    return nil
}

func (db *BioConfDB)SaveQueueConfig(queue int, config *BioflowQueueConfig) error {
    configKey := ConfBuildQueueConfigKey(queue)
    rawData, err := json.Marshal(config)
    if err != nil {
        ConfLogger.Errorf("Can't encode cluster queue config: %s\n",
            err.Error())
        return err
    }
    err = db.SetSingleKey(configKey, string(rawData),
        time.Duration(0)) 
    if err != nil {
        ConfLogger.Errorf("Can't set cluster queue config %s: %s",
            configKey, err.Error())
        return err
    }

    return nil
}

func (db *BioConfDB)LoadQueueGlobalConfig(config *BioflowClusterGlobalConfig) error {
    err, rawData := db.GetSingleKey(ETCD_KEY_CLUSTER_CONFIG) 
    if err != nil {
        ConfLogger.Errorf("Can't get cluster global config %s",
            ETCD_KEY_CLUSTER_CONFIG)
        return err
    }
    err = json.Unmarshal(rawData, config)
    if err != nil {
        ConfLogger.Errorf("Can't decode cluster global config: %s\n",
            err.Error())
        return err
    }

    return nil
}

func (db *BioConfDB)SaveQueueGlobalConfig(config *BioflowClusterGlobalConfig) error {
    rawData, err := json.Marshal(config)
    if err != nil {
        ConfLogger.Errorf("Can't encode cluster global config: %s\n",
            err.Error())
        return err
    }
    err = db.SetSingleKey(ETCD_KEY_CLUSTER_CONFIG, string(rawData),
        time.Duration(0)) 
    if err != nil {
        ConfLogger.Errorf("Can't set cluster global config %s: %s",
            ETCD_KEY_CLUSTER_CONFIG, err.Error())
        return err
    }

    return nil
}

func (db *BioConfDB)SaveClusterConfig(config *BioflowClusterConfig, onlyUpdateQueue bool) error {
    if !onlyUpdateQueue {
        err := db.SaveQueueGlobalConfig(&config.GlobalConfig)
        if err != nil {
            ConfLogger.Errorf("Fail to save cluster global config: %s\n",
                err.Error())
            return err
        }

        /*initialize each queue's seq to 1*/
        seqValue := "1"
        for i := 0; i < config.GlobalConfig.QueueCount; i ++ {
            seqKey := ConfBuildQueueSeqKey(i)
            err = db.SetKeyIfNotExist(seqKey, seqValue)
            if err != nil {
                ConfLogger.Errorf("Set not exist key %s fail (ignored): %s\n",
                    seqKey, err.Error())
            }
        }
    }

    /*save each queue's config*/
    for key, queueConfig := range config.QueueConfig {
        queueCount, err := strconv.Atoi(key)
        if err != nil {
            ConfLogger.Errorf("The cluster queue config invalid, key %s not int: %s\n",
                key, err.Error())
            return err
        }
        err = db.SaveQueueConfig(queueCount, &queueConfig)
        if err != nil {
            ConfLogger.Errorf("Can't save queue config for queue %s: %s\n",
                key, err.Error())
            return err
        }
    }

    return nil
}

func (db *BioConfDB)LoadClusterConfig(config *BioflowClusterConfig) error {
    err := db.LoadQueueGlobalConfig(&config.GlobalConfig)
    if err != nil {
        ConfLogger.Errorf("Fail to load cluster global config: %s\n",
            err.Error())
        return err
    }

    config.QueueConfig = make(map[string]BioflowQueueConfig)
    /*initialize each queue's seq to 1*/
    for i := 0; i < config.GlobalConfig.QueueCount; i ++ {
        queueConfig := BioflowQueueConfig{}
        err = db.LoadQueueConfig(i, &queueConfig)
        if err != nil {
            ConfLogger.Errorf("Can't save queue config for queue %d: %s\n",
                i, err.Error())
        } else {
            queueId := fmt.Sprintf("%d", i)
            config.QueueConfig[queueId] = queueConfig
        }
    }

    return nil
}

func (db *BioConfDB)LoadSecurityConfig(securityConfig *SecurityConfig) error {
    err, rawData := db.GetSingleKey(ETCD_KEY_SECURITY_CONFIG) 
    if err != nil {
        ConfLogger.Errorf("Can't get security config %s",
            ETCD_KEY_SECURITY_CONFIG)
        return err
    }
    err = json.Unmarshal(rawData, securityConfig)
    if err != nil {
        ConfLogger.Errorf("Can't decode security etcd config: %s\n",
            err.Error())
        return err
    }

    return nil
}

func (db *BioConfDB)SaveSecurityConfig(securityConfig *SecurityConfig) error {
    rawData, err := json.Marshal(securityConfig)
    if err != nil {
        ConfLogger.Errorf("Can't encode security etcd config: %s\n",
            err.Error())
        return err
    }
    err = db.SetSingleKey(ETCD_KEY_SECURITY_CONFIG, string(rawData),
        time.Duration(0)) 
    if err != nil {
        ConfLogger.Errorf("Can't set security config %s: %s",
            ETCD_KEY_SECURITY_CONFIG, err.Error())
        return err
    }
    ConfLogger.Infof("Successfully save security config %v to ETCD\n",
        *securityConfig)

    return nil
}

func (db *BioConfDB)SetAdminPassword(oldpass string, newpass string) int {

    key := ETCD_KEY_ADMIN_PASSWORD

    aesEnc := NewAesEncrypt(BIOFLOW_ADMIN_PASSWORD_SEED)
    np, err := aesEnc.Encrypt(newpass)
    if err != nil {
        ConfLogger.Errorf("Failed to encrypt new password.")
        return 2
    }

    if oldpass == BIOFLOW_INITIAL_ADMIN_PASSWORD {
        err = db.SetKeyIfNotExist(key, np)
        if err != nil {
			fmt.Println("Fail to set key :%s", err.Error())
            ConfLogger.Errorf("Failed to set password: %s", err.Error())
            return 1
        }
    } else {
        op, err := aesEnc.Encrypt(oldpass)
        if err != nil {
            ConfLogger.Errorf("Failed to encrypt old password.")
            return 2
        }

        err = db.CompareAndSetKey(key, np, op)
        if err != nil {
            ConfLogger.Errorf("Failed to change password: %s.", err.Error())
            return 3
        }
    }
    return 0
}

func (db *BioConfDB)GetAdminPassword() (error, string) {
    err, rawData := db.GetSingleKey(ETCD_KEY_ADMIN_PASSWORD)
    if err == nil {
        p := rawData

        aesEnc := NewAesEncrypt(BIOFLOW_ADMIN_PASSWORD_SEED)
        pass, err := aesEnc.Decrypt(string(p))
        if err != nil {
            return err, ""
        }
        return nil, pass

    } else if KeyNotFound(err) {
        return nil, ""
    } else {
		return err, ""
	}
}

func (db *BioConfDB) AddMailConfig(b *MailConfig) error {
    rawData, err := json.Marshal(b)
    err = db.SetSingleKey(ETCD_KEY_CONFIG_MAIL, string(rawData),
        time.Duration(0))
    if err != nil {
        ConfLogger.Errorf("Can't set mail config %s: %s",
            ETCD_KEY_CONFIG_MAIL, err.Error())
        return err
    }
    return nil
}

func (db *BioConfDB)LoadMailConfig(mailConfig *MailConfig) error {
    err, rawData := db.GetSingleKey(ETCD_KEY_CONFIG_MAIL)
    if err != nil {
        ConfLogger.Errorf("Can't get mail config %s",
            ETCD_KEY_CONFIG_MAIL)
        return err
    }
    err = json.Unmarshal(rawData, mailConfig)
    if err != nil {
        ConfLogger.Errorf("Can't decode mail etcd config: %s",
            err.Error())
        return err
    }

    return nil
}

func (db *BioConfDB) AddFileDownloadConfig(b *FileDownloadConfig) error {
    rawData, err := json.Marshal(b)
    err = db.SetSingleKey(ETCD_KEY_CONFIG_FILEDOWNLOAD, string(rawData),
        time.Duration(0))
    if err != nil {
        ConfLogger.Errorf("Can't set file download config %s: %s",
            ETCD_KEY_CONFIG_FILEDOWNLOAD, err.Error())
        return err
    }
    return nil
}

func (db *BioConfDB)LoadFileDownloadConfig(config *FileDownloadConfig) error {
    err, rawData := db.GetSingleKey(ETCD_KEY_CONFIG_FILEDOWNLOAD)
    if err != nil {
        ConfLogger.Errorf("Can't get file download config %s",
            ETCD_KEY_CONFIG_FILEDOWNLOAD)
        return err
    }
    err = json.Unmarshal(rawData, config)
    if err != nil {
        ConfLogger.Errorf("Can't decode file download config: %s",
            err.Error())
        return err
    }

    return nil
}

func (db *BioConfDB) SetPipelineStoreConfig(b *PipelineStoreConfig) error {
    rawData, err := json.Marshal(b)
    err = db.SetSingleKey(ETCD_KEY_CONFIG_PIPELINESTORE, string(rawData),
        time.Duration(0))
    if err != nil {
        ConfLogger.Errorf("Can't set pipeline store config %s: %s",
            ETCD_KEY_CONFIG_PIPELINESTORE, err.Error())
        return err
    }
    return nil
}

func (db *BioConfDB)LoadPipelineStoreConfig(storeConfig *PipelineStoreConfig) error {
    err, rawData := db.GetSingleKey(ETCD_KEY_CONFIG_PIPELINESTORE)
    if err != nil {
        ConfLogger.Errorf("Can't get pipeline store config %s",
            ETCD_KEY_CONFIG_PIPELINESTORE)
        return err
    }
    err = json.Unmarshal(rawData, storeConfig)
    if err != nil {
        ConfLogger.Errorf("Can't decode pipeline store etcd config: %s",
            err.Error())
        return err
    }

    return nil
}


func (db *BioConfDB) AddSingleFrontEnd(config *FrontEndConfig, queue int) error{
    endkey := strings.Replace(config.Url, "/", "*", -1)
    key := fmt.Sprintf("%s/%s", ETCD_KEY_CONFIG_FRONTEND, endkey)
    rawData, _ := json.Marshal(config)
    err := db.SetSingleKey(key, string(rawData),
        time.Duration(0))
    if err != nil {
        ConfLogger.Errorf("Can't add front-end %s: %s", config.Url, err.Error())
        return err
    }
    key = fmt.Sprintf("%s/%d/%s", ETCD_KEY_CONFIG_FRONTEND, queue, endkey)
    err = db.SetSingleKey(key, string(rawData),
        time.Duration(0))
    if err != nil {
        ConfLogger.Errorf("Can't add front-end %s: %s", config.Url, err.Error())
        return err
    }
    return nil
}

func (db *BioConfDB) DeleteSingleFrontEnd(config *FrontEndConfig, queue int) error{
    endkey := strings.Replace(config.Url, "/", "*", -1)
    key := fmt.Sprintf("%s/%d/%s", ETCD_KEY_CONFIG_FRONTEND, queue, endkey)
    err := db.DeleteSingleKey(key)
    if err != nil {
        ConfLogger.Errorf("Can't delete front-end %s: %s", config.Url, err.Error())
        return err
    }
    key = fmt.Sprintf("%s/%s", ETCD_KEY_CONFIG_FRONTEND, endkey)
    db.DeleteSingleKey(key)
    return nil
}

func (db *BioConfDB) LoadFrontEndConfig() (error, []FrontEndConfig){
    err, values := db.GetKeysOfDirectory(ETCD_KEY_CONFIG_FRONTEND)
    if err != nil {
        ConfLogger.Errorf("Can't get front-end config %s",
            ETCD_KEY_CONFIG_FRONTEND)
        return err, nil
    }
    var config FrontEndConfig
    configs := make([]FrontEndConfig, 0)
    for _, value := range(values) {
        if value != nil {
            err = json.Unmarshal(value, &config)
            if err != nil {
                continue
            }
            configs = append(configs, config)
        }
    }
    return nil, configs
}

func (db *BioConfDB)CompareAdminPassword() error {
    pass, err := LoadAdminPassword()
    if err != nil {
        return err
    }

    err, pass1 := db.GetAdminPassword()
    if err != nil {
        return err
    }

    if pass != pass1 {
        return errors.New("Permission denied, incorrect admin password!")
    }
    return nil
}


func (db *BioConfDB)NewBackendConfigWatcher() *BackendsWatcher {
    return &BackendsWatcher {
        BaseConfWatcher: *db.NewRecursiveWatcher(ETCD_KEY_CONFIG_BACKENDS),
    }
}

func (db *BioConfDB)NewSecurityConfigWatcher() *SecurityConfigWatcher {
    return &SecurityConfigWatcher {
        BaseConfWatcher: *db.NewRecursiveWatcher(ETCD_KEY_SECURITY),
    }
}

func (db *BioConfDB)NewQueueConfigWatcher() *QueueConfigWatcher {
    return &QueueConfigWatcher{
        BaseConfWatcher: *db.NewRecursiveWatcher(ETCD_KEY_CLUSTER_QUEUES),
    }
}

func (db *BioConfDB)NewGlobalConfigWatcher() *GlobalConfigWatcher {
    return &GlobalConfigWatcher {
        BaseConfWatcher: *db.NewRecursiveWatcher(ETCD_KEY_CONFIG),
    }
}

func (db *BioConfDB)NewAdminPasswordWatcher() *AdminPasswordWatcher {
    return &AdminPasswordWatcher {
        BaseConfWatcher: *db.NewRecursiveWatcher(ETCD_KEY_ADMIN),
    }
}

func (db *BioConfDB)NewMailConfigWatcher() *MailConfigWatcher {
    return &MailConfigWatcher {
        BaseConfWatcher: *db.NewRecursiveWatcher(ETCD_KEY_CONFIG_MAIL),
    }
}


func (db *BioConfDB)NewFileDownloadConfigWatcher() *FileDownloadConfigWatcher {
    return &FileDownloadConfigWatcher {
        BaseConfWatcher: *db.NewRecursiveWatcher(ETCD_KEY_CONFIG_FILEDOWNLOAD),
    }
}

func (db *BioConfDB)NewPipelineStoreConfigWatcher() *PipelineStoreConfigWatcher {
    return &PipelineStoreConfigWatcher {
        BaseConfWatcher: *db.NewRecursiveWatcher(ETCD_KEY_CONFIG_PIPELINESTORE),
    }
}

func (db *BioConfDB)NewFrontEndWatcher(queue int) *FrontEndWatcher {
    queueId := strconv.Itoa(queue)
    return &FrontEndWatcher {
        BaseConfWatcher: *db.NewRecursiveWatcher(ETCD_KEY_CONFIG_FRONTEND + "/" + queueId),
    }
}

func (db *BioConfDB)NewPhysicalConfigWatcher() *PhysicalConfigWatcher {
    return &PhysicalConfigWatcher {
        BaseConfWatcher: *db.NewRecursiveWatcher(ETCD_KEY_CONFIG_PHYSICAL),
    }
}

func (db *BioConfDB)NewLDAPWatcher() *SingleKeyWatcher {
    return &SingleKeyWatcher {
        BaseConfWatcher: *db.NewRecursiveWatcher(ETCD_KEY_LDAP_CHANGE),
    }
}
