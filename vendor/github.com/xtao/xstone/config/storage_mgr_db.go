package config

import (
    "encoding/json"
    "time"
    . "github.com/xtao/xstone/common"
    )

type StorageMgrConfDB struct {
    ETCDConfDB
}

func NewStorageMgrConfDB(endpoints []string) *StorageMgrConfDB {
    db := &StorageMgrConfDB{
                ETCDConfDB: *NewETCDConfDB(endpoints),
            }

    return db
}

func (db *StorageMgrConfDB)LoadServerStoreConfig(serverConfig *ServerStoreConfig) error {
    err, rawData := db.GetSingleKey(ETCD_KEY_STORAGE_SERVER) 
    if err != nil {
        StoneLogger.Errorf("Can't get server storage config %s",
            ETCD_KEY_STORAGE_SERVER)
        return err
    }
    err = json.Unmarshal(rawData, serverConfig)
    if err != nil {
        StoneLogger.Errorf("Can't decode server storage config: %s\n",
            err.Error())
        return err
    }

    return nil
}

func (db *StorageMgrConfDB)SaveServerStoreConfig(serverConfig *ServerStoreConfig) error {
    rawData, err := json.Marshal(serverConfig)
    if err != nil {
        StoneLogger.Errorf("Can't encode server etcd config: %s\n",
            err.Error())
        return err
    }
    err = db.SetSingleKey(ETCD_KEY_STORAGE_SERVER, string(rawData),
        time.Duration(0)) 
    if err != nil {
        StoneLogger.Errorf("Can't set server config %s:%s",
            ETCD_KEY_STORAGE_SERVER, err.Error())
        return err
    }

    return nil
}

func (db *StorageMgrConfDB)LoadSchedulerStoreConfig(schedulerConfig *SchedulerStoreConfig) error {
    err, rawData := db.GetSingleKey(ETCD_KEY_STORAGE_SCHEDULER) 
    if err != nil {
        StoneLogger.Errorf("Can't get scheduler storage config %s",
            ETCD_KEY_STORAGE_SCHEDULER)
        return err
    }
    err = json.Unmarshal(rawData, schedulerConfig)
    if err != nil {
        StoneLogger.Errorf("Can't decode scheduler storage config: %s\n",
            err.Error())
        return err
    }

    return nil
}

func (db *StorageMgrConfDB)SaveSchedulerStoreConfig(config *SchedulerStoreConfig) error {
    rawData, err := json.Marshal(config)
    if err != nil {
        StoneLogger.Errorf("Can't encode scheduler storage etcd config: %s\n",
            err.Error())
        return err
    }
    err = db.SetSingleKey(ETCD_KEY_STORAGE_SCHEDULER, string(rawData),
        time.Duration(0)) 
    if err != nil {
        StoneLogger.Errorf("Can't set server config %s:%s",
            ETCD_KEY_STORAGE_SCHEDULER, err.Error())
        return err
    }

    return nil
}

func (db *StorageMgrConfDB)LoadContainerStoreConfig(containerConfig *ContainerStoreConfig) error {
    err, rawData := db.GetSingleKey(ETCD_KEY_STORAGE_CONTAINER) 
    if err != nil {
        StoneLogger.Errorf("Can't get container storage config %s",
            ETCD_KEY_STORAGE_CONTAINER)
        return err
    }
    err = json.Unmarshal(rawData, &containerConfig)
    if err != nil {
        StoneLogger.Errorf("Can't decode container storage config: %s\n",
            err.Error())
        return err
    }

    return nil
}

func (db *StorageMgrConfDB)SaveContainerStoreConfig(config *ContainerStoreConfig) error {
    rawData, err := json.Marshal(config)
    if err != nil {
        StoneLogger.Errorf("Can't encode container storage etcd config: %s\n",
            err.Error())
        return err
    }
    err = db.SetSingleKey(ETCD_KEY_STORAGE_CONTAINER, string(rawData),
        time.Duration(0)) 
    if err != nil {
        StoneLogger.Errorf("Can't set server config %s:%s",
            ETCD_KEY_STORAGE_CONTAINER, err.Error())
        return err
    }

    return nil
}

func (db *StorageMgrConfDB)LoadStorageMgrConfig(storeConfig *StoreMgrConfig) error {
    /*get server storage mapping config*/
    err := db.LoadServerStoreConfig(&storeConfig.ServerConfig)
    if err != nil {
        StoneLogger.Errorf("Can't load server storage config: %s\n",
            err.Error())
        return err
    }

    /*get scheduler storage mapping config*/
    err = db.LoadSchedulerStoreConfig(&storeConfig.SchedulerConfig)
    if err != nil {
        StoneLogger.Errorf("Can't load scheduler storage config: %s\n",
            err.Error())
        return err
    }

    /*get container storage mapping config*/
    err = db.LoadContainerStoreConfig(&storeConfig.ContainerConfig)
    if err != nil {
        StoneLogger.Errorf("Can't load container storage config: %s\n",
            err.Error())
        return err
    }

    return nil
}

func (db *StorageMgrConfDB)SaveStorageMgrConfig(storeConfig *StoreMgrConfig) error {
    /*get server storage mapping config*/
    err := db.SaveServerStoreConfig(&storeConfig.ServerConfig)
    if err != nil {
        StoneLogger.Errorf("Can't save server storage config: %s\n",
            err.Error())
        return err
    }

    /*get scheduler storage mapping config*/
    err = db.SaveSchedulerStoreConfig(&storeConfig.SchedulerConfig)
    if err != nil {
        StoneLogger.Errorf("Can't save scheduler storage config: %s\n",
            err.Error())
        return err
    }

    /*get container storage mapping config*/
    err = db.SaveContainerStoreConfig(&storeConfig.ContainerConfig)
    if err != nil {
        StoneLogger.Errorf("Can't save container storage config: %s\n",
            err.Error())
        return err
    }

    return nil
}

    
func (db *StorageMgrConfDB)UpdateServerVolumeMap(updateVols map[string]string, 
    deleteVols []string) error {
    serverConfig := ServerStoreConfig{}
    err := db.LoadServerStoreConfig(&serverConfig)
    if err != nil {
        StoneLogger.Errorf("Can't load server config: %s\n",
            err.Error())
        return err
    }

    StoneLogger.Infof("Current server volume map %v\n",
        serverConfig.VolMntPath)

    /*merge changes with current configuration*/
    if serverConfig.VolMntPath == nil {
        serverConfig.VolMntPath = make(map[string]string)
    }

    if updateVols != nil {
        for vol, mntPath := range updateVols {
            serverConfig.VolMntPath[vol] = mntPath
        }
    }

    if deleteVols != nil {
        for i := 0; i < len(deleteVols); i ++ {
            if _, ok := serverConfig.VolMntPath[deleteVols[i]]; ok {
                delete(serverConfig.VolMntPath, deleteVols[i])
            }
        }
    }

    StoneLogger.Infof("Updated server volume map %v\n",
        serverConfig.VolMntPath)

    err = db.SaveServerStoreConfig(&serverConfig)
    if err != nil {
        StoneLogger.Errorf("Can't save server config %s\n",
            err.Error())
        return err
    }

    return nil
}

func (db *StorageMgrConfDB)UpdateSchedulerVolumeMap(updateVols map[string]string, 
    deleteVols []string) error {
    schedulerConfig := SchedulerStoreConfig{}
    err := db.LoadSchedulerStoreConfig(&schedulerConfig)
    if err != nil {
        StoneLogger.Errorf("Can't load scheduler config: %s\n",
            err.Error())
        return err
    }

    StoneLogger.Infof("Current scheduler volume map %v\n",
        schedulerConfig.SchedVolMntPath)

    if schedulerConfig.SchedVolMntPath == nil {
        schedulerConfig.SchedVolMntPath = make(map[string]string)
    }

    /*merge changes with current configuration*/
    if updateVols != nil {
        for vol, mntPath := range updateVols {
            schedulerConfig.SchedVolMntPath[vol] = mntPath
        }
    }

    if deleteVols != nil {
        for i := 0; i < len(deleteVols); i ++ {
            if _, ok := schedulerConfig.SchedVolMntPath[deleteVols[i]]; ok {
                delete(schedulerConfig.SchedVolMntPath, deleteVols[i])
            }
        }
    }

    StoneLogger.Infof("Updated scheduler volume map %v\n",
        schedulerConfig.SchedVolMntPath)

    err = db.SaveSchedulerStoreConfig(&schedulerConfig)
    if err != nil {
        StoneLogger.Errorf("Can't save scheduler config %s\n",
            err.Error())
        return err
    }

    return nil
}

func (db *StorageMgrConfDB)SetSchedulerAutoMount(autoMount bool) error {
    schedulerConfig := SchedulerStoreConfig{}
    err := db.LoadSchedulerStoreConfig(&schedulerConfig)
    if err != nil {
        StoneLogger.Errorf("Can't load scheduler config: %s\n",
            err.Error())
        return err
    }

    schedulerConfig.AutoMount = autoMount

    err = db.SaveSchedulerStoreConfig(&schedulerConfig)
    if err != nil {
        StoneLogger.Errorf("Can't save scheduler config %s\n",
            err.Error())
        return err
    }

    return nil
}

func (db *StorageMgrConfDB)LoadStorageClusterConfig() (error, []StorageClusterMountConfig) {
    err, rawMap := db.GetKeysOfDirectory(ETCD_KEY_STORAGE_CLUSTER) 
    if err != nil {
        StoneLogger.Errorf("Can't get storage cluster config %s",
            ETCD_KEY_STORAGE_CLUSTER)
        return err, nil
    }

    if rawMap == nil {
        return nil, nil
    }

    clusters := make([]StorageClusterMountConfig, 0, len(rawMap))
    for id, rawConfig := range rawMap {
        clusterConfig := StorageClusterMountConfig{}
        err = json.Unmarshal(rawConfig, &clusterConfig)
        if err != nil {
            StoneLogger.Errorf("Can't decode storage cluster %s config: %s\n",
                id, err.Error())
            return err, nil
        }
        clusters = append(clusters, clusterConfig)
    }

    return nil, clusters
}

func (db *StorageMgrConfDB)SaveStorageClusterConfig(config *StorageClusterMountConfig) error {
    rawData, err := json.Marshal(config)
    if err != nil {
        StoneLogger.Errorf("Can't encode storage cluster config: %s\n",
            err.Error())
        return err
    }
    key := ConfBuildStorageClusterConfigKey(config.ClusterName, config.Volume)
    err = db.SetSingleKey(key, string(rawData),
        time.Duration(0)) 
    if err != nil {
        StoneLogger.Errorf("Can't set storage cluster config %s: %s",
            key, err.Error())
        return err
    }

    return nil
}

func (db *StorageMgrConfDB)DeleteStorageClusterConfig(cluster string, vol string) error {
    key := ConfBuildStorageClusterConfigKey(cluster, vol)
    err := db.DeleteSingleKey(key) 
    if err != nil {
        StoneLogger.Errorf("Can't delete the cluster config %s %s: %s",
            cluster, vol, err.Error())
        return err
    }

    return nil
}

func (db *StorageMgrConfDB)NewStoreMgrConfigWatcher() *StoreMgrConfigWatcher {
    return &StoreMgrConfigWatcher{
            BaseConfWatcher: *db.NewRecursiveWatcher(ETCD_KEY_STORAGE_MAPPER),
    }
}

func (db *StorageMgrConfDB)NewStorageClusterConfigWatcher() *StorageClusterConfigWatcher {
    return &StorageClusterConfigWatcher {
        BaseConfWatcher: *db.NewRecursiveWatcher(ETCD_KEY_STORAGE_CLUSTER),
    }
}

func (db *StorageMgrConfDB) UpdateServerJobRootPath(path string) error {
    serverConfig := ServerStoreConfig{}
    err := db.LoadServerStoreConfig(&serverConfig)
    if err != nil {
        StoneLogger.Errorf("Can't load server config: %s\n",
            err.Error())
        return err
    }
    serverConfig.JobRootPath = path

    err = db.SaveServerStoreConfig(&serverConfig)
    if err != nil {
        StoneLogger.Errorf("Can't save server config %s\n",
            err.Error())
        return err
    }

    return nil
}

