package config

import (
    "encoding/json"
	"errors"

    etcdclient "github.com/coreos/etcd/client"
    "golang.org/x/net/context"

    . "github.com/xtao/xstone/common"
    )

type BaseConfWatcher struct {
	watcher etcdclient.Watcher
	ctxt context.Context
	key string
    db *ETCDConfDB
}

func (watcher *BaseConfWatcher)WatchNext() (error, *etcdclient.Response) {
	res, err := watcher.watcher.Next(watcher.ctxt)
	if err != nil {
		StoneLogger.Errorf("watch error on key %s: %s\n",
			watcher.key, err.Error())

        /*
         * Watch error may be caused by network problems and etcd internal
         * failures. So we re-init the watcher to avoid the un-healed errors
         * caused by outdated event index. but return error anyway.
         */
        wopt := etcdclient.WatcherOptions {
            Recursive: true,
        }
        watcher.watcher = watcher.db.keysAPI.Watcher(watcher.key, &wopt)

		return err, nil
	}

	return nil, res
}

type StoreMgrConfigWatcher struct {
	BaseConfWatcher
}

func (watcher *StoreMgrConfigWatcher)WatchChanges() (error, 
	*ServerStoreConfig, *SchedulerStoreConfig, *ContainerStoreConfig) {
	err, res := watcher.WatchNext()
	if err != nil {
		StoneLogger.Errorf("StoreMgr config watch error: %s\n",
			err.Error())
		return err, nil, nil, nil
	}

	if res.Node != nil {
		switch res.Node.Key {
			case ETCD_KEY_STORAGE_SERVER:
				serverConfig := &ServerStoreConfig{}
				err = json.Unmarshal([]byte(res.Node.Value), serverConfig)
				if err != nil {
					StoneLogger.Errorf("Can't decode server storage config: %s\n",
						err.Error())
					return err, nil, nil, nil
				}
				return nil, serverConfig, nil, nil
			case ETCD_KEY_STORAGE_SCHEDULER:
				schedulerConfig := &SchedulerStoreConfig{}
				err = json.Unmarshal([]byte(res.Node.Value), schedulerConfig)
				if err != nil {
					StoneLogger.Errorf("Can't decode scheduler storage config: %s\n",
						err.Error())
					return err, nil, nil, nil
				}
				return nil, nil, schedulerConfig, nil
			case ETCD_KEY_STORAGE_CONTAINER:
				containerConfig := &ContainerStoreConfig{}
				err = json.Unmarshal([]byte(res.Node.Value), containerConfig)
				if err != nil {
					StoneLogger.Errorf("Can't decode server storage config: %s\n",
						err.Error())
					return err, nil, nil, nil
				}
				return nil, nil, nil, containerConfig
			default:
				StoneLogger.Errorf("Watched a unknown key: %s \n",
					res.Node.Key)
				return errors.New("Unknown key " + res.Node.Key), nil, nil, nil
		}
	} else {
		return errors.New("Strange error empty watcher result"), nil, nil, nil
	}
}

type StorageClusterConfigWatcher struct {
	BaseConfWatcher
}

/*
 * Watcher waits for storage cluster config changes, it will return caller the:
 * 1) error
 * 2) updated or created config, nil if no
 * 3) deleted instance, nil if no
 */
func (watcher *StorageClusterConfigWatcher) WatchChanges() (error, 
    *StorageClusterMountConfig, *StorageClusterMountConfig) {
	err, res := watcher.WatchNext()
	if err != nil {
		StoneLogger.Errorf("bioflow storage cluster config watch error: %s\n",
			err.Error())
		return err, nil, nil
	}
    config := &StorageClusterMountConfig{}
    switch res.Action {
        case "set", "create":
            err = json.Unmarshal([]byte(res.Node.Value), config)
            if err != nil {
                StoneLogger.Errorf("Can't unmarshal storage cluster config %s: %s\n",
                    res.Node.Key, err.Error())
                return err, nil, nil
            }
            return nil, config, nil
        case "delete":
            /*
             * This is a hack
             * in some cases which config item is volume@cluster, the key is a volume
             * URI rather than cluster name. So just save the key to cluster name.
             * make sure that the mount manager config watch and update works in exactly
             * same way 
             */
            err, config.ClusterName = ConfParseStorageClusterKeyToName(res.Node.Key)
            if err != nil {
                StoneLogger.Errorf("Can't parse deleted instance config key %s: %s\n",
                    res.Node.Key, err.Error())
                return err, nil, nil
            }
            return nil, nil, config
        default:
            return errors.New("Unknown action " + res.Action), nil,
                nil
    }
}

