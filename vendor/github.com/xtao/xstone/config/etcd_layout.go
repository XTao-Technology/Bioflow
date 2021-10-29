package config

import (
    "fmt"
    "strings"
    "errors"

    "github.com/xtao/xstone/common"
) 

/*
 * The layout of xstone configuraiton space on ETCD is as follows:
 *
 *
 * / storagemgr
 *
 *            storage
 *                      mappers
 *                               server
 *                               scheduler
 *                               container
 *                      clusters
 *                               cluster1
 *                               cluster2
 */

const (
    ETCD_KEY_STORAGE_SERVER string = "/storagemgr/storage/mapper/server"
    ETCD_KEY_STORAGE_SCHEDULER string = "/storagemgr/storage/mapper/scheduler"
    ETCD_KEY_STORAGE_CONTAINER string = "/storagemgr/storage/mapper/container"
    ETCD_KEY_STORAGE string = "/storagemgr/storage"
    ETCD_KEY_STORAGE_MAPPER string = "/storagemgr/storage/mapper"
    ETCD_KEY_STORAGE_CLUSTER string = "/storagemgr/storage/clusters"
)

func ConfBuildStorageClusterConfigKey(cluster string, vol string) string {
    key := cluster
    if vol != "" {
        key = common.FSCommonUtilsBuildVolURI(cluster, vol)
    }
    return fmt.Sprintf("%s/%s", ETCD_KEY_STORAGE_CLUSTER, key)
}

func ConfParseStorageClusterKeyToName(key string) (error, string) {
    if key == "" {
        return errors.New("Empty key"), ""
    }

    dirItems := strings.Split(key, "/")
	if len(dirItems) < 2 {
		return errors.New("Invalid key for storage cluster"), ""
	}

	return nil, dirItems[len(dirItems) - 1]
}
