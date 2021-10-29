package xstone

import (
	"errors"
	"fmt"
	bioconfig "github.com/xtao/bioflow/confbase"
	"os/user"
	"strconv"
	"time"

	"github.com/docker/go-plugins-helpers/volume"
	. "github.com/xtao/xstone/common"
	. "github.com/xtao/xstone/config"
	. "github.com/xtao/xstone/src/automount-manager/plugin"
	"github.com/xtao/xstone/src/automount-manager/server"
	"github.com/xtao/xstone/storage"
)

const (
	XSTONE_WATCH_RETRY_INTERVAL time.Duration = 30 * time.Second
)

const (
	/*
	 * start as emulation mode:
	 * 1) load local config file
	 * 2) run simulator scheduler backend
	 */
	//XSTONE_MODE_EMULATION string = "emulation"
	/*
	 * start as docker mode:
	 * 1) load config from etcd
	 * 2) run ermetic or kubernettes scheduler backend
	 */
	XSTONE_MODE_DOCKER string = "docker"
	/*
	 * start as local mode:
	 * 1) load config from local config file
	 * 2) run ermetic or kubernettes scheduler backend
	 */
	//XSTONE_MODE_LOCAL string = "local"
)

type LoggerConfig struct {
	Logfile  string
	LogLevel int
}

func XstoneStartWatchers(db *bioconfig.BioConfDB) error {
	mountConfigWatcher := db.NewStorageClusterConfigWatcher()
	mountMgr := storage.GetMountMgr()

	go func() {
		for {
			err, updated, removed := mountConfigWatcher.WatchChanges()
			if err != nil {
				StoneLogger.Errorf("storage cluster config watch error: %s\n",
					err.Error())
				time.Sleep(XSTONE_WATCH_RETRY_INTERVAL)
				continue
			}

			updateList := make([]StorageClusterMountConfig, 0)
			if updated != nil {
				updateList = append(updateList, *updated)
			}
			removeList := make([]StorageClusterMountConfig, 0)
			if removed != nil {
				removeList = append(removeList, *removed)
			}
			err = mountMgr.UpdateMounters(updateList, removeList)
			if err != nil {
				StoneLogger.Errorf("Fail to update the mounters config: %s\n",
					err.Error())
			}
		}
	}()

	return nil
}

func XstoneStart(logLevel int, rootDir string) error {
	/*step 1: initialize the logger facility*/
	loggerConfig := LoggerConfig{
		Logfile:  "/var/log/xstone.log",
		LogLevel: 0,
	}

	if logLevel != -1 {
		loggerConfig.LogLevel = logLevel
	}

	XLoggerInit(loggerConfig.Logfile, loggerConfig.LogLevel)

	/*step 2: load ENV info*/
	envSetting := EnvUtilsParseEnvSetting()

	/*step 3: get etcd info,the xstone only uses with compute etcd*/
	var err error
	var confDB *bioconfig.BioConfDB = nil
	var clusterMountConfig []StorageClusterMountConfig = nil

	confDB = bioconfig.NewBioConfDB(envSetting.EtcdEndPoints)
	err, clusterMountConfig = confDB.LoadStorageClusterConfig()
	if err != nil {
		StoneLogger.Errorf("Fail to load storage cluster config: %s\n",
			err.Error())
	}

	/*Only use for debug*/
	if loggerConfig.LogLevel == 0 {
		i := 0
		for _, clusterInfo := range clusterMountConfig {
			i++
			StoneLogger.Infof("The %d cluster info is:%s\n", i, clusterInfo)
		}
	}

	if clusterMountConfig != nil {
		StoneLogger.Infof("Xstone update mount according cluster mount config start\n")
		err = storage.GetMountMgr().UpdateMounters(clusterMountConfig, nil)
		if err != nil {
			StoneLogger.Errorf("fail to update mounters: %s\n",
				err.Error())
			return err
		}
		StoneLogger.Infof("Xstone update mount according cluster mount config end\n")
	}

	err = XstoneStartWatchers(confDB)
	if err != nil {
		StoneLogger.Errorf("Xstone fail to create watchers: %s\n",
			err.Error())
		return err
	}

	/*step 4:Storage manager Plugin server*/
	storageManagerChan := make(chan bool)
	if rootDir != "" {
		envSetting.PluginVolume = rootDir
		server.RootMountDir = rootDir
	} else {
		server.RootMountDir = envSetting.PluginVolume
	}
	driver, err := NewStorageManagerDriver(envSetting.PluginVolume)
	if err != nil {
		StoneLogger.Errorf("Failed to new plugin driver")
		return errors.New("Failed to new plugin driver")
	}
	handler := volume.NewHandler(driver)
	u, uErr := user.Lookup("root")
	if uErr != nil {
		StoneLogger.Errorf("Failed to find user root UnKnowUserError %v\n", uErr.Error())
		return errors.New("Can't find the user root")
	}
	gid, _ := strconv.Atoi(u.Gid)
	StoneLogger.Infof("Storage manager plugin server start!")
	go func() {
		StoneLogger.Infof("Plugin go routine start!")
		err := handler.ServeUnix(driver.Name, gid)
		if err != nil {
			StoneLogger.Errorf("Storage manager plugin server error: %v\n", err.Error())
			storageManagerChan <- true
		}
	}()
	StoneLogger.Infof("Storage manager plugin server start end!")

	/*step 5:Auto mount Restful server*/
	restServiceChan := make(chan bool)
	restAddr := fmt.Sprintf(":%s", envSetting.XstoneRestPort)
	StoneLogger.Infof("Xstone starts REST Server on %s Now\n",
		restAddr)
	xstoneServer := server.NewSTONERESTServer(restAddr)
	go func() {
		xstoneServer.StartRESTServer()
		restServiceChan <- true
	}()
	StoneLogger.Infof("Wait for REST service done\n")
	select {
	case <-storageManagerChan:
		return err
	case <-restServiceChan:
		return errors.New("Rest server chan exit")
	}
	return nil
}
