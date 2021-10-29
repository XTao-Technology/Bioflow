package storage

import (
	"errors"
	"fmt"
	"os"
	"sync"
	"syscall"

	. "github.com/xtao/xstone/common"
	. "github.com/xtao/xstone/config"
)

const (
	MOUNT_TIMEOUT int    = 120
	XTAO_MNT_BASE string = "/mnt/xtao"
)

/*
 * The scheduler should automatically mount/umount a file system
 * when it need access it. This keep tracks all the mount points.
 */
type fsMountMgr struct {
	mapLock  sync.RWMutex
	mounters map[string]FSMounter
	mounts   map[string]string
	waiters  map[string]*TimedWaiter
}

func NewFSMountManager() *fsMountMgr {
	return &fsMountMgr{
		mounters: make(map[string]FSMounter),
		mounts:   make(map[string]string),
		waiters:  make(map[string]*TimedWaiter),
	}
}

var mountMgr *fsMountMgr = nil

func GetMountMgr() *fsMountMgr {
	if mountMgr == nil {
		mountMgr = NewFSMountManager()
	}

	return mountMgr
}

func (mgr *fsMountMgr) UpdateMounters(updated []StorageClusterMountConfig,
	removed []StorageClusterMountConfig) error {

	if updated != nil {
		/*Update the mounters*/
		for i := 0; i < len(updated); i++ {
			configItem := updated[i]
			mounter := NewFSMounter(&configItem)
			if mounter == nil {
				StoneLogger.Errorf("Fail to create mounter for cluster %s\n",
					configItem.ClusterName)
			} else {
				StoneLogger.Infof("Updated mounter for cluster %s volume %s\n",
					configItem.ClusterName, configItem.Volume)
				mgr.mapLock.Lock()
				if configItem.Volume == "" {
					mgr.mounters[configItem.ClusterName] = mounter
				} else {
					/*the mounter config is cluster-volume level*/
					volConfigKey := FSCommonUtilsBuildVolURI(configItem.ClusterName,
						configItem.Volume)
					mgr.mounters[volConfigKey] = mounter
				}
				mgr.mapLock.Unlock()
			}
		}
	}

	/*Remove the mounter*/
	if removed != nil {
		for i := 0; i < len(removed); i++ {
			configItem := removed[i]
			/*
			 * Noted that the ClusterName here is actually the key of the config
			 * item in etcd. because for a removed key, we can't get its config,
			 * and only the key. So the ClusterName is hacked to save:
			 * 1) the real cluster name for a per-cluster config
			 * 2) volume URI for cluster-volume level config
			 */
			key := configItem.ClusterName
			if _, ok := mgr.mounters[key]; ok {
				StoneLogger.Infof("Remove the  mounter for cluster %s \n",
					configItem.ClusterName)
				mgr.mapLock.Lock()
				delete(mgr.mounters, key)
				mgr.mapLock.Unlock()
			} else {
				StoneLogger.Infof("The mounter for cluster %s not found, don't remove it\n",
					configItem.ClusterName)
			}
		}
	}

	return nil
}

func (mgr *fsMountMgr) GetVolumeMount(vol string, cluster string) (error, string) {
    return mgr._GetVolumeMount("XStoneGlobalMountOwner", vol, cluster)
}

func (mgr *fsMountMgr) _GetVolumeMount(pluginVolume string, vol string, cluster string) (error, string) {
	volURI := FSPluginCommonUtilsBuildVolURI(pluginVolume, cluster, vol)
	mgr.mapLock.RLock()
	if mountpoint, ok := mgr.mounts[volURI]; ok {
		mgr.mapLock.RUnlock()
		return nil, mountpoint
	}
	mgr.mapLock.RUnlock()

	mountTarget := ""
	if cluster == "" {
		mountTarget = fmt.Sprintf("%s/local-%s", XTAO_MNT_BASE, vol)
	} else {
		mountTarget = fmt.Sprintf("%s/%s-%s", XTAO_MNT_BASE, cluster, vol)
	}
	err := mgr.OwnerMountVolume(pluginVolume, vol, cluster, mountTarget, MOUNT_TIMEOUT)
	if err != nil {
		return err, ""
	}

	mgr.mapLock.RLock()
	if mountpoint, ok := mgr.mounts[volURI]; ok {
		mgr.mapLock.RUnlock()
		return nil, mountpoint
	} else {
		mgr.mapLock.RUnlock()
		StoneLogger.Errorf("Supposed to mount success, but can't find mountpoint for %s\n",
			volURI)
		return errors.New("Mount fail for vol " + volURI), ""
	}
}

func (mgr *fsMountMgr) ClearWaiter(volURI string) {
	mgr.mapLock.Lock()
	defer mgr.mapLock.Unlock()

	delete(mgr.waiters, volURI)
}

func (mgr *fsMountMgr) OwnerMountVolume(pluginVolume string, vol string, cluster string, mountpoint string,
	timeout int) error {
	isMounter := true
	volURI := FSPluginCommonUtilsBuildVolURI(pluginVolume, cluster, vol)

	/*sync with possible other mounters*/
	mgr.mapLock.Lock()
	if _, ok := mgr.mounts[volURI]; ok {
		/*others already mount it*/
		mgr.mapLock.Unlock()
		return nil
	}

	waiter, found := mgr.waiters[volURI]
	if !found {
		waiter = NewTimedWaiter()
		mgr.waiters[volURI] = waiter
	} else {
		isMounter = false
	}
	mgr.mapLock.Unlock()

	/*
	 * For one volume, only allow one thread to mount. so
	 * if the thread is not the mounter, it only waits for
	 * others to complete the mount operation.
	 */
	if !isMounter {
		err := waiter.Wait(timeout)
		if err == TW_WAIT_TIMEOUT {
			StoneLogger.Errorf("Timeout on mount volume %s \n",
				volURI)
			return errors.New("mount timeout")
		} else if err != nil {
			StoneLogger.Errorf("Fail to mount the volume %s:%s\n",
				volURI, err.Error())
			return err
		}

		return nil
	}

	mgr.mapLock.RLock()
	targetCluster := cluster
	if targetCluster == "" {
		targetCluster = "default"
	}
	/*
	 * Two kinds of mount config exist:
	 * 1) per-cluster mount config. e.g, alamo, gluster, ceph
	 * 2) per-cluster-volume mount config: e.g, the legacy NFS mount,
	 *    the different volume may be mounted from different path of
	 *    same storage cluster. In this case, mount config is stored
	 *    and indexed by vol URI.
	 */
	mounter, found := mgr.mounters[targetCluster]
	if !found {
		volConfigKey := FSCommonUtilsBuildVolURI(targetCluster, vol)
		mounter, found = mgr.mounters[volConfigKey]
	}

	if !found || mounter == nil {
		mgr.mapLock.RUnlock()
		mgr.ClearWaiter(volURI)

		StoneLogger.Infof("No mounter config found for cluster %s\n",
			cluster)
		waiter.Signal()
		return errors.New("No mounter config for " + cluster)
	}
	mgr.mapLock.RUnlock()

	StoneLogger.Infof("Create the mountpoint %s\n",
		mountpoint)
	oldMask := syscall.Umask(0)
	err := os.MkdirAll(mountpoint, os.ModePerm)
	syscall.Umask(oldMask)

	if err != nil {
		StoneLogger.Errorf("Fail to create the mountpoint %s: %s\n",
			mountpoint, err.Error())
		mgr.ClearWaiter(volURI)
		waiter.Signal()
		return err
	}

	StoneLogger.Infof("Try to mount %s on %s\n",
		volURI, mountpoint)
	err, mntPath := mounter.MountVolume(vol, mountpoint, timeout)
	if err != nil {
		StoneLogger.Errorf("mounter fail to mount the volume %s to %s: %s\n",
			volURI, mountpoint, err.Error())
		mgr.ClearWaiter(volURI)
		waiter.Signal()
        fail_reason := "mounter fail to mount the volume " + volURI + " to" + mountpoint + ": " + err.Error()
		return errors.New(fail_reason)
	}
	StoneLogger.Infof("The vol %s mounted on %s\n",
		volURI, mountpoint)

	mgr.mapLock.Lock()
	mgr.mounts[volURI] = mntPath
	delete(mgr.waiters, volURI)
	mgr.mapLock.Unlock()
	waiter.Signal()

	return nil
}

/*If we don't concern plugin volume call this func to mount volume,if not last func we should direct call */
func (mgr *fsMountMgr) MountVolume(vol string, cluster string, mountpoint string,
	timeout int) error {
	return mgr.OwnerMountVolume("XStoneGlobalMountOwner", vol, cluster, mountpoint, timeout)
}

/*Umount for the auto umount command, the umountFlag is umount preferences, num 0 is umount for nothing flag,but num 1 is -l for force umount*/
func (mgr *fsMountMgr) UMountVolume(pluginVolume string, vol string, cluster string, mountpoint string, umountFlag int, timeout int) error {
	volURI := FSPluginCommonUtilsBuildVolURI(pluginVolume, cluster, vol)
	/*sync with possible other mounters*/
	mgr.mapLock.Lock()

	var umountCommand string
	isMounter := true

	/*sync with possible other mounters*/
	if _, ok := mgr.mounts[volURI]; ok {
		waiter, found := mgr.waiters[volURI]
		if !found {
			waiter = NewTimedWaiter()
			mgr.waiters[volURI] = waiter
		} else {
			isMounter = false
		}

		/*
		 * For one volume, only allow one thread to mount. so
		 * if the thread is not the mounter, it only waits for
		 * others to complete the mount operation.
		 */
		mgr.mapLock.Unlock()
		if !isMounter {
			err := waiter.Wait(timeout)
			if err == TW_WAIT_TIMEOUT {
				StoneLogger.Errorf("Timeout on umount volume %s \n",
					volURI)
				return errors.New("umount timeout")
			} else if err != nil {
				StoneLogger.Errorf("Fail to umount the volume %s:%s\n",
					volURI, err.Error())
				return err
			}

			return nil
		}
		/*others already mount it, we need umount it*/
		if umountFlag != 0 {
			umountCommand = "umount -l " + mountpoint
		} else {
			umountCommand = "umount " + mountpoint
		}
		StoneLogger.Infof("The umount command is :%s\n", umountCommand)
		err := RunCommandAsync(umountCommand, 0)
		if err != nil {
			mgr.ClearWaiter(volURI)
			StoneLogger.Errorf("The umount command exec failed, err: %v", err.Error())
			return err
		}

		deleteMountDirCmd := "rm -rf " + mountpoint
		err = RunCommandAsync(deleteMountDirCmd, 0)
		if err != nil {
			mgr.ClearWaiter(volURI)
			StoneLogger.Errorf("The umount command exec failed, err: %v", err.Error())
			return err
		}

		mgr.mapLock.Lock()
		delete(mgr.mounts, volURI)
		delete(mgr.waiters, volURI)
		mgr.mapLock.Unlock()
		return nil
	} else {
		mgr.mapLock.Unlock()
		StoneLogger.Infof("The vol mountpoint:%s has not been mounted, so need not umount", mountpoint)
		return nil
	}
}

func (mgr *fsMountMgr) GetAllStorageManifests() []*StorageManifest {
	mgr.mapLock.Lock()
    defer mgr.mapLock.Unlock()

    manifests := make([]*StorageManifest, 0)
    for _, mounter := range mgr.mounters {
        manifests = append(manifests, mounter.GetManifest())
    }

    return manifests
}

func (mgr *fsMountMgr) GetStorageManifest(cluster string) *StorageManifest {
	mgr.mapLock.RLock()
	defer mgr.mapLock.RUnlock()
	if manifest, ok := mgr.mounters[cluster]; !ok {
		return nil
	} else {
		return manifest.GetManifest()
	}
}