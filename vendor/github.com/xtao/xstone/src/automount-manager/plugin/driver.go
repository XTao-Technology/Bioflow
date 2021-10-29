package plugin

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"

	"github.com/docker/go-plugins-helpers/volume"
	. "github.com/xtao/xstone/common"
	. "github.com/xtao/xstone/src/automount-manager/common"

	"github.com/xtao/xstone/storage"
)

var (
	TrueFlag  = 1
	FalseFlag = 0
)

type StorageManagerDriver struct {
	volumes                map[string]string
	m                      *sync.Mutex
	mountPoint             string
	Name                   string
	pluginVolumeMountFlag  map[string]int //TrueFlag:no mounted,can mount.or has beened mounted
	ClusterVolumeMountFlag map[string]int
}

var driver *StorageManagerDriver = nil

func NewDriverMgr() *StorageManagerDriver {
	return &StorageManagerDriver{
		volumes: make(map[string]string),
		Name:    "AutoMountManager",
		ClusterVolumeMountFlag: make(map[string]int),
	}
}

func GetDriverMgr() *StorageManagerDriver {
	if driver == nil {
		driver = NewDriverMgr()
	}

	return driver
}

/*Plugin volume info end*/

func NewStorageManagerDriver(StorageMnagerPluginVolume string) (*StorageManagerDriver, error) {
	StoneLogger.Infof("Plugin start.....")
	os.Mkdir(StorageMnagerPluginVolume, 0750)

	mountVolumes := make(map[string]string)

	driver = &StorageManagerDriver{
		volumes:    mountVolumes,
		m:          &sync.Mutex{},
		mountPoint: StorageMnagerPluginVolume,
		Name:       "AutoMountManager",
		pluginVolumeMountFlag:  make(map[string]int),
		ClusterVolumeMountFlag: make(map[string]int),
	}

	/*Load last exist volume*/
	volumesInfo, err := LoadPluginVolumeInfoFromConfig()
	if err != nil {
		StoneLogger.Errorf("Load volumes json file error:", err)
		return driver, err
	}

	for _, v := range volumesInfo {
		driver.volumes[v.Name] = v.MountPoint
		driver.pluginVolumeMountFlag[v.Name] = TrueFlag
		driver.ClusterVolumeMountFlag[v.Name] = TrueFlag
		StoneLogger.Infof("Name: %v, MountPoint: %v", v.Name, v.MountPoint)
	}

	return driver, nil
}

func (driver *StorageManagerDriver) Create(req volume.Request) volume.Response {
	StoneLogger.Infof("INFO: API Create(%s), r.Name %s", req, req.Name)
	driver.m.Lock()

	// check for mount
	// do we already know about this volume? return early
	if _, found := driver.volumes[req.Name]; found {
		StoneLogger.Infof("INFO: Volume is already in known mounts: %s", driver.volumes[req.Name])
		driver.m.Unlock()
		return volume.Response{}
	}

	volumePath := driver.buildMountPath(req.Name)
	StoneLogger.Infof("volumePath is :%s\n", volumePath)

	err := Mkdir(volumePath)
	if err != nil {
		StoneLogger.Errorf("Create path %s error and info is: %v", volumePath, err.Error())
		driver.m.Unlock()
		return volume.Response{Err: fmt.Sprintf("Error: create: %s, %s", volumePath, err.Error())}
	}

	driver.volumes[req.Name] = volumePath
	driver.pluginVolumeMountFlag[req.Name] = TrueFlag
	driver.ClusterVolumeMountFlag[req.Name] = TrueFlag

	clusterVolumesInfoArray := make([]ClusterVolumeInfo, 0)

	vol := Volume{
		Name:       req.Name,
		MountPoint: volumePath,

		ClusterVolumesInfo: clusterVolumesInfoArray,
	}

	driver.m.Unlock()
	/*data persistence*/
	err = PersistentPluginVolumeToConfig(vol)
	if err != nil {
		StoneLogger.Errorf("Date persistence failed when create plugin volume, error: %v", err.Error())
		driver.m.Lock()
		delete(driver.volumes, req.Name)
		delete(driver.pluginVolumeMountFlag, req.Name)
		delete(driver.ClusterVolumeMountFlag, req.Name)
		driver.m.Unlock()
		return volume.Response{Err: fmt.Sprintf("Date persistence failed when create plugin volume, error: %v", err.Error())}
	}

	StoneLogger.Infof("INFO: API Create(%s), r.Name %s, Success", req, req.Name)
	return volume.Response{}
}

func (driver *StorageManagerDriver) List(req volume.Request) volume.Response {
	StoneLogger.Infof("INFO: API List(%s), r.Name %s", req, req.Name)
	driver.m.Lock()
	defer driver.m.Unlock()

	var vols []*volume.Volume

	//vols := make([]*volume.Volume, 0, len(driver.volumes))

	for k, v := range driver.volumes {
		vols = append(vols, &volume.Volume{
			Name:       k,
			Mountpoint: v,
		})
	}

	StoneLogger.Infof("INFO: List request => %s, Success", vols)

	return volume.Response{Volumes: vols}
}

func (driver *StorageManagerDriver) Get(req volume.Request) volume.Response {
	StoneLogger.Infof("INFO: API Get(%s), r.Name %s", req, req.Name)
	driver.m.Lock()
	defer driver.m.Unlock()
	mountPath := driver.buildMountPath(req.Name)
	StoneLogger.Infof("INFO: Get request => mountPath :%s", mountPath)

	for k, v := range driver.volumes {
		StoneLogger.Infof("INFO: name %s, buildMountPath %s", k, v)
		if strings.Compare(k, req.Name) == 0 {
			return volume.Response{Volume: &volume.Volume{Name: req.Name, Mountpoint: mountPath}}
		}
	}

	StoneLogger.Infof("INFO: Get request Success")
	return volume.Response{}
}

func (driver *StorageManagerDriver) Remove(req volume.Request) volume.Response {
	StoneLogger.Infof("INFO: API Remove(%s), r.Name %s", req, req.Name)
	driver.m.Lock()

	mountPath := driver.buildMountPath(req.Name)

	StoneLogger.Infof("INFO: Remove request => mountPath :%s", mountPath)

	driver.m.Unlock()
	/*Umount the cluster volumes*/
	err := driver.UMountClusterVolumes(req.Name)
	if err != nil {
		StoneLogger.Errorf("Umount cluster vol failed :%v\n", err.Error())
		return volume.Response{Err: fmt.Sprintf("Error: remove: %s, %s", mountPath, err.Error())}
	}
	/*In this way, we always success*/
	Rmdir(mountPath)

	/*update json file content which data persistence in*/
	err = ClearPluginVolumeFromConfig(req.Name)
	if err != nil {
		StoneLogger.Errorf("Delete the volume from json file failed, err:%v", err.Error())
		return volume.Response{Err: fmt.Sprintf("Error: remove: %s, %s", mountPath, err.Error())}
	}

	driver.m.Lock()
	delete(driver.volumes, req.Name)
	delete(driver.pluginVolumeMountFlag, req.Name)
	delete(driver.ClusterVolumeMountFlag, req.Name)
	driver.m.Unlock()
	StoneLogger.Infof("INFO: Remove request => %s, Success", req.Name)
	return volume.Response{}
}

func (driver *StorageManagerDriver) Path(req volume.Request) volume.Response {
	StoneLogger.Infof("INFO: API Path(%s), r.Name %s", req, req.Name)
	driver.m.Lock()
	defer driver.m.Unlock()
	mountPath := driver.buildMountPath(req.Name)

	StoneLogger.Infof("INFO: Path request => mountPath :%s, Success", mountPath)

	return volume.Response{Mountpoint: mountPath}
}

func (driver *StorageManagerDriver) Mount(req volume.MountRequest) volume.Response {
	StoneLogger.Infof("INFO: API Mount(%s), r.Name %s, ID: %v", req, req.Name, req.ID)
	driver.m.Lock()
	defer driver.m.Unlock()

	flag, ok := driver.pluginVolumeMountFlag[req.Name]
	if ok && flag == FalseFlag {
		StoneLogger.Errorf("AutoMountManager plugin only support mount once,the volume has been mounted")
		return volume.Response{Err: fmt.Sprintf("Error: mount: %s, AutoMountManager plugin only support mount once,the volume has been mounted", driver.volumes[req.Name])}
	}

	mountPath := driver.buildMountPath(req.Name)
	driver.pluginVolumeMountFlag[req.Name] = FalseFlag

	StoneLogger.Infof("INFO: Mount request => mountPath :%s", mountPath)

	return volume.Response{Volume: &volume.Volume{Name: req.Name, Mountpoint: mountPath}}
}

func (driver *StorageManagerDriver) Unmount(req volume.UnmountRequest) volume.Response {
	StoneLogger.Infof("INFO: API UnMount(%s), r.Name %s, ID: %v", req, req.Name, req.ID)
	driver.m.Lock()

	volumePath := driver.buildMountPath(req.Name)
	StoneLogger.Infof("volumePath is :%s\n", volumePath)

	driver.m.Unlock()
	/*Umount the cluster volumes*/
	err := driver.UMountClusterVolumes(req.Name)
	if err != nil {
		StoneLogger.Errorf("Umount cluster vol failed :%v\n", err.Error())
		return volume.Response{Err: fmt.Sprintf("Error: umount: %s, %s", volumePath, err.Error())}
	}

	driver.pluginVolumeMountFlag[req.Name] = TrueFlag

	StoneLogger.Infof("INFO: UnMount request => mountPath Success")
	return volume.Response{}
}

func (driver StorageManagerDriver) Capabilities(req volume.Request) volume.Response {

	return volume.Response{
		Capabilities: volume.Capability{
			Scope: "global",
		},
	}
}

/*Clear storage package mount mgr mount map*/
func (driver *StorageManagerDriver) UMountClusterVolumes(pluginVolumeName string) error {
	/*Get the volume about cluater vol*/
	volumesInfo, err := LoadPluginVolumeInfoFromConfig()
	StoneLogger.Infof("UmountClusterVolumes func get json file volumes info: %v", volumesInfo)
	if err != nil {
		StoneLogger.Errorf("Load volumes json file error: %v", err.Error())
		return err
	}

	var clusterVol string
	var clusterName string
	var mountPoint string

	for _, pluginVolume := range volumesInfo {
		StoneLogger.Infof("pluginVolume: %v", pluginVolume)
		if pluginVolume.Name == pluginVolumeName {
			for _, value := range pluginVolume.ClusterVolumesInfo {
				StoneLogger.Infof("value: %v", value)
				clusterVol = value.ClusterVol
				clusterName = value.Cluster
				mountPoint = value.MountPath
				StoneLogger.Infof("clusterVol: %v, clusterName: %v, buildMountPath: %v", clusterVol, clusterName, mountPoint)
				break
			}
		}
	}

	if clusterVol == "" || clusterName == "" {
		StoneLogger.Infof("The cluster volume has not been mounted, we not need umount")
		return nil
	}

	err = storage.GetMountMgr().UMountVolume(pluginVolumeName, clusterVol, clusterName, mountPoint, TrueFlag, UMOUNT_TIMEOUT)
	if err != nil {
		StoneLogger.Errorf("Umount clustervol failed")
		return err
	}

	err = DeleteClusterVolumeInfo(mountPoint, pluginVolumeName)
	if err != nil {
		StoneLogger.Errorf("Delete cluster volume info from config failed!")
		return err
	}

	return nil
}

func (driver *StorageManagerDriver) buildMountPath(name string) string {
	return filepath.Join(driver.mountPoint, name)
}
