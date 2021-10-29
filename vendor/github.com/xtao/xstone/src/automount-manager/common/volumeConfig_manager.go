package common

import (
	"encoding/json"
	"io/ioutil"
	"os"
	"sync"

	. "github.com/xtao/xstone/common"
)

var (
	VolumeJsonFile = "/opt/xstone/volumeConfig.json"
	UMOUNT_TIMEOUT = 120
	m              = new(sync.RWMutex)
)

/*Cluster volume info start*/
type ClusterVolumeInfo struct {
	/*The cluster info*/
	ClusterVol string
	Cluster    string
	MountPath  string
}

type Volume struct {
	Name               string
	MountPoint         string
	ClusterVolumesInfo []ClusterVolumeInfo
}

/*Cluster volume info end*/

type AlreadyExistedVolumes struct {
	Volumes []Volume
}

func Mkdir(volumePath string) error {
	cmd := "mkdir -p " + volumePath
	err := RunCommandAsync(cmd, 0)
	if err != nil {
		StoneLogger.Errorf("Create path %s error and info is: %v", volumePath, err.Error())
		return err
	}

	_, err = os.Lstat(volumePath)
	if err != nil {
		StoneLogger.Errorf("Error %s %v", volumePath, err.Error())
		return err
	}

	return nil
}

func Rmdir(mountPath string) error {
	rmCommand := "rm -rf " + mountPath
	sum := 0

	for {
		sum++
		if sum >= 10 {
			break
		}

		err := RunCommandAsync(rmCommand, 0)
		if err != nil {
			StoneLogger.Errorf("Rm %s error and info is: %v", mountPath, err.Error())
		}
	}

	return nil
}

func LoadPluginVolumeInfoFromConfig() ([]Volume, error) {
	m.RLock()
	defer m.RUnlock()

	newVolumesInfo := make([]Volume, 1)
	file, err := ioutil.ReadFile(VolumeJsonFile)
	if err != nil {
		StoneLogger.Errorf("Read file error: %v", err.Error())
		return newVolumesInfo, err
	}
	var volumesInfo AlreadyExistedVolumes
	err = json.Unmarshal(file, &volumesInfo)
	if err != nil {
		StoneLogger.Errorf("Load volumes json file error: %v", err.Error())
		return newVolumesInfo, err
	}

	StoneLogger.Infof("Load volume from config is:%v", volumesInfo.Volumes)
	return volumesInfo.Volumes, nil
}

func UpdateJsonDataToConfig(pluginVolumeInfo AlreadyExistedVolumes) error {
	m.Lock()
	defer m.Unlock()

	info_json, _ := json.Marshal(pluginVolumeInfo)
	err := ioutil.WriteFile(VolumeJsonFile, info_json, 0x644)
	if err != nil {
		StoneLogger.Errorf("Write volumes json file error: %v", err.Error())
		return err
	}

	return nil
}

/*The func write file only support one volume*/
func PersistentPluginVolumeToConfig(vol Volume) error {
	volumes, err := LoadPluginVolumeInfoFromConfig()
	if err != nil {
		StoneLogger.Errorf("Read file failed, err: %v", err.Error())
		return err
	}

	newVolumesArray := make([]Volume, len(volumes)+1)

	for k, v := range volumes {
		newVolumesArray[k] = volumes[k]
		StoneLogger.Infof("volumes: %v", v)
	}
	newVolumesArray[len(volumes)] = vol
	newPluginVolumeInfo := AlreadyExistedVolumes{
		Volumes: newVolumesArray,
	}

	err = UpdateJsonDataToConfig(newPluginVolumeInfo)
	if err != nil {
		StoneLogger.Errorf("Update config after Persistent the plugin volume info!")
		return err
	}
	return nil
}

/*Used for remove API*/
func ClearPluginVolumeFromConfig(name string) error {
	pluginVolumeInfo, err := LoadPluginVolumeInfoFromConfig()
	if err != nil {
		StoneLogger.Errorf("Load config failed, err: %v", err.Error())
		return err
	}
	count := 0
	result := make([]Volume, 0)
	for _, v := range pluginVolumeInfo {
		if v.Name == name {
			StoneLogger.Infof("We will delete from the file is num:%d, name:%v", count, v.Name)
			break
		}
		count++
	}

	result = append(pluginVolumeInfo[:count], pluginVolumeInfo[count+1:]...)
	remainPluginVolumeinfo := AlreadyExistedVolumes{
		Volumes: result,
	}

	err = UpdateJsonDataToConfig(remainPluginVolumeinfo)
	if err != nil {
		StoneLogger.Errorf("Update config after clear the plugin volume info!")
		return err
	}
	return nil
}

/*Used for mount volume for http server*/
func AppendClusterVolumeInfo(ClusterName string, ClusterVolume string, MountPath string, PluginVolume string) error {
	pluginVolumeInfo, err := LoadPluginVolumeInfoFromConfig()

	StoneLogger.Infof("The append cluster volume info is, ClusterName: %s, ClusterVolume: %s, MountPath: %s!", ClusterName, ClusterVolume, MountPath, PluginVolume)
	if err != nil {
		StoneLogger.Errorf("Load config failed, err: %v", err.Error())
		return err
	}
	num := 0
	var count int

	for _, v := range pluginVolumeInfo {
		count = 0
		StoneLogger.Infof("the plugin volume name in config: %s", v.Name)
		if v.Name == PluginVolume {
			for _, value := range v.ClusterVolumesInfo {
				StoneLogger.Infof("value: %s", value)
				if value.MountPath == MountPath {
					StoneLogger.Infof("The will append cluster volume info is MountPath:%s", value.MountPath)
					continue
				}
			}
			count++
			break
		}
		num++
	}

	StoneLogger.Infof("The num is:%d, count is :%d", num, count)

	appendVol := ClusterVolumeInfo{
		ClusterVol: ClusterVolume,
		Cluster:    ClusterName,
		MountPath:  MountPath,
	}

	newClusterVolumes := make([]ClusterVolumeInfo, (len(pluginVolumeInfo[num].ClusterVolumesInfo) + 1))
	for k, v := range pluginVolumeInfo[num].ClusterVolumesInfo {
		newClusterVolumes[k] = pluginVolumeInfo[num].ClusterVolumesInfo[k]
		StoneLogger.Infof("volumes: %v", v)
	}

	newClusterVolumes[len(pluginVolumeInfo[num].ClusterVolumesInfo)] = appendVol
	pluginVolumeInfo[num].ClusterVolumesInfo = newClusterVolumes

	volumesInfo := AlreadyExistedVolumes{
		Volumes: pluginVolumeInfo,
	}
	err = UpdateJsonDataToConfig(volumesInfo)
	if err != nil {
		StoneLogger.Errorf("Update config after Add the cluster volume info!")
		return err
	}
	return nil
}

func DeleteClusterVolumeInfo(MountPath string, PluginVolume string) error {
	pluginVolumeInfo, err := LoadPluginVolumeInfoFromConfig()
	if err != nil {
		StoneLogger.Errorf("Load config failed, err: %v", err.Error())
		return err
	}

	num := 0
	var count int

	for _, v := range pluginVolumeInfo {
		count = 0
		if v.Name == PluginVolume {
			for _, value := range v.ClusterVolumesInfo {
				if value.MountPath == MountPath {
					continue
				}
				count++
			}
			break
		}
		num++
	}

	ClusterVolumeArray := make([]ClusterVolumeInfo, count)
	for _, v := range pluginVolumeInfo {
		count = 0
		if v.Name == PluginVolume {
			for _, value := range v.ClusterVolumesInfo {
				if value.MountPath == MountPath {
					continue
				}
				ClusterVolumeArray[count] = value
				count++
			}
			break
		}
	}

	pluginVolumeInfo[num].ClusterVolumesInfo = ClusterVolumeArray
	volumesInfo := AlreadyExistedVolumes{
		Volumes: pluginVolumeInfo,
	}

	err = UpdateJsonDataToConfig(volumesInfo)
	if err != nil {
		StoneLogger.Errorf("Update config after delete the cluster volume info!")
		return err
	}
	return nil
}
