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
package server

import (
	"errors"
	"net/http"
	"path/filepath"

	. "github.com/xtao/xstone/common"
	. "github.com/xtao/xstone/message"
	. "github.com/xtao/xstone/src/automount-manager/common"
	"github.com/xtao/xstone/src/automount-manager/plugin"
	"github.com/xtao/xstone/storage"
)

var RootMountDir = "/var/run/storage/mnt/xtao"
var (
	TrueFlag  = 1
	FalseFlag = 0
)

type MountVolumeInfo struct {
	volume    string
	cluster   string
	mountpath string
}

func MountVolume(w http.ResponseWriter, req *http.Request) {
	defer req.Body.Close()
	req.ParseForm()

	var result XstoneAPIResult
	var mountPath string

	//StoneLogger.Infof("Xstone get request vas:%s\n", vars)
	volume := req.FormValue("Volume")
	cluster := req.FormValue("ClusterName")
	mountpoint := req.FormValue("MountPoint")
	pluginVolume := req.FormValue("PluginVolume")

	StoneLogger.Infof("volume:%s, cluster:%s, pluginVolume: %s, mountpoint:%s\n", volume, cluster, mountpoint, pluginVolume)

	if volume == "" || cluster == "" || pluginVolume == "" {
		result.Status = XSTONE_API_RET_FAIL
		mountErr := errors.New("The Volume, ClusterName and ContainerID must not empty")
		result.Msg = mountErr.Error()
		writeJSON(500, result, w)
		return
	}
	if mountpoint == "" {
		volCluster := cluster + "-" + volume
		mountPath = filepath.Join(RootMountDir, pluginVolume, volCluster)
	} else {
		mountPath = filepath.Join(RootMountDir, pluginVolume, mountpoint)
	}
	StoneLogger.Infof("mountPath: %s\n", mountPath)

	MountFlag := plugin.GetDriverMgr().ClusterVolumeMountFlag[pluginVolume]
	if MountFlag == FalseFlag {
		result.Status = XSTONE_API_RET_FAIL
		result.Msg = "The plugin volume is mounting some volume now, please try later!"
		StoneLogger.Infof("The plugin volume is mounting some volume now, please try later!")
		writeJSON(500, result, w)
		return
	}

	plugin.GetDriverMgr().ClusterVolumeMountFlag[pluginVolume] = FalseFlag

	err := storage.GetMountMgr().OwnerMountVolume(pluginVolume, volume, cluster, mountPath, storage.MOUNT_TIMEOUT)

	if err != nil {
		result.Status = XSTONE_API_RET_FAIL
		result.Msg = err.Error()
		StoneLogger.Errorf("Mount volume end err:%s\n!", err.Error())
		plugin.GetDriverMgr().ClusterVolumeMountFlag[pluginVolume] = TrueFlag
		writeJSON(500, result, w)
		return
	} else {
		err = AppendClusterVolumeInfo(cluster, volume, mountPath, pluginVolume)
		if err != nil {
			result.Status = XSTONE_API_RET_FAIL
			result.Msg = err.Error()
			plugin.GetDriverMgr().ClusterVolumeMountFlag[pluginVolume] = TrueFlag
			StoneLogger.Errorf("Append cluster volume info to json file failed: %v", err.Error())
			writeJSON(500, result, w)
			return
		}
		result.Status = XSTONE_API_RET_OK
		plugin.GetDriverMgr().ClusterVolumeMountFlag[pluginVolume] = TrueFlag
		result.Msg = "successfully mount volume!"
		writeJSON(http.StatusAccepted, result, w)
		return
	}
}

func UMountVolume(w http.ResponseWriter, req *http.Request) {
	defer req.Body.Close()
	req.ParseForm()

	var result XstoneAPIResult
	var mountPath string

	volume := req.FormValue("Volume")
	cluster := req.FormValue("ClusterName")
	mountpoint := req.FormValue("MountPoint")
	pluginVolume := req.FormValue("PluginVolume")

	StoneLogger.Infof("volume:%s, cluster:%s, pluginVolume: %s, mountpoint:%s\n", volume, cluster, mountpoint, pluginVolume)

	if volume == "" || cluster == "" || pluginVolume == "" {
		result.Status = XSTONE_API_RET_FAIL
		mountErr := errors.New("The Volume, ClusterName and ContainerID must not empty")
		result.Msg = mountErr.Error()
		writeJSON(500, result, w)
		return
	}

	if mountpoint == "" {
		volCluster := cluster + "-" + volume
		mountPath = filepath.Join(RootMountDir, pluginVolume, volCluster)
	} else {
		mountPath = filepath.Join(RootMountDir, pluginVolume, mountpoint)
	}
	StoneLogger.Infof("mountPath: %s\n", mountPath)

	MountFlag := plugin.GetDriverMgr().ClusterVolumeMountFlag[pluginVolume]
	if MountFlag == FalseFlag {
		result.Status = XSTONE_API_RET_FAIL
		result.Msg = "The plugin volume is mounting some volume now, please try later!"
		StoneLogger.Infof("The plugin volume is mounting some volume now, please try later!")
		writeJSON(500, result, w)
		return
	}

	plugin.GetDriverMgr().ClusterVolumeMountFlag[pluginVolume] = FalseFlag

	err := storage.GetMountMgr().UMountVolume(pluginVolume, volume, cluster, mountPath, FalseFlag, 0)
	plugin.GetDriverMgr().ClusterVolumeMountFlag[pluginVolume] = TrueFlag
	if err != nil {
		result.Status = XSTONE_API_RET_FAIL
		result.Msg = err.Error()
		StoneLogger.Errorf("UMount volume end err:%s\n!", err.Error())
		writeJSON(500, result, w)
		return
	} else {
		err = DeleteClusterVolumeInfo(mountPath, pluginVolume)
		if err != nil {
			result.Status = XSTONE_API_RET_FAIL
			result.Msg = err.Error()
			writeJSON(500, result, w)
			StoneLogger.Errorf("Delete json file cluster volume info failed, err: %v", err.Error())
			return
		}
		result.Status = XSTONE_API_RET_OK
		result.Msg = "successfully umount volume!"
		writeJSON(http.StatusAccepted, result, w)
		return
	}
}

func MountTest(w http.ResponseWriter, req *http.Request) {
	var result XstoneAPIResult
	result.Status = XSTONE_API_RET_OK
	result.Msg = "successfully mount volume"
	writeJSON(http.StatusAccepted, result, w)
}
