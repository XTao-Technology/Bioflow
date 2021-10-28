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
package storage

import (
    "errors"
	"sync"

    . "github.com/xtao/bioflow/common"
    . "github.com/xtao/bioflow/message"
    xstorage "github.com/xtao/xstone/storage"
    xconfig "github.com/xtao/xstone/config"
    "fmt"
)

const (
    WORKDIR string = "run"
    DATADIR string = "data"
    LOGDIR string = "log"
)

type StorageMgr interface {
    GetVolumesMap(string, string, string) (string, string, string)
    MapDataPathToContainer(string, string) string
    GetContainerExecDir() (string, string, string)
    GetSchedulerVolumesMap(vol string) string
	UpdateServerVolConfig(config *xconfig.ServerStoreConfig) error
	UpdateSchedulerVolConfig(config *xconfig.SchedulerStoreConfig) error
	UpdateContainerVolConfig(config *xconfig.ContainerStoreConfig) error
    MkdirOnScheduler(path string, recursive bool) error
    BuildStorageVolConstraints(vol string) (error, string, string)
    GetStats() *BioflowStorageMgrStats
    GenerateDirsForJob(jobId string)(string, string, string)
}

type storageMgr struct {
	mapLock sync.RWMutex
    volMountMap map[string]string
    schedVolMountMap map[string]string
    schedAutoMount bool

    tmpMount string
    containerDataDir string
    containerWorkDir string
    containerTempDir string
    runPath string
}

const (
    CONTAINER_DATA_DIR string = "/mnt/xtao"
    CONTAINER_TEMP_DIR string = "/vols/temp"
    SERVER_TEMP_DIR string = "/tmp"
)

var storeMgr *storageMgr = nil

func NewStorageMgr(config *xconfig.StoreMgrConfig) *storageMgr {
    storeMgr = &storageMgr {
        volMountMap: config.ServerConfig.VolMntPath,
        runPath: config.ServerConfig.JobRootPath,
        schedVolMountMap: config.SchedulerConfig.SchedVolMntPath,
        schedAutoMount: config.SchedulerConfig.AutoMount,
        tmpMount:  config.ServerConfig.TempMnt,
        containerDataDir: config.ContainerConfig.ContainerDataDir,
        containerWorkDir: config.ContainerConfig.ContainerWorkDir,
        containerTempDir: config.ContainerConfig.ContainerTempDir,
    }

    /*
     * Assign default value for container dirs if user don't
     * specify a valid one
     */
    if storeMgr.containerDataDir == "" {
        storeMgr.containerDataDir = CONTAINER_DATA_DIR
    }
    if storeMgr.containerTempDir == "" {
        storeMgr.containerTempDir = CONTAINER_TEMP_DIR
    }
    if storeMgr.tmpMount == "" {
        storeMgr.tmpMount = SERVER_TEMP_DIR
    }

    return storeMgr
}

func GetStorageMgr() *storageMgr {
    return storeMgr
}

func (mgr *storageMgr) UpdateSchedulerVolConfig(config *xconfig.SchedulerStoreConfig) error {
	StorageLogger.Infof("Storage manager will update scheduler config\n")
	mgr.mapLock.Lock()
	defer mgr.mapLock.Unlock()

	mgr.schedVolMountMap = config.SchedVolMntPath
    mgr.schedAutoMount = config.AutoMount

	StorageLogger.Infof("Storage manager complete update the scheduler config\n")

	return nil
}

func (mgr *storageMgr) UpdateServerVolConfig(config *xconfig.ServerStoreConfig) error {
	StorageLogger.Infof("Storage manager will update server config\n")
	mgr.mapLock.Lock()
	defer mgr.mapLock.Unlock()

	mgr.volMountMap = config.VolMntPath
    mgr.runPath = config.JobRootPath

	StorageLogger.Infof("Storage manager complete update the server config\n")

	return nil
}

func (mgr *storageMgr) UpdateContainerVolConfig(config *xconfig.ContainerStoreConfig) error {
	StorageLogger.Infof("Storage manager will update container config\n")
	mgr.mapLock.Lock()
	defer mgr.mapLock.Unlock()

    mgr.containerDataDir = config.ContainerDataDir
    mgr.containerWorkDir = config.ContainerWorkDir
    mgr.containerTempDir = config.ContainerTempDir

    /*
     * Assign default value for container dirs if user don't
     * specify a valid one
     */
    if mgr.containerDataDir == "" {
        mgr.containerDataDir = CONTAINER_DATA_DIR
    }
    if mgr.containerTempDir == "" {
        mgr.containerTempDir = CONTAINER_TEMP_DIR
    }

	StorageLogger.Infof("Storage manager complete update the container config\n")

	return nil
}

func (mgr *storageMgr) GetTempVolumeMount() string {
    return mgr.tmpMount
}

func (mgr *storageMgr) GetDataVolumesMap(vol string) string {
	mgr.mapLock.RLock()
	defer mgr.mapLock.RUnlock()

    if path, ok := mgr.volMountMap[vol]; !ok {
        StorageLogger.Infof("Storage Manager can't map volume %s to mountpoints\n", vol)
        return ""
    } else {
        return path
    }
}

/*
 * stoage manager may need place constraints on slaves to excute tasks based on the
 * volumes the task need to access. It produces key-value pair which will be used
 * by scheduler backend.
 */
func (mgr *storageMgr) BuildStorageVolConstraints(vol string) (error, string, string) {
    return BuildClusterLevelConstraint(vol)
}

func (mgr *storageMgr) GetSchedulerVolumesMap(vol string) string {
	mgr.mapLock.RLock()
	defer mgr.mapLock.RUnlock()

    if path, ok := mgr.schedVolMountMap[vol]; ok {
        return path
    }

    return ""
}

/*Delete a file according to scheduler volume mount*/
func (mgr *storageMgr) DeleteFileOnScheduler(file string) error {
	err, path := mgr.MapPathToSchedulerVolumeMount(file)
	if err != nil {
		StorageLogger.Errorf("Fail to map file %s to scheduler: %s\n",
			file, err.Error())
		return err
	}

	return FSUtilsDeleteFile(path)
}

/*Delete a directory according to scheduler volume mount*/
func (mgr *storageMgr) DeleteDirOnScheduler(file string, recursive bool) error {
	err, path := mgr.MapPathToSchedulerVolumeMount(file)
	if err != nil {
		StorageLogger.Errorf("Fail to map dir %s to scheduler: %s\n",
			file, err.Error())
		return err
	}

	return FSUtilsDeleteDir(path, recursive)
}

/*Tag a file with the file list it origins from*/
func (mgr *storageMgr) SetFileOriginFromTag(file string, originFiles []string, info *StageTagInfo) error {
	err, realFilePath := mgr.MapPathToSchedulerVolumeMount(file)
	if err != nil {
		StorageLogger.Errorf("Fail to map file %s to scheduler: %s\n",
			file, err.Error())
		return err
	}
    realSrcFiles := make([]string, 0)
    for i := 0; i < len(originFiles); i ++ {
	    err, realPath := mgr.MapPathToSchedulerVolumeMount(originFiles[i])
        if err != nil {
            StorageLogger.Errorf("Fail to map file %s to scheduler: %s\n",
                originFiles[i], err.Error())
            return err
        }
        realSrcFiles = append(realSrcFiles, realPath)
    }

    return TagUtilsSetFileOriginFromTag(realFilePath, originFiles, realSrcFiles, info)
}

/*make directory on scheduler volume mount*/
func (mgr *storageMgr) MkdirOnScheduler(path string, 
    recursive bool, accountInfo *UserAccountInfo) error {
	err, schedPath := mgr.MapPathToSchedulerVolumeMount(path)
	if err != nil {
		StorageLogger.Errorf("Fail to map directory %s to scheduler mnt: %s\n",
			path, err.Error())
		return err
	}

    lastDirPath := schedPath
    found := true
    if accountInfo != nil {
        found, lastDirPath, err = FSUtilsGetShortestNonExistPath(schedPath)
        if err != nil {
            StorageLogger.Errorf("Fail to get parent non-exist path for %s/%s: %s\n",
                lastDirPath, schedPath, err.Error())
            return err
        }

        /*Don't create it if the path exists*/
        if !found {
            return nil
        }
    }

	err = FSUtilsMkdir(schedPath, recursive)
    if err != nil {
        return err
    }

    if accountInfo != nil {
        return FSUtilsChangeDirUserPrivilege(lastDirPath, accountInfo, recursive, nil)
    } else {
        return nil
    }
}

/*merge directory files on scheduler volume mount*/
func (mgr *storageMgr) MergeDirFilesOnScheduler(oldDir string, 
    newDir string, dupPrefix string) error {
	err, oldDirPath := mgr.MapPathToSchedulerVolumeMount(oldDir)
	if err != nil {
		StorageLogger.Errorf("Fail to map directory %s to scheduler mnt: %s\n",
			oldDir, err.Error())
		return err
	}
	err, newDirPath := mgr.MapPathToSchedulerVolumeMount(newDir)
	if err != nil {
		StorageLogger.Errorf("Fail to map directory %s to scheduler mnt: %s\n",
			newDir, err.Error())
		return err
	}

	return FSUtilsMergeDirFiles(oldDirPath, newDirPath, dupPrefix)
}

/*Rename path on scheduler*/
func (mgr *storageMgr) MovePathOnScheduler(oldPath string, 
    newPath string) error {
	err, oldMappedPath := mgr.MapPathToSchedulerVolumeMount(oldPath)
	if err != nil {
		StorageLogger.Errorf("Fail to map path %s to scheduler mnt: %s\n",
			oldPath, err.Error())
		return err
    }
	err, newMappedPath := mgr.MapPathToSchedulerVolumeMount(newPath)
	if err != nil {
		StorageLogger.Errorf("Fail to map path %s to scheduler mnt: %s\n",
			newPath, err.Error())
		return err
    }

    return FSUtilsRename(oldMappedPath, newMappedPath)
}

/*
 * map a file identified with vol@cluster:path URI to a path relative to
 * the scheduler volume mount point
 */
func (mgr *storageMgr) MapPathToSchedulerVolumeMount(filePath string) (error, string){
    err, pathCluster, pathVol, pathFile := FSUtilsParseFileURI(filePath)
    if err != nil {
        StorageLogger.Errorf("Can't map file %s on scheduler: %s\n",
            filePath, err.Error())
        return err, ""
    }
    
    if mgr.schedAutoMount {
        err, mntDir := xstorage.GetMountMgr().GetVolumeMount(pathVol, pathCluster)
        if err != nil {
            return err, ""
        }
        StorageLogger.Debugf("The path %s is mapped to %s\n",
            filePath, mntDir + "/" + pathFile)
        return nil, mntDir + "/" + pathFile
    }

    volURI := FSUtilsBuildVolURI(pathCluster, pathVol)
	
	mgr.mapLock.RLock()
	defer mgr.mapLock.RUnlock()

    mntDir := mgr.GetSchedulerVolumesMap(volURI)
    if mntDir == "" {
        StorageLogger.Errorf("No scheduler mount for vol %s@%s\n",
            pathVol, pathCluster)
        return errors.New("No scheduler volume mount for " + pathVol), ""
    }
    StorageLogger.Debugf("The path %s is mapped to %s\n",
        filePath, mntDir + "/" + pathFile)
    return nil, mntDir + "/" + pathFile
}


func (mgr *storageMgr) GetContainerExecDir() (string, string, string) {
    return mgr.containerDataDir, mgr.containerWorkDir, mgr.containerTempDir
}

func (mgr *storageMgr) ResetStats() {
    if perfStats != nil {
        perfStats.Reset()
    }
}

func (mgr *storageMgr) GetStats() *BioflowStorageMgrStats {
    return &BioflowStorageMgrStats{
            MaxReadDir: perfStats.MaxReadDir,
            MinReadDir: perfStats.MinReadDir,
            AvgReadDir: perfStats.AvgReadDir,
            MaxRename: perfStats.MaxRename,
            MinRename: perfStats.MinRename,
            AvgRename: perfStats.AvgRename,
            MaxMkDir: perfStats.MaxMkDir,
            MinMkDir: perfStats.MinMkDir,
            AvgMkDir: perfStats.AvgMkDir,
            MaxReadFile: perfStats.MaxReadFile,
            MinReadFile: perfStats.MinReadFile,
            AvgReadFile: perfStats.AvgReadFile,
            MaxRmDir: perfStats.MaxRmDir,
            MinRmDir: perfStats.MinRmDir,
            AvgRmDir: perfStats.AvgRmDir,
            MaxDeleteFile: perfStats.MaxDeleteFile,
            MinDeleteFile: perfStats.MinDeleteFile,
            AvgDeleteFile: perfStats.AvgDeleteFile,
            MaxWriteFile: perfStats.MaxWriteFile,
            MinWriteFile: perfStats.MinWriteFile,
            AvgWriteFile: perfStats.AvgWriteFile,
            MaxMkDirPath: perfStats.MaxMkDirPath,
            MinMkDirPath: perfStats.MinMkDirPath,
            AvgMkDirPath: perfStats.AvgMkDirPath,
            MaxChown: perfStats.MaxChown,
            MinChown: perfStats.MinChown,
            AvgChown: perfStats.AvgChown,
            MaxChmod: perfStats.MaxChmod,
            MinChmod: perfStats.MinChmod,
            AvgChmod: perfStats.AvgChmod,
            MaxNFSMount: perfStats.MaxNFSMount,
            MinNFSMount: perfStats.MinNFSMount,
            MaxGlusterMount: perfStats.MaxGlusterMount,
            MinGlusterMount: perfStats.MinGlusterMount,
            MaxCephMount: perfStats.MaxCephMount,
            MinCephMount: perfStats.MinCephMount,
            OutstandingMkDirCount: perfStats.OutstandingMkDirCount,
            TotalMkDirCount: perfStats.TotalMkDirCount,
            OutstandingMkDirPathCount: perfStats.OutstandingMkDirPathCount,
            TotalMkDirPathCount: perfStats.TotalMkDirPathCount,
            OutstandingRmDirCount: perfStats.OutstandingRmDirCount,
            TotalRmDirCount: perfStats.TotalRmDirCount,
            OutstandingReadDirCount: perfStats.OutstandingReadDirCount,
            TotalReadDirCount: perfStats.TotalReadDirCount,
            OutstandingReadFileCount: perfStats.OutstandingReadFileCount,
            TotalReadFileCount: perfStats.TotalReadFileCount,
            OutstandingWriteFileCount: perfStats.OutstandingWriteFileCount,
            TotalWriteFileCount: perfStats.TotalWriteFileCount,
            OutstandingDeleteFileCount: perfStats.OutstandingDeleteFileCount,
            TotalDeleteFileCount: perfStats.TotalDeleteFileCount,
            OutstandingChmodCount: perfStats.OutstandingChmodCount,
            TotalChmodCount: perfStats.TotalChmodCount,
            OutstandingChownCount: perfStats.OutstandingChownCount,
            TotalChownCount: perfStats.TotalChownCount,
            OutstandingNFSMountCount: perfStats.OutstandingNFSMountCount,
            OutstandingGlusterMountCount: perfStats.OutstandingGlusterMountCount,
            OutstandingCephMountCount: perfStats.OutstandingCephMountCount,
            OutstandingRenameCount: perfStats.OutstandingRenameCount,
            TotalRenameCount: perfStats.TotalRenameCount,
            TotalRetryCount: perfStats.TotalRetryCount,
    }
}

func (mgr *storageMgr) GenerateDirsForJob(jobId string)(error, string, string, string) {
    mgr.mapLock.RLock()
    path := mgr.runPath
    mgr.mapLock.RUnlock()

    if path == "" {
        return errors.New("No run path set"), "", "", ""
    }

    workDir := fmt.Sprintf("%s/%s/%s", path, jobId, WORKDIR)
    dataDir := fmt.Sprintf("%s/%s/%s", path, jobId, DATADIR)
    logDir := fmt.Sprintf("%s/%s/%s", path, jobId, LOGDIR)
    return nil, workDir, dataDir, logDir
}

func (mgr *storageMgr) JobRootDir(jobId string) (error, string) {
    mgr.mapLock.RLock()
    path := mgr.runPath
    mgr.mapLock.RUnlock()

    if path == "" {
        return errors.New("No run path set"), ""
    }

    return nil, fmt.Sprintf("%s/%s", path, jobId)
}
