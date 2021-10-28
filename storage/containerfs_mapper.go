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
    . "github.com/xtao/bioflow/common"
    xstorage "github.com/xtao/xstone/storage"
)

type ContainerVolMgr struct {
    volsMap map[string]string
    dirMap map[string]string
    hdfsVols map[string]bool
    volBaseDir string
    mapPath string
    objects map[string]bool
}

func NewContainerVolMgr(baseDir string, mapPath string) *ContainerVolMgr {
    if baseDir == "" {
        baseDir = "/"
    }

    return &ContainerVolMgr{
            volBaseDir: baseDir,
            mapPath: mapPath,
            volsMap: make(map[string]string),
            dirMap: make(map[string]string),
            hdfsVols: make(map[string]bool),
            objects: make(map[string]bool),
    }
}

/*
For a path "os:bucket@name/of/object" or "name/of/object":
    1.get object name "name/of/object"
    2.generate file name on fs "name-of-object"
    3.record "os:bucket@name/of/object" of "name/of/object" in mgr.objects
    4.change path to "vol@cluster:name-of-object"
 */
func (mgr *ContainerVolMgr) MapDataPathToContainer(vol string, path string) (error, string) {
    if FSUtilsIsOSPath(path) {
        if mgr.mapPath == "" {
            err := errors.New("No map path set for restore object")
            StorageLogger.Errorf("Can't parse file URI %s: %s\n",
                path, err.Error())
            return err, ""
        }
        err, _, objName := FSUtilsParseOSAddress(path)
        if err != nil {
            StorageLogger.Errorf("Can't parse file URI %s: %s\n", path, err.Error())
            return err, ""
        }
        fileName := FSUtilsGenerateOSIdFromPath(objName)
        mgr.objects[path] = true
        path = FSUtilsBuildFsPath(mgr.mapPath, fileName)
    }

    /* If vol is not empty, don't parse path as URI and treat it as
     * normal file path
     */
    pathVol := vol
    pathFile := path
    pathCluster := ""
    var err error = nil 
    if vol == "" {
        err, pathCluster, pathVol, pathFile = FSUtilsParseFileURI(path)
        if err != nil {
            StorageLogger.Errorf("Can't parse file URI %s: %s\n",
                path, err.Error())
            return err, ""
        }
    }

    if FSUtilsIsHDFSVol(pathVol, pathCluster) {
        /* HDFS should be accessed by HDFS client, so don't track
         * and map mount points here
         */
        err, accessPath := xstorage.GetMountMgr().GetVolumeMount(pathVol, pathCluster)
        if err != nil {
            return err, ""
        }
        volURI := FSUtilsBuildVolURI(pathCluster, pathVol)
        mgr.hdfsVols[volURI] = true
        return nil, accessPath + "/" + pathFile
    } else {
        volURI := FSUtilsBuildVolURI(pathCluster, pathVol)
        if conVol, ok := mgr.volsMap[volURI]; ok {
            return nil, conVol + "/" + pathFile
        } else {
            conVolMountPoint := GetStorageMgr().GetDataVolumesMap(volURI)
            if conVolMountPoint == "" {
                return errors.New("Can't get volumes map of " + volURI), ""
            }
            mgr.volsMap[volURI] = conVolMountPoint
            return nil, conVolMountPoint + "/" + pathFile
        }
    }
}

func (mgr *ContainerVolMgr) MapDirToContainer(dirURI string, dirTarget string) error {
    if conDir, ok := mgr.dirMap[dirURI]; ok {
        if conDir == dirTarget {
            return nil
        } else {
            return errors.New(dirURI + " mapped two targets " + conDir + "," + dirTarget)
        }
    } else {
        mgr.dirMap[dirURI] = dirTarget
    }

    return nil
}

func (mgr *ContainerVolMgr) GetMappedVols() (error, []*DataVol) {
    vols := make([]*DataVol, 0)

    for volURI, conPath := range mgr.volsMap {
        dataVol := NewDataVolMap(volURI, "", conPath)
        vols = append(vols, dataVol)
    }

    for dirURI, conPath := range mgr.dirMap {
        err, pathCluster, pathVol, pathFile := FSUtilsParseFileURI(dirURI)
        if err != nil {
            StorageLogger.Errorf("Can't parse file URI %s: %s\n",
                dirURI, err.Error())
            return err, nil
        }
        volURI := FSUtilsBuildVolURI(pathCluster, pathVol)
        dataVol := NewDataVolMap(volURI, pathFile, conPath)
        vols = append(vols, dataVol)
    }

    for volURI, _ := range mgr.hdfsVols {
        dataVol := NewDataVolMap(volURI, "", "")
        dataVol.SetAsHDFSVol()
        vols = append(vols, dataVol)
    }

    return nil, vols
}

func (mgr *ContainerVolMgr) FilesNeedPrepare() []string {
    var files []string
    for item, _ := range mgr.objects {
        files = append(files, item)
    }
    return files
}

/*
 * Destroy all the objects tracked in the container vol mgr
 */
func (mgr *ContainerVolMgr) Destroy() {
    for key, _ := range mgr.volsMap {
        delete(mgr.volsMap, key)
        mgr.volsMap = nil
    }
    for key, _ := range mgr.dirMap {
        delete(mgr.dirMap, key)
        mgr.dirMap = nil
    }
    for key, _ := range mgr.hdfsVols {
        delete(mgr.hdfsVols, key)
        mgr.hdfsVols = nil
    }
    for key, _ := range mgr.objects {
        delete(mgr.objects, key)
        mgr.objects = nil
    }
}
