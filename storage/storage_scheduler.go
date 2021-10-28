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

    xstorage "github.com/xtao/xstone/storage"
    xconfig "github.com/xtao/xstone/config"
)

type storageScheduler struct {
    lock sync.Mutex
    hdfsClusters map[string]*xconfig.StorageManifest
}

var gStorageScheduler *storageScheduler

func init() {
    gStorageScheduler = &storageScheduler{
        hdfsClusters: make(map[string]*xconfig.StorageManifest),
    }
}

func GetStorageScheduler() *storageScheduler{
    return gStorageScheduler
}

/*select a HDFS cluster to use*/
func (scheduler *storageScheduler) SelectHDFSCluster() (error, string) {
    scheduler.lock.Lock()
    defer scheduler.lock.Unlock()

    /*need a policy to select the most suitable HDFS cluster*/
findOne:
    if len(scheduler.hdfsClusters) > 0 {
        /*choose any one*/
        for cluster, _ := range scheduler.hdfsClusters {
            return nil, cluster
        }
    } else {
        /*Try to get the latest cluster information from mount manager*/
        clusters := xstorage.GetMountMgr().GetAllStorageManifests()
        if clusters != nil && len(clusters) > 0 {
            count := 0
            for i := 0; i < len(clusters); i ++ {
                manifest := clusters[i]
                if manifest.FSType == xconfig.FS_HDFS {
                    if _, ok := scheduler.hdfsClusters[manifest.Cluster]; !ok {
                        scheduler.hdfsClusters[manifest.Cluster] = manifest
                        count ++
                    }
                }
            }

            if count > 0 {
                goto findOne
            }
        } else {
            return errors.New("No available HDFS cluster"), ""
        }
    }

    return errors.New("No HDFS storage"), ""
}
