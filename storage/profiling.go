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
    "time"
    "sync/atomic"

    . "github.com/xtao/bioflow/common"
)

var perfStats *storagePerfStats = NewStoragePerfStats()

/*
 * storage performance stats
 */
type storagePerfStats struct {
    MaxReadDir float64
    MinReadDir float64
    AvgReadDir float64
    MaxMkDir float64
    MinMkDir float64
    AvgMkDir float64
    MaxReadFile float64
    MinReadFile float64
    AvgReadFile float64
    MaxRmDir float64
    MinRmDir float64
    AvgRmDir float64
    MaxRename float64
    MinRename float64
    AvgRename float64
    MaxDeleteFile float64
    MinDeleteFile float64
    AvgDeleteFile float64
    MaxWriteFile float64
    MinWriteFile float64
    AvgWriteFile float64
    MaxMkDirPath float64
    MinMkDirPath float64
    AvgMkDirPath float64
    MaxChown float64
    MinChown float64
    AvgChown float64
    MaxChmod float64
    MinChmod float64
    AvgChmod float64
    MaxGlusterMount float64
    MinGlusterMount float64
    MaxNFSMount float64
    MinNFSMount float64
    MaxCephMount float64
    MinCephMount float64

    /*accounting outstanding operations*/
    OutstandingMkDirCount int64
    TotalMkDirCount int64
    OutstandingReadDirCount int64
    TotalReadDirCount int64
    OutstandingRmDirCount int64
    TotalRmDirCount int64
    OutstandingMkDirPathCount int64
    TotalMkDirPathCount int64
    OutstandingReadFileCount int64
    TotalReadFileCount int64
    OutstandingWriteFileCount int64
    TotalWriteFileCount int64
    OutstandingDeleteFileCount int64
    TotalDeleteFileCount int64
    OutstandingChownCount int64
    TotalChownCount int64
    OutstandingChmodCount int64
    TotalChmodCount int64
    OutstandingNFSMountCount int64
    OutstandingGlusterMountCount int64
    OutstandingCephMountCount int64
    OutstandingRenameCount int64
    TotalRenameCount int64
    TotalRetryCount int64
}

func NewStoragePerfStats() *storagePerfStats {
    return &storagePerfStats {
        MaxReadDir: 0,
        MinReadDir: INITIAL_MIN_VALUE,
        MaxMkDir: 0,
        MinMkDir: INITIAL_MIN_VALUE,
        MaxReadFile: 0,
        MinReadFile: INITIAL_MIN_VALUE,
        MaxRmDir: 0,
        MinRmDir: INITIAL_MIN_VALUE,
        MaxDeleteFile: 0,
        MinDeleteFile: INITIAL_MIN_VALUE,
        MaxWriteFile: 0,
        MinWriteFile: INITIAL_MIN_VALUE,
        MaxMkDirPath: 0,
        MinMkDirPath: INITIAL_MIN_VALUE,
        MaxChown: 0,
        MinChown: INITIAL_MIN_VALUE,
        MaxChmod: 0,
        MinChmod: INITIAL_MIN_VALUE,
        MaxNFSMount: 0,
        MinNFSMount: INITIAL_MIN_VALUE,
        MaxGlusterMount: 0,
        MinGlusterMount: INITIAL_MIN_VALUE,
        MaxCephMount: 0,
        MinCephMount: INITIAL_MIN_VALUE,
        MaxRename: 0,
        MinRename: INITIAL_MIN_VALUE,
        TotalRetryCount: 0,
    }
}

func (stats *storagePerfStats)Reset() {
    stats.MaxReadDir = 0
    stats.AvgReadDir = 0
    stats.MaxMkDir = 0
    stats.AvgMkDir = 0
    stats.MaxReadFile = 0
    stats.AvgReadFile = 0
    stats.MaxRmDir = 0
    stats.AvgRmDir = 0
    stats.MaxRename = 0
    stats.AvgRename = 0
    stats.MaxDeleteFile = 0
    stats.AvgDeleteFile = 0
    stats.MaxWriteFile = 0
    stats.AvgWriteFile = 0
    stats.MaxMkDirPath = 0
    stats.AvgMkDirPath = 0
    stats.MaxChown = 0
    stats.AvgChown = 0
    stats.MaxChmod = 0
    stats.AvgChmod = 0
    stats.MaxGlusterMount = 0
    stats.MaxNFSMount = 0
    stats.MaxCephMount = 0

    /*accounting outstanding operations*/
    stats.TotalMkDirCount = 0
    stats.TotalReadDirCount = 0
    stats.TotalRmDirCount = 0
    stats.TotalMkDirPathCount = 0
    stats.TotalReadFileCount = 0
    stats.TotalWriteFileCount = 0
    stats.TotalDeleteFileCount = 0
    stats.TotalChownCount = 0
    stats.TotalChmodCount = 0
    stats.TotalRenameCount = 0
    stats.TotalRetryCount = 0
}

func (stats *storagePerfStats)ProfileRetryCount() {
    atomic.AddInt64(&stats.TotalRetryCount, 1)
}

func (stats *storagePerfStats)StartProfileMkDir(preTime *time.Time) {
    *preTime = time.Now()
    atomic.AddInt64(&stats.OutstandingMkDirCount, 1)
}

func (stats *storagePerfStats)EndProfileMkDir(preTime time.Time) {
    latency := time.Now().Sub(preTime).Seconds()
    StatUtilsMin(&stats.MinMkDir, latency)
    StatUtilsMax(&stats.MaxMkDir, latency)
    StatUtilsAvg(&stats.AvgMkDir, latency, &stats.TotalMkDirCount)
    atomic.AddInt64(&stats.OutstandingMkDirCount, -1)
}

func (stats *storagePerfStats)StartProfileMkDirPath(preTime *time.Time) {
    *preTime = time.Now()
    atomic.AddInt64(&stats.OutstandingMkDirPathCount, 1)
}

func (stats *storagePerfStats)EndProfileMkDirPath(preTime time.Time) {
    latency := time.Now().Sub(preTime).Seconds()
    StatUtilsMin(&stats.MinMkDirPath, latency)
    StatUtilsMax(&stats.MaxMkDirPath, latency)
    StatUtilsAvg(&stats.AvgMkDirPath, latency, &stats.TotalMkDirPathCount)
    atomic.AddInt64(&stats.OutstandingMkDirPathCount, -1)
}

func (stats *storagePerfStats)StartProfileRmDir(preTime *time.Time) {
    *preTime = time.Now()
    atomic.AddInt64(&stats.OutstandingRmDirCount, 1)
}

func (stats *storagePerfStats)EndProfileRmDir(preTime time.Time) {
    latency := time.Now().Sub(preTime).Seconds()
    StatUtilsMin(&stats.MinRmDir, latency)
    StatUtilsMax(&stats.MaxRmDir, latency)
    StatUtilsAvg(&stats.AvgRmDir, latency, &stats.TotalRmDirCount)
    atomic.AddInt64(&stats.OutstandingRmDirCount, -1)
}

func (stats *storagePerfStats)StartProfileReadDir(preTime *time.Time) {
    *preTime = time.Now()
    atomic.AddInt64(&stats.OutstandingReadDirCount, 1)
}

func (stats *storagePerfStats)EndProfileReadDir(preTime time.Time) {
    latency := time.Now().Sub(preTime).Seconds()
    StatUtilsMin(&stats.MinReadDir, latency)
    StatUtilsMax(&stats.MaxReadDir, latency)
    StatUtilsAvg(&stats.AvgReadDir, latency, &stats.TotalReadDirCount)
    atomic.AddInt64(&stats.OutstandingReadDirCount, -1)
}

func (stats *storagePerfStats)StartProfileDeleteFile(preTime *time.Time) {
    *preTime = time.Now()
    atomic.AddInt64(&stats.OutstandingDeleteFileCount, 1)
}

func (stats *storagePerfStats)EndProfileDeleteFile(preTime time.Time) {
    latency := time.Now().Sub(preTime).Seconds()
    StatUtilsMin(&stats.MinDeleteFile, latency)
    StatUtilsMax(&stats.MaxDeleteFile, latency)
    StatUtilsAvg(&stats.AvgDeleteFile, latency, &stats.TotalDeleteFileCount)
    atomic.AddInt64(&stats.OutstandingDeleteFileCount, -1)
}

func (stats *storagePerfStats)StartProfileReadFile(preTime *time.Time) {
    *preTime = time.Now()
    atomic.AddInt64(&stats.OutstandingReadFileCount, 1)
}

func (stats *storagePerfStats)EndProfileReadFile(preTime time.Time) {
    latency := time.Now().Sub(preTime).Seconds()
    StatUtilsMin(&stats.MinReadFile, latency)
    StatUtilsMax(&stats.MaxReadFile, latency)
    StatUtilsAvg(&stats.AvgReadFile, latency, &stats.TotalReadFileCount)
    atomic.AddInt64(&stats.OutstandingReadFileCount, -1)
}

func (stats *storagePerfStats)StartProfileWriteFile(preTime *time.Time) {
    *preTime = time.Now()
    atomic.AddInt64(&stats.OutstandingWriteFileCount, 1)
}

func (stats *storagePerfStats)EndProfileWriteFile(preTime time.Time) {
    latency := time.Now().Sub(preTime).Seconds()
    StatUtilsMin(&stats.MinWriteFile, latency)
    StatUtilsMax(&stats.MaxWriteFile, latency)
    StatUtilsAvg(&stats.AvgWriteFile, latency, &stats.TotalWriteFileCount)
    atomic.AddInt64(&stats.OutstandingWriteFileCount, -1)
}

func (stats *storagePerfStats)StartProfileChown(preTime *time.Time) {
    *preTime = time.Now()
    atomic.AddInt64(&stats.OutstandingChownCount, 1)
}

func (stats *storagePerfStats)EndProfileChown(preTime time.Time) {
    latency := time.Now().Sub(preTime).Seconds()
    StatUtilsMin(&stats.MinChown, latency)
    StatUtilsMax(&stats.MaxChown, latency)
    StatUtilsAvg(&stats.AvgChown, latency, &stats.TotalChownCount)
    atomic.AddInt64(&stats.OutstandingChownCount, -1)
}

func (stats *storagePerfStats)StartProfileChmod(preTime *time.Time) {
    *preTime = time.Now()
    atomic.AddInt64(&stats.OutstandingChmodCount, 1)
}

func (stats *storagePerfStats)EndProfileChmod(preTime time.Time) {
    latency := time.Now().Sub(preTime).Seconds()
    StatUtilsMin(&stats.MinChmod, latency)
    StatUtilsMax(&stats.MaxChmod, latency)
    StatUtilsAvg(&stats.AvgChmod, latency, &stats.TotalChmodCount)
    atomic.AddInt64(&stats.OutstandingChmodCount, -1)
}

func (stats *storagePerfStats)StartProfileNFSMount(preTime *time.Time) {
    *preTime = time.Now()
    atomic.AddInt64(&stats.OutstandingNFSMountCount, 1)
}

func (stats *storagePerfStats)EndProfileNFSMount(preTime time.Time) {
    latency := time.Now().Sub(preTime).Seconds()
    StatUtilsMin(&stats.MinNFSMount, latency)
    StatUtilsMax(&stats.MaxNFSMount, latency)
    atomic.AddInt64(&stats.OutstandingNFSMountCount, -1)
}

func (stats *storagePerfStats)StartProfileGlusterMount(preTime *time.Time) {
    *preTime = time.Now()
    atomic.AddInt64(&stats.OutstandingGlusterMountCount, 1)
}

func (stats *storagePerfStats)EndProfileGlusterMount(preTime time.Time) {
    latency := time.Now().Sub(preTime).Seconds()
    StatUtilsMin(&stats.MinGlusterMount, latency)
    StatUtilsMax(&stats.MaxGlusterMount, latency)
    atomic.AddInt64(&stats.OutstandingGlusterMountCount, -1)
}

func (stats *storagePerfStats)StartProfileCephMount(preTime *time.Time) {
    *preTime = time.Now()
    atomic.AddInt64(&stats.OutstandingCephMountCount, 1)
}

func (stats *storagePerfStats)EndProfileCephMount(preTime time.Time) {
    latency := time.Now().Sub(preTime).Seconds()
    StatUtilsMin(&stats.MinCephMount, latency)
    StatUtilsMax(&stats.MaxCephMount, latency)
    atomic.AddInt64(&stats.OutstandingCephMountCount, -1)
}

func (stats *storagePerfStats)StartProfileRename(preTime *time.Time) {
    *preTime = time.Now()
    atomic.AddInt64(&stats.OutstandingRenameCount, 1)
}

func (stats *storagePerfStats)EndProfileRename(preTime time.Time) {
    latency := time.Now().Sub(preTime).Seconds()
    StatUtilsMin(&stats.MinRename, latency)
    StatUtilsMax(&stats.MaxRename, latency)
    StatUtilsAvg(&stats.AvgRename, latency, &stats.TotalRenameCount)
    atomic.AddInt64(&stats.OutstandingRenameCount, -1)
}
