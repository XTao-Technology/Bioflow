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
package common

import (
    "sync/atomic"
    "sync"
    "time"
)

const (
    INITIAL_MIN_VALUE float64 = 999999
)

type KVStats struct {
    lock sync.RWMutex
    stats map[string]*int64
}

func NewKVStats() *KVStats {
    return &KVStats{
        stats: make(map[string]*int64),
    }
}

func (stats *KVStats)Update(name string, num int) {
    numVal := int64(num)
    stats.lock.RLock()
    if _, ok := stats.stats[name]; ok {
        atomic.AddInt64(stats.stats[name], numVal)
        stats.lock.RUnlock()
    } else {
        stats.lock.RUnlock()
        stats.lock.Lock()
        stats.stats[name] = &numVal
        stats.lock.Unlock()
    }
}

func (stats *KVStats)GetStats() map[string]int64 {
    retMap := make(map[string]int64)
    stats.lock.RLock()
    defer stats.lock.RUnlock()

    for key, val := range stats.stats {
        retMap[key] = *val
    }

    return retMap
}

func (stats *KVStats)Clear(name string) {
    stats.lock.RLock()
    defer stats.lock.RUnlock()

    if val, ok := stats.stats[name]; ok {
        *val = 0
    }
}

func (stats *KVStats)Destroy(name string) {
    stats.lock.Lock()
    defer stats.lock.Unlock()

    delete(stats.stats, name)
}

func (stats *KVStats)Get(name string) int64{
    stats.lock.RLock()
    defer stats.lock.RUnlock()

    if num, ok := stats.stats[name]; ok {
        return *num
    } else {
        return 0
    }
}

func StatUtilsCalcMaxDiffSeconds(curVal *float64, preTime time.Time) {
    secs := time.Now().Sub(preTime).Seconds()
    if secs > *curVal {
        *curVal = secs
    }
}

func StatUtilsCalcMinDiffSeconds(curVal *float64, preTime time.Time) {
    secs := time.Now().Sub(preTime).Seconds()
    if secs < *curVal {
        *curVal = secs
    }
}

func StatUtilsCalcDiffSeconds(curVal *float64, preTime time.Time) {
    secs := time.Now().Sub(preTime).Seconds()
    *curVal = secs
}

func StatUtilsMin(val *float64, newVal float64) {
    if *val > newVal {
        *val = newVal
    }
}

func StatUtilsMax(val *float64, newVal float64) {
    if *val < newVal {
        *val = newVal
    }
}

func StatUtilsAvg(val *float64, newVal float64, count *int64) {
    atomic.AddInt64(count, int64(1))
    if *count <= 0 {
        *count = 1
    }

    *val = (*val * float64(*count - 1) + newVal) / float64(*count)
}
