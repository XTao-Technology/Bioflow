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
    "time"
    "sync"
)

type DelayMerger struct {
    rwlock sync.Mutex
    closedList map[string]interface{}
    openList map[string]interface{}
    delay time.Duration
}

func NewDelayMerger(delay int) *DelayMerger {
    return &DelayMerger{
            delay: time.Duration(delay),
            openList: make(map[string]interface{}),
            closedList: nil,
    }
}

func (merger *DelayMerger)Add(id string, obj interface{}) error {
    merger.rwlock.Lock()
    defer merger.rwlock.Unlock()

    if merger.closedList != nil {
        if _, ok := merger.closedList[id]; ok {
            return nil
        }
    }

    if _, ok := merger.openList[id]; !ok {
        merger.openList[id] = obj
    }

    return nil
}

func (merger *DelayMerger)Remove(id string) {
    merger.rwlock.Lock()
    defer merger.rwlock.Unlock()

    delete(merger.closedList, id)
    delete(merger.openList, id)
}

func (merger *DelayMerger)GetMergedList() map[string]interface{} {
    merger.rwlock.Lock()
    defer merger.rwlock.Unlock()

    retList := merger.closedList
    merger.closedList = merger.openList
    merger.openList = make(map[string]interface{})

    return retList
}

func (merger *DelayMerger)TimedWait(timeout int) map[string]interface{} {
    
    merger.rwlock.Lock()
    if merger.closedList == nil {
        merger.closedList = merger.openList
        merger.openList = make(map[string]interface{})
    }
    merger.rwlock.Unlock()

    waitDuration := time.Duration(timeout) * time.Second
    select {
        case <- time.After(waitDuration):
            return merger.GetMergedList()
    }
}
