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
    "errors"
    "time"
    "hash/fnv"
    "math"
    "sort"
    "sync"

    "gonum.org/v1/gonum/stat"
)

type syncWorkTask struct {
    fn func(interface{}) error
    data interface{}
    sync bool
    resp chan bool
    err error
    mergeKey string
}

type SyncWorker struct {
    tasks chan *syncWorkTask
    mergeTask bool
    lock sync.Mutex
    pendingTasks map[string]bool
    mergedTaskCount uint64
}

func NewSyncWorker(size int, workers int, mergeTask bool) *SyncWorker {
    syncer := &SyncWorker{
            tasks: make(chan *syncWorkTask, size),
            pendingTasks: make(map[string]bool),
            mergeTask: mergeTask,
            mergedTaskCount: 0,
    }

    for i := 0; i < workers; i ++ {
        go func() {
            var task *syncWorkTask = nil
            for {
                task = <- syncer.tasks
                if syncer.mergeTask && task.mergeKey != "" {
                    syncer.lock.Lock()
                    delete(syncer.pendingTasks, task.mergeKey)
                    syncer.lock.Unlock()
                }
                syncer.HandleTask(task)
                task = nil
            }
        }()
    }

    return syncer
}

func (syncer *SyncWorker)HandleTask(task *syncWorkTask) {
    if task == nil {
        return
    }

    if task.fn != nil {
        err := task.fn(task.data)
        task.err = err
        if task.sync && task.resp != nil {
            task.resp <- true 
        }
    } else {
        task.err = errors.New("No handler")
    }
}

func (syncer *SyncWorker)QueueLen() int {
    return len(syncer.tasks)
}

func (syncer *SyncWorker)MergedCount() uint64 {
    return syncer.mergedTaskCount
}

/*
 * Issue an operation to execute asynchronously. If the "id" is specified, and mergeTask option
 * is on. The tasks with same id will be merged and called only once. So the caller should be
 * aware that the "id" should be only used in the cases that all the function does the same work.
 */
func (syncer *SyncWorker)IssueOperation(data interface{}, fn func(interface{})error, sync bool,
    timeout int, id string) error {
    /*
     * Only the async operation task with non-empty merge identifier can be merged
     */
    mergeKey := ""
    if syncer.mergeTask && id != "" && !sync {
        syncer.lock.Lock()
        if exist, found := syncer.pendingTasks[id]; found && exist {
            syncer.mergedTaskCount ++
            syncer.lock.Unlock()
            /*
             * If merge tasks is on, don't allow issue operation synchronously
             */
            if sync {
                return errors.New("Don't allow sync operation on worker with merge task capability")
            } else {
                return nil
            }
        }
        syncer.pendingTasks[id] = true
        mergeKey = id
        syncer.lock.Unlock()
    }

    task := &syncWorkTask {
            data: data,
            fn: fn,
            sync: sync,
            mergeKey: mergeKey,
    }

    if sync {
        task.resp = make(chan bool, 1)
    } else {
        task.resp = nil
    }

    syncer.tasks <- task

    if sync {
        if timeout > 0 {
            select {
                case <- time.After(time.Duration(timeout) * time.Second):
                    return errors.New("The sync operation is timeout")
                case <- task.resp:
                    return task.err
            }
        } else {
            <- task.resp
            return task.err
        }
    }

    return nil
}

type HashSyncWorker struct {
    workerCount uint32
    workers map[uint32]*SyncWorker
}

func NewHashSyncWorker(workers uint32, size int, mergeTask bool) *HashSyncWorker {
    hashWorker := &HashSyncWorker {
        workerCount: workers,
        workers: make(map[uint32]*SyncWorker),
    }
    var i uint32
    for i = 0; i < workers; i ++ {
        /*Each sync worker of hash sync workers only allow 1 worker thread*/
        hashWorker.workers[i] = NewSyncWorker(size, 1, mergeTask)
    }

    return hashWorker
}

func _hashStrToInt(s string) uint32 {
    h := fnv.New32a()
    h.Write([]byte(s))
    return h.Sum32()
}

func (syncer *HashSyncWorker)IssueOperation(key string, data interface{}, fn func(interface{})error,
    sync bool, timeout int, id string) error {
    workerIndex := _hashStrToInt(key) % syncer.workerCount

    return syncer.workers[workerIndex].IssueOperation(data, fn, sync, timeout, id)
}

func (syncer *HashSyncWorker)SyncAllOperations(timeout int) {
    var i uint32
    for i = 0; i < syncer.workerCount; i ++ {
        syncer.workers[i].IssueOperation(nil,
            func(data interface{}) error {
                return nil
            }, true, timeout, "")
    }
}

type HashSyncerWorkerStats struct {
    QueueLens []float64
    Mean float64
    Median float64
    StdVariance float64
    MergedCounts []uint64
}

func (syncer *HashSyncWorker)Stats() *HashSyncerWorkerStats {
    var lens []float64
    var mergedCounts []uint64
    for _, worker := range syncer.workers {
        lens = append(lens, float64(worker.QueueLen()))
        mergedCounts = append(mergedCounts, worker.MergedCount())
    }

    sort.Float64s(lens)
    mean := stat.Mean(lens, nil)
    median := stat.Quantile(0.5, stat.Empirical, lens, nil)
    variance := math.Sqrt(stat.Variance(lens, nil))
    return &HashSyncerWorkerStats{
            QueueLens: lens,
            Mean: mean,
            StdVariance: variance,
            Median: median,
            MergedCounts: mergedCounts,
    }
}
