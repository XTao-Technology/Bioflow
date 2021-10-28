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
package scheduler

import (
	"fmt"
    "sync"
    "sync/atomic"
    "container/heap"
    "errors"
    "time"
    "strings"

    . "github.com/xtao/bioflow/common"
    . "github.com/xtao/bioflow/message"
    . "github.com/xtao/bioflow/scheduler/common"
)

var (
    /*
     * The tunables are used to control how long waits for a queued
     * or running task to be considered hang
     */
    MaxAllowTaskTimeout float64 = DEF_MAX_ALLOW_DELAY_INTERVAL
)


/*
 * A jobPriHeap implements heap.Interface and holds Jobs.
 */
type jobPriHeap []Job

func NewJobPriHeap() jobPriHeap {
    jobHeap := make(jobPriHeap, 0)
    heap.Init(&jobHeap)
    return jobHeap
}

func (pq jobPriHeap) Len() int { return len(pq) }

func (pq jobPriHeap) Less(i, j int) bool {
    /*
     * the jobs should be sorted in time order. The earlier
     * jobs should be scheduled before later sumbitted jobs.
     */
    return pq[i].CreateTime().Before(pq[j].CreateTime())
}

func (pq jobPriHeap) Swap(i, j int) {
    pq[i], pq[j] = pq[j], pq[i]
    pq[i].SetQueueIndex(i)
    pq[j].SetQueueIndex(j)
}

func (pq *jobPriHeap) Push(item interface{}) {
    size := len(*pq)
    job := item.(Job)
    job.SetQueueIndex(size)
    *pq = append(*pq, job)
}

func (pq *jobPriHeap) Pop() interface{} {
    old := *pq
    n := len(old)
    job := old[n-1]
    *pq = old[0 : n-1]
    job.SetQueueIndex(-1)
    return job
}

/* update job pos in the queue after priority updated*/
func (pq *jobPriHeap) UpdateJobPos(job Job) {
    heap.Fix(pq, job.QueueIndex())
}

type queueScheduleMetric struct {
    lock sync.Mutex

    totalQueuedTasks int64
    totalSubmittedTasks int64
    totalRunningTasks int64
    totalQueueTime float64
    maxTaskQueueTime float64
    lastMaxTimeUpdated time.Time

    quotaThroateJobNum int64
    throateJobs int64
    throateScheduleJobs int64
    delayJobs int64
    completeTasks int64
    completeJobs int64
    scheduleRounds int64
    throateRounds int64
    delayRounds int64

    /*
     * stats for delay schedule to aggregate
     * resources for large jobs. These data will
     * be helpful to evaluate resource utilization.
     */
    aggDelayRounds int64
    aggDelay bool
    maxAggDelayPeriod float64
    totalAggDelayPeriod float64
    aggDelayTime time.Time
    maxAggTaskNum int64
    minAggTaskNum int64
}

type jobQueue struct {
    rwlock sync.RWMutex
    needSchedule map[string]bool
    jobHeap jobPriHeap
    jobNum int64
    needScheduleNum int64
    
    metric queueScheduleMetric
}

func NewJobQueue() *jobQueue{
    queue := &jobQueue{
        needSchedule: make(map[string]bool),
        jobHeap: NewJobPriHeap(),
        jobNum: 0,
        metric: queueScheduleMetric {
            totalQueuedTasks: 0,
            totalRunningTasks: 0,
            totalQueueTime: 0,
            maxTaskQueueTime: 0,
            throateJobs: 0,
            throateScheduleJobs: 0,
            completeTasks: 0,
            completeJobs: 0,
            scheduleRounds: 0,
            throateRounds: 0,
            delayRounds: 0,
            aggDelay: false,
            aggDelayRounds: 0,
            maxAggDelayPeriod: 0,
            maxAggTaskNum: 0,
            minAggTaskNum: 9999999,
        },
    }

    return queue
}

func (queue *jobQueue)Enqueue(job Job) error{
    queue.rwlock.Lock()
    defer queue.rwlock.Unlock()

    queue.jobNum ++

    return nil
}

func (queue *jobQueue)Dequeue(job Job) error{
    queue.rwlock.Lock()
    defer queue.rwlock.Unlock()

    queue.jobNum --
    queue.metric.completeJobs ++
    jobId := job.GetID()
    if _, ok := queue.needSchedule[jobId]; ok {
        delete(queue.needSchedule, jobId)
        queue.needScheduleNum --
    }

    qIndex := job.QueueIndex()
    if qIndex >= 0 {
        heap.Remove(&queue.jobHeap, qIndex)
    }
    job.SetQueueIndex(-1)

    return nil
}

func (queue *jobQueue)MarkJobToSchedule(job Job) error{
    queue.rwlock.Lock()
    defer queue.rwlock.Unlock()

    if job.JobTerminated() || job.Canceled(){
        return errors.New("Mark canceled job to schedule")
    }

    jobId := job.GetID()

    if _, ok := queue.needSchedule[jobId]; !ok {
        queue.needSchedule[jobId] = true
        queue.needScheduleNum ++
        heap.Push(&queue.jobHeap, job)
    }

    return nil
}

func (queue *jobQueue)UpdateJobInQueue(job Job) error{
    queue.rwlock.Lock()
    defer queue.rwlock.Unlock()

    if job.Canceled() {
        return errors.New("update canceled job")
    }

    jobId := job.GetID()

    if _, ok := queue.needSchedule[jobId]; ok {
        heap.Push(&queue.jobHeap, job)
        pq := &queue.jobHeap
        pq.UpdateJobPos(job)
    }

    return nil
}

func (queue *jobQueue)SelectJobs(jobLimit int, ratio float64) []Job {
    queue.rwlock.RLock()
    jobCount := len(queue.needSchedule)
    queue.rwlock.RUnlock()

    atomic.AddInt64(&queue.metric.scheduleRounds,
            1)

    /*don't schedule jobs if not allowed*/
    if jobLimit <= 0 {
        atomic.AddInt64(&queue.metric.delayJobs,
            int64(jobCount))
        atomic.AddInt64(&queue.metric.delayRounds,
            1)
        return nil
    }

    maxCount := int(float64(jobCount) * ratio)
    if maxCount > jobCount {
        maxCount = jobCount
    }

    /*
     * The job limit is to limit the maximum job count 
     * can be scheduled on time. So shouldn't schedule
     * jobs beyond the limit.
     */
    if jobLimit > 0 && maxCount > jobLimit {
        maxCount = jobLimit
    }

    var jobs []Job = nil
    if maxCount > 0 {
        jobs = queue.GetNeedScheduleJobs(maxCount)
    }

    /* update the metric
     * use atomit operations rather than lock here
     * to maximize the concurrency
     */
    if maxCount <= 0 && jobCount > 0 {
        atomic.AddInt64(&queue.metric.delayJobs,
            int64(jobCount))
        atomic.AddInt64(&queue.metric.delayRounds,
            1)
    } else if maxCount > 0 && jobCount > 0 {
        atomic.AddInt64(&queue.metric.throateJobs,
            int64(jobCount - maxCount))
    }
    if jobs != nil && len(jobs) > 0 && maxCount < jobCount {
        atomic.AddInt64(&queue.metric.throateScheduleJobs,
            int64(len(jobs)))
        atomic.AddInt64(&queue.metric.throateRounds,
            1)
    }

    return jobs
}

func (queue *jobQueue)GetNeedScheduleJobs(maxCount int) []Job {
    count := 0
    jobs := make([]Job, 0)
    queue.rwlock.Lock()
    for queue.jobHeap.Len() > 0 {
        item := heap.Pop(&queue.jobHeap)
        job := item.(Job)
        if _, ok := queue.needSchedule[job.GetID()]; ok{
            delete(queue.needSchedule, job.GetID())
            queue.needScheduleNum --
        }
        jobs = append(jobs, job)
        count ++

        if maxCount > 0 && count >= maxCount {
            break
        }
    }
    queue.rwlock.Unlock()

    return jobs
}

func (queue *jobQueue)GetQueueMetric() *queueScheduleMetric {
    queue.rwlock.RLock()
    defer queue.rwlock.RUnlock()

    return &queue.metric
}

func (queue *jobQueue)GetJobStats() *BioflowJobStats {
    queue.rwlock.RLock()
    defer queue.rwlock.RUnlock()

    var delayTime float64 = 0
    if queue.metric.aggDelay {
        delayTime = time.Now().Sub(queue.metric.aggDelayTime).Minutes()
    }

    return &BioflowJobStats{
        JobCount: queue.jobNum,
        PendingScheduleJobs: queue.needScheduleNum,
        QueuedTasks: queue.metric.totalQueuedTasks,
        RunningTasks: queue.metric.totalRunningTasks,
        MaxQueueTime: queue.metric.maxTaskQueueTime,
        TotalQueueTime: queue.metric.totalQueueTime,
        ThroateJobs: queue.metric.throateJobs,
        ThroateScheduleJobs: queue.metric.throateScheduleJobs,
        DelayJobs: queue.metric.delayJobs,
        SubmitTasks: queue.metric.totalSubmittedTasks,
        CompleteTasks: queue.metric.completeTasks,
        CompleteJobs: queue.metric.completeJobs,
        ScheduleRounds: queue.metric.scheduleRounds,
        DelayRounds: queue.metric.delayRounds,
        ThroateRounds: queue.metric.throateRounds,
        AggDelayRounds: queue.metric.aggDelayRounds,
        MaxAggDelayPeriod: queue.metric.maxAggDelayPeriod,
        TotalAggDelayPeriod: queue.metric.totalAggDelayPeriod,
        MaxAggTaskNum: queue.metric.maxAggTaskNum,
        MinAggTaskNum: queue.metric.minAggTaskNum,
        DelayingSchedule: queue.metric.aggDelay,
        DelayTimeTillNow: delayTime,
    }
}

func (queue *jobQueue)UpdateJobMetric(job Job,
    clear bool) {
    queue.rwlock.Lock()
    defer queue.rwlock.Unlock()

    schedInfo := job.GetScheduleInfo()
    if schedInfo == nil {
        return
    }

    newMetric := JobScheduleMetric{
                    QueuedTasks: 0,
                    RunningTasks: 0,
                    MaxTaskQueueTime: 0,
                    TotalQueueTime: 0,
    }
    if !clear {
        schedInfo.EvaluateMetric(&newMetric, MaxAllowTaskTimeout)
    }

    oldMetric := job.GetScheduleInfo().Metric()

    /*update job metric change to queue metric*/
    qMetric := &queue.metric
    qMetric.lock.Lock()

    qMetric.totalQueuedTasks += (newMetric.QueuedTasks - oldMetric.QueuedTasks)
    qMetric.totalRunningTasks += (newMetric.RunningTasks - oldMetric.RunningTasks)
    if newMetric.MaxTaskQueueTime > qMetric.maxTaskQueueTime {
        qMetric.maxTaskQueueTime = newMetric.MaxTaskQueueTime
        qMetric.lastMaxTimeUpdated = time.Now()
    }
    qMetric.totalQueueTime += (newMetric.TotalQueueTime - oldMetric.TotalQueueTime)
    if qMetric.totalQueueTime < 0 {
        qMetric.totalQueueTime = 0
    }
    queue._resetMaxQueueTime()

    qMetric.lock.Unlock()


    job.GetScheduleInfo().SetMetric(&newMetric)
}

/*
 * It is expensive to maintain the max queued time for all tasks. Because if
 * the task has the max queued time finished, we don't know which is the second
 * longest. But re-scanning all task list when a task finish is too expensive. So
 * we use another approach to estimate the max queued time of tasks:
 * 1) When a task state is checked, update the max queued time if its queue time
 *    is longer
 * 2) Periodically reset max queue time to the average queued time.
 *
 * max queued time is helpful when we need throate schedule if a task which require
 * a lot of CPU and memory queued too long. It may not have a chance to be scheduled
 * when many tasks requiring less resource are continuously submitted. The scheduler
 * need wait for a while to aggregate enough resources for these tasks.
 */
func (queue *jobQueue)_resetMaxQueueTime() {
    qMetric := &queue.metric
    if qMetric.totalQueuedTasks <= 0 {
        qMetric.maxTaskQueueTime = 0
    } else {
        /*
         * If the last time updated has passed 40 minutes, we may think that
         * it is stale
         */
        stale := true
        if time.Now().Sub(qMetric.lastMaxTimeUpdated).Minutes() < MAX_QUEUE_TIME_STALE_PERIOD {
            stale = false
        }

        if qMetric.maxTaskQueueTime >= qMetric.totalQueueTime || stale {
            qMetric.maxTaskQueueTime = 1.5 * qMetric.totalQueueTime / float64(qMetric.totalQueuedTasks)
            if qMetric.maxTaskQueueTime > qMetric.totalQueueTime {
                qMetric.maxTaskQueueTime = qMetric.totalQueueTime
            }
        }
    }
}

func (queue *jobQueue)ResetMaxQueueTime() {
    queue.metric.lock.Lock()
    queue._resetMaxQueueTime()
    queue.metric.lock.Unlock()
}

func (queue *jobQueue)UpdateTaskMetric(submit int, complete int) {
    atomic.AddInt64(&queue.metric.totalSubmittedTasks, int64(submit))
    atomic.AddInt64(&queue.metric.completeTasks, int64(complete))
}

type jobPriQueue struct {
    lock sync.RWMutex
    queues map[int]*jobQueue
}

func NewJobPriQueue() *jobPriQueue {
    return &jobPriQueue {
        queues: make(map[int]*jobQueue),
    }
}

func (queue *jobPriQueue)GetQueue(pri int, create bool) *jobQueue{
    queue.lock.RLock()
    if priQueue, ok := queue.queues[pri]; ok {
        queue.lock.RUnlock()
        return priQueue
    } else if create {
        queue.lock.RUnlock()

        priQueue = NewJobQueue()
        queue.lock.Lock()
        queue.queues[pri] = priQueue
        queue.lock.Unlock()
        return priQueue
    } else {
        queue.lock.RUnlock()
        return nil
    }
}

func (queue *jobPriQueue)Enqueue(job Job) error {
    pri := job.Priority()
    
    jobQueue := queue.GetQueue(pri, true)
    if jobQueue == nil {
        SchedulerLogger.Errorf("Can't allocate memory for queue\n")
        return errors.New("No memory for queue")
    }
    return jobQueue.Enqueue(job)
}

func (queue *jobPriQueue)Dequeue(job Job) error {
    pri := job.Priority()
    
    jobQueue := queue.GetQueue(pri, false)
    if jobQueue == nil {
        SchedulerLogger.Errorf("Can't allocate memory for queue\n")
        return errors.New("No memory for queue")
    }

    return jobQueue.Dequeue(job)
}

func (queue *jobPriQueue)MarkJobNeedSchedule(job Job, enforce bool) error {
    pri := job.Priority()
    
    jobQueue := queue.GetQueue(pri, false)
    if jobQueue == nil {
        SchedulerLogger.Errorf("Can't allocate memory for queue\n")
        return errors.New("No memory for queue")
    }

    return jobQueue.MarkJobToSchedule(job)
}

func (queue *jobPriQueue)UpdateJobPriority(job Job, oldPri int) error {
    pri := job.Priority()
    oldQueue := queue.GetQueue(oldPri, false)
    if oldQueue == nil {
        SchedulerLogger.Errorf("job %s is not in a queue with old pri %d\n",
            job.GetID(), oldPri)
        return errors.New("job not in priority queue")
    }
    oldQueue.Dequeue(job)
    /*reset job's metric to old queue*/
    oldQueue.UpdateJobMetric(job, true)

    /*insert it to the new queue and mark it on purpose*/
    jobQueue := queue.GetQueue(pri, true)
    if jobQueue == nil {
        SchedulerLogger.Errorf("Fail insert job %s to new priority queue for no memory\n",
            job.GetID())
        return errors.New("No memory for queue")
    }

    jobQueue.Enqueue(job)
    err := jobQueue.MarkJobToSchedule(job)
    if err == nil {
        /*update job's metric to new queue*/
        jobQueue.UpdateJobMetric(job, false)
    }

    return err
}

func (queue *jobPriQueue)UpdateJobMetric(job Job,
    clear bool) {
    pri := job.Priority()
    
    jobQueue := queue.GetQueue(pri, false)
    if jobQueue != nil {
        jobQueue.UpdateJobMetric(job, clear)
    }
}

func (queue *jobPriQueue)UpdateTaskMetric(job Job,
    submit int, complete int) {
    pri := job.Priority()
    
    jobQueue := queue.GetQueue(pri, false)
    if jobQueue != nil {
        jobQueue.UpdateTaskMetric(submit, complete)
    }
}

func (queue *jobPriQueue)GetJobStats() map[string]BioflowJobStats {
    queue.lock.RLock()
    defer queue.lock.RUnlock()
    
    jobStats := make(map[string]BioflowJobStats)
    for pri, jobQueue := range queue.queues {
        jobStat := jobQueue.GetJobStats()
        jobStats[fmt.Sprintf("%d", pri)] = *jobStat
    }

    return jobStats
}

func (queue *jobPriQueue)ResetMaxQueueTime() {
    queue.lock.RLock()
    defer queue.lock.RUnlock()
    
    for _, jobQueue := range queue.queues {
        jobQueue.ResetMaxQueueTime()
    }
}

type jobTracker struct {
    lock sync.Mutex
    priQueue *jobPriQueue
    taskCount int64

    /*a hash map to track job id <-> job*/
    rwHashLock sync.RWMutex
    jobHashQueue map[string]Job

    jobScheduler *priJobScheduler

    /*statistics for job scheduler*/
    maxScheduleTime float64
    maxMarkScheduleTime float64
    maxJobCheckTime float64
}

func NewJobTracker(config *JobSchedulerConfig) *jobTracker {
    tracker := &jobTracker{
            priQueue: NewJobPriQueue(),
            jobHashQueue: make(map[string]Job),
            taskCount: 0,
    }

    scheduler := NewPriJobScheduler(tracker.priQueue)
    tracker.jobScheduler = scheduler

    tracker.UpdateConfig(config)

    return tracker
}

func (tracker *jobTracker)UpdateConfig(config *JobSchedulerConfig) {
    tracker.jobScheduler.UpdateConfig(config)
}

func (tracker *jobTracker)SelectJobs(maxJobs int) []Job {
    preTime := time.Now()
    jobs := tracker.jobScheduler.SelectJobs(maxJobs)
    StatUtilsCalcMaxDiffSeconds(&tracker.maxScheduleTime,
        preTime)

    return jobs
}

func (tracker *jobTracker)UpdateJobPriority(job Job, oldPri int) error {
    return tracker.priQueue.UpdateJobPriority(job, oldPri)
}

func (tracker *jobTracker)MarkJobNeedSchedule(job Job,
    enforce bool) error {
    preTime := time.Now()
    err := tracker.priQueue.MarkJobNeedSchedule(job,
        enforce)
    StatUtilsCalcMaxDiffSeconds(&tracker.maxMarkScheduleTime,
        preTime)

    return err
}

func (tracker *jobTracker)AddJob(job Job) error {
    err := tracker.priQueue.Enqueue(job)
    if err != nil {
        SchedulerLogger.Errorf("Fail to add job %s to tracker:%s\n",
            job.GetID(), err.Error())
        return err
    }

    tracker.rwHashLock.Lock()
    tracker.jobHashQueue[job.GetID()] = job
    tracker.rwHashLock.Unlock()

    tracker.EvaluateJobMetric(job, false)


    return nil
}

func (tracker *jobTracker)RemoveJob(job Job) error {
    /*
     * update job metric before untrack it, this is to
     * reflect changes to queue metrics
     */
    tracker.EvaluateJobMetric(job, true)
    tracker.rwHashLock.Lock()
    delete(tracker.jobHashQueue, job.GetID())
    tracker.rwHashLock.Unlock()

    err := tracker.priQueue.Dequeue(job)

    SchedulerLogger.Infof("Job %s is removed from job tracker\n",
        job.GetID())

    /*Finally destroy job to help GC*/
    job.Fini()

    return err
}

func (tracker *jobTracker) TrackBackendTask(job Job, stage Stage, backendId string,
    taskId string) error {
    schedInfo := job.GetScheduleInfo()
    if schedInfo == nil {
        return errors.New("No schedule info for job")
    }

    schedInfo.AddPendingStage(stage, backendId, taskId)
    if taskId != "" {
        tracker.lock.Lock()
        tracker.taskCount ++
        tracker.lock.Unlock()

        tracker.priQueue.UpdateTaskMetric(job, 1, 0)
        GetUserMgr().AccountTasks(job.GetSecurityContext(), 1)
    }

    return nil
}

func (tracker *jobTracker) UnTrackBackendTask(job Job, stageId string,
    success bool) error {
    schedInfo := job.GetScheduleInfo()
    if schedInfo != nil {
        schedInfo.CompletePendingStage(stageId,
            success)
        tracker.lock.Lock()
        tracker.taskCount --
        tracker.lock.Unlock()

        tracker.priQueue.UpdateTaskMetric(job, 0, 1)
        GetUserMgr().AccountTasks(job.GetSecurityContext(), -1)
    } else {
        return errors.New("No schedule info for job")
    }

    return nil
}

func (tracker *jobTracker) GetJobStats() map[string]BioflowJobStats {
    return tracker.priQueue.GetJobStats()
}

func (tracker *jobTracker) GetSchedulePerfStats() *BioflowJobSchedulePerfStats {
    return &BioflowJobSchedulePerfStats{
            MaxScheduleTime: tracker.maxScheduleTime,
            MaxMarkScheduleTime: tracker.maxMarkScheduleTime,
            MaxJobCheckTime: tracker.maxJobCheckTime,
    }
}

func (tracker *jobTracker) MaxScheduleTime() float64 {
    return tracker.maxScheduleTime
}

func (tracker *jobTracker) MaxMarkScheduleTime() float64 {
    return tracker.maxMarkScheduleTime
}

func (tracker *jobTracker) EvaluateJobMetric(job Job,
    clear bool) {
    tracker.priQueue.UpdateJobMetric(job, clear)
}

func (tracker *jobTracker) FindJobByID(jobId string) (error, Job) {
    tracker.rwHashLock.RLock()
    defer tracker.rwHashLock.RUnlock()

    if job, ok := tracker.jobHashQueue[jobId]; ok {
        return nil, job
    }

    return errors.New("Job " + jobId + " not exist"),
        nil
}

func (tracker *jobTracker) TraverseJobList(fn func(Job)bool) {
    tracker.rwHashLock.RLock()
    defer tracker.rwHashLock.RUnlock()

    for _, job := range tracker.jobHashQueue {
        if fn(job) {
            return
        }
    }
}

func (tracker *jobTracker) SearchJobList(ctxt *SecurityContext,
    listOpt *JobListOpt, curCount *int) (error, []BioflowJobInfo) {
    tracker.rwHashLock.RLock()
    defer tracker.rwHashLock.RUnlock()

    sum := *curCount
    jobList := make([]BioflowJobInfo, 0)
    var err error = nil
    var start time.Time
    var end time.Time
	if listOpt.After != "" {
		start, err = TimeUtilsUserInputToBioflowTime(listOpt.After)
        if err != nil {
            SchedulerLogger.Errorf("Parse user input time %s error: %s\n",
                listOpt.After, err.Error())
            return err, nil
        }
	}
	if listOpt.Before != "" {
		end, err = TimeUtilsUserInputToBioflowTime(listOpt.Before)
        if err != nil {
            SchedulerLogger.Errorf("Parse user input time %s error: %s\n",
                listOpt.Before, err.Error())
            return err, nil
        }
	}

    for _, job := range tracker.jobHashQueue {
		if listOpt.Count != 0 && sum == listOpt.Count {
			break
		}

		if listOpt.Name != "" && job.Name() != listOpt.Name {
			continue
		}

		if listOpt.Pipeline != "" && job.PipelineName() != listOpt.Pipeline {
			continue
		}

        /*if priority is -1, don't use it*/
        if listOpt.Priority != -1 && job.Priority() != listOpt.Priority {
            continue
        }

        if listOpt.State != "" && JobStateToStr(job.State()) != strings.ToUpper(listOpt.State) {
            continue
        }

        /*security check*/
        if !ctxt.CheckSecID(job.SecID()) {
            continue
        }

		if listOpt.Finished == "" {
			var t time.Time

			t = job.CreateTime()

			if err == nil && listOpt.After != "" &&  !t.After(start) {
				continue
			}

			if err == nil && listOpt.Before != "" && !t.Before(end) {
				continue
			}
		}

        jobInfo := BioflowJobInfo {
            JobId: job.GetID(),
            Name: job.Name(),
            Pipeline: job.PipelineName(),
            Created: job.CreateTime().Format(BIOFLOW_TIME_LAYOUT),
            State: JobStateToStr(job.State()),
            Owner: job.SecID(),
            Priority: job.Priority(),
            ExecMode: job.ExecMode(),
        }
        jobList = append(jobList, jobInfo)
		sum += 1
    }
    *curCount = sum

    return nil, jobList
}

func (tracker *jobTracker)GetAllJobsByPrivilege(ctxt *SecurityContext) []Job {
    tracker.rwHashLock.RLock()
    defer tracker.rwHashLock.RUnlock()

    runJobs := make([]Job, 0)
    for _, job := range tracker.jobHashQueue {
        /*Security check*/
        if !ctxt.CheckSecID(job.SecID()) {
            continue
        }

        runJobs = append(runJobs, job)
    }

    return runJobs
}

func (tracker *jobTracker)GetHangJobs(ctxt *SecurityContext) []BioflowJobInfo {
    tracker.rwHashLock.RLock()
    defer tracker.rwHashLock.RUnlock()

    hangJobs := make([]BioflowJobInfo, 0)
    for _, job := range tracker.jobHashQueue {
        /*Security check*/
        if !ctxt.CheckSecID(job.SecID()) || !job.IsRunning() {
            continue
        }

        schedInfo := job.GetScheduleInfo()
        if schedInfo != nil {
            hangStages := schedInfo.GetHangStageSummaryInfo()
            if hangStages != nil && len(hangStages) > 0 {
                jobInfo := BioflowJobInfo {
                    JobId: job.GetID(),
                    Name: job.Name(),
                    Pipeline: job.PipelineName(),
                    Created: job.CreateTime().Format(BIOFLOW_TIME_LAYOUT),
                    State: JobStateToStr(job.State()),
                    Owner: job.SecID(),
                    Priority: job.Priority(),
                    ExecMode: job.ExecMode(),
                    HangStages: hangStages,
                }
                hangJobs = append(hangJobs, jobInfo)
            }
        }
    }

    return hangJobs
}

/*
 * Get all jobs list which not updated state for a long time
 *
 */
func (tracker *jobTracker)GetJobsNeedCheck(minutes float64) []Job {
    /*
     * When the job state check task wants to check all task state which
     * haven't been updated for a long time. It may be a good chance to
     * re-caculate the max queued time for all tasks. So we reset the 
     * queue max time here.
     *
     * It is not good enough, so need a better place to do the task.
     */
    preTime := time.Now()
    tracker.priQueue.ResetMaxQueueTime()

    tracker.rwHashLock.RLock()
    defer tracker.rwHashLock.RUnlock()
    nowTime := time.Now()
    staleJobs := make([]Job, 0)
    for _, job := range tracker.jobHashQueue {
        /*
         * There may be more than 10000+ jobs, so need filter it by only
         * check:
         * 1) the running jobs
         * 2) jobs with pending tasks
         * 3) jobs whose state is stale
         */
        if job.IsRunning() {
            schedInfo := job.GetScheduleInfo()
            if schedInfo == nil {
                continue
            }

            hasPendingTask, hasHangTask := schedInfo.CheckPendingOrHangTasks()
            if hasHangTask {
                staleJobs = append(staleJobs, job)
                continue
            }

            if hasPendingTask && nowTime.Sub(job.UpdateTime()).Minutes() > minutes {
                staleJobs = append(staleJobs, job)
            }
        }
    }

    StatUtilsCalcMaxDiffSeconds(&tracker.maxJobCheckTime,
        preTime)

    return staleJobs
}

func (tracker *jobTracker)NeedCheckAllPendingJobs() bool {
    return tracker.jobScheduler.NeedCheckAllPendingJobs()
}
