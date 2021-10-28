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
    "sync"
    "time"

    . "github.com/xtao/bioflow/common"
    . "github.com/xtao/bioflow/message"
    . "github.com/xtao/bioflow/scheduler/common"
    "github.com/xtao/bioflow/scheduler/physicalscheduler"
)


/*
 * A job scheduler to schedule jobs according to the priority
 * and task statistics. It intends to solve the following issues:
 * 1) lower priority job schedules too many tasks to delay high
 *    priority jobs
 * 2) some tasks requiring high CPUs or memories suffer starvation 
 *    because many small tasks continue executing
 */
type priJobScheduler struct {
    lock sync.Mutex
    priQueue *jobPriQueue

    /*tuning parameters*/

    /*avg waiting time to start throate schedule*/
    throateWaterMark float64

    /*avg wait time to start delay schedule*/
    delayWaterMark float64

    /*the discount factor the lower priority be punished*/
    priThroateFactor float64

    /*the queued task num to start throate schedule*/
    throateTaskNum int64

    /*the queued task num to start delay schedule*/
    delayTaskNum int64

    /*
     * the max agg delay period to start schedule task again.
     * if a task queued too much, we need delay scheduler to
     * let backend aggregate enough resources for the task. but
     * if one task delays other ready tasks too much, it will
     * lower the resource utilization. So we choose schedule some
     * jobs every interval to not hang the scheduler. by default,
     * it will be set very big.
     */
    maxAllowDelayInterval float64

    enforceAllJobCheck bool
}

const (
    DEF_THROATE_WATERMARK float64 = 500
    DEF_DELAY_WATERMARK float64 = 30000
    FLOAT_ZERO_LIMIT float64 = 0.0001
    PRI_THROATE_FACTOR float64 = 0.5
    DEF_THROATE_TASK_NUM int64 = 3000
    DEF_DELAY_TASK_NUM int64 = 10000
    MAX_QUEUE_TIME_STALE_PERIOD float64 = 40
    DEF_MAX_ALLOW_DELAY_INTERVAL float64 = 99999
)


func NewPriJobScheduler(queue *jobPriQueue) *priJobScheduler{
    return &priJobScheduler{
            priQueue: queue,
            throateWaterMark: DEF_THROATE_WATERMARK,
            delayWaterMark: DEF_DELAY_WATERMARK,
            priThroateFactor: PRI_THROATE_FACTOR,
            throateTaskNum: DEF_THROATE_TASK_NUM,
            delayTaskNum: DEF_DELAY_TASK_NUM,
            maxAllowDelayInterval: DEF_MAX_ALLOW_DELAY_INTERVAL,
    }
}

const (
    VALID_WM_CONFIG_LIMIT float64 = 5
    VALID_FACTOR_CONFIG_LIMIT float64 = 0.05
    MIN_AGG_DELAY_THROATE_RATIO float64 = 0.0
    ENFORCE_ALL_JOB_CHECK_TASK_NUM int64 = 20
)

func (scheduler *priJobScheduler)NeedCheckAllPendingJobs() bool {
    return scheduler.enforceAllJobCheck
}

func (scheduler *priJobScheduler)MarkAllJobNeedCheck(need bool) {
    scheduler.enforceAllJobCheck = need
}


func (scheduler *priJobScheduler)UpdateConfig(config *JobSchedulerConfig) {
    if config != nil {
        if config.QueueThroateThreshold > VALID_WM_CONFIG_LIMIT {
            scheduler.throateWaterMark = config.QueueThroateThreshold
        }
        if config.QueueDelayThreshold > VALID_WM_CONFIG_LIMIT {
            scheduler.delayWaterMark = config.QueueDelayThreshold
        }
        if config.PriThroateFactor > VALID_FACTOR_CONFIG_LIMIT && config.PriThroateFactor < 1 {
            scheduler.priThroateFactor = config.PriThroateFactor
        }
        if config.ThroateTaskNum > 0 {
            scheduler.throateTaskNum = config.ThroateTaskNum
        }
        if config.DelayTaskNum > 0 {
            scheduler.delayTaskNum = config.DelayTaskNum
        }

        /*don't support too small setting*/
        if config.MaxAllowDelayInterval > 3.0 {
            scheduler.maxAllowDelayInterval = config.MaxAllowDelayInterval
            MaxAllowTaskTimeout = config.MaxAllowDelayInterval
        }
    }
}

func (scheduler *priJobScheduler)ThroatePolicy(metric *queueScheduleMetric) (bool, float64) {
    metric.lock.Lock()
    defer metric.lock.Unlock()

    /*
     * Policy 1: limit the max queued time of tasks. Because if a task
     * queued too long in backend and can't be executed.  It may be because
     * that it requires a lot of resources. So if tasks requiring less resources
     * keeps being submitted, it will not have a chance to be scheduled. So the
     * scheduler should be throated to wait for while. This will help backend scheduler
     * to aggregate enough resources.

     * But if a task can't be scheduled by backend in some special case, whole
     * scheduler will be blocked. So we should be very careful and collect enough
     * statistics.
     *
     */
    if metric.maxTaskQueueTime > scheduler.delayWaterMark {
        /* collect stats to evaluate aggregate effects on resource utilization
         * because delay will lower the utilization
         */
        metric.aggDelayRounds ++
        totalTasks := metric.totalQueuedTasks + metric.totalRunningTasks
        if totalTasks > metric.maxAggTaskNum {
            metric.maxAggTaskNum = totalTasks
        }
        if totalTasks < metric.minAggTaskNum {
            metric.minAggTaskNum = totalTasks
        }

        if totalTasks < ENFORCE_ALL_JOB_CHECK_TASK_NUM {
            scheduler.MarkAllJobNeedCheck(true)
        }

        if !metric.aggDelay {
            metric.aggDelay = true
            metric.aggDelayTime = time.Now()
        } else {
            /*
             * A hang task may block scheduler forever, so we may need schedule
             * some jobs after a timeout. Don't schedule jobs by default.
             */
            delayTime := time.Now().Sub(metric.aggDelayTime).Minutes()
            if delayTime > scheduler.maxAllowDelayInterval {
                SchedulerLogger.Infof("Delay schedule %f minutes, schedule %f jobs\n",
                    delayTime, MIN_AGG_DELAY_THROATE_RATIO)
                return true, MIN_AGG_DELAY_THROATE_RATIO
            }
        }
        return true, 0
    } else {
        scheduler.MarkAllJobNeedCheck(false)
        if metric.aggDelay {
            metric.aggDelay = false
            delayTime := time.Now().Sub(metric.aggDelayTime).Minutes()
            if delayTime > metric.maxAggDelayPeriod {
                metric.maxAggDelayPeriod = delayTime
            }
            metric.totalAggDelayPeriod += delayTime
        }
    }

    /*
     * Policy 2: limit the number of tasks queued in backend. 
     */
    if metric.totalQueuedTasks <= 0 {
        return false, 0
    }

    if metric.totalQueuedTasks > scheduler.delayTaskNum {
        return true, 0
    }

    /*
     * Policy 3: limit the average queue time
     */
    avgQueueTime := metric.totalQueueTime / float64(metric.totalQueuedTasks)
    if avgQueueTime > scheduler.delayWaterMark {
        return true, 0
    }

    /*
     * Policy 4: throate if too many queued tasks
     */
    if metric.totalQueuedTasks > scheduler.throateTaskNum {
        return true,
            float64(scheduler.throateTaskNum) / float64(2 * metric.totalQueuedTasks) 
    }

    /*
     * Policy 5: throate if too many average queue time
     */
    ratio := avgQueueTime / scheduler.throateWaterMark
    if ratio < 1.0 {
        return false, 0
    }

    return true, 1 / (2 * ratio)
}

func (scheduler *priJobScheduler)MaxJobsAllowedToSchedule(metric *queueScheduleMetric) int {
    maxTasks := scheduler.delayTaskNum - metric.totalQueuedTasks
    return int(maxTasks)
}

func (scheduler *priJobScheduler)SelectJobs(scheduleLimit int) []Job {
    /*
     * select jobs with following policy:
     * 1) select jobs from higher priority to lower priority
     * 2) drop jobs whose user exceeding task quota
     * 3) If some priority queue's total queue time exceeds the
     *    throate water mark, just slow lower priority queue's 
     *    schedule
     * 5) If some priority queue's total queue time exceeds the
     *    disable water mark, don't schedule lower priority queue's
     *    jobs
     */
    priQueue := scheduler.priQueue
    maxJobs := scheduleLimit

    /*
     * Throate policy is simple:
     * 1) schedule jobs from high priority to low priority
     * 2) if a priority queued time exceeds threshold, then
     *    evaluated a priRatio, which means what ratio of current
     *    need schedule jobs of this priority can be scheduled. 
     * 3) The throateRatio records the ratio inherits from higher
     *    priority. Current priority should use the:
     *       min(priRatio, throateRatio * 0.5)
     *
     *    the 0.5 is a factor to make low priority jobs punished more
     *    than high priority jobs
     * 4) if too many queued tasks (or time), the ratio will be 0. which
     *    means all job below this priority will not be scheduled for
     *    a period of time
     */
    var throateRatio float64 = 0
    highPriThroate := false
    allJobs := make([]Job, 0)
    jobThroated := false
    jobDelayed := false
    for pri := JOB_MAX_PRI; pri >= JOB_MIN_PRI; pri -- {
        jobQueue := priQueue.GetQueue(pri, false)
        if jobQueue == nil {
            continue
        }
        queueMetric := jobQueue.GetQueueMetric()
        throate, priRatio := scheduler.ThroatePolicy(queueMetric)
        /*
         * Evaluate the job throate state to notify physical machine
         * scheduler
         */
        if throate {
            jobThroated = true
            if priRatio == 0 {
                jobDelayed = true
            }
        }

        /*
         * Need control the maximal jobs scheduled per time. two cases:
         * 1) priority quota > job schedule limit: use job schedule limit
         * 2) priority quota < job schedule limit: adjust job schedule limit
         *    to restrict all jobs scheduled this or lower priority
         */
        maxJobsAllowed := scheduler.MaxJobsAllowedToSchedule(queueMetric)
        if maxJobsAllowed > maxJobs {
            maxJobsAllowed = maxJobs
        } else {
            maxJobs = maxJobsAllowed
        }

        if !highPriThroate && !throate {
            /*
             * case 1: both higher pri and this pri don't require
             * throate, so just schedule all the jobs
             */
            jobs := jobQueue.SelectJobs(maxJobsAllowed, 1.1)
            if jobs != nil && len(jobs) > 0 {
                maxJobs -= len(jobs)
                allJobs = append(allJobs, jobs ...)
            }
        } else { 
            if highPriThroate && !throate {
                throateRatio = throateRatio * scheduler.priThroateFactor
            } else if !highPriThroate && throate {
                highPriThroate = true
                throateRatio = priRatio
            } else if highPriThroate && throate {
                throateRatio = throateRatio * scheduler.priThroateFactor
                if throateRatio > priRatio {
                    throateRatio = priRatio
                }
            }
            
            jobs := jobQueue.SelectJobs(maxJobsAllowed, throateRatio)
            if jobs != nil && len(jobs) > 0 {
                maxJobs -= len(jobs)
                allJobs = append(allJobs, jobs ...)
            }
        }

        if maxJobs <= 0 {
            /*
             * notify physical machine scheduler current job schedule state
             * this may trigger expand the backend compute cluster
             */
            scheduler.NotifyPhysicalSchedulerEvent(jobThroated, jobDelayed)
            return allJobs
        }
    }

    /*
     * notify physical machine scheduler current job schedule state
     * this may trigger expand the backend compute cluster
     */
    scheduler.NotifyPhysicalSchedulerEvent(jobThroated, jobDelayed)
    return allJobs
}

/*
 * Notify physical machine scheduler events:
 * 1) job throated: EVENTTHROATE
 * 2) job delayed: EVENTDELAY
 * 3) other: EVENTIDLE
 */
func (scheduler *priJobScheduler)NotifyPhysicalSchedulerEvent(throate bool, delay bool){
    event := physicalscheduler.EVENTIDLE
    if delay {
        event = physicalscheduler.EVENTDELAY
    } else if throate {
        event = physicalscheduler.EVENTTHROATE
    }
    phyScheduler := physicalscheduler.GetPhysicalScheduler()
    if phyScheduler != nil {
        phyEvent := physicalscheduler.PhysicalEvent{
            EventType: event,
            EventTime: time.Now(),
        }
        SchedulerLogger.Debugf("Notify physical machine scheduler event: %s\n",
            physicalscheduler.PhysicalEventtoString(phyEvent))
        phyScheduler.SubmitEvent(&phyEvent)
    }
}
