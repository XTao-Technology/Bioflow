/* 
 Copyright (c) 2018 XTAO technology <www.xtaotech.com>
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
    "errors"

    "github.com/xtao/profiler"
    . "github.com/xtao/bioflow/common"
    . "github.com/xtao/bioflow/message"
    . "github.com/xtao/bioflow/scheduler/common"
)

var (
    ErrTaskProfilingStatsNotFound error = errors.New("Task profiling stats not found")
)

const (
    TRACE_PROFILING_INTERVAL float64 = 600
)

func GetTaskProfilingSummary(logDir string, stageName string, taskId string) (*ResourceUsageSummary, error) {
    jfs := NewJobFileSet(logDir)
    profilePath, err := jfs.GetProfilingFileByStageTask(stageName, taskId)
    if err != nil {
        SchedulerLogger.Errorf("Fail to evaluate task %s profile file: %s\n",
            taskId, err.Error())
        return nil, err
    }

    pryer := profiler.NewTracePryer()
    report, err := pryer.InquiryTraceFile(TRACE_PROFILING_INTERVAL,
        profilePath, false)
    if err != nil {
        SchedulerLogger.Errorf("The profiler fail to evaluate the trace file %s: %s\n",
            profilePath, err.Error())
        return nil, err
    }

    stats := &ResourceUsageSummary{
        MaxCPURatio: report.MaxCPURatio,
        AvgCPURatio: report.AvgCPURatio,
        LowCPURatio: report.LowCPURatio,
        MaxMem: report.MaxMem,
        AvgMem: report.AvgMem,
        MaxIOMem: report.MaxIOMem,
        AvgIOMem: report.AvgIOMem,
        MaxSwap: report.MaxSwap,
        AvgSwap: report.AvgSwap,
        TotalRead: report.ReadBytes,
        TotalWrite: report.WriteBytes,
        TotalSysCR: report.SysCR,
        TotalSysCW: report.SysCW,
        OOM: report.OOM,
        MaxProfilingCost: report.MaxProfilingCost,
    }

    return stats, nil
}

func GetDetailTaskResourceUsageInfo(logDir string, taskId string) (map[string]ResourceUsageInfo, error) {
    jfs := NewJobFileSet(logDir)
    /* there exists possibility that same duplicate task id assigned to different
     * stages. Although it is very low probability.
     */
    filesMap, err := jfs.GetTaskProfilingFiles(taskId)
    if err != nil {
        SchedulerLogger.Errorf("Fail to get task %s profile file: %s\n",
            taskId, err.Error())
        return nil, err
    }

    SchedulerLogger.Debugf("Found %d files for task %s\n",
        len(filesMap), taskId)

    usageReports := make(map[string]ResourceUsageInfo)
    pryer := profiler.NewTracePryer()
    for task, file := range filesMap {
        report, err := pryer.InquiryTraceFile(TRACE_PROFILING_INTERVAL,
            file, true)
        if err != nil {
            SchedulerLogger.Errorf("The profiler fail to evaluate the trace file %s: %s\n",
                file, err.Error())
            return nil, err
        }
        usageReport := ResourceUsageInfo{
            MaxCPURatio: report.MaxCPURatio,
            AvgCPURatio: report.AvgCPURatio,
            LowCPURatio: report.LowCPURatio,
            MaxMem: report.MaxMem,
            AvgMem: report.AvgMem,
            MaxIOMem: report.MaxIOMem,
            AvgIOMem: report.AvgIOMem,
            MaxSwap: report.MaxSwap,
            AvgSwap: report.AvgSwap,
            TotalRead: report.ReadBytes,
            TotalWrite: report.WriteBytes,
            TotalSysCR: report.SysCR,
            TotalSysCW: report.SysCW,
            OOM: report.OOM,
            MaxProfilingCost: report.MaxProfilingCost,
        }
        if report.Stats != nil {
            usageReport.Periods = make([]PeriodResourceUsageInfo, 0)
            for _, stat := range report.Stats {
                periodInfo := PeriodResourceUsageInfo{
                    AvgSysRatio: stat.AvgSysRatio,
                    AvgUserRatio: stat.AvgUserRatio,
                    MaxMem: stat.MaxMem,
                    AvgMem: stat.AvgMem,
                    MaxCache: stat.MaxCache,
                    AvgCache: stat.AvgCache,
                    MaxSwap: stat.MaxSwap,
                    AvgSwap: stat.AvgSwap,
                    TotalRead: stat.ReadBytes,
                    TotalWrite: stat.WriteBytes,
                    TotalSysCR: stat.SysCR,
                    TotalSysCW: stat.SysCW,
                }
                usageReport.Periods = append(usageReport.Periods, periodInfo)
            }
        }

        usageReports[task] = usageReport
    }

    return usageReports, nil
}
