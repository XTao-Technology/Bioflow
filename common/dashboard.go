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
    "sort"
)

const (
    STAT_JOB_TOTAL string = "TotalJobs"
    STAT_JOB_RUNNING string = "RunningJobs"
    STAT_JOB_COMPLETE string = "CompletedJobs"
    STAT_JOB_CANCEL string = "CanceledJobs"
    STAT_JOB_FAIL string = "FailJobs"
    STAT_JOB_PSEUDONE string = "PseudoneJobs"
    STAT_JOB_RUN_FAIL string = "RunFailJobs"
    STAT_JOB_BUILD_FAIL string = "BuildFailJobs"
    STAT_JOB_PARSE_FAIL string = "ParseFailJobs"
    STAT_JOB_SUBMIT string = "SubmitJobs"
    STAT_JOB_SUBMIT_QUIESCE string = "QuiesceFailSubmitJobs"
    STAT_JOB_SUBMIT_FAIL string = "OtherFailSubmitJobs"
    STAT_JOB_RECOVER string = "RecoveredJobs"
    STAT_JOB_RECOVER_FAIL string = "RecoverFailedJobs"
    STAT_JOB_PREPARE_FAIL string = "PrepareFailedJobs"
    STAT_JOB_GRAPH_RESTORE_FAIL string = "GraphResotreFailedJobs"
    STAT_JOB_PRIVILEGE_FAIL string = "SetUserPrivilegeFailedJobs"

    STAT_DB_UPDATE_FAIL string = "DatabaseUpdateFailOps"
    STAT_DB_READ_FAIL string = "DatabaseReadFailOps"
    STAT_DB_INSERT_FAIL string = "DatabaseInsertFailOps"
    STAT_DB_RESTORE_FAIL string = "ResotreStateFromDBFailOps"
    
    STAT_OP_CLEANUP_STALE_OUTPUT string = "CleanupedStaleOutputFiles"

    STAT_TASK_TOTAL string = "TotalTasks"
    STAT_TASK_SUBMIT string = "SubmittedTasks"
    STAT_TASK_RUNNING string = "RunningTasks"
    STAT_TASK_COMPLETE string = "CompletedTasks"
    STAT_TASK_FAIL string = "FailTasks"
    STAT_TASK_SUBMIT_FAIL string = "SubmitFailTasks"
    STAT_TASK_LOST string = "LostTasks"
    STAT_TASK_RETRY string = "RetryTasks"
    STAT_TASK_KILL string = "KilledTasks"
    STAT_TASK_KILL_FAIL string = "KillFailedTasks"
    STAT_TASK_RECLAIM string = "ReclaimedTasks"
    STAT_TASK_RECLAIM_FAIL string = "ReclaimFailedTasks"
    STAT_INVALID_BACKEND_ID string = "FailInvalidBackendId"
    STAT_INVALID_TASK_ID string = "FailInvalidTaskId"
    STAT_RECOVERED_TASKS string = "RecoveredBackendTasks"
    STAT_RECOVER_TASK_FAIL string = "FailRecoveredBackendTasks"
    STAT_TASK_TRACK_FAIL string = "FailTrackedTasks"
    STAT_RESCHEDULE_HANG_TASK string = "ReScheduleHangTasks"

    STAT_TASK_STALE_NOTIFY string = "StaleTaskNotify"

    STAT_SCHED_FAIL_BY_QUIESCE string = "ScheduleFailByQuiesce"

    STAT_OP_MKDIR string = "MkdirOps"
    STAT_OP_MKDIR_FAIL string = "MkdirFailOps"
    STAT_OP_RMDIR string = "RmdirOps"
    STAT_OP_RMDIR_FAIL string = "RmdirFailOps"
    STAT_OP_CREAT_FILE string = "CreateFileOps"
    STAT_OP_CREAT_FILE_FAIL string = "CreateFileFailOps"
    STAT_OP_REMOVE_FILE string = "RemoveFileOps"
    STAT_OP_REMOVE_FILE_FAIL string = "RemoveFileFailOps"
    STAT_OP_READDIR string = "ReadDirOps"
    STAT_OP_READDIR_FAIL string = "ReadDirFailOps"
    STAT_OP_CHOWN string = "ChownOps"
    STAT_OP_CHOWN_FAIL string = "ChownFailOps"
    STAT_OP_CHMOD string = "ChmodOps"
    STAT_OP_CHMOD_FAIL string = "ChmodFailOps"
    STAT_OP_RENAME string = "RenameOps"
    STAT_OP_RENAME_FAIL string = "RenameFailOps"

    STAT_OP_SUBMIT_TASK string = "SubmitTaskOps"
    STAT_OP_SUBMIT_TASK_FAIL string = "FailSubmitTaskOps"
    STAT_OP_KILL_TASK string = "KillTaskOps"
    STAT_OP_KILL_TASK_FAIL string = "FailKillTaskOps"
    STAT_OP_CHECK_TASK string = "CheckTaskOps"
    STAT_OP_CHECK_TASK_FAIL string = "FailCheckTaskOps"
    STAT_OP_GET_LOG string = "GetLogOps"
    STAT_OP_GET_LOG_FAIL string = "FailGetLogOps"
    STAT_OP_DELETE_TASK string = "DeleteTaskOps"
    STAT_OP_DELETE_TASK_FAIL string = "FailDeleteTaskOps"
    STAT_OP_TASK_NOTIFY string = "TaskNotifyOps"

    STAT_OP_FS_MOUNT string = "FSMountOps"
    STAT_OP_FS_MOUNT_FAIL string = "FailFSMountOps"
)

type DashBoard struct {
    stats *KVStats
}

var gDashBoard *DashBoard = nil

func NewDashBoard() *DashBoard {
    return &DashBoard{
        stats: NewKVStats(),
    }
}

func GetDashBoard() *DashBoard {
    if gDashBoard == nil {
        gDashBoard = NewDashBoard()
    }

    return gDashBoard
}

func (board *DashBoard)UpdateStats(name string, num int) {
    board.stats.Update(name, num)
}

func (board *DashBoard)GetStats() map[string]int64 {
    return board.stats.GetStats()
}

func (board *DashBoard)GetOpStats() ([]string, []int64) {
    statMap := board.stats.GetStats()
    statKeys := make([]string, 0)
    for key, _ := range statMap {
        statKeys = append(statKeys, key)
    }
    sort.Strings(statKeys)
    statVals := make([]int64, 0)
    for i := 0; i < len(statKeys); i ++ {
        statVals = append(statVals, statMap[statKeys[i]])
    }

    return statKeys, statVals
}

