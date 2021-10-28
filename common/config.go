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
    "fmt"
	"path"
	"os/user"

    xconfig "github.com/xtao/xstone/config"
)

const (
	BIOFLOW_DEFAULT_ENDPOINT = "http://127.0.0.1:2379"
	BIOFLOW_DEFAULT_SERVER = "127.0.0.1:9090"
)

func ClientConfFile() string {
    dir := ClientConfDir()
	if dir == "" {
		return ""
	}
	return path.Join(dir, "bioflow_client.conf")
}

func ClientConfDir() string {
	user, err := user.Current()
	if err != nil {
        Logger.Errorf("Fail to get current user: %s\n",
            err.Error())
		return ""
	}
	return user.HomeDir
}

type JobMgrConfig struct {
}

type DBServiceConfig struct {
   Server string
   Port   string
   User   string
   Password string
   Driver  string
   DBName   string
}

type LoggerConfig struct {
    Logfile string
    LogLevel int
}


type BioflowConfig struct {
    StoreConfig xconfig.StoreMgrConfig 
    DBConfig DBServiceConfig
    LoggerConfig LoggerConfig
    BackendConfig []BackendConfig
    RestConfig RestServerConfig
    ClusterConfig BioflowClusterConfig
    SecurityConfig SecurityConfig
    MailConfig MailConfig
    FrontEndConfig []FrontEndConfig
    PipelineStoreConfig PipelineStoreConfig
    PhysicalConfig PhysicalConfig
    FileDownloadConfig FileDownloadConfig
}

type LoglevelServerConfig struct {
    LogLevel int
}

type GlobalConfig struct {
    LogLevel int
}

/*define the backend type*/
const (
    BACKEND_TYPE_PALADIN string = "paladin"
    BACKEND_TYPE_KUBERNETES string = "kubernetes"
    BACKEND_TYPE_SIMULATOR string = "simulator"
    BACKEND_TYPE_EREMETIC string = "eremetic"
)

type BackendConfig struct {
    Type string      //kubernettes or paladin
    Server string
    Port string
}

type RestServerConfig struct {
    Server string
    Port string
}

/*configuration for bioflow queue*/
type BioflowQueueConfig struct {
    DBConfig DBServiceConfig
    RestConfig RestServerConfig
    LoglevelConfig LoglevelServerConfig
}

type BioflowClusterGlobalConfig struct {
    QueueCount int
}

/*configuration for bioflow cluster or queues*/
type BioflowClusterConfig struct {
    GlobalConfig BioflowClusterGlobalConfig
    QueueConfig map[string]BioflowQueueConfig
}

type PhysicalConfig struct {
    SchedEnabled bool
    EndPoint string
    NFSServer string
    ThroatePeriod int
    ThroateBatchCount int
    DelayPeriod int
    DelayBatchCount int
    ThroateInstancesLimit int
    DelayInstancesLimit int
    MasterInstances int
    InstanceType string
    DeleteInstancesInterval int
}

func (config *BioflowClusterConfig) GetQueueDBConfig(queue int) *DBServiceConfig {
    queueKey := fmt.Sprintf("%d", queue)
    if queueConfig, ok := config.QueueConfig[queueKey]; ok {
        return &queueConfig.DBConfig 
    }

    return nil
}

func (config *BioflowClusterConfig) GetQueueRestConfig(queue int) *RestServerConfig {
    queueKey := fmt.Sprintf("%d", queue)
    if queueConfig, ok := config.QueueConfig[queueKey]; ok {
        return &queueConfig.RestConfig
    }

    return nil
}

func (config *BioflowClusterConfig) GetQueueLoglevelConfig(queue int) *LoglevelServerConfig {
    queueKey := fmt.Sprintf("%d", queue)
    if queueConfig, ok := config.QueueConfig[queueKey]; ok {
        return &queueConfig.LoglevelConfig
    }

    return nil
}


const (
    BIOFLOW_STATE_INVALID int = 0
    BIOFLOW_STATE_RUNNING int = 1
    BIOFLOW_STATE_EXITED int = 2
    BIOFLOW_STATE_SYNC int = 3
    BIOFLOW_STATE_WAIT_OTHER_EXIT int = 4
    BIOFLOW_STATE_REGISTERED int = 5
    BIOFLOW_STATE_REREGISTERED int = 6
    BIOFLOW_STATE_QUIESCING int = 7
    BIOFLOW_STATE_QUIESCED int = 8
)

func BioflowStateToStr(state int) string {
    switch state {
        case BIOFLOW_STATE_INVALID:
            return "INVALID"
        case BIOFLOW_STATE_RUNNING:
            return "RUNNING"
        case BIOFLOW_STATE_EXITED:
            return "EXITED"
        case BIOFLOW_STATE_SYNC:
            return "SYNC"
        case BIOFLOW_STATE_WAIT_OTHER_EXIT:
            return "WAIT OTHER EXIT"
        case BIOFLOW_STATE_REGISTERED:
            return "REGISTERED"
        case BIOFLOW_STATE_REREGISTERED:
            return "REREGISTERED"
        case BIOFLOW_STATE_QUIESCING:
            return "QUIESCING"
        case BIOFLOW_STATE_QUIESCED:
            return "QUIESCED"
        default:
            return "UNKNOWN"
    }
}

type BioflowInstanceConfig struct {
    Seq uint64
    Version string
    State int
}

/*
 * configuration about the security
 */
const (
    SEC_MODE_ISOLATION string = "isolation"
    SEC_MODE_STRICT string = "strict"
)

type SecurityConfig struct {
    SecMode string
    MaxTaskNum int64
    MaxJobNum int64
    LDAPServer string
    UseLDAP bool
}

type MailConfig struct {
    User string
    Pass string
    Host string
}

type PipelineStoreConfig struct {
    BaseDir string
}

type FrontEndConfig struct {
    Url string
    Queue int
}

type FileDownloadConfig struct {
    Cpu float64
    Mem float64
}

/*
 * configuration or tunning parameters for job 
 * scheduler
 */
type JobSchedulerConfig struct {
    MaxJobsSchedulePerRound int
    QueueThroateThreshold float64
    QueueDelayThreshold float64
    PriThroateFactor float64
    MaxJobsAllowed int64
    MaxTasksAllowed int64
    JobDelayCheckTimeout int
    ScheduleCheckPeriod int
    JobCheckInterval int
    ThroateTaskNum int64
    DelayTaskNum int64
    MaxAllowDelayInterval float64
    AutoReScheduleHangTasks bool
    DisableOOMKill bool
    SoftLimitRatio float64
    AutoScheduleHDFS bool
    QosEnabled bool
    ProfilerEnabled bool
    CPUCapRatio float64
    MemCapRatio float64
    EnableStorageConstraint bool
    EnforceServerTypeConstraint bool
    SetStageWorkdirToContainerWorkdir bool
    CleanupPatternEnable bool
    MaxCPUPerTask float64
    MaxMemoryPerTask float64
    UseTaskOwner bool
    EnableRightControl bool
    EnableStageWorker bool
    JobScheduleWorkerCount int64
    StageQuota map[int]int
    StageRetryDelay int
    AsyncTrackTaskInDB bool
}
