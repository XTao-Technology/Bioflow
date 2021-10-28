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
package main

import (
    "fmt"
    "strings"
    "os"
    "strconv"
    . "github.com/xtao/bioflow/common"
    . "github.com/xtao/bioflow/confbase"

    xconfig "github.com/xtao/xstone/config"
    "github.com/xtao/bioflow/message"
)

type configCommand struct {
}

func newConfigCommand() *configCommand {
    return &configCommand{
    }
}

func ShowBioflowConfig(config *BioflowConfig) {
    fmt.Printf("Bioflow Configuration:\n")
    fmt.Printf(" DB Configuration:\n")
    fmt.Printf("  Server: %s\n", config.DBConfig.Server)
    fmt.Printf("  Port: %s\n", config.DBConfig.Port)
    fmt.Printf("  User: %s\n", config.DBConfig.User)
    fmt.Printf("  Password: %s\n", config.DBConfig.Password)
    fmt.Printf("  Driver: %s\n", config.DBConfig.Driver)
    fmt.Printf("  Database: %s\n", config.DBConfig.DBName)
    fmt.Printf(" Storage Manager Configuration:\n")
    fmt.Printf("  Container Configuration: \n")
    conConfig := config.StoreConfig.ContainerConfig
    fmt.Printf("    Data Dir: %s \n", conConfig.ContainerDataDir)
    fmt.Printf("    Work Dir: %s \n", conConfig.ContainerWorkDir)
    fmt.Printf("    Temp Dir: %s \n", conConfig.ContainerTempDir)
    serverConfig := config.StoreConfig.ServerConfig
    fmt.Printf("  Server Configuration: \n")
    fmt.Printf("    Temp mount: %s \n", serverConfig.TempMnt)
    fmt.Printf("    Vol Mount Points: \n")
    for volURI, path := range serverConfig.VolMntPath {
        fmt.Printf("      %s:%s \n", volURI, path)
    }
    if serverConfig.JobRootPath != "" {
        fmt.Printf("    Job root path:\n")
        fmt.Printf("       %s\n", serverConfig.JobRootPath)
    }

    if config.FileDownloadConfig.Cpu != 0 || config.FileDownloadConfig.Mem != 0 {
        fmt.Printf("  File Download Config:\n")
        fmt.Printf("    Cpu: %f\n", config.FileDownloadConfig.Cpu)
        fmt.Printf("    Mem: %f\n", config.FileDownloadConfig.Mem)
    }
    schedulerConfig := config.StoreConfig.SchedulerConfig
    fmt.Printf("  Scheduler Configuration: \n")
    if schedulerConfig.AutoMount {
        fmt.Printf("    Vol Mount Mode: Auto\n")
    } else {
        fmt.Printf("    Vol Mount Mode: Manual\n")
    }

    fmt.Printf("    Vol Mount Points: \n")
    for volURI, path := range schedulerConfig.SchedVolMntPath {
        fmt.Printf("      %s:%s \n", volURI, path)
    }
    fmt.Printf(" PipelineStore Configuration:\n")
    fmt.Printf("  BaseDir:%s\n", config.PipelineStoreConfig.BaseDir)
    fmt.Printf(" Backend Configuration:\n")
    backendConfig := config.BackendConfig
    if backendConfig == nil {
        fmt.Printf("  No backend configured\n")
    } else {
        for i := 0; i < len(backendConfig); i ++ {
            fmt.Printf("  Backend %d:\n", i + 1)
            fmt.Printf("    Type:%s\n", backendConfig[i].Type)
            fmt.Printf("    Server:%s\n", backendConfig[i].Server)
            fmt.Printf("    Port:%s\n", backendConfig[i].Port)
        }
    }
    fmt.Printf(" Rest Server Configuration:\n")
    fmt.Printf("  Server: %s\n", config.RestConfig.Server)
    fmt.Printf("  Port: %s\n", config.RestConfig.Port)
    fmt.Printf(" Security Configuration:\n")
    fmt.Printf("  Security Mode: %s\n", config.SecurityConfig.SecMode)
    fmt.Printf("  MaxTaskNum: %v\n", config.SecurityConfig.MaxTaskNum)
    fmt.Printf("  MaxJobNum: %v\n", config.SecurityConfig.MaxJobNum)
    fmt.Printf("  LDAPServer: %v\n", config.SecurityConfig.LDAPServer)
    fmt.Printf("  UseLDAP: %v\n", config.SecurityConfig.UseLDAP)
    fmt.Printf(" Cluster Configuration:\n")
    fmt.Printf("  Queues: %d\n", config.ClusterConfig.GlobalConfig.QueueCount)
    for queueId, queueConfig := range config.ClusterConfig.QueueConfig {
        fmt.Printf("    Queue %s config: \n", queueId)
        fmt.Printf("      DB Configuration:\n")
        fmt.Printf("        Server: %s\n", queueConfig.DBConfig.Server)
        fmt.Printf("        Port: %s\n", queueConfig.DBConfig.Port)
        fmt.Printf("        User: %s\n", queueConfig.DBConfig.User)
        fmt.Printf("        Password: %s\n", queueConfig.DBConfig.Password)
        fmt.Printf("        Driver: %s\n", queueConfig.DBConfig.Driver)
        fmt.Printf("        Database: %s\n", queueConfig.DBConfig.DBName)
        fmt.Printf("      Rest Server Configuration:\n")
        fmt.Printf("        Server: %s\n", queueConfig.RestConfig.Server)
        fmt.Printf("        Port: %s\n", queueConfig.RestConfig.Port)
    }
    fmt.Printf(" Mail Configuration:\n")
    fmt.Printf("     User: %s\n", config.MailConfig.User)
    fmt.Printf("     Password: ******\n")
    fmt.Printf("     Host: %s\n", config.MailConfig.Host)

    if len(config.FrontEndConfig) > 0 {
        fmt.Printf(" Front-End Configuration:\n")
        for i, config := range(config.FrontEndConfig) {
            fmt.Printf("  Front-End %d\n", i)
            fmt.Printf("    Queue: %d\n", config.Queue)
            fmt.Printf("    Url: %s\n", config.Url)
        }
    }

    if config.PhysicalConfig.SchedEnabled {
        fmt.Printf(" Physical Scheduler Configuration:\n")
        fmt.Printf("  EndPoint: %s\n", config.PhysicalConfig.EndPoint)
        fmt.Printf("  NFSServer: %s\n", config.PhysicalConfig.NFSServer)
        fmt.Printf("  ThroatePeriod: %d\n", config.PhysicalConfig.ThroatePeriod)
        fmt.Printf("  ThroateBatchCount: %d\n", config.PhysicalConfig.ThroateBatchCount)
        fmt.Printf("  DelayPeriod: %d\n", config.PhysicalConfig.DelayPeriod)
        fmt.Printf("  DelayBatchCount: %d\n", config.PhysicalConfig.DelayBatchCount)
        fmt.Printf("  ThroateInstancesLimit: %d\n", config.PhysicalConfig.ThroateInstancesLimit)
        fmt.Printf("  DelayInstancesLimit: %d\n", config.PhysicalConfig.DelayInstancesLimit)
        fmt.Printf("  MasterInstances: %d\n", config.PhysicalConfig.MasterInstances)
        fmt.Printf("  InstanceType: %s\n", config.PhysicalConfig.InstanceType)
        fmt.Printf("  DeleteInstancesInterval: %d\n", config.PhysicalConfig.DeleteInstancesInterval)
    }
}

func (cmd *configCommand) Init(epoints string, file string) {
    config := LoadConfigFromJSONFile(file)
    if config == nil {
        fmt.Printf("Fail to parse config from file %s\n", file)
        os.Exit(1)
    }
    servers := ParseEndpoints(epoints)
    db := NewBioConfDB(servers)

    err := db.CompareAdminPassword()
    if err != nil {
        fmt.Printf("%s\n", err.Error())
        os.Exit(1)
    }

    err = db.SaveBioflowConfig(config)
    if err != nil {
        fmt.Printf("Fail to save config to ETCD: %s\n",
            err.Error())
        os.Exit(1)
    }
}

func (cmd *configCommand) UpdateClusterConfig(epoints, file string) {
    config := LoadConfigFromJSONFile(file)
    if config == nil {
        fmt.Printf("Fail to parse config from file %s\n", file)
        os.Exit(1)
    }
    servers := ParseEndpoints(epoints)
    db := NewBioConfDB(servers)

    err := db.CompareAdminPassword()
    if err != nil {
        fmt.Printf("%s\n", err.Error())
        os.Exit(1)
    }

    /*only update the queue config*/
    err = db.SaveClusterConfig(&config.ClusterConfig, true)
    if err != nil {
        fmt.Printf("Fail to save cluster config to ETCD: %s\n",
            err.Error())
        os.Exit(1)
    }
}

func (cmd *configCommand) Dump(epoints string) {
    servers := ParseEndpoints(epoints)
    db := NewBioConfDB(servers)

    err := db.CompareAdminPassword()
    if err != nil {
        fmt.Printf("%s\n", err.Error())
        os.Exit(1)
    }

    err, config := db.LoadBioflowConfig()
    if err != nil {
        fmt.Printf("Fail to load config from ETCD: %s\n",
            err.Error())
        os.Exit(1)
    } else {
        ShowBioflowConfig(config)
    }
}

func (cmd *configCommand) MapServerVol(epoints string, volMap string) {
    servers := ParseEndpoints(epoints)
    db := NewBioConfDB(servers)

    err := db.CompareAdminPassword()
    if err != nil {
        fmt.Printf("%s\n", err.Error())
        os.Exit(1)
    }

    volItems := strings.Split(volMap, ":")
    if len(volItems) != 2 || volItems[0] == "" || volItems[1] == "" {
        fmt.Printf("The -m parameter should be: vol@cluster:mnt or vol:mnt\n")
        os.Exit(1)
    }
    volMaps := make(map[string]string)
    volMaps[volItems[0]] = volItems[1]
    err = db.UpdateServerVolumeMap(volMaps, nil)
    if err != nil {
        fmt.Printf("Fail to update server volume map: %s\n",
            err.Error())
        os.Exit(1)
    }
}

func (cmd *configCommand) UnmapServerVol(epoints string, vol string) {
    servers := ParseEndpoints(epoints)
    db := NewBioConfDB(servers)

    err := db.CompareAdminPassword()
    if err != nil {
        fmt.Printf("%s\n", err.Error())
        os.Exit(1)
    }

    vols := make([]string, 0)
    vols = append(vols, vol)
    err = db.UpdateServerVolumeMap(nil, vols)
    if err != nil {
        fmt.Printf("Fail to delete %s from server volume map: %s\n",
            vol, err.Error())
        os.Exit(1)
    }
}

func (cmd *configCommand) SetJobRootPath(epoints string, path string) {
    servers := ParseEndpoints(epoints)
    db := NewBioConfDB(servers)

    err := db.CompareAdminPassword()
    if err != nil {
        fmt.Printf("%s\n", err.Error())
        os.Exit(1)
    }
    path = strings.TrimSuffix(path, "/")

    err = db.UpdateServerJobRootPath(path)
    if err != nil {
        fmt.Printf("Fail to update job root path: %s\n",
            err.Error())
        os.Exit(1)
    }
}

func (cmd *configCommand) UnSetJobRootPath(epoints string) {
    servers := ParseEndpoints(epoints)
    db := NewBioConfDB(servers)

    err := db.CompareAdminPassword()
    if err != nil {
        fmt.Printf("%s\n", err.Error())
        os.Exit(1)
    }

    err = db.UpdateServerJobRootPath("")
    if err != nil {
        fmt.Printf("Fail to delete job root path: %s\n",
            err.Error())
        os.Exit(1)
    }
}

func (cmd *configCommand) MapSchedulerVol(epoints string, volMap string) {
    servers := ParseEndpoints(epoints)
    db := NewBioConfDB(servers)

    err := db.CompareAdminPassword()
    if err != nil {
        fmt.Printf("%s\n", err.Error())
        os.Exit(1)
    }

    volItems := strings.Split(volMap, ":")
    if len(volItems) != 2 || volItems[0] == "" || volItems[1] == "" {
        fmt.Printf("The -m parameter should be: vol@cluster:mnt or vol:mnt\n")
        os.Exit(1)
    }
    volMaps := make(map[string]string)
    volMaps[volItems[0]] = volItems[1]
    err = db.UpdateSchedulerVolumeMap(volMaps, nil)
    if err != nil {
        fmt.Printf("Fail to update scheduler volume map: %s\n",
            err.Error())
        os.Exit(1)
    }
}

func (cmd *configCommand) UnmapSchedulerVol(epoints string, vol string) {
    servers := ParseEndpoints(epoints)
    db := NewBioConfDB(servers)

    err := db.CompareAdminPassword()
    if err != nil {
        fmt.Printf("%s\n", err.Error())
        os.Exit(1)
    }

    vols := make([]string, 0)
    vols = append(vols, vol)
    err = db.UpdateSchedulerVolumeMap(nil, vols)
    if err != nil {
        fmt.Printf("Fail to delete %s from scheduler volume map: %s\n",
            vol, err.Error())
        os.Exit(1)
    }
}

func (cmd *configCommand) SetSchedulerAutoMount(epoints string, autoMount bool) {
    servers := ParseEndpoints(epoints)
    db := NewBioConfDB(servers)

    err := db.CompareAdminPassword()
    if err != nil {
        fmt.Printf("%s\n", err.Error())
        os.Exit(1)
    }

    err = db.SetSchedulerAutoMount(autoMount)
    if err != nil {
        fmt.Printf("Fail to set scheduler auto mount: %s\n",
            err.Error())
        os.Exit(1)
    }
}

func (cmd *configCommand) ShowAllInstanceState(epoints string, queue string) {
    servers := ParseEndpoints(epoints)
    db := NewInstanceDB(servers)

    err := db.CompareAdminPassword()
    if err != nil {
        fmt.Printf("%s\n", err.Error())
        os.Exit(1)
    }

    queueNum, err := strconv.Atoi(queue)
    if err != nil {
        fmt.Printf("The queue %s should be a number: %s\n",
            queue, err.Error())
        os.Exit(1)
    }

    err, instances := db.GetAllInstanceState(queueNum)
    if err != nil {
        Logger.Printf("Can't get instance state from ETCD: %s\n",
            err.Error())
        os.Exit(1)
    }

    if len(instances) == 0 {
        Logger.Printf("No instances exist\n")
    }

    for i := 0; i < len(instances); i ++ {
        fmt.Printf("Instance %d: \n", i + 1)
        fmt.Printf("  Seq: %d\n", instances[i].Seq)
        fmt.Printf("  Version: %s\n", instances[i].Version)
        fmt.Printf("  State: %s\n", 
            BioflowStateToStr(instances[i].State))
    }
}

func (cmd *configCommand) DeleteInstanceState(epoints string, queue string,
    seq string) {
    servers := ParseEndpoints(epoints)
    db := NewInstanceDB(servers)

    err := db.CompareAdminPassword()
    if err != nil {
        fmt.Printf("%s\n", err.Error())
        os.Exit(1)
    }

    queueNum, err := strconv.Atoi(queue)
    if err != nil {
        fmt.Printf("The queue %s should be a number: %s\n",
            queue, err.Error())
        os.Exit(1)
    }

    seqNum, err := strconv.ParseUint(seq, 10, 64)
    if err != nil {
        fmt.Printf("The seq %s should be a number: %s\n",
            seq, err.Error())
        os.Exit(1)
    }

    err = db.DeleteInstanceState(queueNum, seqNum)
    if err != nil {
        Logger.Printf("Can't delete instance state %s/%s from ETCD: %s\n",
            queue, seq, err.Error())
        os.Exit(1)
    }

}

func (cmd *configCommand) ShowAllClusterMountConfig(epoints string) {
    servers := ParseEndpoints(epoints)
    db := NewInstanceDB(servers)

    err := db.CompareAdminPassword()
    if err != nil {
        fmt.Printf("%s\n", err.Error())
        os.Exit(1)
    }

    err, clusters := db.LoadStorageClusterConfig()
    if err != nil {
        fmt.Printf("Can't get storage cluster config from ETCD: %s\n",
            err.Error())
        os.Exit(1)
    }

    if clusters == nil || len(clusters) == 0 {
        fmt.Printf("No storage cluster config\n")
        os.Exit(0)
    }

    for i := 0; i < len(clusters); i ++ {
        if clusters[i].Volume == "" {
            fmt.Printf("Cluster %s: \n", clusters[i].ClusterName)
        } else {
            fmt.Printf("Cluster %s Volume %s: \n",
                clusters[i].ClusterName, clusters[i].Volume)
        }
        fmt.Printf("  FSType: %s\n", clusters[i].FSType)
        fmt.Printf("  Servers:  ")
        for j := 0; j < len(clusters[i].Servers); j ++ {
            fmt.Printf(" %s ", clusters[i].Servers[j])
        }
        fmt.Printf("\n")
        fmt.Printf("  Options: %s\n", clusters[i].Options)
        fmt.Printf("  MountPath: %s\n", clusters[i].MountPath)
    }
}

func (cmd *configCommand) AddStorageClusterConfig(epoints string, name string,
    fstype string, servers string, opts string, mntPath string, vol string) {
    etcdServers := ParseEndpoints(epoints)
    db := NewInstanceDB(etcdServers)

    err := db.CompareAdminPassword()
    if err != nil {
        fmt.Printf("%s\n", err.Error())
        os.Exit(1)
    }

    if name == "" {
        fmt.Printf("The cluster name is required\n")
        os.Exit(-1)
    }

    config := &xconfig.StorageClusterMountConfig{
        ClusterName: name,
        Options: opts,
        MountPath: mntPath,
        Volume: vol,
    }

    /*check fstype: nfs, gluster, ceph*/
    switch fstype {
        case "nfs":
            config.FSType = xconfig.FS_NFS
        case "gluster":
            config.FSType = xconfig.FS_GLUSTER
        case "ceph":
            config.FSType = xconfig.FS_CEPH
        case "alamo":
            config.FSType = xconfig.FS_ALAMO
        case "anna":
            config.FSType = xconfig.FS_ANNA
        case "hdfs":
            config.FSType = xconfig.FS_HDFS
        case "oss-tencent":
            config.FSType = xconfig.OSS_TENCENT
        case "oss-alibaba":
            config.FSType = xconfig.OSS_ALIBABA
        case "oss-rgw":
            config.FSType = xconfig.OSS_RGW
        default:
            fmt.Printf("Only support nfs|alamo|anna|ceph|gluster|hdfs, %s not supported\n",
                fstype)
            os.Exit(-1)
    }

    if servers == "" {
        fmt.Printf("The server list is required\n")
        os.Exit(-1)
    }
    serverList := strings.Split(servers, ",")
    config.Servers = serverList

    err = db.SaveStorageClusterConfig(config)
    if err != nil {
        fmt.Printf("Fail to save storage cluster config: %s\n",
            err.Error())
        os.Exit(-1)
    }
}

func (cmd *configCommand) DeleteStorageClusterConfig(epoints string, name string, vol string) {
    etcdServers := ParseEndpoints(epoints)
    db := NewInstanceDB(etcdServers)

    err := db.CompareAdminPassword()
    if err != nil {
        fmt.Printf("%s\n", err.Error())
        os.Exit(1)
    }

    if name == "" {
        fmt.Printf("The cluster name is required\n")
        os.Exit(-1)
    }

    err = db.DeleteStorageClusterConfig(name, vol)
    if err != nil {
        fmt.Printf("Fail to delete storage cluster %s volume %s config: %s\n",
            name, vol, err.Error())
        os.Exit(-1)
    }
}

func (cmd *configCommand) SetSecurityConfig(epoints string,
    param string, value string) {
    servers := ParseEndpoints(epoints)
    db := NewBioConfDB(servers)

    err := db.CompareAdminPassword()
    if err != nil {
        fmt.Printf("%s\n", err.Error())
        os.Exit(1)
    }

    secConfig := &SecurityConfig {}
    err = db.LoadSecurityConfig(secConfig)
    if err != nil {
        Logger.Errorf("Fail to load etcd config, ignore: %s\n",
            err.Error())
    }

    switch strings.ToUpper(param) {
        case "SECMODE":
            if value != SEC_MODE_ISOLATION && 
                value != SEC_MODE_STRICT {
                fmt.Printf("The valid security mode should be isolation|strict \n")
                os.Exit(-1)
            }
            secConfig.SecMode = value
        case "MAXTASKNUM":
            val, err := strconv.ParseInt(value, 10, 64)
            if err != nil {
                fmt.Printf("Value %s for %s not valid\n",
                    value, param)
                os.Exit(-1)
            }
            secConfig.MaxTaskNum = val
        case "MAXJOBNUM":
            val, err := strconv.ParseInt(value, 10, 64)
            if err != nil {
                fmt.Printf("Value %s for %s not valid\n",
                    value, param)
                os.Exit(-1)
            }
            secConfig.MaxJobNum = val
        case "LDAPSERVER":
            secConfig.LDAPServer = value
        case "USELDAP":
            if strings.ToUpper(value) == "ENABLE" {
                secConfig.UseLDAP = true
            } else if strings.ToUpper(value) == "DISABLE" {
                secConfig.UseLDAP = false
            } else {
                fmt.Printf("Value %s for %s not valid, should be enable|disable\n",
                    value, param)
                os.Exit(-1)
            }
        default:
            fmt.Printf("Only support param: secmode, maxtasknum, maxjobnum, ldapserver, useldap\n")
            os.Exit(-1)
    }

    err = db.SaveSecurityConfig(secConfig)
    if err != nil {
        fmt.Printf("Fail to save security mode: %s\n",
            err.Error())
        os.Exit(-1)
    }
}

func (cmd *configCommand) SetSchedulerConfig(epoints string,
    param string, value string) {
    servers := ParseEndpoints(epoints)
    db := NewSchedulerDB(servers)

    err := db.CompareAdminPassword()
    if err != nil {
        fmt.Printf("%s\n", err.Error())
        os.Exit(1)
    }

    /*load old config from etcd*/
    schedulerConfig := &JobSchedulerConfig{}
    err = db.LoadJobSchedulerConfig(schedulerConfig)
    if err != nil {
        Logger.Errorf("Fail to load old config, ignore: %s\n",
            err.Error())
    }
    switch strings.ToUpper(param) {
        case "JOBSCHEDULELIMIT":
            val, err := strconv.Atoi(value)
            if err != nil {
                fmt.Printf("Value %s for %s not valid\n",
                    value, param)
                os.Exit(-1)
            }
            schedulerConfig.MaxJobsSchedulePerRound = val
        case "THROATEWATERMARK":
            val, err := strconv.ParseFloat(value, 64)
            if err != nil {
                fmt.Printf("Value %s for %s not valid\n",
                    value, param)
                os.Exit(-1)
            }
            schedulerConfig.QueueThroateThreshold = val
        case "DELAYWATERMARK":
            val, err := strconv.ParseFloat(value, 64)
            if err != nil {
                fmt.Printf("Value %s for %s not valid\n",
                    value, param)
                os.Exit(-1)
            }
            schedulerConfig.QueueDelayThreshold = val
        case "PRITHROATEFACTOR":
            val, err := strconv.ParseFloat(value, 64)
            if err != nil {
                fmt.Printf("Value %s for %s not valid\n",
                    value, param)
                os.Exit(-1)
            }
            schedulerConfig.PriThroateFactor = val
        case "MAXALLOWDELAYINTERVAL":
            val, err := strconv.ParseFloat(value, 64)
            if err != nil {
                fmt.Printf("Value %s for %s not valid\n",
                    value, param)
                os.Exit(-1)
            }
            schedulerConfig.MaxAllowDelayInterval = val
        case "DELAYCHECKTIMEOUT":
            val, err := strconv.Atoi(value)
            if err != nil {
                fmt.Printf("Value %s for %s not valid\n",
                    value, param)
                os.Exit(-1)
            }
            schedulerConfig.JobDelayCheckTimeout = val
        case "SCHEDULEPERIOD":
            val, err := strconv.Atoi(value)
            if err != nil {
                fmt.Printf("Value %s for %s not valid\n",
                    value, param)
                os.Exit(-1)
            }
            schedulerConfig.ScheduleCheckPeriod = val
        case "JOBCHECKPERIOD":
            val, err := strconv.Atoi(value)
            if err != nil {
                fmt.Printf("Value %s for %s not valid\n",
                    value, param)
                os.Exit(-1)
            }
            schedulerConfig.JobCheckInterval = val
        case "MAXJOBS":
            val, err := strconv.ParseInt(value, 10, 64)
            if err != nil {
                fmt.Printf("Value %s for %s not valid\n",
                    value, param)
                os.Exit(-1)
            }
            schedulerConfig.MaxJobsAllowed = val
        case "MAXTASKS":
            val, err := strconv.ParseInt(value, 10, 64)
            if err != nil {
                fmt.Printf("Value %s for %s not valid\n",
                    value, param)
                os.Exit(-1)
            }
            schedulerConfig.MaxTasksAllowed = val
        case "THROATETASKNUM":
            val, err := strconv.ParseInt(value, 10, 64)
            if err != nil {
                fmt.Printf("Value %s for %s not valid\n",
                    value, param)
                os.Exit(-1)
            }
            schedulerConfig.ThroateTaskNum = val
        case "DELAYTASKNUM":
            val, err := strconv.ParseInt(value, 10, 64)
            if err != nil {
                fmt.Printf("Value %s for %s not valid\n",
                    value, param)
                os.Exit(-1)
            }
            schedulerConfig.DelayTaskNum = val
        case "AUTORESCHEDULEHANGTASK":
            if strings.ToUpper(value) == "ENABLE" {
                schedulerConfig.AutoReScheduleHangTasks = true
            } else if strings.ToUpper(value) == "DISABLE" {
                schedulerConfig.AutoReScheduleHangTasks = false
            } else {
                fmt.Printf("Value %s for %s not valid, should be enable|disable\n",
                    value, param)
                os.Exit(-1)
            }
        case "DISABLEOOMCHECK":
            if strings.ToUpper(value) == "ENABLE" {
                schedulerConfig.DisableOOMKill = true
            } else if strings.ToUpper(value) == "DISABLE" {
                schedulerConfig.DisableOOMKill = false
            } else {
                fmt.Printf("Value %s for %s not valid, should be enable|disable\n",
                    value, param)
                os.Exit(-1)
            }
        case "SOFTLIMITRATIO":
            val, err := strconv.ParseFloat(value, 64)
            if err != nil {
                fmt.Printf("Value %s for %s not valid\n",
                    value, param)
                os.Exit(-1)
            }
            schedulerConfig.SoftLimitRatio = val
        case "AUTOSCHEDULEHDFS":
            if strings.ToUpper(value) == "ENABLE" {
                schedulerConfig.AutoScheduleHDFS = true
            } else if strings.ToUpper(value) == "DISABLE" {
                schedulerConfig.AutoScheduleHDFS = false
            } else {
                fmt.Printf("Value %s for %s not valid, should be enable|disable\n",
                    value, param)
                os.Exit(-1)
            }
        case "QOSENABLED":
            if strings.ToUpper(value) == "ENABLE" {
                schedulerConfig.QosEnabled = true
            } else if strings.ToUpper(value) == "DISABLE" {
                schedulerConfig.QosEnabled = false
            } else {
                fmt.Printf("Value %s for %s not valid, should be enable|disable\n",
                    value, param)
                os.Exit(-1)
            }
        case "PROFILERENABLED":
            if strings.ToUpper(value) == "ENABLE" {
                schedulerConfig.ProfilerEnabled = true
            } else if strings.ToUpper(value) == "DISABLE" {
                schedulerConfig.ProfilerEnabled = false
            } else {
                fmt.Printf("Value %s for %s not valid, should be enable|disable\n",
                    value, param)
                os.Exit(-1)
            }
        case "ENABLESTORAGECONSTRAINT":
            if strings.ToUpper(value) == "ENABLE" {
                schedulerConfig.EnableStorageConstraint = true
            } else if strings.ToUpper(value) == "DISABLE" {
                schedulerConfig.EnableStorageConstraint = false
            } else {
                fmt.Printf("Value %s for %s not valid, should be enable|disable\n",
                    value, param)
                os.Exit(-1)
            }
        case "ENFORCESERVERTYPECONSTRAINT":
            if strings.ToUpper(value) == "ENABLE" {
                schedulerConfig.EnforceServerTypeConstraint = true
            } else if strings.ToUpper(value) == "DISABLE" {
                schedulerConfig.EnforceServerTypeConstraint = false
            } else {
                fmt.Printf("Value %s for %s not valid, should be enable|disable\n",
                    value, param)
                os.Exit(-1)
            }
        case "CPUCAPRATIO":
            val, err := strconv.ParseFloat(value, 64)
            if err != nil {
                fmt.Printf("Value %s for %s not valid\n",
                    value, param)
                os.Exit(-1)
            }
            schedulerConfig.CPUCapRatio = val
        case "MEMCAPRATIO":
            val, err := strconv.ParseFloat(value, 64)
            if err != nil {
                fmt.Printf("Value %s for %s not valid\n",
                    value, param)
                os.Exit(-1)
            }
            schedulerConfig.MemCapRatio = val
        case "USESTAGEWORKDIR":
            if strings.ToUpper(value) == "ENABLE" {
                schedulerConfig.SetStageWorkdirToContainerWorkdir = true
            } else if strings.ToUpper(value) == "DISABLE"{
                schedulerConfig.SetStageWorkdirToContainerWorkdir = false
            } else {
                fmt.Printf("Value %s for %s not valid, should be enable|disable\n",
                    value, param)
                os.Exit(-1)
            }
        case "USETASKOWNER":
            if strings.ToUpper(value) == "ENABLE" {
                schedulerConfig.UseTaskOwner = true
            } else if strings.ToUpper(value) == "DISABLE"{
                schedulerConfig.UseTaskOwner = false
            } else {
                fmt.Printf("Value %s for %s not valid, should be enable|disable\n",
                    value, param)
                os.Exit(-1)
            }
        case "ENABLERIGHTCONTROL":
            if strings.ToUpper(value) == "ENABLE" {
                schedulerConfig.EnableRightControl = true
            } else if strings.ToUpper(value) == "DISABLE"{
                schedulerConfig.EnableRightControl = false
            } else {
                fmt.Printf("Value %s for %s not valid, should be enable|disable\n",
                    value, param)
                os.Exit(-1)
            }
        case "CLEANUPPATTERN":
            if strings.ToUpper(value) == "ENABLE" {
                schedulerConfig.CleanupPatternEnable = true
            } else if strings.ToUpper(value) == "DISABLE"{
                schedulerConfig.CleanupPatternEnable = false
            } else {
                fmt.Printf("Value %s for %s not valid, should be enable|disable\n",
                    value, param)
                os.Exit(-1)
            }
        case "ENABLESTAGEWORKER":
            if strings.ToUpper(value) == "ENABLE" {
                schedulerConfig.EnableStageWorker = true
            } else if strings.ToUpper(value) == "DISABLE"{
                schedulerConfig.EnableStageWorker = false
            } else {
                fmt.Printf("Value %s for %s not valid, should be enable|disable\n",
                    value, param)
                os.Exit(-1)
            }
        case "ASYNCTRACKTASKINDB":
            if strings.ToUpper(value) == "ENABLE" {
                schedulerConfig.AsyncTrackTaskInDB = true
            } else if strings.ToUpper(value) == "DISABLE"{
                schedulerConfig.AsyncTrackTaskInDB = false
            } else {
                fmt.Printf("Value %s for %s not valid, should be enable|disable\n",
                    value, param)
                os.Exit(-1)
            }
        case "MAXCPUPERTASK":
            val, err := strconv.ParseFloat(value, 64)
            if err != nil {
                fmt.Printf("Value %s for %s not valid\n",
                    value, param)
                os.Exit(-1)
            }
            schedulerConfig.MaxCPUPerTask = val
        case "MAXMEMORYPERTASK":
            val, err := strconv.ParseFloat(value, 64)
            if err != nil {
                fmt.Printf("Value %s for %s not valid\n",
                    value, param)
                os.Exit(-1)
            }
            schedulerConfig.MaxMemoryPerTask = val
        case "JOBSCHEDULEWORKERCOUNT":
            val, err := strconv.ParseInt(value, 10, 64)
            if err != nil {
                fmt.Printf("Value %s for %s not valid\n",
                    value, param)
                os.Exit(-1)
            }
            schedulerConfig.JobScheduleWorkerCount = val
        case "STAGEQUOTA":
            rErr := fmt.Errorf("invalid argument")
            args := strings.Split(value, ";")
            if schedulerConfig.StageQuota == nil {
                schedulerConfig.StageQuota = make(map[int]int)
            }
            for _, arg := range args {
                pair := strings.Split(arg, ":")
                if len(pair) != 2 {
                    fmt.Println(rErr.Error())
                    os.Exit(-1)
                }
                priority, err := strconv.Atoi(pair[0])
                quota, err1 := strconv.Atoi(pair[1])
                if err != nil ||
                    err1 != nil ||
                    priority < message.JOB_MIN_PRI ||
                    priority > message.JOB_MAX_PRI ||
                    quota < -1{
                    fmt.Println(rErr.Error())
                    os.Exit(-1)
                }
                schedulerConfig.StageQuota[priority] = quota
            }
            case "RETRYDELAY":
                rErr := fmt.Errorf("invalid argument")
                delay, err := strconv.Atoi(value)
                if err != nil {
                    fmt.Println(rErr.Error())
                    os.Exit(-1)
                }
                schedulerConfig.StageRetryDelay = delay
       default:
            fmt.Printf("Unknown param %s \n", param)
            fmt.Printf("Only support: jobschedulelimit,throatewatermark, delaywatermark,")
            fmt.Printf("prithroatefactor,delaychecktimeout,scheduleperiod,jobcheckperiod,")
            fmt.Printf("maxjobs,maxtasks,throatetasknum,delaytasknum,maxallowdelayinterval,")
            fmt.Printf("autoreschedulehangtask,disableoomcheck,softlimitratio,autoschedulehdfs,\n")
            fmt.Printf("qosenabled,cpucapratio,memcapratio,enablestorageconstraint,profilerenabled,\n")
            fmt.Printf("cleanuppattern,usestageworkdir,maxcpupertask,maxmemorypertask,enablestageworker,\n")
            fmt.Printf("EnforceServerTypeConstraint,UseTaskOwner,EnableRightControl,JobScheduleWorkerCount,\n")
            fmt.Printf("AsyncTrackTaskInDB\n")
            os.Exit(-1)
    }
    err = db.SaveJobSchedulerConfig(schedulerConfig)
    if err != nil {
        fmt.Printf("Fail to save job scheduler config\n")
        os.Exit(-1)
    }
}

func ShowSchedulerConfig(config *JobSchedulerConfig) {
    fmt.Printf("Job Scheduler Configuration:\n")
    fmt.Printf("  ThroateWaterMark: %f\n", config.QueueThroateThreshold)
    fmt.Printf("  DelayWaterMark: %f\n", config.QueueDelayThreshold)
    fmt.Printf("  ThroateTaskNum: %d\n", config.ThroateTaskNum)
    fmt.Printf("  DelayTaskNum: %d\n", config.DelayTaskNum)
    fmt.Printf("  PriThroateFactor: %f\n", config.PriThroateFactor)
    fmt.Printf("  JobScheduleLimit: %d\n", config.MaxJobsSchedulePerRound)
    fmt.Printf("  DelayCheckTimeout: %d\n", config.JobDelayCheckTimeout)
    fmt.Printf("  JobCheckPeriod: %d\n", config.JobCheckInterval)
    fmt.Printf("  SchedulePeriod: %d\n", config.ScheduleCheckPeriod)
    fmt.Printf("  MaxJobs: %d\n", config.MaxJobsAllowed)
    fmt.Printf("  MaxTasks: %d\n", config.MaxTasksAllowed)
    fmt.Printf("  MaxAllowDelayInterval: %f\n", config.MaxAllowDelayInterval)
    fmt.Printf("  AutoReScheduleHangTask: %v\n", config.AutoReScheduleHangTasks)
    fmt.Printf("  DisableOOMCheck: %v\n", config.DisableOOMKill)
    fmt.Printf("  SoftLimitRatio: %f\n", config.SoftLimitRatio)
    fmt.Printf("  AutoScheduleHDFS: %v\n", config.AutoScheduleHDFS)
    fmt.Printf("  QosEnabled: %v\n", config.QosEnabled)
    fmt.Printf("  ProfilerEnabled: %v\n", config.ProfilerEnabled)
    fmt.Printf("  CPUCapRatio: %f\n", config.CPUCapRatio)
    fmt.Printf("  MemCapRatio: %f\n", config.MemCapRatio)
    fmt.Printf("  UseStageWorkdir: %v\n", config.SetStageWorkdirToContainerWorkdir)
    fmt.Printf("  EnableStorageConstraint: %v\n", config.EnableStorageConstraint)
    fmt.Printf("  EnforceServerTypeConstraint: %v\n", config.EnforceServerTypeConstraint)
    fmt.Printf("  MaxCPUPerTask: %f\n", config.MaxCPUPerTask)
    fmt.Printf("  MaxMemoryPerTask: %f\n", config.MaxMemoryPerTask)
    fmt.Printf("  UseTaskOwner: %v\n", config.UseTaskOwner)
    fmt.Printf("  EnableRightControl: %v\n", config.EnableRightControl)
    fmt.Printf("  EnableStageWorker: %v\n", config.EnableStageWorker)
    fmt.Printf("  JobScheduleWorkerCount: %d\n", config.JobScheduleWorkerCount)
    fmt.Printf("  CleanupPattern: %v\n", config.CleanupPatternEnable)
    fmt.Printf("  StageQuota:\n")
    for pri, quota := range config.StageQuota {
        fmt.Printf("    Priority %d: %d\n", pri, quota)
    }
    fmt.Printf("  AsyncTrackTaskInDB: %v\n", config.AsyncTrackTaskInDB)
}

func (cmd *configCommand)ShowSchedulerConfig(epoints string) {
    servers := ParseEndpoints(epoints)
    db := NewSchedulerDB(servers)

    err := db.CompareAdminPassword()
    if err != nil {
        fmt.Printf("%s\n", err.Error())
        os.Exit(1)
    }

    /*load old config from etcd*/
    schedulerConfig := &JobSchedulerConfig{}
    err = db.LoadJobSchedulerConfig(schedulerConfig)
    if err != nil {
        fmt.Printf("Fail to load old config, ignore: %s\n",
            err.Error())
        os.Exit(-1)
    }

    ShowSchedulerConfig(schedulerConfig) 
}

func (cmd *configCommand)SetLogLevel(epoints string, level int, queue int) {
    servers := ParseEndpoints(epoints)
    db := NewBioConfDB(servers)

    err := db.CompareAdminPassword()
    if err != nil {
        fmt.Printf("%s\n", err.Error())
        os.Exit(1)
    }

    config := &GlobalConfig {}
    queueConfig := &BioflowQueueConfig{}
    err = db.LoadGlobalConfig(config)
    if err != nil {
        Logger.Errorf("Fail to load global config, ignore: %s\n",
            err.Error())
    }

    if level < 0 || level > 3 {
        fmt.Printf("The level should be: 0(info), 1(debug), 2(warn), 3(error)\n")
        os.Exit(-1)
    }
    if queue < 0 || queue > 20 {
        fmt.Printf("Queue: %d, current global log level %d, set to %d \n",
            queue, config.LogLevel, level)
        config.LogLevel = level
        err = db.SaveGlobalConfig(config)
        if err != nil {
            fmt.Printf("Fail to save global config to etcd: %s\n",
                err.Error())
            os.Exit(-1)
        }
    } else {
        fmt.Printf("Current queue: %d log level %d, set to %d \n",
            queue, config.LogLevel, level)
        err = db.LoadQueueConfig(queue, queueConfig)
        if err != nil {
            Logger.Errorf("Fail to load queue loglevel config, ignore: %s\n",
                err.Error())
        }
        queueConfig.LoglevelConfig.LogLevel = level
        err = db.SaveQueueConfig(queue, queueConfig)
        if err != nil {
            fmt.Printf("Fail to save global config to etcd: %s\n",
                err.Error())
            os.Exit(-1)
        }
    }
}

func (cmd *configCommand)SetMail(epoints, user, password, host string){
    servers := ParseEndpoints(epoints)
    db := NewBioConfDB(servers)

    err := db.CompareAdminPassword()
    if err != nil {
        fmt.Printf("%s\n", err.Error())
        os.Exit(1)
    }

    config := &MailConfig{
        User: user,
        Pass: password,
        Host: host,
    }

    err = db.AddMailConfig(config)
    if err != nil {
        fmt.Printf("Fail to add e-mail account %s\n", err.Error())
        os.Exit(1)
    } else {
        fmt.Printf("Succeed to add e-mail account\n")
    }
}

func (cmd *configCommand)SetFileDownloadConf(epoints string, cpu, mem float64){
    servers := ParseEndpoints(epoints)
    db := NewBioConfDB(servers)

    err := db.CompareAdminPassword()
    if err != nil {
        fmt.Printf("%s\n", err.Error())
        os.Exit(1)
    }

    config := &FileDownloadConfig{
        Cpu: cpu,
        Mem: mem,
    }

    err = db.AddFileDownloadConfig(config)
    if err != nil {
        fmt.Printf("Fail to add file download config %s\n", err.Error())
        os.Exit(1)
    } else {
        fmt.Printf("Succeed to add file download config\n")
    }
}

func (cmd *configCommand)SetPipelineStore(epoints, baseDir string){
    servers := ParseEndpoints(epoints)
    db := NewBioConfDB(servers)

    err := db.CompareAdminPassword()
    if err != nil {
        fmt.Printf("%s\n", err.Error())
        os.Exit(1)
    }

    config := &PipelineStoreConfig{
        BaseDir: baseDir,
    }

    err = db.SetPipelineStoreConfig(config)
    if err != nil {
        fmt.Printf("Fail to set pipeline store config %s\n", err.Error())
        os.Exit(1)
    } else {
        fmt.Printf("Succeed to set pipeline store config\n")
    }
}

func (cmd *configCommand)SetFrontEnd(epoints, opt, url string, queue int) {
    servers := ParseEndpoints(epoints)
    db := NewBioConfDB(servers)

    err := db.CompareAdminPassword()
    if err != nil {
        fmt.Printf("%s\n", err.Error())
        os.Exit(1)
    }

    config := &FrontEndConfig{
        Url: url,
        Queue: queue,
    }

    switch opt{
    case "add":
        err = db.AddSingleFrontEnd(config, queue)
        if err != nil {
            fmt.Printf("Fail to add front-end %s\n", err.Error())
            os.Exit(1)
        } else {
            fmt.Printf("Succeed to add front-end\n")
        }
    case "del":
        err = db.DeleteSingleFrontEnd(config, queue)
        if err != nil {
            fmt.Printf("Fail to delete front-end %s\n", err.Error())
            os.Exit(1)
        } else {
            fmt.Printf("Succeed to delete front-end\n")
        }
    default:
        fmt.Printf("Argument 'opt' must be 'add' or 'del'\n")
        os.Exit(1)
    }

}

func (cmd *configCommand)SetContainerDataDir(epoints string, datadir string) {
    servers := ParseEndpoints(epoints)
    db := NewBioConfDB(servers)

    err := db.CompareAdminPassword()
    if err != nil {
        fmt.Printf("%s\n", err.Error())
        os.Exit(1)
    }

    storageMtrConfig := xconfig.StoreMgrConfig{}
    err = db.LoadStorageMgrConfig(&storageMtrConfig)
    if err != nil {
        fmt.Printf("Can't load store config: %s\n", err.Error())
        os.Exit(1)
    }

    storageMtrConfig.ContainerConfig.ContainerDataDir = datadir

    err = db.SaveStorageMgrConfig(&storageMtrConfig)
    if err != nil {
        ConfLogger.Errorf("Can't save store config to ETCD: %s\n",
            err.Error())
        os.Exit(1)
    } else {
        fmt.Printf("Succeed to set store config\n")
    }
}

func (cmd *configCommand)SetPhysicalSchedulerConfig(epoints string, param, value string) {
    servers := ParseEndpoints(epoints)
    db := NewBioConfDB(servers)

    err := db.CompareAdminPassword()
    if err != nil {
        fmt.Printf("%s\n", err.Error())
        os.Exit(1)
    }

    config := PhysicalConfig{}
    err = db.LoadPhysicalConfig(&config)
    if err != nil {
        fmt.Printf("Can't load physical scheduler config: %s\n", err.Error())
        os.Exit(1)
    }

    switch strings.ToUpper(param) {
        case "SCHEDENABLED":
            if strings.ToUpper(value) == "ENABLE" {
                config.SchedEnabled = true
            } else if strings.ToUpper(value) == "DISABLE" {
                config.SchedEnabled = false
            } else {
                fmt.Printf("Value %s for %s not valid, should be enable|disable\n",
                    value, param)
                os.Exit(-1)
            }
        case "ENDPOINT":
            config.EndPoint = value
        case "NFSSERVER":
            config.NFSServer = value
        case "THROATEPERIOD":
            val, err := strconv.Atoi(value)
            if err != nil {
                fmt.Printf("Value %s for %s not valid\n",
                    value, param)
                os.Exit(-1)
            }
            config.ThroatePeriod = val
        case "THROATEBATCHCOUNT":
            val, err := strconv.Atoi(value)
            if err != nil {
                fmt.Printf("Value %s for %s not valid\n",
                    value, param)
                os.Exit(-1)
            }
            config.ThroateBatchCount = val
        case "DELAYPERIOD":
            val, err := strconv.Atoi(value)
            if err != nil {
                fmt.Printf("Value %s for %s not valid\n",
                    value, param)
                os.Exit(-1)
            }
            config.DelayPeriod = val
        case "DELAYBATCHCOUNT":
            val, err := strconv.Atoi(value)
            if err != nil {
                fmt.Printf("Value %s for %s not valid\n",
                    value, param)
                os.Exit(-1)
            }
            config.DelayBatchCount = val
        case "THROATEINSTANCESLIMIT":
            val, err := strconv.Atoi(value)
            if err != nil {
                fmt.Printf("Value %s for %s not valid\n",
                    value, param)
                os.Exit(-1)
            }
            config.ThroateInstancesLimit = val
        case "DELAYINSTANCESLIMIT":
            val, err := strconv.Atoi(value)
            if err != nil {
                fmt.Printf("Value %s for %s not valid\n",
                    value, param)
                os.Exit(-1)
            }
            config.DelayInstancesLimit = val
        case "MASTERINSTANCES":
            val, err := strconv.Atoi(value)
            if err != nil {
                fmt.Printf("Value %s for %s not valid\n",
                    value, param)
                os.Exit(-1)
            }
            config.MasterInstances = val
        case "INSTANCETYPE":
            if strings.ToUpper(value) == "FAT" {
                config.InstanceType = value
            } else if strings.ToUpper(value) == "SLIM" {
                config.InstanceType = value
            } else if strings.ToUpper(value) == "NORMAL" {
                config.InstanceType = value
            } else {
                fmt.Printf("Value %s for %s not valid, should be fat|slim|normal\n",
                    value, param)
                os.Exit(-1)
            }
        case "DELETEINSTANCESINTERVAL":
            val, err := strconv.Atoi(value)
            if err != nil {
                fmt.Printf("Value %s for %s not valid\n",
                    value, param)
                os.Exit(-1)
            }
            config.DeleteInstancesInterval = val
        default:
            fmt.Printf("Unknown param %s \n", param)
            fmt.Printf("Only support: schedenabled, endpoint, nfsserver\n")
            fmt.Printf("throateperiod, throatebatchcount, delayperiod, delaybatchcount\n")
            fmt.Printf("throateinstanceslimit, delayinstanceslimit, masterinstances\n")
            fmt.Printf("instancetype, deleteInstancesInterval\n")
            os.Exit(-1)
    }

    err = db.SavePhysicalConfig(&config)
    if err != nil {
        fmt.Printf("Fail to set physical scheduler config %s\n", err.Error())
        os.Exit(1)
    } else {
        fmt.Printf("Succeed to set physical scheduler config\n")
    }
}

