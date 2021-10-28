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

package bioflow

import (
    "fmt"
    "errors"
    "time"
    "github.com/xtao/bioflow/scheduler"
    "github.com/xtao/bioflow/server"
    "github.com/xtao/bioflow/confbase"
    "github.com/xtao/bioflow/dbservice"
    "github.com/xtao/bioflow/storage"
    "github.com/xtao/bioflow/cluster"
    . "github.com/xtao/bioflow/common"

    xstorage "github.com/xtao/xstone/storage"
    xconfig "github.com/xtao/xstone/config"
    "github.com/xtao/bioflow/eventbus"
    "github.com/xtao/dsl-go/engine"
    "github.com/xtao/bioflow/scheduler/physicalscheduler"
    "github.com/xtao/bioflow/scheduler/dataprovider"
)

const (
    /*
     * start as emulation mode:
     * 1) load local config file
     * 2) run simulator scheduler backend 
     */
    BIOFLOW_MODE_EMULATION string = "emulation"
    /*
     * start as docker mode:
     * 1) load config from etcd
     * 2) run ermetic or kubernettes scheduler backend 
     */
    BIOFLOW_MODE_DOCKER string = "docker"
    /*
     * start as local mode:
     * 1) load config from local config file
     * 2) run ermetic or kubernettes scheduler backend 
     */
    BIOFLOW_MODE_LOCAL string = "local"
)

const (
    BIOFLOW_WATCH_RETRY_INTERVAL time.Duration = 30 * time.Second
)

func BioflowCreateBackends(bioflowConfig *BioflowConfig) error {
    if bioflowConfig.BackendConfig == nil {
        Logger.Errorf("Fail to obtain backend config\n")
        return errors.New("No Valid backend config")
    }

    backendMgr := scheduler.GetBackendMgr()

    backendConfig := bioflowConfig.BackendConfig
    for i := 0; i < len(backendConfig); i ++ {
        config := backendConfig[i]
        backendMgr.AddBackend(config.Type, config.Server, config.Port)
    }

    return nil
}

func BioflowCreateSimulator() {
    backendMgr := scheduler.GetBackendMgr()
    backendMgr.CreateSimBackend()
}

func BioflowStartWatchers(db *confbase.BioConfDB, logFileName string, queue int) error {
    storeMgrWatcher := db.NewStoreMgrConfigWatcher()
    if storeMgrWatcher == nil {
        Logger.Errorf("Can't create store mgr config watcher\n")
        return errors.New("Fail create store mgr config watcher")
    }
    storeMgr := storage.GetStorageMgr()

    go func() {
        for {
            err, serverConfig, schedulerConfig, containerConfig :=
                storeMgrWatcher.WatchChanges()
            if err != nil {
                Logger.Errorf("store mgr watch error: %s\n",
                    err.Error())
                time.Sleep(BIOFLOW_WATCH_RETRY_INTERVAL)
                continue
            }
            if serverConfig != nil {
                storeMgr.UpdateServerVolConfig(serverConfig)
            }
            if schedulerConfig != nil {
                storeMgr.UpdateSchedulerVolConfig(schedulerConfig)
            }
            if containerConfig != nil {
                storeMgr.UpdateContainerVolConfig(containerConfig)
            }
        }
    } ()

    backendsWatcher := db.NewBackendConfigWatcher()
    backendMgr := scheduler.GetBackendMgr()

    go func() {
        for {
            err, op, config, id := backendsWatcher.WatchChanges()
            if err != nil {
                Logger.Errorf("store mgr watch error: %s\n",
                    err.Error())
                time.Sleep(BIOFLOW_WATCH_RETRY_INTERVAL)
                continue
            }

            switch op {
            case "add" :
                backendMgr.AddBackend(config.Type, config.Server, config.Port)
            case "delete":
                backendMgr.DeleteBackend(id)
            }
        }
    } ()

    mountConfigWatcher := db.NewStorageClusterConfigWatcher()
    mountMgr := xstorage.GetMountMgr()

    go func() {
        for {
            err, updated, removed := mountConfigWatcher.WatchChanges()
            if err != nil {
                Logger.Errorf("storage cluster config watch error: %s\n",
                    err.Error())
                time.Sleep(BIOFLOW_WATCH_RETRY_INTERVAL)
                continue
            }

            updateList := make([]xconfig.StorageClusterMountConfig, 0)
            if updated != nil {
                updateList = append(updateList, *updated)
            }
            removeList := make([]xconfig.StorageClusterMountConfig, 0)
            if removed != nil {
                removeList = append(removeList, *removed)
            }
            err = mountMgr.UpdateMounters(updateList, removeList)
            if err != nil {
                Logger.Errorf("Fail to update the mounters config: %s\n",
                    err.Error())
            }
        }
    }()


    securityConfigWatcher := db.NewSecurityConfigWatcher()
    userMgr := scheduler.GetUserMgr()
    go func() {
        for {
            err, config := securityConfigWatcher.WatchChanges()
            if err != nil {
                Logger.Errorf("security config watch error: %s\n",
                    err.Error())
                time.Sleep(BIOFLOW_WATCH_RETRY_INTERVAL)
                continue
            }
            
            err = userMgr.Configure(config, nil)
            if err != nil {
                Logger.Errorf("User mgr fail to update config: %s\n",
                    err.Error())
            }
        }
    }()

    ldapConfigWatcher := db.NewLDAPWatcher()
    if userMgr.UseLDAP() {
        go func() {
            for {
                err, changed := ldapConfigWatcher.WatchChanges()
                if err != nil {
                    Logger.Errorf("LDAP config watch error: %s\n",
                        err.Error())
                    time.Sleep(BIOFLOW_WATCH_RETRY_INTERVAL)
                    continue
                } 
                if changed {
                    err = userMgr.LoadExternalUsers(3)
                    if err != nil {
                        Logger.Errorf("User mgr fail to load users: %s\n",
                            err.Error())
                    }
                }
            }
        }()
    }

    schedulerConfigWatcher := db.NewJobSchedulerConfigWatcher()
    sche := scheduler.GetScheduler()
    go func() {
        for {
            err, config := schedulerConfigWatcher.WatchChanges()
            if err != nil {
                Logger.Errorf("scheduler config watch error: %s\n",
                    err.Error())
                time.Sleep(BIOFLOW_WATCH_RETRY_INTERVAL)
                continue
            }

            sche.UpdateConfig(config)
        }
    }()

    adminPasswordWatcher := db.NewAdminPasswordWatcher()
    go func() {
        for {
            err, pass := adminPasswordWatcher.WatchChanges()
            if err != nil {
                Logger.Errorf("admin password watch error: %s\n",
                    err.Error())
                time.Sleep(BIOFLOW_WATCH_RETRY_INTERVAL)
                continue
            }

            /*
             * decode admin pass and encrypt to adminskey
             */
            aesEnc := NewAesEncrypt(BIOFLOW_ADMIN_PASSWORD_SEED)
            p, err := aesEnc.Decrypt(pass)
            if err != nil {
                Logger.Errorf("Decrypt admin password error: %s\n",
                    err.Error())
                continue
            }

            aesEnc = NewAesEncrypt(BIOFLOW_SECURITY_KEY_SEED)
            skey, err := aesEnc.Encrypt(p)
            if err != nil {
                Logger.Errorf("Ecrypt admin password error: %s\n",
                    err.Error())
                continue
            }
            server.SetGlobalAdminSkey(skey)
        }
    }()

    globalConfigWatcher := db.NewGlobalConfigWatcher()
    go func() {
        for {
            err, config := globalConfigWatcher.WatchChanges()
            if err != nil {
                Logger.Errorf("global config watch error: %s\n",
                    err.Error())
                time.Sleep(BIOFLOW_WATCH_RETRY_INTERVAL)
                continue
            }
            if config != nil {
                /*re-init the logger to set log level*/
                Logger.Infof("Re-Init log to set log level to %d\n",
                    config.LogLevel)
                loggerConfig := LoggerConfig{
                        Logfile: logFileName,
                        LogLevel: config.LogLevel,
                }
                LoggerInit(&loggerConfig)
                
                Logger.Infof("Update global config: %v\n",
                    config)
            }
        }
    }()

    queueConfigWatcher := db.NewQueueConfigWatcher()
    go func() {
        for {
            envSetting := EnvUtilsParseEnvSetting()
            err, config := queueConfigWatcher.WatchChanges(envSetting.Queue)
            if err != nil {
                Logger.Errorf("global config watch error: %s\n",
                    err.Error())
                time.Sleep(BIOFLOW_WATCH_RETRY_INTERVAL)
                continue
            }
            if config != nil {
                if &config.LoglevelConfig != nil {
                    /*re-init the logger to set log level*/
                    Logger.Infof("Re-Init log to set log level to %d\n",
                        config.LoglevelConfig.LogLevel)
                    loggerConfig := LoggerConfig{
                        Logfile:  logFileName,
                        LogLevel: config.LoglevelConfig.LogLevel,
                    }
                    LoggerInit(&loggerConfig)
                }

                Logger.Infof("Update queue loglevel config: %v\n",
                    config)
            }
        }
    }()

    mailConfigWatcher := db.NewMailConfigWatcher()
    go func() {
        for {
            err, config := mailConfigWatcher.WatchChanges()
            if err != nil {
                Logger.Errorf("mail config watch error: %s\n",
                    err.Error())
                time.Sleep(BIOFLOW_WATCH_RETRY_INTERVAL)
                continue
            }
            if config == nil {
                continue
            }
            Logger.Infof("Update mail config: %v\n", config)
            eventbus.SetMailSender(config.User, config.Pass, config.Host)
        }
    }()

    fileDownloadConfigWatcher := db.NewFileDownloadConfigWatcher()
    go func() {
        for {
            err, config := fileDownloadConfigWatcher.WatchChanges()
            if err != nil {
                Logger.Errorf("file download config watch error: %s\n",
                    err.Error())
                time.Sleep(BIOFLOW_WATCH_RETRY_INTERVAL)
                continue
            }
            if config == nil {
                continue
            }
            Logger.Infof("Update file download config: %v\n", config)
            dataprovider.SetDownloadWorkResource(config.Cpu, config.Mem)
        }
    }()

    frontEndWathcher := db.NewFrontEndWatcher(queue)
    go func() {
        for {
            err, config, opt := frontEndWathcher.WatchChanges()
            if err != nil {
                Logger.Errorf("Front-end config watch error: %s\n",
                    err.Error())
                time.Sleep(BIOFLOW_WATCH_RETRY_INTERVAL)
                continue
            }
            if config == nil {
                continue
            }
            switch opt {
            case "ADD":
                Logger.Infof("Register new Front-end\n", config.Url)
                scheduler.GetRestApiMgr().Subscribe(config.Url)
            case "DEL":
                Logger.Infof("UnRegister Front-end %s\n", config.Url)
                scheduler.GetRestApiMgr().Unsubscribe(config.Url)
            }
        }
    }()

    pipelineConfigWatcher := db.NewPipelineStoreConfigWatcher()
    go func() {
        for {
            err, config := pipelineConfigWatcher.WatchChanges()
            if err != nil {
                Logger.Errorf("pipeline store config watch error: %s\n",
                    err.Error())
                time.Sleep(BIOFLOW_WATCH_RETRY_INTERVAL)
                continue
            }
            if config == nil {
                continue
            }
            Logger.Infof("Update pipeline store config: %v\n", config)
            err = scheduler.GetPipelineStore().ResetConfig(config)
            if err != nil {
                Logger.Errorf("Fail to reset pipeline store config: %s\n",
                    err.Error())
            }
        }
    }()

    physicalConfigWatcher := db.NewPhysicalConfigWatcher()
    go func() {
        for {
            err, config := physicalConfigWatcher.WatchChanges()
            if err != nil {
                Logger.Errorf("physical config watch error: %s\n",
                    err.Error())
                time.Sleep(BIOFLOW_WATCH_RETRY_INTERVAL)
            }
            if config == nil {
                continue
            }

            phyScheduler := physicalscheduler.GetPhysicalScheduler()
            if config.SchedEnabled {
                phyScheduler.SetConfig(*config)
                if !phyScheduler.Enabled() {
                    Logger.Info("Enable physical scheduler")
                    phyScheduler.Enable()
                }
            } else {
                if phyScheduler.Enabled() {
                    Logger.Info("Disable physical scheduler")
                    phyScheduler.Disable()
                }
            }
        }
    }()

    return nil
}

func BioflowStart(mode string) error {
    /*step 1: initialize the logger facility*/
    envSetting := EnvUtilsParseEnvSetting()

    /* the log file name should be different for 
     * bioflow instances of different queues
     */
    logFileName := fmt.Sprintf("/var/log/bioflow-queue%d.log",
        envSetting.Queue)
    loggerConfig := LoggerConfig{
                        Logfile: logFileName,
                        LogLevel: 0,
                    }
    if mode == BIOFLOW_MODE_DOCKER {
        loggerConfig.Logfile = envSetting.Logfile
    }
    LoggerInit(&loggerConfig)

    /*init version info*/
    InitBioflowVersion()
    Logger.Infof("Bioflow %s started with %s mode\n",
        GetBioflowVersion(), mode)

    /*Setup the signal handler to dump the go routine stack*/
    SetupDumpStackTrap()

    /*save the configuration for external service*/
    Logger.Infof("The Spark Mesos Master is %s\n",
        envSetting.MesosMaster)
    ServiceManifestSetMesosMaster(envSetting.MesosMaster)

    Logger.Infof("Bioflow starts loading configuration\n")

    NewDashBoard()

    /*step 2: Load the configuration*/
    var err error
    var bioflowConfig *BioflowConfig = nil
    var confDB *confbase.BioConfDB = nil
    var instanceDB *confbase.InstanceDB = nil
    var schedulerDB *confbase.SchedulerDB = nil
    schedulerConfig := JobSchedulerConfig{}
    globalConfig := GlobalConfig{}
    var clusterMountConfig []xconfig.StorageClusterMountConfig = nil
    if mode == BIOFLOW_MODE_EMULATION || mode == BIOFLOW_MODE_LOCAL {
        bioflowConfig = confbase.LoadConfigFromJSONFile(envSetting.Configfile)
        if bioflowConfig == nil {
            Logger.Errorf("Fail to load config from file %s\n",
                envSetting.Configfile)
            return errors.New("Fail to load config from " + envSetting.Configfile)
        }

		Logger.Errorf("%v", bioflowConfig)

    } else {
        confDB = confbase.NewBioConfDB(envSetting.EtcdEndPoints)
        instanceDB = confbase.NewInstanceDB(envSetting.EtcdEndPoints)
        err, bioflowConfig = confDB.LoadBioflowConfig()
        if err != nil {
            Logger.Errorf("Can't load configuration from confdb: %s\n",
                err.Error())
            return err
        }
        err = confDB.LoadGlobalConfig(&globalConfig)
        if err != nil {
            Logger.Errorf("Fail to load global config, ignore: %s\n",
                err.Error())
        }

        err, pass := confDB.GetAdminPassword()
        if err != nil {
            Logger.Errorf("Failed to load admin password: %s\n",
                err.Error())
        } else {
            /*
             * get encrypted password.
             * decrypt and encrypt to skey
             */
            aesEnc := NewAesEncrypt(BIOFLOW_SECURITY_KEY_SEED)
            skey, err := aesEnc.Encrypt(pass)
            if err == nil {
                server.SetGlobalAdminSkey(skey)
            }
        }

        err, clusterMountConfig = confDB.LoadStorageClusterConfig()
        if err != nil {
            Logger.Errorf("Fail to load storage cluster config: %s\n",
                err.Error())
        }
        if clusterMountConfig != nil {
            err = xstorage.GetMountMgr().UpdateMounters(clusterMountConfig, nil)
            if err != nil {
                Logger.Errorf("fail to update mounters: %s\n",
                    err.Error())
                return err
            }
        }
        schedulerDB = confbase.NewSchedulerDB(envSetting.EtcdEndPoints)
        err = schedulerDB.LoadJobSchedulerConfig(&schedulerConfig)
        if err != nil {
            Logger.Errorf("Fail to load job scheduler config: %s\n",
                err.Error())
        }
    }
    Logger.Infof("Bioflow configuration loaded: %v", bioflowConfig)

    /*step 3: cluster controller coordinates bioflow cluster*/
    if mode == BIOFLOW_MODE_DOCKER {
        controller := cluster.NewClusterController(envSetting.Queue, 
            instanceDB)
        err = controller.Start()
        if err != nil {
            Logger.Errorf("Create cluster controller failure: %s\n",
                err.Error())
            return err
        }
        Logger.Infof("Bioflow waits for service allow running\n")
        controller.WaitForAllowRunning()
        Logger.Infof("Bioflow is allowed running\n")
    }

    /*step 4: starts service*/
    Logger.Infof("Bioflow starts database service\n")
    /*
     * Bioflow should use its queue's db loglevel and rest config. Use global if
     * no queue config found.
     */
    queueLoglevelConfig := bioflowConfig.ClusterConfig.GetQueueLoglevelConfig(envSetting.Queue)
    if queueLoglevelConfig == nil {
        Logger.Infof("The queue %d loglevel config not exist, use global default\n",
            envSetting.Queue)
        queueLoglevelConfig.LogLevel = globalConfig.LogLevel
    }
    Logger.Infof("Re-Init log to set log level to %d\n",
        queueLoglevelConfig.LogLevel)
    loggerConfig = LoggerConfig {
        Logfile: logFileName,
        LogLevel: queueLoglevelConfig.LogLevel,
    }
    LoggerInit(&loggerConfig)

    queueDBConfig := bioflowConfig.ClusterConfig.GetQueueDBConfig(envSetting.Queue)
    if queueDBConfig == nil {
        Logger.Infof("The queue %d db config not exist, use global default\n",
            envSetting.Queue)
        queueDBConfig = &bioflowConfig.DBConfig
    }
    restConfig := bioflowConfig.ClusterConfig.GetQueueRestConfig(envSetting.Queue)
    if restConfig == nil {
        Logger.Infof("The queue %d rest config not exist, use global default\n",
            envSetting.Queue)
        restConfig = &bioflowConfig.RestConfig
    }
    dbService := dbservice.NewDBService(queueDBConfig)
    if dbService == nil {
        Logger.Errorf("Fail to init DB service")
        return errors.New("Fail to init database")
    }

    Logger.Infof("Bioflow starts storage manager\n")
    storageMgr := storage.NewStorageMgr(&bioflowConfig.StoreConfig)
    if storageMgr == nil {
        Logger.Errorf("Fail to init storage manager\n")
        return errors.New("Fail to init storage manager")
    }

    err = scheduler.InitPipelineStore(&bioflowConfig.PipelineStoreConfig)
    if err != nil {
        Logger.Errorf("Fail to init the pipeline store: %s\n",
            err.Error())
        /*ignore the error*/
    }

    engine.SetLogger(WomLogger)

    mailConfig := bioflowConfig.MailConfig
    if mailConfig.User != ""{
        eventbus.SetMailSender(mailConfig.User, mailConfig.Pass, mailConfig.Host)
    }

    frontConfigs := bioflowConfig.FrontEndConfig
    for _, config := range(frontConfigs) {
        if config.Queue == envSetting.Queue {
            Logger.Infof("Register new front-end %s\n", config.Url)
            scheduler.GetRestApiMgr().Subscribe(config.Url)
        }
    }

    /*
     * Start user manager
     */
    userMgr := scheduler.GetUserMgr()
    securityConfig := &bioflowConfig.SecurityConfig
    userMgr.Configure(securityConfig, envSetting.EtcdEndPoints)
    if userMgr.IsStrictMode() {
        /*load users from external config*/
        Logger.Info("Start load users\n")
        err := userMgr.LoadExternalUsers(3)
        if err != nil {
            Logger.Errorf("Fail to load users in strict mode: %s\n",
                err.Error())
        } else {
            Logger.Info("Successfully load users\n")
        }

        err = userMgr.Recover()
        if err != nil {
            Logger.Errorf("Fail to recover user state from db: %s\n",
                err.Error())
        } else {
            Logger.Infof("Successfully recover user state\n")
        }
    }

    /*
     * Start physical scheduler
     */
    Logger.Info("Bioflow starts physical scheduler\n")
    userId := fmt.Sprintf("bioflow-queue%d", envSetting.Queue)
    phyScheduler := physicalscheduler.NewPhysicalScheduler(userId)
    phyScheduler.Disable()
    phyScheduler.Start()

    phyConfig := bioflowConfig.PhysicalConfig
    if phyConfig.SchedEnabled {
        Logger.Info("Enable physical scheduler")
        phyScheduler.SetConfig(phyConfig)
        phyScheduler.Enable()
    }

    /*
     * Now do core service startup, it should be done
     * in correct order:
     * 1) start pipeline manager
     * 2) start job manager, but don't recover jobs
     * 3) start scheduler backends
     * 4) start scheduler
     * 5) calls job manager to recover jobs
     * 6) starts REST server to serve user requests
     */
    Logger.Infof("Bioflow starts pipeline manager\n")
    pipelineMgr := scheduler.NewPipelineMgr()
    if pipelineMgr == nil {
        Logger.Errorf("Fail to init pipeline manager\n")
        return errors.New("Fail to init pipeline manager")
    }

    pipelineMgr.Start()

    Logger.Infof("Bioflow starts job manager\n")
    jobTracker := scheduler.NewJobTracker(&schedulerConfig)
    jobMgr := scheduler.NewJobMgr(nil, jobTracker)
    if jobMgr == nil {
        Logger.Errorf("Fail to init job manager\n")
        return errors.New("Fail to init job manager")
    }
    jobMgr.Start()

    scheduler.NewBackendMgr()

    Logger.Println("Bioflow starts scheduler")
    callbackURL := fmt.Sprintf("http://%s:%s/v1/tasknotify",
        restConfig.Server, restConfig.Port)
    scheduler := scheduler.CreateScheduler(callbackURL, jobTracker,
        &schedulerConfig)


    if mode == BIOFLOW_MODE_EMULATION {
        BioflowCreateSimulator()
        Logger.Infof("Simulator backend created and started\n")
    } else {
        err = BioflowCreateBackends(bioflowConfig)
        if err != nil {
            Logger.Errorf("Fail to create the schedule backends: %s\n",
                err.Error())
            return err
        }
    }

    /*
     * Start scheduler
     */
    scheduler.Start()

    /*Now start watchers*/
    Logger.Infof("Bioflow starts watchers now\n")
    if mode == BIOFLOW_MODE_DOCKER {
        err = BioflowStartWatchers(confDB, logFileName, envSetting.Queue)
        if err != nil {
            Logger.Errorf("Bioflow fail to create watchers: %s\n",
                err.Error())
        }
    }

    restServiceChan := make(chan bool)
    restAddr := fmt.Sprintf(":%s", restConfig.Port)
    Logger.Infof("Bioflow starts REST Server on %s Now\n",
        restAddr)
    bioServer := server.NewBIORESTServer(restAddr)
    go func() {
        bioServer.StartRESTServer()
        restServiceChan <- true
    }()

    Logger.Info("Bioflow starts recover jobs\n")
    jobMgr.RecoverFromDB()

    Logger.Info("Wait for REST service done\n")
    <- restServiceChan

    return nil
}

