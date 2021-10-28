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

package cluster

import (
    "os"
    "time"
    "sync"
    "errors"

    . "github.com/xtao/bioflow/confbase"
    . "github.com/xtao/bioflow/common"
    . "github.com/xtao/bioflow/scheduler"
)


/*
 * The cluster controller is responsible for 
 * 1) Coordinate bioflow instances for single
 *    queue to ensure schedule or job data consistency
 *    when network partitions
 * 2) Coordinate bioflow instances when marathon
 *    launch multiple instances on mesos slave lost
 *
 */
type ClusterController struct {
    //each instance is identified by
    //its queue and per-queue instance id
    lock sync.RWMutex
    queueID int
    instanceID uint64
    lastInstanceID uint64
    instanceMap map[uint64]*BioflowInstanceConfig

    stateLock *sync.Mutex
    stateCond *sync.Cond
    state int

    //facility operating ETCD
    memberWatcher *ClusterQueueMemberWatcher
    instanceDB *InstanceDB
}

func NewClusterController(queueId int, instanceDB *InstanceDB) *ClusterController {
    controller := &ClusterController {
            queueID: queueId,
            instanceID: 0,
            instanceDB: instanceDB,
            instanceMap: make(map[uint64]*BioflowInstanceConfig),
            state: BIOFLOW_STATE_INVALID,
    }
    
    controller.stateLock = new(sync.Mutex)
    controller.stateCond = sync.NewCond(controller.stateLock)
    controller.memberWatcher = instanceDB.NewQueueMemberWatcher(queueId)
    return controller
}

func (controller *ClusterController)Queue() int {
    return controller.queueID
}

func (controller *ClusterController)InstanceID() uint64 {
    return controller.instanceID
}

func (controller *ClusterController)LastInstanceID() uint64 {
    return controller.lastInstanceID
}

func (controller *ClusterController)SetState(state int) {
    controller.stateLock.Lock()
    defer controller.stateLock.Unlock()

    controller.state = state
    controller.stateCond.Broadcast()
}

func (controller *ClusterController)State() int {
    return controller.state
}

func (controller *ClusterController)WaitForAllowRunning() {
    controller.stateLock.Lock()
    defer controller.stateLock.Unlock()

    for {
        if controller.state != BIOFLOW_STATE_RUNNING {
            controller.stateCond.Wait()
        } else {
            return
        }
    }
}

func (controller *ClusterController)SetInstanceID(id uint64) {
    controller.lock.Lock()
    defer controller.lock.Unlock()

    controller.lastInstanceID = controller.instanceID
    controller.instanceID = id
}

func (controller *ClusterController)AddNewInstanceToMap(instConfig *BioflowInstanceConfig) {
    controller.lock.Lock()
    defer controller.lock.Unlock()

    controller.instanceMap[instConfig.Seq] = instConfig
}

func (controller *ClusterController)DeleteInstanceFromMap(instConfig *BioflowInstanceConfig) {
    controller.lock.Lock()
    defer controller.lock.Unlock()

    if _, ok := controller.instanceMap[instConfig.Seq]; ok {
        delete(controller.instanceMap, instConfig.Seq)
    }
}

/*Bioflow cluster events*/
const (
    BIOFLOW_EVENT_REGISTERED int = 1
    BIOFLOW_EVENT_REREGISTERED int = 2
    BIOFLOW_EVENT_INSTANCE_EXIT int = 3
    BIOFLOW_EVENT_INSTANCE_CREATED int = 4
    BIOFLOW_EVENT_INSTANCE_LOST int = 5
    BIOFLOW_EVENT_ETCD_LOST int = 6
)

/*Bioflow default setting*/
const (
    BIOFLOW_RETRY_INTERVAL int = 5
    BIOFLOW_RETRY_TIMEOUT int = 5
    BIOFLOW_REFRESH_INTERVAL int = 120
)

const (
    BIOFLOW_INSTANCE_TTL time.Duration = time.Duration(180) * time.Second
)

func (controller *ClusterController)ProcessClusterState() error {
    lowerSeqExist := false
    higherSeqExist := false
    lastSeqExist := false
    /*
     * Traverse instances list and check following cases:
     * 1) a new instance with higher seq exist, mean cluster start
     *    a new bioflow and we should exit
     * 2) a lower seq (not my last seq) instance exists, we notify it to exit.
     *    we need wait its exit. after it exits, we may take over its job.
     * 3) only me exist, starts working now
     * 4) only me and my last seq exists, I need remove my last sequence. 
     *    currently the last seq not persist, so if I die and be restarted soon, 
     *    I will see a lower seq and just wait for it to expire.
     */
    controller.lock.RLock()
    for seq, _ := range controller.instanceMap {
        if seq > controller.InstanceID() {
            higherSeqExist = true
        } else if seq < controller.InstanceID() {
            lowerSeqExist = true
            if seq == controller.LastInstanceID() {
                lastSeqExist = true
            }
        }
    }
    controller.lock.RUnlock()

    if higherSeqExist {
        /* A new bioflow instance created, we need quiesce all service
         * and exit
         */
        ClusterLogger.Infof("A new instance with higher seq found, quiesce myself\n")
        err := controller.QuiesceService()
        if err != nil {
            ClusterLogger.Infof("Fail to quiesce service: %s\n",
                err.Error())
        }
        ClusterLogger.Infof("Try to gracefully shutdown\n")
        err = controller.instanceDB.DeleteInstanceState(controller.Queue(),
            controller.InstanceID())
        if err != nil {
            ClusterLogger.Infof("Fail to delete instance state %d/%d\n",
                controller.Queue(), controller.InstanceID())
            os.Exit(-1)
        }
        os.Exit(0)
    } else if lowerSeqExist {
        if lastSeqExist {
            /*I am the old one, remove it and starts working*/
            controller.instanceDB.DeleteInstanceState(controller.Queue(),
                controller.LastInstanceID())
        }
        /*Wait for the old one to exit*/
        controller.SetState(BIOFLOW_STATE_WAIT_OTHER_EXIT)
        ClusterLogger.Infof("Wait for lower seq instance exit\n")
    } else {
        /*start working now*/
        controller.ResumeService()
    }

    return nil
}

func (controller *ClusterController)QuiesceService() error {
    ClusterLogger.Infof("Try to quiesce service now\n")
    controller.SetState(BIOFLOW_STATE_QUIESCING)
    scheduler := GetScheduler()
    if scheduler != nil {
        ClusterLogger.Infof("Try to quiesce scheduler\n")
        if err := scheduler.Disable(); err != nil {
            ClusterLogger.Infof("Fail to quiesce service %s\n",
                err.Error())
            return err
        } else {
            ClusterLogger.Infof("Succeed to quiesce scheduler\n")
        }
    }
    controller.SetState(BIOFLOW_STATE_QUIESCED)
    ClusterLogger.Infof("All service quiesced\n")
    return nil
}

func (controller *ClusterController)ResumeService() error {
    needResume := false
    if controller.State() == BIOFLOW_STATE_QUIESCED ||
        controller.State() == BIOFLOW_STATE_QUIESCING {
        needResume = true 
    }

    if needResume {
        ClusterLogger.Infof("Try to resume service now\n")
        scheduler := GetScheduler()
        if scheduler != nil {
            ClusterLogger.Infof("Try to resume scheduler\n")
            if err := scheduler.Enable(); err != nil {
                ClusterLogger.Infof("Fail to resume service %s\n",
                    err.Error())
                return err
            } else {
                ClusterLogger.Infof("Succeed to resume scheduler\n")
            }
        }
        ClusterLogger.Infof("All service are resumed\n")
    }

    controller.SetState(BIOFLOW_STATE_RUNNING)

    return nil
}

/*
 * Cluster controller state machine handles all events on
 * bioflow cluster and network
 */
func (controller *ClusterController)HandleClusterEvent(event int,
    instSeq uint64) error {

    ClusterLogger.Infof("Cluster Controller starts handle event %d seq %d\n",
        event, instSeq)
    switch event {
        case BIOFLOW_EVENT_REGISTERED:
            controller.ProcessClusterState()
        case BIOFLOW_EVENT_REREGISTERED:
            controller.ProcessClusterState()
        case BIOFLOW_EVENT_INSTANCE_EXIT:
            controller.ProcessClusterState()
        case BIOFLOW_EVENT_INSTANCE_LOST:
            controller.ProcessClusterState()
        case BIOFLOW_EVENT_INSTANCE_CREATED:
            controller.ProcessClusterState()
        case BIOFLOW_EVENT_ETCD_LOST:
            ClusterLogger.Infof("ETCD lost and can't recover, exit now\n")
            os.Exit(1)
        default:
            ClusterLogger.Infof("Unknown event %d, ignore it\n", event)
            return errors.New("Unknown event")
    }
    ClusterLogger.Infof("Cluster Controller complete handle event %d seq %d\n",
        event, instSeq)

    return nil
}

/*register to ETCD as a new instance with higher instance sequence*/
func (controller *ClusterController)RegisterInstance(timeout int) error {
    queue := controller.Queue()
    retryCount := 0
    var mySeq uint64 = 0
    gotSeq := false
    db := controller.instanceDB
    for ;retryCount < timeout; {
        /*atomtically get a higher instance sequence via ETCD*/
        err, curSeq := db.GetQueueSeq(queue)
        if err != nil {
            ClusterLogger.Infof("Can't get sequence number for queue %d: %s\n",
                queue, err.Error())
            time.Sleep(time.Duration(BIOFLOW_RETRY_INTERVAL) * time.Second)
            retryCount ++
            continue
        } else {
            mySeq = curSeq + 1
            err = db.SetQueueSeq(queue, mySeq, curSeq)
            if err != nil {
                ClusterLogger.Infof("Can't increase set sequence %d/%d: %s\n",
                    mySeq, curSeq, err.Error())
                time.Sleep(time.Duration(BIOFLOW_RETRY_INTERVAL) * time.Second)
                retryCount ++
                continue
            } else {
                ClusterLogger.Infof("Successfully get my sequence %d\n",
                    mySeq)
                gotSeq = true
                break
            }
        }
    }

    if !gotSeq {
        return errors.New("Can't get sequence")
    }

    /*register my instance to queue*/
    controller.SetInstanceID(mySeq)
    instConfig := &BioflowInstanceConfig{
                Seq: mySeq,
                Version: GetBioflowVersion(),
                State: BIOFLOW_STATE_REGISTERED,
        }
    for ;retryCount <= timeout; {
        err := db.CreateInstanceState(queue, mySeq, instConfig,
            BIOFLOW_INSTANCE_TTL)
        if err != nil {
            ClusterLogger.Infof("Can't create instance state for %d/%d:%s\n",
                queue, mySeq, err.Error())
            time.Sleep(time.Duration(BIOFLOW_RETRY_INTERVAL) * time.Second)
            retryCount ++
        } else {
            ClusterLogger.Infof("Successfully register the instance\n")
            return nil
        }
    }

    return errors.New("Fail to create instance state")
}

/*reconnect to ETCD and register as my current instance sequence*/
func (controller *ClusterController)ReRegisterInstance(timeout int) error {
    retryCount := 0
    queue := controller.Queue()
    mySeq := controller.InstanceID()
    db := controller.instanceDB

    ClusterLogger.Infof("Controller re-register instance\n")
    instConfig := &BioflowInstanceConfig{
                Seq: mySeq,
                Version: GetBioflowVersion(),
                State: BIOFLOW_STATE_REREGISTERED,
        }
    for ;retryCount <= timeout; {
        err := db.CreateInstanceState(queue, mySeq, instConfig,
            BIOFLOW_INSTANCE_TTL)
        if err != nil {
            ClusterLogger.Infof("Can't create instance state for %d/%d:%s\n",
                queue, mySeq, err.Error())
            time.Sleep(time.Duration(BIOFLOW_RETRY_INTERVAL) * time.Second)
            retryCount ++
        } else {
            ClusterLogger.Infof("Controller re-register instance done\n")
            return nil
        }
    }

    return errors.New("Fail to create instance state")
}

/*watch and handle instance membership change for each queue*/
func (controller *ClusterController)HandleQueueMembershipChange() error {
    /*
     * When netowrk partitions or slave node hosting bioflow
     * disconnect from master, marathon may launch multiple bioflow
     * instances. So bioflow need handles this itself:
     * 1) If a new instance with higher id observed, it need exit
     * 2) If disconnected from ETCD, retry for some time and exit
     *    if still not connected
     *
     */
    err, newInst, exitInst, expireInst := 
        controller.memberWatcher.WatchChanges()
    if err != nil {
        ClusterLogger.Infof("watch queue %d membership error: %s\n",
            controller.Queue(), err.Error())
        /*
         * we may have network problems between bioflow and etcd
         * cluster. So check network first.
         */
        ClusterLogger.Infof("We may have network problems, so quiesce\n")
        err = controller.QuiesceService()
        if err != nil {
            ClusterLogger.Infof("Quiesce service failure %s\n",
                err.Error())
        }
        err = controller.ReRegisterInstance(BIOFLOW_RETRY_TIMEOUT)
        if err != nil {
            ClusterLogger.Infof("The bioflow can't reregister to ETCD %s\n",
                err.Error())
            controller.HandleClusterEvent(BIOFLOW_EVENT_ETCD_LOST,
                0)
        } else {
            ClusterLogger.Infof("Bioflow instance re-registered\n")
            controller.HandleClusterEvent(BIOFLOW_EVENT_REREGISTERED,
                0)
        }
        return nil
    }

    /* 
     * New instance start-up, compare sequence number and the one with
     * lower sequence should exit
     */
    if newInst != nil {
        ClusterLogger.Infof("New Instance observed: %d, %s, %d\n",
            newInst.Seq, newInst.Version, newInst.State)
        controller.AddNewInstanceToMap(newInst)
        controller.HandleClusterEvent(BIOFLOW_EVENT_INSTANCE_CREATED,
            newInst.Seq)
    }

    /*
     * Some instance exit gracefully, so we may take over its job safely
     * now.
     */
    if exitInst != nil {
        ClusterLogger.Infof("Instance exit gracefully observed: %d, %s, %d\n",
            exitInst.Seq, exitInst.Version, exitInst.State)
        controller.DeleteInstanceFromMap(exitInst)
        controller.HandleClusterEvent(BIOFLOW_EVENT_INSTANCE_EXIT,
            exitInst.Seq)
    }

    /*
     * Some instance expired because of following cases:
     * 1) its connection to ETCD lost and can't reconnect
     * 2) it panic or exit abnormally
     */
    if expireInst != nil {
        ClusterLogger.Infof("Instance exit abnormally observed: %d, %s, %d\n",
            expireInst.Seq, expireInst.Version, expireInst.State)
        controller.DeleteInstanceFromMap(expireInst)
        controller.HandleClusterEvent(BIOFLOW_EVENT_INSTANCE_LOST,
            expireInst.Seq)
    }

    return nil
}

func (controller *ClusterController)RefreshInstanceState() {
    db := controller.instanceDB
    for {
        err := db.RefreshInstanceState(controller.Queue(),
            controller.InstanceID(), BIOFLOW_INSTANCE_TTL)
        if err != nil {
            ClusterLogger.Infof("Fail refresh state %s, may have connection problem\n",
                err.Error())
            ClusterLogger.Infof("Try to quiesce service\n")
            err = controller.QuiesceService()
            if err != nil {
                ClusterLogger.Infof("Quiesce service failure %s\n",
                    err.Error())
            }
            err = controller.ReRegisterInstance(BIOFLOW_RETRY_TIMEOUT)
            if err != nil {
                ClusterLogger.Infof("The bioflow can't reregister to ETCD %s\n",
                    err.Error())
                controller.HandleClusterEvent(BIOFLOW_EVENT_ETCD_LOST,
                    0)
            } else {
                ClusterLogger.Infof("Bioflow instance re-registered\n")
                controller.HandleClusterEvent(BIOFLOW_EVENT_REREGISTERED,
                    0)
            }
        }
        time.Sleep(time.Duration(BIOFLOW_REFRESH_INTERVAL) * time.Second)
    }
}

func (controller *ClusterController)Start() error {
    db := controller.instanceDB

    /*step 1: register myself*/
    ClusterLogger.Infof("Bioflow register it to ETCD\n")
    err := controller.RegisterInstance(BIOFLOW_RETRY_TIMEOUT)
    if err != nil {
        ClusterLogger.Infof("Can't register itself\n")
        return err
    }
    controller.SetState(BIOFLOW_STATE_REGISTERED)

    /*step 2: start refresh instance state*/
    ClusterLogger.Infof("Bioflow starts refresh instance state\n")
    go func() {
        controller.RefreshInstanceState()
    }()

    /*step 3: start watch the membership change*/
    ClusterLogger.Infof("Bioflow starts watch membership changes\n")
    go func() {
        for {
            err := controller.HandleQueueMembershipChange()
            if err != nil {
                ClusterLogger.Infof("Handle membership change failure: %s\n",
                    err.Error())
                time.Sleep(10 * time.Second)
            }
        }
    }()

    /*step 4: initial coordinate with other instances*/
    err, instances := db.GetAllInstanceState(controller.Queue())
    if err != nil {
        ClusterLogger.Infof("Can't get instance list: %s\n",
            err.Error())
        controller.HandleClusterEvent(BIOFLOW_EVENT_ETCD_LOST, 0)
    }
    for i := 0; i < len(instances); i ++ {
        controller.AddNewInstanceToMap(&instances[i])
    }
    controller.HandleClusterEvent(BIOFLOW_EVENT_REGISTERED, 0)

    return nil
}
