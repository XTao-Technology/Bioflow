package physicalscheduler

import (
	"fmt"
	"github.com/Sirupsen/logrus"
	. "github.com/xtao/bioflow/common"
	"math"
	"sync"
	"time"
)

const (
	EVENTIDLE = iota
	EVENTTHROATE
	EVENTDELAY
	EVENTDELETE
	EVENTMAXNUM

	INITIALINSTANCES  = 3
	DEFAULTEVENTQUOTA = 10000
	SECSPERMIN        = 60
	DELETEINSTANCESINTERVAL = 600
)

type physicalScheduler struct {
	userId     string
	config     physicalConfig
	configLock sync.RWMutex

	machineMgr machineMgr
	eventChan  chan *PhysicalEvent

	instLock          sync.RWMutex
	curSlaveInstances int
	baseInstances     int
	throated          bool
	delayed           bool
	prevTime          int64

	timer *time.Timer

	enabled bool
	lock    sync.RWMutex
}

type physicalConfig struct {
	ThroatePeriod           int
	ThroateBatchCount       int
	DelayPeriod             int
	DelayBatchCount         int
	ThroateInstancesLimit   int
	DelayInstancesLimit     int
	MasterInstances         int
	DeleteInstancesInterval int
}

type PhysicalEvent struct {
	EventType int
	EventTime time.Time
	EventData []string
}

var globalPhysicalScheduler *physicalScheduler = nil

func GetPhysicalScheduler() *physicalScheduler {
	if globalPhysicalScheduler == nil {
		globalPhysicalScheduler = &physicalScheduler{
			enabled: false,
		}
	}

	return globalPhysicalScheduler
}

func NewPhysicalScheduler(userId string) *physicalScheduler {
	if globalPhysicalScheduler == nil {
		scheduler := &physicalScheduler{
			userId:     userId,
			machineMgr: NewVirtualMachineMgr(),
			eventChan:  make(chan *PhysicalEvent, DEFAULTEVENTQUOTA),
			throated:   false,
			delayed:    false,
		}

		globalPhysicalScheduler = scheduler
	}

	return globalPhysicalScheduler
}

func (phySched *physicalScheduler) SetConfig(config PhysicalConfig) {
	phySched.configLock.Lock()
	phySched.config.ThroatePeriod = config.ThroatePeriod
	phySched.config.ThroateBatchCount = config.ThroateBatchCount
	phySched.config.DelayPeriod = config.DelayPeriod
	phySched.config.DelayBatchCount = config.DelayBatchCount
	phySched.config.ThroateInstancesLimit = config.ThroateInstancesLimit
	phySched.config.DelayInstancesLimit = config.DelayInstancesLimit
	phySched.config.MasterInstances = config.MasterInstances
	if config.DeleteInstancesInterval == 0 {
		phySched.config.DeleteInstancesInterval = DELETEINSTANCESINTERVAL
	} else {
		phySched.config.DeleteInstancesInterval = config.DeleteInstancesInterval
	}

	PhysicalLogger.WithFields(logrus.Fields{
		"throatePeriod":           phySched.config.ThroatePeriod,
		"throateBatchCount":       phySched.config.ThroateBatchCount,
		"delayPeriod":             phySched.config.DelayPeriod,
		"delayBatchCount":         phySched.config.DelayBatchCount,
		"throateInstancesLimit":   phySched.config.ThroateInstancesLimit,
		"delayInstancesLimit":     phySched.config.DelayInstancesLimit,
		"masterInstances":         phySched.config.MasterInstances,
		"deleteInstancesInterval": phySched.config.DeleteInstancesInterval,
	}).Debug("Set physical scheduler config")
	phySched.configLock.Unlock()

	phySched.machineMgr.setConfig(config)
}

func (phySched *physicalScheduler) GetConfig() physicalConfig {
	phySched.configLock.RLock()
	defer phySched.configLock.RUnlock()

	return phySched.config
}

func (phySched *physicalScheduler) Enable() {
	phySched.lock.Lock()
	defer phySched.lock.Unlock()

	phySched.enabled = true
}

func (phySched *physicalScheduler) Disable() {
	phySched.lock.Lock()
	defer phySched.lock.Unlock()

	phySched.enabled = false
}

func (phySched *physicalScheduler) Enabled() bool {
	phySched.lock.RLock()
	defer phySched.lock.RUnlock()

	return phySched.enabled
}

func (phySched *physicalScheduler) Start() {
	go func() {
		vmStarted := false
		for true {
			event := <-phySched.eventChan

			if !vmStarted {
				totalInstances, err := phySched.machineMgr.listInstances()
				if err == nil {
					vmStarted = true
					curSlaveInstances := 0
					slaveInstances := totalInstances - phySched.GetConfig().MasterInstances
					if slaveInstances < INITIALINSTANCES {
						err := phySched.machineMgr.createInstances(INITIALINSTANCES-slaveInstances, phySched.userId)
						if err != nil {
							curSlaveInstances = phySched.setCurInstances(slaveInstances)
							ParserLogger.Error("Failed to create instances when event can be handled")
						} else {
							curSlaveInstances = phySched.setCurInstances(INITIALINSTANCES)
						}
					} else {
						curSlaveInstances = phySched.setCurInstances(slaveInstances)
					}

					PhysicalLogger.WithFields(logrus.Fields{
						"totalInstances":    totalInstances,
						"curSlaveInstances": curSlaveInstances,
						"masterInstances":   phySched.GetConfig().MasterInstances,
					}).Info("Handle first event")
				}
			}

			if vmStarted {
				if event.EventType >= EVENTMAXNUM {
					PhysicalLogger.Errorln("No such event, it will be discarded")
					continue
				}

				switch event.EventType {
					case EVENTIDLE:
						phySched.idleHandler(event)
					case EVENTTHROATE:
						phySched.throateHandler(event)
					case EVENTDELAY:
						phySched.delayHandler(event)
					case EVENTDELETE:
						phySched.deleteHandler(event)
				}
			}
		}
	}()
}

func (phySched *physicalScheduler) SubmitEvent(event *PhysicalEvent) {
	if phySched.Enabled() {
		phySched.eventChan <- event
	}
}

func (phySched *physicalScheduler) idleHandler(event *PhysicalEvent) {
	PhysicalLogger.Infoln("Handle idle event")

	if phySched.throated == true {
		phySched.throated = false
	} else if phySched.delayed == true {
		phySched.delayed = false
	}
	phySched.prevTime = 0
}

func (phySched *physicalScheduler) deleteHandler(event *PhysicalEvent) {
	PhysicalLogger.Infoln("Handle delete event")

	if (len(event.EventData)) == 0 {
		return
	}

	err := phySched.machineMgr.deleteInstances(event.EventData)
	if err != nil {
		PhysicalLogger.Errorf("Failed to delete instances, err: %s", err.Error())
	} else {
		curSlaveInstances := phySched.decCurInstances(len(event.EventData))
		PhysicalLogger.WithFields(logrus.Fields{
			"curSlaveInstances": curSlaveInstances,
		}).Info("Handle delete event")
	}
}

func (phySched *physicalScheduler) policy(instancesLimit, period, batchCount int) int {
	capacity := 0

	curSlaveInstances := phySched.getCurInstances()
	if curSlaveInstances < instancesLimit {
		incCount := (time.Now().Unix() - phySched.prevTime) / int64(period*SECSPERMIN) * int64(batchCount)
		totalCount := math.Min(float64(int64(curSlaveInstances)+incCount), float64(instancesLimit))
		capacity = int(totalCount) - curSlaveInstances
		PhysicalLogger.WithFields(logrus.Fields{
			"IncCount":   incCount,
			"totalCount": totalCount,
			"capacity":   capacity,
		}).Debug("policy of physicalScheduler")
	}

	return capacity
}

func (phySched *physicalScheduler) docheckCurInstances() {
	totalInstances, err := phySched.machineMgr.listInstances()
	if err == nil {
		curSlaveInstances := phySched.setCurInstances(totalInstances - phySched.GetConfig().MasterInstances)
		PhysicalLogger.WithFields(logrus.Fields{
			"totalInstances":    totalInstances,
			"curSlaveInstances": curSlaveInstances,
		}).Info("Do check current instances")
	} else {
		PhysicalLogger.Errorf("Failed to do check current instances, err: %s", err.Error())
	}

	phySched.timer = nil
}

func (phySched *physicalScheduler) getCurInstances() int {
	phySched.instLock.RLock()
	defer phySched.instLock.RUnlock()

	return phySched.curSlaveInstances
}

func (phySched *physicalScheduler) setCurInstances(curSlaveInstances int) int {
	phySched.instLock.Lock()
	defer phySched.instLock.Unlock()

	phySched.curSlaveInstances = curSlaveInstances
	return phySched.curSlaveInstances
}

func (phySched *physicalScheduler) incCurInstances(incInstances int) int {
	phySched.instLock.Lock()
	defer phySched.instLock.Unlock()

	phySched.curSlaveInstances += incInstances
	return phySched.curSlaveInstances
}

func (phySched *physicalScheduler) decCurInstances(decInstances int) int {
	phySched.instLock.Lock()
	defer phySched.instLock.Unlock()

	phySched.curSlaveInstances -= decInstances
	return phySched.curSlaveInstances
}

func (phySched *physicalScheduler) checkCurInstances(period int) {
	if phySched.timer == nil {
		go func() {
			phySched.timer = time.NewTimer(time.Duration(period) * SECSPERMIN * time.Second)
			select {
			case <-phySched.timer.C:
				PhysicalLogger.Info("Check current instances")
				phySched.docheckCurInstances()
			}
		}()
	}
}

func (phySched *physicalScheduler) throateHandler(event *PhysicalEvent) {
	PhysicalLogger.Infoln("Handle throate event")

	config := phySched.GetConfig()
	curSlaveInstances := phySched.getCurInstances()
	if curSlaveInstances >= config.ThroateInstancesLimit {
		PhysicalLogger.WithFields(logrus.Fields{
			"curSlaveInstances": curSlaveInstances,
			"throateLimit":      config.ThroateInstancesLimit,
		}).Info("Handle throate event: current instances is beyond of the throate limit")

		phySched.checkCurInstances(config.ThroatePeriod)
		return
	}

	if !phySched.throated {
		phySched.throated = true
		phySched.baseInstances = phySched.getCurInstances()
		phySched.prevTime = event.EventTime.Unix()
	}

	capacity := phySched.policy(config.ThroateInstancesLimit,
		config.ThroatePeriod, config.ThroateBatchCount)
	if capacity > 0 {
		err := phySched.machineMgr.createInstances(capacity, phySched.userId)
		if err != nil {
			ParserLogger.Error("Failed to create instances for throate event")
		} else {
			curSlaveInstances := phySched.incCurInstances(capacity)
			phySched.prevTime = event.EventTime.Unix()
			PhysicalLogger.WithFields(logrus.Fields{
				"curSlaveInstances": curSlaveInstances,
				"incInstances":      capacity,
			}).Info("Handle throate event")
		}
	}

	return
}

func (phySched *physicalScheduler) delayHandler(event *PhysicalEvent) {
	PhysicalLogger.Infoln("Handle delay event")

	config := phySched.GetConfig()
	curSlaveInstances := phySched.getCurInstances()
	if curSlaveInstances >= config.DelayInstancesLimit {
		PhysicalLogger.WithFields(logrus.Fields{
			"curSlaveInstances": curSlaveInstances,
			"delayLimit":        config.DelayInstancesLimit,
		}).Info("Handle delay event: current instances is beyond of the delay limit")

		phySched.checkCurInstances(config.DelayPeriod)
		return
	}

	if !phySched.delayed {
		phySched.delayed = true
		phySched.baseInstances = phySched.getCurInstances()
		phySched.prevTime = event.EventTime.Unix()
	}

	capacity := phySched.policy(config.DelayInstancesLimit,
		config.DelayPeriod, config.DelayBatchCount)
	if capacity > 0 {
		err := phySched.machineMgr.createInstances(capacity, phySched.userId)
		if err != nil {
			ParserLogger.Error("Failed to create instances for delay event")
		} else {
			curSlaveInstances := phySched.incCurInstances(capacity)
			phySched.prevTime = event.EventTime.Unix()
			PhysicalLogger.WithFields(logrus.Fields{
				"curSlaveInstances": curSlaveInstances,
				"incInstances":      capacity,
			}).Info("Handle delay event")
		}
	}

	return
}

func PhysicalEventtoString(event PhysicalEvent) string {
	eventStr := "Event: "
	switch event.EventType {
		case EVENTIDLE:
			eventStr = "idle"
		case EVENTTHROATE:
			eventStr = "throate"
		case EVENTDELAY:
			eventStr = "delay"
		case EVENTDELETE:
			eventStr = "delete"
		default:
			return "Invalid event"
	}
	eventStr += fmt.Sprintf(" Time: %s ", event.EventTime.String())
	if len(event.EventData) > 0 {
		eventStr += fmt.Sprintf("IP: ")
		for _, v := range event.EventData {
			eventStr += fmt.Sprintf("%s ", v)
		}
	}

	return eventStr
}
