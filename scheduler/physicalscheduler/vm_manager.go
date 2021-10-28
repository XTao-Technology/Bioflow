package physicalscheduler

import (
	"bytes"
	"encoding/json"
	"fmt"
	"github.com/Sirupsen/logrus"
	. "github.com/xtao/bioflow/common"
	. "github.com/xtao/xcloud/vmmanager/message"
	"net/http"
	"sync"
	"io"
)

const (
	DEFAULTCHARGETYPE = "postpaid"
	DEFAULTROLE       = "cloud"
)

type VirtualMachineMgr struct {
	config vmMgrConfig
	lock   sync.RWMutex

	client *http.Client
}

type vmMgrConfig struct {
	endPoint     string
	nfsServer    string
	instanceType string
}

func NewVirtualMachineMgr() *VirtualMachineMgr {
	return &VirtualMachineMgr{
		client: new(http.Client),
	}
}

func (vmMgr *VirtualMachineMgr) setConfig(config PhysicalConfig) {
	vmMgr.lock.Lock()
	defer vmMgr.lock.Unlock()

	vmMgr.config.endPoint = config.EndPoint
	vmMgr.config.nfsServer = config.NFSServer
	vmMgr.config.instanceType = config.InstanceType

	PhysicalLogger.WithFields(logrus.Fields{
		"endPoint":     vmMgr.config.endPoint,
		"nfsServer":    vmMgr.config.nfsServer,
		"instanceType": vmMgr.config.instanceType,
	}).Info("Set virtual machine manager config")
}

func (vmMgr *VirtualMachineMgr) getConfig() vmMgrConfig {
	vmMgr.lock.RLock()
	defer vmMgr.lock.RUnlock()

	return vmMgr.config
}

func (vmMgr *VirtualMachineMgr) createInstances(capacity int, userId string) error {
	PhysicalLogger.WithFields(logrus.Fields{
		"capacity": capacity,
		"userId":   userId,
	}).Info("Create instances")

	message := CreateInstanceMessage{
		UserId:     userId,
		Role:       DEFAULTROLE,
		Nfs:        vmMgr.getConfig().nfsServer,
		Type:       vmMgr.getConfig().instanceType,
		Num:        capacity,
		ChargeType: DEFAULTCHARGETYPE,
	}

	buf := bytes.Buffer{}
	err := json.NewEncoder(&buf).Encode(message)
	if err != nil {
		PhysicalLogger.Errorln("Failed to encode HTTP request")
		return err
	}

	url := fmt.Sprintf("http://%s%s", vmMgr.getConfig().endPoint, "/api/instance/create")
	return vmMgr.handleHttpRequest("POST", url, &buf, nil)
}

func (vmMgr *VirtualMachineMgr) deleteInstances(slaveIps []string) error {
	PhysicalLogger.Infoln("Delete instances")

	message := DeleteInstanceMessage{}
	for _, slaveIp := range slaveIps {
		message.Ips = append(message.Ips, slaveIp)
	}

	buf := bytes.Buffer{}
	err := json.NewEncoder(&buf).Encode(message)
	if err != nil {
		logrus.WithError(err).Error("Failed to encode HTTP request")
		return err
	}

	url := fmt.Sprintf("http://%s%s", vmMgr.getConfig().endPoint, "/api/instance/delete")
	return vmMgr.handleHttpRequest("POST", url, &buf, nil)
}

func (vmMgr *VirtualMachineMgr) listInstances() (int, error) {
	PhysicalLogger.Infoln("List instances")

	data := Result{}
	url := fmt.Sprintf("http://%s%s", vmMgr.getConfig().endPoint, "/api/slaves/get")
	err := vmMgr.handleHttpRequest("GET", url, nil, func(resp *http.Response) error {
		err := json.NewDecoder(resp.Body).Decode(&data)
		if err != nil {
			PhysicalLogger.Errorln("Failed to decode HTTP response")
		}

		return err
	})

	var instanceCount int = 0
	if err == nil {
		instanceCount = len(data.Slaves)
	} else {
		PhysicalLogger.Errorf("Failed to get mesos slaves state, err: %s", err.Error())
	}

	PhysicalLogger.WithFields(logrus.Fields{
		"instances": instanceCount,
	}).Info("List instances")

	return instanceCount, err
}

func (vmMgr *VirtualMachineMgr) handleHttpRequest(method, url string, body io.Reader,
	handler func(resp *http.Response) error) error {

	req, err := http.NewRequest(method, url, body)
	if err != nil {
		PhysicalLogger.Errorln("Failed to create a new HTTP request")
		return err
	}

	resp, err := vmMgr.client.Do(req)
	if err != nil {
		PhysicalLogger.Errorln("Failed to send HTTP request")
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusAccepted {
		err = fmt.Errorf("Received StatusCode: %d\n", resp.StatusCode)
		return err
	}

	if handler != nil {
		err = handler(resp)
	}

	return err
}
