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
	"github.com/xtao/bioflow/scheduler/backend"
    . "github.com/xtao/bioflow/common"
    . "github.com/xtao/bioflow/message"
)

type BackendMgr interface {
	ListBackend() (error, []BioflowBackendInfo)
	AddBackend(t string, server string, port string) (error, *BioflowBackendInfo)
	DeleteBackend(backendId string) error
	EnableBackend(backendId string) error
	DisableBackend(backendId string) error
	CreateSimBackend()
}

type backendMgr struct {
}

var globalBackendMgr BackendMgr = nil

func GetBackendMgr() BackendMgr {
	return globalBackendMgr
}

func NewBackendMgr() BackendMgr {
	if globalBackendMgr == nil {
		mgr := &backendMgr {
		}

		globalBackendMgr = mgr
	}

	return globalBackendMgr
}

func (mgr *backendMgr) CreateSimBackend() {
        b := backend.NewSimulatorBackend(10)
        b.Start()
}

func (mgr *backendMgr) AddBackend(t string, server string, port string) (error, *BioflowBackendInfo) {

	var b backend.ScheduleBackend

	id := GenBackendId(t, server, port)

	switch t {
	case BACKEND_TYPE_PALADIN, BACKEND_TYPE_EREMETIC:
		eremeticURL := fmt.Sprintf("http://%s:%s", server, port)
		b = backend.NewEremeticBackend(id, eremeticURL)
		b.Start()
		SchedulerLogger.Infof("Created and started eremetic backend %s\n",
			eremeticURL)
	case BACKEND_TYPE_KUBERNETES:
		k8sURL := fmt.Sprintf("http://%s:%s", server, port)
		b = backend.NewK8sBackend(id, k8sURL)
		b.Start()
		SchedulerLogger.Infof("Created and started kubernetes backend %s\n",
			k8sURL)
	}

	beScheduler := GetBeScheduler()
	beScheduler.AddBackend(b)
	err := beScheduler.EnableBackend(id)
    if err != nil {
        SchedulerLogger.Errorf("Fail to add and enable backend %s\n",
            id)
        return err, nil
    }

	return beScheduler.GetBackendInfo(id)
}

func (mgr *backendMgr) DeleteBackend(id string) error {
	beScheduler := GetBeScheduler()
	beScheduler.QuiesceBackend(id, true)
	return beScheduler.DeleteBackend(id)
}

func (mgr *backendMgr) DisableBackend(id string) error {
	beScheduler := GetBeScheduler()
	return beScheduler.QuiesceBackend(id, true)
}

func (mgr *backendMgr) EnableBackend(id string) error{
	beScheduler := GetBeScheduler()
	return beScheduler.EnableBackend(id)
}

func (mgr *backendMgr) ListBackend() (error, []BioflowBackendInfo) {
	beScheduler := GetBeScheduler()
	return beScheduler.GetBackendListInfo()
}
