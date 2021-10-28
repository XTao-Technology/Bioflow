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
    "os"
    "fmt"
    "strconv"
    "strings"
)

var EtcdEndPoints []string

type BioflowEnvSetting struct {
    Logfile string
    Configfile string
    Queue int
    EtcdEndPoints []string
    MesosMaster string
}

func EnvUtilsParseEnvSetting() *BioflowEnvSetting {
    envSetting := &BioflowEnvSetting {
                    Logfile: "/var/log/bioflow.log",
                    Configfile: "bioflow.json",
                }
    configFile := os.Getenv("CONFIGFILE")
    if configFile != "" {
        envSetting.Configfile = configFile
    }
    mesosMaster := os.Getenv("MESOS_MASTER")
    if mesosMaster != "" {
        envSetting.MesosMaster = mesosMaster
    }
    etcdHosts := os.Getenv("ETCD_CLUSTER")
    if etcdHosts == "" {
        etcdHosts = "http://localhost:2379"
    }
    queue := os.Getenv("QUEUE")
    if queue == "" {
        envSetting.Queue = 0
    } else {
        queueNum, err := strconv.Atoi(queue)
        if err != nil {
            Logger.Errorf("Queue env %s set wrong: %s\n",
                queue, err.Error())
            envSetting.Queue = 0
        } else {
            envSetting.Queue = queueNum
        }
    }
    logFile := os.Getenv("LOGFILE")
    if logFile != "" {
        envSetting.Logfile = logFile
    } else {
        envSetting.Logfile = fmt.Sprintf("/var/log/bioflow-queue%d.log",
            envSetting.Queue)
    }
    hostItems := strings.Split(etcdHosts, ",")
    endPoints := make([]string, 0)
    for i := 0; i < len(hostItems); i ++ {
        if hostItems[i] != "" {
            endPoints = append(endPoints, hostItems[i])
        }
    }
    envSetting.EtcdEndPoints = endPoints
    EtcdEndPoints = endPoints
    return envSetting
}