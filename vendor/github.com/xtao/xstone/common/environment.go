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
	"strings"
)

type BioflowEnvSetting struct {
	Logfile        string
	Configfile     string
	XstoneRestPort string
	EtcdEndPoints  []string
	PluginVolume   string
}

func EnvUtilsParseEnvSetting() *BioflowEnvSetting {
	envSetting := &BioflowEnvSetting{
		Logfile:    "/var/log/xstone.log",
		Configfile: "xstone.json",
	}
	logFile := os.Getenv("LOGFILE")
	if logFile != "" {
		envSetting.Logfile = logFile
	}
	configFile := os.Getenv("CONFIGFILE")
	if configFile != "" {
		envSetting.Configfile = configFile
	}
	etcdHosts := os.Getenv("ETCD_CLUSTER")
	if etcdHosts == "" {
		etcdHosts = "http://localhost:2380"
	}

	xstonePort := os.Getenv("XstoneRestAddr")
	if xstonePort == "" {
		envSetting.XstoneRestPort = "8188"
	}

	pluginVolume := os.Getenv("StorageManagerPluginVolume")
	if pluginVolume == "" {
		envSetting.PluginVolume = "/var/run/storage/mnt/xtao"
	}

	hostItems := strings.Split(etcdHosts, ",")
	endPoints := make([]string, 0, len(hostItems))
	for i := 0; i < len(hostItems); i++ {
		if hostItems[i] != "" {
			endPoints = append(endPoints, hostItems[i])
		}
	}
	envSetting.EtcdEndPoints = endPoints

	return envSetting
}
