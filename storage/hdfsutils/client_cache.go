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
package hdfsutils

import (
	"errors"
	"os"
    "sync"

	"github.com/colinmarc/hdfs"
)

var cachedClients map[string]*hdfs.Client = nil
var clientMutex sync.Mutex
var enableClientCache bool = false

func getClient(namenode string) (*hdfs.Client, error) {
	if namenode == "" {
		namenode = os.Getenv("HADOOP_NAMENODE")
	}

    /*
     * The connection from client to namenode may be disconnected.
     * so we need reconnect mechanisms to enable client cache. Disable
     * it by default.
     */
    if enableClientCache {
        clientMutex.Lock()
	    if cachedClients == nil {
            cachedClients = make(map[string]*hdfs.Client)
	    } else {
            if client, ok := cachedClients[namenode]; ok {
                clientMutex.Unlock()
                return client, nil
            }
        }
        clientMutex.Unlock()
    }

	if namenode == "" && os.Getenv("HADOOP_CONF_DIR") == "" {
		return nil, errors.New("Couldn't find a namenode to connect to. You should specify hdfs://<namenode>:<port> in your paths. Alternatively, set HADOOP_NAMENODE or HADOOP_CONF_DIR in your environment.")
	}

	c, err := hdfs.New(namenode)
	if err != nil {
		return nil, err
	}

    if enableClientCache {
        clientMutex.Lock()
	    cachedClients[namenode] = c
        clientMutex.Unlock()
    }

	return c, nil
}
