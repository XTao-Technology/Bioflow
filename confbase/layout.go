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
package confbase

import (
    "fmt"
    "strconv"
    "strings"
    "errors"

) 

/*
 * The layout of bioflow configuraiton space on ETCD is as follows:
 * /  bioflow
 *            config
 *                      db
 *                      backends
 *                      rest
 *                      service
 *            security
 *                      config
 *            scheduler
 *                      config
 *            version
 *                      bioflow
 *                      images
 *           
 *
 *
 *            cluster
 *                      config
 *                      queues
 *                              queue-0
 *                                       config
 *                                       seq
 *                                       members
 *
 *
 *
 *
 * (maintained by xstone)
 * / storagemgr
 *
 *            storage
 *                      mappers
 *                               server
 *                               scheduler
 *                               container
 *                      clusters
 *                               cluster1
 *                               cluster2
 */

const (
    ETCD_KEY_CONFIG string = "/bioflow/config"
    ETCD_KEY_CONFIG_DB string = "/bioflow/config/db"
    ETCD_KEY_CONFIG_BACKENDS string = "/bioflow/config/backends"
    ETCD_KEY_CONFIG_GLOBAL string = "/bioflow/config/global"
    ETCD_KEY_CONFIG_REST string = "/bioflow/config/rest"
    ETCD_KEY_CONFIG_MAIL string = "/bioflow/config/mail"
    ETCD_KEY_CONFIG_FILEDOWNLOAD string = "/bioflow/config/filedownload"
    ETCD_KEY_CONFIG_FRONTEND string = "/bioflow/config/frontend"
    ETCD_KEY_CONFIG_PIPELINESTORE string = "/bioflow/config/pipelinestore"
    ETCD_KEY_CONFIG_PHYSICAL string = "/bioflow/config/physical"

    ETCD_KEY_CLUSTER_CONFIG string = "/bioflow/cluster/config"
    ETCD_KEY_CLUSTER_QUEUES string = "/bioflow/cluster/queues"

    ETCD_KEY_SECURITY_CONFIG string = "/bioflow/security/config"
    ETCD_KEY_SECURITY string = "/bioflow/security"
    ETCD_KEY_LDAP_CHANGE string = "/xtao/ldap/changes"

    ETCD_KEY_SCHEDULER string = "/bioflow/scheduler"
    ETCD_KEY_SCHEDULER_CONFIG string = "/bioflow/scheduler/config"

	ETCD_KEY_ADMIN string = "/bioflow/admin"
	ETCD_KEY_ADMIN_PASSWORD string = "/bioflow/admin/password"
)

/*define the name for etcd node*/
const (
    ETCD_ITEM_CONFIG string = "config"
    ETCD_ITEM_SEQ string = "seq"
    ETCD_ITEM_MEMBERS string = "members"
)

func ConfBuildQueueSeqKey(queue int) string {
    return fmt.Sprintf("%s/queue-%d/%s", ETCD_KEY_CLUSTER_QUEUES,
        queue, ETCD_ITEM_SEQ)
}

func ConfBuildQueueMemberKey(queue int) string {
    return fmt.Sprintf("%s/queue-%d/%s", ETCD_KEY_CLUSTER_QUEUES,
        queue, ETCD_ITEM_MEMBERS)
}

func ConfBuildQueueInstanceKey(queue int, seq uint64) string {
    return fmt.Sprintf("%s/queue-%d/%s/instance-%d", ETCD_KEY_CLUSTER_QUEUES,
        queue, ETCD_ITEM_MEMBERS, seq)
}

func ConfBuildQueueConfigKey(queue int) string {
    return fmt.Sprintf("%s/queue-%d/%s", ETCD_KEY_CLUSTER_QUEUES,
        queue, ETCD_ITEM_CONFIG)
}

func ConfParseQueueInstanceKeyToSeq(key string) (error, uint64) {
    if key == "" {
        return errors.New("Empty key"), 0
    }
    dirItems := strings.Split(key, "/")
    keyItems := strings.Split(dirItems[len(dirItems) - 1], "-")
    if len(keyItems) != 2 {
        return errors.New("Key not instance-num format"), 0
    }

    num, err := strconv.ParseUint(keyItems[1], 10, 64)
    if err != nil {
        return err, 0
    }

    return nil, num
}

func ConfParseBackendKeyToId(key string) (error, string) {
    if key == "" {
        return errors.New("Empty key"), ""
    }

    dirItems := strings.Split(key, "/")
	if len(dirItems) < 2 {
		return errors.New("Invalid key for backend"), ""
	}

	return nil, dirItems[len(dirItems) - 1]
}

