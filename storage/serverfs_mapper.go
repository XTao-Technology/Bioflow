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
package storage

import (
    "errors"

    . "github.com/xtao/bioflow/common"
)

/*
 * In order to schedule on a large compute cluster, it is not practical
 * to require that all servers mount all the storage volumes. so we will
 * divide compute nodes to several regions and require that same region
 * to mount all volumes of same storage cluster. It is practical because
 * that:
 * 1) the number of volumes of a storage cluster will not grow too large
 */
 func BuildClusterLevelConstraint(volURI string) (error, string, string) {
    /*
     * currently we only return volname:enabled as attributed key-value pair
     */
    if volURI == "" {
        return errors.New("Invalid volume " + volURI),
            "", "" 
    }
    err, cluster, _ := FSUtilsParseVolURI(volURI)
    if err != nil {
        StorageLogger.Errorf("Can't parse vol URI %s: %s\n",
            volURI, err.Error())
        return err, "", ""
    }

    if cluster == "" {
        /*
         * The volume not specified with cluster name will not
         * have constraints
         */
        return nil, "", ""
    } else {
        return nil, cluster, "enabled"
    }
 }

