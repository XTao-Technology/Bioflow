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
    "strings"
    )

type DataVol struct {
    vol string
    cluster string
    /*support dir map*/
    path string

    conPath string
    isHDFSVol bool
}

func (vol *DataVol)Vol() string {
    return vol.vol
}

func (vol *DataVol)SetAsHDFSVol() {
    vol.isHDFSVol = true
}

func (vol *DataVol)IsHDFSVol() bool {
    return vol.isHDFSVol
}

func (vol *DataVol)Cluster() string {
    return vol.cluster
}

func (vol *DataVol)Path() string {
    return vol.path
}

func (vol *DataVol)ContainerPath() string {
    return vol.conPath
}

func (vol *DataVol)VolURI() string {
    if vol.cluster != "" {
        return vol.vol + "@" + vol.cluster
    } else {
        return vol.vol
    }
}

func (vol *DataVol)FromVolURI(volURI string) {
    volItems := strings.Split(volURI, "@")
    if len(volItems) >= 2 {
        vol.vol = volItems[0]
        vol.cluster = volItems[1]
    } else if len(volItems) == 1 {
        vol.vol = volItems[0]
        vol.cluster = ""
    }
}

func NewDataVolMap(volURI string, path string, conPath string) *DataVol {
    dataVol := &DataVol{
                conPath: conPath,
                path: path,
            }
    dataVol.FromVolURI(volURI)

    return dataVol
}
