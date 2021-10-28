/* 
 Copyright (c) 2019 XTAO technology <www.xtaotech.com>
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
    "strconv"
    "github.com/xtao/bioflow/message"
)

type IOPattern int8

func (p IOPattern)String() string {
    switch int(p) {
        case 1:
            return "SEQUENTIAL"
        case 2:
            return "RANDOM"
        case 3:
            return "HYBRID"
        default:
            return "INVALID"
    }
}

const (
    IO_INVALID IOPattern = 0
    IO_SEQ IOPattern = 1
    IO_RAND IOPattern = 2
    IO_HYBRID IOPattern = 3
)

type RWPattern int8

func (p RWPattern)String() string {
    switch int(p) {
        case 1:
            return "READ"
        case 2:
            return "REREAD"
        case 3:
            return "WRITE"
        case 4:
            return "REWRITE"
        case 5:
            return "HYBRID"
        default:
            return "INVALID"
    }
}

const (
    RW_READ RWPattern = 1
    RW_REREAD RWPattern = 2
    RW_WRITE RWPattern = 3
    RW_REWRITE RWPattern = 4
    RW_HYBRID RWPattern = 5
)

type IsoLevel int8
func (l IsoLevel)String() string {
    switch int(l) {
        case 1:
            return "STAGE"
        case 2:
            return "IO"
        default:
            return "INVALID"
    }
}

const (
    ISO_STAGE IsoLevel = 1
    ISO_IO IsoLevel = 2
)

type EphLevel int8
func (l EphLevel)String() string {
    switch int(l) {
        case 1:
            return "STAGE"
        case 2:
            return "JOB"
        case 3:
            return "USER"
        default:
            return "INVALID"
    }
}

const (
    EPH_STAGE EphLevel = 1
    EPH_JOB EphLevel = 2
    EPH_USER EphLevel = 3
)

type IOAttr struct {
    ioPattern IOPattern
    rwPattern RWPattern
    isoLevel IsoLevel
    ephLevel EphLevel
    ephPattern string
    ephMap map[string]EphLevel
    workingSet float32
    largeSmallFiles bool
}

func (ia *IOAttr)IORandom() bool {
    return ia.ioPattern == IO_RAND
}

func (ia *IOAttr)IOSeq() bool {
    return ia.ioPattern == IO_SEQ
}

func (ia *IOAttr)IOHybrid() bool {
    return ia.ioPattern == IO_HYBRID
}

func (ia *IOAttr)Write() bool {
    return ia.rwPattern == RW_WRITE
}

func (ia *IOAttr)ReWrite() bool {
    return ia.rwPattern == RW_REWRITE
}

func (ia *IOAttr)Read() bool {
    return ia.rwPattern == RW_READ
}

func (ia *IOAttr)ReRead() bool {
    return ia.rwPattern == RW_REREAD
}

func (ia *IOAttr)Hybrid() bool {
    return ia.rwPattern == RW_HYBRID
}

func (ia *IOAttr)LargeSmallFiles() bool {
    return ia.largeSmallFiles
}

func (ia *IOAttr)SetIOPattern(ioPattern string) {
    switch strings.ToUpper(ioPattern) {
        case message.IO_PATTERN_SEQUENTIAL:
            ia.ioPattern = IO_SEQ
        case message.IO_PATTERN_RANDOM:
            ia.ioPattern = IO_RAND
        case message.IO_PATTERN_HYBRID:
            ia.ioPattern = IO_HYBRID
        default:
            ia.ioPattern = 0
    }
}

func (ia *IOAttr)SetRWPattern(rwPattern string) {
    switch strings.ToUpper(rwPattern) {
        case message.RW_PATTERN_READ:
            ia.rwPattern = RW_READ
        case message.RW_PATTERN_REREAD:
            ia.rwPattern = RW_REREAD
        case message.RW_PATTERN_WRITE:
            ia.rwPattern = RW_WRITE
        case message.RW_PATTERN_REWRITE:
            ia.rwPattern = RW_REWRITE
        case message.RW_PATTERN_HYBRID:
            ia.rwPattern = RW_HYBRID
        default:
            ia.rwPattern = 0
    }
}

func (ia *IOAttr)SetIsoLevel(isoLevel string) {
    switch strings.ToUpper(isoLevel) {
        case message.ISO_LEVEL_STAGE:
            ia.isoLevel = ISO_STAGE
        case message.ISO_LEVEL_IO:
            ia.isoLevel = ISO_IO
        default:
            ia.isoLevel = 0
    }
}

func (ia *IOAttr)IsoLevel() IsoLevel {
    return ia.isoLevel
}

func ParseUserEphLevel(ephLevel string) EphLevel {
    var ephVal EphLevel = 0
    switch strings.ToUpper(ephLevel) {
        case message.EPH_LEVEL_STAGE:
            ephVal = EPH_STAGE
        case message.EPH_LEVEL_JOB:
            ephVal = EPH_JOB
        case message.EPH_LEVEL_USER:
            ephVal = EPH_USER
    }

    return ephVal
}

func (ia *IOAttr)SetEphLevel(ephLevel string) {
    ia.ephLevel = ParseUserEphLevel(ephLevel)
}

func (ia *IOAttr)EphLevel() EphLevel {
    return ia.ephLevel
}

func (ia *IOAttr)SetEphPattern(pattern string) {
    ia.ephPattern = pattern
}

func (ia *IOAttr)EphPattern() string {
    return ia.ephPattern
}

func (ia *IOAttr)SetEphMap(ephMap map[string]string) {
    if ephMap != nil {
        ia.ephMap = make(map[string]EphLevel)
        for key, val := range ephMap {
            ia.ephMap[key] = ParseUserEphLevel(val)
        }
    }
}

func (ia *IOAttr)EphMap() map[string]EphLevel {
    return ia.ephMap
}

func ParseStringValueToMB(value string) (float32, error) {
    units := []string{"K", "M", "G"}
    ratios := []float32{0.001, 1, 1000}
    strValue := strings.ToUpper(value)
    var ratio float32 = 1
    for index, unit := range units {
        if strings.HasSuffix(strValue, unit) {
            strValue = strings.TrimSuffix(strValue, unit)
            ratio = ratios[index]
            break
        }
    }
    strValue = strings.TrimSpace(strValue)
    if f, err := strconv.ParseFloat(strValue, 32); err == nil {
        return float32(f) * ratio, nil
    } else {
        return 0, err
    }
}

func (ia *IOAttr)SetWorkingSet(workingSet string) {
    val, err := ParseStringValueToMB(workingSet)
    if err == nil {
        ia.workingSet = val
    }
}

func (ia *IOAttr)SetLargeSmallFiles(ls bool) {
    ia.largeSmallFiles = ls
}
