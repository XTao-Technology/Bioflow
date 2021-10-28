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
    "encoding/json"
)

/*
 * Bioflow mantains user statistics for billing:
 * 1) Job count
 * 2) Total cpu time: 1 cpu * 1 minutes
 * 3) Total memory: 1 GB * 1 minutes
 */
type userStatsInfo struct {
    JobCount uint64
    TotalCPU float64       /*1CPU * 1 minute*/
    TotalMem float64       /*1GB * 1 minute*/
}

func (stats *userStatsInfo)UpdateResourceUsage(duration float64,
    cpu float64, mem float64) {
    stats.TotalCPU += duration * cpu
    stats.TotalMem += duration * mem
}

func (stats *userStatsInfo)FromJSON(jsonData string) error {
    err := json.Unmarshal([]byte(jsonData), stats)
    if err != nil {
        return err
    }

    return nil
}

func (stats *userStatsInfo)ToJSON() (error, string) {
    body, err := json.Marshal(stats)
    if err != nil {
        return err, ""
    }

    return nil, string(body)
}

