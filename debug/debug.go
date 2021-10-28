/* 
 Copyright (c) 2018 XTAO technology <www.xtaotech.com>
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

package debug

import (
    "os"
    "errors"
    "strings"
    "runtime"
	"runtime/pprof"

    . "github.com/xtao/bioflow/common"
    . "github.com/xtao/bioflow/scheduler"
    . "github.com/xtao/bioflow/storage"
)

var (
	CpuProfile = "/mnt/mesos/sandbox/bioflow.cpuprof"
	MemProfile = "/mnt/mesos/sandbox/bioflow.memprof"
)

var debugCPUProf *os.File = nil
var debugMemProf *os.File = nil

func DoProfileOperation(op string) error {
	switch strings.ToUpper(op) {
	    case "STARTCPUPROFILE":
		    if debugCPUProf == nil {
			    if f, err := os.Create(CpuProfile); err != nil {
				    Logger.Errorf("Failed to create cpu profile file %s, err: %s\n",
                        CpuProfile, err.Error())
                    return errors.New("can't create cpu profile")
			    } else {
				    Logger.Info("Start cpu profile\n")
				    pprof.StartCPUProfile(f)
                    debugCPUProf = f
			    }
		    }
        case "STOPCPUPROFILE":
		    if debugCPUProf != nil {
			    pprof.StopCPUProfile()
			    debugCPUProf.Close()
			    debugCPUProf = nil
			    Logger.Info("Stop cpu profile")
		    }
        case "DOMEMPROFILE":
		    if debugMemProf == nil {
			    if f, err := os.Create(MemProfile); err != nil {
				    Logger.Errorf("Failed to create memory %s, err: %s\n",
                        MemProfile, err.Error())
                    return errors.New("can't create mem profile")
			    } else {
				    Logger.Info("Do memory profile\n")
				    debugMemProf = f
				    runtime.GC()
				    pprof.WriteHeapProfile(f)
				    f.Close()
				    debugMemProf = nil
			    }
            }
        case "RESETSTATS":
            scheduler := GetScheduler()
            if scheduler != nil {
                scheduler.ResetStats()
            }
            storageMgr := GetStorageMgr()
            if storageMgr != nil {
                storageMgr.ResetStats()
            }
	    default:
		    Logger.Errorf("Unable to support such operation: %s\n", op)
            return errors.New("the operation not supported")
	}

    return nil
}
