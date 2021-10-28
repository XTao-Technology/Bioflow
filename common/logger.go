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
    "github.com/Sirupsen/logrus"
    xcommon "github.com/xtao/xstone/common"
    )

var Logger *logrus.Entry = nil
var SchedulerLogger *logrus.Entry = nil
var BackendLogger *logrus.Entry = nil
var StorageLogger *logrus.Entry = nil
var BuilderLogger *logrus.Entry = nil
var ParserLogger *logrus.Entry = nil
var ClusterLogger *logrus.Entry = nil
var ServerLogger *logrus.Entry = nil
var DBLogger *logrus.Entry = nil
var ConfLogger *logrus.Entry = nil
var WomLogger *logrus.Entry = nil
var PhysicalLogger *logrus.Entry = nil

func LoggerInit(config *LoggerConfig) error {
    globalLogger := xcommon.XLoggerInit(config.Logfile, config.LogLevel)

    Logger = globalLogger.WithFields(logrus.Fields{
            "Module": "Bioflow",
            })
    SchedulerLogger = globalLogger.WithFields(logrus.Fields{
            "Module": "Scheduler",
            })
    BackendLogger = globalLogger.WithFields(logrus.Fields{
            "Module": "ScheduleBackend",
            })
    StorageLogger = globalLogger.WithFields(logrus.Fields{
            "Module": "Storage",
            })
    BuilderLogger = globalLogger.WithFields(logrus.Fields{
            "Module": "GraphBuilder",
            })
    ParserLogger = globalLogger.WithFields(logrus.Fields{
            "Module": "Parser",
            })
    DBLogger = globalLogger.WithFields(logrus.Fields{
            "Module": "Database",
            })
    ConfLogger = globalLogger.WithFields(logrus.Fields{
            "Module": "ConfDB",
            })
    ClusterLogger = globalLogger.WithFields(logrus.Fields{
            "Module": "ClusterController",
            })
    ServerLogger = globalLogger.WithFields(logrus.Fields{
            "Module": "RestServer",
            })
    WomLogger = globalLogger.WithFields(logrus.Fields{
            "Module": "Wom",
            })
    PhysicalLogger = globalLogger.WithFields(logrus.Fields{
            "Module": "Physical",
            })

    return nil
}

