package common

import (
    "os"
    "fmt"
    "github.com/Sirupsen/logrus"
    )

var StoneLogger *logrus.Entry = nil
var globalLogger = logrus.New()

const(
    LOG_LEVEL_INFO int = 0
    LOG_LEVEL_DEBUG int = 1
    LOG_LEVEL_WARN int = 2
    LOG_LEVEL_ERROR int = 3
)

func XLoggerSetLevel(level int) {
    switch level {
        case LOG_LEVEL_DEBUG:
            logrus.SetLevel(logrus.DebugLevel) 
            globalLogger.Level = logrus.DebugLevel
        case LOG_LEVEL_INFO:
            logrus.SetLevel(logrus.InfoLevel) 
            globalLogger.Level = logrus.InfoLevel
        case LOG_LEVEL_ERROR:
            logrus.SetLevel(logrus.ErrorLevel) 
            globalLogger.Level = logrus.ErrorLevel
        case LOG_LEVEL_WARN:
            logrus.SetLevel(logrus.WarnLevel)
            globalLogger.Level = logrus.WarnLevel
        default:
            fmt.Printf("Unknown log level %d\n",
                level)
    }
}

var logFile *os.File = nil
func XLoggerInit(logFilePath string, level int) *logrus.Logger {
    logrus.SetFormatter(&logrus.JSONFormatter{})
    var err error = nil
    if logFile == nil {
        logFile, err = os.OpenFile(logFilePath, os.O_RDWR | os.O_CREATE | os.O_APPEND, 0777)
        if err != nil {
            /*use the log file in current directory*/
            logFile, err = os.OpenFile("./biocli.log",
                os.O_RDWR | os.O_CREATE | os.O_APPEND, 0777)
        }
    }
    
    if err == nil {
        globalLogger.Out = logFile
    }

    XLoggerSetLevel(level)
    StoneLogger = globalLogger.WithFields(logrus.Fields{
                "Module": "XStone",
            })

    return globalLogger
}

func XLogger() *logrus.Logger {
    return globalLogger
}


