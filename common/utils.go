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
    "bufio"
    "fmt"
    "regexp"
    "io/ioutil"
    "encoding/json"
    "os"
    "os/user"
    "strings"
    "syscall"
    "time"
    "errors"

    etcd "github.com/coreos/etcd/client"
    )

func IsConnectionError(err error) bool {
    reg := regexp.MustCompile("connection?")

    return reg.FindStringIndex(err.Error()) != nil
}

func IndentPrint (indent bool, format string, a ...interface{}) {
    if indent == true {
        fmt.Printf("    ")
    }
    fmt.Printf(format, a ...)
}

const (
    GET_JOBID_FROM_MEM int = 1
    GET_JOBID_FROM_DB int = 1 << 1
    GET_JOBID_FROM_ALL int = GET_JOBID_FROM_MEM | GET_JOBID_FROM_DB
)

const (
    BIOFLOW_TIME_LAYOUT = time.RFC3339
    BIOFLOW_TIME_USERINPUT = "2006-01-02 15:04:05"
)

func TimeUtilsUserInputToBioflowTime(userTime string) (time.Time, error) {
    loc, err := time.LoadLocation("Local")
    if err != nil {
        return time.Now(), errors.New("No local location found")
    }

    tm, err := time.ParseInLocation(BIOFLOW_TIME_USERINPUT, userTime, loc)
    if err != nil {
        errMsg := fmt.Sprintf("User input time format not correct: %s",
            err.Error())
        return tm, errors.New(errMsg)
    }

    return tm, nil
}

func GenBackendId(t string, server string, port string) string{
    return fmt.Sprintf("%s:%s:%s", t, server, port)
}

/*
 * If biocli gives endpoints, use endpoints. Otherwise,
 * try default client config file; if all tries fail,
 * just uses http://localhost:2379 as endpoints
 */
func ParseEndpoints (endpoints string) []string {
    if endpoints == "" {
        _, err := os.Stat(ClientConfFile())
        if err != nil {
            endpoints = BIOFLOW_DEFAULT_ENDPOINT
        } else {
            raw, err := ioutil.ReadFile(ClientConfFile())
            if err == nil {
                var config map[string]interface{}
                err = json.Unmarshal(raw, &config)
                if err == nil {
                    svrs, ok := config["endpoints"]
                    if ok {
                        endpoints = svrs.(string)
                    }
                }
            }
        }
    }

    return strings.Split(endpoints, ",")
}

func ParseBioflowServer(s string) string {
    var server string

    server = BIOFLOW_DEFAULT_SERVER

	if s != "" {
		server = s
	} else {
		if s == "" {
			_, err := os.Stat(ClientConfFile())
			if err == nil {
				raw, err := ioutil.ReadFile(ClientConfFile())
				if err == nil {
					var config map[string]interface{}
					err = json.Unmarshal(raw, &config)
					if err == nil {
						v, ok := config["server"]
						if ok {
							server = v.(string)
						}
					}
				}
			}
		}
	}
    return server
}

func GetClientUserInfo() *UserAccountInfo {
    curUser, err := user.Current()
    if err != nil {
        fmt.Printf("Fail to get current user: %s\n",
            err.Error())
        return &UserAccountInfo{}
    }

    info := &UserAccountInfo {
        Username: curUser.Username,
        Uid: curUser.Uid,
        Gid: curUser.Gid,
        Umask: fmt.Sprintf("%d", syscall.Umask(0)),
    }

    return info
}

func GetUserLogFile() string {
    curUser, err := user.Current()
    if err != nil {
        fmt.Printf("Fail to get current user: %s\n",
            err.Error())
        return "./biocli.log"
    }

    return curUser.HomeDir + "/biocli.log"
}

func KeyNotFound(err error) bool {
	if err != nil {
		if etcdError, ok := err.(etcd.Error); ok {
			if etcdError.Code == etcd.ErrorCodeKeyNotFound {
				return true
			}
		}
	}
	return false
}

func GetTimeStringFromRange(timeRange string) (error, string, string) {
	var fromTime string = ""
	var toTime string = ""
	var err error

	if timeRange != "" {
		timeFormatError := false
		if strings.HasSuffix(timeRange, "]") {
			timeRange = strings.TrimRight(timeRange, "]")
		} else {
			timeFormatError = true
		}

		if strings.HasPrefix(timeRange, "[") {
			timeRange = strings.TrimLeft(timeRange, "[")
		} else {
			timeFormatError = true
		}

		if timeFormatError {
			err = errors.New("Time range input invalid, format: \"[2017-06-01 12:00:00, 2017-07-01 12:00:00]\"")
			return err, "", ""
		}

		timeList := strings.Split(timeRange, ",")
		fromTime = strings.TrimSpace(timeList[0])
		toTime = strings.TrimSpace(timeList[1])

		if fromTime == "-" && toTime == "-" {
			err = errors.New("Please specify the range!")
			return err, "", ""
		}

		if fromTime == "-" || fromTime == "" {
			fromTime = ""
		}

		if toTime == "-" || toTime == "" {
			toTime = ""
		}
	}

	//fmt.Printf("fromTime=%s, toTime=%s", fromTime, toTime)
	return nil, fromTime, toTime
}

func UtilsOpenFile(file string) (*os.File, error) {
    _, err := os.Stat(file)
    if err != nil {
        if os.IsNotExist(err) {
            return os.Create(file)
        } else {
            return nil, err
        }
    } else {
        return os.OpenFile(file, os.O_RDWR, 0666)
    }
}

func UtilsWriteFile(f *os.File, content string) error {
    defer f.Close()
    w := bufio.NewWriter(f)
    _, err := w.WriteString(content)
    if err != nil {
        return err
    }
    w.Flush()
    return nil
}
