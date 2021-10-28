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
    "encoding/json"
    "io/ioutil"

    . "github.com/xtao/bioflow/common"
    )

func LoadConfigFromJSONFile(file string) *BioflowConfig {
    raw, err := ioutil.ReadFile(file)
    if err != nil {
        fmt.Println(err.Error())
        return nil
    }

    bioConfig := BioflowConfig{
            DBConfig: DBServiceConfig {
                User: "bioflow",
                Password: "bioflow",
                DBName: "bioflow",
                Driver: "postgres",
            },
            LoggerConfig: LoggerConfig {
                Logfile: "/var/log/bioflow.log",
            },
        }
    err = json.Unmarshal(raw, &bioConfig)
    if err != nil {
        fmt.Println("Fail to load the JSON config")
        fmt.Println(err.Error())
        return nil
    }

    return &bioConfig
}
