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
package main

import (
	"io/ioutil"
	"encoding/json"
	"fmt"
	"os"

    . "github.com/xtao/bioflow/common"
)

type envCommand struct {
}

func newEnvCommand() *envCommand {
	return &envCommand{
	}
}

func (cmd *envCommand) Load() (error, map[string]interface{}) {
	m := make(map[string]interface{})
	_, err := os.Stat(ClientConfFile())
	if os.IsNotExist(err) {
		content := []byte("{}")
		err = ioutil.WriteFile(ClientConfFile(), content, 644)
		return err, m
	}

	o, err := ioutil.ReadFile(ClientConfFile())
	if err != nil {
		return err, m
	}

	err = json.Unmarshal(o, &m)
	return err, m
}

func (cmd *envCommand) Set(key string, val string) {
	tmpfile, err := ioutil.TempFile(ClientConfDir(), "bioflow_env")
	if err != nil {
		fmt.Println("Fail to create temp env file", err)
		return
	}
	defer os.Remove(tmpfile.Name())
	err, m := cmd.Load()
	if err != nil {
		fmt.Println("Load config file error!", err)
		return
	}

	m[key] = val
	res, err := json.Marshal(m)
    if err != nil {
		fmt.Println("Fail to encode user config!", err)
		return
    }

	if _, err := tmpfile.Write(res); err != nil {
		fmt.Println("Write temp config file error!", err)
		return
	}

	err = os.Rename(tmpfile.Name(), ClientConfFile())
    if err != nil {
        fmt.Println("Fail to rename file: %s\n", err.Error())
    }

	tmpfile.Close()
}

func (cmd *envCommand) Get(key string) {
	err, m := cmd.Load()
	if err != nil {
		fmt.Println("Load config file error!", err)
		return
	}

	if key != "all" {
		if _, ok := m[key]; ok {
			fmt.Println(m[key].(string))
		} else {
			fmt.Printf("Unknown env key.\n")
		}
	} else {
		res, _ := json.Marshal(m)
		fmt.Println(string(res))
	}
}
