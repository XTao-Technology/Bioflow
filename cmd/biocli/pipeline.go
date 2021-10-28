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
	"fmt"
    "os"
    "io/ioutil"
    "strings"
    "encoding/json"

    . "github.com/xtao/bioflow/message"
    . "github.com/xtao/bioflow/client"
    . "github.com/xtao/bioflow/scheduler/common"
    . "github.com/xtao/bioflow/common"
    "github.com/xtao/dsl-go/engine"
    "github.com/xtao/dsl-go/wdl/scanner"
)


type pipelineCommand struct {
	client *BioflowClient
}

func newPipelineCommand(c *BioflowClient) *pipelineCommand {
	return &pipelineCommand{
		client: c,
	}
}

func PrintSpace(spaceNum int) {
    for i := 0; i < spaceNum; i ++ {
        fmt.Printf(" ")
    }
}

func ShowPipeline(pipeline *BioflowPipelineInfo, showDetails bool) {
    fmt.Printf("Pipeline %s: \n", pipeline.Name)
    fmt.Printf("   Type: %s\n", pipeline.Type)
    fmt.Printf("   Items: %d\n", pipeline.ItemCount)
    fmt.Printf("   State: %s\n", pipeline.State)
    fmt.Printf("   WorkDir: %s\n", pipeline.WorkDir)
    fmt.Printf("   IgnoreDir: %s\n", pipeline.IgnoreDir)
    fmt.Printf("   Parent: %s\n", pipeline.Parent)
    fmt.Printf("   LastVersion: %s\n", pipeline.LastVersion)
    fmt.Printf("   Version: %s\n", pipeline.Version)
    if pipeline.HDFSWorkDir != "" {
        fmt.Printf("   HDFSWorkDir: %s\n", pipeline.HDFSWorkDir)
    }
    fmt.Printf("   Description: %s\n", pipeline.Description)
    fmt.Printf("   Owner: %s\n", pipeline.Owner)
    if pipeline.InputMap != nil && showDetails {
        fmt.Printf("   InputMap: \n")
        for key, value := range pipeline.InputMap {
            fmt.Printf("     %s:%s \n", key, value)
        }
    }

    if pipeline.WorkflowFile != "" {
        fmt.Printf("   WorkflowFile: %s\n", pipeline.WorkflowFile)
    }

    if showDetails {
        fmt.Printf("   Items: \n")
        for i := 0; i < len(pipeline.Items); i ++ {
            ShowPipelineItem(&pipeline.Items[i], 5, true)
        }
    }
}

func ShowPipelineAllVersions(pipeline []BioflowPipelineInfo) {
    fmt.Printf("Got %d versions of the Pipeline:\n", len(pipeline))
    fmt.Println("")
    for i := 0; i < len(pipeline); i ++ {
        ShowPipeline(&pipeline[i], false)
        fmt.Println("")
    }
}

func ShowPipelines(pipelines []BioflowPipelineInfo) {
    fmt.Printf("Got %d Pipelines:\n", len(pipelines))
    fmt.Println("")
    for i := 0; i < len(pipelines); i ++ {
        ShowPipeline(&pipelines[i], false)
        fmt.Println("")
    }
}

func ValidateBuildPipelineJSON(file string, srcDir string, isYaml bool) (string, bool) {
    jsonFile := file
    /*convert from yaml file to JSON file*/
    if isYaml {
        var err error
        jsonFile, err = YamlFileToJSON(file)
        if err != nil {
            fmt.Printf("Fail to convert from yaml to JSON: %s\n",
                err.Error())
            os.Exit(-1)
        }
    }
    raw, err := ioutil.ReadFile(jsonFile)
    if err != nil {
        fmt.Printf("Fail to read pipeline file %s: %s\n",
            file, err.Error())
        os.Exit(-1)
    }
    var pipelineJson PipelineJSONData
    err = json.Unmarshal(raw, &pipelineJson)
    if err != nil {
        fmt.Printf("Fail to parse pipeline file %s: %s\n",
            file, err.Error())
        os.Exit(-1)
    }

    validatedJSONFile := jsonFile
    deleteFile := false
    switch strings.ToUpper(pipelineJson.Type) {
        case "", PIPELINE_TYPE_BIOBPIPE:
        case PIPELINE_TYPE_WDL:
            if pipelineJson.Wdl.WorkflowFile == "" {
                fmt.Printf("Must specify the workflow file for WDL pipeline\n")
                os.Exit(-1)
            }

            if srcDir == "" {
                fmt.Printf("Need specify source directory for WDL pipeline\n")
                os.Exit(-1)
            }

            /*Validate the syntax of the source files first*/
            wdlSourceFile := srcDir + "/" + pipelineJson.Wdl.WorkflowFile
            err := engine.Validate(wdlSourceFile)
            if err != nil {
                // The error may be a Error List, so parse them as list
                if errList, ok := err.(scanner.ErrorList); ok {
                    for _, e := range errList {
                        fmt.Printf("Parse sources errors:\n")
                        fmt.Printf("%s\n", e)
                    }
                } else {
                    fmt.Printf("Build sources error: %s\n", err.Error())
                }
                os.Exit(-1)
            }


            tmpFileName := pipelineJson.Name + ".zipfile"
            writer := NewZipWriter(srcDir, tmpFileName, true)
            err = writer.Write()
            if err != nil {
                fmt.Printf("Fail to zip wdl source code: %s\n",
                    err.Error())
                os.Exit(-1)
            }
            raw, err := ioutil.ReadFile(tmpFileName)
            if err != nil {
                fmt.Printf("Fail to read archive file %s: %s\n",
                    tmpFileName, err.Error())
                os.Exit(-1)
            }
            pipelineJson.Wdl.Blob = raw
            newJsonData, _ := json.Marshal(&pipelineJson)
            if !isYaml {
                validatedJSONFile = file + ".validated"
            }
            err = ioutil.WriteFile(validatedJSONFile, newJsonData, 0644)
            if err != nil {
                fmt.Printf("Fail to persist the pipeline json: %s\n",
                    err.Error())
                os.Exit(-1)
            }
            os.Remove(tmpFileName)
            deleteFile = true
    }

    return validatedJSONFile, deleteFile
}

func (cmd *pipelineCommand) Add(file string, srcDir string, isYaml bool) {
    validatedJSONFile, deleteOnSucceed := ValidateBuildPipelineJSON(file, srcDir, isYaml)
	err := cmd.client.AddPipelineFile(validatedJSONFile)
	if err != nil {
		fmt.Printf("The pipeline added failure: %s\n", 
			err.Error())
        if deleteOnSucceed  || isYaml {
            os.Remove(validatedJSONFile)
        }
		os.Exit(1)
	} else {
        if deleteOnSucceed  || isYaml{
            os.Remove(validatedJSONFile)
        }
		fmt.Printf("The pipeline added success\n")
	}
}

func (cmd *pipelineCommand) Delete(name string) {
	if name == "" {
		fmt.Printf("You must specify a valid pipeline name\n")
		os.Exit(1)
	}

	err := cmd.client.DeletePipeline(name)
	if err != nil {
		fmt.Printf("Fail to delete pipeline %s: %s\n",
			name, err.Error())
		os.Exit(1)
	} else {
		fmt.Printf("Succeed to delete pipeline %s\n",
			name)
	}
}

func (cmd *pipelineCommand) Version(name string) {
    err, pipelines := cmd.client.ListPipelineAllVersions(name)
    if err != nil {
        fmt.Printf("Fail to list pipelines: %s\n",
            err.Error())
        os.Exit(1)
    } else {
        ShowPipelineAllVersions(pipelines)
    }
}

func (cmd *pipelineCommand) List() {
	err, pipelines := cmd.client.ListPipelines()
	if err != nil {
		fmt.Printf("Fail to list pipelines: %s\n",
			err.Error())
		os.Exit(1)
	} else {
		ShowPipelines(pipelines)
	}
}

func (cmd *pipelineCommand) Info(name string, version string) {
	if name == "" {
		fmt.Printf("You must specify a valid pipeline name\n")
		os.Exit(1)
	}

	err, pipelineInfo := cmd.client.GetPipelineInfo(name, version)
	if err != nil {
		fmt.Printf("Fail to get pipeline %s info: %s\n",
			name, err.Error())
		os.Exit(1)
	} else {
		ShowPipeline(pipelineInfo, true)
	}
}

func (cmd *pipelineCommand) Update(file string, srcDir string, isForce bool, isYaml bool) {
    validatedJSONFile, deleteOnSucceed := ValidateBuildPipelineJSON(file, srcDir, isYaml)
	err := cmd.client.UpdatePipelineFile(validatedJSONFile, isForce)
	if err != nil {
		fmt.Printf("The pipeline updated failure: %s\n", 
			err.Error())
        if deleteOnSucceed  || isYaml {
            os.Remove(validatedJSONFile)
        }
		os.Exit(1)
	} else {
        if deleteOnSucceed || isYaml {
            os.Remove(validatedJSONFile)
        }
		fmt.Printf("The pipeline updated success\n")
	}
}

func (cmd *pipelineCommand) Clone(src string, dst string) {
        if src == "" {
            fmt.Printf("You must specify source pipeline\n")
            os.Exit(1)
        }
        if dst == "" {
            fmt.Printf("You must specify destination pipeline\n")
            os.Exit(1)
        }

        err := cmd.client.ClonePipeline(src, dst)
        if err != nil {
            fmt.Printf("Fail to clone pipeline %s to %s: %s\n",
                src, dst, err.Error())
		    os.Exit(1)
        } else {
            fmt.Printf("Succeed to clone pipeline %s to %s\n",
                src, dst)
        }
}

func (cmd *pipelineCommand) Dump(name string, version string, file string, isYaml bool) {
    if name == "" {
        fmt.Printf("You must specify pipeline name\n")
        os.Exit(1)
    }

    if file == "" {
        fmt.Printf("You must specify file name\n")
        os.Exit(1)
    }

    err := cmd.client.DumpPipeline(name, version, file, isYaml)
    if err != nil {
        fmt.Printf("Fail to dump pipeline %s/%s: %s\n",
            name, version, err.Error())
        os.Exit(1)
    } else {
        fmt.Printf("Success to dump pipeline %s/%s to file %s\n",
            name, version, file)
    }
}
