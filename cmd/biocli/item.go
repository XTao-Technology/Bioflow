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

    . "github.com/xtao/bioflow/message"
    . "github.com/xtao/bioflow/client"
)


type itemCommand struct {
	client *BioflowClient
}

func newItemCommand(c *BioflowClient) *itemCommand {
	return &itemCommand{
		client: c,
	}
}

func ShowPipelineItems(pipelineItems []BioflowPipelineItemInfo, showAll bool) {
    fmt.Printf("Got %d Pipeline Items:\n", len(pipelineItems))
    fmt.Println("")
    for i := 0; i < len(pipelineItems); i ++ {
        ShowPipelineItem(&pipelineItems[i], 0, showAll)
        fmt.Println("")
    }
}

func ShowPipelineItem(item *BioflowPipelineItemInfo, spaceNum int, showAll bool) {
    PrintSpace(spaceNum)
    fmt.Printf("Item %s: \n", item.Name)
    PrintSpace(spaceNum)
    if item.Owner != "" {
        PrintSpace(spaceNum)
        fmt.Printf("  Owner: %s\n", item.Owner)
    }
    if showAll {
        fmt.Printf("  Type: %s\n", item.Type)
        PrintSpace(spaceNum)
        fmt.Printf("  Cmd: %s \n", item.Cmd)
        if item.Comments != "" {
            PrintSpace(spaceNum)
            fmt.Printf("  Comments: %s \n", item.Comments)
        }
        if item.GroupPattern != "" {
            PrintSpace(spaceNum)
            fmt.Printf("  GroupPattern: %s \n", item.GroupPattern)
        }
        if item.MatchPattern != "" {
            PrintSpace(spaceNum)
            fmt.Printf("  MatchPattern: %s \n", item.MatchPattern)
        }
        if item.SortPattern != "" {
            PrintSpace(spaceNum)
            fmt.Printf("  SortPattern: %s \n", item.SortPattern)
        }
        if item.Cleanup != "" {
            PrintSpace(spaceNum)
            fmt.Printf("  Cleanup: %s \n", item.Cleanup)
        }
        if item.Filter != "" {
            PrintSpace(spaceNum)
            fmt.Printf("  Filter: %s \n", item.Filter)
        }
        if item.Image != "" {
            PrintSpace(spaceNum)
            fmt.Printf("  Image: %s \n", item.Image)
        }
        if item.OutputFile != "" {
            PrintSpace(spaceNum)
            fmt.Printf("  OuputFile: %s \n", item.OutputFile)
        }
        if item.OutputFileMap != nil && len(item.OutputFileMap) != 0 {
            PrintSpace(spaceNum)
            fmt.Printf("  OutputFileMap: {\n")
            for key, val := range item.OutputFileMap {
                PrintSpace(spaceNum)
                fmt.Printf("    %s: %s \n", key, val)
            }
            PrintSpace(spaceNum)
            fmt.Printf("  }\n")
        }

        if item.OutputDir != "" {
            PrintSpace(spaceNum)
            fmt.Printf("  OutputDir: %s \n", item.OutputDir)
        }
        if item.OutputDirTag != "" {
            PrintSpace(spaceNum)
            fmt.Printf("  OutputDirTag: %s \n", item.OutputDirTag)
        }
        if item.InputDir != "" {
            PrintSpace(spaceNum)
            fmt.Printf("  InputDir: %s \n", item.InputDir)
        }
        if item.InputDirTag != "" {
            PrintSpace(spaceNum)
            fmt.Printf("  InputDirTag: %s \n", item.InputDirTag)
        }
        if item.InputDirMapTarget != "" {
            PrintSpace(spaceNum)
            fmt.Printf("  InputDirMapTarget: %s \n", item.InputDirMapTarget)
        }
        if item.WorkDirMapTarget != "" {
            PrintSpace(spaceNum)
            fmt.Printf("  InputDirMapTarget: %s \n", item.WorkDirMapTarget)
        }
        if item.InputFile != "" {
            PrintSpace(spaceNum)
            fmt.Printf("  InputFile: %s \n", item.InputFile)
        }
        if item.InputFileTag != "" {
            PrintSpace(spaceNum)
            fmt.Printf("  InputFileTag: %s \n", item.InputFileTag)
        }
        if item.WorkDir != "" {
            PrintSpace(spaceNum)
            fmt.Printf("  WorkDir: %s \n", item.WorkDir)
        }
        if item.StorageType != "" {
            PrintSpace(spaceNum)
            fmt.Printf("  StorageType: %s \n", item.StorageType)
        }
        if item.ServerType != "" {
            PrintSpace(spaceNum)
            fmt.Printf("  ServerType: %s \n", item.ServerType)
        }
        PrintSpace(spaceNum)
        fmt.Printf("  Privileged: %v \n", item.Privileged)
        if item.Env != nil {
            PrintSpace(spaceNum)
            fmt.Printf("  Environment Variables: \n")
            for key, val := range item.Env {
                PrintSpace(spaceNum)
                fmt.Printf("     %s:%s \n", key, val)
            }
        }
        if item.Volumes != nil {
            PrintSpace(spaceNum)
            fmt.Printf("  Volumes: \n")
            for key, val := range item.Volumes {
                PrintSpace(spaceNum)
                fmt.Printf("     %s:%s \n", key, val)
            }
        }
        if item.ShardGroupSize != 0 {
            PrintSpace(spaceNum)
            fmt.Printf("  ShardGroupSize: %d \n", item.ShardGroupSize)
        }

        if item.BranchVarList != nil {
            for name, varList := range item.BranchVarList {
                PrintSpace(spaceNum)
                fmt.Printf("  BranchVarList %s: %v \n", name, varList)
            }
        }

        if item.BranchVarFiles != nil {
            for name, varFile := range item.BranchVarFiles {
                PrintSpace(spaceNum)
                fmt.Printf("  BranchVarFile %s: %v \n", name, varFile)
            }
        }

        if item.BranchVarTags != nil {
            for name, varTag := range item.BranchVarTags {
                PrintSpace(spaceNum)
                fmt.Printf("  BranchVarTag %s: %v \n", name, varTag)
            }
        }

        if item.BranchVarMapFile != "" {
            PrintSpace(spaceNum)
            fmt.Printf("  BranchVarMapFile: %s\n",
                item.BranchVarMapFile)
        }

        if item.BranchVarMapTag != "" {
            PrintSpace(spaceNum)
            fmt.Printf("BranchVarMapTag: %s\n",
                item.BranchVarMapTag)
        }

        if item.ExtensionMap != nil && len(item.ExtensionMap) != 0 {
            PrintSpace(spaceNum)
            fmt.Printf("  ExtensionMap: {\n")
            for key, val := range item.ExtensionMap {
                PrintSpace(spaceNum)
                fmt.Printf("    %s: %s \n", key, val)
            }
            PrintSpace(spaceNum)
            fmt.Printf("  }\n")
        } else if item.ExtensionName != "" {
            PrintSpace(spaceNum)
            fmt.Printf("  ExtensionName: %s \n", item.ExtensionName)
        }

        if item.TagPrefixMap != nil && len(item.TagPrefixMap) != 0 {
            PrintSpace(spaceNum)
            fmt.Printf("  TagPrefixMap: {\n")
            for key, val := range item.TagPrefixMap {
                PrintSpace(spaceNum)
                fmt.Printf("    %s: %s \n", key, val)
            }
            PrintSpace(spaceNum)
            fmt.Printf("  }\n")
        } else if item.TagPrefix != "" {
            PrintSpace(spaceNum)
            fmt.Printf("  TagPrefix: %s \n", item.TagPrefix)
        }

        for key, val := range item.ResourceSpec {
            PrintSpace(spaceNum)
            switch val.(type){
                case string:
                    fmt.Printf("  %s: %s \n", key, val)
                case float64:
                    fmt.Printf("  %s: %f \n", key, val)
                case int64:
                    fmt.Printf("  %s: %d \n", key, val)
            }
        }
    
        PrintSpace(spaceNum)
        fmt.Printf("  FailRetryLimit: %d \n", item.FailRetryLimit)
        PrintSpace(spaceNum)
        fmt.Printf("  FailIgnore: %v \n", item.FailIgnore)
        PrintSpace(spaceNum)
        fmt.Printf("  CPUTuneRatio: %f \n", item.CPUTuneRatio)
        PrintSpace(spaceNum)
        fmt.Printf("  MemTuneRatio: %f \n", item.MemTuneRatio)
        if item.ExecMode != "" {
            PrintSpace(spaceNum)
            fmt.Printf("  ExecMode: %s \n", item.ExecMode)
        }
        if len(item.Constraints) > 0 {
            PrintSpace(spaceNum)
            fmt.Printf("  Constraints:\n")
            for k, v := range item.Constraints {
                PrintSpace(spaceNum)
                fmt.Printf("    %s:%s\n", k, v)
            }
        }
        if len(item.Discard) > 0 {
            PrintSpace(spaceNum)
            fmt.Printf("  Discard: ")
            for index, v := range item.Discard {
                if index != 0 {
                    fmt.Printf(",")
                }
                fmt.Printf("%s", v)
            }
            fmt.Printf("\n")
        }
        if len(item.Forward) > 0 {
            PrintSpace(spaceNum)
            fmt.Printf("  Forward: ")
            for index, v := range item.Forward {
                if index != 0 {
                    fmt.Printf(",")
                }
                fmt.Printf("%s", v)
            }
            fmt.Printf("\n")
        }

        if item.BranchSelectorFile != "" {
            PrintSpace(spaceNum)
            fmt.Printf("  BranchSelectorFile: %s \n",
                item.BranchSelectorFile)
        }
        if item.BranchSelectorTag != "" {
            PrintSpace(spaceNum)
            fmt.Printf("  BranchSelectorTag: %s \n",
                item.BranchSelectorTag)
        }
        if item.IOPattern != "" {
            PrintSpace(spaceNum)
            fmt.Printf("  IOPattern: %s \n",
                item.IOPattern)
        }
        if item.RWPattern != "" {
            PrintSpace(spaceNum)
            fmt.Printf("  RWPattern: %s \n",
                item.RWPattern)
        }
        if item.WorkingSet != "" {
            PrintSpace(spaceNum)
            fmt.Printf("  WorkingSet: %s \n",
                item.WorkingSet)
        }
        if item.IsolationLevel != "" {
            PrintSpace(spaceNum)
            fmt.Printf("  IsolationLevel: %s \n",
                item.IsolationLevel)
        }
        if item.EpheremalLevel != "" {
            PrintSpace(spaceNum)
            fmt.Printf("  EpheremalLevel: %s \n",
                item.EpheremalLevel)
        }
        if item.EpheremalFilePattern != "" {
            PrintSpace(spaceNum)
            fmt.Printf("  EpheremalFilePattern: %s \n",
                item.EpheremalFilePattern)
        }
        if item.EpheremalMap != nil {
            PrintSpace(spaceNum)
            mapStr := ""
            for key, val := range item.EpheremalMap {
                if mapStr != "" {
                    mapStr += ","
                }
                mapStr += key + ":" + val
            }
            fmt.Printf("  EpheremalMap: %s\n", mapStr)
        }
    }

    if len(item.Items) > 0 {
        PrintSpace(spaceNum)
        fmt.Printf("  SubPipeline: \n")
        for i := 0; i < len(item.Items); i ++ {
            ShowPipelineItem(&item.Items[i], spaceNum + 4, showAll)
        }
    }
}

func (cmd *itemCommand) Add(file string, isYaml bool) {
    jsonFile := file
    var err error
    if isYaml {
        jsonFile, err = YamlFileToJSON(file)
        if err != nil {
            fmt.Printf("Fail to convert from yaml to json: %s\n",
                err.Error())
            os.Exit(1)
        }
    }
	err = cmd.client.AddPipelineItemFile(jsonFile)
	if err != nil {
		fmt.Printf("The pipeline items added failure: %s\n", err.Error())
        if isYaml {
            os.Remove(jsonFile)
        }
		os.Exit(1)
	} else {
		fmt.Printf("The pipeline item added success\n")
        if isYaml {
            os.Remove(jsonFile)
        }
	}
}

func (cmd *itemCommand) Update(file string, isYaml bool) {
    jsonFile := file
    var err error
    if isYaml {
        jsonFile, err = YamlFileToJSON(file)
        if err != nil {
            fmt.Printf("Fail to convert from yaml to json: %s\n",
                err.Error())
            os.Exit(1)
        }
    }
	err = cmd.client.UpdatePipelineItemFile(jsonFile)
	if err != nil {
		fmt.Printf("The pipeline items updated failure: %s\n", err.Error())
        if isYaml {
            os.Remove(jsonFile)
        }
		os.Exit(1)
	} else {
        if isYaml {
            os.Remove(jsonFile)
        }
		fmt.Printf("The pipeline item updated success\n")
	}
}

func (cmd *itemCommand) Delete(name string) {
	if name == "" {
		fmt.Printf("You must specify a valid pipeline item name\n")
		os.Exit(1)
	}
	
	err := cmd.client.DeletePipelineItem(name)
	if err != nil {
		fmt.Printf("Fail to delete pipeline item %s: %s\n",
			name, err.Error())
		os.Exit(1)
	} else {
		fmt.Printf("Succeed to delete pipeline item %s\n",
			name)
	}
}

func (cmd *itemCommand) Info(name string) {
	if name == "" {
		fmt.Printf("You must specify a valid pipeline name\n")
		os.Exit(1)
	}
	
	err, pipelineItemInfo := cmd.client.GetPipelineItemInfo(name)
	if err != nil {
		fmt.Printf("Fail to get pipeline item info: %s\n",
			err.Error())
		os.Exit(1)
	} else {
		ShowPipelineItem(pipelineItemInfo, 0, true)
	}
}

func (cmd *itemCommand) List() {
	err, pipelineItems := cmd.client.ListPipelineItems()
	if err != nil {
		fmt.Printf("Fail to list pipeline items: %s\n",
			err.Error())
		os.Exit(1)
	} else {
		ShowPipelineItems(pipelineItems, false)
	}
}

func (cmd *itemCommand) Dump(name string, file string, isYaml bool) {
    if name == "" || file == "" {
        fmt.Printf("You must specify item name and file\n")
        os.Exit(1)
    }

	err := cmd.client.DumpPipelineItem(name, file, isYaml)
	if err != nil {
		fmt.Printf("Fail to dump pipeline items: %s\n",
			err.Error())
		os.Exit(1)
	} else {
        fmt.Printf("Success to dump pipeline items\n")
	}
}
