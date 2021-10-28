package main

import (
        "fmt"
        "github.com/xtao/bioflow/scheduler"
)

func main() {
    mgr := scheduler.NewPipelineMgr()
    pipeline := mgr.NewBIOPipelineFromJSONFile("pipeline.json")
    fmt.Println(pipeline)
}
