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
    "io/ioutil"
    "io"
    "sync"
    "errors"
    "fmt"
	"time"
    "strings"
    "strconv"

    . "github.com/xtao/bioflow/dbservice"
    . "github.com/xtao/bioflow/common"
    . "github.com/xtao/bioflow/message"
    . "github.com/xtao/bioflow/scheduler/common"
)

const (
    PIPELINE_INITIAL_VERSION string = "1"
)


type PipelineMgr interface {
    AddPipeline(*SecurityContext, Pipeline) error
    GetPipeline(string, string) Pipeline
    NewPipelineFromJSONStream(*SecurityContext, io.ReadCloser) (error, Pipeline)
    NewBIOPipelineFromJSONFile(string) Pipeline
    Start()
    GetPipelineInfo(*SecurityContext, string, string) (error, *BioflowPipelineInfo)
    ListPipelineAllVersions(*SecurityContext, string) (error, []BioflowPipelineInfo)
    ListPipelines(*SecurityContext) (error, []BioflowPipelineInfo)
    NewPipelineItemsFromJSONStream(*SecurityContext, io.ReadCloser) (error, []PipelineItem)
    AddPipelineItem(*SecurityContext, PipelineItem) error
    AddPipelineItems(*SecurityContext, []PipelineItem) error
    UpdatePipelineItems(*SecurityContext, []PipelineItem) error
    GetPipelineItem(string, string) PipelineItem
    ListPipelineItems(*SecurityContext) (error, []BioflowPipelineItemInfo)
    GetPipelineItemInfo(*SecurityContext, string) (error, *BioflowPipelineItemInfo)
    DeletePipelineItem(*SecurityContext, string) error
    DeletePipeline(*SecurityContext, string) error
    DumpBIOPipelineToJSONFile(*SecurityContext, string, string) (error, *PipelineJSONData)
    ClonePipeline(*SecurityContext, string, string) error
    UpdatePipeline(*SecurityContext, Pipeline, bool) error
    DumpPipelineItemToJSON(*SecurityContext, string) (error, *PipelineItemsJSONData)
}

type pipelineMgr struct {
    lock   sync.RWMutex
    pipelines map[string]map[string]Pipeline
    pipelineItems map[string]map[string]PipelineItem
	pipelinesLoaded bool
	pipelineItemsLoaded bool
}

var globalPipelineMgr PipelineMgr = nil

func GetPipelineMgr() PipelineMgr {
    return globalPipelineMgr
}

func NewPipelineMgr() PipelineMgr {
    if globalPipelineMgr == nil {
        mgr := &pipelineMgr {
            pipelines: make(map[string]map[string]Pipeline),
            pipelineItems: make(map[string]map[string]PipelineItem),
			pipelinesLoaded: false,
			pipelineItemsLoaded: false,
        }
        globalPipelineMgr = mgr
    }

    return globalPipelineMgr
}

func (mgr *pipelineMgr) Start() {

	for {
		if mgr.pipelineItemsLoaded == false {
			err := mgr.LoadPipelineItemsFromDB()
			if err == nil {
				mgr.pipelineItemsLoaded = true
			}
		}

		if mgr.pipelinesLoaded == false {
			err := mgr.LoadPipelinesFromDB()
			if err == nil {
				mgr.pipelinesLoaded = true
			}
		}

		if mgr.pipelinesLoaded == true && mgr.pipelineItemsLoaded == true {
			break
		} else {
			time.Sleep(time.Second * 10)				
		}
	}
}

func (mgr *pipelineMgr) LoadPipelineItemsFromDB() error{
    db := GetDBService()

    err, itemKeys := db.GetAllPipelineItemNames()
    if err != nil || itemKeys == nil {
        SchedulerLogger.Errorf("Can't load pipeline item names from DB: %s",
            err.Error())
        return err
    }

    for i := 0; i < len(itemKeys); i ++ {
        item := new(bioPipelineItem)
        err, dbInfo := db.GetPipelineItem(&itemKeys[i])
        if err != nil {
            SchedulerLogger.Errorf("Fail to load pipeline item %s/%s from DB: %s\n",
                itemKeys[i].Name, itemKeys[i].SecID, err.Error())
        } else {
            jsonData := &BioPipelineItemJSONData{}

            err = json.Unmarshal([]byte(dbInfo.Json), jsonData)
            if err != nil {
                SchedulerLogger.Errorf("Can't unmarshal pipeline item %s/%s from DB: %s\n",
                    itemKeys[i].Name, itemKeys[i].SecID, err.Error())
            } else {
                err = item.FromJSON(jsonData)
                if err != nil {
                    SchedulerLogger.Errorf("Can't create pipeline item %s/%s from JSON: %s\n", 
                        itemKeys[i].Name, itemKeys[i].SecID, err.Error())
                } else {
                    item.SetSecID(dbInfo.SecID)
                    ns := NSMapFromSecID(dbInfo.SecID)
                    err := mgr.AddPipelineItemToNameSpace(ns, item, false)
                    if err != nil {
                        SchedulerLogger.Errorf("Fail to add pipeline item %s to ns %s: %s\n",
                            item.Name(), ns, err.Error())
                    }
                }
            }
        }
    }

    return nil
}

/*Check update pipeline is reliable*/
func (mgr *pipelineMgr) _VerifyPipelineExsit (ns string,
    pipeline Pipeline, update bool) error {
    mgr.lock.Lock()
    defer mgr.lock.Unlock()

    nsPipelines, ok := mgr.pipelines[ns]
    if !ok || nsPipelines == nil {
        nsPipelines = make(map[string]Pipeline)
        mgr.pipelines[ns] = nsPipelines
    }

    _, ok = nsPipelines[pipeline.Name()]
    if ok && !update {
        return errors.New("Pipeline with name " + pipeline.Name() + " exist")
    } else if update && !ok {
        return errors.New("Pipeline with name " + pipeline.Name() + " not exist")
    }
    return nil
}

func (mgr *pipelineMgr) _AddPipelineToNameSpace(ns string,
    pipeline Pipeline) {
    mgr.lock.Lock()
    defer mgr.lock.Unlock()
    nsPipelines, ok := mgr.pipelines[ns]
    if !ok || nsPipelines == nil {
        nsPipelines = make(map[string]Pipeline)
        mgr.pipelines[ns] = nsPipelines
    }
    nsPipelines[pipeline.Name()] = pipeline
}

func (mgr *pipelineMgr) AddPipelineToNameSpace(ns string,
    pipeline Pipeline, update bool) error{

    err := mgr._VerifyPipelineExsit(ns, pipeline, update)
    if err != nil {
        SchedulerLogger.Errorf("PipelineMgr(pipeline %s): update pipeline check failed: %s",
            pipeline.Name(), err.Error())
        return err
    }

    mgr._AddPipelineToNameSpace(ns, pipeline)

    return nil
}

func (mgr *pipelineMgr) AddPipelineItemToNameSpace(ns string,
    item PipelineItem, update bool) error{
    mgr.lock.Lock()
    defer mgr.lock.Unlock()

    nsItems, ok := mgr.pipelineItems[ns]
    if !ok || nsItems == nil {
        nsItems = make(map[string]PipelineItem)
        mgr.pipelineItems[ns] = nsItems
    }

    _, ok = nsItems[item.Name()]
    if ok && !update {
        return errors.New("Pipeline Item with name " + item.Name() + " exist")
    }

    nsItems[item.Name()] = item

    return nil
}

func (mgr *pipelineMgr) LoadPipelineAllVersionsFromDB(key *PipelineDBKey) ([]Pipeline, error) {
    db := GetDBService()
    err, dbInfo := db.GetPipelineAllVersions(key)
    if err != nil {
        SchedulerLogger.Errorf("Fail to load pipeline %s/%s from DB: %s\n",key.Name, key.SecID, err.Error())
        return nil, err
    } else {
        pipelineVerions := make([]Pipeline, 0)
        for i := 0; i < len(dbInfo); i ++ {
            pipeline := new(bioPipeline)
            jsonData := &PipelineJSONData{}
            err = json.Unmarshal([]byte(dbInfo[i].Json), jsonData)
            if err != nil {
                SchedulerLogger.Errorf("Can't unmarshal pipeline %s/%s from DB: %s\n",key.Name, key.SecID, err.Error())
                return nil, err
            } else {
                err = pipeline.FromJSON(jsonData)
                if err != nil {
                    SchedulerLogger.Errorf("Can't create pipeline %s/%s from JSON: %s\n", key.Name, key.SecID, err.Error())
                    return nil, err
                } else {
                    pipeline.SetSecID(dbInfo[i].SecID)
                    pipeline.SetState(dbInfo[i].State)
                    pipeline.SetParent(dbInfo[i].Parent)
                    pipeline.SetLastVersion(dbInfo[i].LastVersion)
                    pipeline.SetVersion(dbInfo[i].Version)
                }
            }
            pipelineVerions = append(pipelineVerions, pipeline)
        }
        return pipelineVerions, nil
    }
}

func (mgr *pipelineMgr) LoadPipelineFromDB(key *PipelineDBKey, version string) (Pipeline, error) {
    var err error = nil
    var dbInfo *PipelineDBInfo = nil
    pipeline := new(bioPipeline)
    db := GetDBService()
    if version == "" {
        err, dbInfo = db.GetPipeline(key)
    } else {
        err, dbInfo = db.GetPipelineByVersion(key, version)
    }

    if err != nil {
        SchedulerLogger.Errorf("Fail to load pipeline %s/%s from DB: %s\n",
            key.Name, key.SecID, err.Error())
        return nil, err
    } else {
        jsonData := &PipelineJSONData{}
        err = json.Unmarshal([]byte(dbInfo.Json), jsonData)
        if err != nil {
            SchedulerLogger.Errorf("Can't unmarshal pipeline %s/%s from DB: %s\n",
                key.Name, key.SecID, err.Error())
        } else {
            err = pipeline.FromJSON(jsonData)
            if err != nil {
                SchedulerLogger.Errorf("Can't create pipeline %s/%s from JSON: %s\n", 
                    key.Name, key.SecID, err.Error())
                return nil, err
            } else {
                pipeline.SetSecID(dbInfo.SecID)
                pipeline.SetState(dbInfo.State)
                pipeline.SetParent(dbInfo.Parent)
                pipeline.SetLastVersion(dbInfo.LastVersion)
                pipeline.SetVersion(dbInfo.Version)

                return pipeline, nil
            }
        }
    }

    return nil, nil
}

func (mgr *pipelineMgr) LoadPipelinesFromDB() error{
    db := GetDBService()

    err, pipelineKeys := db.GetAllPipelineNames()
    if err != nil || pipelineKeys == nil {
        SchedulerLogger.Errorf("Can't load pipeline names from DB: %s",
            err.Error())
        return err
    }

    for i := 0; i < len(pipelineKeys); i ++ {
        pipeline, err := mgr.LoadPipelineFromDB(&pipelineKeys[i], "")
        if err != nil {
            SchedulerLogger.Errorf("Fail to load pipeline %s/%s from DB: %s\n",
                pipelineKeys[i].Name, pipelineKeys[i].SecID, err.Error())
        } else {
            ns := NSMapFromSecID(pipeline.SecID())
            err = mgr.AddPipelineToNameSpace(ns, pipeline, false)
            if err != nil {
                SchedulerLogger.Errorf("Fail to add pipeline %s to namespace: %s\n",
                    pipeline.Name(), err.Error())
            }
        }
    }

    return nil
}

/*
 * Update a pipeline with the same name
 * the JSON definition of the pipeline will be updated
 * and persist to database
 */
func (mgr *pipelineMgr) UpdatePipeline(ctxt *SecurityContext, pipeline Pipeline, isForce bool) error {
    if !isForce {
        useJobs := GetJobMgr().GetJobIdsByPipeline(ctxt, pipeline.Name())
        if len(useJobs) != 0 {
            SchedulerLogger.Errorf("UpdatePipeline(pipeline %s): %d jobs such as %s use the pipeline, can't update\n",
                pipeline.Name(), len(useJobs), useJobs[0])
            errStr := fmt.Sprintf("pipeline %s used by %d jobs (%s)",
                pipeline.Name(), len(useJobs), useJobs[0])
            return errors.New(errStr)
        }
    }

    ns := NSMapFromContext(ctxt)
    oldPipeline := mgr.GetPipeline(ns, pipeline.Name())
    if oldPipeline != nil && !ctxt.CheckSecID(oldPipeline.SecID()) {
        SchedulerLogger.Errorf("UpdatePipeline(pipeline %s): User %s can't delete %s's pipeline\n",
            pipeline.Name(), ctxt.ID(), oldPipeline.SecID())
        return errors.New("Security check failure: no privilege")
    } else if oldPipeline == nil {
        SchedulerLogger.Errorf("UpdatePipeline(pipeline %s): The update pipeline not exist!",
            pipeline.Name())
        return errors.New("The update pipeline not exist, please create it first")
    }

    pipeline.SetSecID(ctxt.ID())
    pipeline.SetParent(oldPipeline.Name())
    pipeline.SetLastVersion(oldPipeline.Version())

    /*
     * Build the current version number. Currently it is the
     * old version + 1
     */
    curVersion, err := strconv.ParseUint(oldPipeline.Version(), 10, 64)
    if err != nil {
        SchedulerLogger.Errorf("UpdatePipeline(pipeline %s):can't generate the version information %s\n",
            pipeline.Name(), err.Error())
        return err
    }
    newVersion := fmt.Sprintf("%d", curVersion + 1)
    pipeline.SetVersion(newVersion)

    err = mgr._VerifyPipelineExsit(ns, pipeline, true)
    if err != nil {
        SchedulerLogger.Errorf("UpdatePipeline(pipeline %s): update pipeline check failed: %s",
            pipeline.Name(), err.Error())
        return err
    }

    /*For wdl or cwl pipeline, need persist the source first*/
    err = pipeline.PersistToStore()
    if err != nil {
        SchedulerLogger.Errorf("UpdatePipeline(pipeline %s/%s):Fail to persist pipeline: %s\n",
            pipeline.Name(), pipeline.Version(), err.Error())
        return err
    }

    /*save pipeline to database*/
    err, pipelineJson := pipeline.ToJSON()
    if err != nil {
        SchedulerLogger.Errorf("UpdatePipeline(pipeline %s): Serialize pipeline failure: %s\n",
            pipeline.Name(), err.Error())
        return err
    }
    body, err := json.Marshal(&pipelineJson)
    if err != nil {
        SchedulerLogger.Errorf("UpdatePipeline(pipeline %s): Marshal failure: %s\n",
            pipeline.Name(), err.Error())
        return err
    } else {
        dbInfo := &PipelineDBInfo{
                SecID: ctxt.ID(),
                Name: pipeline.Name(),
                Json: string(body),
                Parent: pipeline.Parent(),
                LastVersion: pipeline.LastVersion(),
                Version: pipeline.Version(),
                State: pipeline.State(),
        }
        db := GetDBService()
        err = db.UpdatePipeline(dbInfo)
        if err != nil {
            SchedulerLogger.Errorf("UpdatePipeline(pipeline %s/%s): to DB failure: %s\n",
                pipeline.Name(), newVersion, err.Error())
            return err
        } else {
            SchedulerLogger.Infof("UpdatePipeline(pipeline %s/%s):Succeed update to database\n",
                pipeline.Name(), newVersion)
        }
    }

    mgr._AddPipelineToNameSpace(ns, pipeline)

    return nil
}

func (mgr *pipelineMgr) DumpBIOPipelineToJSONFile(ctxt *SecurityContext, name string,
    version string) (error, *PipelineJSONData) {
    pipeline, err := mgr.GetPipelineByVersion(ctxt, name, version)
    if err != nil {
        return err, nil
    }
    if pipeline == nil {
        SchedulerLogger.Errorf("The pipeline with name %s/%s not found\n",
            name, version)
        return errors.New("Pipeline not found"), nil
    }

    if GetUserMgr().IsStrictMode() {
        curUserAccountInfo := ctxt.GetUserInfo()
        /*
         *Security check, if the cur user and the pipeline owner in the same group,the cur user
         *has privilege to info the pipeline, otherwise not allowed to do it.
         */
        if !GetUserMgr().CheckGroupPrivilege(ctxt, pipeline.SecID(),
            curUserAccountInfo.Username) {
            SchedulerLogger.Errorf("Only the user in the same group of owner allowed to dump the pipeline")
            return errors.New("You have no privilege to dump the pipeline"), nil
        }
    }

    /*Generate Pipeline JSON*/
    err, PipelineJsonData := pipeline.ToJSON()
    if err != nil {
        SchedulerLogger.Errorf("Dump json data from pipeline failed: %s\n!",
            err.Error())
        return err, nil
    } else {
        SchedulerLogger.Infof("Dump json data from pipeline success\n")
    }

    /*Read the WDL source files from pipeline store if it is a WDL type pipeline*/
    if pipeline.Type() == PIPELINE_TYPE_WDL {
        srcData, err := pipeline.ReadFromStore()
        if err != nil {
            SchedulerLogger.Errorf("Fail to read pipeline %s/%s from store: %s\n",
                name, version, err.Error())
            return err, nil
        }
        PipelineJsonData.Wdl.Blob = srcData
    }

    return nil, PipelineJsonData
}

func (mgr *pipelineMgr) ClonePipeline(ctxt *SecurityContext, src string, dst string) error {
    src = strings.ToUpper(src)
    dst = strings.ToUpper(dst)
    ns := NSMapFromContext(ctxt)
    srcPipeline := mgr.GetPipeline(ns, src)
    if srcPipeline == nil {
        SchedulerLogger.Errorf("Clone failure, source pipeline %s not found\n",
            src)
        return errors.New("Pipeline " + src + " not found")
    }
    dstPipeline := mgr.GetPipeline(ns, dst)
    if dstPipeline != nil {
        SchedulerLogger.Errorf("Clone failure, dest pipeline %s exist\n",
            dst)
        return errors.New("Pipeline " + dst + " already exist")
    }

    if GetUserMgr().IsStrictMode() {
        curUserAccountInfo := ctxt.GetUserInfo()
        /*
         * Security check. if the cur user and the pipeline owner in the same group,
         * the user has privilege to clone the pipeline, otherwise not allowed to do it.
         */
        if !GetUserMgr().CheckGroupPrivilege(ctxt, srcPipeline.SecID(),
                curUserAccountInfo.Username) {
            SchedulerLogger.Errorf("User %s have no privilege to clone the pipeline, not in the same group with owner")
            return errors.New("You have no privilege to clone the pipeline")
        }
    }

    err, dstPipeline := srcPipeline.Clone()
    if err != nil {
        SchedulerLogger.Errorf("Can't clone pipeline %s: %s\n",
            src, err.Error())
        return err
    }
    dstPipeline.SetName(dst)
    dstPipeline.SetParent(srcPipeline.Name())
    dstPipeline.SetLastVersion(srcPipeline.Version())

    err = mgr.AddPipeline(ctxt, dstPipeline)
    if err != nil {
        SchedulerLogger.Errorf("Add cloned pipeline %s failure: %s\n",
            dst, err.Error())
        return err
    }

    SchedulerLogger.Infof("Successfully clone pipeline from %s to %s\n",
        src, dst)

    return nil
}

func (mgr *pipelineMgr) AddPipeline(ctxt *SecurityContext, pipeline Pipeline) error {
    ns := NSMapFromContext(ctxt)
    err := mgr.AddPipelineToNameSpace(ns, pipeline, false)
    if err != nil {
        SchedulerLogger.Errorf("Fail to add pipeline %s to namespace: %s\n",
            pipeline.Name(), err.Error())
        return err
    }
    pipeline.SetSecID(ctxt.ID())
    pipeline.SetVersion(PIPELINE_INITIAL_VERSION)
    pipeline.SetState(PSTATE_HEAD)

    /*For wdl or cwl pipeline, need persist the source first*/
    err = pipeline.PersistToStore()
    if err != nil {
        SchedulerLogger.Errorf("Fail to persist pipeline %s/%s: %s\n",
            pipeline.Name(), pipeline.Version(), err.Error())
        return err
    }
    SchedulerLogger.Infof("Succeed to persist pipeline %s\n",
        pipeline.Name())

    /*save pipeline to database*/
    err, pipelineJson := pipeline.ToJSON()
    if err != nil {
        SchedulerLogger.Errorf("Serialize pipeline failure: %s\n",
            err.Error())
    }
    body, err := json.Marshal(&pipelineJson)
    if err != nil {
        SchedulerLogger.Errorf("Marshal failure: %s\n", err.Error())
    } else {
        /*Create the new pipeline with initial version*/
        dbInfo := &PipelineDBInfo{
                SecID: ctxt.ID(),
                Name: pipeline.Name(),
                Parent: pipeline.Parent(),
                LastVersion: pipeline.LastVersion(),
                Version: pipeline.Version(),
                State: pipeline.State(),
                Json: string(body),
        }
        db := GetDBService()
        err = db.AddPipeline(dbInfo)
        if err != nil {
            SchedulerLogger.Errorf("Persist pipeline %s to DB failure: %s\n",
                pipeline.Name(), err.Error())
        } else {
            SchedulerLogger.Infof("Succeed to persist pipeline %s to database\n",
                pipeline.Name())
        }
    }

    return nil
}

func (mgr *pipelineMgr) GetPipeline(ns string, name string) Pipeline {
    mgr.lock.RLock()
    defer mgr.lock.RUnlock()

    if nsPipelines, ok := mgr.pipelines[ns]; ok {
        return nsPipelines[strings.ToUpper(name)]
    }

    return nil
}

func (mgr *pipelineMgr) deletePipeline(ns string, name string) error {
    mgr.lock.Lock()
    defer mgr.lock.Unlock()

    if nsPipelines, ok := mgr.pipelines[ns]; ok {
        delete(nsPipelines, name)
    }

    return nil
}

func (mgr *pipelineMgr) NewPipelineFromJSONStream(ctxt *SecurityContext,
    body io.ReadCloser) (error, Pipeline) {
    decoder := json.NewDecoder(body)
    
    var pipelineJson PipelineJSONData
    pipelineJson.UseExistingItem = true

    err := decoder.Decode(&pipelineJson)
    if err != nil {
        Logger.Println("Can't parse the request " + err.Error())
        return err, nil
    }

    pipeline := new(bioPipeline)
    err = pipeline.FromJSON(&pipelineJson)
    if err != nil {
        SchedulerLogger.Errorf("Parse pipeline failure: %s\n",
            err.Error())
        return err, nil
    }

    switch pipeline.Type() {
        case PIPELINE_TYPE_BIOBPIPE:
            err = mgr.ValidateFixUserPipeline(ctxt, pipeline, pipelineJson.UseExistingItem)
            if err != nil {
                SchedulerLogger.Errorf("Error on validate pipeline: %s\n",
                    err.Error())
                return err, nil
            }
        case PIPELINE_TYPE_WDL:
    }

    return nil, pipeline
}

func (mgr *pipelineMgr) NewBIOPipelineFromJSONFile(file string) Pipeline {
    raw, err := ioutil.ReadFile(file)
    if err != nil {
        Logger.Println(err.Error())
        return nil
    }

    var pipelineJson PipelineJSONData
    err = json.Unmarshal(raw, &pipelineJson)
    if err != nil {
        Logger.Println(err.Error())
        return nil
    }

    pipeline := new(bioPipeline)
    err = pipeline.FromJSON(&pipelineJson)
    if err != nil {
        SchedulerLogger.Errorf("Parse pipeline failure: %s\n",
            err.Error())
        return nil
    }
    return pipeline
}

func (mgr *pipelineMgr) ValidateFixUserPipelineItem(ctxt *SecurityContext,
    item PipelineItem, replace bool) error {
    ns := NSMapFromContext(ctxt)
    isBasicItem := item.ItemType() == GEN_PT_SIMPLE && item.ItemsCount() == 0
    if isBasicItem {
        if item.Name() == "" {
            SchedulerLogger.Errorf("Empty item Name \n")
            return errors.New("Empty item name")
        }
        if item.Cmd() == "" {
            SchedulerLogger.Errorf("Empty Cmd for item %s \n",
                item.Name())
            errStr := fmt.Sprintf("Empty Cmd for item %s",
                item.Name())
            return errors.New(errStr)
        }

        return nil
    } else {
        for i := 0; i < item.ItemsCount(); i ++ {
            subItem := item.Item(i)
            isBasicItem := subItem.ItemType() == GEN_PT_SIMPLE && 
                subItem.ItemsCount() == 0
            if subItem.Name() == "" {
                SchedulerLogger.Errorf("The name of the %d sub item is empty for %s\n",
                    i, item.Name())
                errStr := fmt.Sprintf("The name of the %d sub item is empty for %s",
                    i, item.Name())
                return errors.New(errStr)
            } else if replace && isBasicItem {
                existingItem := mgr.GetPipelineItem(ns, subItem.Name())
                if existingItem != nil {
                    SchedulerLogger.Infof("Replace subitem %s in item %s with existing one\n",
                        subItem.Name(), item.Name())
                    item.SetItem(i, existingItem)
                    continue
                } 
            } else if subItem.ItemType() == GEN_PT_PIPELINE {
                subPipeline := mgr.GetPipeline(ns, subItem.Name())
                if subPipeline == nil {
                    SchedulerLogger.Errorf("The pipeline only allowed be called a exsiting pipeline\n")
                    return errors.New("The pipeline only allowed be called a exsiting pipeline\n")
                } else {
                    subItem.SetItems(subPipeline.Items())
                    continue
                }
            }

            err := mgr.ValidateFixUserPipelineItem(ctxt, subItem, replace)
            if err != nil {
                SchedulerLogger.Errorf("Validate item %s sub item %s fail: %s\n",
                    item.Name(), item.Item(i).Name(), err.Error())
                return err
            }
        }
    }

    return nil
}



func (mgr *pipelineMgr) ValidateFixUserPipeline(ctxt *SecurityContext,
    pipeline Pipeline, replace bool) error {
    if pipeline.Name() == "" {
        SchedulerLogger.Errorf("The pipeline should have a name\n")
        return errors.New("Pipeline should have name")
    }

    ns := NSMapFromContext(ctxt)
    for i := 0; i < pipeline.Size(); i ++ {
        item := pipeline.Item(i)
        if replace && item.ItemType() != GEN_PT_PIPELINE {
            existingItem := mgr.GetPipelineItem(ns, item.Name())
            if existingItem != nil {
                SchedulerLogger.Infof("Replace subitem %s in item %s with existing one\n",
                    item.Name(), pipeline.Name())
                pipeline.SetItem(i, existingItem)
                continue
            }
        } else if item.ItemType() == GEN_PT_PIPELINE && item.Name() == pipeline.Name() {
            SchedulerLogger.Errorf("The pipeline %s should not use itself as sub pipeline\n",
                pipeline.Name())
            return errors.New("The pipeline should not use itself as sub pipeline")
        } else if item.ItemType() == GEN_PT_PIPELINE && item.Name() != pipeline.Name() {
            subPipeline := mgr.GetPipeline(ns, item.Name())
            if subPipeline == nil {
                SchedulerLogger.Errorf("The pipeline %s only allowed be called a exsiting pipeline\n",
                    pipeline.Name())
                return errors.New("The pipeline only allowed be called a exsiting pipeline\n")
            } else {
                item.SetItems(subPipeline.Items())
                continue
            }
        }
        err := mgr.ValidateFixUserPipelineItem(ctxt, item, replace)
        if err != nil {
            SchedulerLogger.Errorf("Validate user pipeline item %s fail for pipeline %s: %s\n",
                item.Name(), pipeline.Name(), err.Error())
            return err
        }
    }
    
    return nil
}

func (mgr *pipelineMgr) ListPipelines(ctxt *SecurityContext) (error, []BioflowPipelineInfo) {
    mgr.lock.RLock()
    defer mgr.lock.RUnlock()

    pipelines := make([]BioflowPipelineInfo, 0)
    ns := NSMapFromContext(ctxt)
    nsPipelines := mgr.pipelines[ns]
    if nsPipelines == nil {
        return nil, pipelines
    }
    for _, pipeline := range nsPipelines {
        err, pipelineInfo := pipeline.ToBioflowPipelineInfo(false)
        if err != nil {
            SchedulerLogger.Errorf("Get pipeline list fail: %s \n",
                err.Error())
            return err, nil
        }
        pipelines = append(pipelines, *pipelineInfo)
    }

    return nil, pipelines
}

func (mgr *pipelineMgr) ListPipelineAllVersions(ctxt *SecurityContext, name string) (error, []BioflowPipelineInfo) {
    var err error = nil
    name = strings.ToUpper(name)
    bioPipelineVersions := make([]BioflowPipelineInfo, 0)
    ns := NSMapFromContext(ctxt)

    key := &PipelineDBKey{
        Name: name,
    }
    pipelineVersions, err := mgr.LoadPipelineAllVersionsFromDB(key)
    if err != nil {
        return err, nil
    }

    if len(pipelineVersions) == 0 {
        SchedulerLogger.Errorf("The pipeline with name %s/%s not found any versions\n",
            name, ns)
        return errors.New("Pipeline not found any versions"), nil
    }

    for i := 0; i < len(pipelineVersions); i ++ {
        err, pipelineInfo := pipelineVersions[i].ToBioflowPipelineInfo(true)
        if err != nil {
            SchedulerLogger.Errorf("%v\n", err.Error())
            return err, nil
        }
        bioPipelineVersions= append(bioPipelineVersions, *pipelineInfo)
    }

    return nil, bioPipelineVersions
}

func (mgr *pipelineMgr) GetPipelineByVersion(ctxt *SecurityContext, name string,
    version string) (Pipeline, error) {
    var pipeline Pipeline = nil
    var err error = nil
    name = strings.ToUpper(name)
    ns := NSMapFromContext(ctxt)
    if strings.ToUpper(version) == PIPELINE_VERSION_HEAD || version == "" {
        pipeline = mgr.GetPipeline(ns, name)
    } else if strings.ToUpper(version) == PIPELINE_VERSION_PREV {
        headPipeline := mgr.GetPipeline(ns, name)
        if headPipeline != nil {
            if headPipeline.Parent() == "" || headPipeline.LastVersion() == "" {
                SchedulerLogger.Errorf("No parent and last version exist for pipeline %s/%s\n",
                    name, ns)
                return nil, errors.New("The pipeline is the initial version")
            }

            key := &PipelineDBKey{
                    Name: headPipeline.Parent(),
            }
            pipeline, err = mgr.LoadPipelineFromDB(key, headPipeline.LastVersion())
            if err != nil {
                return nil, err
            }
        } else {
            SchedulerLogger.Errorf("Can't find the head pipeline %s/%s\n",
                name, ns)
            pipeline = nil
        }
    } else {
        key := &PipelineDBKey{
            Name: name,
        }
        pipeline, err = mgr.LoadPipelineFromDB(key, version)
        if err != nil {
            return nil, err
        }
    }

    return pipeline, nil
}

func (mgr *pipelineMgr) GetPipelineInfo(ctxt *SecurityContext,
    name string, version string) (error, *BioflowPipelineInfo) {
    pipeline, err := mgr.GetPipelineByVersion(ctxt, name, version)
    if err != nil {
        return err, nil
    }
    if pipeline == nil {
        SchedulerLogger.Errorf("The pipeline with name %s/%s not found\n",
            name, version)
        return errors.New("Pipeline not found"), nil
    }

    err, pipelineInfo := pipeline.ToBioflowPipelineInfo(true)
    if err != nil {
        SchedulerLogger.Errorf("%v\n", err.Error())
        return err, nil
    }

    if !GetUserMgr().IsStrictMode() {
        return nil, pipelineInfo
    }

    curUserAccountInfo := ctxt.GetUserInfo()
    /*
     *Security check, if the cur user and the pipeline owner in the same group,the cur user
     *has privilege to info the pipeline, otherwise not allowed to do it.
     */
    if !GetUserMgr().CheckGroupPrivilege(ctxt, pipeline.SecID(),
            curUserAccountInfo.Username) {
        SchedulerLogger.Errorf("Only the user in the same group of owner allowed to info the pipeline")
        return errors.New("You have no privilege to info the pipeline"), nil
    }

    return nil, pipelineInfo
}

func (mgr *pipelineMgr) DeletePipeline(ctxt *SecurityContext, name string) error {
    ns := NSMapFromContext(ctxt)
    name = strings.ToUpper(name)
    pipeline := mgr.GetPipeline(ns, name)
    if pipeline == nil {
        SchedulerLogger.Errorf("The pipeline %s not found\n",
            name)
        errStr := fmt.Sprintf("The pipeline %s not found",
            name)
        return errors.New(errStr)
    }

    if !ctxt.CheckSecID(pipeline.SecID()) {
        SchedulerLogger.Errorf("User %s not allowed to delete pipeline %s of %s\n",
            ctxt.ID(), pipeline.Name(), pipeline.SecID())
        return errors.New("Security check failure: no priviledge")
    }

    useJobs := GetJobMgr().GetJobIdsByPipeline(ctxt, name)
    if len(useJobs) != 0 {
        SchedulerLogger.Errorf("%d jobs such as %s use the pipeline %s, can't delete\n",
            len(useJobs), useJobs[0], name)
        errStr := fmt.Sprintf("pipeline %s used by %d jobs (%s)",
            name, len(useJobs), useJobs[0])
        return errors.New(errStr)
    }

    /*Delete all version source files from disk*/
    err := pipeline.DeleteFromStore()
    if err != nil {
        SchedulerLogger.Errorf("Fail to delete pipeline %s/%s sources from disk: %s\n",
            name, pipeline.SecID(), err.Error())
    }
    SchedulerLogger.Infof("Succeed to delete pipeline %s/%s sources from disk\n",
        name, pipeline.SecID())

    db := GetDBService()
    err = db.DeletePipeline(name, pipeline.SecID())
    if err != nil {
        SchedulerLogger.Errorf("Can't delete pipeline %s/%s from DB: %s\n",
            name, pipeline.SecID(), err.Error())
        return err
    } else {
        SchedulerLogger.Infof("Succeed to delete pipeline %s/%s from DB\n",
            name, pipeline.SecID())
    }

    mgr.deletePipeline(ns, name)

    return nil
}

func (mgr *pipelineMgr) DeletePipelineItem(ctxt *SecurityContext,
    name string) error {
    ns := NSMapFromContext(ctxt)
    item := mgr.GetPipelineItem(ns, name)
    if item == nil {
        SchedulerLogger.Errorf("The pipeline item %s not exist in ns %s\n",
            name, ns)
        return errors.New("The pipeline item " + name + " not exist")
    }

    if !ctxt.CheckSecID(item.SecID()) {
        SchedulerLogger.Errorf("User %s not allowed to delete pipeline item of %s\n",
            ctxt.ID(), item.SecID())
        return errors.New("Security check failure: no privilege")
    }

    mgr.deletePipelineItem(ns, name)

    db := GetDBService()
    err := db.DeletePipelineItem(strings.ToUpper(name), item.SecID())
    if err != nil {
        SchedulerLogger.Errorf("Fail to delete pipeline item %s/%s from DB: %s\n",
            name, item.SecID(), err.Error())
        return errors.New("Fail delete from DB")
    } else {
        SchedulerLogger.Infof("Succeed to delete %s/%s from DB",
            name, item.SecID())
    }
    SchedulerLogger.Infof("The pipeline item %s in %s deleted\n",
        name, ns)

    return nil
}

func (mgr *pipelineMgr) AddPipelineItems(ctxt *SecurityContext,
    items []PipelineItem) error {
    for i := 0; i < len(items); i ++ {
        err := mgr.AddPipelineItem(ctxt, items[i])
        if err != nil {
            SchedulerLogger.Errorf("Fail to add item %s: %s\n",
                items[i].Name(), err.Error())
            return err
        }
    }

    return nil
}

func (mgr *pipelineMgr) AddPipelineItem(ctxt *SecurityContext,
    item PipelineItem) error {
    ns := NSMapFromContext(ctxt)
    err := mgr.AddPipelineItemToNameSpace(ns, item, false)
    if err != nil {
        SchedulerLogger.Errorf("Fail to add item %s to namespace %s:%s\n",
            item.Name(), ns, err.Error())
        return err
    }

    item.SetSecID(ctxt.ID())

    /*save pipeline item to database*/
    err, itemJson := item.ToJSON()
    if err != nil {
        SchedulerLogger.Errorf("Serialize pipeline item failure: %s\n",
            err.Error())
    }
    body, err := json.Marshal(&itemJson)
    if err != nil {
        SchedulerLogger.Errorf("Marshal failure: %s\n", err.Error())
    } else {
        dbInfo := &PipelineItemDBInfo{
                SecID: ctxt.ID(),
                Name: strings.ToUpper(item.Name()),
                State: item.State(),
                Json: string(body),
        }
        db := GetDBService()
        err = db.AddPipelineItem(dbInfo)
        if err != nil {
            SchedulerLogger.Errorf("Persist pipeline item %s/%s to DB failure: %s\n",
                item.Name(), ctxt.ID(), err.Error())
        } else {
            SchedulerLogger.Infof("Succeed to persist pipeline item %s/%s to database\n",
                item.Name(), ctxt.ID())
        }
    }

    return err
}

func (mgr *pipelineMgr) UpdatePipelineItems(ctxt *SecurityContext,
    items []PipelineItem) error {
    for i := 0; i < len(items); i ++ {
        err := mgr.UpdatePipelineItem(ctxt, items[i])
        if err != nil {
            SchedulerLogger.Errorf("Fail to update item %s: %s\n",
                items[i].Name(), err.Error())
            return err
        }
    }

    return nil
}

func (mgr *pipelineMgr) UpdatePipelineItem(ctxt *SecurityContext,
    item PipelineItem) error {
    ns := NSMapFromContext(ctxt)
    oldItem := mgr.GetPipelineItem(ns, item.Name())
    if oldItem != nil {
        if !ctxt.CheckSecID(oldItem.SecID()) {
            SchedulerLogger.Infof("Security check failure: %s not allow update %s's item\n",
                ctxt.ID(), oldItem.SecID())
            return errors.New("Security failure: no privilege to operate other's item")
        }
    }

    err := mgr.AddPipelineItemToNameSpace(ns, item, true)
    if err != nil {
        SchedulerLogger.Errorf("Fail to add item %s to namespace %s:%s\n",
            item.Name(), ns, err.Error())
        return err
    }

    item.SetSecID(ctxt.ID())

    /*save pipeline item to database*/
    err, itemJson := item.ToJSON()
    if err != nil {
        SchedulerLogger.Errorf("Serialize pipeline item failure: %s\n",
            err.Error())
    }
    body, err := json.Marshal(&itemJson)
    if err != nil {
        SchedulerLogger.Errorf("Marshal failure: %s\n", err.Error())
    } else {
        dbInfo := &PipelineItemDBInfo{
                SecID: ctxt.ID(),
                Name: item.Name(),
                State: item.State(),
                Json: string(body),
        }
        db := GetDBService()
        err = db.UpdatePipelineItem(dbInfo)
        if err != nil {
            SchedulerLogger.Errorf("Update pipeline item %s/%s to DB failure: %s\n",
                item.Name(), ctxt.ID(), err.Error())
        } else {
            SchedulerLogger.Infof("Succeed to update pipeline item %s/%s to database\n",
                item.Name(), ctxt.ID())
        }
    }

    return err
}

func (mgr *pipelineMgr) GetPipelineItem(ns string, name string) PipelineItem {
    mgr.lock.RLock()
    defer mgr.lock.RUnlock()

    if nsItems, ok := mgr.pipelineItems[ns]; ok {
        return nsItems[strings.ToUpper(name)]
    } else {
        return nil
    }
}

func (mgr *pipelineMgr) deletePipelineItem(ns string, name string) error {
    mgr.lock.Lock()
    defer mgr.lock.Unlock()

    if nsItems, ok := mgr.pipelineItems[ns]; ok {
        delete(nsItems, strings.ToUpper(name))
    }

    return nil
}

func (mgr *pipelineMgr) NewPipelineItemsFromJSONStream(ctxt *SecurityContext,
    body io.ReadCloser) (error, []PipelineItem) {
    decoder := json.NewDecoder(body)
    var itemsJson PipelineItemsJSONData
    err := decoder.Decode(&itemsJson)
    if err != nil {
        Logger.Println("Can't parse the request " + err.Error())
        return err, nil
    }

    items := make([]PipelineItem, 0)
    for i := 0; i < len(itemsJson.Items); i ++ {
        itemJson := itemsJson.Items[i]
        pipelineItem := new(bioPipelineItem)
        err = pipelineItem.FromJSON(&itemJson)
        if err != nil {
            SchedulerLogger.Errorf("Parse pipeline item failure: %s\n",
                err.Error())
            return err, nil
        }
        err = mgr.ValidateFixUserPipelineItem(ctxt, pipelineItem, false)
        if err != nil {
            SchedulerLogger.Errorf("Validate pipeline item %s fail: %s\n",
                pipelineItem.Name(), err.Error())
            return err, nil
        }
        items = append(items, pipelineItem)
    }
    return nil, items
}

func (mgr *pipelineMgr) ListPipelineItems(ctxt *SecurityContext) (error, []BioflowPipelineItemInfo) {
    mgr.lock.RLock()
    defer mgr.lock.RUnlock()

    pipelineItems := make([]BioflowPipelineItemInfo, 0)
    ns := NSMapFromContext(ctxt)
    nsItems := mgr.pipelineItems[ns]
    if nsItems == nil {
        return nil, pipelineItems
    }

    /*Only list simple item info: name, owner here for privilege*/
    for _, pipelineItem := range nsItems {
        err, pipelineItemInfo := pipelineItem.ToBioflowPipelineItemInfo(false)
        if err != nil {
            SchedulerLogger.Errorf("Get pipeline list fail: %s \n",
                err.Error())
            return err, nil
        }
        pipelineItems = append(pipelineItems, *pipelineItemInfo)
    }

    return nil, pipelineItems
}

func (mgr *pipelineMgr) GetPipelineItemInfo(ctxt *SecurityContext,
    name string) (error, *BioflowPipelineItemInfo) {
    ns := NSMapFromContext(ctxt)
    pipelineItem := mgr.GetPipelineItem(ns, name)

    if pipelineItem == nil {
        SchedulerLogger.Errorf("The pipeline item with name %s/%s not found\n",
            name, ctxt.ID())
        return errors.New("Pipeline item not found"), nil
    }
    if GetUserMgr().IsStrictMode() {
        curUserAccountInfo := ctxt.GetUserInfo()
        /*
         *Security check, if the cur user and the pipeline item owner in the same group,the cur user
         *has privilege to info the pipeline item, otherwise not allowed to do it.
         */
        if !GetUserMgr().CheckGroupPrivilege(ctxt, pipelineItem.SecID(),
            curUserAccountInfo.Username) {
            SchedulerLogger.Errorf("Deny the user %s to info the item because no privilege",
                curUserAccountInfo.Username)
            return errors.New("You have no privilege to view the item"), nil
        }
    }

    return pipelineItem.ToBioflowPipelineItemInfo(true)
}

func (mgr *pipelineMgr) DumpPipelineItemToJSON(ctxt *SecurityContext,
    name string) (error, *PipelineItemsJSONData) {
    ns := NSMapFromContext(ctxt)
    itemsJsonData := &PipelineItemsJSONData{}
    userMgr := GetUserMgr()
    checkPrivilege := userMgr.IsStrictMode()
    userInfo := ctxt.GetUserInfo()
    var items []BioPipelineItemJSONData
    if name != "*" {
        pipelineItem := mgr.GetPipelineItem(ns, name)
        if pipelineItem == nil {
            SchedulerLogger.Errorf("The pipeline item with name %s/%s not found\n",
                name, ctxt.ID())
            return errors.New("Pipeline item not found"), nil
        }
        if checkPrivilege && !userMgr.CheckGroupPrivilege(ctxt, pipelineItem.SecID(),
            userInfo.Username) {
            SchedulerLogger.Errorf("Deny the user %s to dump the item because no privilege",
                userInfo.Username)
            return errors.New("You have no privilege to dump the item"), nil
        }
        err, itemJson := pipelineItem.ToJSON()
        if err != nil {
            return err, nil
        }
        items = append(items, *itemJson)
    } else {
        mgr.lock.RLock()
        for _, pipelineItem := range mgr.pipelineItems[ns] {
            if checkPrivilege && !userMgr.CheckGroupPrivilege(ctxt, pipelineItem.SecID(),
                userInfo.Username) {
                continue
            }
            err, itemJson := pipelineItem.ToJSON()
            if err != nil {
                mgr.lock.RUnlock()
                return err, nil
            }
            items = append(items, *itemJson)
        }
        mgr.lock.RUnlock()
    }
    itemsJsonData.Items = items

    return nil, itemsJsonData
}
