package dataprovider

import (
    . "github.com/xtao/bioflow/scheduler/common"
    . "github.com/xtao/bioflow/blparser"
    "sync"
    "github.com/xtao/bioflow/storage"
    "fmt"
    "strings"
    "github.com/xtao/bioflow/common"
    "github.com/xtao/xcloud/oss/sdk"
)

const (
    DOWNLOADSTAGE string = "DOWNLOAD_OBJECT"
    UPLOADSTAGE string = "UPLOAD_OBJECT"
)

type StageContainer struct {
    next map[string]map[string]bool
    prev map[string]int
    fileToStage map[string]string
    stages map[string]Stage
    dispatched map[string]bool
    lock *sync.RWMutex
}

func NewStageContainer() *StageContainer {
    return &StageContainer{
        next: make(map[string]map[string]bool),
        prev: make(map[string]int),
        fileToStage: make(map[string]string),
        stages: make(map[string]Stage),
        dispatched: make(map[string]bool),
        lock: new(sync.RWMutex),
    }
}

func (container *StageContainer) addFileRecord(file string) {
    container.lock.Lock()
    defer container.lock.Unlock()
    container.fileToStage[file] = ""
}

func (container *StageContainer) addStages(tStage Stage, sStages map[string]Stage, num int) {
    container.lock.Lock()
    defer container.lock.Unlock()
    stages := make(map[string]bool)
    if tStage == nil {
        stages = nil
    }else {
        stages = map[string]bool{
            tStage.GetID(): true,
        }
        container.prev[tStage.GetID()] += num
        container.stages[tStage.GetID()] = tStage
    }

    for file, sStage := range sStages {
        container.next[sStage.GetID()] = stages
        container.fileToStage[file] = sStage.GetID()
        container.stages[sStage.GetID()] = sStage
    }
}

/*
* For a file the stage need, we check whether it be handled already(prepare for other stage).
* Consider the situation 'yes', if the stage created to prepare this file is already finished,
* we just obtain current reference count in 'prev', but if the stage failed, then we will
* create new stage to retry to prepare this file.
*/
func (container *StageContainer) tryAddFileDependence(file string, stage Stage) bool{
    container.lock.Lock()
    defer container.lock.Unlock()
    if id, ok := container.fileToStage[file]; ok {
        srcId := stage.GetID()
        if stages, ok := container.next[id]; ok {
            stages[srcId] = true
            container.prev[srcId]++
        }else {
            if _, ok := container.prev[srcId]; !ok {
                container.prev[srcId] = 0
            }
        }
        container.stages[srcId] = stage
        return true
    }else {
        return false
    }
}

func (container *StageContainer) getStagesByDependence(id string) []string{
    container.lock.RLock()
    defer container.lock.RUnlock()
    var stages []string
    for tId, _ := range container.next[id] {
        stages = append(stages, tId)
    }
    return stages
}

func (container *StageContainer) getReadyStages(try bool) map[string]Stage {
    container.lock.RLock()
    defer container.lock.RUnlock()
    readyStages := make(map[string]Stage)
    for sId, _ := range container.next {
        if _, ok := container.dispatched[sId]; ok {
            continue
        }
        readyStages[sId] = container.stages[sId]
        if !try {
            container.dispatched[sId] = true
        }
    }
    for tId, ref := range container.prev {
        if ref == 0 {
            readyStages[tId] = container.stages[tId]
            if !try {
                delete(container.prev, tId)
                delete(container.stages, tId)
            }
        }
    }

    for id, stage := range readyStages {
        state := stage.State()
        if state != STAGE_INITIAL && state != STAGE_FAIL && state != STAGE_LOST {
            delete(readyStages, id)
        }
    }
    return readyStages
}

func (container *StageContainer) getDataStage(id string) Stage {
    container.lock.RLock()
    defer container.lock.RUnlock()
    _, ok := container.next[id]
    if ok {
        return container.stages[id]
    }else {
        return nil
    }
}

func (container *StageContainer) getSrcStage(id string) Stage {
    container.lock.RLock()
    defer container.lock.RUnlock()

    if _, ok := container.prev[id]; !ok {
        return nil
    }else {
        return container.stages[id]
    }
}

func (container *StageContainer) deleteDependence(id string, deleteFile bool) {
    container.lock.Lock()
    defer container.lock.Unlock()

    for stageId, _ := range container.next[id] {
        container.prev[stageId]--
    }
    delete(container.next, id)
    delete(container.stages, id)
    if deleteFile {
        var deleteFile []string
        for file, stageId := range container.fileToStage {
            if stageId == id {
                deleteFile = append(deleteFile, file)
            }
        }
        for _, file := range deleteFile {
            delete(container.fileToStage, file)
        }
    }

}

func (container *StageContainer) isEmpty() bool {
    container.lock.Lock()
    defer container.lock.Unlock()
    return len(container.stages) == 0
}

func (container *StageContainer) deleteDispatchRecord(id string) {
    container.lock.Lock()
    defer container.lock.Unlock()
    delete(container.dispatched, id)
}

type OsDataProvider struct {
    need int
    job Job
    uploadBucket string
    uploadPrefix string
    logBucket string
    logPrefix string
    uploadFilter []string
    container *StageContainer
    hasFailure bool
    lock *sync.Mutex
}

func NewOsDataProvider(job Job) *OsDataProvider {
    return &OsDataProvider{
        job: job,
        need: -1,
        container: NewStageContainer(),
        hasFailure: false,
        lock: new(sync.Mutex),
    }
}

func (provider *OsDataProvider) NeedPrepare() bool {
    need := provider.need
    if need != -1 {
        if need == 1 {
            return true
        }
        return false
    }
    dataSet := provider.job.InputDataSet()
    need = 0
    files := dataSet.Files()
    inputMap := dataSet.InputMap()
    inputDir := dataSet.InputDir()
    uploadPath := dataSet.RestorePath()
    if needPrepare(inputDir) {
        need = 1
        goto verify
    }
    if needPrepare(uploadPath) {
        need = 1
        goto verify
    }
    for _, file := range files {
        if needPrepare(file) {
            need = 1
            goto verify
        }
    }
    for _, value := range inputMap {
        if needPrepare(value) {
            need = 1
            goto verify
        }
    }

verify:
    provider.need = need
    if need == 1 {
        return true
    }
    return false
}

func (provider *OsDataProvider) GetStageByID(id string) Stage {
    return provider.container.getDataStage(id)
}

func (provider *OsDataProvider) PreHandleInput() (error, ACTION) {
    if !provider.NeedPrepare() {
        return nil, DATA_PROVISON_ACTION_NONE
    }
    dataSet := provider.job.InputDataSet()
    uploadBucket := dataSet.RestorePath()

    if uploadBucket == "" {
        return fmt.Errorf("No restore path set"), DATA_PROVISON_ACTION_ABORT
    }

    err, bucket, prefix := storage.FSUtilsParseOSAddress(uploadBucket)
    if err != nil {
        return err, DATA_PROVISON_ACTION_ABORT
    }

    filter := dataSet.RestoreFilter()
    if filter == nil {
        filter = []string{"*"}
    }

    provider.uploadBucket = bucket
    provider.uploadPrefix = prefix
    provider.uploadFilter = filter

    logPath := dataSet.LogPath()
    if logPath != "" {
        err, bucket, prefix := storage.FSUtilsParseOSAddress(logPath)
        if err != nil {
            return err, DATA_PROVISON_ACTION_ABORT
        }
        provider.logBucket = bucket
        provider.logPrefix = prefix
    }

    /*
    *change os path to fs path in dataset input dir
    */
    dataSet.SetInputDir(provider.job.DataDir())

    err, files := provider.analyzeInput(dataSet)
    if err != nil {
        return err, DATA_PROVISON_ACTION_ABORT
    }
    common.SchedulerLogger.Debugf("Files %+v will be prepared", files)
    if len(files) == 0 {
        return nil, DATA_PROVISON_ACTION_NONE
    }

    for _, filesInOrder := range files {
        err, stages, num := provider.prepareFilesSerially(filesInOrder)
        if err != nil {
            return err, DATA_PROVISON_ACTION_ABORT
        }
        provider.container.addStages(nil, stages, num)
    }
    return nil, DATA_PROVISON_ACTION_SCHEDULE
}

func (provider *OsDataProvider) GetReadyStages(stages map[string]Stage) (map[string]Stage, error) {
    common.SchedulerLogger.Debugf("Data provider check prepare work for stages %+v", stages)
    readyStages := make(map[string]Stage)
    for id, stage := range stages {
        /*
        *One stage maybe be handled already, we ignore this stage.
        */
        if provider.container.getSrcStage(id) != nil {
            common.SchedulerLogger.Debugf("Ignore stage %s because it already be handled.", stage.GetID())
            continue
        }

        filesNeedPrepare := make(map[string]string)
        files := stage.FilesNeedPrepare()

        if len(files) == 0 {
            common.SchedulerLogger.Debugf("Stage %s doesn't need preapre files", stage.GetID())
            readyStages[id] = stage
            continue
        }

        for _, item := range files {
            if !provider.container.tryAddFileDependence(item, stage) {
                common.SchedulerLogger.Debugf("File %s for stage %s already be prepared", item, stage.GetID())
                err, _, objName := storage.FSUtilsParseOSAddress(item)
                if err != nil {
                    return nil, fmt.Errorf("Can't parse path %s", item)
                }
                fileName := storage.FSUtilsGenerateOSIdFromPath(objName)
                filesNeedPrepare[item] = fileName
            }
        }

        if len(filesNeedPrepare) > 0 {
            newStages, num := provider.prepareFiles(filesNeedPrepare)
            provider.container.addStages(stage, newStages, num)
        }
    }

    readyStages = merge(readyStages, provider.container.getReadyStages(false))
    common.SchedulerLogger.Debugf("Stages %+v are ready to be schduled", readyStages)
    return readyStages, nil
}

func (provider *OsDataProvider) HandleStageEvent(stageId string, event STAGEEVENT) []string {
    provider.lock.Lock()
    defer provider.lock.Unlock()
    var stages []string = nil
    switch event {
        case DATA_DONE:
            provider.container.deleteDependence(stageId, false)
            provider.container.deleteDispatchRecord(stageId)
        case DATA_ABORT:
            stages = provider.container.getStagesByDependence(stageId)
            provider.container.deleteDependence(stageId, true)
            provider.hasFailure = true
            provider.container.deleteDispatchRecord(stageId)
        case DATA_LOST, DATA_FAIL, DATA_SUBMIT_FAIL:
            provider.container.deleteDispatchRecord(stageId)
        default:
            stages = nil
    }
    return stages
}

func readDir(dir string) (error, []string) {
    dir = strings.TrimSuffix(dir, "/")
    err, path := storage.GetStorageMgr().MapPathToSchedulerVolumeMount(dir)
    if err != nil {
        return err, nil
    }
    err, files := storage.FSUtilsReadDirFilesRecursively(path)
    if err !=nil {
        return err, nil
    }

    return nil, files
}

func (provider *OsDataProvider) RestoreData() (error, ACTION){
    if provider.need == 0 {
        return nil, DATA_PROVISON_ACTION_NONE
    }
    err, files := readDir(provider.job.WorkDir())
    if err !=nil {
        return err, DATA_PROVISON_ACTION_ABORT
    }
    transmit := make(map[string]string)
    for _, file := range files {
        if BLUtilsMatchStringByPatternsStrict(file, provider.uploadFilter) {
            objectName := file
            objectName = provider.uploadPrefix + objectName
            transmit[objectName] = file
        }
    }

    err, files = readDir(provider.job.LogDir())
    if err !=nil {
        return err, DATA_PROVISON_ACTION_ABORT
    }

    logFileTransmit := make(map[string]string)
    if provider.logBucket != "" {
        for _, file := range files {
            if !strings.HasPrefix(file, DOWNLOADSTAGE) && !strings.HasPrefix(file, UPLOADSTAGE) {
                objectName := file
                objectName = provider.logPrefix + objectName
                logFileTransmit[objectName] = file
            }
        }
        stages, num := provider.restoreFiles(logFileTransmit, provider.logBucket, provider.job.LogDir())
        provider.container.addStages(nil, stages, num)
    }

    if len(transmit) + len(logFileTransmit) == 0 {
        return nil, DATA_PROVISON_ACTION_NONE
    }
    stages, num := provider.restoreFiles(transmit, provider.uploadBucket, provider.job.WorkDir())
    provider.container.addStages(nil, stages, num)
    return nil, DATA_PROVISON_ACTION_SCHEDULE
}

func (provider *OsDataProvider) RecoverFromStage(stage Stage) {
    if stage == nil {
        return
    }
    files := stage.FilesNeedPrepare()
    for _, item := range files {
        common.SchedulerLogger.Infof("Data provider add file record %s", item)
        provider.container.addFileRecord(item)
    }
}

func (provider *OsDataProvider) CheckDataProvision() ACTION {
    provider.lock.Lock()
    defer provider.lock.Unlock()
    if provider.container.isEmpty() {
        if provider.hasFailure == true {
            return DATA_PROVISON_ACTION_PSEUDO_FINISH
        } else {
            return DATA_PROVISON_ACTION_FINISH
        }
    } else {
        stages := provider.container.getReadyStages(true)
        if len(stages) > 0 {
            return DATA_PROVISON_ACTION_SCHEDULE
        }else {
            return DATA_PROVISON_ACTION_NONE
        }
    }
}

func (provider *OsDataProvider) analyzeInput(dataSet *BioDataSet) (error, [][]string) {
    files := dataSet.Files()
    filesInOrder := dataSet.FilesInOrder()
    filesNeedPrepare := filesInOrder
    handled := make(map[string]bool)
    for _, files := range filesInOrder {
        for _, file := range files {
            handled[file] = true
        }
    }

    for _, file := range files {
        if !needPrepare(file) {
            continue
        }

        err, bucket, key := storage.FSUtilsParseOSAddress(file)
        if err != nil {
            return err, nil
        }

        if isPrefix, prefix := storage.FSUtilsIsOSPrefix(key); isPrefix {
            err, list := listObject(bucket, prefix)
            if err != nil {
                return fmt.Errorf("Failed to get object list from %s: %s", file, err.Error()), nil
            }
            common.SchedulerLogger.Debugf("Get object %+v from %s", list, file)
            for _, item := range list {
                item = storage.FSUtilsBuildOSAddressWithPrefix(file, prefix, item)
                if _, ok := handled[item]; !ok {
                    handled[item] = true
                    filesNeedPrepare = append(filesNeedPrepare, []string{item})
                }
            }
        } else {
            if _, ok := handled[file]; !ok {
                handled[file] = true
                filesNeedPrepare = append(filesNeedPrepare, []string{file})
            }
        }
    }
    return nil, filesNeedPrepare
}

func (provider *OsDataProvider) prepareFiles(files map[string]string) (map[string]Stage, int) {
    job := provider.job
    dir := job.DataDir()
    logDir := job.LogDir()
    stages := make(map[string]Stage)
    for src, des := range files {
        _, bucket, object := storage.FSUtilsParseOSAddress(src)
        id := fmt.Sprintf("Stage-Download-%s", object)
        stage := NewDataStage(id, PREPARE, job, dir, logDir)
        stage.SetName(DOWNLOADSTAGE)
        stage.SetBucket(bucket)
        stage.AddTransmitWork(object, des)
        stages[src] = stage
        common.SchedulerLogger.Debugf("Create stage %s for transmit task %s:%s", id, bucket, object)
    }

    return stages, len(files)
}

func (provider *OsDataProvider) prepareFilesSerially(files []string) (error, map[string]Stage, int) {
    job := provider.job
    dir := job.DataDir()
    logDir := job.LogDir()
    stages := make(map[string]Stage)
    id := "Stage-Download"
    aBucket := ""
    for _, file := range files {
        err, bucket, object := storage.FSUtilsParseOSAddress(file)
        if err != nil {
            return err, nil, 0
        }
        if aBucket != "" && aBucket != bucket {
            return fmt.Errorf("Not same bucket of files in arry %+v", files), nil, 0
        }
        aBucket = bucket
        id += "-" + object
    }
    stage := NewDataStage(id, PREPARE, job, dir, logDir)
    stage.SetWorkSerially()
    stage.SetName(DOWNLOADSTAGE)
    stage.SetBucket(aBucket)

    for _, file := range files {
        _, _, object := storage.FSUtilsParseOSAddress(file)

        fileName := storage.FSUtilsGenerateOSIdFromPath(object)
        stage.AddTransmitWork(object, fileName)
        stages[file] = stage
    }

    common.SchedulerLogger.Debugf("Create stage %s for transmit task %+v", stage.GetID(), files)
    return nil, stages, 1
}

func (provider *OsDataProvider) restoreFiles(transmit map[string]string, bucket string, dir string) (map[string]Stage, int) {
    stages := make(map[string]Stage)
    job := provider.job
    logDir := job.LogDir()
    for object, file := range transmit {
        id := fmt.Sprintf("Stage-Upload-%s", object)
        stage := NewDataStage(id, RESTORE, job, dir, logDir)
        stage.SetName(UPLOADSTAGE)
        stage.SetBucket(bucket)
        stage.AddTransmitWork(object, file)
        stages[file] = stage
        common.SchedulerLogger.Infof("Create stage %s for transmit task %s -> %s:%s", id, file, provider.uploadBucket, object)
    }
    return stages, len(transmit)
}

func needPrepare(file string) bool {
    return storage.FSUtilsIsOSPath(file)
}

func merge(des map[string]Stage, src map[string]Stage) map[string]Stage {
    for id, stage := range src {
        des[id] = stage
    }
    return des
}

func listObject(bucket, prefix string) (error, []string) {
    etcdEndPoint := strings.Join(common.EtcdEndPoints, ETCDENDPOINTSEP)
    return sdk.ListObject(bucket, prefix, etcdEndPoint)
}
