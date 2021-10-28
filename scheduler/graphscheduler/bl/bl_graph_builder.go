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
package bl

import (
    "fmt"
    "strings"
    "errors"
    "io/ioutil"
    "encoding/json"
    "strconv"
    "sort"
    "github.com/satori/go.uuid"
    . "github.com/xtao/bioflow/blparser"
    . "github.com/xtao/bioflow/common"
    . "github.com/xtao/bioflow/message"
    . "github.com/xtao/bioflow/storage"
    . "github.com/xtao/bioflow/scheduler/common"
)

/*Max file name in linux is 255, reserved 16 for filter*/
const MAX_FILENAME_SIZE int = 239
/*
 * The branch is the core concept of graph builder and bioflow
 * pipeline. The branch is defined to abstract a sequential
 * execution stream of a slice of pipeline. It holds all the context
 * information of the execution stream:
 * 1) name:  each branch is assigned a unique name to distinguish other
 *    branch. so if same pipeline run on same input file, each run is 
 *    a branch and has different name.
 * 2) branch variables: branch variable works the same way as thread local
 *    storage, which means same named variable is assigned different value
 *    in different branch.
 * 3) output map: it holds the named output information for the branch. if
 *    node1 produces output named by $output.bam, then the output map is 
 *    modified to map bam <--> node1. then all subsequent node which reference
 *    $input.bam will reference node1's output. But at some time, if a node
 *    node2 produces $output.bam, then the mapping becomes bam <----> node2.
 * 4) inputDir/dirFiles: it holds the input files and directory. It is always
 *    used by first pipeline item definied by "ShardFiles".
 * 5) : holds the key<--->value pair defined by user in pipeline or 
 *    job. The difference between branch variable is that they are same for
 *    all branches. So don't modify them in each branch because it is shared
 *    by all branches.
 * 6) prevCtxts: it holds all previous sub branch of current branch. it is used
 *    to handle the merge stage in divide and conquer execution pattern. Please
 *    refer the following execution graph:
 *
 *         lane1-pair1 lane1-pair2   lane2-pair1 lane2-pair2    lane3-pair1 lane3-pair2
 *                bwa mem                   bwa mem                     bwa mem   
 *                  |                         |                            |
 *                picard reorder            picard reorder              picard reorder 
 *                  |                         |                            |
 *                samtools view             samtools view               samtools view
 *                  |                         |                            |
 *                picard sort               picard sort                 picard sort
 *                  |                         |                            |
 *                  ________________________________________________________    
 *                                            |
 *                                        picard mergesamfiles
 *                                            |
 *                                        gatk indelrealign
 *                                            |
 *                                            .
 *                                            .
 *                                        gatk print
 *                                            |
 *                               _________________________________
 *                              |                                 |
 *                           gatk genotype                  gatk mutect2
 *
 *       In the above execution graph. The whole execution itself is a branch which is the root branch.
 *     in the bwa mem stage, the "ShardFiles" generate 3 child branches to run in parallel. Each branch 
 *     has its own context and can't affect each other. when picard sort done, 3 child branches complete
 *     and merge to the root branch again. So at the point of "picard mergesamfiles", the root branch's
 *     previous branches are the 3 child branches. So the merge stage can collect and reference all the
 *     named output produced in the 3 child branches.
 *
 *
 */
type branchContext struct {
    /*name of the branch, unique, it is
     *sample name by default
     */
    name string

    /* sample of the branch, set by
     * user in the job 
     */
    sampleName string

    /*
     * branchVariableMap holds the branch variables
     * for the branch. It can be modified,
     * and other branch can't see it.
     */
    branchVariableMap map[string]string

    /*
     * output file map holds named output file for
     * each branch. it is not shared.
     */
    outputFileMap *NamedNodeMap

    /*
     * output directory map holds named output dir for
     * each branch. it is not shared.
     */
    outputDirMap *NamedNodeMap

    workDir string
    hdfsWorkDir string
    logDir string

    /*holds the input directory and files*/
    inputDir string
    dirFiles []string

    /*
     * previous branches of current branch. it
     * is used by merge stage to collect inputs.
     */
    prevCtxts []*branchContext
}

func NewBranchContext() *branchContext {
    return &branchContext{
            outputFileMap: NewNamedNodeMap(),
            outputDirMap: NewNamedNodeMap(),
            branchVariableMap: make(map[string]string),
            prevCtxts: nil,
            name: "branch",
    }
}

func (ctxt *branchContext) GetBranchVariable(name string) string {
    if val, ok := ctxt.branchVariableMap[name]; ok {
        return val
    }

    BuilderLogger.Errorf("No variable named %s exist in branch %s \n", 
        name, ctxt.name)

    return ""
}

func (ctxt *branchContext) SetBranchVariable(name string, val string) {
    ctxt.branchVariableMap[name] = val
}

/*
 * warning!!!The func important.
 * Do not easily modify the func if you do not fully understand the merge rule with bsl.
 * In bsl, there is the following merge rules:
 * 1.The clone child branch does not merge back into the parent
 *   branch by default if it has multiple identical tags;
 * 2.The shard and variable child branch merge back into the parent
 *   branch by default no matter if it has mutiple identical tags or not;
 * 3.In the serial process, the tag generated after always overrides the
 *   same tag in front. The same is true for child and parent branches;
 * 4.In clone the forwardTag takes precedence over discardTag,and it is
 *   the opposite in the shard and variable
 * 5.discardtag and forwardtag we support type *,and discard * takes precedence
 *   over forward *.
 */
func (ctxt *branchContext) MergeBranchOutput(output []*NamedNodeMap,
    discardTagSlice *tagSlice, forwardTagSlice *tagSlice, mergeSameTag bool) map[string][]Node {
    outputBranchCount := make(map[string]int)
    mergeBranchOutputNodesMap := make(map[string][]Node)
    mergeParentOutputNodesMap := make(map[string][]Node)

    for i := 0; i < len(output); i ++ {
        for name, nodes := range output[i].nameNodeMap {
            /*
             * In bsl, the output of the parent branch will merge
             * into the sub branch,at the end of the sub branch and
             * we use the sub branch's output of the same parent tag
             * replace the parent's output, the branch replacement mark is set to true.
             * 1) replacementMark false: it means its associated output is the
             *    parent branch merge to the sub branch, needn't merge back;
             * 2) replacemnetMark true: output is generated by sub branch,use the sub branch's
             *    output of the same parent tag replace the parent's output.
             */
            if nodes.GetReplacementMark() {
                if _, ok := mergeBranchOutputNodesMap[name]; ok {
                    outputBranchCount[name] ++
                    mergeBranchOutputNodesMap[name] = append(mergeBranchOutputNodesMap[name], nodes.nodes ...)
                } else {
                    outputBranchCount[name] = 1
                    mergeBranchOutputNodesMap[name] = nodes.nodes
                }
            } else {
                if _, ok := mergeParentOutputNodesMap[name]; !ok {
                    mergeParentOutputNodesMap[name] = nodes.nodes
                }
            }
        }
    }

    /*
     * if discard is *, any child tag does not merge back into the parent.
     */
    if discardTagSlice.InSlice("*") {
        return mergeParentOutputNodesMap
    }

    mergeOutputNodesMap := make(map[string][]Node)
    /* Append sub branch not generate tag from parent branch */
    for name, nodes := range mergeParentOutputNodesMap {
        if _, ok := mergeBranchOutputNodesMap[name]; !ok {
            mergeOutputNodesMap[name] = append(mergeOutputNodesMap[name], nodes ...)
        }
    }
    /*
     * Append sub branch generate tag from sub branch,
     * override the same tag generated by the parent branch.
     */
    for name, nodes := range mergeBranchOutputNodesMap {
        mergeOutputNodesMap[name] = append(mergeOutputNodesMap[name], nodes ...)
    }
    /* if forward is *,any child tag merge back into parent*/
    if forwardTagSlice.InSlice("*") {
        return mergeOutputNodesMap
    }
    /*
     * In the case of clone item, the tag with the same branch does not merge back
     * to the parent branch, while the tag with the same branch of shard/variable item will
     * automatically merge back to the parent branch.
     */
    if !mergeSameTag {
        falseTagOutputNodesMap := make(map[string][]Node)
        for name, nodes := range mergeParentOutputNodesMap {
            falseTagOutputNodesMap[name] = append(falseTagOutputNodesMap[name], nodes ...)
        }
        for name, nodes := range mergeBranchOutputNodesMap {
            if outputBranchCount[name] == 1 || forwardTagSlice.InSlice(name) {
                if _, ok := falseTagOutputNodesMap[name]; ok {
                    delete(falseTagOutputNodesMap, name)
                }
                falseTagOutputNodesMap[name] = append(falseTagOutputNodesMap[name], nodes ...)
            }
        }
        return falseTagOutputNodesMap
    } else {
        /*
         * mergeOutputNodesMap hold the tags including all tags generated
         * by sub branch, while those by parent are not the same with sub branch.
         */
        for name, _ := range mergeOutputNodesMap {
            if discardTagSlice.InSlice(name) {
                delete(mergeOutputNodesMap, name)
                /* if discard sub branch tag, and parent has it,use the parent */
                if _, ok := mergeParentOutputNodesMap[name]; ok {
                    mergeOutputNodesMap[name] = mergeParentOutputNodesMap[name]
                }

            }
        }
        return mergeOutputNodesMap
    }
}

func (ctxt *branchContext) MergeOutputsFromChildBranch(ctxts []*branchContext,
    discardTagName []string, forwardTagName []string, mergeSameTag bool) {
    discardTagSlice := NewTagSlice(discardTagName)
    forwardTagSlice := NewTagSlice(forwardTagName)

    outputFile := make([]*NamedNodeMap, 0)
    outputDir := make([]*NamedNodeMap, 0)
    for i := 0; i < len(ctxts); i ++ {
        outputFile = append(outputFile, ctxts[i].outputFileMap)
        outputDir = append(outputDir, ctxts[i].outputDirMap)
    }
    mergeOutputFileNodesMap := ctxt.MergeBranchOutput(outputFile,
        discardTagSlice, forwardTagSlice, mergeSameTag)
    for name, nodes := range mergeOutputFileNodesMap {
        ctxt.outputFileMap.ReplaceNameNodes(name, nodes)
    }
    mergeOutputDirNodesMap := ctxt.MergeBranchOutput(outputDir,
        discardTagSlice, forwardTagSlice, mergeSameTag)
    for name, nodes := range mergeOutputDirNodesMap {
        ctxt.outputDirMap.ReplaceNameNodes(name, nodes)
    }
}

func (ctxt *branchContext) CloneBranchContext(cloneCtxt *branchContext) {
    ctxt.name = cloneCtxt.name
    ctxt.sampleName = cloneCtxt.sampleName
    ctxt.branchVariableMap = make(map[string]string)
    for key, val := range cloneCtxt.branchVariableMap {
        ctxt.branchVariableMap[key] = val
    }
    ctxt.outputFileMap = NewNamedNodeMap()
    for name, nodes := range cloneCtxt.outputFileMap.nameNodeMap {
        ctxt.outputFileMap.AddNamedNodes(name, nodes.nodes)
    }

    ctxt.outputDirMap = NewNamedNodeMap()
    for name, nodes := range cloneCtxt.outputDirMap.nameNodeMap {
        ctxt.outputDirMap.AddNamedNodes(name, nodes.nodes)
    }
    ctxt.workDir = cloneCtxt.workDir
    ctxt.dirFiles = cloneCtxt.dirFiles

    /*
     * Clone branches's previous context. This will make the
     * children branches can access the main branch's previous
     * children branches' input/out map
     */
    ctxt.prevCtxts = cloneCtxt.prevCtxts
}


func (ctxt *branchContext) SetPrevBranchContext(ctxts []*branchContext){
    ctxt.prevCtxts = ctxts
}

func (ctxt *branchContext) GetPrevBanchContext() []*branchContext {
    return ctxt.prevCtxts
}

func (ctxt *branchContext) SetWorkDir(workDir string) {
    ctxt.workDir = workDir
}

func (ctxt *branchContext) SetHDFSWorkDir(workDir string) {
    ctxt.hdfsWorkDir = workDir
}

func (ctxt *branchContext) SetLogDir(logDir string) {
    ctxt.logDir = logDir
}

func (ctxt *branchContext) SetInputDir(inputDir string) {
    ctxt.inputDir = inputDir
}

func (ctxt *branchContext) SetDirFiles(files []string) {
    ctxt.dirFiles = files
}

func (ctxt *branchContext) GetDirFiles() []string {
    return ctxt.dirFiles
}

func (ctxt *branchContext) WorkDir() string {
    return ctxt.workDir
}

func (ctxt *branchContext) HDFSWorkDir() string {
    return ctxt.hdfsWorkDir
}

func (ctxt *branchContext) LogDir() string {
    return ctxt.logDir
}

func (ctxt *branchContext) InputDir() string {
    return ctxt.inputDir
}

func (ctxt *branchContext) Name() string {
    return ctxt.name
}

func (ctxt *branchContext) SetName(name string) {
    ctxt.name = name
}

func (ctxt *branchContext) SampleName() string {
    return ctxt.sampleName
}

func (ctxt *branchContext) SetSampleName(sampleName string) {
    ctxt.sampleName = sampleName
}

func (ctxt *branchContext) GetBranchVarMap() map[string]string {
    return ctxt.branchVariableMap
}

func (ctxt *branchContext) MapOutputFileToNode(name string, node Node) {
    ctxt.outputFileMap.ReplaceNameNode(name, node)
}

func (ctxt *branchContext) GetNamedOutputFileNode(name string) []Node {
    return ctxt.outputFileMap.GetNamedNodes(name)
}

func (ctxt *branchContext) ResetNamedOutputFileMap() {
        ctxt.outputFileMap.DeleteAllNamedNodes()
}

func (ctxt *branchContext) MapOutputDirToNode(name string, node Node) {
    ctxt.outputDirMap.ReplaceNameNode(name, node)
}

func (ctxt *branchContext) GetNamedOutputDirNode(name string) []Node{
    return ctxt.outputDirMap.GetNamedNodes(name)
}

func (ctxt *branchContext) BuildFullFilePath(files []string) {
    for i := 0; i < len(files); i ++ {
        files[i] = ctxt.inputDir + files[i]
    }
}

/*
 * Graph builder is core component of bioflow to combine pipeline definition
 * and input dataset to generate a directed acyclic graph. Each node of the 
 * graph is a "stage" which will be executed by a docker instance. The bio
 * graph builder is data-driven. So it needs parse all files of input directory  
 * to generate streams of computation. The following execution pattern should 
 * be supported:
 * 1) Sequential: run a series of toolkits on a input file. each step is transofmed
 *    to a "stage" and becomes a node of flow graph. the later stage may need use
 *    the output of some earlier stage as input. the input/output association is 
 *    tracked by stage data structure.
 * 2) ShardFiles: filter files under input directory by a MatchPattern, and then divide
 *    them into file groups by GroupPattern. The ShardFiles will specify a sub pipeline,
 *    it launches separate sequential execution stream (which is called "branch") of the
 *    sub pipeline for each file group.  Shardfiles is used to handle the situation that
 *    a sequencing machine may have several lanes and each lane will output a sequencing 
 *    output (e.g, a read pair) for the sample. We need run the bwa pipeline on each lane 
 *    independently and create a BAM file for each lane. 
 * 3) CloneParallel: sometimes we may need run different variation calling approaches for
 *    same BAM file of same Sample. The CloneParrallel is used to launch two sub pipeline
 *    independently on same input file. each sub pipeline is a series of pipeline items.
 * 4) VariableParallel: in bioinformatics, we may need run same pipeline on same input data
 *    with different variable. For example, a input file may have data for many chrosomes.
 *    The pipeline only handles on chrosome each time. The VariableParallel is used to launch
 *    many parallel execution stream of same pipeline on same input file. But each execution
 *    stream is assigned different value for a branch variable.
 *
 * From the principle described above, the graph builder works as follows:
 * 1) branchGraphBuilder: parent graph builder for seqGraphBuilder and holds basic context
 *    information. It doesn't involve graph building work.
 * 2) seqGraphBuilder is basic graph builder. It generates a sequential execution of streams,
 *    which is a path of Nodes in the flow graph. Each node will be executed in order and the
 *    later one will reference the output of the earlier one. In one word, seqGraphBuilder is
 *    responsible for building a subgraph consists of a path. It has unique start node and end
 *    node. there is one and only one edge between adajcent nodes.
 *
 */

/*
 * branchGraphBuilder is parent object for each graph builder. 
 * It holds the context information. So each graph builder will have
 * its own context. But someone will clone its context from others when
 * it is created.
 */
type branchGraphBuilder struct {
    /*holds the context of the branch*/
    branchCtxt *branchContext

    /*
     *each graph will output one or more nodes. These nodes
     *are the union of output nodes of its sub graph builder.
     *they are always the end nodes of a path in the flow graph.
     */
    outputNodes []Node

    /*
     *each graph may have one or more nodes as prevNodes. All
     *the nodes created by this graph will depend on the prevNodes
     *and only be executed after their prevNodes completed execution.
     *they are always the outputNodes of the sub graph builder of the
     *preceding sub pipeline.
     */
    prevNodes []Node

    /*data structure to represent a directed graph*/
    graph Graph

    /*
     *each graph builder is assigned a slice of pipeline.
     *it will build the sub graph for the sub pipeline.its
     *parent graph builder will combine the sub graph to the
     *whole flow graph.
     */
    pipelineItems map[int]PipelineItem

    job Job
}

func (builder *branchGraphBuilder) PrevNodes() []Node {
    return builder.prevNodes
}

func (builder *branchGraphBuilder) SetPrevNodes(nodes []Node) {
    builder.prevNodes = nodes
}

func (builder *branchGraphBuilder) Job() Job {
    return builder.job
}

func (builder *branchGraphBuilder) ItemsCount() int {
    return len(builder.pipelineItems)
}

func (builder *branchGraphBuilder) SetDirFiles(files []string) {
    builder.branchCtxt.SetDirFiles(files)
}

/*
 * If the graph builder is responsible for building the first
 * sub pipeline. They don't have inputs which are the output of
 * some earlier pipeline. The input files are from:
 * 1) file list user specified in the job definition. The ShardFiles
 *    group these files and feed them to the graph builder as input.
 * 2) input directory is specified. The ShardFiles will read file names
 *    from the directory, group them and feed them as input of graph
 *    builder.
 */
func (builder *branchGraphBuilder) GetDirFiles() (error, []string) {
    files := builder.branchCtxt.GetDirFiles()
    var err error = nil
    if files == nil || len(files) == 0 {
        err, files = builder.ReadDirFiles()
    }

    return err, files
}

/*
 * The files are specified by input directory and file names. 
 * during execution, we need use the full path name. So this
 * routine will produce the full path name.
 */
func (builder *branchGraphBuilder) GetDirFullPathFiles() (error, []string) {
    files := builder.branchCtxt.GetDirFiles()
    var err error = nil
    if files == nil || len(files) == 0 {
        err, files = builder.ReadDirFiles()
    }
    if err != nil {
        return err, nil
    }

    fullFiles := make([]string, len(files), len(files))
    for i := 0; i < len(files); i ++ {
        fullFiles[i] = builder.branchCtxt.inputDir + "/" + files[i]
    }

    return nil, fullFiles
}

/*
 * read file names from directory of storage. Be noted that the
 * the vol@cluster:fullpath need be transformed to full path name
 * relative to the scheduler mount points. Storage manager will
 * help on the transformation.
 */
func (builder *branchGraphBuilder) ReadDirFiles() (error, []string) {
    branchCtxt := builder.BranchContext()
    if branchCtxt.InputDir() == "" {
        return errors.New("No input directory"), nil
    }

    err, dirPath := GetStorageMgr().MapPathToSchedulerVolumeMount(
        branchCtxt.InputDir())
    if err != nil {
        BuilderLogger.Errorf("Can't map path %s to scheduler volume mount:%s\n",
            branchCtxt.InputDir(), err.Error())
        return err, nil
    }

    err, files := FSUtilsReadDirFiles(dirPath)
    if err != nil {
        BuilderLogger.Errorf("Read directory files error: %s\n",
            err.Error())
        return err, nil
    }

    return nil, files
}

/*read a file line by line, each line is added to a list*/
func (builder *branchGraphBuilder) ReadLines(file string) (error, []string) {
    err, filePath := GetStorageMgr().MapPathToSchedulerVolumeMount(
        file)
    if err != nil {
        BuilderLogger.Errorf("Can't map path %s to scheduler volume mount:%s\n",
            file, err.Error())
        return err, nil
    }

    return FSUtilsReadLines(filePath)
}

func ParseJSONDataToFileList(jsonData interface{}) (bool, []string) {
    if jsonData == nil {
        return false, nil
    }

    valid := true
    var files []string
    switch jsonData.(type) {
        case []interface{}:
            for _, value := range jsonData.([]interface{}) {
                switch value.(type) {
                    case string:
                        files = append(files, value.(string))
                    default:
                        valid = false
                }
            }
        default:
            valid = false
    }

    return valid, files
}

type ShardFileGroup struct {
    key string
    files []string
}

/*read shard file groups from a JSON file*/
func (builder *branchGraphBuilder) ReadShardFilesJSONFile(file string) (error, []ShardFileGroup) {
    err, filePath := GetStorageMgr().MapPathToSchedulerVolumeMount(
        file)
    if err != nil {
        BuilderLogger.Errorf("Can't map path %s to scheduler volume mount:%s\n",
            file, err.Error())
        return err, nil
    }

    raw, err := FSUtilsReadFile(filePath)
    if err != nil {
        BuilderLogger.Errorf("Read shard file %s fail: %s\n",
            filePath, err.Error())
        return err, nil
    }

    var shardGroups []ShardFileGroup
    /*
     * Try the V1 format first
     */
    jsonData := make(map[string]interface{})
    err = json.Unmarshal(raw, &jsonData)
    if err != nil {
        BuilderLogger.Warnf("Fail load shard files as JSON data V1, try V2: %s",
            err.Error())
        return err, shardGroups
    }

    /*
     * The shard file list can be read from a json file as follows:
     * {
     *   "files" : [
     *        ["file1", "file2"],
     *        ["file3", "file4"],
     *        ...
     *    ]
     * }
     *
     * or 
     *
     * {
     *   "tumor" : {
     *        "files" : ["file1", "file2"]
     *     }
     *   "normal" : {
     *        "files" : ["file3", "file4"]
     *     }
     *        ...
     * }
     */

    filesGroup := make(map[string][]string)
    for key, value := range jsonData {
        if strings.ToUpper(key) == "FILES" {
            /*Parse it as the first format*/
            files, isFiles := value.([]interface{}) 
            if isFiles {
                var fileArrayList [][]string
                for _, arrayItem := range files {
                    isValid, itemList := ParseJSONDataToFileList(arrayItem)
                    if isValid {
                        fileArrayList = append(fileArrayList, itemList)
                    }
                }
                return nil, BuildFilesGroupFromArray(fileArrayList)
            }
        }

        /*Try the V2 format*/
        validFormat := false
        v2GroupFiles, isV2Group := value.(map[string]interface{})
        if !isV2Group {
            return errors.New("File not Shard Group JSON format"),
                shardGroups
        }
        for itemKey, itemValue := range v2GroupFiles {
            if strings.ToUpper(itemKey) == "FILES" {
                isValid, files := ParseJSONDataToFileList(itemValue)
                if isValid {
                    validFormat = true
                    filesGroup[key] = files
                }
            }
        }

        if !validFormat {
            return errors.New("File not Shard Group JSON format"),
                shardGroups
        }
    }

    /*
     * For the V2 Format, the order of key-value pairs may be different when read
     * from JSON file. So we need sort here to build the shard group list.
     */
    shardKeys := make([]string, 0)
    for key, _ := range filesGroup {
        shardKeys = append(shardKeys, key)
    }
    sort.Strings(shardKeys)
    for _, key := range shardKeys {
        shardGroup := ShardFileGroup{
            key: key,
            files: filesGroup[key],
        }
        shardGroups = append(shardGroups, shardGroup)
    }

    return nil, shardGroups
}

func (builder *branchGraphBuilder) Item(i int) PipelineItem {
    return builder.pipelineItems[i]
}

func (builder *branchGraphBuilder)Graph() Graph {
    return builder.graph
}

func (builder *branchGraphBuilder)OutputNodes() []Node{
    return builder.outputNodes
}

func (builder *branchGraphBuilder)SetOutputNodes(nodes []Node) {
    builder.outputNodes = nodes
}

func (builder *branchGraphBuilder) AddStageOutputFileToMap(name string,
    node Node) {
    /*do nothing if name is empty*/
    if name != "" {
        builder.branchCtxt.MapOutputFileToNode(name, node)
    }
}

func (builder *branchGraphBuilder)GetNamedOutputFileNode(name string) []Node {
    return builder.BranchContext().GetNamedOutputFileNode(name)
}

func (builder *branchGraphBuilder) AddStageOutputDirToMap(name string,
    node Node) {
    if name != "" {
        builder.branchCtxt.MapOutputDirToNode(name, node)
    }
}

func (builder *branchGraphBuilder)GetNamedOutputDirNode(name string) []Node {
    return builder.BranchContext().GetNamedOutputDirNode(name)
}

func (builder *branchGraphBuilder) CloneMapContext(cloneBuilder *branchGraphBuilder) {
    builder.BranchContext().CloneBranchContext(cloneBuilder.BranchContext())
}

func (builder *branchGraphBuilder) BranchContext() *branchContext {
    return builder.branchCtxt
}

/*
 * Handle get previous named output nodes exist in two cases:
 * 1) The stage is the one following a Parallel (e.g, Shard, Clone, ...) stage,
 *    so get the outputs from all previous branches
 * 2) The stage is not the first one after parallel stage, get the output from
 *    stages (after parallel stages) of current branch first. If not found, get
 *    from previous branches. But never from the stages before the shard stage
 */
func (builder *branchGraphBuilder) GetAllPrevOutputFileNodes(name string) []Node {
    return builder.GetNamedOutputFileNode(name)
}

func (builder *branchGraphBuilder) GetAllPrevOutputDirNodes(name string) []Node {
    return builder.GetNamedOutputDirNode(name)
}

/*
 * The graph builder is the basic graph builder. It receives a slice of pipeline
 * and produce a path of nodes for it. Each node is associated a stage. The stage
 * is created according to the information of pipeline item. It will create the 
 * specific execution commands, enviroment, input/output when scheduled to 
 * execution.
 */
type seqGraphBuilder struct {
    branchGraphBuilder

    /*
     * The pipeline may combine a sub pipeline, so:
     * 1) main pipeline is level 0
     * 2) child pipeline of main is level 1
     * 3) child pipeline level =  parent pipeline level + 1
     * the graph builder's level is same with the pipeline
     * it is working on
     */
    level int

    /*
     * The resumeBuild label hold cur seqGraphBuilder if suspend first time,
     * it is used for judge resume build graph
     */
    resumeBuild bool

    /*
     * The item index building now. it is stored here to
     * resume build graph next time
     */
    itemPos int
    buildPos int

    /*
     * A list to save sub graph builders
     */
    childGraphbuilders *BlChildGraphBuilderTree
    /*
     * A flag to indicate what kind of sub graph is built:
     * 1) shard:  it is used to handle a single file group
     * 2) clone:  to build graph for a new indepedent sub pipeline
     * 3) variable parallel: it is to handle pipeline for one branch variable
     * 4) main: it is to build the main pipeline
     */
    graphType int

    /*the pipeline to build*/
    pipeline Pipeline
}

const (
    SG_TYPE_MAIN int = 1
    SG_TYPE_SHARD int = 2
    SG_TYPE_CLONE int = 3
    SG_TYPE_VARIABLE int = 4
    SG_TYPE_PIPELINE int = 5
)

func NewSeqGraphBuilder(graph Graph, items map[int]PipelineItem, 
    job Job, graphType int) *seqGraphBuilder {
    builder := &seqGraphBuilder{
                branchGraphBuilder: branchGraphBuilder{
                    graph: graph,
                    pipelineItems: items,
                    job: job,
                },
                level: 0,
                resumeBuild: false,
                itemPos: 0,
                buildPos: 0,
                graphType: graphType,
                childGraphbuilders: NewBlSubGraphBuiler(),
    }
    builder.branchGraphBuilder.branchCtxt = NewBranchContext() 

    return builder
}

func (builder *seqGraphBuilder)SetResumeBuild(isResumeBuild bool) {
    builder.resumeBuild = isResumeBuild
}

func (builder *seqGraphBuilder)GetResumeBuild() bool {
    return builder.resumeBuild
}

func (builder *seqGraphBuilder)ChildGraphBuilerTree() *BlChildGraphBuilderTree {
    return builder.childGraphbuilders
}

func (builder *seqGraphBuilder)SetPipeline(pipeline Pipeline) {
    builder.pipeline = pipeline
}

func (builder *seqGraphBuilder)GetPipeline() Pipeline {
    return builder.pipeline
}

func (builder *seqGraphBuilder)IsMainGraphBuilder() bool {
    return builder.graphType == SG_TYPE_MAIN
}

func (builder *seqGraphBuilder)IsShardGraphBuilder() bool {
    return builder.graphType == SG_TYPE_SHARD
}

func (builder *seqGraphBuilder)IsCloneGraphBuilder() bool {
    return builder.graphType == SG_TYPE_CLONE
}

func (builder *seqGraphBuilder)IsVariableGraphBuilder() bool {
    return builder.graphType == SG_TYPE_VARIABLE
}

func (builder *seqGraphBuilder)Level() int {
    return builder.level
}

func (builder *seqGraphBuilder)SetLevel(level int) {
    builder.level = level
}

func (builder *seqGraphBuilder)CurrentItemPos() int {
    return builder.itemPos
}

func (builder *seqGraphBuilder)SetItemPos(pos int) {
    builder.itemPos = pos
}

func (builder *seqGraphBuilder)CurrentBuildPos() int {
    return builder.buildPos
}

func (builder *seqGraphBuilder)SetBuildPos(pos int) {
    builder.buildPos = pos
}

func (builder *seqGraphBuilder) EvaluateResourceSpec(item PipelineItem, stage Stage) (error, *BuildStageErrorInfo) {
    /*If the resource is variable, Gets the actual value*/
    var cpu float64 = -1
    var memory float64 = -1
    var disk float64 = -1
    var gpu float64 = -1
    var gpuMemory float64 = -1
    var err error
    resourceSpecExpression := item.ResourceSpecExpression()
    if resourceSpecExpression.Cpu != "" {
        buildStageErrInfo, cpuString := builder.EvaluateExpression(resourceSpecExpression.Cpu)
        if buildStageErrInfo != nil {
            BuilderLogger.Errorf("Can't evaluate the expression %s: %s\n",
                resourceSpecExpression.Cpu, buildStageErrInfo.Err.Error())
            return errors.New("Fail to parse cpu " + resourceSpecExpression.Cpu), buildStageErrInfo
        }
        cpu, err = strconv.ParseFloat(cpuString, 64)
        if err != nil {
            BuilderLogger.Errorf("Fail to parse cpu to float64 " + cpuString)
            return errors.New("Fail to parse cpu to float64 " + cpuString), nil
        }
    }
    if resourceSpecExpression.Memory != "" {
        buildStageErrInfo, memString := builder.EvaluateExpression(resourceSpecExpression.Memory)
        if buildStageErrInfo != nil {
            BuilderLogger.Errorf("Can't evaluate the expression %s: %s\n",
                resourceSpecExpression.Memory, buildStageErrInfo.Err.Error())
            return errors.New("Fail to parse Memory " + resourceSpecExpression.Memory), buildStageErrInfo
        }
        memory, err = strconv.ParseFloat(memString, 64)
        if err != nil {
            BuilderLogger.Errorf("Fail to parse memory to float64 " + memString)
            return errors.New("Fail to parse memory to float64 " + memString), nil
        }
    }
    if resourceSpecExpression.Disk != "" {
        buildStageErrInfo, diskString := builder.EvaluateExpression(resourceSpecExpression.Disk)
        if buildStageErrInfo != nil {
            BuilderLogger.Errorf("Can't evaluate the expression %s: %s\n",
                resourceSpecExpression.Disk, buildStageErrInfo.Err.Error())
            return errors.New("Fail to parse Disk " + resourceSpecExpression.Disk), buildStageErrInfo
        }
        disk, err = strconv.ParseFloat(diskString, 64)
        if err != nil {
            BuilderLogger.Errorf("Fail to parse disk to float64 " + diskString)
            return errors.New("Fail to parse disk to float64 " + diskString), nil
        }
    }
    if resourceSpecExpression.GPU != "" {
        buildStageErrInfo, gpuString := builder.EvaluateExpression(resourceSpecExpression.GPU)
        if buildStageErrInfo != nil {
            BuilderLogger.Errorf("Can't evaluate the expression %s: %s\n",
                resourceSpecExpression.GPU, buildStageErrInfo.Err.Error())
            return errors.New("Fail to parse GPU " + resourceSpecExpression.GPU), buildStageErrInfo
        }
        gpu, err = strconv.ParseFloat(gpuString, 64)
        if err != nil {
            BuilderLogger.Errorf("Fail to parse gpu to float64 " + gpuString)
            return errors.New("Fail to parse disk to float64 " + gpuString), nil
        }
    }
    if resourceSpecExpression.GPUMemory != "" {
        buildStageErrInfo, gpuMemoryString := builder.EvaluateExpression(resourceSpecExpression.GPUMemory)
        if buildStageErrInfo != nil {
            BuilderLogger.Errorf("Can't evaluate the expression %s: %s\n",
                resourceSpecExpression.GPUMemory, buildStageErrInfo.Err.Error())
            return errors.New("Fail to parse GPU Memory " + resourceSpecExpression.GPUMemory),
                buildStageErrInfo
        }
        gpu, err = strconv.ParseFloat(gpuMemoryString, 64)
        if err != nil {
            BuilderLogger.Errorf("Fail to parse gpu memory to float64 " + gpuMemoryString)
            return errors.New("Fail to parse gpu memory to float64 " + gpuMemoryString), nil
        }
    }
    stage.MergeResourceSpec(cpu, memory, disk, gpu, gpuMemory)

    return nil, nil
}

func (builder *seqGraphBuilder)BuildFileGroupFromFile(item PipelineItem,
    files []string)(error, []ShardFileGroup, *BuildStageErrorInfo) {
    job := builder.Job()
    var fileGroup [][]string
    parser := NewBLParser()
    if item.GroupPattern() != "" || item.MatchPattern() != "" {
        BuilderLogger.Debugf("Shard files with pattern %s/%s/%s: %v\n",
            item.GroupPattern(), item.MatchPattern(), item.SortPattern(),
            files)
        fileGroup = parser.ShardFileListByPattern(files, item.GroupPattern(),
            item.MatchPattern(), item.SortPattern())
    } else {
        groupSize := item.ShardGroupSize()
        if groupSize == 0 {
            /*error*/
            BuilderLogger.Errorf("The item %s for job %s have no pattern and 0 group size\n",
                item.Name(), job.GetID())
            errMsg := fmt.Sprintf("Shard pattern and group size invalid for item %s", item.Name())
            buildStageErrorInfo := NewBuildStageErrorInfo(0, BIOSTAGE_FAILREASON_SHARDFILEMISTAKE,
                item.Name(), errors.New(errMsg))
            buildStageErrorInfo.ItemName = item.Name()
            return errors.New(errMsg), nil, buildStageErrorInfo
        }
        BuilderLogger.Debugf("No pattern, shard file list by group size %d, list: %v\n",
            groupSize, files)
        fileGroup = parser.ShardFileListByGroupSize(files, groupSize)
        if fileGroup == nil {
            BuilderLogger.Errorf("Fail to shard file list by group size\n")
            errMsg := fmt.Sprintf("Shard by Group size failure")
            buildStageErrorInfo := NewBuildStageErrorInfo(0, BIOSTAGE_FAILREASON_SHARDFILEMISTAKE,
                item.Name(), errors.New(errMsg))
            buildStageErrorInfo.ItemName = item.Name()
            return errors.New(errMsg), nil, buildStageErrorInfo
        }
    }

    /*
     * Construct group-name files map and return
     */
    shardGroups := BuildFilesGroupFromArray(fileGroup)
    return nil, shardGroups, nil
}

func BuildFilesGroupFromArray(files [][]string) []ShardFileGroup {
    var filesGroup []ShardFileGroup
    for index, filesArray := range files {
        fileGroup := ShardFileGroup{
            key: fmt.Sprintf("group%d", index),
            files: filesArray,
        }
        filesGroup = append(filesGroup, fileGroup)
    }

    return filesGroup
}

func (builder *seqGraphBuilder)BuildInputDir(item PipelineItem) (string, error){
    job := builder.Job()
    /*
     * For a shard files in middle stage, it must have an input dir, so
     * need reset it here
     */
    inputDir, inputDirTag := item.InputDirAndTag()
    if inputDir != "" {
        inputDir = job.WorkDir() + "/" + inputDir
        BuilderLogger.Debugf("The input dir/tag: %s/%s exist, so reset input files\n",
            inputDir, inputDirTag)
    } else if inputDirTag != "" {
        nodes := builder.GetAllPrevOutputDirNodes(inputDirTag)
        if len(nodes) == 0 {
            BuilderLogger.Errorf("Can't find output dir by name %s\n",
                inputDirTag)
            errMsg := fmt.Sprintf("Non-exist output dir %s", inputDirTag)
            return "", errors.New(errMsg)
        }
        inputDir = nodes[0].Stage().OutputDir()
        BuilderLogger.Debugf("The input dir/tag: %s/%s exist, so reset input files\n",
            inputDir, inputDirTag)
    } else {
        inputDir = job.InputDataSet().InputDir()
    }
    return inputDir, nil
}

func (builder *seqGraphBuilder)_BuildShardFileGroup(item PipelineItem) (error,
    []ShardFileGroup, string, *BuildStageErrorInfo){
    var fileGroup []ShardFileGroup
    var files []string = nil
    var err error

    inputDir, err := builder.BuildInputDir(item)
    if err != nil {
        buildStageErrorInfo := NewBuildStageErrorInfo(0, BIOSTAGE_FAILREASON_OUTPUTMISTAKE,
            item.Name(), err)
        buildStageErrorInfo.ItemName = item.Name()
        return err, nil, "", buildStageErrorInfo
    }

    buildStageErrInfo, inputdir := builder.EvaluateExpression(inputDir)
    if buildStageErrInfo != nil {
        BuilderLogger.Errorf("Can't evaluate the expression %s: %s\n",
            item.OutputFile(), buildStageErrInfo.Err.Error())
        buildStageErrInfo.ItemName = item.Name()
        buildStageErrInfo.ItemCmdStr = item.Cmd()
        return errors.New("Fail to parse output dir " + inputDir), nil, "", buildStageErrInfo
    }
    builder.BranchContext().SetInputDir(inputdir)
    builder.BranchContext().SetDirFiles(nil)

    inputsFile, inputsFileTag := item.ShardInputsFileAndTag()
    if inputsFileTag != "" {
        inputDir = builder.BranchContext().InputDir()
        nodes := builder.GetNamedOutputFileNode(inputsFileTag)
        if nodes == nil {
            BuilderLogger.Errorf("Fail to build file list from non-exist tag file %s\n",
                inputsFileTag)
            errMsg := fmt.Sprintf("unknown tag file %s", inputsFileTag)
            buildStageErrorInfo := NewBuildStageErrorInfo(0, BIOSTAGE_FAILREASON_INPUTDIRTAGMISTAKE,
                item.Name(), errors.New(errMsg))
            buildStageErrorInfo.ItemName = item.Name()
            return errors.New(errMsg), nil, "", buildStageErrorInfo
        }
        /*The json file must output with one node*/
        outFile := nodes[0].Stage().GetTargetOutput(inputsFileTag)

        /*
         * Two kinds of input files format is supported:
         * 1) input files and group them in a JSON format
         * 2) just input a text file list and group them by pattern
         */
        BuilderLogger.Debugf("Try to read shard files and groups from JSON file %s\n",
            outFile)
        err, fileGroup = builder.ReadShardFilesJSONFile(outFile)
        if err != nil {
            BuilderLogger.Infof("Can't read file %s as JSON format (%s), try text file\n",
                outFile, err.Error())
            err, files = builder.ReadLines(outFile)
            if err != nil {
                BuilderLogger.Errorf("Fail to read file list from file %s: %s\n",
                    outFile, err.Error())
                buildStageErrorInfo := NewBuildStageErrorInfo(0, BIOSTAGE_FAILREASON_SHARDFILEMISTAKE,
                    item.Name(), err)
                buildStageErrorInfo.ItemName = item.Name()
                return err, nil, "", buildStageErrorInfo
            }
        }
    } else if inputsFile != "" {
        fileType, builtFilePath := builder.ParseBuildPath(inputsFile)
        file := ""
        if fileType == GB_PATH_TYPE_ABS {
            file = builtFilePath
            BuilderLogger.Debugf("Use absolute input file %s for item %s\n",
                file, item.Name())
        } else {
            inputDir = builder.BranchContext().InputDir()
            buildStageErrInfo, inputFile := builder.EvaluateExpression(inputsFile)
            if buildStageErrInfo != nil {
                BuilderLogger.Errorf("Can't evaluate the expression %s: %s\n",
                    item.OutputFile(), buildStageErrInfo.Err.Error())
                buildStageErrInfo.ItemName = item.Name()
                buildStageErrInfo.ItemCmdStr = item.Cmd()
                return errors.New("Fail to parse output dir " + inputsFile), nil, "", buildStageErrInfo
            }
            file = inputDir + "/" + inputFile
        }

        /*
         * Two kinds of input files format is supported:
         * 1) input files and group them in a JSON format
         * 2) just input a text file list and group them by pattern
         */
        BuilderLogger.Debugf("Try to read shard files and groups from JSON file %s\n",
            file)
        err, fileGroup = builder.ReadShardFilesJSONFile(file)
        if err != nil {
            BuilderLogger.Infof("Can't read file %s as JSON format (%s), try text file\n",
                file, err.Error())
            err, files = builder.ReadLines(file)
            if err != nil {
                BuilderLogger.Errorf("Fail to read file list from file %s: %s\n",
                    file, err.Error())
                buildStageErrorInfo := NewBuildStageErrorInfo(0, BIOSTAGE_FAILREASON_SHARDFILEMISTAKE,
                    item.Name(), err)
                buildStageErrorInfo.ItemName = item.Name()
                return err, nil, "", buildStageErrorInfo
            }
        }
    } else {
        inputDir = builder.BranchContext().InputDir()

        BuilderLogger.Debugf("Shard files for dir %s, group pattern %s match pattern %s\n",
            inputDir, item.GroupPattern(), item.MatchPattern())

        err, files = builder.GetDirFiles()
        if err != nil {
            BuilderLogger.Errorf("Graph builder can't get file list: %s\n",
                err.Error())
            buildStageErrorInfo := NewBuildStageErrorInfo(0, BIOSTAGE_FAILREASON_SHARDFILEMISTAKE,
                item.Name(), err)
            buildStageErrorInfo.ItemName = item.Name()
            return err, nil, "", buildStageErrorInfo
        }
    }

    if fileGroup == nil {
        err, filegroup, buildStageErrorInfo := builder.BuildFileGroupFromFile(item, files)
        if err != nil {
            return err, nil, "", buildStageErrorInfo
        } else {
            return nil, filegroup, inputDir, nil
        }
    } else {
        return nil, fileGroup, inputDir, nil
    }
}

func (builder *seqGraphBuilder) _NeedSuspendGraphBuild(item PipelineItem) (error, bool) {
    /**********************************************************************************************
     * Now we only support suspend in the first level when build graph, so we add the judging function,
     * if it is clone pipeline we want to make sure that each child item are not shardfile (because
     * the clone is parallel), other pieline we only need to ensure that the first item type is not
     * shardfile (serial).
     ***********************************************************************************************/
    Itemtype := item.ItemType()
    if Itemtype == GEN_PT_PIPELINE ||
    Itemtype == GEN_PT_VARIABLEPARALLEL ||
    (Itemtype == GEN_PT_SIMPLE && len(item.Items()) > 0) {
        _, allowed := builder._NeedSuspendGraphBuild(item.Item(0))
        if !allowed {
            return GB_ERR_SUSPEND_BUILD, false
        }
    } else if Itemtype == GEN_PT_SHARDFILES {
        return GB_ERR_SUSPEND_BUILD, false
    } else if Itemtype == GEN_PT_CLONEPARALLEL {
        for i := 0; i < len(item.Items()); i ++ {
            subItem := item.Item(i)
            subItemType := subItem.ItemType()
            if subItemType == GEN_PT_SHARDFILES ||
            subItemType == GEN_PT_VARIABLEPARALLEL {
                return GB_ERR_SUSPEND_BUILD, false
            } else if subItemType == GEN_PT_CLONEPARALLEL ||
            subItemType == GEN_PT_PIPELINE ||
            subItemType == GEN_PT_VARIABLEPARALLEL ||
            (subItemType == GEN_PT_SIMPLE && len(subItem.Items()) > 0) {
                /*The pipeline type with Linear perform, so only check it's first subitem*/
                _, allowed := builder._NeedSuspendGraphBuild(subItem)
                if !allowed {
                    return GB_ERR_SUSPEND_BUILD, false
                }
            }
        }
    }

    return nil, true
}

func (builder *seqGraphBuilder)NeedSuspendGraphBuild(item PipelineItem) (error, bool) {
    if builder.CurrentBuildPos() != builder.CurrentItemPos() {
        return builder._NeedSuspendGraphBuild(item)
    }
    return nil, true
}

func (builder *seqGraphBuilder)CheckShardOperation(item PipelineItem) (error, bool) {
    if !builder.GetResumeBuild() {
        /* suspend when first build shard*/
        builder.SetResumeBuild(true)
        BuilderLogger.Debugf("A shard files stage exist in middle of pipeline, suspend it\n")
        return GB_ERR_SUSPEND_BUILD, false
    } else {
        /*
         * when a builder hold more than one shardfile Graph,
         * we need reset the shard file label.
         */
        builder.SetResumeBuild(false)
    }

    return nil, true
}

func (builder *seqGraphBuilder)BuildeBuildVariableBranchVarList(item PipelineItem,
    inputDir string) (error, map[string][]string) {

    var err error
    /*
     * Check and prepare the branch variables here, it can be specified in
     * three kinds of approaches:
     * 1) var list:
     * 2) an input file:
     * 3) a tag specifying an input file
     * 4) a file storing the whole name/value maps
     * 5) a tag file storing the whole name/value maps
     */
    branchVarListMap := item.GetAllBranchVarListMap()
    branchVarFileMap := item.GetAllBranchVarFileMap()
    branchVarTagMap := item.GetAllBranchVarTagMap()
    branchVarMapFile, branchVarMapFileTag := item.BranchVarMapFileAndTag()

    if branchVarListMap != nil && len(branchVarListMap) != 0 {
        BuilderLogger.Debugf("The item %s use branch variable list\n",
            item.Name())
    } else if branchVarFileMap != nil && len(branchVarFileMap) != 0 {
        /*check whether the inputs are ready or not*/
        if builder.CurrentBuildPos() != builder.CurrentItemPos() {
            BuilderLogger.Debugf("Suspend build variable parallel for branch var file ready\n")
            return GB_ERR_SUSPEND_BUILD, nil
        }

        /*read branch var file to construct a branch var list map*/
        err, branchVarListMap = builder.BuildBranchVarListMapFromFile(item,
            branchVarFileMap)
        if err != nil {
            BuilderLogger.Errorf("Fail to build branch vars from var file map for item %s: %s\n",
                item.Name(), err.Error())
            return err, nil
        }
    } else if branchVarTagMap != nil && len(branchVarTagMap) != 0 {
        if builder.CurrentBuildPos() != builder.CurrentItemPos() {
            BuilderLogger.Debugf("Suspend build variable parallel for branch var file tag ready\n")
            return GB_ERR_SUSPEND_BUILD, nil
        }
        err, branchVarListMap = builder.BuildBranchVarListMapFromTag(item,
            branchVarTagMap)
        if err != nil {
            BuilderLogger.Errorf("Fail to build branch vars from var tag map for item %s: %s\n",
                item.Name(), err.Error())
            return err, nil
        }
    } else if branchVarMapFile != "" {
        if builder.CurrentBuildPos() != builder.CurrentItemPos() {
            BuilderLogger.Debugf("Suspend build variable parallel for branch var JSON file ready\n")
            return GB_ERR_SUSPEND_BUILD, nil
        }
        /*The JSON file should be relative to input directory*/
        jsonFile := inputDir + "/" + branchVarMapFile
        err, branchVarListMap = builder.BuildBranchVarListMapFromJSONFile(item,
            jsonFile)
        if err != nil {
            BuilderLogger.Errorf("Fail to build branch vars from json file %s for item %s: %s\n",
                branchVarMapFile, item.Name(), err.Error())
            return err, nil
        }
    } else if branchVarMapFileTag != "" {
        if builder.CurrentBuildPos() != builder.CurrentItemPos() {
            BuilderLogger.Debugf("Suspend build variable parallel for branch var JSON file tag ready\n")
            return GB_ERR_SUSPEND_BUILD, nil
        }
        err, branchVarListMap = builder.BuildBranchVarListMapFromJSONFileTag(item,
            branchVarMapFileTag)
        if err != nil {
            BuilderLogger.Errorf("Fail to build branch vars from json file tag %s for item %s: %s\n",
                branchVarMapFileTag, item.Name(), err.Error())
            buildStageErrorInfo := NewBuildStageErrorInfo(0, BIOSTAGE_FAILREASON_VARIABLEPARALLELMISTAKE,
                item.Name(), err)
            buildStageErrorInfo.ItemName = item.Name()
            return err, nil
        }
    } else {
        BuilderLogger.Errorf("The Variable Parallel should have branch variables")
        errMsg := fmt.Sprintf("Empty branch variable for variable parallel %s", item.Name())
        return errors.New(errMsg), nil
    }
    return nil, branchVarListMap
}

/*
 * This is a core routine of graph builder. It will build all input
 * for the stage. the input and output is specified by user in the
 * pipeline item definition. they are placeholders and need to be
 * parsed first. Then it works as follows:
 * 1) if the input is specified by $files.*, it means the placeholder
 *    should be replaced with the file list "file1 file2 ..." of the
 *    input files. The input files are already constructed by the parent
 *    graph builders and feed to it as input files. so get them from
 *    branch context.
 * 2) if the input is specified by $inputs.tag, it means it need collect
 *    all output files named with "tag" of previous branches. This is a
 *    merge stage. So get all the nodes named "tag" in previous branches.
 *    here we only store the node pointers rather than the file names. It
 *    is because that the node may fail and retried when it is executed.
 *    so its output file path can't be determined in graph build time.
 * 3) if the input is specified by $input.tag, it means that the input is
 *    the output of a latest node which names its result as "tag" name.
 *    then get the node pointer from output map of branch context.
 */

type InputDataCollection struct {
    inputFiles []string
    fileNodes *NamedNodeMap
    dirNodes *NamedNodeMap
}

func (builder *seqGraphBuilder) CollectBuildExpandInputs(collectMap *InputCollectMap,
    tag string, inputData *InputDataCollection, item PipelineItem) error {
    /*
     * case 1: $inputs.tag like inputs, so get nodes from context 
     * of all previous branches.
     */
    tagValue := tag
    if tagValue == "" {
        tagValue = GB_TAG_LAST_OUTPUT
    }

    if tagValue == GB_TAG_LAST_OUTPUT && 
        builder.IsShardGraphBuilder() && 
        builder.CurrentItemPos() == 0 {

        /*make sure that files only be collected once*/
        if collectMap.IsFilesCollected() {
            return nil
        }

        /*
         * for $inputs.* like placeholders, if we are the first item of a
         * shard graph, we need feed them with input files when it is the first
         * stage in ShardFiles. This is to support flexible pipeline item
         * definition, which needn't be rewrote to handle file or input produced
         * by previous stage.
         */
        err, files := builder.GetDirFullPathFiles()
        if err != nil {
            BuilderLogger.Errorf("Can't get input files for shard item %s: %s\n",
                item.Name(), err.Error())
            return errors.New("No input files for shard item " + item.Name())
        } else {
            inputData.inputFiles = files
            collectMap.MarkFilesCollected()
        }


    } else {
        /*make sure that each file node only be collected once*/
        if collectMap.IsFileInputCollected(tagValue) {
            return nil
        }

        prevTagedNodes := builder.GetAllPrevOutputFileNodes(tagValue)
        if len(prevTagedNodes) > 0 {
            inputData.fileNodes.AddNamedNodes(tagValue, prevTagedNodes)
        } else {
            BuilderLogger.Errorf("Can't get tagged %s output files for item %s\n",
                tagValue, item.Name())
            return errors.New("No tagged " + tagValue + " output files")
        }

        collectMap.MarkFileInputCollected(tagValue)
    }

    return nil
}

func (builder *seqGraphBuilder) CollectInputFiles(collectMap *InputCollectMap,
    inputData *InputDataCollection, item PipelineItem) error {
    if collectMap.IsFilesCollected() {
        return nil
    }

    err, files := builder.GetDirFullPathFiles()
    if err != nil {
        BuilderLogger.Errorf("Can't get dir files: %s\n",
            err.Error())
        return err
    }
    inputData.inputFiles = files

    collectMap.MarkFilesCollected()

    return nil
}

func (builder *seqGraphBuilder) CollectBuildTagInputs(collectMap *InputCollectMap,
    tag string, inputData *InputDataCollection, item PipelineItem) error {
    var err error = nil
    /*
     * case 3: $input.tag like placeholder, just get the node
     * from the output map of current branch.
     * if $input is specified, it means use the output produced
     * last stage
     */
    tagValue := tag
    if tagValue == "" {
        /*
         * $input like reference need point to the output of last
         * stage
         */
        tagValue = GB_TAG_LAST_OUTPUT
    }
    
    if collectMap.IsFileInputCollected(tagValue) {
        return nil
    }

    nodes := builder.GetNamedOutputFileNode(tagValue)
    if len(nodes) != 0 && nodes != nil {
        inputData.fileNodes.AddNamedNodes(tagValue, nodes)
        collectMap.MarkFileInputCollected(tagValue)
    } else {
        err = errors.New("No named input " + tagValue)
    }

    /*
     * for $input like placeholders, if we don't have prev tagged
     * outputs, we need feed them with input files when it is the first
     * stage in ShardFiles. This is to support flexible pipeline item
     * definition, which needn't be rewrote to handle file or input produced
     * by previous stage.
     */
    if len(nodes) == 0 && tagValue == GB_TAG_LAST_OUTPUT &&
            builder.IsShardGraphBuilder() && builder.CurrentItemPos() == 0 {
        if !collectMap.IsFilesCollected() {
            var files []string
            err, files = builder.GetDirFullPathFiles()
            if err == nil {
                inputData.inputFiles = files
                collectMap.MarkFilesCollected()
            }
        }
    }
    
    if err != nil {
        errMsg := fmt.Sprintf("Can't find named %s output file for item %s",
            tagValue, item.Name())
        BuilderLogger.Errorf("Prepare input failure: %s\n", errMsg)
        return errors.New(errMsg)
    }

    return nil
}

func (builder *seqGraphBuilder) CollectBuildTagIndexInputs(collectMap *InputCollectMap,
    tag string, inputData *InputDataCollection, item PipelineItem) error {
    /*
     * case 4: $input1.tag, $input2.tag like placeholder, need collect
     * all the tagged nodes. if the "tag" is not specified, just use
     * the last stage's by default.
     * Two cases here:
     * 1) This stage is following a shard stage, so we need get all outputs
     *    from previous branches
     * 2) This is not the first stage after ShardFiles stage, so we should
     *    use the last stage of this branch
     */
    tagValue := tag
    if tagValue == "" {
        tagValue = GB_TAG_LAST_OUTPUT
    }

    if collectMap.IsFileInputCollected(tagValue) {
        return nil
    }
    
    /*
     * for $inputX like placeholders, if we don't have prev tagged
     * outputs, we need feed them with input files when it is the first
     * stage in ShardFiles. This is to support flexible pipeline item
     * definition, which needn't be rewrote to handle file or input produced
     * by previous stage.
     */
     if tagValue == GB_TAG_LAST_OUTPUT && 
         builder.IsShardGraphBuilder() && builder.CurrentItemPos() == 0 {
        if !collectMap.IsFilesCollected() {
            err, files := builder.GetDirFullPathFiles()
            if err != nil {
                BuilderLogger.Errorf("Can't get last output or dir files: %s\n",
                    err.Error())
                return err
            } else {
                inputData.inputFiles = files
                collectMap.MarkFilesCollected()
                return nil
            }
        }
    } else {
        prevTagedNodes := builder.GetAllPrevOutputFileNodes(tagValue)
        if len(prevTagedNodes) > 0  {
            inputData.fileNodes.AddNamedNodes(tagValue, prevTagedNodes)
            collectMap.MarkFileInputCollected(tagValue)
        }
    }

    return nil
}

func (builder *seqGraphBuilder) CollectBuildTagDirInputs(collectMap *InputCollectMap,
    tag string, inputData *InputDataCollection, item PipelineItem) error {
    /*$inputdir will be evaluated to job's input dir*/
    tagValue := tag
    if tagValue == "" {
        return nil
    }

    if collectMap.IsDirInputCollected(tagValue) {
        return nil
    }

    /*
     * case 5: $inputdir.tag like placeholder, just get the node
     * from the output map of current branch.
     */
    nodes := builder.GetAllPrevOutputDirNodes(tagValue)
    if len(nodes) == 0 {
        errMsg := fmt.Sprintf("Can't find named %s output dir for %s",
            tagValue, item.Name())
        BuilderLogger.Errorf("Prepare input dir failure: %s\n",
            errMsg)
        return errors.New(errMsg)
    }
    inputData.dirNodes.AddNamedNodes(tagValue, nodes)
    collectMap.MarkDirInputCollected(tagValue)

    return nil
}

func (builder *seqGraphBuilder) BuildStageInput(item PipelineItem, cmd string, 
    prevNodes []Node) (*BuildStageErrorInfo, *NamedNodeMap, *NamedNodeMap, []string) {
    collectMap := NewInputCollectMap()
    inputData := &InputDataCollection {
        fileNodes: NewNamedNodeMap(),
        dirNodes: NewNamedNodeMap(),
        inputFiles: nil,
    }
    parser := NewBLParser()
    var err error
    if cmd != "" {
        buildStageErrInfo := parser.TranslatePlaceHolders(cmd,
                func (stmt BLStatement) (error, bool) {
                    stmtType := stmt.StmtType()
                    stmtValue := stmt.Value()
                    if stmtType.IsExpandInput() {
                        err = builder.CollectBuildExpandInputs(collectMap,
                            stmtValue, inputData, item)
                        if err != nil {
                            return err, false
                        }
                    } else if stmtType.IsExpandFile() || stmtType.IsIndexFile() {
                        /*
                         * case 2: $files.* like placeholder. it is always the first
                         * stage of the ShardFiles pipeline. Just list the full path
                         * of the graph's input files here
                         */
                        err = builder.CollectInputFiles(collectMap, inputData,
                            item)
                        if err != nil {
                            return err, false
                        }
                    } else if stmtType.IsTagInput() {
                        err = builder.CollectBuildTagInputs(collectMap, stmtValue,
                            inputData, item)
                        if err != nil {
                            return err, false
                        }
                    } else if stmtType.IsTagIndexInput() {
                        err = builder.CollectBuildTagIndexInputs(collectMap, stmtValue,
                            inputData, item)
                        if err != nil {
                            return err, false
                        }
                    } else if stmtType.IsTagInputDir() {
                        err = builder.CollectBuildTagDirInputs(collectMap, stmtValue,
                            inputData, item)
                        if err != nil {
                            return err, false
                        }
                    }

                    return nil, true
                })
        if buildStageErrInfo.Err != nil {
            BuilderLogger.Errorf("Build node input failure %s\n",
                buildStageErrInfo.Err.Error())
            buildStageErrInfo.ItemName = item.Name()
            buildStageErrInfo.ItemCmdStr = cmd
            buildStageErrInfo.IsCMDErr = true
            buildStageErrInfo.ErrInfo = fmt.Sprintf("Build command: %s", buildStageErrInfo.Err.Error())
            return buildStageErrInfo, nil, nil, nil
        }
    }
    return nil, inputData.fileNodes, inputData.dirNodes, inputData.inputFiles
}

/*Use the rule defined by pipeline item to produce the output file name*/
func (builder *seqGraphBuilder) BuildOutputName(item PipelineItem) string {
    branchCtxt := builder.BranchContext()
    outputName := branchCtxt.Name() + "."
    if item.Filter() != "" {
        outputName += item.Filter()
    } else {
        nameItems := strings.Split(strings.ToLower(item.Name()), " ")
        for i := 0; i < len(nameItems); i ++ {
            if nameItems[i] != "" {
                outputName += nameItems[i]
            }
        }
    }

    return outputName
}

const (
    GB_PATH_TYPE_ABS int = 1
    GB_PATH_TYPE_REL int = 2
)

/*
 * Evaluate a expression which may be used by user:
 * 1) specify the "outputdir" in pipeline item
 * 2) specify the "outputfile" in pipeline item
 */
func (builder *seqGraphBuilder) EvaluateExpression(expr string) (*BuildStageErrorInfo, string) {
    parser := NewBLParser()
    buildStr := ""
    job := builder.Job()
    inputMap := job.InputDataSet().InputMap()
    branchCtxt := builder.BranchContext()
    if expr != "" {
        buildStageErrInfo := parser.TranslatePlaceHolders(expr,
                func (stmt BLStatement) (error, bool) {
                    stmtType := stmt.StmtType()
                    stmtValue := stmt.Value()
                    stmtPrefix := stmt.Prefix()
                    if stmtType.IsExpandInput() {
                        return errors.New("Don't support $inputs"), false
                    } else if stmtType.IsExpandFile() || stmtType.IsIndexFile() {
                        return errors.New("Don't support $file"), false
                    } else if stmtType.IsTagInput() {
                        return errors.New("Don't support $input"), false
                    } else if stmtType.IsTagIndexInput() {
                        return errors.New("Don't support $input"), false
                    } else if stmtType.IsTagInputDir() {
                        return errors.New("Don't support $inputdir"), false
                    } else if stmtType.IsTagVariable() {
                        if val, ok := inputMap[stmtValue]; ok {
                            buildStr += stmtPrefix + val
                        } else {
                            return errors.New("Reference Unknown Variable " + stmtValue),
                                false
                        }
                    } else if stmtType.IsStr() {
                        buildStr += stmtValue
                    } else if stmtType.IsTagBranch() {
                        branchVarValue := branchCtxt.GetBranchVariable(stmtValue)
                        if branchVarValue == "" {
                            return errors.New("Reference Unknown Branch variable " + stmtValue),
                                false
                        }
                        buildStr += stmtPrefix + branchVarValue
                    }

                    return nil, true
                })
        if buildStageErrInfo.Err != nil {
            BuilderLogger.Errorf("Evaulate expression failure %s\n",
                buildStageErrInfo.Err.Error())
            buildStageErrInfo.IsCMDErr = true
            buildStageErrInfo.ErrInfo = fmt.Sprintf("Build command: %s", buildStageErrInfo.Err.Error())
            return buildStageErrInfo, ""
        }
    }

    return nil, buildStr
}

/*It tries to parse and build a file or directory path*/
func (builder *seqGraphBuilder) ParseBuildPath(pathValue string) (int, string) {
    path := strings.TrimSpace(pathValue)
    if path == "" {
        return -1, ""
    }

    /*
     * 1) First check and evaulate the expression to resolve the variables
     * in the input map
     */
    buildStageErrInfo, parsedPath := builder.EvaluateExpression(path)
    if buildStageErrInfo != nil {
        BuilderLogger.Errorf("Can't evaluate the expression %s: %s\n",
            path, buildStageErrInfo.Err.Error())
        return -1, ""
    }

    /*
     * 2) Check whether it is a URI. it is not recommended, but
     * allowed
     */
    err, _, vol, file := FSUtilsParseFileURI(parsedPath) 
    if err == nil && file != "" && vol != "" {
        /*It is a valid file URI, use it*/
        return GB_PATH_TYPE_ABS, parsedPath
    }

    /*
     * 3) Regard it as a relative path to current work directory
     */
     return GB_PATH_TYPE_REL, parsedPath
}

/*
 * This is the routine does actual work to build the flow graph.
 * 1) Create the cmdStage for the pipeline item. Use the command,
 *    image and other information of pipeline item.
 * 2) Build a node for the flow graph. Associate the stage created
 *    by this routine to the node. Add edges between previous nodes
 *    and this node.
 * 3) Modify output map of current branch. If the pipeline item will
 *    output a file specified by placeholder $output.tag, then the node
 *    will be set to output map named by "tag". The previous node named
 *    "tag" will never be seen after this point. This implements the 
 *    "forward" like semantic of Bpipe.
 * 4) The node created this step now becomes the prevNodes of next step.
 */
func (builder *seqGraphBuilder) BuildSimpleItemGraph(item PipelineItem,
    prevNodes []Node) (error, []Node, *BuildStageErrorInfo) {
    var err error
    job := builder.Job()
    cmdStr := item.Cmd()
    stageId := fmt.Sprintf("stage-%d", job.GenerateStageIndex())
    buildStageErrInfo, inputFileNodes, inputDirNodes, inputFiles := builder.BuildStageInput(item,
        cmdStr, prevNodes)
    if buildStageErrInfo != nil {
        return errors.New("Build graph: " + buildStageErrInfo.Err.Error()), nil, buildStageErrInfo
    }
    inputMap := job.InputDataSet().InputMap()
    branchCtxt := builder.BranchContext()
    outputName := builder.BuildOutputName(item)

    /*
     * Some item may reset its work directory to a file variable or a URI,
     * so get correct work direcotry first
     */
    stageWorkDir := branchCtxt.WorkDir()
    /*
     * If the item require work on HDFS, switch the work directory
     */
    storageType := StringToStorageType(item.StorageType())
    if storageType == STORAGE_TYPE_INVALID {
        /*user don't specify storage type and try to deduce from input*/
        matcher := NewStorageMatcher(inputFiles, inputFileNodes, inputDirNodes)
        if matcher != nil {
            err, storageType = matcher.DeduceStorageTypeFromInput()
            if err != nil {
                BuilderLogger.Errorf("Fail to deduce storage type for item %s: %s\n",
                    item.Name(), err.Error())
                storageType = STORAGE_TYPE_DEFAULT
            }
        }
    }

    if storageType == STORAGE_TYPE_HDFS {
        stageWorkDir = branchCtxt.HDFSWorkDir()
        if stageWorkDir == "" {
            SchedulerLogger.Errorf("job %s has no HDFS work directory set \n",
                job.GetID())
            buildStageErrorInfo := NewBuildStageErrorInfo(0, BIOSTAGE_FAILREASON_INPUTMISTAKE,
                item.Name(), errors.New("No HDFS Work Directory set"))
            buildStageErrorInfo.ItemName = item.Name()
            return errors.New("No HDFS Work Directory set"), nil, buildStageErrorInfo
        }
    }

    dataDir := job.DataDir()
    /*Create a stage for the pipeline item and configure its properties*/
    stage := NewBlCmdStage(job, stageId, cmdStr, stageWorkDir,
        branchCtxt.LogDir(), inputFileNodes, inputMap, outputName, dataDir)
    if inputFiles != nil {
        stage.SetInputFiles(inputFiles)
    }
    if inputDirNodes != nil {
        stage.SetInputDirNodeMap(inputDirNodes)
    }
    if item.OutputFileMap() != nil && len(item.OutputFileMap()) != 0 {
        tmpOuputFileMap := make(map[string]string)
        for tag, outputFile := range item.OutputFileMap() {
            buildStageErrInfo, parsedOutputFile := builder.EvaluateExpression(outputFile)
            if buildStageErrInfo != nil {
                BuilderLogger.Errorf("Can't evaluate the expression %s: %s\n",
                    item.OutputFile(), buildStageErrInfo.Err.Error())
                buildStageErrInfo.ItemName = item.Name()
                buildStageErrInfo.ItemCmdStr = item.Cmd()
                return errors.New("Fail to parse output file " + item.OutputFile()), nil, buildStageErrInfo
            }
            tmpOuputFileMap[tag] = parsedOutputFile
        }
        stage.SetOutputFileMap(tmpOuputFileMap)
    } else if item.OutputFile() != "" {
        buildStageErrInfo, parsedOutputFile := builder.EvaluateExpression(item.OutputFile())
        if buildStageErrInfo != nil {
            BuilderLogger.Errorf("Can't evaluate the expression %s: %s\n",
                item.OutputFile(), buildStageErrInfo.Err.Error())
            buildStageErrInfo.ItemName = item.Name()
            buildStageErrInfo.ItemCmdStr = item.Cmd()
            return errors.New("Fail to parse output file " + item.OutputFile()), nil, buildStageErrInfo
        }
        stage.SetOutputFile(parsedOutputFile)
    }

    if item.ExtensionMap() != nil && len(item.ExtensionMap()) != 0 {
        BuilderLogger.Debugf("Set extension map :%v\n",
            item.ExtensionMap())
        if stage.CheckExtensionMap(item.ExtensionMap()) {
            stage.SetExtensionMap(item.ExtensionMap())
        } else {
            buildStageErrorInfo := NewBuildStageErrorInfo(0, BIOSTAGE_FAILREASON_EXTENSIONNAMEMISTAKE,
                item.Name(), errors.New("Set extension map but not use"))
            buildStageErrorInfo.ItemName = item.Name()
            return errors.New("Set extension map but not use"), nil, buildStageErrorInfo
        }
    } else if item.ExtensionName() != "" {
        BuilderLogger.Debugf("Set extension name :%s\n",
            item.ExtensionName())
        if stage.CheckExtensionName(item.ExtensionName()) {
            stage.SetExtensionName(item.ExtensionName())
        } else {
            buildStageErrorInfo := NewBuildStageErrorInfo(0, BIOSTAGE_FAILREASON_EXTENSIONNAMEMISTAKE, item.Name(),
                errors.New("When there are multiple output tags in an item, you cannot use extensionname, use extensionmap."))
            buildStageErrorInfo.ItemName = item.Name()
            return errors.New("When there are multiple output tags in an item, you cannot use extensionname, use extensionmap."),
                nil, buildStageErrorInfo
        }
    }

    stage.SetBranchVarMap(branchCtxt.GetBranchVarMap())
    /*
     * 1. Set item resource spec to stage
     * 2. Maybe some resource spec use Expression,
     *    so evaluate it and set it to stage.
     */
    stage.SetResourceSpec(item.ResourceSpec())
    err, buildStageErrInfo = builder.EvaluateResourceSpec(item, stage)
    if err != nil {
        BuilderLogger.Errorf("Evaluate ResourceSpec falied")
        return err, nil, buildStageErrInfo
    }
    stage.SetName(item.Name())
    if item.Image() != "" {
        stage.SetImage(item.Image())
    }
    stage.SetCleanupPattern(item.CleanupPattern())

    /*evaluate expression in the environment variables*/
    if envVars := item.Env(); envVars != nil {
        parsedEnvs := make(map[string]string)
        for key, value := range envVars {
            buildStageErrInfo, parsedEnvValue := builder.EvaluateExpression(value)
            if buildStageErrInfo != nil {
                BuilderLogger.Errorf("Can't evaluate the environment variables expression %s: %s\n",
                    value, buildStageErrInfo.Err.Error())
                buildStageErrInfo.ItemName = item.Name()
                buildStageErrInfo.ItemCmdStr = item.Cmd()
                return errors.New("Fail to parse environment variable " + value), nil, buildStageErrInfo
            }
            parsedEnvs[key] = parsedEnvValue
        }
        stage.SetEnv(parsedEnvs)
    }
    stage.SetResourceTuneRatio(item.ResourceTuneRatio())
    stage.SetFailRetryLimit(item.FailRetryLimit())
    stage.SetFailAbort(item.FailAbort())
    stage.SetStorageType(storageType)
    stage.SetPrivileged(item.Privileged())
    stage.SetVolumes(item.Volumes())
    pipeline := builder.GetPipeline()
    stage.SetPipelineName(pipeline.Name())
    stage.SetPipelineVersion(pipeline.Version())

    /*Set the io attributes if needed*/
    ioPattern, rwPattern := item.IORWPattern()
    isoLevel := item.IsolationLevel()
    ephLevel, ephPattern, ephMap := item.EpheremalProperty()
    workingSet := item.WorkingSet()
    largeSmallFiles := item.LargeSmallFiles()
    if ioPattern != "" || rwPattern != "" || isoLevel != "" ||
        ephLevel != "" || ephPattern != "" || ephMap != nil ||
        workingSet != "" || largeSmallFiles {
        ioAttr := &IOAttr{}
        ioAttr.SetIOPattern(ioPattern)
        ioAttr.SetRWPattern(rwPattern)
        ioAttr.SetIsoLevel(isoLevel)
        ioAttr.SetWorkingSet(workingSet)
        ioAttr.SetEphLevel(ephLevel)
        ioAttr.SetEphPattern(ephPattern)
        ioAttr.SetEphMap(ephMap)
        ioAttr.SetLargeSmallFiles(largeSmallFiles)
        stage.SetIOAttr(ioAttr)
    }

    execMode := job.ExecMode()
    if execMode == "" {
        execMode = item.ExecMode()
    }
    stage.SetExecMode(execMode)

    inputDirTarget, workDirTarget := item.DirMapTargets()
    if inputDirTarget != "" {
        err := stage.MapDirToContainer(branchCtxt.InputDir(),
            inputDirTarget)
        if err != nil {
            BuilderLogger.Errorf("Map input dir failure: %s\n",
                err.Error())
            buildStageErrorInfo := NewBuildStageErrorInfo(0, BIOSTAGE_FAILREASON_INPUTMISTAKE,
                item.Name(), err)
            buildStageErrorInfo.ItemName = item.Name()
            return err, nil, buildStageErrorInfo
        }
    }
    if workDirTarget != "" {
        err := stage.MapDirToContainer(stageWorkDir,
            workDirTarget)
        if err != nil {
            BuilderLogger.Errorf("Map work dir failure: %s\n",
                err.Error())
            buildStageErrorInfo := NewBuildStageErrorInfo(0, BIOSTAGE_FAILREASON_INPUTMISTAKE,
                item.Name(), err)
            buildStageErrorInfo.ItemName = item.Name()
            return err, nil, buildStageErrorInfo
        }
    }
    /*
     * Handle directory related operations here
     */
    outputDir, outputDirTag := item.OutputDirAndTag()
    if outputDir != "" {
        outputDirIsRel := true
        /*
         * The output dir may be a named variable, or a URI,
         * or a sub directory relative to current work dir
         */
        fullPathOutputDir := ""
        dirType, builtDir := builder.ParseBuildPath(outputDir)
        if dirType == GB_PATH_TYPE_ABS {
            fullPathOutputDir = builtDir
            outputDirIsRel = false
            BuilderLogger.Debugf("Absolute output dir %s set for item %s job %s\n",
                fullPathOutputDir, item.Name(), job.GetID())
        } else if dirType == GB_PATH_TYPE_REL {
            /*
             * pipeline item require to create a sub directory
             * to store output files
             */
            fullPathOutputDir = stageWorkDir + "/" + builtDir
            BuilderLogger.Debugf("Relative output dir %s set for item %s job %s\n",
                fullPathOutputDir, item.Name(), job.GetID())
        } else {
            BuilderLogger.Infof("Can't build output dir %s for item %s job %s\n",
                outputDir, item.Name(), job.GetID())
            errMsg := fmt.Sprintf("Invalid output dir %s", outputDir)
            buildStageErrorInfo := NewBuildStageErrorInfo(0, BIOSTAGE_FAILREASON_OUTPUTMISTAKE,
                item.Name(), errors.New(errMsg))
            buildStageErrorInfo.ItemName = item.Name()
            return errors.New(errMsg), nil, buildStageErrorInfo
        }

        /*
         * In order to speedup building graph, the output directory will be created
         * in a later time.
         */
        if !outputDirIsRel {
            /*output directory is abs*/
            stage.SetOutputDir(fullPathOutputDir, false)
        } else {
            stage.SetOutputDir(builtDir, true)
        }
    }

    stageNode := NewNode(stageId, stage)
    builder.Graph().AddNode(stageNode)

    rawTagPrefix := item.TagPrefix()
    rawTagPrefixMap := item.TagPrefixMap()
    /*Evaluate the expressions in the tag prefixs*/
    evalErrInfo, tagPrefix := builder.EvaluateExpression(rawTagPrefix)
    if evalErrInfo != nil {
        return errors.New("fail evalute tag prefix"), nil, evalErrInfo
    }
    tagPrefixMap := make(map[string]string)
    for key, value := range rawTagPrefixMap {
        evalErrInfo, tagPrefixVal := builder.EvaluateExpression(value)
        if evalErrInfo != nil {
            return errors.New("fail evalute tag prefix map"), nil, evalErrInfo
        }
        tagPrefixMap[key] = tagPrefixVal
    }
    stage.SetTagPrefix(tagPrefix, tagPrefixMap)

    stage.AddConstraints(item.Constraints())

    /*
     * Map the output file tag with node here, need map two
     * names:
     * 1) output tag (defined in $output.tag)
     * 2) BF-LAST-OUTPUT-NAME: if next stage refer it by $input or 
     *    $input1, it will point to the output mapped with this name
     */
    outputTags, outputDirTags := stage.GetOutputTags()
    if outputTags != nil {
        for i := 0; i < len(outputTags); i ++ {
            outputTag := outputTags[i]
            if outputTag != "" {
                builder.AddStageOutputFileToMap(outputTag,
                    stageNode)
            }
        }
    }
    builder.AddStageOutputFileToMap(GB_TAG_LAST_OUTPUT, stageNode)

    /*The outputdir in cmd must use tag to appoint*/
    if outputDirTags != nil {
        for i := 0; i < len(outputDirTags); i ++ {
            outputTag := outputDirTags[i]
            if outputTag != "" {
                builder.AddStageOutputDirToMap(outputTag,
                    stageNode)
            }
        }
    }
    /*set output dir name <-->node map*/
    if outputDir != "" && outputDirTag != "" {
        builder.AddStageOutputDirToMap(outputDirTag, stageNode)
    }

    /*Add dependency edges in the flow graph*/
    if prevNodes != nil {
        for i := 0; i < len(prevNodes); i ++ {
            builder.Graph().AddEdge(StringID(prevNodes[i].Stage().GetID()),
                StringID(stageNode.Stage().GetID()), 0)
        }
    }

    nodes := make([]Node, 0, 1)
    nodes = append(nodes, stageNode)
    return nil, nodes, nil
}

/*
 * Handle the "ShardFiles" pipeline item. It works in divde and conquer approach.
 * 1) Shard the input files by MatchPattern and GroupPattern. This will produce
 *    several file group.
 * 2) For each file group, create a sub seqGraphBuilder. The sub graph builder will
 *    have same slice of pipeline but the input files is the file group.
 * 3) Calls the sub seqGraphBuilder to build flow graph. So it produces several 
 *    sub graphs. 
 * 4) Merge these sub graphs. The output nodes of current step should be the union of
 *    the output nodes of sub graph builders.
 *
 */
func (builder *seqGraphBuilder) BuildShardFilesGraph(item PipelineItem,
    prevNodes []Node, graphInfoTree *BlGraphInfoTree) (error, []Node, *BuildStageErrorInfo) {
    job := builder.Job()
    childGraphBuilderTree := builder.ChildGraphBuilerTree()

    discardTag := item.Discard()
    forwardTag := item.Forward()

    childGraphBuilderTree.SetChildGraphBuildersDiscard(discardTag)
    childGraphBuilderTree.SetChildGraphBuildersForward(forwardTag)
    childGraphBuilderTree.SetMergeTag(true)
    /*
     * The ShardFiles stage can only be done in restricted style. 
     * It only supports shard in two ways:
     * 1) It shards a dataset's input directory or known file list. This 
     *    must be in begining stage, or the begining stage of the sub 
     *    pipeline of a ShardFiles pipeline item.
     * 2) Shards in middle of pipeline. This is only allowed in the level
     *    0 pipeline (the main pipeline). And the job will be executed in
     *    the following way:
     *    1) build flow graph until hit a ShardFiles stage in the middle of
     *       the main pipeline, pause build and start executing
     *    2) scheduler runs the partial build flow graph
     *    3) flow graph run done, post the job to job manager again. The job
     *       manager resumes build the flow graph from last paused item. It
     *       builds until end or hit another shard stage.
     *    4) scheduler executes the new built part of graph
     *    5) ...
     */
    if err, allowed := builder.CheckShardOperation(item); !allowed {
        BuilderLogger.Infof("Job %s item %s shard files in middle, suspend it\n",
            job.GetID(), item.Name())
        buildStageErrorInfo := NewBuildStageErrorInfo(0, BIOSTAGE_FAILREASON_SHARDFILEMISTAKE,
            item.Name(), err)
        buildStageErrorInfo.ItemName = item.Name()
        return err, nil, buildStageErrorInfo
    }

    err, fileGroup, inputDir, buildStageErrorInfo := builder._BuildShardFileGroup(item)
    if err != nil {
        BuilderLogger.Errorf("Build shard file group failed: %s\n", err.Error())
        return err, nil, buildStageErrorInfo
    }


    graph := builder.Graph()

    if fileGroup == nil || len(fileGroup) == 0 {
        BuilderLogger.Warnf("The Shardfiles stage of job %s on item %s group no files\n",
            job.GetID(), item.Name())
    }

    for _, shardGroup := range fileGroup {
        name := shardGroup.key
        groupFiles := shardGroup.files
        if len(groupFiles) != 0 {
            BuilderLogger.Debugf("Build job %s sub graph for group %s files: %v\n",
                job.GetID(), name, groupFiles)
            subBuilder := NewSeqGraphBuilder(graph, item.Items(), job,
                SG_TYPE_SHARD)
            subBuilder.SetPipeline(builder.GetPipeline())
            subBuilder.SetLevel(builder.Level() + 1)
            subBuilder.SetPrevNodes(prevNodes)
            subBuilder.CloneMapContext(&builder.branchGraphBuilder)
            subBuilder.BranchContext().SetWorkDir(job.WorkDir())
            subBuilder.BranchContext().SetHDFSWorkDir(job.HDFSWorkDir())
	        subBuilder.BranchContext().SetLogDir(job.LogDir())
            subBuilder.BranchContext().SetInputDir(inputDir)
            subBuilder.BranchContext().SetDirFiles(groupFiles)
            /*
             * The branch name should be built with full file name, including "."
             * suffix. Because some applications may generate a series of files,
             * whose name only differ by "." suffix. So remove suffix will generate
             * same branch name and same output file. But if the branch name to long
             * when full file too many,we will replace it with uuid.
             */
            branchName := strings.Replace(groupFiles[0], "/", "-", -1)
            for i := 1; i < len(groupFiles); i ++ {
                branchName += "-" + strings.Replace(groupFiles[i], "/", "-", -1)
            }
            if len(branchName) > MAX_FILENAME_SIZE {
                u1, _ := uuid.NewV4()
                branchName = u1.String()
            }
            subBuilder.BranchContext().SetName(branchName)

            /*
             * Set built-in branch variables for sub branch here
             */
            subBuilder.BranchContext().SetBranchVariable(BL_VAR_NAME,
                branchName)
            subBuilder.BranchContext().SetBranchVariable(BL_VAR_SHARDGROUP,
                name)
            subBuilder.BranchContext().SetBranchVariable(BL_VAR_SAMPLE,
                builder.BranchContext().SampleName())

            graphInfo := NewBlGraphInfoTree()
            graphInfo.SetBranchItemPos(subBuilder.CurrentBuildPos())
            err, buildStageErrInfo := subBuilder.BuildFlowGraph(graphInfo)
            if err != nil {
                if err == GB_ERR_SUSPEND_BUILD {
                    BuilderLogger.Infof("BuildShardfileGraph(job id %s), subBuilder build pos %d need suspend!",
                        job.ID(), subBuilder.CurrentBuildPos())
                    childGraphBuilderTree.AddChildGraphBuilder(subBuilder, false)
                    graphInfo.SetComplete(false)
                    graphInfoTree.AppendBlGraphInfoTree(graphInfo)
                    continue
                }
                BuilderLogger.Errorf("Build sub graph fail: %s\n",
                    err.Error())
                buildStageErrInfo.ItemName = item.Name()
                return err, nil, buildStageErrInfo
            }

            childGraphBuilderTree.AddChildGraphBuilder(subBuilder, true)
            graphInfo.SetComplete(true)
            graphInfoTree.AppendBlGraphInfoTree(graphInfo)
        } else {
            BuilderLogger.Infof("The filegroup %s has no files\n",
                name)
        }
    }

    /*If no sub graph builder suspend, we go on building otherwise suspend it*/
    if !childGraphBuilderTree.AllChildBuildersComplete() {
        BuilderLogger.Infof("BuildShardfileGraph(job id %s), %d subBuilder suspend!",
            job.ID(), len(childGraphBuilderTree.CurrentChildGraphBuilders()))
        return GB_ERR_SUSPEND_BUILD, nil, nil
    }

    outputNodes, prevCtxts := childGraphBuilderTree.MergeOutputNodes()
    /*
     * Set current's previous branch context here
     */

    builder.BranchContext().MergeOutputsFromChildBranch(prevCtxts, discardTag, forwardTag, true)

    childGraphBuilderTree.DeleteAllChildGraphBuilders()
    graphInfoTree.DeleteAllBlGraphInfoTree()

    return nil, outputNodes, nil
}

func (builder *seqGraphBuilder) BuildBranchSelectorFromJSONFile(item PipelineItem,
    file string) (error, map[int]bool) {
    err, filePath := GetStorageMgr().MapPathToSchedulerVolumeMount(
        file)
    if err != nil {
        BuilderLogger.Errorf("Can't map path %s to scheduler volume mount:%s\n",
            file, err.Error())
        return err, nil
    }

    raw, err := ioutil.ReadFile(filePath)
    if err != nil {
        BuilderLogger.Errorf("Read branch selector file %s fail: %s\n",
            filePath, err.Error())
        return err, nil
    }

    jsonData := BioBranchSelectorJSONData{}
    err = json.Unmarshal(raw, &jsonData)
    if err != nil {
        BuilderLogger.Warnf("Fail load branch variables from file: %s",
            err.Error())
        return err, nil
    }

    /*
     * try to convert name list to index list
     */
    targetIndex := make(map[int]bool)
    if jsonData.Index != nil && len(jsonData.Index) > 0 {
        for i := 0; i < len(jsonData.Index); i ++ {
            index, err := strconv.Atoi(jsonData.Index[i])
            if err != nil {
                msg := fmt.Sprintf("Branch selector index list has non-int value %s, err %s",
                    jsonData.Index[i], err.Error())
                SchedulerLogger.Errorf(msg)
                return errors.New(msg), nil
            } else {
                targetIndex[index] = true
            }
        }
    } else if jsonData.Name != nil && len(jsonData.Name) > 0 {
        for i := 0; i < len(jsonData.Name); i ++ {
            index := -1
            for j := 0; j < len(item.Items()); j ++ {
                subItem := item.Item(j)
                if strings.ToUpper(subItem.Name()) == strings.ToUpper(jsonData.Name[i]) {
                    index = j
                }
            }
            if index < 0 {
                msg := fmt.Sprintf("Branch selector name list has invalid value %s not exist",
                    jsonData.Name[i])
                return errors.New(msg), nil
            } else {
                targetIndex[index] = true
            }
        }
    }

    return nil, targetIndex
}

func (builder *seqGraphBuilder) BuildBranchSelectorFromJSONFileTag(item PipelineItem,
    tag string) (error, map[int]bool) {
    nodes := builder.GetNamedOutputFileNode(tag)
    if len(nodes) == 0 {
        BuilderLogger.Errorf("Fail to build branch selector from non-exist tag file %s\n",
            tag)
        return errors.New("unknown tag file " + tag), nil
    }
    outFile := nodes[0].Stage().GetTargetOutput(tag)
    return builder.BuildBranchSelectorFromJSONFile(item, outFile)
}

/*
 * Handle "CloneParallel" type pipeline item. It works in divide and conquer
 * approach:
 * 1) create a sub seqGraphBuilder for each sub items. input slice of pipeline
 *    is the sub item.
 * 2) each sub graph builder's initial branch context is cloned from current
 *    branch
 * 3) Merge the sub graph produced by each sub graph builders. Union the output
 *    nodes.
 */
func (builder *seqGraphBuilder) BuildCloneParallelGraph(item PipelineItem, 
    prevNodes []Node, graphInfoTree *BlGraphInfoTree) (error,[]Node, *BuildStageErrorInfo) {
    job := builder.Job()
    graph := builder.Graph()

    childGraphBuilderTree := builder.ChildGraphBuilerTree()

    discardTag := item.Discard()
    forwardTag := item.Forward()

    childGraphBuilderTree.SetChildGraphBuildersDiscard(discardTag)
    childGraphBuilderTree.SetChildGraphBuildersForward(forwardTag)
    childGraphBuilderTree.SetMergeTag(false)

    inputDir, _ := item.InputDirAndTag()
    if inputDir == "" {
        inputDir = builder.branchCtxt.inputDir
    } else {
        inputDir = job.WorkDir() + "/" + inputDir
    }
    /*
     * If the item specifies a branch selection condition, the graph builder
     * should suspend and wait for all previous stages complete executing.
     *
     */
    branchSelectorFile, branchSelectorTag := item.BranchSelector()
    if branchSelectorFile != "" || branchSelectorTag != "" {
        /*check whether the inputs are ready or not*/
        if builder.CurrentBuildPos() != builder.CurrentItemPos() {
            BuilderLogger.Debugf("Suspend build clone parallel for selector file ready\n")
            return GB_ERR_SUSPEND_BUILD, nil, nil
        }
    }
    
    var targetBranches map[int]bool = nil
    var err error = nil
    if branchSelectorFile != "" {
        /*build the clone parallel branches by the selector file*/
        workDir := builder.BranchContext().WorkDir()
        file := workDir + "/" + branchSelectorFile
        SchedulerLogger.Debugf("Build branch selector from file %s\n",
            file)
        err, targetBranches = builder.BuildBranchSelectorFromJSONFile(item, file)
        if err != nil {
            SchedulerLogger.Errorf("Build branch selector from file %s fail: %s\n",
                file, err.Error())
            buildStageErrorInfo := NewBuildStageErrorInfo(0, BIOSTAGE_FAILREASON_CLONEPARALLELMISTAKE,
                item.Name(), err)
            buildStageErrorInfo.ItemName = item.Name()
            return err, nil, buildStageErrorInfo
        }
    } else if branchSelectorTag != "" {
        err, targetBranches = builder.BuildBranchSelectorFromJSONFileTag(item,
            branchSelectorTag)
        if err != nil {
            SchedulerLogger.Errorf("Build branch selector from tag %s fail: %s\n",
                branchSelectorTag, err.Error())
            buildStageErrorInfo := NewBuildStageErrorInfo(0, BIOSTAGE_FAILREASON_CLONEPARALLELMISTAKE,
                item.Name(), err)
            buildStageErrorInfo.ItemName = item.Name()
            return err, nil, buildStageErrorInfo
        }
    }


    if targetBranches != nil && len(targetBranches) > 0 {
        /*validate the branch selector*/
        for index, useBranch := range targetBranches {
            if index < 0 || index >= len(item.Items()) {
                SchedulerLogger.Errorf("Job %s item %s has branch selector %d out of range\n",
                    job.GetID(), item.Name(), index)
                errMsg := fmt.Sprintf("Item %s has branch selector %d out of range",
                    item.Name(), index)
                buildStageErrorInfo := NewBuildStageErrorInfo(0, BIOSTAGE_FAILREASON_CLONEPARALLELMISTAKE,
                    item.Name(), errors.New(errMsg))
                buildStageErrorInfo.ItemName = item.Name()
                return errors.New(errMsg), nil, buildStageErrorInfo
            } else if useBranch {
                SchedulerLogger.Debugf("Build job %s item %s by selecting branch %d\n",
                    job.GetID(), item.Name(), index)
            } else {
                SchedulerLogger.Debugf("Build job %s item %s by de-selecting branch %d\n",
                    job.GetID(), item.Name(), index)
            }
        }
    } else {
        SchedulerLogger.Debugf("Build job %s item %s without valid branch selector\n",
            job.GetID(), item.Name())
    }

    for i := 0; i < len(item.Items()); i ++ {
        subItem := item.Item(i)
        if targetBranches != nil {
            if targetBranches[i] {
                SchedulerLogger.Debugf("Build job %s item %s clone branch %d/%s by selector\n",
                    job.GetID(), item.Name(), i, subItem.Name())
            } else {
                SchedulerLogger.Debugf("Skip build job %s item %s clone branch %d/%s by selector\n",
                    job.GetID(), item.Name(), i, subItem.Name())
                continue
            }
        }

        /*
         * We should support two kinds of cases here:
         * 1) subItem itself is a simple item, so we need clone branches
         *    to run the subItems in parallel
         * 2) subItem isn't a simple item, it is a sub pipeline. Now the
         *    sub branches should be cloned to run the subItem's items
         */

        /*by default run it as a sub pipeline*/
        subItemType := subItem.ItemType()
        subItems := subItem.Items()
        /*The item type default is the simple type and if sub items is not zero we want to ensure that run it as a subpipeline.*/
        isNotSubPipeline := (subItemType == GEN_PT_SHARDFILES) ||
                            (subItemType == GEN_PT_VARIABLEPARALLEL) ||
                            (subItemType == GEN_PT_CLONEPARALLEL) ||
                            (subItemType == GEN_PT_PIPELINE) ||
                            (subItemType == GEN_PT_SIMPLE && (subItems == nil || len(subItems) == 0))
        if isNotSubPipeline {
            /*not a sub pipeline, run subItem itself*/
            subItems = make(map[int]PipelineItem)
            subItems[0] = subItem
        }
        BuilderLogger.Debugf("Will build clone sub graph for item %s\n",
            subItem.Name())
        subBuilder := NewSeqGraphBuilder(graph, subItems, job,
            SG_TYPE_CLONE)
        subBuilder.SetPipeline(builder.GetPipeline())
        subBuilder.SetLevel(builder.Level() + 1)
        subBuilder.SetPrevNodes(prevNodes)
        subBuilder.CloneMapContext(&builder.branchGraphBuilder)
        branchName := fmt.Sprintf("%s-%s", builder.BranchContext().Name(),
            subItem.Name())
        subBuilder.BranchContext().SetName(branchName)
        subBuilder.BranchContext().SetInputDir(inputDir)
        subBuilder.BranchContext().SetLogDir(job.LogDir())
        
        /*
         * Set built-in branch variables for sub branch here
         */
        subBuilder.BranchContext().SetBranchVariable(BL_VAR_NAME,
            branchName)
        subBuilder.BranchContext().SetBranchVariable(BL_VAR_SAMPLE,
            builder.BranchContext().SampleName())

        graphInfo := NewBlGraphInfoTree()
        graphInfo.SetBranchItemPos(subBuilder.CurrentBuildPos())

        err, buildStageErrInfo := subBuilder.BuildFlowGraph(graphInfo)
        if err != nil {
            if err == GB_ERR_SUSPEND_BUILD {
                BuilderLogger.Infof("BuildCloneGraph(job id %s), subBuild build pos %d need suspend!",
                    job.ID(), subBuilder.CurrentBuildPos())
                childGraphBuilderTree.AddChildGraphBuilder(subBuilder, false)
                graphInfo.SetComplete(false)
                graphInfoTree.AppendBlGraphInfoTree(graphInfo)
                continue
            }
            BuilderLogger.Errorf("Build sub graph fail: %s\n",
                err.Error())
            return err, nil, buildStageErrInfo
        }

        childGraphBuilderTree.AddChildGraphBuilder(subBuilder, true)
        graphInfo.SetComplete(true)
        graphInfoTree.AppendBlGraphInfoTree(graphInfo)
    }

    /*If no sub graph builder suspend, we go on building otherwise suspend it*/
    if !childGraphBuilderTree.AllChildBuildersComplete() {
        BuilderLogger.Infof("BuildCloneGraph(job id %s), %d subBuilder suspend!",
            job.ID(), len(childGraphBuilderTree.CurrentChildGraphBuilders()))
        return GB_ERR_SUSPEND_BUILD, nil, nil
    }

    outputNodes, prevCtxts := childGraphBuilderTree.MergeOutputNodes()
    /*
     * Set current's previous branch context here
     */
    builder.BranchContext().MergeOutputsFromChildBranch(prevCtxts, discardTag, forwardTag, false)

    childGraphBuilderTree.DeleteAllChildGraphBuilders()
    graphInfoTree.DeleteAllBlGraphInfoTree()

    return nil, outputNodes, nil
}

/*
 * Handle "SubPipeline" type pipeline item.
 * 1) create a sub seqGraphBuilder for the sub items.
 * 2) the builder inputdir is sub builder inputdir.
 * 3) sub builder build the graph.
 * 4) copy the sub builder context to builder context.
 */
func (builder *seqGraphBuilder) BuildSubPipelineGraph(item PipelineItem,
    prevNodes []Node, graphInfoTree *BlGraphInfoTree) (error, []Node, *BuildStageErrorInfo) {
    job := builder.Job()
    graph := builder.Graph()

    childGraphBuilderTree := builder.ChildGraphBuilerTree()

    discardTag := item.Discard()
    forwardTag := item.Forward()

    childGraphBuilderTree.SetChildGraphBuildersDiscard(discardTag)
    childGraphBuilderTree.SetChildGraphBuildersForward(forwardTag)
    childGraphBuilderTree.SetMergeTag(false)

    inputDir, _ := item.InputDirAndTag()
    if inputDir == "" {
        inputDir = builder.branchCtxt.inputDir
    } else {
        inputDir = job.WorkDir() + "/" + inputDir
    }

    subItems := item.Items()
    if subItems == nil || len(subItems) == 0 {
        BuilderLogger.Errorf("Build sub graph fail: the item %s type is a sub pipeline but has no sub items\n",
            item.Name())
        errMsg := fmt.Sprintf("the item %s type is a sub pipeline but has no sub items", item.Name())
        buildStageErrorInfo := NewBuildStageErrorInfo(0, BIOSTAGE_FAILREASON_SUBPIPELINEMISTAKE,
            item.Name(), errors.New(errMsg))
        buildStageErrorInfo.ItemName = item.Name()
        return errors.New(errMsg), nil, nil
    }

    subBuilder := NewSeqGraphBuilder(graph, subItems, job,
        SG_TYPE_PIPELINE)
    subBuilder.SetPipeline(builder.GetPipeline())
    subBuilder.SetLevel(builder.Level() + 1)
    subBuilder.SetPrevNodes(prevNodes)
    subBuilder.CloneMapContext(&builder.branchGraphBuilder)
    branchName := fmt.Sprintf("%s-%s", builder.BranchContext().Name(),
        item.Name())
    subBuilder.BranchContext().SetName(branchName)
    subBuilder.BranchContext().SetWorkDir(job.WorkDir())
    subBuilder.BranchContext().SetHDFSWorkDir(job.HDFSWorkDir())
    subBuilder.BranchContext().SetLogDir(job.LogDir())
    subBuilder.BranchContext().SetInputDir(inputDir)

    /*
     * Set built-in branch variables for sub branch here
     */
    subBuilder.BranchContext().SetBranchVariable(BL_VAR_NAME,
        branchName)
    subBuilder.BranchContext().SetBranchVariable(BL_VAR_SAMPLE,
        builder.BranchContext().SampleName())

    graphInfo := NewBlGraphInfoTree()
    graphInfo.SetBranchItemPos(subBuilder.CurrentBuildPos())

    err, buildStageErrInfo := subBuilder.BuildFlowGraph(graphInfo)
    if err != nil {
        if err == GB_ERR_SUSPEND_BUILD {
            BuilderLogger.Infof("BuildPipelineGraph(job id %s), subBuild build pos %d need suspend!",
                job.ID(), subBuilder.CurrentBuildPos())
            childGraphBuilderTree.AddChildGraphBuilder(subBuilder, false)
            graphInfo.SetComplete(false)
            graphInfoTree.AppendBlGraphInfoTree(graphInfo)
        }
        BuilderLogger.Errorf("Build sub graph fail: %s\n",
            err.Error())
        return err, nil, buildStageErrInfo
    }

    childGraphBuilderTree.AddChildGraphBuilder(subBuilder, true)
    graphInfo.SetComplete(true)
    graphInfoTree.AppendBlGraphInfoTree(graphInfo)

    outputNodes, prevCtxts := childGraphBuilderTree.MergeOutputNodes()
    /*
     * Set current's previous branch context here
     */
    builder.BranchContext().MergeOutputsFromChildBranch(prevCtxts, discardTag, forwardTag, false)

    childGraphBuilderTree.DeleteAllChildGraphBuilders()
    graphInfoTree.DeleteAllBlGraphInfoTree()

    return nil, outputNodes, nil
}

func (builder *seqGraphBuilder) BuildBranchVarListMapFromFile(item PipelineItem,
    fileMap map[string]string) (error, map[string][]string) {
    branchVarList := make(map[string][]string)
    job := builder.Job()
    inputDir := job.InputDataSet().InputDir()

    for name, file := range fileMap {
        inputFile := inputDir + "/" + file
        err, varList := builder.ReadLines(inputFile)
        if err != nil {
            BuilderLogger.Errorf("Fail to read lines of file %s: %s\n",
                inputFile, err.Error())
            return err, nil
        }
        branchVarList[name] = varList
    }

    return nil, branchVarList
}

func (builder *seqGraphBuilder) BuildBranchVarListMapFromJSONFile(item PipelineItem,
    file string) (error, map[string][]string) {
    err, filePath := GetStorageMgr().MapPathToSchedulerVolumeMount(
        file)
    if err != nil {
        BuilderLogger.Errorf("Can't map path %s to scheduler volume mount:%s\n",
            file, err.Error())
        return err, nil
    }

    raw, err := ioutil.ReadFile(filePath)
    if err != nil {
        BuilderLogger.Errorf("Read shard file %s fail: %s\n",
            filePath, err.Error())
        return err, nil
    }

    jsonData := BioBranchVarMapJSONData{}
    err = json.Unmarshal(raw, &jsonData)
    if err != nil {
        BuilderLogger.Warnf("Fail load branch variables from file: %s",
            err.Error())
        return err, nil
    }

    return nil, jsonData.Vars
}

func (builder *seqGraphBuilder) BuildBranchVarListMapFromTag(item PipelineItem,
    tagMap map[string]string) (error, map[string][]string) {
    branchVarList := make(map[string][]string)

    for name, tag := range tagMap {
        nodes := builder.GetNamedOutputFileNode(tag)
        if len(nodes) == 0 {
            BuilderLogger.Errorf("Fail to build branch var %s from non-exist tag file %s\n",
                name, tag)
            return errors.New("unknown tag file " + tag), nil
        }
        outFile := nodes[0].Stage().GetTargetOutput(tag)
        err, varList := builder.ReadLines(outFile)
        if err != nil {
            BuilderLogger.Errorf("Fail to read lines of file %s: %s\n",
                outFile, err.Error())
            return err, nil
        }
        branchVarList[name] = varList
    }

    return nil, branchVarList
}

func (builder *seqGraphBuilder) BuildBranchVarListMapFromJSONFileTag(item PipelineItem,
    tag string) (error, map[string][]string) {
    nodes := builder.GetNamedOutputFileNode(tag)
    if len(nodes) == 0 {
        BuilderLogger.Errorf("Fail to build branch var from non-exist tag file %s\n",
            tag)
        return errors.New("unknown tag file " + tag), nil
    }
    outFile := nodes[0].Stage().GetTargetOutput(tag)
    return builder.BuildBranchVarListMapFromJSONFile(item, outFile)
}

/*
 * Handle the "VariableParallel" style pipeline item. It works in the
 * divide and conquer approach:
 * 1) For each value of the branch variable, create a sub seqGraphBuilder.
 *    The intial branch context is cloned from current branch. The input
 *    files are same but the branch variable is set different.
 * 2) Calls each sub graph builder to produce the sub graph.
 * 3) Merge each output of the sub graph builder and union the output nodes.
 */
func (builder *seqGraphBuilder) BuildVariableParallelGraph(item PipelineItem, 
    prevNodes []Node, graphInfoTree *BlGraphInfoTree) (error, []Node, *BuildStageErrorInfo) {
    job := builder.Job()
    inputDir := job.InputDataSet().InputDir()
    graph := builder.Graph()
    childGraphBuilderTree := builder.ChildGraphBuilerTree()

    discardTag := item.Discard()
    forwardTag := item.Forward()

    childGraphBuilderTree.SetChildGraphBuildersDiscard(discardTag)
    childGraphBuilderTree.SetChildGraphBuildersForward(forwardTag)
    childGraphBuilderTree.SetMergeTag(true)
    var err error
    subBranchCount := 0

    err, branchVarListMap := builder.BuildeBuildVariableBranchVarList(item, inputDir)
    if err != nil {
        BuilderLogger.Infof("BuildVariableParallelGraph(job %s): check variable operation %s",
            job.ID(), err.Error())
        buildStageErrorInfo := NewBuildStageErrorInfo(0, BIOSTAGE_FAILREASON_VARIABLEPARALLELMISTAKE,
            item.Name(), err)
        return err, nil, buildStageErrorInfo
    }
    
    BuilderLogger.Debugf("Branch Var List for job %s item %s: %v\n",
        builder.Job().GetID(), item.Name(), branchVarListMap)

    /*many number of branch variables can be set here, but must be equal size*/
    for key, value := range branchVarListMap {
        for k, v := range branchVarListMap {
            if len(value) != len(v) {
                BuilderLogger.Errorf("The Varibale Parallel branch variables %s lenth %d not equal %s lenth %d",
                    key, len(value), k, len(v))
                errMsg := fmt.Sprintf("Item %s Varibale Parallel branch variables must be equal", item.Name())
                buildStageErrorInfo := NewBuildStageErrorInfo(0, BIOSTAGE_FAILREASON_VARIABLEPARALLELMISTAKE,
                    item.Name(), errors.New(errMsg))
                buildStageErrorInfo.ItemName = item.Name()
                return errors.New(errMsg), nil, buildStageErrorInfo
            }
        }
        subBranchCount = len(branchVarListMap[key])
    }

    for i := 0; i < subBranchCount; i ++ {
        subBuilder := NewSeqGraphBuilder(graph, item.Items(), job,
            SG_TYPE_VARIABLE)
        subBuilder.SetPipeline(builder.GetPipeline())
        subBuilder.SetLevel(builder.Level() + 1)
        subBuilder.SetPrevNodes(prevNodes)
        subBuilder.CloneMapContext(&builder.branchGraphBuilder)
        branchName := builder.BranchContext().Name()
        for key, varList := range branchVarListMap {
            subBuilder.BranchContext().SetBranchVariable(key, varList[i])
            branchName += "-" + varList[i]
        }
        subBuilder.BranchContext().SetName(branchName)

        /*
         * Set built-in branch variables for sub branch here
         */
        subBuilder.BranchContext().SetBranchVariable(BL_VAR_NAME,
            branchName)
        subBuilder.BranchContext().SetBranchVariable(BL_VAR_SAMPLE,
            builder.BranchContext().SampleName())
        subBuilder.BranchContext().SetLogDir(job.LogDir())

        graphInfo := NewBlGraphInfoTree()
        graphInfo.SetBranchItemPos(subBuilder.CurrentBuildPos())

        err, buildStageErrInfo := subBuilder.BuildFlowGraph(graphInfo)
        if err != nil {
            if err == GB_ERR_SUSPEND_BUILD {
                BuilderLogger.Infof("BuildVariableGraph(job id %s), subBuild build pos %d need suspend!",
                    job.ID(), subBuilder.CurrentBuildPos())
                childGraphBuilderTree.AddChildGraphBuilder(subBuilder, false)
                graphInfo.SetComplete(false)
                graphInfoTree.AppendBlGraphInfoTree(graphInfo)
                continue
            }
            BuilderLogger.Errorf("Build sub graph fail: %s\n",
                err.Error())
            return err, nil, buildStageErrInfo
        }

        childGraphBuilderTree.AddChildGraphBuilder(subBuilder, true)
        graphInfo.SetComplete(true)
        graphInfoTree.AppendBlGraphInfoTree(graphInfo)
    }

    /*If no sub graph builder suspend, we go on building otherwise suspend it*/
    if !childGraphBuilderTree.AllChildBuildersComplete() {
        BuilderLogger.Infof("BuildVariableParallelGraph(job id %s), %d subBuilder suspend!",
            job.ID(), len(childGraphBuilderTree.CurrentChildGraphBuilders()))
        return GB_ERR_SUSPEND_BUILD, nil, nil
    }
    outputNodes, prevCtxts := childGraphBuilderTree.MergeOutputNodes()
    /*
     * Set current's previous branch context here
     */
    builder.BranchContext().MergeOutputsFromChildBranch(prevCtxts, discardTag, forwardTag, true)

    childGraphBuilderTree.DeleteAllChildGraphBuilders()
    graphInfoTree.DeleteAllBlGraphInfoTree()

    return nil, outputNodes, nil
}

func (builder *seqGraphBuilder) BuildSingleItemGraph(item PipelineItem, 
    prevNodes []Node, graphInfoTree *BlGraphInfoTree) (error, []Node, *BuildStageErrorInfo) {
    switch item.ItemType() {
        case GEN_PT_SIMPLE:
            return builder.BuildSimpleItemGraph(item, prevNodes)
        case GEN_PT_SHARDFILES:
            return builder.BuildShardFilesGraph(item, prevNodes, graphInfoTree)
        case GEN_PT_VARIABLEPARALLEL:
            return builder.BuildVariableParallelGraph(item, prevNodes, graphInfoTree)
        case GEN_PT_CLONEPARALLEL:
            return builder.BuildCloneParallelGraph(item, prevNodes, graphInfoTree)
        case GEN_PT_PIPELINE:
            return builder.BuildSubPipelineGraph(item, prevNodes, graphInfoTree)
        default:
            BuilderLogger.Errorf("The item %s type %s not recognized \n", 
                item.Name(), item.ItemType())
            return errors.New("Unknown pipeline type " + item.ItemType()),
                nil, nil
    }
}


func (builder *seqGraphBuilder) _BuildFlowGraph(item PipelineItem,
    nodes []Node, graphInfoTree *BlGraphInfoTree) (error, []Node, *BuildStageErrorInfo) {
    job := builder.Job()
    childGraphBuilderTree := builder.ChildGraphBuilerTree()
    if childGraphBuilderTree.AllChildBuildersComplete() {
        BuilderLogger.Debugf("BuildFlowGraph(job id %s), Build single flow graph for item: %s!",
            job.ID(), item.Name())
        return builder.BuildSingleItemGraph(item, nodes, graphInfoTree)
    } else {
        BuilderLogger.Debugf("BuildFlowGraph(job id %s), Build sub flow graph for item: %s!",
            job.ID(), item.Name())
        err, nodes, prevCtxts, buildStageErrorInfo := childGraphBuilderTree.BuildChildGraphBuilders(graphInfoTree)
        if err != nil {
            if err != GB_ERR_SUSPEND_BUILD {
                BuilderLogger.Errorf("BuildFlowGraph(job id %s), fail to build sub graph for item %s: %s\n",
                    job.ID(), item.Name(), err.Error())
            } else {
                BuilderLogger.Debugf("BuildFlowGraph(job id %s), suspend build sub graph when build item %s: %s and start execution\n",
                    job.ID(), item.Name(), err.Error())
            }
            return err, nil, buildStageErrorInfo
        }

        discardTag := childGraphBuilderTree.GetChildGraphBuildersDiscard()
        forwardTag := childGraphBuilderTree.GetChildGraphBuildersForward()
        mergeSameTag := childGraphBuilderTree.GetMergeTag()

        builder.BranchContext().MergeOutputsFromChildBranch(prevCtxts, discardTag, forwardTag, mergeSameTag)

        childGraphBuilderTree.DeleteAllChildGraphBuilders()
        graphInfoTree.DeleteAllBlGraphInfoTree()
        return nil, nodes, nil
    }
}

func (builder *seqGraphBuilder) BuildFlowGraph(graphInfoTree *BlGraphInfoTree) (error, *BuildStageErrorInfo) {
    itemCount := builder.ItemsCount()
    var nodes []Node = builder.PrevNodes()
    var err error
    var buildStageErrInfo *BuildStageErrorInfo
    job := builder.Job()
    builder.SetBuildPos(builder.CurrentItemPos())
    for j := builder.CurrentItemPos(); j < itemCount; j ++ {
        builder.SetItemPos(j)
        item := builder.Item(j)
        graphInfoTree.SetBranchItemPos(builder.CurrentItemPos())
        err, nodes, buildStageErrInfo = builder._BuildFlowGraph(item, nodes, graphInfoTree)
        if err != nil {
            if err != GB_ERR_SUSPEND_BUILD {
                BuilderLogger.Errorf("BuildFlowGraph(job id %s),fail to build graph for item %s: %s\n",
                    job.ID(), item.Name(), err.Error())
            } else {
                BuilderLogger.Debugf("BuildFlowGraph(job id %s), Suspend build graph when build item %s: %s and start execution\n",
                    job.ID(), item.Name(), err.Error())
                graphInfoTree.SetComplete(false)
            }
            return err, buildStageErrInfo
        }
        builder.SetPrevNodes(nodes)
        if j == (itemCount - 1) {
            /*last stage of this builder*/
            builder.SetOutputNodes(nodes)
        }
    }
    graphInfoTree.SetComplete(true)
    return nil, nil
}

type blGraphBuilder struct {
    mainGraphBuilder *seqGraphBuilder
    mainGraphInfo *BlFlowGraphInfo
}

func NewBLGraphBuilder() GraphBuilder {
    return &blGraphBuilder {
        mainGraphBuilder: nil,
        mainGraphInfo: NewBlFlowGraphInfo(),
    }
}

/*
 * blGraphBuilder just calls seqGraphBuilder to produce the flow graph.
 * the seqGraphBuilder works in recursive way and complete the build job.
 */
func (builder *blGraphBuilder) BuildJobFlowGraph(job Job, 
    pipeline Pipeline) (error, Graph, *BuildStageErrorInfo) {
    graph := NewGraph()
    seqBuilder := NewSeqGraphBuilder(graph, pipeline.Items(), job,
        SG_TYPE_MAIN)
    seqBuilder.SetPrevNodes(nil)
    seqBuilder.SetPipeline(pipeline)
    
    /*set work directory and files*/
    dataset := job.InputDataSet()
    seqBuilder.BranchContext().SetWorkDir(job.WorkDir())
    seqBuilder.BranchContext().SetHDFSWorkDir(job.HDFSWorkDir())
    seqBuilder.BranchContext().SetLogDir(job.LogDir())
    seqBuilder.BranchContext().SetInputDir(dataset.InputDir())
    if dataset.Files() != nil && len(dataset.Files()) > 0 {
        seqBuilder.BranchContext().SetDirFiles(dataset.Files())
    }

    /*
     * Initialize the built-in branch variables here:
     * 1) sampleName: it is the sample id of job
     * 2) name: branch name is the sample name by default
     */
    seqBuilder.BranchContext().SetSampleName(job.SMID())
    seqBuilder.BranchContext().SetName(job.SMID())

    graphInfo := NewBlGraphInfoTree()

    err, buildStageErrInfo := seqBuilder.BuildFlowGraph(graphInfo)
    if err != nil {
        if err != GB_ERR_SUSPEND_BUILD {
            BuilderLogger.Errorf("Fail to build flow graph for job %s: %s\n",
                job.GetID(), err.Error())
            return err, nil, buildStageErrInfo
        } else {
            BuilderLogger.Debugf("Build a partial graph now, execute it\n")
            builder.mainGraphBuilder = seqBuilder
            builder.mainGraphInfo.Completed = false
            builder.mainGraphInfo.GraphInfoTree = graphInfo
            return err, graph, nil
        }
    } else {
        builder.mainGraphBuilder = nil
        builder.mainGraphInfo = nil
    }

    return nil, graph, nil
}

func (builder *blGraphBuilder) ResumeBuildJobFlowGraph() (error, *BuildStageErrorInfo) {
    if builder.mainGraphBuilder != nil {
        return builder.mainGraphBuilder.BuildFlowGraph(builder.mainGraphInfo.GraphInfoTree)
    } else {
        return errors.New("No graph builder context"), nil
    }
}

func (builder *blGraphBuilder) GetGraphBuildInfo(info *BlFlowGraphInfo) {
    if builder.mainGraphInfo != nil {
        info.Completed = builder.mainGraphInfo.Completed
        info.GraphInfoTree = builder.mainGraphInfo.GraphInfoTree
    } else {
        info.Completed = true
    }
}

