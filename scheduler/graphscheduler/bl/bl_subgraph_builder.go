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
    . "github.com/xtao/bioflow/scheduler/common"
    . "github.com/xtao/bioflow/message"
    . "github.com/xtao/bioflow/common"
)


type BlChildGraphBuilderTree struct {
    ChildGraphBuilders []*seqGraphBuilder
    /*
     * The slice label hold every subgraphbuilder if
     * build accomplish,so complete labels have as many
     * as subGraphBuilders,and they come in pairs.
     */
    Completed []bool

    Discard []string
    Forward []string
    /*
     * The merge tag is used for whether merge same tag
     * from child branch to father branch, by default,
     * shardfiles and variable is true other is false.
     */
    MergeTag bool
}

func NewBlSubGraphBuiler() *BlChildGraphBuilderTree {
    return &BlChildGraphBuilderTree{
        ChildGraphBuilders: make([]*seqGraphBuilder, 0),
        Discard: make([]string, 0),
        Forward: make([]string, 0),
        MergeTag: false,
    }
}

func (childGraphBuilder *BlChildGraphBuilderTree) SetMergeTag(mergeTag bool) {
    childGraphBuilder.MergeTag = mergeTag
}

func(childGraphBuilder *BlChildGraphBuilderTree) GetMergeTag() bool {
    return childGraphBuilder.MergeTag
}

/*judge the graph builders whether hold not complete subgraphbuilders*/
func (childGraphBuilder *BlChildGraphBuilderTree) AllChildBuildersComplete() bool {
    flag := true
    for i := 0; i < len(childGraphBuilder.Completed); i ++ {
        if childGraphBuilder.Completed[i] {
            continue
        } else {
            flag = false
            break
        }
    }
    return flag
}

func (childGraphBuilder *BlChildGraphBuilderTree) SetChildGraphBuildersDiscard(discardTag []string) {
    childGraphBuilder.Discard = discardTag
}

func (childGraphBuilder *BlChildGraphBuilderTree) GetChildGraphBuildersDiscard() []string {
    return childGraphBuilder.Discard
}

func (childGraphBuilder *BlChildGraphBuilderTree) SetChildGraphBuildersForward(forwardTag []string) {
    childGraphBuilder.Forward = forwardTag
}

func (childGraphBuilder *BlChildGraphBuilderTree) GetChildGraphBuildersForward() []string {
    return childGraphBuilder.Forward
}

func (childGraphBuilder *BlChildGraphBuilderTree) CurrentChildGraphBuilders() []*seqGraphBuilder {
    return childGraphBuilder.ChildGraphBuilders
}

func (childGraphBuilder *BlChildGraphBuilderTree) AddChildGraphBuilder(graphBuilder *seqGraphBuilder, completed bool) {
    childGraphBuilder.ChildGraphBuilders = append(childGraphBuilder.ChildGraphBuilders, graphBuilder)
    childGraphBuilder.Completed = append(childGraphBuilder.Completed, completed)
}

func (childGraphBuilder *BlChildGraphBuilderTree) DeleteAllChildGraphBuilders() {
    childGraphBuilder.ChildGraphBuilders = childGraphBuilder.ChildGraphBuilders[:0:0]
    childGraphBuilder.Completed = childGraphBuilder.Completed[:0:0]
}

func (childGraphBuilder *BlChildGraphBuilderTree) SetCompletedLabel(complete bool, i int) {
    childGraphBuilder.Completed[i] = complete
}

func (childGraphBuilder *BlChildGraphBuilderTree) BuildChildGraphBuilders(graphInfoTree *BlGraphInfoTree) (error,
    []Node, []*branchContext, *BuildStageErrorInfo) {
    outputNodes := make([]Node, 0)
    prevCtxts := make([]*branchContext, 0)
    for i := 0; i < len(childGraphBuilder.ChildGraphBuilders); i ++ {
        subBuilder := childGraphBuilder.ChildGraphBuilders[i]
        if !childGraphBuilder.Completed[i] {
            BuilderLogger.Debugf("BuildFlowGraph(job id %s), Build sub flow graph, graphPos: %d, graphInfoTree lenth: %d!",
                subBuilder.Job().ID(), i, len(graphInfoTree.BlGraphInfoTree))
            err, buildStageErrorInfo := subBuilder.BuildFlowGraph(graphInfoTree.BlGraphInfoTree[i])
            if err != nil {
                if err == GB_ERR_SUSPEND_BUILD {
                    continue
                } else {
                    return err, nil, nil, buildStageErrorInfo
                }
            } else {
                childGraphBuilder.SetCompletedLabel(true, i)
            }
        }
    }

    if !childGraphBuilder.AllChildBuildersComplete() {
        return GB_ERR_SUSPEND_BUILD, nil, nil, nil
    } else {
        for i := 0; i < len(childGraphBuilder.ChildGraphBuilders); i ++ {
            subBuilder := childGraphBuilder.ChildGraphBuilders[i]
            outputNodes = append(outputNodes, subBuilder.OutputNodes()...)
            prevCtxts = append(prevCtxts, subBuilder.BranchContext())
        }
        return nil, outputNodes, prevCtxts, nil
    }
}

func (childGraphBuilder *BlChildGraphBuilderTree) MergeOutputNodes() ([]Node, []*branchContext) {
    outputNodes := make([]Node, 0)
    prevCtxts := make([]*branchContext, 0)
    for i := 0; i < len(childGraphBuilder.ChildGraphBuilders); i ++ {
        subBuilder := childGraphBuilder.ChildGraphBuilders[i]
        outputNodes = append(outputNodes, subBuilder.OutputNodes()...)
        prevCtxts = append(prevCtxts, subBuilder.BranchContext())
    }

    return outputNodes, prevCtxts
}