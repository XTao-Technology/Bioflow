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
    "encoding/json"
)


/*tracking flow graph build state*/
type BlFlowGraphInfo struct {
    Completed bool
    GraphInfoTree *BlGraphInfoTree
}

func NewBlFlowGraphInfo() *BlFlowGraphInfo {
    return &BlFlowGraphInfo {
        Completed: false,
        GraphInfoTree: NewBlGraphInfoTree(),
    }
}


func (info *BlFlowGraphInfo)FlowGraphInfoToJSON() (error, string) {
    body, err := json.Marshal(&info)
    if err != nil {
        return err, ""
    }
    return nil, string(body)
}

func (info *BlFlowGraphInfo)FlowGraphInfoFromJSON(jsonData string) error {
    infoJson := BlFlowGraphInfo {
        GraphInfoTree: NewBlGraphInfoTree(),
    }
    err := json.Unmarshal([]byte(jsonData), &infoJson)
    if err != nil {
        return err
    }

    info.Completed = infoJson.Completed
    blGraphInfo := NewBlGraphInfoTree()
    blGraphInfo.FromJson(infoJson.GraphInfoTree)
    info.GraphInfoTree = blGraphInfo

    return nil
}

func (info *BlFlowGraphInfo)FlowGraphInfoCompleted() bool {
    return info.Completed
}

func (info *BlFlowGraphInfo)FlowGraphInfoSetCompleted(complete bool) {
    info.Completed = complete
}

func (info *BlFlowGraphInfo)GetBlGraphInfoTree() *BlGraphInfoTree {
    return info.GraphInfoTree
}


type BlGraphInfoTree struct {
    BranchItemPos int
    Complete bool
    BlGraphInfoTree  []*BlGraphInfoTree
}

func NewBlGraphInfoTree() *BlGraphInfoTree {
    return &BlGraphInfoTree {
        BranchItemPos: 0,
        Complete: false,
        BlGraphInfoTree: make([]*BlGraphInfoTree, 0),
    }
}

func (infoTree *BlGraphInfoTree)FromJson(graphInfoTree *BlGraphInfoTree) {
    infoTree.Complete = graphInfoTree.Complete
    infoTree.BranchItemPos = graphInfoTree.BranchItemPos
    for i := 0; i < len(graphInfoTree.BlGraphInfoTree); i ++ {
        subInfoTree := NewBlGraphInfoTree()
        subInfoTree.FromJson(graphInfoTree.BlGraphInfoTree[i])
        infoTree.BlGraphInfoTree = append(infoTree.BlGraphInfoTree, subInfoTree)
    }
}

func (infoTree *BlGraphInfoTree)SetBranchItemPos(pos int) {
    infoTree.BranchItemPos = pos
}

func (infoTree *BlGraphInfoTree)SetComplete(complete bool) {
    infoTree.Complete = complete
}

func (infoTree *BlGraphInfoTree)AppendBlGraphInfoTree(graphInfoTree *BlGraphInfoTree) {
    infoTree.BlGraphInfoTree = append(infoTree.BlGraphInfoTree, graphInfoTree)
}

func (infoTree *BlGraphInfoTree)DeleteAllBlGraphInfoTree() {
    infoTree.BlGraphInfoTree = infoTree.BlGraphInfoTree[:0:0]
}