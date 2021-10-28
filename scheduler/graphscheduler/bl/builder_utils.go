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
    "errors"
    "sort"
)

/*In accordance with the Node.ID() sort from large to small*/
type NodeSlice [] Node

func (a NodeSlice) Len() int {
    return len(a)
}
func (a NodeSlice) Swap(i, j int){
    a[i], a[j] = a[j], a[i]
}

/*sort from large to small*/
func (a NodeSlice) Less(i, j int) bool {
    return a[j].ID().String() < a[i].ID().String()
}

type nodeSlice struct{
}

/*delete lenth nodes*/
func (b *nodeSlice) DeleteNodes(nodes []Node, lenth int) []Node {
    /*First, the sorting ensures that the node id is at the beginning*/
    sort.Sort(sort.Reverse(NodeSlice(nodes)))
    end := len(nodes)
    return nodes[lenth:end]
}


type tagSlice struct {
    tags []string
}

func NewTagSlice(tags []string) *tagSlice {
    tagSlice := &tagSlice{
        tags: make([]string, 0),
    }
    tagSlice.tags = append(tagSlice.tags, tags ...)
    return tagSlice
}

func (tags *tagSlice) SliceToMap() map[string]bool {
    tmpMap := make(map[string]bool)
    for i := 0; i < len(tags.tags); i ++ {
        tmpMap[tags.tags[i]] = true
    }
    return tmpMap
}

func (tags *tagSlice) InSlice(value string) bool {
    if _, ok := tags.SliceToMap()[value]; ok {
        return true
    }
    return false
}

type sortedNodeMap struct {
    nodes []Node
    /*
     * nodes are replaced the replacementMarked
     * is true if not it is false
     */
    replacementMarked bool
}

func NewSortedNodeMap(size int) *sortedNodeMap {
    if size <= 0 {
        return &sortedNodeMap{
            nodes: make([]Node, 0),
            replacementMarked: false,
        }
    } else {
        return &sortedNodeMap{
            nodes: make([]Node, size),
            replacementMarked: false,
        }
    }
}

func (nodeMap *sortedNodeMap)GetReplacementMark() bool {
    return nodeMap.replacementMarked
}

func (nodeMap *sortedNodeMap)ReplaceNode(node Node) {
    tmpNode := make([]Node, 0)
    tmpNode = append(tmpNode, node)
    nodeMap.nodes = tmpNode
    nodeMap.replacementMarked = true
}

func (nodeMap *sortedNodeMap)ReplaceNodes(nodes []Node) {
    tmpNode := make([]Node, 0)
    tmpNode = append(tmpNode, nodes ...)
    nodeMap.nodes = tmpNode
}

func (nodeMap *sortedNodeMap)AddNode(node Node) {
    nodeMap.nodes = append(nodeMap.nodes, node)
}

func (nodeMap *sortedNodeMap)AddNodes(nodes []Node) {
    nodeMap.nodes = append(nodeMap.nodes, nodes ...)
}

func (nodeMap *sortedNodeMap)GetIndexedNode(index int) (Node, bool) {
    if index >= len(nodeMap.nodes) || index < 0 {
        return nil, false
    }

    return nodeMap.nodes[index], true
}

func (nodeMap *sortedNodeMap)GetAllNodes() []Node {
    return nodeMap.nodes
}

func (nodeMap *sortedNodeMap)Size() int{
    if nodeMap.nodes == nil {
        return 0
    }

    return len(nodeMap.nodes)
}

/*
 * basic utils to support input map of stage
 */
type NamedNodeMap struct {
    nameNodeMap map[string]*sortedNodeMap 
}

func NewNamedNodeMap() *NamedNodeMap{
    return &NamedNodeMap{
            nameNodeMap: make(map[string]*sortedNodeMap),
    }
}

/*The func is seriousness, The same name is overridden*/
func (nodeMap *NamedNodeMap)ReplaceNameNode(name string, node Node) error {
    if _, ok := nodeMap.nameNodeMap[name]; !ok {
        newMap := NewSortedNodeMap(0)
        if newMap == nil {
            return errors.New("Out of memory")
        }
        nodeMap.nameNodeMap[name] = newMap
    }

    nodeMap.nameNodeMap[name].ReplaceNode(node)

    return  nil
}

func (nodeMap *NamedNodeMap)ReplaceNameNodes(name string, nodes []Node) error {
    if _, ok := nodeMap.nameNodeMap[name]; !ok {
        newMap := NewSortedNodeMap(0)
        if newMap == nil {
            return errors.New("Out of memory")
        }
        nodeMap.nameNodeMap[name] = newMap
    }

    nodeMap.nameNodeMap[name].ReplaceNodes(nodes)

    return nil
}

func (nodeMap *NamedNodeMap)AddNamedNode(name string, node Node) error {
    if _, ok := nodeMap.nameNodeMap[name]; !ok {
        newMap := NewSortedNodeMap(0) 
        if newMap == nil {
            return errors.New("Out of memory")
        }
        nodeMap.nameNodeMap[name] = newMap
    }

    nodeMap.nameNodeMap[name].AddNode(node)

    return nil
}

func (nodeMap *NamedNodeMap)AddNamedNodes(name string, nodes []Node) error {
    if _, ok := nodeMap.nameNodeMap[name]; !ok {
        newMap := NewSortedNodeMap(0) 
        if newMap == nil {
            return errors.New("Out of memory")
        }
        nodeMap.nameNodeMap[name] = newMap
    }

    nodeMap.nameNodeMap[name].AddNodes(nodes)

    return nil
}

func (nodeMap *NamedNodeMap)DeleteNamedNodes(name string) {
    if _, ok := nodeMap.nameNodeMap[name]; ok {
        delete(nodeMap.nameNodeMap, name)
    }
}

func (nodeMap *NamedNodeMap)DeleteAllNamedNodes() {
    for name, _ := range nodeMap.nameNodeMap  {
        nodeMap.DeleteNamedNodes(name)
    }
}

func (nodeMap *NamedNodeMap)GetIndexedNamedNode(name string,
    index int) (Node, bool) {
    if sortedMap, ok := nodeMap.nameNodeMap[name]; ok {
        return sortedMap.GetIndexedNode(index)
    }

    return nil, false
}

/*Get all the nodes tagged with name*/
func (nodeMap *NamedNodeMap)GetNamedNodes(name string) []Node {
    if sortedMap, ok := nodeMap.nameNodeMap[name]; ok {
        return sortedMap.GetAllNodes()
    }

    return nil
}

/*Get the first node tagged with name*/
func (nodeMap *NamedNodeMap)GetNamedNode(name string) (Node, bool) {
    return nodeMap.GetIndexedNamedNode(name, 0)
}

func (nodeMap *NamedNodeMap)Visit(fn func(string, Node) bool) {
    if nodeMap.nameNodeMap != nil {
        for name, sortedMap := range nodeMap.nameNodeMap {
            nodes := sortedMap.GetAllNodes()
            for i := 0; i < len(nodes); i ++ {
                if fn(name, nodes[i]) {
                    return
                }
            }
        }
    }
}

type InputCollectMap struct {
    fileNodeCollected map[string]bool
    dirNodeCollected map[string]bool
    filesCollected bool
}

func NewInputCollectMap() *InputCollectMap {
    return &InputCollectMap {
            fileNodeCollected: make(map[string]bool),
            dirNodeCollected: make(map[string]bool),
            filesCollected: false,
    }
}

func (cmap *InputCollectMap) IsFileInputCollected(tag string) bool {
    if has, ok := cmap.fileNodeCollected[tag]; ok && has {
        return true
    }

    return false
}

func (cmap *InputCollectMap) IsDirInputCollected(tag string) bool {
    if has, ok := cmap.dirNodeCollected[tag]; ok && has {
        return true
    }

    return false
}

func (cmap *InputCollectMap) IsFilesCollected() bool {
    return cmap.filesCollected
}

func (cmap *InputCollectMap) MarkFileInputCollected(tag string) {
    cmap.fileNodeCollected[tag] = true
}

func (cmap *InputCollectMap) MarkDirInputCollected(tag string) {
    cmap.dirNodeCollected[tag] = true
}

func (cmap *InputCollectMap) MarkFilesCollected() {
    cmap.filesCollected = true
}
