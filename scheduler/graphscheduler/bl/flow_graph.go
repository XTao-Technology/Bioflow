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
	"bytes"
	"fmt"
	"sync"
)

// ID is unique identifier.
type ID interface {
	// String returns the string ID.
	String() string
}

type StringID string

func (s StringID) String() string {
	return string(s)
}

const NODE_STATE_INITIAL uint32 = 1
const NODE_STATE_PENDING uint32 = 2
const NODE_STATE_COMPLETE uint32 = 3
const NODE_STATE_FAIL uint32 = 4
const NODE_STATE_LOST uint32 = 5
const NODE_STATE_FORBIDDEN uint32 = 6
const NODE_STATE_SCHEDULING uint32 = 7

const GRAPH_STATE_COMPLETE_FINISH uint32 = 1
const GRAPH_STATE_PSEUDO_FINISH uint32 = 2

// Node is vertex. The ID must be unique within the graph.
type Node interface {
	// ID returns the ID.
	ID() ID
	String() string
    Stage() *blCmdStage
    State() uint32
    SetState(state uint32)
}

type node struct {
    state uint32
	id string
    stage *blCmdStage
}

func NewNode(id string, stage *blCmdStage) Node {
	return &node{
		id: id,
        stage: stage,
        state: NODE_STATE_INITIAL ,
	}
}

func (n *node) ID() ID {
	return StringID(n.id)
}

func (n *node) String() string {
	return n.id
}

func (n *node) Stage() *blCmdStage {
    return n.stage
}

func (n *node) State() uint32 {
    return n.state
}

func (n *node) SetState(state uint32) {
    n.state = state
}

// Edge connects between two Nodes.
type Edge interface {
	Source() Node
	Target() Node
	String() string
    Weight() float64
}

// edge is an Edge from Source to Target.
type edge struct {
	src Node
	tgt Node
}

func NewEdge(src, tgt Node) Edge {
	return &edge{
		src: src,
		tgt: tgt,
	}
}

func (e *edge) Source() Node {
	return e.src
}

func (e *edge) Target() Node {
	return e.tgt
}

func (e *edge) Weight() float64 {
    return 0
}

func (e *edge) String() string {
	return fmt.Sprintf("%s -→ %s\n", e.src, e.tgt)
}

type EdgeSlice []Edge

func (e EdgeSlice) Len() int           { return len(e) }
func (e EdgeSlice) Less(i, j int) bool { return e[i].Weight() < e[j].Weight() }
func (e EdgeSlice) Swap(i, j int)      { e[i], e[j] = e[j], e[i] }

// Graph describes the methods of graph operations.
// It assumes that the identifier of a Node is unique.
// And weight values is float64.
type Graph interface {
	// Init initializes a Graph.
	Init()

	// GetNodeCount returns the total number of nodes.
	GetNodeCount() int

	// GetNode finds the Node. It returns nil if the Node
	// does not exist in the graph.
	GetNode(id ID) Node

	// GetNodes returns a map from node ID to
	// empty struct value. Graph does not allow duplicate
	// node ID or name.
	GetNodes() map[ID]Node

	// AddNode adds a node to a graph, and returns false
	// if the node already existed in the graph.
	AddNode(nd Node) bool

	// DeleteNode deletes a node from a graph.
	// It returns true if it got deleted.
	// And false if it didn't get deleted.
	DeleteNode(id ID) bool

	// AddEdge adds an edge from nd1 to nd2 with the weight.
	// It returns error if a node does not exist.
	AddEdge(id1, id2 ID, weight float64) error

	// ReplaceEdge replaces an edge from id1 to id2 with the weight.
	ReplaceEdge(id1, id2 ID, weight float64) error

	// DeleteEdge deletes an edge from id1 to id2.
	DeleteEdge(id1, id2 ID) error

	// GetWeight returns the weight from id1 to id2.
	GetWeight(id1, id2 ID) (float64, error)

	// GetSources returns the map of parent Nodes.
	// (Nodes that come towards the argument vertex.)
	GetSources(id ID) (map[ID]Node, error)

	// GetTargets returns the map of child Nodes.
	// (Nodes that go out of the argument vertex.)
	GetTargets(id ID) (map[ID]Node, error)

	// String describes the Graph.
	String() string

    // Find a node not depending on any other pending nodes
    FindReadyNodes(bool) (map[ID]Node, uint32)

    // Find all nodes waiting to be run
    FindWaitingNodes() (map[ID]Node, map[ID]Node)

    // Mark all nodes forbid state when a node failed
    MarkNodesForbiddenState() int
}

// graph is an internal default graph type that
// implements all methods in Graph interface.
type graph struct {
	mu sync.RWMutex // guards the following

	// idToNodes stores all nodes.
	idToNodes map[ID]Node

	// nodeToSources maps a Node identifer to sources(parents) with edge weights.
	nodeToSources map[ID]map[ID]bool

	// nodeToTargets maps a Node identifer to targets(children) with edge weights.
	nodeToTargets map[ID]map[ID]bool
}

// newGraph returns a new graph.
func newGraph() *graph {
	return &graph{
		idToNodes:     make(map[ID]Node),
		nodeToSources: make(map[ID]map[ID]bool),
		nodeToTargets: make(map[ID]map[ID]bool),
		//
		// without this
		// panic: assignment to entry in nil map
	}
}

// NewGraph returns a new graph.
func NewGraph() Graph {
	return newGraph()
}

func (g *graph) Init() {
	// (X) g = newGraph()
	// this only updates the pointer
	//
	//
	// (X) *g = *newGraph()
	// assignment copies lock value

	g.idToNodes = make(map[ID]Node)
	g.nodeToSources = make(map[ID]map[ID]bool)
	g.nodeToTargets = make(map[ID]map[ID]bool)
}

func (g *graph) GetNodeCount() int {
	g.mu.RLock()
	defer g.mu.RUnlock()

	return len(g.idToNodes)
}

func (g *graph) GetNode(id ID) Node {
	g.mu.RLock()
	defer g.mu.RUnlock()

	return g.idToNodes[id]
}

func (g *graph) GetNodes() map[ID]Node {
	g.mu.RLock()
	defer g.mu.RUnlock()

	return g.idToNodes
}

func (g *graph) unsafeExistID(id ID) bool {
	_, ok := g.idToNodes[id]
	return ok
}

func (g *graph) AddNode(nd Node) bool {
	g.mu.Lock()
	defer g.mu.Unlock()

	if g.unsafeExistID(nd.ID()) {
		return false
	}

	id := nd.ID()
	g.idToNodes[id] = nd
	return true
}

func (g *graph) DeleteNode(id ID) bool {
	g.mu.Lock()
	defer g.mu.Unlock()

	if !g.unsafeExistID(id) {
		return false
	}

	delete(g.idToNodes, id)

	delete(g.nodeToTargets, id)
	for _, smap := range g.nodeToTargets {
		delete(smap, id)
	}

	delete(g.nodeToSources, id)
	for _, smap := range g.nodeToSources {
		delete(smap, id)
	}

	return true
}

func (g *graph) AddEdge(id1, id2 ID, weight float64) error {
	g.mu.Lock()
	defer g.mu.Unlock()

	if !g.unsafeExistID(id1) {
		return fmt.Errorf("%s does not exist in the graph.", id1)
	}
	if !g.unsafeExistID(id2) {
		return fmt.Errorf("%s does not exist in the graph.", id2)
	}

	if _, ok := g.nodeToTargets[id1]; ok {
		if _, ok2 := g.nodeToTargets[id1][id2]; ok2 {
			g.nodeToTargets[id1][id2] = true
		} else {
			g.nodeToTargets[id1][id2] = true
		}
	} else {
		tmap := make(map[ID]bool)
		tmap[id2] = true
		g.nodeToTargets[id1] = tmap
	}
	if _, ok := g.nodeToSources[id2]; ok {
		if _, ok2 := g.nodeToSources[id2][id1]; ok2 {
			g.nodeToSources[id2][id1] = true
		} else {
			g.nodeToSources[id2][id1] = true
		}
	} else {
		tmap := make(map[ID]bool)
		tmap[id1] = true
		g.nodeToSources[id2] = tmap
	}

	return nil
}

func (g *graph) ReplaceEdge(id1, id2 ID, weight float64) error {
	g.mu.Lock()
	defer g.mu.Unlock()

	if !g.unsafeExistID(id1) {
		return fmt.Errorf("%s does not exist in the graph.", id1)
	}
	if !g.unsafeExistID(id2) {
		return fmt.Errorf("%s does not exist in the graph.", id2)
	}

	if _, ok := g.nodeToTargets[id1]; ok {
		g.nodeToTargets[id1][id2] = true
	} else {
		tmap := make(map[ID]bool)
		tmap[id2] = true
		g.nodeToTargets[id1] = tmap
	}
	if _, ok := g.nodeToSources[id2]; ok {
		g.nodeToSources[id2][id1] = true
	} else {
		tmap := make(map[ID]bool)
		tmap[id1] = true
		g.nodeToSources[id2] = tmap
	}
	return nil
}

func (g *graph) DeleteEdge(id1, id2 ID) error {
	g.mu.Lock()
	defer g.mu.Unlock()

	if !g.unsafeExistID(id1) {
		return fmt.Errorf("%s does not exist in the graph.", id1)
	}
	if !g.unsafeExistID(id2) {
		return fmt.Errorf("%s does not exist in the graph.", id2)
	}

	if _, ok := g.nodeToTargets[id1]; ok {
		if _, ok := g.nodeToTargets[id1][id2]; ok {
			delete(g.nodeToTargets[id1], id2)
		}
	}
	if _, ok := g.nodeToSources[id2]; ok {
		if _, ok := g.nodeToSources[id2][id1]; ok {
			delete(g.nodeToSources[id2], id1)
		}
	}
	return nil
}

func (g *graph) GetWeight(id1, id2 ID) (float64, error) {
	g.mu.RLock()
	defer g.mu.RUnlock()

	if !g.unsafeExistID(id1) {
		return 0, fmt.Errorf("%s does not exist in the graph.", id1)
	}
	if !g.unsafeExistID(id2) {
		return 0, fmt.Errorf("%s does not exist in the graph.", id2)
	}

	if _, ok := g.nodeToTargets[id1]; ok {
		if _, ok := g.nodeToTargets[id1][id2]; ok {
			return 1, nil
		}
	}
	return 0.0, fmt.Errorf("there is no edge from %s to %s", id1, id2)
}

func (g *graph) _GetSources(id ID) (map[ID]Node, error) {
    if !g.unsafeExistID(id) {
        return nil, fmt.Errorf("%s does not exist in the graph.", id)
    }

    rs := make(map[ID]Node)
    if _, ok := g.nodeToSources[id]; ok {
        for n := range g.nodeToSources[id] {
            rs[n] = g.idToNodes[n]
        }
    }
    return rs, nil
}

func (g *graph) GetSources(id ID) (map[ID]Node, error) {
	g.mu.RLock()
	defer g.mu.RUnlock()

	return g._GetSources(id)
}

func (g *graph) _GetTargets(id ID) (map[ID]Node, error) {
    if !g.unsafeExistID(id) {
        return nil, fmt.Errorf("%s does not exist in the graph.", id)
    }

    rs := make(map[ID]Node)
    if _, ok := g.nodeToTargets[id]; ok {
        for n := range g.nodeToTargets[id] {
            rs[n] = g.idToNodes[n]
        }
    }
    return rs, nil    
}

func (g *graph) GetTargets(id ID) (map[ID]Node, error) {
	g.mu.RLock()
	defer g.mu.RUnlock()

	return g._GetTargets(id)
}

func (g *graph) String() string {
	g.mu.RLock()
	defer g.mu.RUnlock()

	buf := new(bytes.Buffer)
	for id1, nd1 := range g.idToNodes {
		nmap, _ := g.GetTargets(id1)
		for id2, nd2 := range nmap {
			weight, _ := g.GetWeight(id1, id2)
			fmt.Fprintf(buf, "%s -- %.3f -→ %s\n", nd1, weight, nd2)
		}
	}
	return buf.String()
}

func (g *graph) MarkNodesForbiddenState() int {
    g.mu.Lock()
    defer g.mu.Unlock()

    var forbidden_num int = 0
    var i int = 0
    var mark_num int = 0

    for i = 0; i < len(g.idToNodes); i ++ {
        for id, nd := range g.idToNodes {
            snmap, _ := g._GetTargets(id)
            if nd.State() == NODE_STATE_FORBIDDEN {
                for _, ndTarget := range snmap {
                    if ndTarget.State() != NODE_STATE_FORBIDDEN {
                        ndTarget.SetState(NODE_STATE_FORBIDDEN)
                        forbidden_num ++
                        mark_num ++
                    }
                }
            }
        }
        if forbidden_num != 0 {
            forbidden_num = 0
            continue
        } else {
            /*The freudian algorithm only has no sign in all of the nodes that we're done.*/
            break
        }
    }
    return mark_num
}

func (g *graph) FindReadyNodes(markState bool) (map[ID]Node, uint32) {
    g.mu.Lock()
    defer g.mu.Unlock()

    var complete_num int = 0
    var total_pending_num int = 0
    var total_forbidden_num int = 0
    var pending int = 0
	rs := make(map[ID]Node)

	for id1, nd1 := range g.idToNodes {
		snmap, _ := g._GetSources(id1)
        switch nd1.State() {
        case NODE_STATE_COMPLETE:
            complete_num ++
        case NODE_STATE_PENDING:
            total_pending_num ++
        case NODE_STATE_FORBIDDEN:
            total_forbidden_num ++
        case NODE_STATE_SCHEDULING:
            /*The node is scheduling, should not be scheduled again*/
        case NODE_STATE_INITIAL, NODE_STATE_LOST, NODE_STATE_FAIL:
            pending = 0
            for _, nd2 := range snmap {
                if nd2.State() != NODE_STATE_COMPLETE {
                    pending ++
                }
            }
            if pending == 0 {
                rs[id1] = nd1
                if markState {
                    /*
                     * Mark the node SCHEDULING to avoid schedule it twice
                     */
                    nd1.SetState(NODE_STATE_SCHEDULING)
                }
            }
        }
    }

    if complete_num == len(g.idToNodes) {
        return nil, GRAPH_STATE_COMPLETE_FINISH
    } else if len(g.idToNodes) == (total_forbidden_num + complete_num) {
        /*
         * Some node failures cause other nodes that are
         * associated with it not to be executed.
         */
        return nil, GRAPH_STATE_PSEUDO_FINISH
    } else {
        return rs, 0
    }
}

func (g *graph) FindWaitingNodes() (map[ID]Node, map[ID]Node) {
	g.mu.RLock()
	defer g.mu.RUnlock()

	rs := make(map[ID]Node)
	fs := make(map[ID]Node)

	for id1, nd1 := range g.idToNodes {
        if nd1.State() != NODE_STATE_COMPLETE  &&
            nd1.State() != NODE_STATE_PENDING &&
            nd1.State() != NODE_STATE_FAIL &&
            nd1.State() != NODE_STATE_SCHEDULING &&
            nd1.State() != NODE_STATE_FORBIDDEN {
                rs[id1] = nd1
        }
        if nd1.State() == NODE_STATE_FORBIDDEN {
            fs[id1] = nd1
        }
	}

    return rs, fs
}
