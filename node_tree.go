package main

import (
	"fmt"
	"os"
	"strings"
)

const AllocNodeChunk = 100000

type iNode struct {
	Parent uint64
	Name   []byte
}

type Node struct {
	SnapId map[uint32]*iNode
}

type NodeTree struct {
	prealloc     []Node
	preallocUsed int
	data         map[uint64]*Node
	//	prevPathID   uint64
	//	prevPath     []string
}

func NewNodeTree() *NodeTree {
	return &NodeTree{
		data:         make(map[uint64]*Node),
		prealloc:     make([]Node, AllocNodeChunk),
		preallocUsed: 0,
		//		prevPathID:   RootInodeID,
		//		prevPath:     []string{"/"},
	}
}

func (t *NodeTree) NewNode() *Node {
	if t.preallocUsed >= AllocNodeChunk {
		t.prealloc = make([]Node, AllocNodeChunk)
		t.preallocUsed = 0
	}
	n := &t.prealloc[t.preallocUsed]
	t.preallocUsed++
	return n
}

func (t *NodeTree) SetParent(key uint64, snapshot uint32, parent uint64) {
	n := t.data[key]
	if n != nil {
		s := n.SnapId[snapshot]
		if s != nil {
			s.Parent = parent
			return
		}
		n.SnapId[snapshot] = &iNode{Parent: parent}
		return
	}
	n = t.NewNode()
	snapId := make(map[uint32]*iNode)
	snapId[snapshot] = &iNode{Parent: parent}
	n.SnapId = snapId
	t.data[key] = n
}

func (t *NodeTree) SetName(key uint64, snapshot uint32, name []byte) {

	if key == RootInodeID {
		return
	}

	n := t.data[key]
	if n != nil {
		s := n.SnapId[snapshot]
		if s != nil {
			s.Name = name
			return
		}
		for ss := range n.SnapId {
			if len(n.SnapId[ss].Name) == 0 {
				n.SnapId[ss].Name = name
			}
		}
		return
	}
	fmt.Printf("call SetName(key=%d, snap=%d, name=%s): unknown key\n", key, snapshot, string(name))
	os.Exit(1)
}

func (t *NodeTree) SetParentName(key uint64, snapshot uint32, parent uint64, name []byte) {
	n := t.data[key]
	if n != nil {
		n.SnapId[snapshot] = &iNode{Parent: parent, Name: name}
		return
	}
	n = t.NewNode()
	snapId := make(map[uint32]*iNode)
	snapId[snapshot] = &iNode{Parent: parent, Name: name}
	n.SnapId = snapId
	t.data[key] = n
}

func (t *NodeTree) GetName(key uint64, snapshot uint32) []byte {
	n := t.data[key]
	if n != nil {
		return n.SnapId[snapshot].Name
	}
	return nil
}

func (t *NodeTree) GetParents(key uint64) map[uint32]*iNode {
	n := t.data[key]
	if n != nil {
		return n.SnapId
	}
	return map[uint32]*iNode{}
}

func getPathsReq(key uint64, snap uint32, tree *NodeTree) (uint64, []string) {

	if key == RootInodeID {
		return key, []string{}
	}
	if key == 0 {
		return key, []string{UnknownName}
	}

	ps := tree.GetParents(key)

	if ps[snap] != nil {

		path := []string{string(ps[snap].Name)}
		p, n := getPathsReq(ps[snap].Parent, snap, tree)
		path = append(n, path...)
		return p, path

	} else if ps[0] != nil {

		path := []string{string(ps[0].Name)}
		p, n := getPathsReq(ps[0].Parent, snap, tree)
		path = append(n, path...)
		return p, path

	} else {

		// find max snapid
		maxSnap := uint32(0)
		for s := range ps {
			if s > maxSnap {
				maxSnap = s
			}
		}
		if maxSnap > 0 {
			snap = maxSnap
			path := []string{string(ps[snap].Name)}
			p, n := getPathsReq(ps[snap].Parent, snap, tree)
			path = append(n, path...)
			return p, path

		}
		return 0, []string{UnknownName}

	}

	return 0, []string{UnknownName}
}

func getPaths(key uint64, name string, tree *NodeTree, isDir bool, snapCleanup *bool) []string {

	paths := []string{}
	ps := tree.GetParents(key)

	// snapCleanup mode
	if *snapCleanup && !isDir && len(ps) > 1 {
		_, ok := ps[0]
		if ok {
			pt := make(map[uint32]*iNode)
			pt[0] = ps[0]
			ps = pt
		} else {
			// find MAX snapId
			maxSnap := uint32(0)
			for s := range ps {
				if s > maxSnap {
					maxSnap = s
				}
			}
			pt := make(map[uint32]*iNode)
			pt[maxSnap] = ps[maxSnap]
			ps = pt
		}
	}

	for snap := range ps {

		// skip dirs in snapshot
		if isDir && snap != 0 && *snapCleanup {
			continue
		}

		if len(ps[snap].Name) > 0 {
			name = string(ps[snap].Name)
		}

		parent := ps[snap].Parent

		if len(name) == 0 {
			fmt.Printf("call getPaths(key=%d, snap=%d): empty name\n", key, snap)
			os.Exit(1)
		}

		_, path := getPathsReq(parent, snap, tree)
		path = append(path, name)
		rpath := fmt.Sprintf("/%s", strings.Join(path, "/"))
		paths = append(paths, rpath)
	}
	return paths
}
