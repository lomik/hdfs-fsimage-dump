package main

import (
	"fmt"
	"os"
	"strings"
)

const AllocNodeChunk = 100000

type Node struct {
	Parent uint64
	Name   []byte
	SnapId uint32
}

type NodeTree struct {
	prealloc     [][]Node
	preallocUsed int
	data         map[uint64]*[]Node
}

func NewNodeTree() *NodeTree {
	return &NodeTree{
		data:         make(map[uint64]*[]Node),
		prealloc:     make([][]Node, AllocNodeChunk),
		preallocUsed: 0,
	}
}

func (t *NodeTree) NewNode() *[]Node {
	if t.preallocUsed >= AllocNodeChunk {
		t.prealloc = make([][]Node, AllocNodeChunk)
		t.preallocUsed = 0
	}
	n := &t.prealloc[t.preallocUsed]
	t.preallocUsed++
	return n
}

func (t *NodeTree) SetParent(key uint64, snapshot uint32, parent uint64) {
	n := t.data[key]
	if n != nil {
		nodes := *n
		for i := range nodes {
			if nodes[i].SnapId == snapshot {
				nodes[i].Parent = parent
				return
			}
		}
		*n = append(*n, Node{Parent: parent, SnapId: snapshot})
		return
	}
	n = t.NewNode()
	*n = append(*n, Node{Parent: parent, SnapId: snapshot})
	t.data[key] = n
}

func (t *NodeTree) SetName(key uint64, snapshot uint32, name []byte) {

	if key == RootInodeID {
		return
	}
	n := t.data[key]
	if n != nil {
		nodes := *n
		for i := range nodes {
			if nodes[i].SnapId == snapshot {
				nodes[i].Name = name
				return
			}
		}
		for i := range nodes {
			if len(nodes[i].Name) == 0 {
				nodes[i].Name = name
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
		nodes := *n
		for i := range nodes {
			if nodes[i].SnapId == snapshot {
				nodes[i].Name = name
				nodes[i].Parent = parent
				return
			}
		}
		*n = append(*n, Node{Parent: parent, SnapId: snapshot, Name: name})
		return
	}
	n = t.NewNode()
	*n = append(*n, Node{Parent: parent, SnapId: snapshot, Name: name})
	t.data[key] = n
}

func (t *NodeTree) GetName(key uint64, snapshot uint32) []byte {
	n := t.data[key]
	if n != nil {
		for _, node := range *n {
			if node.SnapId == snapshot {
				return node.Name
			}
		}
	}
	return nil
}

func (t *NodeTree) GetParents(key uint64) []Node {
	n := t.data[key]
	if n != nil {
		return *n
	}
	return []Node{}
}

func getPathsReq(key uint64, snap uint32, tree *NodeTree) (uint64, []string) {

	if key == RootInodeID {
		return key, []string{}
	}
	if key == 0 {
		return key, []string{UnknownName}
	}

	ps := tree.GetParents(key)

	for _, node := range ps {
		if node.SnapId == snap {
			path := []string{string(node.Name)}
			p, n := getPathsReq(node.Parent, snap, tree)
			path = append(n, path...)
			return p, path
		}
	}

	for _, node := range ps {
		if node.SnapId == 0 {
			path := []string{string(node.Name)}
			p, n := getPathsReq(node.Parent, snap, tree)
			path = append(n, path...)
			return p, path
		}
	}

	// find max snapid
	maxSnap := uint32(0)
	for _, node := range ps {
		if node.SnapId > maxSnap {
			maxSnap = node.SnapId
		}
	}
	if maxSnap > 0 {
		for _, node := range ps {
			if node.SnapId == maxSnap {
				path := []string{string(node.Name)}
				p, n := getPathsReq(node.Parent, maxSnap, tree)
				path = append(n, path...)
				return p, path

			}
		}
	}

	return 0, []string{UnknownName}
}

func getPaths(key uint64, name string, tree *NodeTree, isDir bool, snapCleanup *bool) []string {

	paths := []string{}
	ps := tree.GetParents(key)

	// snapCleanup mode
	if *snapCleanup && !isDir && len(ps) > 1 {
		maxSnap := uint32(0)
		for _, node := range ps {
			if node.SnapId == 0 {
				ps = []Node{node}
				maxSnap = 0
				break
			}
			if node.SnapId > maxSnap {
				maxSnap = node.SnapId
			}
		}
		if maxSnap > 0 {
			for _, node := range ps {
				if node.SnapId == maxSnap {
					ps = []Node{node}
					break
				}
			}
		}
	}

	for _, node := range ps {
		// skip dirs in snapshot
		if isDir && node.SnapId != 0 && *snapCleanup {
			continue
		}

		if len(node.Name) > 0 {
			name = string(node.Name)
		}

		parent := node.Parent

		if len(name) == 0 {
			fmt.Printf("call getPaths(key=%d, snap=%d): empty name\n", key, node.SnapId)
			os.Exit(1)
		}

		_, path := getPathsReq(parent, node.SnapId, tree)
		path = append(path, name)
		rpath := fmt.Sprintf("/%s", strings.Join(path, "/"))
		paths = append(paths, rpath)
	}
	return paths
}
