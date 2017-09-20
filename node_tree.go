package main

import (
	"fmt"
	"strings"
)

const AllocNodeChunk = 100000

type Node struct {
	Parent uint64
	Name   []byte
}

type NodeTree struct {
	prealloc     []Node
	preallocUsed int
	data         map[uint64]*Node
	prevPathID   uint64
	prevPath     string
}

func NewNodeTree() *NodeTree {
	return &NodeTree{
		data:         make(map[uint64]*Node),
		prealloc:     make([]Node, AllocNodeChunk),
		preallocUsed: 0,
		prevPathID:   RootInodeID,
		prevPath:     "/",
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

func (t *NodeTree) SetParent(key uint64, parent uint64) {
	n := t.data[key]
	if n != nil {
		n.Parent = parent
		return
	}
	n = t.NewNode()
	n.Parent = parent
	t.data[key] = n
}

func (t *NodeTree) SetName(key uint64, name []byte) {
	n := t.data[key]
	if n != nil {
		n.Name = name
		return
	}
	n = t.NewNode()
	n.Name = name
	t.data[key] = n
}

func (t *NodeTree) SetParentName(key uint64, parent uint64, name []byte) {
	n := t.data[key]
	if n != nil {
		n.Parent = parent
		n.Name = name
		return
	}
	n = t.NewNode()
	n.Parent = parent
	n.Name = name
	t.data[key] = n
}

func (t *NodeTree) GetName(key uint64) []byte {
	n := t.data[key]
	if n != nil {
		return n.Name
	}
	return nil
}

func (t *NodeTree) GetParent(key uint64) uint64 {
	n := t.data[key]
	if n != nil {
		return n.Parent
	}
	return 0
}

func (t *NodeTree) GetParentName(key uint64) (uint64, []byte) {
	n := t.data[key]
	if n != nil {
		return n.Parent, n.Name
	}
	return 0, nil
}

func getParentName(key uint64, tree *NodeTree, snaptree *NodeTree) (uint64, string) {
	p, n := tree.GetParentName(key)
	ret := []string{}

	if len(n) > 0 {
		ret = append(ret, string(n))
	}

	if p == 0 {
		sp, sn := snaptree.GetParentName(key)
		if len(sn) > 0 {
			// prepend
			ret = append([]string{string(sn)}, ret...)
		}
		p = sp
	}

	if p == 0 {
		// prepend
		ret = append([]string{UnknownName}, ret...)
	}
	rname := strings.Join(ret, "/")

	return p, rname
}

func getPath(key uint64, tree *NodeTree, snaptree *NodeTree) string {
	if key == RootInodeID {
		return "/"
	}

	parent, _ := getParentName(key, tree, snaptree)
	if parent == tree.prevPathID {
		return tree.prevPath
	}

	ret := []string{}
	p := parent

	for p != 0 && p != RootInodeID {
		pp, nn := getParentName(p, tree, snaptree)
		if len(nn) > 0 {
			ret = append([]string{nn}, ret...)
		}
		p = pp
	}

	rname := strings.Join(ret, "/")

	if len(rname) > 0 {
		rname = fmt.Sprintf("%s/", rname)
	}

	if p == RootInodeID {
		rname = fmt.Sprintf("/%s", rname)
	} else {
		rname = fmt.Sprintf("%s%s", DetachedPrefix, rname)
	}

	tree.prevPathID = parent
	tree.prevPath = rname

	return rname
}
