package main

import (
	"fmt"
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

func (t *NodeTree) GetPath(key uint64) string {
	if key == RootInodeID {
		return "/"
	}

	parent := t.GetParent(key)
	if parent == t.prevPathID {
		return t.prevPath
	}

	p := parent
	ret := ""

	for p != 0 && p != RootInodeID {
		pp, n := t.GetParentName(p)
		if len(n) == 0 {
			n = UnknownName
		}
		ret = fmt.Sprintf("%s/%s", string(n), ret)
		p = pp
	}

	if p == RootInodeID {
		ret = fmt.Sprintf("/%s", ret)
	} else {
		ret = fmt.Sprintf("%s%s", DetachedPrefix, ret)
	}

	t.prevPathID = parent
	t.prevPath = ret

	return ret
}
