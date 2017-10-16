package main

const AllocNodeRefChunk = 50000

type inodeRefStruct struct {
	RefId  uint64
	SnapId uint32
	Name   []byte
}

type NodeRefTree struct {
	prealloc     []inodeRefStruct
	preallocUsed int
	data         map[uint32]*inodeRefStruct
}

func NewNodeRefTree() *NodeRefTree {
	return &NodeRefTree{
		data:         make(map[uint32]*inodeRefStruct),
		prealloc:     make([]inodeRefStruct, AllocNodeRefChunk),
		preallocUsed: 0,
	}
}

func (t *NodeRefTree) NewRefNode() *inodeRefStruct {
	if t.preallocUsed >= AllocNodeRefChunk {
		t.prealloc = make([]inodeRefStruct, AllocNodeRefChunk)
		t.preallocUsed = 0
	}
	n := &t.prealloc[t.preallocUsed]
	t.preallocUsed++
	return n
}

func (t *NodeRefTree) SetRefSnapName(key uint32, snapId uint32, refId uint64, name []byte) {
	n := t.data[key]
	if n != nil {
		n.SnapId = snapId
		n.RefId = refId
		n.Name = name
		return
	}
	n = t.NewRefNode()
	n.SnapId = snapId
	n.RefId = refId
	n.Name = name
	t.data[key] = n
}

func (t *NodeRefTree) GetRefId(key uint32) uint64 {
	n := t.data[key]
	if n != nil {
		return n.RefId
	}
	return 0
}

func (t *NodeRefTree) GetRefSnapId(key uint32) uint32 {
	n := t.data[key]
	if n != nil {
		return n.SnapId
	}
	return 0
}

func (t *NodeRefTree) GetRefName(key uint32) []byte {
	n := t.data[key]
	if n != nil {
		return n.Name
	}
	return nil
}
