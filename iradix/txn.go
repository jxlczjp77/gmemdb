package iradix

import (
	"bytes"
)

// Txn 事物对象
type Txn struct {
	Tree
	version      int
	lockDB       int
	currentSP    *TxnSavePoint
	freeList     nodeList
	defSavePoint TxnSavePoint
	savePoints   []TxnSavePoint
}

type TxnSavePoint struct {
	nodePoll
	root *Node
	size int
}

func NewTxn() *Txn {
	txn := &Txn{
		Tree: Tree{
			root:        &Node{},
			size:        0,
			isSortGreat: false,
		},
		version:   0,
		currentSP: nil,
		lockDB:    0,
	}
	txn.freeList.Init()
	return txn
}

func (t *Txn) Clear() {
	t.root = &Node{}
	t.size = 0
}

func (t *Txn) FreeListLen() int {
	return t.freeList.Len()
}

// writeNode 返回可被修改的节点
func (t *Txn) writeNode(n *Node) *Node {
	if t.currentSP == nil {
		t.AllocSavePoint()
	}
	return t.currentSP.newWriteableNode(n, t.lockDB)
}

// newNode 返回可被修改的节点
func (t *Txn) newNode(leaf interface{}, prefix []byte, edges Edges) *Node {
	if t.currentSP == nil {
		t.AllocSavePoint()
	}
	return t.currentSP.newNode(leaf, prefix, edges)
}

// 当节点n仅剩下一条边时调用该函数合并
func (t *Txn) mergeChild(n *Node) {
	e := n.edges[0]
	child := e.node

	n.prefix = concat(n.prefix, child.prefix)
	n.leaf = child.leaf
	if len(child.edges) != 0 {
		n.edges = make(Edges, len(child.edges))
		copy(n.edges, child.edges)
	} else {
		n.edges = nil
	}

	// TODO: 收集合并掉的节点影响删除性能,以后再测试
	// t.currentSP.removeNode(child, t.lockDB)
}

func (t *Txn) insert(n *Node, k, search []byte, v interface{}) (*Node, interface{}, bool) {
	if len(search) == 0 {
		var oldVal interface{}
		didUpdate := false
		if n.isLeaf() {
			oldVal = n.leaf
			didUpdate = true
		}

		nc := t.writeNode(n)
		nc.leaf = v
		return nc, oldVal, didUpdate
	}

	cmpFn := t.SortFn()
	idx, child := n.getEdge(search[0], cmpFn)

	// 没有边,创建它
	if child == nil {
		e := edge{
			label: search[0],
			node:  t.newNode(v, search, nil),
		}
		nc := t.writeNode(n)
		nc.addEdge(e, cmpFn)
		return nc, nil, false
	}

	// 匹配最长前缀
	commonPrefix := longestPrefix(search, child.prefix)
	if commonPrefix == len(child.prefix) {
		// 当前结点完全匹配
		search = search[commonPrefix:]
		newChild, oldVal, didUpdate := t.insert(child, k, search, v)
		if newChild != nil {
			nc := t.writeNode(n)
			nc.edges[idx].node = newChild
			return nc, oldVal, didUpdate
		}
		return nil, oldVal, didUpdate
	}

	// 分裂当前节点
	nc := t.writeNode(n)
	splitNode := t.newNode(nil, search[:commonPrefix], nil)
	nc.replaceEdge(edge{
		label: search[0],
		node:  splitNode,
	}, cmpFn)

	// 将子节点加入分裂后的节点
	modChild := t.writeNode(child)
	splitNode.addEdge(edge{
		label: modChild.prefix[commonPrefix],
		node:  modChild,
	}, cmpFn)
	modChild.prefix = modChild.prefix[commonPrefix:]

	// 创建新的叶子节点
	leaf := v

	// 如果刚分裂的节点匹配,直接放进去
	search = search[commonPrefix:]
	if len(search) == 0 {
		splitNode.leaf = leaf
		return nc, nil, false
	}

	// 否则新增加一条边放进去
	splitNode.addEdge(edge{
		label: search[0],
		node:  t.newNode(leaf, search, nil),
	}, cmpFn)
	return nc, nil, false
}

func (t *Txn) delete(parent, n *Node, search []byte) (*Node, interface{}) {
	if len(search) == 0 {
		if !n.isLeaf() {
			return nil, nil
		}

		leaf := n.leaf
		nc := t.writeNode(n)
		nc.leaf = nil

		// 检查节点是否可以合并
		if n != t.root && len(nc.edges) == 1 {
			t.mergeChild(nc)
		}
		return nc, leaf
	}

	cmpFn := t.SortFn()
	label := search[0]
	idx, child := n.getEdge(label, cmpFn)
	if child == nil || !bytes.HasPrefix(search, child.prefix) {
		return nil, nil
	}

	search = search[len(child.prefix):]
	newChild, leaf := t.delete(n, child, search)
	if newChild == nil {
		return nil, nil
	}

	nc := t.writeNode(n)
	if newChild.leaf == nil && len(newChild.edges) == 0 {
		nc.delEdge(label, cmpFn)
		if n != t.root && len(nc.edges) == 1 && !nc.isLeaf() {
			t.mergeChild(nc)
		}
	} else {
		nc.edges[idx].node = newChild
	}
	return nc, leaf
}

// Insert 增加或更新数据
func (t *Txn) Insert(k []byte, v interface{}) (interface{}, bool) {
	newRoot, oldVal, didUpdate := t.insert(t.root, k, k, v)
	if newRoot != nil {
		t.root = newRoot
	}
	if !didUpdate {
		t.size++
	}
	return oldVal, didUpdate
}

// Delete 删除节点
func (t *Txn) Delete(k []byte) (interface{}, bool) {
	newRoot, leaf := t.delete(nil, t.root, k)
	if newRoot != nil {
		t.root = newRoot
	}
	if leaf != nil {
		t.size--
		return leaf, true
	}
	return nil, false
}

// Root 返回当前根节点
func (t *Txn) Root() *Tree {
	return &t.Tree
}

// Get 查找
func (t *Txn) Get(k []byte) (interface{}, bool) {
	return t.root.Get(k, t.SortFn())
}

// Commit 提交事物
func (t *Txn) Commit() *Tree {
	if t.currentSP != nil {
		t.version = t.currentSP.version
		for i := len(t.savePoints) - 1; i >= 0; i-- {
			s := &t.savePoints[i]
			s.commit()
		}
		t.defSavePoint.commit()
		t.savePoints = t.savePoints[:0]
		t.currentSP = nil
	}
	t.lockDB = 0
	return t.Root()
}

func (t *Txn) Dirty() bool {
	return t.currentSP != nil
}

func (t *Txn) LockDB() {
	t.lockDB++
}

func (t *Txn) UnLockDB() {
	if t.lockDB > 0 {
		t.lockDB--
	}
}

// LastSavePointID -1表示没有保存点,0表示第一个保存点的索引,...
func (t *Txn) LastSavePointID() int {
	return len(t.savePoints) - 1
}

// AllocSavePoint 分配事物回滚点
func (t *Txn) AllocSavePoint() {
	if t.currentSP == nil {
		sp := &t.defSavePoint
		sp.root = t.root
		sp.size = t.size
		sp.initNodePoll(&t.freeList)
		sp.setVersion(t.version, t.version+1)
		t.currentSP = sp
		t.lockDB = 0
	} else {
		n := len(t.savePoints)
		t.savePoints = append(t.savePoints, TxnSavePoint{root: t.root, size: t.size})
		sp := &t.savePoints[n]
		sp.initNodePoll(&t.freeList)
		sp.setVersion(t.version, t.version+n+2)
		t.currentSP = sp
	}
}

// Rollback 回滚事物
func (t *Txn) Rollback() {
	t.RollbackTo(-1)
}

// RollbackTo 回滚事物保存点
func (t *Txn) RollbackTo(savepointID int) {
	if t.currentSP == nil {
		return
	}
	t.lockDB = 0
	var s *TxnSavePoint
	i := len(t.savePoints)
	for i--; i >= 0; i-- {
		s = &t.savePoints[i]
		s.rollback()
		if i == savepointID {
			break
		}
	}
	if i == -1 {
		t.root = t.defSavePoint.root
		t.size = t.defSavePoint.size
		t.defSavePoint.rollback()
		t.currentSP = nil
		t.savePoints = t.savePoints[:0]
		return
	}
	t.root = s.root
	t.size = s.size
	if i == 0 {
		t.currentSP = &t.defSavePoint
		t.savePoints = t.savePoints[:0]
	} else {
		t.currentSP = &t.savePoints[i-1]
		t.savePoints = t.savePoints[:i]
	}
}
