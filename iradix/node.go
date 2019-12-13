package iradix

import (
	"bytes"
	"sort"
)

// WalkFn 用来迭代整颗数,给定key和value,返回true终止遍历
type WalkFn func(k []byte, v interface{}) bool

// 叶子节点用来存储值
type leafNode struct {
	key []byte
	val interface{}
}

type edge struct {
	label byte
	node  *Node
}

// Node 不可变的radix tree节点
type Node struct {
	next    *Node
	prev    *Node
	leaf    interface{}
	prefix  []byte
	edges   Edges
	version int
}

func (n *Node) isLeaf() bool {
	return n.leaf != nil
}

func (n *Node) addEdge(e edge, fn func(l byte, r byte) bool) {
	num := len(n.edges)
	idx := sort.Search(num, func(i int) bool {
		r := n.edges[i].label
		return r == e.label || fn(e.label, r)
	})
	n.edges = append(n.edges, e)
	if idx != num {
		copy(n.edges[idx+1:], n.edges[idx:num])
		n.edges[idx] = e
	}
}

func (n *Node) replaceEdge(e edge, fn func(l byte, r byte) bool) {
	num := len(n.edges)
	idx := sort.Search(num, func(i int) bool {
		r := n.edges[i].label
		return r == e.label || fn(e.label, r)
	})
	if idx < num && n.edges[idx].label == e.label {
		n.edges[idx].node = e.node
		return
	}
	panic("replacing missing edge")
}

func (n *Node) getEdge(label byte, fn func(l byte, r byte) bool) (int, *Node) {
	num := len(n.edges)
	idx := sort.Search(num, func(i int) bool {
		r := n.edges[i].label
		return r == label || fn(label, r)
	})
	if idx < num && n.edges[idx].label == label {
		return idx, n.edges[idx].node
	}
	return -1, nil
}

func (n *Node) delEdge(label byte, fn func(l byte, r byte) bool) {
	num := len(n.edges)
	idx := sort.Search(num, func(i int) bool {
		r := n.edges[i].label
		return r == label || fn(label, r)
	})
	if idx < num && n.edges[idx].label == label {
		copy(n.edges[idx:], n.edges[idx+1:])
		n.edges[len(n.edges)-1] = edge{}
		n.edges = n.edges[:len(n.edges)-1]
	}
}

// Get 搜索数据
func (n *Node) Get(k []byte, fn func(l byte, r byte) bool) (interface{}, bool) {
	search := k
	for {
		// 到末尾了,返回
		if len(search) == 0 {
			if n.isLeaf() {
				return n.leaf, true
			}
			break
		}

		_, n = n.getEdge(search[0], fn)
		if n == nil {
			break
		}

		// 去除前缀
		if bytes.HasPrefix(search, n.prefix) {
			search = search[len(n.prefix):]
		} else {
			break
		}
	}
	return nil, false
}

// Walk 遍历当前节点
func (n *Node) Walk(fn WalkFn) {
	recursiveWalk(n, fn)
}

// WalkPrefix 遍历当前节点指定前缀的数据
func (n *Node) WalkPrefix(prefix []byte, fn WalkFn, cmpFn func(l byte, r byte) bool) {
	search := prefix
	for {
		if len(search) == 0 {
			recursiveWalk(n, fn)
			return
		}

		_, n = n.getEdge(search[0], cmpFn)
		if n == nil {
			break
		}

		if bytes.HasPrefix(search, n.prefix) {
			search = search[len(n.prefix):]

		} else if bytes.HasPrefix(n.prefix, search) {
			recursiveWalk(n, fn)
			return
		} else {
			break
		}
	}
}

func recursiveWalk(n *Node, fn WalkFn) bool {
	if n.leaf != nil && fn([]byte{}, n.leaf) {
		return true
	}

	for _, e := range n.edges {
		if recursiveWalk(e.node, fn) {
			return true
		}
	}
	return false
}

// nodeList nodeList
type nodeList struct {
	root Node
	len  int
}

func (l *nodeList) Init() *nodeList {
	l.root.next = &l.root
	l.root.prev = &l.root
	l.len = 0
	return l
}

func (l *nodeList) Len() int { return l.len }

func (l *nodeList) Front() *Node {
	if l.len == 0 {
		return nil
	}
	return l.root.next
}

func (l *nodeList) Back() *Node {
	if l.len == 0 {
		return nil
	}
	return l.root.prev
}

func (l *nodeList) insert(e, at *Node) *Node {
	n := at.next
	at.next = e
	e.prev = at
	e.next = n
	n.prev = e
	l.len++
	return e
}

func (l *nodeList) Remove(e *Node) *Node {
	e.prev.next = e.next
	e.next.prev = e.prev
	e.next = nil
	e.prev = nil
	l.len--
	return e
}

func (l *nodeList) PushFront(e *Node) *Node {
	l.lazyInit()
	return l.insert(e, &l.root)
}

func (l *nodeList) PushBack(e *Node) *Node {
	l.lazyInit()
	return l.insert(e, l.root.prev)
}

func (l *nodeList) InsertBefore(v, mark *Node) *Node {
	return l.insert(v, mark.prev)
}

func (l *nodeList) InsertAfter(v, mark *Node) *Node {
	return l.insert(v, mark)
}

func (l *nodeList) MoveToFront(e *Node) {
	if l.root.next == e {
		return
	}
	l.insert(l.Remove(e), &l.root)
}

func (l *nodeList) MoveToBack(e *Node) {
	if l.root.prev == e {
		return
	}
	l.insert(l.Remove(e), l.root.prev)
}

func (l *nodeList) MoveBefore(e, mark *Node) {
	if e == mark {
		return
	}
	l.insert(l.Remove(e), mark.prev)
}

func (l *nodeList) MoveAfter(e, mark *Node) {
	if e == mark {
		return
	}
	l.insert(l.Remove(e), mark)
}

func (l *nodeList) PushBackList(other *nodeList) {
	l.lazyInit()
	if other.Len() > 0 {
		otherHead := other.Front()
		otherTail := other.Back()
		tail := l.Back()
		if tail == nil {
			tail = &l.root
		}
		tail.next = otherHead
		otherHead.prev = tail
		otherTail.next = &l.root
		l.root.prev = otherTail
		l.len += other.len
		other.Init()
	}
}

func (l *nodeList) lazyInit() {
	if l.root.next == nil {
		l.Init()
	}
}
