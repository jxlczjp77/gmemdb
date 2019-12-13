package iradix

import (
	"bytes"
	"sort"
)

type tStack struct {
	nodes []*Node
	pos   int
}

func (s *tStack) addNode(n *Node) *tStack {
	s.nodes = append(s.nodes, n)
	return s
}

func (s *tStack) next(key *ByteBuffer) *Node {
	pos := s.pos + 1
	if pos < len(s.nodes) {
		if pos == 0 {
			key.Write(s.nodes[0].prefix)
		} else {
			preNode := s.nodes[s.pos]
			nowNode := s.nodes[pos]
			key.Truncate(key.Len() - len(preNode.prefix))
			key.Write(nowNode.prefix)
		}
		s.pos = pos
		return s.nodes[pos]
	}
	return nil
}

// RawIterator 迭代Node节点
type RawIterator struct {
	stack   []tStack
	limitLv int
	key     ByteBuffer
	cmpFn   func(l byte, r byte) bool
}

func (s *RawIterator) init() {
	s.stack = s.stack[:0]
	s.limitLv = -1
	s.key.Reset()
}
func (s *RawIterator) newStack() *tStack {
	s.stack = append(s.stack, tStack{pos: -1})
	return &s.stack[len(s.stack)-1]
}

func (s *RawIterator) popStack() {
	n := len(s.stack) - 1
	stack := s.stack[n]
	s.key.Truncate(s.key.Len() - len(stack.nodes[stack.pos].prefix))
	s.stack = s.stack[:n]
}

// SeekPrefix 设置迭代前缀
func (s *RawIterator) SeekPrefix(node *Node, prefix []byte) bool {
	search := prefix
	s.init()
	for {
		if len(search) == 0 {
			s.newStack().addNode(node)
			s.limitLv = len(s.stack)
			return true
		}

		_, nextNode := node.getEdge(search[0], s.cmpFn)
		if nextNode == nil {
			return false
		}

		if bytes.HasPrefix(search, nextNode.prefix) {
			s.newStack().addNode(node).next(&s.key)
			search = search[len(nextNode.prefix):]
			if len(search) == 0 {
				s.newStack().addNode(nextNode)
				s.limitLv = len(s.stack)
				return true
			}
			node = nextNode
		} else if bytes.HasPrefix(nextNode.prefix, search) {
			s.newStack().addNode(node).next(&s.key)
			s.newStack().addNode(nextNode)
			s.limitLv = len(s.stack)
			return true
		} else {
			return false
		}
	}
}

// RawNext 移动到下一个节点
func (s *RawIterator) RawNext() ([]byte, interface{}, bool) {
	return s.doNext(true)
}

// Next 移动到下一个节点
func (s *RawIterator) Next() ([]byte, interface{}, bool) {
	return s.doNext(false)
}

func (s *RawIterator) doNext(raw bool) ([]byte, interface{}, bool) {
	minLv := s.limitLv
	if raw {
		minLv = -1
	}
	for n := len(s.stack); n > 0 && n >= minLv; n = len(s.stack) {
		stack := &s.stack[n-1]
		elem := stack.next(&s.key)
		if elem == nil {
			if n <= s.limitLv && raw && n > 1 {
				if !s.expendStack(stack, &s.stack[n-2]) {
					s.popStack()
				}
			} else {
				s.popStack()
			}
			continue
		}

		if len(elem.edges) > 0 {
			ns := s.newStack()
			for _, e := range elem.edges {
				ns.addNode(e.node)
			}
		}
		if elem.leaf != nil {
			return s.key.Bytes(), elem.leaf, true
		}
	}
	return nil, nil, false
}

func (s *RawIterator) expendStack(stack, preStack *tStack) bool {
	last := stack.nodes[stack.pos]
	pre := preStack.nodes[preStack.pos]
	num := len(pre.edges)
	if num > 1 {
		label := last.prefix[0]
		idx := sort.Search(num, func(n int) bool {
			return s.cmpFn(label, pre.edges[n].label)
		})
		if idx < len(pre.edges) {
			for i := idx; i < len(pre.edges); i++ {
				stack.addNode(pre.edges[i].node)
			}
			return true
		}
	}
	return false
}
