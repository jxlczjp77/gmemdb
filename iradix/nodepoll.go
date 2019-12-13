package iradix

type nodePoll struct {
	version    int
	preVersion int
	freeList   *nodeList
	txOldNodes nodeList
	txNewNodes nodeList
	txTmpNodes nodeList
}

func (s *nodePoll) initNodePoll(freeList *nodeList) {
	s.version = 0
	s.preVersion = 0
	s.freeList = freeList
	s.txOldNodes.Init()
	s.txNewNodes.Init()
	s.txTmpNodes.Init()

}
func (s *nodePoll) setVersion(preVersion int, version int) {
	s.version = version
	s.preVersion = preVersion
}

func (s *nodePoll) commit() {
	s.fixNodeList(&s.txNewNodes)
	s.freeNodeList(&s.txTmpNodes)
	s.freeNodeList(&s.txOldNodes)
}

func (s *nodePoll) rollback() {
	s.freeNodeList(&s.txNewNodes)
	s.freeNodeList(&s.txTmpNodes)
	s.fixNodeList(&s.txOldNodes)
}

func (s *nodePoll) freeNodeList(l *nodeList) {
	for l.Len() > 0 {
		node := l.Front()
		l.Remove(node)
		s.freeNode(node)
	}
}

func (s *nodePoll) fixNodeList(l *nodeList) {
	node := l.Front()
	n := l.Len()
	for i := 0; i < n; i++ {
		l := len(node.edges)
		if l == 0 {
			node.edges = nil
		} else {
			c := cap(node.edges)
			pp := float32(c-l) / float32(c)
			if pp > 0.5 {
				new_edges := make(Edges, l, l)
				copy(new_edges, node.edges)
				node.edges = new_edges
			}
		}
		node = node.next
	}
	l.Init()
}

func (s *nodePoll) freeNode(n *Node) {
	n.leaf = nil
	n.edges = n.edges[:0]
	n.prefix = n.prefix[:0]
	n.version = 0
	s.freeList.PushBack(n)
}

func (s *nodePoll) removeNode(n *Node, lockDB int) {
	if lockDB == 0 && n.version == s.version {
		return
	}
	if n.version == s.version {
		s.txTmpNodes.PushBack(s.txNewNodes.Remove(n))
	} else if n.version <= s.preVersion {
		s.txOldNodes.PushBack(n)
	}
}

func (s *nodePoll) newWriteableNode(n *Node, lockDB int) *Node {
	if lockDB == 0 && n.version == s.version {
		return n
	}

	nc := s.newNode(n.leaf, n.prefix, n.edges)
	if n.version == s.version {
		s.txTmpNodes.PushBack(s.txNewNodes.Remove(n))
	} else if n.version <= s.preVersion {
		s.txOldNodes.PushBack(n)
	}
	return nc
}

func (s *nodePoll) newNode(leaf interface{}, prefix []byte, edges Edges) *Node {
	var nc *Node
	if s.freeList.Len() > 0 {
		nc = s.freeList.Front()
		s.freeList.Remove(nc)
	} else {
		nc = &Node{}
	}
	nc.leaf = leaf
	if prefix != nil {
		if nc.prefix == nil {
			nc.prefix = make([]byte, len(prefix), len(prefix))
			copy(nc.prefix, prefix)
		} else {
			nc.prefix = append(nc.prefix, prefix...)
		}
	}
	if len(edges) != 0 {
		if nc.edges == nil {
			nc.edges = make(Edges, len(edges), len(edges))
			copy(nc.edges, edges)
		} else {
			nc.edges = append(nc.edges, edges...)
		}
	}
	nc.version = s.version
	s.txNewNodes.PushBack(nc)
	return nc
}
