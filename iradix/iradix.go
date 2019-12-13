package iradix

// Tree 不可变iradix树
type Tree struct {
	root        *Node
	size        int
	isSortGreat bool
}

func less(l byte, r byte) bool {
	return l < r
}

func great(l byte, r byte) bool {
	return l > r
}

// New 新建空树
func New() *Tree {
	return &Tree{root: &Node{}, isSortGreat: false}
}

// Len 返回输的长度
func (t *Tree) Len() int {
	return t.size
}

// RawIterator 返回迭代器
func (t *Tree) RawIterator() *RawIterator {
	return &RawIterator{cmpFn: t.SortFn()}
}

// RawIterator 返回迭代器
func (t *Tree) InitRawIterator(iter *RawIterator) *RawIterator {
	iter.cmpFn = t.SortFn()
	return iter
}

// SortGreat 设置从大到小
func (t *Tree) SortGreat() {
	t.isSortGreat = true
}

// SortLess 设置从小到大
func (t *Tree) SortLess() {
	t.isSortGreat = false
}

// SortFn 设置从大到小
func (t *Tree) SortFn() func(l byte, r byte) bool {
	if t.isSortGreat {
		return great
	}
	return less
}

// IsSortGreat 是否由大到小排序
func (t *Tree) IsSortGreat() bool {
	return t.isSortGreat
}

// Root 返回根节点
func (t *Tree) Root() *Node {
	return t.root
}

// Get 查找
func (t *Tree) Get(k []byte) (interface{}, bool) {
	return t.root.Get(k, t.SortFn())
}

// longestPrefix 匹配最长前缀
func longestPrefix(k1, k2 []byte) int {
	max := len(k1)
	if l := len(k2); l < max {
		max = l
	}
	var i int
	for i = 0; i < max; i++ {
		if k1[i] != k2[i] {
			break
		}
	}
	return i
}

func concat(a, b []byte) []byte {
	var c []byte
	l := len(a)
	n := len(b)
	if l+n <= cap(a) {
		c = a[:l+n]
	} else {
		c = make([]byte, l+n)
		copy(c, a)
	}
	copy(c[l:], b)
	return c
}
