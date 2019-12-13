package gmemdb

import (
	"encoding/binary"
	"math"
	"github.com/jxlczjp77/gmemdb/iradix"
)

// Iterator 迭代器
type Iterator interface {
	RawNext() bool
	Next() bool
	Value() IObject

	Step() IObject
	RawStep() IObject

	LockDB()
	UnLockDB()
}

type radixIterator struct {
	txn           *iradix.Txn
	iter          iradix.RawIterator
	prefixLen     int
	isCompoundKey bool
	isUnique      bool
	fieldCount    int
	keyFieldCount int
	atEnd         bool
	value         IObject
	isSortGreat   bool
}

func (r *radixIterator) LockDB() {
	r.txn.LockDB()
}

func (r *radixIterator) UnLockDB() {
	r.txn.UnLockDB()
}

func (r *radixIterator) RawNext() bool {
	_, value, ok := r.iter.RawNext()
	if ok {
		r.value = value.(IObject)
		return true
	}
	return false
}

func (r *radixIterator) Next() bool {
	if r.atEnd {
		return false
	}
	r.value = r.doNext()
	if r.value == nil {
		r.atEnd = true
		return false
	}
	return true
}

func (r *radixIterator) Value() IObject {
	return r.value
}

func (r *radixIterator) Step() IObject {
	if r.Next() {
		return r.Value()
	}
	return nil
}

func (r *radixIterator) RawStep() IObject {
	if r.RawNext() {
		return r.Value()
	}
	return nil
}

func (r *radixIterator) doNext() IObject {
	key, value, ok := r.iter.Next()
	if ok {
		obj := value.(IObject)
		n := len(key)
		if n == r.prefixLen || r.prefixLen == 0 {
			return obj
		} else if n > r.prefixLen {
			prefixLen := r.prefixLen
			if !r.isUnique {
				if n-r.prefixLen < 4 {
					return nil
				}
				if !r.isCompoundKey && r.prefixLen+4 != n {
					// 非组合key必须精确匹配长度
					return nil
				}
				id := binary.BigEndian.Uint32(key[n-4:])
				if r.isSortGreat {
					// 索引从大到小排列时,id也要倒序一下,不然后插入的记录会排在先插入记录的前面
					id = math.MaxUint32 - id
				}
				if obj.GetID() != id {
					return nil
				}
				if !r.isCompoundKey {
					return obj
				}
				n = n - 4
				key = key[0:n]
			}
			if r.isCompoundKey {
				fieldCount := 0
				i := prefixLen
				for i < n {
					subKeyLen := uint8(key[i])
					i += int(subKeyLen) + 1
					fieldCount++
				}
				if i == n && fieldCount+r.keyFieldCount == r.fieldCount {
					return obj
				}
			}
		}
	}
	return nil
}
