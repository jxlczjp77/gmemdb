package gmemdb

import (
	"bytes"
	"fmt"
	"math"
	"reflect"
	"strings"

	"github.com/jxlczjp77/gmemdb/iradix"
)

// IFactory 带索引的表
type IFactory interface {
	getType() reflect.Type
	name() string
}

// MakeKeyFunc 给定对象返回key
type MakeKeyFunc func(*MdbKey, IObject) error

// MemIndex 内存索引
type MemIndex struct {
	name       []byte
	fieldNames []string
	fields     []reflect.StructField
	idxNum     int
	root       *iradix.Tree
	txn        *iradix.Txn
	makeKey    MakeKeyFunc
	lastError  error
	mdbKey     MdbKey
	mdbKey1    MdbKey
}

// NewMemIndex 新建唯一索引
func NewMemIndex(name string, idxNum int, makeKey MakeKeyFunc, unique bool, table IFactory) *MemIndex {
	txn := iradix.NewTxn()
	root := txn.Root()
	fieldNames := strings.Split(name, "|")
	fields := make([]reflect.StructField, 0, len(fieldNames))
	notMatchFields := []string{}
	Type := table.getType()
	for _, name := range fieldNames {
		if field, ok := Type.FieldByName(name); !ok {
			notMatchFields = append(notMatchFields, name)
		} else {
			fields = append(fields, field)
		}
	}
	if len(notMatchFields) > 0 {
		formatndPanic("表[%s]添加索引[%s]失败: 列[%s]不匹配", table.name(), name, strings.Join(notMatchFields, ","))
	}
	idx := &MemIndex{
		name:       []byte(name),
		fieldNames: fieldNames,
		fields:     fields,
		root:       root,
		txn:        txn,
		makeKey:    makeKey,
		idxNum:     idxNum,
	}
	keyCount := len(fields)
	idx.mdbKey.Init(keyCount, unique)
	idx.mdbKey1.Init(keyCount, unique)
	return idx
}

// Name Name
func (s *MemIndex) Name() string {
	return string(s.name)
}

// FieldNames FieldNames
func (s *MemIndex) FieldNames() []string {
	return s.fieldNames
}

// Find 查找对象
func (s *MemIndex) Find(val IObject) Iterator {
	err := s.makeKeyWithUnique(&s.mdbKey, val)
	if err != nil {
		formatndPanic(err.Error())
	}
	return s.findByKey(&s.mdbKey, false)
}

// SortGreat SortGreat
func (s *MemIndex) SortGreat() {
	s.root.SortGreat()
}

// SortLess SortLess
func (s *MemIndex) SortLess() {
	s.root.SortLess()
}

// DefaultKey DefaultKey
func (s *MemIndex) DefaultKey() *MdbKey {
	return &s.mdbKey
}

// FreeListLen FreeListLen
func (s *MemIndex) FreeListLen() int {
	return s.txn.FreeListLen()
}

// FindByPB 根据proto对象查找
func (s *MemIndex) FindByPB(pb interface{}) (Iterator, error) {
	pbType := reflect.TypeOf(pb).Elem()
	// 内存表第一个字段都是PrimaryID,而pb中没有这个字段
	for _, field := range s.fields {
		pbField := pbType.Field(field.Index[0] - 1)
		if field.Name != pbField.Name {
			return nil, fmt.Errorf("索引字段名不匹配[%s]", field.Name)
		}
	}
	pbValue := reflect.ValueOf(pb).Elem()
	s.mdbKey.Reset()
	for i, field := range s.fields {
		pbVal := pbValue.Field(field.Index[0] - 1)
		if pbVal.Kind() == reflect.Ptr {
			if pbVal.IsNil() {
				if i == 0 {
					// 第一个key必须不为0
					return nil, fmt.Errorf("第一个索引必须不为nil")
				}
				break
			} else {
				s.mdbKey.AppendValue(pbVal.Elem().Interface())
			}
		} else {
			s.mdbKey.AppendValue(pbVal.Interface())
		}
	}
	return s.FindByKey(&s.mdbKey), nil
}

// FindByKey 指定key查找对象
func (s *MemIndex) FindByKey(key *MdbKey) Iterator {
	return s.findByKey(key, true)
}

// Add 添加对象
func (s *MemIndex) Add(val IObject) error {
	err := s.makeKeyWithUnique(&s.mdbKey, val)
	if err != nil {
		return err
	}
	key := s.mdbKey.Key()
	_, didUpdate := s.txn.Insert(key, val)
	if didUpdate {
		return fmt.Errorf("索引冲突 %s", string(key))
	}
	s.root = s.txn.Root()
	return nil
}

// Update 更新对象
func (s *MemIndex) Update(oldVal IObject, newVal IObject) error {
	err1 := s.makeKeyWithUnique(&s.mdbKey, oldVal)
	if err1 != nil {
		return err1
	}
	err2 := s.makeKeyWithUnique(&s.mdbKey1, newVal)
	if err2 != nil {
		return err2
	}
	oldKey := s.mdbKey.Key()
	newKey := s.mdbKey1.Key()
	if !bytes.Equal(oldKey, newKey) {
		_, ok := s.txn.Delete(oldKey)
		if !ok {
			return fmt.Errorf("源索引不存在 %s", string(oldKey))
		}
		_, didUpdate := s.txn.Insert(newKey, newVal)
		if didUpdate {
			return fmt.Errorf("新索引冲突 %s", string(newKey))
		}
	} else {
		_, didUpdate := s.txn.Insert(oldKey, newVal)
		if !didUpdate {
			return fmt.Errorf("源索引不存在 %s", string(oldKey))
		}
	}
	s.root = s.txn.Root()
	return nil
}

// Delete 删除对象
func (s *MemIndex) Delete(val IObject) error {
	err := s.makeKeyWithUnique(&s.mdbKey, val)
	if err != nil {
		return err
	}
	key := s.mdbKey.Key()
	s.txn.Delete(key)
	s.root = s.txn.Root()
	return nil
}

// Clear 清空索引内容
func (s *MemIndex) Clear() {
	g := s.root.IsSortGreat()
	s.txn.Clear()
	s.root = s.txn.Root()
	if g == true {
		s.root.SortGreat()
	}
}

// Begin 返回第一个位置
func (s *MemIndex) Begin() Iterator {
	r := &radixIterator{
		txn:           s.txn,
		isCompoundKey: s.mdbKey.IsCompoundKey(),
		isUnique:      s.mdbKey.IsUnique(),
		prefixLen:     0,
		atEnd:         false,
		isSortGreat:   s.root.IsSortGreat(),
	}
	iter := s.root.InitRawIterator(&r.iter)
	if !iter.SeekPrefix(s.root.Root(), nil) {
		r.atEnd = true
	}
	return r
}

func (s *MemIndex) findByKey(key *MdbKey, skipNil bool) Iterator {
	keyLen := key.Len()
	r := &radixIterator{
		txn:           s.txn,
		isCompoundKey: key.IsCompoundKey(),
		isUnique:      key.IsUnique(),
		prefixLen:     keyLen,
		atEnd:         false,
		isSortGreat:   s.root.IsSortGreat(),
		fieldCount:    key.KeyCount(),
		keyFieldCount: key.KeyNum(),
	}
	iter := s.root.InitRawIterator(&r.iter)
	if !iter.SeekPrefix(s.root.Root(), key.Key()) {
		r.atEnd = true
	}
	return r
}

func (s *MemIndex) makeKeyWithUnique(mdbKey *MdbKey, val IObject) error {
	mdbKey.Reset()
	if mdbKey.IsUnique() {
		return s.makeKey(mdbKey, val)
	}
	id := val.GetID()
	s.makeKey(mdbKey, val)
	if mdbKey.Len() > 255 {
		return fmt.Errorf("非唯一索引Key长度不允许超过255字节")
	}
	buf := mdbKey.Buffer()
	if id != 0 {
		if s.root.IsSortGreat() {
			// 索引从大到小排列时,id也要倒序一下,不然后插入的记录会排在先插入记录的前面
			buf.WriteUInt32(math.MaxUint32 - id)
		} else {
			buf.WriteUInt32(id)
		}
	}
	return nil
}

func (s *MemIndex) setError(err error) {
	s.lastError = err
}
