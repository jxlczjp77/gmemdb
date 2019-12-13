package gmemdb

import (
	"fmt"
	"math"
	"reflect"

	"github.com/jxlczjp77/gmemdb/iradix"
)

func formatndPanic(format string, v ...interface{}) {
	panic(fmt.Sprintf(format, v...))
}

type eDBResourceType int

const (
	eCreate eDBResourceType = iota
	eUpdate
	eDelete
	eNone
)

// DatabaseResource 数据表事物资源
type DatabaseResource struct {
	resourceBase
	factory     *ObjectFactory
	root        *iradix.Tree
	ref         IObject
	tempRef     IObject
	t           eDBResourceType
	savePointID int
}

// IObject 对象接口
type IObject interface {
	GetID() uint32
	SetID(id uint32)
}

// ObjectBase 对象基类
type ObjectBase struct {
	PrimaryID uint32
}

// GetID 读取对象ID
func (s *ObjectBase) GetID() uint32 {
	return s.PrimaryID
}

// SetID 设置对象ID
func (s *ObjectBase) SetID(id uint32) {
	s.PrimaryID = id
}

// ITable 表接口
type ITable interface {
	Name() string
	FactoryID() uint32
	Clear()
	GetStore() *ObjectFactory
	GetPBType() reflect.Type
	GetType() reflect.Type
}

// TableBase 表基类
type TableBase struct {
	Store *ObjectFactory
}

// Init 初始化表
func (s *TableBase) Init(name string, nullTypeVal interface{}, nullPBTypeVal interface{}) {
	s.Store = &ObjectFactory{}
	s.Store.Init(name, nullTypeVal, nullPBTypeVal)
}

// Name 表名字
func (s *TableBase) Name() string { return s.Store.Name }

// FactoryID 表ID
func (s *TableBase) FactoryID() uint32 { return s.Store.FactoryID }

// Clear 清空表
func (s *TableBase) Clear() {
	s.Store.Clear()
	s.Store.ResetMaxID()
}

// ClearData 清空表
func (s *TableBase) ClearData() {
	s.Store.Clear()
}

// RemoveAll 清空表
func (s *TableBase) RemoveAll(transaction *Transaction, reason int32) {
	s.Store.RemoveAll(transaction, reason)
	s.Store.ResetMaxID()
}

// RemoveAllData 清空表
func (s *TableBase) RemoveAllData(transaction *Transaction, reason int32) {
	s.Store.RemoveAll(transaction, reason)
}

// Empty 是否为空
func (s *TableBase) Empty() bool {
	return s.Store.Empty()
}

// Count 表大小
func (s *TableBase) Count() int {
	return s.Store.Count()
}

// GetStore GetStore
func (s *TableBase) GetStore() *ObjectFactory {
	return s.Store
}

// GetPBType GetPBType
func (s *TableBase) GetPBType() reflect.Type { return s.Store.PBType }

// GetType GetType
func (s *TableBase) GetType() reflect.Type { return s.Store.Type }

// ObjectFactory 对象工厂
type ObjectFactory struct {
	FactoryID  uint32
	instanceID uint32
	Name       string
	root       *iradix.Tree
	txn        *iradix.Txn
	maxID      uint32
	Type       reflect.Type
	PBType     reflect.Type

	indexs   []*MemIndex
	indexMap map[string]int

	actionTriggers []IActionTrigger
	commitTriggers []ICommitTrigger
}

// Init 初始化
func (s *ObjectFactory) Init(name string, nullTypeVal interface{}, nullPBValue interface{}) {
	s.Name = name
	s.FactoryID = allocFactoryID()
	s.instanceID = s.FactoryID
	s.txn = iradix.NewTxn()
	s.root = s.txn.Root()
	s.maxID = 1
	if recType := reflect.TypeOf(nullTypeVal); recType != nil {
		s.Type = recType.Elem()
	}
	if pbType := reflect.TypeOf(nullPBValue); pbType != nil {
		s.PBType = pbType.Elem()
	}
	s.indexMap = make(map[string]int)
	s.actionTriggers = make([]IActionTrigger, 0)
	s.commitTriggers = make([]ICommitTrigger, 0)
	s.AddIndex("PrimaryID", func(key *MdbKey, obj IObject) error { return key.AppendUInt32(obj.GetID()) }, true)
}

func (s *ObjectFactory) updateIndexRoot(idx *MemIndex) {
	_, _ = s.txn.Insert(idx.name, idx.root)
	s.root = s.txn.Root()
}

var gfactoryID uint32

func allocFactoryID() uint32 {
	gfactoryID++
	return gfactoryID
}

// Clear 清空数据
func (s *ObjectFactory) Clear() {
	for _, idx := range s.indexs {
		idx.Clear()
		s.updateIndexRoot(idx)
	}
}

// ResetMaxID 清空数据
func (s *ObjectFactory) ResetMaxID() {
	s.maxID = 1
}

// RemoveAll 清空表
func (s *ObjectFactory) RemoveAll(transaction *Transaction, reason int32) {
	it := s.Begin(0)
	for it.Next() {
		s.Remove(it.Value(), transaction, reason)
	}
}

// FindByPrimaryID 根据主键查找
func (s *ObjectFactory) FindByPrimaryID(id uint32) Iterator {
	return s.FindByIndex(0).AppendUInt32(id).Fire()
}

// FindByPB 根据Protobuf结构查找,pb结果和记录结果的差别是,每个pb
// 字段名字相同但是类型为指针类型
func (s *ObjectFactory) FindByPB(pb interface{}) (Iterator, error) {
	n := len(s.indexs)
	var lastErr error
	for i := 1; i < n; i++ {
		idx := s.indexs[i]
		it, err := idx.FindByPB(pb)
		if err != nil {
			lastErr = err
		} else if it != nil {
			return it, nil
		}
	}
	if lastErr == nil {
		lastErr = fmt.Errorf("索引未找到")
	}
	return nil, lastErr
}

// PBToRecord 根据Protobuf结构新建记录
func (s *ObjectFactory) PBToRecord(pb interface{}, checkField bool) IObject {
	pbType := reflect.TypeOf(pb).Elem()
	pbFieldNum := pbType.NumField()
	recFieldNum := s.Type.NumField()
	if checkField {
		if pbFieldNum != s.PBType.NumField() || pbType.Name() != s.PBType.Name() {
			return nil
		}
	}
	obj := reflect.New(s.Type).Elem()
	pbValue := reflect.ValueOf(pb).Elem()
	for i := 0; i < pbFieldNum; i++ {
		j := i + 1
		if j < recFieldNum {
			val := obj.Field(j)
			pbVal := pbValue.Field(i)
			if pbVal.Kind() == reflect.Ptr {
				if !pbVal.IsNil() {
					val.Set(pbVal.Elem())
				}
			} else {
				val.Set(pbVal)
			}
		}
	}
	return obj.Addr().Interface().(IObject)
}

// RecordToPB 内存记录转换成PB结构
func (s *ObjectFactory) RecordToPB(rec IObject, checkField bool) interface{} {
	if s.PBType == nil {
		return nil
	}
	recType := reflect.TypeOf(rec).Elem()
	pbFieldNum := s.PBType.NumField()
	recFieldNum := recType.NumField()
	if checkField {
		if recFieldNum != s.Type.NumField() || recType.Name() != s.Type.Name() {
			return nil
		}
	}
	pb := reflect.New(s.PBType)
	pbValue := pb.Elem()
	recValue := reflect.ValueOf(rec).Elem()
	for i := 0; i < pbFieldNum; i++ {
		j := i + 1
		if j < recFieldNum {
			recVal := recValue.Field(j)
			pbVal := pbValue.Field(i)
			if pbVal.Kind() == reflect.Ptr {
				if pbVal.IsNil() {
					pbVal.Set(reflect.New(s.PBType.Field(i).Type.Elem()))
				}
				pbVal.Elem().Set(recVal)
			} else {
				pbVal.Set(recVal)
			}
		}
	}
	return pb.Interface()
}

// Count 总数量
func (s *ObjectFactory) Count() int {
	return s.indexs[0].root.Len()
}

// Empty 是否为空
func (s *ObjectFactory) Empty() bool {
	return s.Count() == 0
}

// Add 添加对象
func (s *ObjectFactory) Add(obj IObject, transaction *Transaction, reason int32) bool {
	return s.internalAdd(obj, transaction, reason, true)
}

// Update 更新对象
func (s *ObjectFactory) Update(oldObj IObject, newObj IObject, transaction *Transaction, reason int32) bool {
	return s.internalUpdate(oldObj, newObj, transaction, reason, true)
}

// Remove 添加对象
func (s *ObjectFactory) Remove(obj IObject, transaction *Transaction, reason int32) bool {
	return s.internalRemove(obj, transaction, reason, true)
}

// AddActionTrigger 添加Action触发器
func (s *ObjectFactory) AddActionTrigger(p IActionTrigger) IActionTrigger {
	s.RemoveActionTrigger(p)
	s.actionTriggers = append(s.actionTriggers, p)
	return p
}

// RemoveActionTrigger 移除Action触发器
func (s *ObjectFactory) RemoveActionTrigger(p IActionTrigger) {
	for i, action := range s.actionTriggers {
		if action == p {
			s.actionTriggers = append(s.actionTriggers[:i], s.actionTriggers[i+1:]...)
			return
		}
	}
}

// AddCommitTrigger 添加Commit触发器
func (s *ObjectFactory) AddCommitTrigger(p ICommitTrigger) ICommitTrigger {
	s.RemoveCommitTrigger(p)
	s.commitTriggers = append(s.commitTriggers, p)
	return p
}

// RemoveCommitTrigger 移除Commit触发器
func (s *ObjectFactory) RemoveCommitTrigger(p ICommitTrigger) {
	for i, action := range s.commitTriggers {
		if action == p {
			s.commitTriggers = append(s.commitTriggers[:i], s.commitTriggers[i+1:]...)
			return
		}
	}
}

// Walk 遍历数据
func (s *ObjectFactory) Walk(cond func(obj IObject) bool) IObject {
	for it := s.Begin(0); it.Next(); {
		if cond(it.Value()) {
			return it.Value()
		}
	}
	return nil
}

func (s *ObjectFactory) getType() reflect.Type {
	return s.Type
}

func (s *ObjectFactory) name() string {
	return s.Name
}

// AddIndex 添加索引,返回索引编号
func (s *ObjectFactory) AddIndex(fields string, makeKey MakeKeyFunc, unique bool) int {
	if idxNum, ok := s.indexMap[fields]; ok {
		return idxNum
	}
	idxNum := len(s.indexs)
	idx := NewMemIndex(fields, idxNum, makeKey, unique, s)
	_, _ = s.txn.Insert(idx.name, idx.root)
	s.root = s.txn.Commit()
	s.indexs = append(s.indexs, idx)
	s.indexMap[fields] = idxNum
	return idxNum
}

// GetIndex GetIndex
func (s *ObjectFactory) GetIndex(idxNum int) *MemIndex {
	if idxNum < len(s.indexs) {
		return s.indexs[idxNum]
	}
	return nil
}

// GetIndexByName GetIndexByName
func (s *ObjectFactory) GetIndexByName(fields string) *MemIndex {
	return s.GetIndex(s.indexMap[fields])
}

// FindByIndex 指定索引编号和key查找对象
func (s *ObjectFactory) FindByIndex(idxNum int) MdbFinder {
	idx := s.indexs[idxNum]
	idx.mdbKey.Reset()
	return MdbFinder{idx: idx}
}

// FindByIndexName 指定索引编号和key查找对象
func (s *ObjectFactory) FindByIndexName(fields string) MdbFinder {
	idx := s.GetIndexByName(fields)
	if idx != nil {
		idx.mdbKey.Reset()
	}
	return MdbFinder{idx: idx}
}

// Begin 返回第一个位置
func (s *ObjectFactory) Begin(idxNum int) Iterator {
	return s.indexs[idxNum].Begin()
}

func (s *ObjectFactory) internalAdd(obj IObject, transaction *Transaction, reason int32, notify bool) bool {
	if s.maxID > math.MaxInt32 {
		formatndPanic("表[%s]Add失败: 超出最大记录数[%d]限制", s.Name, s.maxID)
	}
	obj.SetID(s.maxID)
	if !s.beforeAdd(obj, transaction, reason, notify) {
		return false
	}
	resource := s.makeResource(transaction, eCreate, obj, nil)
	// var wg sync.WaitGroup
	// wg.Add(len(s.indexs))
	// s.loopIndex(func(idx *MemIndex) {
	// 	utils.DefaultWorkGroup().AddTask(func(ID int) interface{} {
	// 		idx.setError(idx.Add(obj))
	// 		wg.Done()
	// 		return nil
	// 	}, nil)
	// })
	// wg.Wait()
	// s.loopIndex(func(idx *MemIndex) {
	// 	if idx.lastError != nil {
	// 		formatndPanic("表[%s]索引[%s]Add失败: %s", s.Name, idx.name, idx.lastError.Error())
	// 	}
	// 	s.updateIndexRoot(idx)
	// })
	for _, idx := range s.indexs {
		err := idx.Add(obj)
		if err != nil {
			s.rollback()
			formatndPanic("表[%s]索引[%s]Add失败: %s", s.Name, idx.name, err.Error())
		}
		s.updateIndexRoot(idx)
	}
	if transaction == nil {
		s.commit()
		s.afterAdd(obj, transaction, reason, notify)
		s.commitAdd(obj, reason, notify)
	} else {
		transaction.AddResource(resource)
		s.afterAdd(obj, transaction, reason, notify)
	}
	s.maxID++
	return true
}

func (s *ObjectFactory) internalUpdate(oldObj IObject, newObj IObject, transaction *Transaction, reason int32, notify bool) bool {
	if oldObj.GetID() == 0 {
		formatndPanic("表[%s]Update: 更新无效对象(未设置对象ID),请查询后再更新", s.Name)
	}
	newObj.SetID(oldObj.GetID())
	if !s.beforeUpdate(oldObj, newObj, transaction, reason, notify) {
		return false
	}
	resource := s.makeResource(transaction, eUpdate, oldObj, newObj)
	for _, idx := range s.indexs {
		err := idx.Update(oldObj, newObj)
		if err != nil {
			s.rollback()
			formatndPanic("表[%s]索引[%s]Update失败: %s", s.Name, idx.name, err.Error())
		}
		s.updateIndexRoot(idx)
	}
	if transaction == nil {
		s.commit()
		s.afterUpdate(newObj, transaction, reason, notify)
		s.commitUpdate(oldObj, newObj, reason, notify)
	} else {
		transaction.AddResource(resource)
		s.afterUpdate(newObj, transaction, reason, notify)
	}
	return true
}

func (s *ObjectFactory) internalRemove(obj IObject, transaction *Transaction, reason int32, notify bool) bool {
	if obj.GetID() == 0 {
		formatndPanic("表[%s]Remove: 删除无效对象(未设置对象ID),请查询后再删除", s.Name)
	}
	if !s.beforeRemove(obj, transaction, reason, notify) {
		return false
	}
	resource := s.makeResource(transaction, eDelete, obj, nil)
	for _, idx := range s.indexs {
		err := idx.Delete(obj)
		if err != nil {
			s.rollback()
			formatndPanic("表[%s]索引[%s]Remove失败: %s", s.Name, idx.name, err.Error())
		}
		s.updateIndexRoot(idx)
	}
	if transaction == nil {
		s.commit()
		s.commitRemove(obj, reason, notify)
	} else {
		transaction.AddResource(resource)
	}
	return true
}

func (s *ObjectFactory) beforeAdd(obj IObject, transaction *Transaction, reason int32, notify bool) bool {
	if notify {
		for _, action := range s.actionTriggers {
			if !action.BeforeAdd(s.FactoryID, obj, transaction, reason) {
				return false
			}
		}
	}
	return true
}

func (s *ObjectFactory) afterAdd(obj IObject, transaction *Transaction, reason int32, notify bool) {
	if notify {
		for _, action := range s.actionTriggers {
			action.AfterAdd(s.FactoryID, obj, transaction, reason)
		}
	}
}

func (s *ObjectFactory) beforeUpdate(oldObj IObject, newObj IObject, transaction *Transaction, reason int32, notify bool) bool {
	if notify {
		for _, action := range s.actionTriggers {
			if !action.BeforeUpdate(s.FactoryID, oldObj, newObj, transaction, reason) {
				return false
			}
		}
	}
	return true
}

func (s *ObjectFactory) afterUpdate(obj IObject, transaction *Transaction, reason int32, notify bool) {
	if notify {
		for _, action := range s.actionTriggers {
			action.AfterUpdate(s.FactoryID, obj, transaction, reason)
		}
	}
}

func (s *ObjectFactory) beforeRemove(obj IObject, transaction *Transaction, reason int32, notify bool) bool {
	if notify {
		for _, action := range s.actionTriggers {
			if !action.BeforeRemove(s.FactoryID, obj, transaction, reason) {
				return false
			}
		}
	}
	return true
}

func (s *ObjectFactory) commitAdd(obj IObject, reason int32, notify bool) {
	if notify {
		for _, action := range s.commitTriggers {
			action.CommitAdd(s.FactoryID, obj, reason)
		}
	}
}

func (s *ObjectFactory) commitUpdate(oldObj IObject, newObj IObject, reason int32, notify bool) {
	if notify {
		for _, action := range s.commitTriggers {
			action.CommitUpdate(s.FactoryID, oldObj, newObj, reason)
		}
	}
}

func (s *ObjectFactory) commitRemove(obj IObject, reason int32, notify bool) {
	if notify {
		for _, action := range s.commitTriggers {
			action.CommitRemove(s.FactoryID, obj, reason)
		}
	}
}

func (s *ObjectFactory) makeResource(transaction *Transaction, t eDBResourceType, ref IObject, tempRef IObject) *DatabaseResource {
	savePointID := -1
	if transaction != nil {
		savePointID = transaction.LastSavePointID()
		savePointID2 := s.txn.LastSavePointID()
		if savePointID != savePointID2 {
			if savePointID != savePointID2+1 {
				formatndPanic("事物回滚点异常")
			}
			s.txn.AllocSavePoint()
			for _, idx := range s.indexs {
				idx.txn.AllocSavePoint()
			}
		}
	}
	return &DatabaseResource{factory: s, ref: ref, tempRef: tempRef, t: t, root: s.root, savePointID: savePointID}
}

func (s *ObjectFactory) loopIndex(cb func(idx *MemIndex)) {
	for _, idx := range s.indexs {
		cb(idx)
	}
}

func (s *ObjectFactory) commit() {
	if s.txn.Dirty() {
		for _, idx := range s.indexs {
			idx.root = idx.txn.Commit()
		}
		s.root = s.txn.Commit()
	}
}

func (s *ObjectFactory) rollback() {
	s.rollbackTo(-1)
}

func (s *ObjectFactory) rollbackTo(savePointID int) {
	if s.txn.Dirty() {
		s.txn.RollbackTo(savePointID)
		s.root = s.txn.Root()
		for _, idx := range s.indexs {
			idx.txn.RollbackTo(savePointID)
			idx.root = idx.txn.Root()
		}
	}
}

// FreeListLen FreeListLen
func (s *ObjectFactory) FreeListLen() int {
	return s.txn.FreeListLen()
}

// Commit 提交事物
func (s *DatabaseResource) Commit(reason int32) {
	switch s.t {
	case eCreate:
		s.factory.commitAdd(s.ref, reason, true)
		break
	case eUpdate:
		s.factory.commitUpdate(s.ref, s.tempRef, reason, true)
		break
	case eDelete:
		s.factory.commitRemove(s.ref, reason, true)
		break
	case eNone:
		break
	}
	s.factory.commit()
}

// Rollback 回滚事物
func (s *DatabaseResource) Rollback() {
	s.factory.rollbackTo(s.savePointID)
}

func (s *DatabaseResource) isControl() bool {
	return s.t == eNone
}

func (s *DatabaseResource) free() {
}

func (s *DatabaseResource) merge(preResource Resource) mergeResult {
	preRes := preResource.(*DatabaseResource)
	if preRes.t == eNone {
		return eMergeFailThis
	}
	preT := preRes.t
	t := s.t
	switch preT {
	case eCreate:
		switch t {
		case eCreate:
			formatndPanic("不可能创建2次")
			break
		case eUpdate: // 原来是创建，那更新还是创建
			preRes.ref = s.tempRef
			preRes.tempRef = nil
			return eMergeOk
		case eDelete: // 已经被删除了
			preRes.t = eNone
			preRes.ref = nil
			preRes.tempRef = nil
			return eMergeOk
		case eNone:
			formatndPanic("不可能创建后合并")
			break
		}
		break
	case eUpdate:
		switch t {
		case eCreate:
			formatndPanic("不可能先更新后创建")
			break
		case eUpdate:
			preRes.tempRef = s.tempRef
			return eMergeOk
		case eDelete:
			preRes.t = eDelete
			preRes.tempRef = nil
			return eMergeOk
		case eNone:
			formatndPanic("不可能更新后合并")
			break
		}
		break
	case eDelete:
		switch t {
		case eCreate:
			return eMergeFailAll
		case eUpdate:
			formatndPanic("不可能删除后更新")
			break
		case eDelete:
			formatndPanic("不可能删除后删除")
			break
		case eNone:
			formatndPanic("不可能删除后合并")
			break
		}
		break
	case eNone:
		switch t {
		case eCreate:
			return eMergeFailAll
		case eUpdate:
			formatndPanic("不可能合并后更新")
			break
		case eDelete:
			formatndPanic("不可能合并后删除")
			break
		case eNone:
			return eMergeFailAll
		}
		break
	}

	formatndPanic("不可能出现的异常")
	return eMergeFailThis
}

func (s *DatabaseResource) tag() resourceTag {
	return eDatabase
}

func (s *DatabaseResource) id() uint32 {
	return s.factory.instanceID
}

func (s *DatabaseResource) subID() uint32 {
	return s.ref.GetID()
}
