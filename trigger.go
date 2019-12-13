package gmemdb

// IActionTrigger 动作触发器
type IActionTrigger interface {
	BeforeAdd(fid uint32, obj IObject, transaction *Transaction, reason int32) bool
	AfterAdd(fid uint32, obj IObject, transaction *Transaction, reason int32)
	BeforeUpdate(fid uint32, obj IObject, newObj IObject, transaction *Transaction, reason int32) bool
	AfterUpdate(fid uint32, obj IObject, transaction *Transaction, reason int32)
	BeforeRemove(fid uint32, obj IObject, transaction *Transaction, reason int32) bool
}

// ICommitTrigger 提交触发器
type ICommitTrigger interface {
	CommitAdd(fid uint32, obj IObject, reason int32)
	CommitUpdate(fid uint32, obj IObject, newObj IObject, reason int32)
	CommitRemove(fid uint32, obj IObject, reason int32)
}

// BaseActionTrigger BaseActionTrigger
type BaseActionTrigger struct{}

// BaseCommitTrigger BaseCommitTrigger
type BaseCommitTrigger struct{}

// BeforeAdd BeforeAdd
func (s *BaseActionTrigger) BeforeAdd(fid uint32, obj IObject, transaction *Transaction, reason int32) bool {
	return true
}

// AfterAdd AfterAdd
func (s *BaseActionTrigger) AfterAdd(fid uint32, obj IObject, transaction *Transaction, reason int32) {
}

// BeforeUpdate BeforeUpdate
func (s *BaseActionTrigger) BeforeUpdate(fid uint32, obj IObject, newObj IObject, transaction *Transaction, reason int32) bool {
	return true
}

// AfterUpdate AfterUpdate
func (s *BaseActionTrigger) AfterUpdate(fid uint32, obj IObject, transaction *Transaction, reason int32) {
}

// BeforeRemove BeforeRemove
func (s *BaseActionTrigger) BeforeRemove(fid uint32, obj IObject, transaction *Transaction, reason int32) bool {
	return true
}

// CommitAdd CommitAdd
func (s *BaseCommitTrigger) CommitAdd(fid uint32, obj IObject, reason int32) {
}

// CommitUpdate CommitUpdate
func (s *BaseCommitTrigger) CommitUpdate(fid uint32, obj IObject, newObj IObject, reason int32) {
}

// CommitRemove CommitRemove
func (s *BaseCommitTrigger) CommitRemove(fid uint32, obj IObject, reason int32) {
}

// TBeforeAdd TBeforeAdd
type TBeforeAdd func(fid uint32, obj IObject, transaction *Transaction, reason int32) bool

// TAfterAdd TAfterAdd
type TAfterAdd func(fid uint32, obj IObject, transaction *Transaction, reason int32)

// TBeforeUpdate TBeforeUpdate
type TBeforeUpdate func(fid uint32, obj IObject, newObj IObject, transaction *Transaction, reason int32) bool

// TAfterUpdate TAfterUpdate
type TAfterUpdate func(fid uint32, obj IObject, transaction *Transaction, reason int32)

// TBeforeRemove TBeforeRemove
type TBeforeRemove func(fid uint32, obj IObject, transaction *Transaction, reason int32) bool

// ActionTrigger 通用版动作触发器
type ActionTrigger struct {
	bAdd    TBeforeAdd
	aAdd    TAfterAdd
	bUpdate TBeforeUpdate
	aUpdate TAfterUpdate
	bRemove TBeforeRemove
}

// MakeActionTrigger 创建通用版动作触发器
func MakeActionTrigger(bAdd TBeforeAdd, aAdd TAfterAdd, bUpdate TBeforeUpdate, aUpdate TAfterUpdate, bRemove TBeforeRemove) *ActionTrigger {
	return &ActionTrigger{
		bAdd:    bAdd,
		aAdd:    aAdd,
		bUpdate: bUpdate,
		aUpdate: aUpdate,
		bRemove: bRemove,
	}
}

// BeforeAdd BeforeAdd
func (s *ActionTrigger) BeforeAdd(fid uint32, obj IObject, transaction *Transaction, reason int32) bool {
	if s.bAdd != nil {
		return s.bAdd(fid, obj, transaction, reason)
	}
	return true
}

// AfterAdd AfterAdd
func (s *ActionTrigger) AfterAdd(fid uint32, obj IObject, transaction *Transaction, reason int32) {
	if s.aAdd != nil {
		s.aAdd(fid, obj, transaction, reason)
	}
}

// BeforeUpdate BeforeUpdate
func (s *ActionTrigger) BeforeUpdate(fid uint32, obj IObject, newObj IObject, transaction *Transaction, reason int32) bool {
	if s.bUpdate != nil {
		return s.bUpdate(fid, obj, newObj, transaction, reason)
	}
	return true
}

// AfterUpdate AfterUpdate
func (s *ActionTrigger) AfterUpdate(fid uint32, obj IObject, transaction *Transaction, reason int32) {
	if s.aUpdate != nil {
		s.aUpdate(fid, obj, transaction, reason)
	}
}

// BeforeRemove BeforeRemove
func (s *ActionTrigger) BeforeRemove(fid uint32, obj IObject, transaction *Transaction, reason int32) bool {
	if s.bRemove != nil {
		return s.bRemove(fid, obj, transaction, reason)
	}
	return true
}

// TCommitAdd TCommitAdd
type TCommitAdd func(fid uint32, obj IObject, reason int32)

// TCommitUpdate TCommitUpdate
type TCommitUpdate func(fid uint32, obj IObject, newObj IObject, reason int32)

// TCommitRemove TCommitRemove
type TCommitRemove func(fid uint32, obj IObject, reason int32)

// CommitTrigger 通用版提交触发器
type CommitTrigger struct {
	Add    TCommitAdd
	Update TCommitUpdate
	Remove TCommitRemove
}

// MakeCommitTrigger 创建通用版提交触发器
func MakeCommitTrigger(add TCommitAdd, update TCommitUpdate, remove TCommitRemove) *CommitTrigger {
	return &CommitTrigger{
		Add:    add,
		Update: update,
		Remove: remove,
	}
}

// CommitAdd CommitAdd
func (s *CommitTrigger) CommitAdd(fid uint32, obj IObject, reason int32) {
	if s.Add != nil {
		s.Add(fid, obj, reason)
	}
}

// CommitUpdate CommitUpdate
func (s *CommitTrigger) CommitUpdate(fid uint32, obj IObject, newObj IObject, reason int32) {
	if s.Update != nil {
		s.Update(fid, obj, newObj, reason)
	}
}

// CommitRemove CommitRemove
func (s *CommitTrigger) CommitRemove(fid uint32, obj IObject, reason int32) {
	if s.Remove != nil {
		s.Remove(fid, obj, reason)
	}
}
