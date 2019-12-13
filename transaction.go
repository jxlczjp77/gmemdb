package gmemdb

import (
	"sort"
)

type resourceTag int

const (
	eSavepoint resourceTag = iota
	eTransaction
	eDatabase
)

type mergeResult int

const (
	eMergeOk mergeResult = iota
	eMergeFailThis
	eMergeFailAll
)

// Resource 事物资源
type Resource interface {
	tag() resourceTag
	id() uint32
	subID() uint32

	Commit(reason int32)
	Rollback()

	isControl() bool
	free()
	merge(preResource Resource) mergeResult
	GetPos() int
	SetPos(pos int)
}

type resourceBase struct {
	pos int
}

func (s *resourceBase) GetPos() int {
	return s.pos
}

func (s *resourceBase) SetPos(pos int) {
	s.pos = pos
}

// TransactionSavePoint 事物回滚点
type TransactionSavePoint struct {
	resourceBase
	ts *Transaction
}

func (s *TransactionSavePoint) tag() resourceTag {
	return eSavepoint
}

func (s *TransactionSavePoint) id() uint32 {
	return 0
}

func (s *TransactionSavePoint) subID() uint32 {
	return 0
}

// Commit 提交
func (s *TransactionSavePoint) Commit(reason int32) {
}

// Rollback 回滚
func (s *TransactionSavePoint) Rollback() {
	if s.ts != nil {
		s.ts.rollbackToSavePoint(s)
	}
}

// Invalid Invalid
func (s *TransactionSavePoint) Invalid() bool {
	return s.ts == nil
}

func (s *TransactionSavePoint) isControl() bool {
	return true
}

func (s *TransactionSavePoint) free() {
	s.ts = nil
	s.pos = 0
}

func (s *TransactionSavePoint) merge(preResource Resource) mergeResult {
	return eMergeFailAll
}

type mergeMap map[uint64][]int

// Transaction 事物对象
type Transaction struct {
	resources  []Resource
	savePoints []*TransactionSavePoint
	merges     mergeMap
}

// NewTransaction 新建事物
func NewTransaction() *Transaction {
	return &Transaction{
		resources:  make([]Resource, 0),
		merges:     make(mergeMap),
		savePoints: make([]*TransactionSavePoint, 0),
	}
}

// AllocSavePoint 创建事物回滚点
func (s *Transaction) AllocSavePoint() *TransactionSavePoint {
	savePoint := &TransactionSavePoint{ts: s}
	s.AddResource(savePoint)
	s.savePoints = append(s.savePoints, savePoint)
	return savePoint
}

func (s *Transaction) lastSavePoint() *TransactionSavePoint {
	n := len(s.savePoints)
	if n > 0 {
		return s.savePoints[n-1]
	}
	return nil
}

// LastSavePointID -1表示没有保存点,0表示第一个保存点的索引,...
func (s *Transaction) LastSavePointID() int {
	return len(s.savePoints) - 1
}

// AddResource 添加资源到事物
func (s *Transaction) AddResource(resource Resource) {
	pos := len(s.resources)
	if !resource.isControl() {
		lastSP := s.lastSavePoint()
		var endPos int
		if lastSP != nil {
			endPos = lastSP.pos
		} else {
			endPos = 0
		}
		if s.mergeBack(endPos, resource) {
			resource.free()
			return
		}
		id := makeMergeID(resource.id(), resource.subID())
		s.merges[id] = append(s.merges[id], pos)
	}
	resource.SetPos(pos)
	s.resources = append(s.resources, resource)
}

// Commit 提交事物
func (s *Transaction) Commit(reason int32) {
	n := len(s.resources)
	if n == 0 {
		return
	}
	var toBeCommit []Resource
	for i := n - 1; i >= 0; i-- {
		resource := s.resources[i]
		if resource.isControl() {
			resource.free()
			continue
		} else if s.mergeBack(0, resource) {
			resource.free()
			continue
		}
		toBeCommit = append(toBeCommit, resource)
	}

	for i := len(toBeCommit) - 1; i >= 0; i-- {
		resource := toBeCommit[i]
		resource.Commit(reason)
	}
	s.resources = s.resources[:0]
	s.merges = make(mergeMap)
	s.savePoints = s.savePoints[:0]
}

// Rollback 回滚事物
func (s *Transaction) Rollback() {
	s.rollbackToSavePoint(nil)
	s.resources = s.resources[:0]
	s.merges = make(mergeMap)
	if len(s.savePoints) != 0 {
		panic("回滚事物失败：仍存在事物回滚点未回滚")
	}
}

func (s *Transaction) isControl() bool {
	return true
}

func (s *Transaction) mergeBack(endPos int, resource Resource) bool {
	id := makeMergeID(resource.id(), resource.subID())
	posList, ok := s.merges[id]
	if !ok {
		s.merges[id] = []int{}
		return false
	}

	for i := len(posList) - 1; i >= 0; i-- {
		pos := posList[i]
		if pos < endPos {
			break
		} else if pos >= resource.GetPos() {
			continue
		}

		p := s.resources[pos]
		if p.tag() != resource.tag() {
			panic("事物合并异常：尝试合并不同tag的资源")
		}
		if p.id() != resource.id() {
			panic("事物合并异常：尝试合并不同ID的资源")
		}

		pid := makeMergeID(p.id(), p.subID())
		if pid == id {
			switch resource.merge(p) {
			case eMergeOk:
				return true
			case eMergeFailThis:
				break
			case eMergeFailAll:
				return false
			default:
				return false
			}
		}
	}
	return false
}

func (s *Transaction) rollbackToSavePoint(savePoint *TransactionSavePoint) {
	var rollbackPos int
	if savePoint != nil {
		rollbackPos = savePoint.pos
	} else {
		rollbackPos = 0
	}

	toBeRollback := make(map[uint32]int)
	for k, v := range s.merges {
		n := len(v)
		if n > 0 {
			var i int
			if rollbackPos > 0 {
				i = sort.SearchInts(v, rollbackPos)
				if i < n && v[i] < rollbackPos {
					i++
				}
			} else {
				i = 0
			}

			if i < n {
				pos := v[i]
				if pos >= rollbackPos {
					s.merges[k] = v[:i]
					resource := s.resources[pos]
					id := resource.id()
					prePos, ok := toBeRollback[id]

					// 每一类资源只需回滚到最早的一个位置
					if !ok || pos < prePos {
						toBeRollback[id] = pos
					}
				}
			}
		}
	}
	for _, pos := range toBeRollback {
		resource := s.resources[pos]
		resource.Rollback()
		resource.free()
	}

	lastSP := s.lastSavePoint()
	for i := len(s.resources) - 1; i >= rollbackPos; i-- {
		resource := s.resources[i]
		if resource.tag() == eSavepoint && resource.(*TransactionSavePoint).pos == lastSP.pos {
			n := len(s.savePoints)
			s.savePoints = s.savePoints[:n-1]
			lastSP = s.lastSavePoint()
		}
		resource.free()
	}
	s.resources = s.resources[:rollbackPos]
}

func makeMergeID(id uint32, subID uint32) uint64 {
	return uint64(id)<<32 | uint64(subID)
}
