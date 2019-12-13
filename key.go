package gmemdb

import (
	"fmt"
	"math"
	"reflect"

	"github.com/jxlczjp77/gmemdb/iradix"
)

func float32ToUint32(f float32) uint32 {
	u := math.Float32bits(f)
	if f >= 0 {
		u |= 0x80000000
	} else {
		u = ^u
	}
	return u
}

func float64ToUint64(f float64) uint64 {
	u := math.Float64bits(f)
	if f >= 0 {
		u |= 0x8000000000000000
	} else {
		u = ^u
	}
	return u
}

func uint32ToFloat32(u uint32) float32 {
	if u&0x80000000 > 0 {
		u &= ^uint32(0x80000000)
	} else {
		u = ^u
	}
	return math.Float32frombits(u)
}

func uint64ToFloat64(u uint64) float64 {
	if u&0x8000000000000000 > 0 {
		u &= ^uint64(0x8000000000000000)
	} else {
		u = ^u
	}
	return math.Float64frombits(u)
}

// MdbKey MdbKey
type MdbKey struct {
	buf      iradix.ByteBuffer
	isUnique bool
	keyCount int

	keyNum  int
	tailNil bool
}

func (s *MdbKey) Init(keyCount int, isUnique bool) {
	s.keyCount = keyCount
	s.isUnique = isUnique
	if keyCount <= 0 {
		panic("必须至少有一个key")
	}
	if keyCount > 255 {
		panic("组合键最多允许255个子项")
	}
}

func (s *MdbKey) IsUnique() bool {
	return s.isUnique
}

func (s *MdbKey) IsCompoundKey() bool {
	return s.keyCount > 1
}

func (s *MdbKey) KeyCount() int {
	return s.keyCount
}

func (s *MdbKey) KeyNum() int {
	return s.keyNum
}

func (s *MdbKey) Reset() {
	s.buf.Reset()
	s.keyNum = 0
	s.tailNil = false
}
func (s *MdbKey) Key() []byte {
	return s.buf.Bytes()
}
func (s *MdbKey) Len() int {
	return s.buf.Len()
}
func (s *MdbKey) Buffer() *iradix.ByteBuffer {
	return &s.buf
}
func (s *MdbKey) AppendBytes(val []byte) error {
	s.writeHead(len(val))
	return s.buf.Write(val)
}
func (s *MdbKey) AppendString(val string) error {
	s.writeHead(len(val))
	return s.buf.WriteString(val)
}

func (s *MdbKey) AppendInt16(val int16) error {
	s.writeHead(3)
	if val >= 0 {
		s.buf.WriteByte('>')
	} else {
		s.buf.WriteByte('-')
	}
	return s.buf.WriteUInt16(uint16(val))
}
func (s *MdbKey) AppendInt32(val int32) error {
	s.writeHead(5)
	if val >= 0 {
		s.buf.WriteByte('>')
	} else {
		s.buf.WriteByte('-')
	}
	return s.buf.WriteUInt32(uint32(val))
}
func (s *MdbKey) AppendInt64(val int64) error {
	s.writeHead(9)
	if val >= 0 {
		s.buf.WriteByte('>')
	} else {
		s.buf.WriteByte('-')
	}
	return s.buf.WriteUInt64(uint64(val))
}
func (s *MdbKey) AppendInt(val int) error   { return s.AppendInt32(int32(val)) }
func (s *MdbKey) AppendUInt(val uint) error { return s.AppendUInt32(uint32(val)) }
func (s *MdbKey) AppendUInt16(val uint16) error {
	s.writeHead(2)
	return s.buf.WriteUInt16(val)
}
func (s *MdbKey) AppendUInt32(val uint32) error {
	s.writeHead(4)
	return s.buf.WriteUInt32(val)
}
func (s *MdbKey) AppendUInt64(val uint64) error {
	s.writeHead(8)
	return s.buf.WriteUInt64(val)
}
func (s *MdbKey) AppendFloat32(val float32) error {
	return s.AppendUInt32(float32ToUint32(val))
}
func (s *MdbKey) AppendFloat64(val float64) error {
	return s.AppendUInt64(float64ToUint64(val))
}
func (s *MdbKey) AppendValue(val interface{}) error {
	switch t := val.(type) {
	case int16:
		return s.AppendInt16(val.(int16))
	case int32:
		return s.AppendInt32(val.(int32))
	case int:
		return s.AppendInt(val.(int))
	case int64:
		return s.AppendInt64(val.(int64))
	case uint16:
		return s.AppendUInt16(val.(uint16))
	case uint32:
		return s.AppendUInt32(val.(uint32))
	case uint:
		return s.AppendUInt(val.(uint))
	case uint64:
		return s.AppendUInt64(val.(uint64))
	case string:
		return s.AppendString(val.(string))
	case []byte:
		return s.AppendBytes(val.([]byte))
	default:
		pVal := reflect.ValueOf(val)
		f := pVal.MethodByName("Val")
		if !f.IsValid() {
			return fmt.Errorf("不支持的key类型[%v]", t)
		}
		subVal := f.Call([]reflect.Value{})[0].Interface()
		return s.AppendValue(subVal)
	}
}

func (s *MdbKey) writeHead(n int) error {
	if s.keyNum > s.keyCount {
		return fmt.Errorf("超出给定Key数量[%d]", s.keyCount)
	}
	if s.keyCount > 1 {
		if s.keyNum == 0 {
			s.buf.WriteByte(byte(s.keyCount))
		}
		s.buf.WriteByte(byte(n))
	}
	s.keyNum++
	return nil
}
