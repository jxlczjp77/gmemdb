package iradix

import "encoding/binary"

type ByteBuffer struct {
	buf       []byte
	bootstrap [64]byte
}

func (s *ByteBuffer) Reset() {
	s.buf = s.buf[:0]
}
func (s *ByteBuffer) Grow(n int) {
	m := s.grow(n)
	s.buf = s.buf[0:m]
}

func (s *ByteBuffer) tryGrowByReslice(n int) (int, bool) {
	if l := len(s.buf); l+n <= cap(s.buf) {
		s.buf = s.buf[:l+n]
		return l, true
	}
	return 0, false
}

func (s *ByteBuffer) Len() int { return len(s.buf) }

func (s *ByteBuffer) Cap() int { return cap(s.buf) }

func (s *ByteBuffer) Bytes() []byte { return s.buf }

func (s *ByteBuffer) grow(n int) int {
	m := s.Len()
	if i, ok := s.tryGrowByReslice(n); ok {
		return i
	}
	if s.buf == nil && n <= len(s.bootstrap) {
		s.buf = s.bootstrap[:n]
		return 0
	}
	buf := make([]byte, 2*cap(s.buf)+n)
	copy(buf, s.buf[:])
	s.buf = buf
	s.buf = s.buf[:m+n]
	return m
}

func (s *ByteBuffer) Truncate(n int) {
	if n == 0 {
		s.Reset()
		return
	}
	s.buf = s.buf[:n]
}

func (s *ByteBuffer) Write(p []byte) error {
	m, ok := s.tryGrowByReslice(len(p))
	if !ok {
		m = s.grow(len(p))
	}
	copy(s.buf[m:], p)
	return nil
}

func (s *ByteBuffer) WriteByte(c byte) error {
	m, ok := s.tryGrowByReslice(1)
	if !ok {
		m = s.grow(1)
	}
	s.buf[m] = c
	return nil
}

func (s *ByteBuffer) WriteString(v string) error {
	m, ok := s.tryGrowByReslice(len(v))
	if !ok {
		m = s.grow(len(v))
	}
	copy(s.buf[m:], v)
	return nil
}

func (s *ByteBuffer) WriteUInt16(v uint16) error {
	m, ok := s.tryGrowByReslice(2)
	if !ok {
		m = s.grow(2)
	}
	binary.BigEndian.PutUint16(s.buf[m:], v)
	return nil
}

func (s *ByteBuffer) WriteUInt32(v uint32) error {
	m, ok := s.tryGrowByReslice(4)
	if !ok {
		m = s.grow(4)
	}
	binary.BigEndian.PutUint32(s.buf[m:], v)
	return nil
}

func (s *ByteBuffer) WriteUInt64(v uint64) error {
	m, ok := s.tryGrowByReslice(8)
	if !ok {
		m = s.grow(8)
	}
	binary.BigEndian.PutUint64(s.buf[m:], v)
	return nil
}
