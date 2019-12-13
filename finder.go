package gmemdb

type MdbFinder struct {
	idx *MemIndex
	err error
}

func (s MdbFinder) AppendBytes(val []byte) MdbFinder {
	if s.err == nil {
		s.err = s.idx.mdbKey.AppendBytes(val)
	}
	return s
}
func (s MdbFinder) AppendString(val string) MdbFinder {
	if s.err == nil {
		s.err = s.idx.mdbKey.AppendString(val)
	}
	return s
}
func (s MdbFinder) AppendInt16(val int16) MdbFinder {
	if s.err == nil {
		s.err = s.idx.mdbKey.AppendInt16(val)
	}
	return s
}
func (s MdbFinder) AppendInt32(val int32) MdbFinder {
	if s.err == nil {
		s.err = s.idx.mdbKey.AppendInt32(val)
	}
	return s
}
func (s MdbFinder) AppendInt64(val int64) MdbFinder {
	if s.err == nil {
		s.err = s.idx.mdbKey.AppendInt64(val)
	}
	return s
}
func (s MdbFinder) AppendInt(val int) MdbFinder {
	if s.err == nil {
		s.err = s.idx.mdbKey.AppendInt(val)
	}
	return s
}
func (s MdbFinder) AppendUInt(val uint) MdbFinder {
	if s.err == nil {
		s.err = s.idx.mdbKey.AppendUInt(val)
	}
	return s
}
func (s MdbFinder) AppendUInt16(val uint16) MdbFinder {
	if s.err == nil {
		s.err = s.idx.mdbKey.AppendUInt16(val)
	}
	return s
}
func (s MdbFinder) AppendUInt32(val uint32) MdbFinder {
	if s.err == nil {
		s.err = s.idx.mdbKey.AppendUInt32(val)
	}
	return s
}
func (s MdbFinder) AppendUInt64(val uint64) MdbFinder {
	if s.err == nil {
		s.err = s.idx.mdbKey.AppendUInt64(val)
	}
	return s
}
func (s MdbFinder) AppendFloat32(val float32) MdbFinder {
	if s.err == nil {
		s.err = s.idx.mdbKey.AppendFloat32(val)
	}
	return s
}
func (s MdbFinder) AppendFloat64(val float64) MdbFinder {
	if s.err == nil {
		s.err = s.idx.mdbKey.AppendFloat64(val)
	}
	return s
}
func (s MdbFinder) Fire() Iterator {
	if s.err != nil {
		return &radixIterator{atEnd: true}
	}
	return s.idx.findByKey(&s.idx.mdbKey, true)
}
