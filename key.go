package hashring

import "encoding/binary"

type Int64PairHashKey struct {
	High int64
	Low  int64
}

func (k Int64PairHashKey) Less(other HashKey) bool {
	o := other.(Int64PairHashKey)
	if k.High < o.High {
		return true
	}
	return k.High == o.High && k.Low < o.Low
}

func NewInt64PairHashKey(bytes []byte) Int64PairHashKey {
	return Int64PairHashKey{
		High: int64(binary.LittleEndian.Uint64(bytes[:8])),
		Low:  int64(binary.LittleEndian.Uint64(bytes[8:])),
	}
}
