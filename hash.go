package hashring

import "crypto/md5"

// HashResult allows to use a builder pattern to create different HashFunc objects
// Example: hashFunc := MD5().Int64PairHash()
type HashResult func([]byte) []byte

func (r HashResult) Int64PairHash() HashFunc {
	return func(key []byte) HashKey {
		bytes := r(key)
		return NewInt64PairHashKey(bytes)
	}
}

func MD5() HashResult {
	return func(key []byte) []byte {
		bytes := md5.Sum(key)
		return bytes[:]
	}
}
