package hashring

import (
	"fmt"
	"hash"
)

// HashSum allows to use a builder pattern to create different HashFunc objects.
// See examples for details.
type HashSum func([]byte) []byte

func (r HashSum) Int64PairHash() (HashFunc, error) {
	// check HashSum for errors
	testResult := r([]byte("test"))
	_, err := NewInt64PairHashKey(testResult)
	if err != nil {
		return nil, fmt.Errorf("can't use given hash.Hash with Int64PairHash: %w", err)
	}

	// build HashFunc
	return func(key []byte) HashKey {
		bytes := r(key)
		// ignore error because we already checked HashSum earlier
		hashKey, _ := NewInt64PairHashKey(bytes)
		return hashKey
	}, nil
}

func NewHashSum(hasher hash.Hash) HashSum {
	return func(key []byte) []byte {
		return hasher.Sum(key)
	}
}
