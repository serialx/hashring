package hashring

import (
	"fmt"
	"hash"
)

// HashSum allows to use a builder pattern to create different HashFunc objects.
// See examples for details.
type HashSum func([]byte) []byte

func (r HashSum) Use(
	hashKeyFunc func(bytes []byte) (HashKey, error),
) (HashFunc, error) {
	// check HashSum for errors
	testResult := r([]byte("test"))
	_, err := hashKeyFunc(testResult)
	if err != nil {
		const msg = "can't use given hash.Hash with given hashKeyFunc"
		return nil, fmt.Errorf("%s: %w", msg, err)
	}

	// build HashFunc
	return func(key []byte) HashKey {
		bytes := r(key)
		// ignore error because we already checked HashSum earlier
		hashKey, err := hashKeyFunc(bytes)
		if err != nil {
			panic(fmt.Sprintf("hashKeyFunc failure: %v", err))
		}
		return hashKey
	}, nil
}

func NewHash(hasher hash.Hash) HashSum {
	return func(key []byte) []byte {
		hasher.Reset()
		hasher.Write(key)
		return hasher.Sum(nil)
	}
}

func (r HashSum) FirstBytes(n int) HashSum {
	return func(bytes []byte) []byte {
		return r(bytes)[:n]
	}
}

func (r HashSum) LastBytes(n int) HashSum {
	return func(bytes []byte) []byte {
		result := r(bytes)
		return result[len(result)-n:]
	}
}
