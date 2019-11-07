package hashring

import (
	"crypto/md5"
	"crypto/sha256"
	"fmt"
)

func ExampleCustomHashKey() {
	hashFunc, _ := NewHashSum(md5.New()).Int64PairHash()
	hashRing := NewWithHash([]string{"node1", "node2", "node3"}, hashFunc)
	nodes, _ := hashRing.GetNodes("key", hashRing.Size())
	fmt.Printf("%v", nodes)
	// Output: [node3 node2 node1]
}

func ExampleGetAllNodes() {
	hashRing := New([]string{"node1", "node2", "node3"})
	nodes, _ := hashRing.GetNodes("key", hashRing.Size())
	fmt.Printf("%v", nodes)
	// Output: [node3 node2 node1]
}
