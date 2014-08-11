package hashring

import (
	"testing"
)

func expectNode(t *testing.T, hashRing *HashRing, key string, expectedNode string) {
	node, ok := hashRing.GetNode(key)
	if !ok || node != expectedNode {
		t.Error("GetNode(", key, ") expected", expectedNode, "but got", node)
	}
}

func expectNodesABC(t *testing.T, hashRing *HashRing) {
	// Python hash_ring module test case
	expectNode(t, hashRing, "test", "a")
	expectNode(t, hashRing, "test", "a")
	expectNode(t, hashRing, "test1", "b")
	expectNode(t, hashRing, "test2", "b")
	expectNode(t, hashRing, "test3", "c")
	expectNode(t, hashRing, "test4", "c")
	expectNode(t, hashRing, "test5", "a")
	expectNode(t, hashRing, "aaaa", "b")
	expectNode(t, hashRing, "bbbb", "a")
}

func expectNodesABCD(t *testing.T, hashRing *HashRing) {
	// Somehow adding d does not load balance these keys...
	expectNodesABC(t, hashRing)
}

func TestNewHashRing(t *testing.T) {
	nodes := []string{"a", "b", "c"}
	weights := make(map[string]int)
	hashRing := NewHashRing(nodes, weights)

	expectNodesABC(t, hashRing)
}

func TestNewHashRingWeighted(t *testing.T) {
	nodes := []string{"a", "b", "c"}
	weights := make(map[string]int)
	weights["b"] = 2
	hashRing := NewHashRing(nodes, weights)

	expectNode(t, hashRing, "test", "b")
	expectNode(t, hashRing, "test", "b")
	expectNode(t, hashRing, "test1", "b")
	expectNode(t, hashRing, "test2", "b")
	expectNode(t, hashRing, "test3", "c")
	expectNode(t, hashRing, "test4", "b")
	expectNode(t, hashRing, "test5", "b")
	expectNode(t, hashRing, "aaaa", "b")
	expectNode(t, hashRing, "bbbb", "a")
}

func TestRemoveNode(t *testing.T) {
	nodes := []string{"a", "b", "c"}
	weights := make(map[string]int)
	hashRing := NewHashRing(nodes, weights)
	hashRing.RemoveNode("b")

	expectNode(t, hashRing, "test", "a")
	expectNode(t, hashRing, "test", "a")
	expectNode(t, hashRing, "test1", "c") // Migrated to c from b
	expectNode(t, hashRing, "test2", "a") // Migrated to a from b
	expectNode(t, hashRing, "test3", "c")
	expectNode(t, hashRing, "test4", "c")
	expectNode(t, hashRing, "test5", "a")
	expectNode(t, hashRing, "aaaa", "a") // Migrated to a from b
	expectNode(t, hashRing, "bbbb", "a")
}

func TestAddNode(t *testing.T) {
	nodes := []string{"a", "c"}
	weights := make(map[string]int)
	hashRing := NewHashRing(nodes, weights)
	hashRing.AddNode("b")

	expectNodesABC(t, hashRing)
}

func TestAddNode2(t *testing.T) {
	nodes := []string{"a", "b", "c"}
	weights := make(map[string]int)
	hashRing := NewHashRing(nodes, weights)
	hashRing.AddNode("d")

	// Somehow adding d does not load balance these keys...
	expectNodesABCD(t, hashRing)

	hashRing.AddNode("e")

	expectNode(t, hashRing, "test", "a")
	expectNode(t, hashRing, "test", "a")
	expectNode(t, hashRing, "test1", "b")
	expectNode(t, hashRing, "test2", "b")
	expectNode(t, hashRing, "test3", "c")
	expectNode(t, hashRing, "test4", "c")
	expectNode(t, hashRing, "test5", "a")
	expectNode(t, hashRing, "aaaa", "b")
	expectNode(t, hashRing, "bbbb", "e") // Migrated to e from a

	hashRing.AddNode("f")

	expectNode(t, hashRing, "test", "a")
	expectNode(t, hashRing, "test", "a")
	expectNode(t, hashRing, "test1", "b")
	expectNode(t, hashRing, "test2", "f") // Migrated to f from b
	expectNode(t, hashRing, "test3", "f") // Migrated to f from c
	expectNode(t, hashRing, "test4", "c")
	expectNode(t, hashRing, "test5", "f") // Migrated to f from a
	expectNode(t, hashRing, "aaaa", "b")
	expectNode(t, hashRing, "bbbb", "e")
}

func TestRemoveAddNode(t *testing.T) {
	nodes := []string{"a", "b", "c"}
	weights := make(map[string]int)
	hashRing := NewHashRing(nodes, weights)

	expectNodesABC(t, hashRing)

	hashRing.RemoveNode("b")

	expectNode(t, hashRing, "test", "a")
	expectNode(t, hashRing, "test", "a")
	expectNode(t, hashRing, "test1", "c") // Migrated to c from b
	expectNode(t, hashRing, "test2", "a") // Migrated to a from b
	expectNode(t, hashRing, "test3", "c")
	expectNode(t, hashRing, "test4", "c")
	expectNode(t, hashRing, "test5", "a")
	expectNode(t, hashRing, "aaaa", "a") // Migrated to a from b
	expectNode(t, hashRing, "bbbb", "a")

	hashRing.AddNode("b")

	expectNodesABC(t, hashRing)
}

func TestAddRemoveNode(t *testing.T) {
	nodes := []string{"a", "b", "c"}
	weights := make(map[string]int)
	hashRing := NewHashRing(nodes, weights)
	hashRing.AddNode("d")

	// Somehow adding d does not load balance these keys...
	expectNodesABCD(t, hashRing)

	hashRing.AddNode("e")

	expectNode(t, hashRing, "test", "a")
	expectNode(t, hashRing, "test", "a")
	expectNode(t, hashRing, "test1", "b")
	expectNode(t, hashRing, "test2", "b")
	expectNode(t, hashRing, "test3", "c")
	expectNode(t, hashRing, "test4", "c")
	expectNode(t, hashRing, "test5", "a")
	expectNode(t, hashRing, "aaaa", "b")
	expectNode(t, hashRing, "bbbb", "e") // Migrated to e from a

	hashRing.AddNode("f")

	expectNode(t, hashRing, "test", "a")
	expectNode(t, hashRing, "test", "a")
	expectNode(t, hashRing, "test1", "b")
	expectNode(t, hashRing, "test2", "f") // Migrated to f from b
	expectNode(t, hashRing, "test3", "f") // Migrated to f from c
	expectNode(t, hashRing, "test4", "c")
	expectNode(t, hashRing, "test5", "f") // Migrated to f from a
	expectNode(t, hashRing, "aaaa", "b")
	expectNode(t, hashRing, "bbbb", "e")

	hashRing.RemoveNode("e")

	expectNode(t, hashRing, "test", "a")
	expectNode(t, hashRing, "test", "a")
	expectNode(t, hashRing, "test1", "b")
	expectNode(t, hashRing, "test2", "f")
	expectNode(t, hashRing, "test3", "f")
	expectNode(t, hashRing, "test4", "c")
	expectNode(t, hashRing, "test5", "f")
	expectNode(t, hashRing, "aaaa", "b")
	expectNode(t, hashRing, "bbbb", "f") // Migrated to f from e

	hashRing.RemoveNode("f")

	expectNodesABCD(t, hashRing)

	hashRing.RemoveNode("d")

	expectNodesABC(t, hashRing)
}
