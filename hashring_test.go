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

func TestNewHashRingEmpty(t *testing.T) {
	nodes := []string{}
	weights := make(map[string]int)
	hashRing := NewHashRing(nodes, weights)

	node, ok := hashRing.GetNode("test")
	if ok || node != "" {
		t.Error("GetNode(test) expected (\"\", false) but got (", node, ",", ok, ")")
	}
}

func TestNewHashRingSingle(t *testing.T) {
	nodes := []string{"a"}
	weights := make(map[string]int)
	hashRing := NewHashRing(nodes, weights)

	expectNode(t, hashRing, "test", "a")
	expectNode(t, hashRing, "test", "a")
	expectNode(t, hashRing, "test1", "a")
	expectNode(t, hashRing, "test2", "a")
	expectNode(t, hashRing, "test3", "a")

	// This triggers the edge case where sortedKey search resulting in not found
	expectNode(t, hashRing, "test14", "a")

	expectNode(t, hashRing, "test15", "a")
	expectNode(t, hashRing, "test16", "a")
	expectNode(t, hashRing, "test17", "a")
	expectNode(t, hashRing, "test18", "a")
	expectNode(t, hashRing, "test19", "a")
	expectNode(t, hashRing, "test20", "a")
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
	hashRing = hashRing.RemoveNode("b")

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
	hashRing = hashRing.AddNode("b")

	expectNodesABC(t, hashRing)
}

func TestAddNode2(t *testing.T) {
	nodes := []string{"a", "c"}
	weights := make(map[string]int)
	hashRing := NewHashRing(nodes, weights)
	hashRing = hashRing.AddNode("b")
	hashRing = hashRing.AddNode("b")

	expectNodesABC(t, hashRing)
}

func TestAddNode3(t *testing.T) {
	nodes := []string{"a", "b", "c"}
	weights := make(map[string]int)
	hashRing := NewHashRing(nodes, weights)
	hashRing = hashRing.AddNode("d")

	// Somehow adding d does not load balance these keys...
	expectNodesABCD(t, hashRing)

	hashRing = hashRing.AddNode("e")

	expectNode(t, hashRing, "test", "a")
	expectNode(t, hashRing, "test", "a")
	expectNode(t, hashRing, "test1", "b")
	expectNode(t, hashRing, "test2", "b")
	expectNode(t, hashRing, "test3", "c")
	expectNode(t, hashRing, "test4", "c")
	expectNode(t, hashRing, "test5", "a")
	expectNode(t, hashRing, "aaaa", "b")
	expectNode(t, hashRing, "bbbb", "e") // Migrated to e from a

	hashRing = hashRing.AddNode("f")

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

func TestAddWeightedNode(t *testing.T) {
	nodes := []string{"a", "c"}
	weights := make(map[string]int)
	hashRing := NewHashRing(nodes, weights)
	hashRing = hashRing.AddWeightedNode("b", 0)
	hashRing = hashRing.AddWeightedNode("b", 2)
	hashRing = hashRing.AddWeightedNode("b", 2)

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

func TestRemoveAddNode(t *testing.T) {
	nodes := []string{"a", "b", "c"}
	weights := make(map[string]int)
	hashRing := NewHashRing(nodes, weights)

	expectNodesABC(t, hashRing)

	hashRing = hashRing.RemoveNode("b")

	expectNode(t, hashRing, "test", "a")
	expectNode(t, hashRing, "test", "a")
	expectNode(t, hashRing, "test1", "c") // Migrated to c from b
	expectNode(t, hashRing, "test2", "a") // Migrated to a from b
	expectNode(t, hashRing, "test3", "c")
	expectNode(t, hashRing, "test4", "c")
	expectNode(t, hashRing, "test5", "a")
	expectNode(t, hashRing, "aaaa", "a") // Migrated to a from b
	expectNode(t, hashRing, "bbbb", "a")

	hashRing = hashRing.AddNode("b")

	expectNodesABC(t, hashRing)
}

func TestRemoveAddWeightedNode(t *testing.T) {
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

	hashRing = hashRing.RemoveNode("c")

	expectNode(t, hashRing, "test", "b")
	expectNode(t, hashRing, "test", "b")
	expectNode(t, hashRing, "test1", "b")
	expectNode(t, hashRing, "test2", "b")
	expectNode(t, hashRing, "test3", "b") // Migrated to b from c
	expectNode(t, hashRing, "test4", "b")
	expectNode(t, hashRing, "test5", "b")
	expectNode(t, hashRing, "aaaa", "b")
	expectNode(t, hashRing, "bbbb", "a")
}

func TestAddRemoveNode(t *testing.T) {
	nodes := []string{"a", "b", "c"}
	weights := make(map[string]int)
	hashRing := NewHashRing(nodes, weights)
	hashRing = hashRing.AddNode("d")

	// Somehow adding d does not load balance these keys...
	expectNodesABCD(t, hashRing)

	hashRing = hashRing.AddNode("e")

	expectNode(t, hashRing, "test", "a")
	expectNode(t, hashRing, "test", "a")
	expectNode(t, hashRing, "test1", "b")
	expectNode(t, hashRing, "test2", "b")
	expectNode(t, hashRing, "test3", "c")
	expectNode(t, hashRing, "test4", "c")
	expectNode(t, hashRing, "test5", "a")
	expectNode(t, hashRing, "aaaa", "b")
	expectNode(t, hashRing, "bbbb", "e") // Migrated to e from a

	hashRing = hashRing.AddNode("f")

	expectNode(t, hashRing, "test", "a")
	expectNode(t, hashRing, "test", "a")
	expectNode(t, hashRing, "test1", "b")
	expectNode(t, hashRing, "test2", "f") // Migrated to f from b
	expectNode(t, hashRing, "test3", "f") // Migrated to f from c
	expectNode(t, hashRing, "test4", "c")
	expectNode(t, hashRing, "test5", "f") // Migrated to f from a
	expectNode(t, hashRing, "aaaa", "b")
	expectNode(t, hashRing, "bbbb", "e")

	hashRing = hashRing.RemoveNode("e")

	expectNode(t, hashRing, "test", "a")
	expectNode(t, hashRing, "test", "a")
	expectNode(t, hashRing, "test1", "b")
	expectNode(t, hashRing, "test2", "f")
	expectNode(t, hashRing, "test3", "f")
	expectNode(t, hashRing, "test4", "c")
	expectNode(t, hashRing, "test5", "f")
	expectNode(t, hashRing, "aaaa", "b")
	expectNode(t, hashRing, "bbbb", "f") // Migrated to f from e

	hashRing = hashRing.RemoveNode("f")

	expectNodesABCD(t, hashRing)

	hashRing = hashRing.RemoveNode("d")

	expectNodesABC(t, hashRing)
}
