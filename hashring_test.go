package hashring

import (
	"reflect"
	"testing"

	"github.com/stretchr/testify/assert"
)

func expectNodes(t *testing.T, ring *HashRing, key string, expectedNodes []string) {
	nodes, ok := ring.GetNodes(key, 2)
	sliceEquality := reflect.DeepEqual(nodes, expectedNodes)
	if !ok || !sliceEquality {
		t.Error("GetNodes(", key, ") expected", expectedNodes, "but got", nodes)
	}
}

func expectWeights(t *testing.T, ring *HashRing, expectedWeights map[string]int) {
	weightsEquality := reflect.DeepEqual(ring.weights, expectedWeights)
	if !weightsEquality {
		t.Error("Weights expected", expectedWeights, "but got", ring.weights)
	}
}

type testPair struct {
	key  string
	node string
}

func assertNodes(t *testing.T, prefix string, ring *HashRing, data []testPair) {
	for _, pair := range data {
		t.Run(prefix+pair.key, func(t *testing.T) {
			node, ok := ring.GetNode(pair.key)
			assert.True(t, ok)
			assert.Equal(t, pair.node, node)
		})
	}
}

func expectNodesABC(t *testing.T, prefix string, ring *HashRing) {
	// Python hash_ring module test case
	assertNodes(t, prefix, ring, []testPair{
		{"test", "a"},
		{"test", "a"},
		{"test1", "b"},
		{"test2", "b"},
		{"test3", "c"},
		{"test4", "a"},
		{"test5", "c"},
		{"aaaa", "c"},
		{"bbbb", "a"},
	})
}

func expectNodeRangesABC(t *testing.T, ring *HashRing) {
	expectNodes(t, ring, "test", []string{"a", "c"})
	expectNodes(t, ring, "test", []string{"a", "c"})
	expectNodes(t, ring, "test1", []string{"b", "a"})
	expectNodes(t, ring, "test2", []string{"b", "a"})
	expectNodes(t, ring, "test3", []string{"c", "b"})
	expectNodes(t, ring, "test4", []string{"a", "c"})
	expectNodes(t, ring, "test5", []string{"c", "b"})
	expectNodes(t, ring, "aaaa", []string{"c", "b"})
	expectNodes(t, ring, "bbbb", []string{"a", "c"})
}

func expectNodesABCD(t *testing.T, prefix string, ring *HashRing) {
	assertNodes(t, prefix, ring, []testPair{
		{"test", "d"},
		{"test", "d"},
		{"test1", "b"},
		{"test2", "b"},
		{"test3", "c"},
		{"test4", "d"},
		{"test5", "c"},
		{"aaaa", "c"},
		{"bbbb", "d"},
	})
}

func TestNew(t *testing.T) {
	nodes := []string{"a", "b", "c"}
	ring := New(nodes)

	expectNodesABC(t, "TestNew_1_", ring)
	expectNodeRangesABC(t, ring)
}

func TestNewEmpty(t *testing.T) {
	nodes := []string{}
	ring := New(nodes)

	node, ok := ring.GetNode("test")
	if ok || node != "" {
		t.Error("GetNode(test) expected (\"\", false) but got (", node, ",", ok, ")")
	}

	nodes, rok := ring.GetNodes("test", 2)
	if rok || !(len(nodes) == 0) {
		t.Error("GetNode(test) expected ( [], false ) but got (", nodes, ",", rok, ")")
	}
}

func TestForMoreNodes(t *testing.T) {
	nodes := []string{"a", "b", "c"}
	ring := New(nodes)

	nodes, ok := ring.GetNodes("test", 5)
	if ok || !(len(nodes) == 0) {
		t.Error("GetNode(test) expected ( [], false ) but got (", nodes, ",", ok, ")")
	}
}

func TestForEqualNodes(t *testing.T) {
	nodes := []string{"a", "b", "c"}
	ring := New(nodes)

	nodes, ok := ring.GetNodes("test", 3)
	if !ok && (len(nodes) == 3) {
		t.Error("GetNode(test) expected ( [a b c], true ) but got (", nodes, ",", ok, ")")
	}
}

func TestNewSingle(t *testing.T) {
	nodes := []string{"a"}
	ring := New(nodes)

	assertNodes(t, "", ring, []testPair{
		{"test", "a"},
		{"test", "a"},
		{"test1", "a"},
		{"test2", "a"},
		{"test3", "a"},

		// This triggers the edge case where sortedKey search resulting in not found
		{"test14", "a"},

		{"test15", "a"},
		{"test16", "a"},
		{"test17", "a"},
		{"test18", "a"},
		{"test19", "a"},
		{"test20", "a"},
	})
}

func TestNewWeighted(t *testing.T) {
	weights := make(map[string]int)
	weights["a"] = 1
	weights["b"] = 2
	weights["c"] = 1
	ring := NewWithWeights(weights)

	assertNodes(t, "", ring, []testPair{
		{"test", "b"},
		{"test", "b"},
		{"test1", "b"},
		{"test2", "b"},
		{"test3", "c"},
		{"test4", "b"},
		{"test5", "c"},
		{"aaaa", "c"},
		{"bbbb", "b"},
	})

	expectNodes(t, ring, "test", []string{"b", "a"})
}

func TestRemoveNode(t *testing.T) {
	nodes := []string{"a", "b", "c"}
	ring := New(nodes)
	ring = ring.RemoveNode("b")

	assertNodes(t, "", ring, []testPair{
		{"test", "a"},
		{"test", "a"},
		{"test1", "a"}, // Migrated to c from b
		{"test2", "a"}, // Migrated to a from b
		{"test3", "c"},
		{"test4", "a"},
		{"test5", "c"},
		{"aaaa", "c"}, // Migrated to a from b
		{"bbbb", "a"},
	})

	expectNodes(t, ring, "test", []string{"a", "c"})
}

func TestAddNode(t *testing.T) {
	nodes := []string{"a", "c"}
	ring := New(nodes)
	ring = ring.AddNode("b")

	expectNodesABC(t, "TestAddNode_1_", ring)

	defaultWeights := map[string]int{
		"a": 1,
		"b": 1,
		"c": 1,
	}
	expectWeights(t, ring, defaultWeights)
}

func TestAddNode2(t *testing.T) {
	nodes := []string{"a", "c"}
	ring := New(nodes)
	ring = ring.AddNode("b")
	ring = ring.AddNode("b")

	expectNodesABC(t, "TestAddNode2_", ring)
	expectNodeRangesABC(t, ring)
}

func TestAddNode3(t *testing.T) {
	nodes := []string{"a", "b", "c"}
	ring := New(nodes)
	ring = ring.AddNode("d")

	expectNodesABCD(t, "TestAddNode3_1_", ring)

	ring = ring.AddNode("e")

	assertNodes(t, "TestAddNode3_2_", ring, []testPair{
		{"test", "d"},
		{"test", "d"},
		{"test1", "b"},
		{"test2", "e"},
		{"test3", "c"},
		{"test4", "d"},
		{"test5", "c"},
		{"aaaa", "c"},
		{"bbbb", "d"},
	})

	expectNodes(t, ring, "test", []string{"d", "a"})

	ring = ring.AddNode("f")

	assertNodes(t, "TestAddNode3_3_", ring, []testPair{
		{"test", "d"},
		{"test", "d"},
		{"test1", "b"},
		{"test2", "e"}, // Migrated to f from b
		{"test3", "c"}, // Migrated to f from c
		{"test4", "d"},
		{"test5", "c"}, // Migrated to f from a
		{"aaaa", "c"},
		{"bbbb", "d"},
	})

	expectNodes(t, ring, "test", []string{"d", "a"})
}

func TestDuplicateNodes(t *testing.T) {
	nodes := []string{"a", "a", "a", "a", "b"}
	ring := New(nodes)

	assertNodes(t, "TestDuplicateNodes_", ring, []testPair{
		{"test", "a"},
		{"test", "a"},
		{"test1", "b"},
		{"test2", "b"},
		{"test3", "b"},
		{"test4", "a"},
		{"test5", "b"},
		{"aaaa", "b"},
		{"bbbb", "a"},
	})
}

func TestAddWeightedNode(t *testing.T) {
	nodes := []string{"a", "c"}
	ring := New(nodes)
	ring = ring.AddWeightedNode("b", 0)
	ring = ring.AddWeightedNode("b", 2)
	ring = ring.AddWeightedNode("b", 2)

	assertNodes(t, "TestAddWeightedNode_", ring, []testPair{
		{"test", "b"},
		{"test", "b"},
		{"test1", "b"},
		{"test2", "b"},
		{"test3", "c"},
		{"test4", "b"},
		{"test5", "c"},
		{"aaaa", "c"},
		{"bbbb", "b"},
	})

	expectNodes(t, ring, "test", []string{"b", "a"})
}

func TestUpdateWeightedNode(t *testing.T) {
	nodes := []string{"a", "c"}
	ring := New(nodes)
	ring = ring.AddWeightedNode("b", 1)
	ring = ring.UpdateWeightedNode("b", 2)
	ring = ring.UpdateWeightedNode("b", 2)
	ring = ring.UpdateWeightedNode("b", 0)
	ring = ring.UpdateWeightedNode("d", 2)

	assertNodes(t, "TestUpdateWeightedNode_", ring, []testPair{
		{"test", "b"},
		{"test", "b"},
		{"test1", "b"},
		{"test2", "b"},
		{"test3", "c"},
		{"test4", "b"},
		{"test5", "c"},
		{"aaaa", "c"},
		{"bbbb", "b"},
	})

	expectNodes(t, ring, "test", []string{"b", "a"})
}

func TestRemoveAddNode(t *testing.T) {
	nodes := []string{"a", "b", "c"}
	ring := New(nodes)

	expectNodesABC(t, "TestRemoveAddNode_1_", ring)
	expectNodeRangesABC(t, ring)

	ring = ring.RemoveNode("b")

	assertNodes(t, "TestRemoveAddNode_2_", ring, []testPair{
		{"test", "a"},
		{"test", "a"},
		{"test1", "a"}, // Migrated to c from b
		{"test2", "a"}, // Migrated to a from b
		{"test3", "c"},
		{"test4", "a"},
		{"test5", "c"},
		{"aaaa", "c"}, // Migrated to a from b
		{"bbbb", "a"},
	})

	expectNodes(t, ring, "test", []string{"a", "c"})
	expectNodes(t, ring, "test", []string{"a", "c"})
	expectNodes(t, ring, "test1", []string{"c", "a"})
	expectNodes(t, ring, "test2", []string{"a", "c"})
	expectNodes(t, ring, "test3", []string{"c", "a"})
	expectNodes(t, ring, "test4", []string{"c", "a"})
	expectNodes(t, ring, "test5", []string{"a", "c"})
	expectNodes(t, ring, "aaaa", []string{"a", "c"})
	expectNodes(t, ring, "bbbb", []string{"a", "c"})

	ring = ring.AddNode("b")

	expectNodesABC(t, "TestRemoveAddNode_3_", ring)
	expectNodeRangesABC(t, ring)
}

func TestRemoveAddWeightedNode(t *testing.T) {
	weights := make(map[string]int)
	weights["a"] = 1
	weights["b"] = 2
	weights["c"] = 1
	ring := NewWithWeights(weights)

	expectWeights(t, ring, weights)

	assertNodes(t, "TestRemoveAddWeightedNode_1_", ring, []testPair{
		{"test", "b"},
		{"test", "b"},
		{"test1", "b"},
		{"test2", "b"},
		{"test3", "c"},
		{"test4", "b"},
		{"test5", "c"},
		{"aaaa", "c"},
		{"bbbb", "b"},
	})

	expectNodes(t, ring, "test", []string{"b", "a"})
	expectNodes(t, ring, "test", []string{"b", "a"})
	expectNodes(t, ring, "test1", []string{"b", "c"})
	expectNodes(t, ring, "test2", []string{"b", "a"})
	expectNodes(t, ring, "test3", []string{"c", "b"})
	expectNodes(t, ring, "test4", []string{"b", "a"})
	expectNodes(t, ring, "test5", []string{"b", "a"})
	expectNodes(t, ring, "aaaa", []string{"b", "a"})
	expectNodes(t, ring, "bbbb", []string{"a", "b"})

	ring = ring.RemoveNode("c")

	delete(weights, "c")
	expectWeights(t, ring, weights)

	assertNodes(t, "TestRemoveAddWeightedNode_2_", ring, []testPair{
		{"test", "b"},
		{"test", "b"},
		{"test1", "b"},
		{"test2", "b"},
		{"test3", "b"}, // Migrated to b from c
		{"test4", "b"},
		{"test5", "b"},
		{"aaaa", "b"},
		{"bbbb", "a"},
	})

	expectNodes(t, ring, "test", []string{"b", "a"})
	expectNodes(t, ring, "test", []string{"b", "a"})
	expectNodes(t, ring, "test1", []string{"b", "a"})
	expectNodes(t, ring, "test2", []string{"b", "a"})
	expectNodes(t, ring, "test3", []string{"b", "a"})
	expectNodes(t, ring, "test4", []string{"b", "a"})
	expectNodes(t, ring, "test5", []string{"b", "a"})
	expectNodes(t, ring, "aaaa", []string{"b", "a"})
	expectNodes(t, ring, "bbbb", []string{"a", "b"})
}

func TestAddRemoveNode(t *testing.T) {
	nodes := []string{"a", "b", "c"}
	ring := New(nodes)
	ring = ring.AddNode("d")

	// Somehow adding d does not load balance these keys...
	expectNodesABCD(t, "TestAddRemoveNode_1_", ring)

	expectNodes(t, ring, "test", []string{"a", "b"})
	expectNodes(t, ring, "test", []string{"a", "b"})
	expectNodes(t, ring, "test1", []string{"b", "d"})
	expectNodes(t, ring, "test2", []string{"b", "d"})
	expectNodes(t, ring, "test3", []string{"c", "d"})
	expectNodes(t, ring, "test4", []string{"c", "b"})
	expectNodes(t, ring, "test5", []string{"a", "d"})
	expectNodes(t, ring, "aaaa", []string{"b", "a"})
	expectNodes(t, ring, "bbbb", []string{"a", "b"})

	ring = ring.AddNode("e")

	assertNodes(t, "TestAddRemoveNode_2_", ring, []testPair{
		{"test", "a"},
		{"test", "a"},
		{"test1", "b"},
		{"test2", "b"},
		{"test3", "c"},
		{"test4", "c"},
		{"test5", "a"},
		{"aaaa", "b"},
		{"bbbb", "e"}, // Migrated to e from a
	})

	expectNodes(t, ring, "test", []string{"a", "b"})
	expectNodes(t, ring, "test", []string{"a", "b"})
	expectNodes(t, ring, "test1", []string{"b", "d"})
	expectNodes(t, ring, "test2", []string{"b", "d"})
	expectNodes(t, ring, "test3", []string{"c", "e"})
	expectNodes(t, ring, "test4", []string{"c", "b"})
	expectNodes(t, ring, "test5", []string{"a", "e"})
	expectNodes(t, ring, "aaaa", []string{"b", "e"})
	expectNodes(t, ring, "bbbb", []string{"e", "a"})

	ring = ring.AddNode("f")

	assertNodes(t, "TestAddRemoveNode_3_", ring, []testPair{
		{"test", "a"},
		{"test", "a"},
		{"test1", "b"},
		{"test2", "f"}, // Migrated to f from b
		{"test3", "f"}, // Migrated to f from c
		{"test4", "c"},
		{"test5", "f"}, // Migrated to f from a
		{"aaaa", "b"},
		{"bbbb", "e"},
	})

	expectNodes(t, ring, "test", []string{"a", "b"})
	expectNodes(t, ring, "test", []string{"a", "b"})
	expectNodes(t, ring, "test1", []string{"b", "d"})
	expectNodes(t, ring, "test2", []string{"f", "b"})
	expectNodes(t, ring, "test3", []string{"f", "c"})
	expectNodes(t, ring, "test4", []string{"c", "b"})
	expectNodes(t, ring, "test5", []string{"f", "a"})
	expectNodes(t, ring, "aaaa", []string{"b", "e"})
	expectNodes(t, ring, "bbbb", []string{"e", "f"})

	ring = ring.RemoveNode("e")

	assertNodes(t, "TestAddRemoveNode_4_", ring, []testPair{
		{"test", "a"},
		{"test", "a"},
		{"test1", "b"},
		{"test2", "f"},
		{"test3", "f"},
		{"test4", "c"},
		{"test5", "f"},
		{"aaaa", "b"},
		{"bbbb", "f"}, // Migrated to f from e
	})

	expectNodes(t, ring, "test", []string{"a", "b"})
	expectNodes(t, ring, "test", []string{"a", "b"})
	expectNodes(t, ring, "test1", []string{"b", "d"})
	expectNodes(t, ring, "test2", []string{"f", "b"})
	expectNodes(t, ring, "test3", []string{"f", "c"})
	expectNodes(t, ring, "test4", []string{"c", "b"})
	expectNodes(t, ring, "test5", []string{"f", "a"})
	expectNodes(t, ring, "aaaa", []string{"b", "a"})
	expectNodes(t, ring, "bbbb", []string{"f", "a"})

	ring = ring.RemoveNode("f")

	expectNodesABCD(t, "TestAddRemoveNode_5_", ring)

	expectNodes(t, ring, "test", []string{"a", "b"})
	expectNodes(t, ring, "test", []string{"a", "b"})
	expectNodes(t, ring, "test1", []string{"b", "d"})
	expectNodes(t, ring, "test2", []string{"b", "d"})
	expectNodes(t, ring, "test3", []string{"c", "d"})
	expectNodes(t, ring, "test4", []string{"c", "b"})
	expectNodes(t, ring, "test5", []string{"a", "d"})
	expectNodes(t, ring, "aaaa", []string{"b", "a"})
	expectNodes(t, ring, "bbbb", []string{"a", "b"})

	ring = ring.RemoveNode("d")

	expectNodesABC(t, "TestAddRemoveNode_6_", ring)
	expectNodeRangesABC(t, ring)
}

func BenchmarkHashes(b *testing.B) {
	nodes := []string{"a", "b", "c", "d", "e", "f", "g"}
	ring := New(nodes)
	tt := []struct {
		key   string
		nodes []string
	}{
		{"test", []string{"a", "b"}},
		{"test", []string{"a", "b"}},
		{"test1", []string{"b", "d"}},
		{"test2", []string{"f", "b"}},
		{"test3", []string{"f", "c"}},
		{"test4", []string{"c", "b"}},
		{"test5", []string{"f", "a"}},
		{"aaaa", []string{"b", "a"}},
		{"bbbb", []string{"f", "a"}},
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		o := tt[i%len(tt)]
		ring.GetNodes(o.key, 2)
	}
}

func BenchmarkHashesSingle(b *testing.B) {
	nodes := []string{"a", "b", "c", "d", "e", "f", "g"}
	ring := New(nodes)
	tt := []struct {
		key   string
		nodes []string
	}{
		{"test", []string{"a", "b"}},
		{"test", []string{"a", "b"}},
		{"test1", []string{"b", "d"}},
		{"test2", []string{"f", "b"}},
		{"test3", []string{"f", "c"}},
		{"test4", []string{"c", "b"}},
		{"test5", []string{"f", "a"}},
		{"aaaa", []string{"b", "a"}},
		{"bbbb", []string{"f", "a"}},
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		o := tt[i%len(tt)]
		ring.GetNode(o.key)
	}
}

func BenchmarkNew(b *testing.B) {
	nodes := []string{"a", "b", "c", "d", "e", "f", "g"}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = New(nodes)
	}
}
