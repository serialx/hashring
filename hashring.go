package hashring

import (
	"crypto/md5"
	"fmt"
	"math"
	"sort"
)

type HashKey uint32

type HashKeyOrder []HashKey

func (h HashKeyOrder) Len() int           { return len(h) }
func (h HashKeyOrder) Swap(i, j int)      { h[i], h[j] = h[j], h[i] }
func (h HashKeyOrder) Less(i, j int) bool { return h[i] < h[j] }

type HashRing struct {
	ring       map[HashKey]string
	sortedKeys []HashKey
	nodes      []string
	weights    map[string]int
}

func NewHashRing(nodes []string, weights map[string]int) *HashRing {
	hashRing := &HashRing{
		ring:       make(map[HashKey]string),
		sortedKeys: make([]HashKey, 0),
		nodes:      nodes,
		weights:    weights,
	}
	hashRing.generateCircle()
	return hashRing
}

func hashVal(bKey []byte, entryFn func(int) int) HashKey {
	return ((HashKey(bKey[entryFn(3)]) << 24) |
		(HashKey(bKey[entryFn(2)]) << 16) |
		(HashKey(bKey[entryFn(1)]) << 8) |
		(HashKey(bKey[entryFn(0)])))
}

func hashDigest(key string) []byte {
	m := md5.New()
	m.Write([]byte(key))
	return m.Sum(nil)
}

func (h *HashRing) generateCircle() {
	totalWeight := 0
	for _, node := range h.nodes {
		if weight, ok := h.weights[node]; ok {
			totalWeight += weight
		} else {
			totalWeight += 1
		}
	}

	for _, node := range h.nodes {
		weight := 1

		if _, ok := h.weights[node]; ok {
			weight = h.weights[node]
		}

		factor := math.Floor(float64(40*len(h.nodes)*weight) / float64(totalWeight))

		for j := 0; j < int(factor); j++ {
			nodeKey := fmt.Sprintf("%s-%d", node, j)
			bKey := hashDigest(nodeKey)

			for i := 0; i < 3; i++ {
				key := hashVal(bKey, func(x int) int { return x + i*4 })
				h.ring[key] = node
				h.sortedKeys = append(h.sortedKeys, key)
			}
		}
	}

	sort.Sort(HashKeyOrder(h.sortedKeys))
}

func (h *HashRing) GetNode(stringKey string) (node string, ok bool) {
	pos, ok := h.GetNodePos(stringKey)
	if !ok {
		return "", false
	}
	return h.ring[h.sortedKeys[pos]], true
}

func (h *HashRing) GetNodePos(stringKey string) (pos int, ok bool) {
	if len(h.ring) == 0 {
		return 0, false
	}

	key := h.GenKey(stringKey)

	nodes := h.sortedKeys
	pos = sort.Search(len(nodes), func(i int) bool { return nodes[i] > key })

	if pos == len(nodes) {
		return 0, false
	} else {
		return pos, true
	}
}

func (h *HashRing) GenKey(key string) HashKey {
	bKey := hashDigest(key)
	return hashVal(bKey, func(x int) int { return x })
}

func (h *HashRing) AddNode(node string) *HashRing {
	for _, eNode := range h.nodes {
		if eNode == node {
			return h
		}
	}

	h.ring = make(map[HashKey]string)
	h.sortedKeys = make([]HashKey, 0)
	h.nodes = append(h.nodes, node)
	h.generateCircle()
	return h
}

func (h *HashRing) RemoveNode(node string) *HashRing {
	nodes := make([]string, 0)
	for _, eNode := range h.nodes {
		if eNode != node {
			nodes = append(nodes, eNode)
		}
	}

	weights := make(map[string]int)
	for eNode, eWeight := range h.weights {
		if eNode != node {
			weights[eNode] = eWeight
		}
	}

	h.ring = make(map[HashKey]string)
	h.sortedKeys = make([]HashKey, 0)
	h.nodes = nodes
	h.weights = weights
	h.generateCircle()
	return h
}
