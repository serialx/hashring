package hashring

import (
	"crypto/md5"
	"fmt"
	"math"
	"sort"
	"sync"

	"github.com/1046102779/slicelement"
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
	mtx        sync.Mutex
}

func New(nodes []string) *HashRing {
	// initial weight value is 1
	weights := make(map[string]int)
	for _, node := range nodes {
		weights[node] = 1
	}
	hashRing := &HashRing{
		ring:       make(map[HashKey]string),
		sortedKeys: make([]HashKey, 0),
		nodes:      nodes,
		weights:    weights,
	}
	hashRing.generateCircle()
	return hashRing
}

func NewWithWeights(weights map[string]int) *HashRing {
	nodes := make([]string, 0, len(weights))
	for node, weight := range weights {
		nodes = append(nodes, node)
		if weight <= 0 {
			weights[node] = 1
		}
	}
	hashRing := &HashRing{
		ring:       make(map[HashKey]string),
		sortedKeys: make([]HashKey, 0),
		nodes:      nodes,
		weights:    weights,
	}
	hashRing.generateCircle()
	return hashRing
}

func (h *HashRing) Size() int {
	return len(h.nodes)
}

func (h *HashRing) UpdateWithWeights(weights map[string]int) *HashRing {
	nodesChgFlg := false
	for node, newWeight := range weights {
		oldWeight, ok := h.weights[node]
		if !ok || oldWeight != newWeight {
			nodesChgFlg = true
			break
		}
	}

	if !nodesChgFlg {
		return h
	}
	return NewWithWeights(weights)
}

func (h *HashRing) generateCircle() {
	var totalWeight int
	for _, node := range h.nodes {
		totalWeight += h.weights[node]
	}

	for _, node := range h.nodes {
		factor := math.Floor(float64(40*len(h.nodes)*h.weights[node]) / float64(totalWeight))
		for j := 0; j < int(factor); j++ {
			nodeKey := fmt.Sprintf("%s-%d", node, j)
			bKey := hashDigest(nodeKey)

			for i := 0; i < 3; i++ {
				key := hashVal(bKey[i*4 : i*4+4])
				h.ring[key] = node
				h.sortedKeys = append(h.sortedKeys, key)
			}
		}
	}
	sort.Sort(HashKeyOrder(h.sortedKeys))
}

func (h *HashRing) GetNode(key string) (node string, ok bool) {
	var pos int
	if pos, ok = h.GetNodePos(key); !ok {
		return
	}
	return h.ring[h.sortedKeys[pos]], true
}

func (h *HashRing) GetNodePos(key string) (pos int, ok bool) {
	if len(h.ring) == 0 {
		return
	}

	hashKey := h.GenKey(key)
	nodes := h.sortedKeys
	pos = sort.Search(len(nodes), func(i int) bool { return nodes[i] > hashKey })

	if pos == len(nodes) {
		// Wrap the search, should return first node
		return 0, true
	}
	return pos, true
}

func (h *HashRing) GenKey(key string) HashKey {
	bKey := hashDigest(key)
	return hashVal(bKey[0:4])

}

func (h *HashRing) GetNodes(key string, size int) (nodes []string, ok bool) {
	if size > len(h.nodes) {
		return
	}

	var pos int
	if pos, ok = h.GetNodePos(key); !ok {
		return
	}

	flags := make(map[string]bool)
	for len(nodes) != size {
		val := h.ring[h.sortedKeys[pos%len(h.sortedKeys)]]
		if !flags[val] {
			flags[val] = true
			nodes = append(nodes, val)
		}
		pos++
	}

	return nodes, true
}

func (h *HashRing) AddNode(node string) *HashRing {
	return h.AddWeightedNode(node, 1)
}

func (h *HashRing) AddWeightedNode(node string, weight int) *HashRing {
	if weight <= 0 || len(h.nodes) <= 0 {
		return h
	}
	if isExist, err := slicelement.Contains(h.nodes, node, ""); err != nil || isExist {
		return h
	}

	h.mtx.Lock()
	defer h.mtx.Unlock()
	h.weights[node] = weight
	h = &HashRing{
		ring:       make(map[HashKey]string),
		sortedKeys: []HashKey{},
		nodes:      append(h.nodes, node),
		weights:    h.weights,
	}
	h.generateCircle()
	return h
}

func (h *HashRing) UpdateWeightedNode(node string, weight int) *HashRing {
	if weight <= 0 || len(node) <= 0 {
		return h
	}

	/* node is not need to update for node is not existed or weight is not changed */
	if oldWeight, ok := h.weights[node]; (!ok) || (ok && oldWeight == weight) {
		return h
	}

	h.mtx.Lock()
	defer h.mtx.Unlock()
	h.weights[node] = weight
	h = &HashRing{
		ring:       make(map[HashKey]string),
		sortedKeys: []HashKey{},
		nodes:      h.nodes,
		weights:    h.weights,
	}
	h.generateCircle()
	return h
}

func (h *HashRing) RemoveNode(node string) *HashRing {
	var (
		index int
		err   error
	)
	/* if node isn't exist in hashring, don't refresh hashring */
	if index, err = slicelement.GetIndex(h.nodes, node, ""); err != nil || index < 0 {
		return h
	}
	h.mtx.Lock()
	defer h.mtx.Unlock()
	delete(h.weights, node)
	h.nodes = append(h.nodes[:index], h.nodes[index+1:]...)
	h = &HashRing{
		ring:       make(map[HashKey]string),
		sortedKeys: make([]HashKey, 0),
		nodes:      h.nodes,
		weights:    h.weights,
	}
	h.generateCircle()
	return h
}

func hashVal(bKey []byte) HashKey {
	return ((HashKey(bKey[3]) << 24) |
		(HashKey(bKey[2]) << 16) |
		(HashKey(bKey[1]) << 8) |
		(HashKey(bKey[0])))
}

func hashDigest(key string) [md5.Size]byte {
	return md5.Sum([]byte(key))
}
