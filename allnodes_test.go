package hashring

import (
	"fmt"
	"sort"
	"testing"

	"github.com/stretchr/testify/assert"
)

func generate(n int) map[string]int {
	result := make(map[string]int)
	for i := 0; i < n; i++ {
		result[fmt.Sprintf("%03d", i)] = 1
	}
	return result
}

func TestIterateAllNodes(t *testing.T) {
	weights := generate(1000)
	ring := NewWithWeights(weights)
	nodes, ok := ring.GetNodes("1", ring.Size())
	assert.True(t, ok)
	if !assert.Equal(t, ring.Size(), len(nodes)) {
		sort.Strings(nodes)
		fmt.Printf("%v\n", nodes)
	}
}
