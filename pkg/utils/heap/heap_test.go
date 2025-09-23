package heap_test

import (
	"container/heap"
	"fmt"
	"slices"
	"testing"

	"github.com/stretchr/testify/require"
	"golang.org/x/exp/constraints"

	hp "github.com/hankgalt/batch-orchestra/pkg/utils/heap"
)

func TopKMin[T constraints.Ordered](h []T, k int) []T {
	if len(h) == 0 || k <= 0 {
		return h
	}

	maxHeap := &hp.MaxHeap[T]{}
	heap.Init(maxHeap)

	for _, v := range h {
		heap.Push(maxHeap, v)
		if maxHeap.Len() > k {
			popped := heap.Pop(maxHeap)
			fmt.Println("Popped:", popped)
		}
	}

	out := make([]T, maxHeap.Len())
	for i := range out {
		out[i] = heap.Pop(maxHeap).(T)
	}
	slices.Sort(out)

	return out
}

func TestTopKMin(t *testing.T) {
	nums := []int{5, 8, 1, 2, 7, 4, 6}
	topKMin := TopKMin(nums, 3)
	require.Equal(t, []int{1, 2, 4}, topKMin)
}

func TestMinHeap(t *testing.T) {
	minHeap := &hp.MinHeap[int]{}
	heap.Init(minHeap)

	heap.Push(minHeap, 5)
	heap.Push(minHeap, 2)
	heap.Push(minHeap, 8)

	require.Equal(t, 3, minHeap.Len())

	require.Equal(t, 2, heap.Pop(minHeap).(int))
	require.Equal(t, 5, heap.Pop(minHeap).(int))
	require.Equal(t, 8, heap.Pop(minHeap).(int))

	require.Equal(t, 0, minHeap.Len())
}

func TestMaxHeap(t *testing.T) {
	maxHeap := &hp.MaxHeap[int]{}
	heap.Init(maxHeap)

	heap.Push(maxHeap, 5)
	heap.Push(maxHeap, 2)
	heap.Push(maxHeap, 8)

	require.Equal(t, 3, maxHeap.Len())

	require.Equal(t, 8, heap.Pop(maxHeap).(int))
	require.Equal(t, 5, heap.Pop(maxHeap).(int))
	require.Equal(t, 2, heap.Pop(maxHeap).(int))

	require.Equal(t, 0, maxHeap.Len())
}
