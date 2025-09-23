package heap

import "golang.org/x/exp/constraints"

type MaxHeap[T constraints.Ordered] []T

func (h MaxHeap[T]) Len() int { return len(h) }
func (h MaxHeap[T]) Swap(i, j int) {
	h[i], h[j] = h[j], h[i]
}
func (h MaxHeap[T]) Less(i, j int) bool {
	return h[i] > h[j]
}
func (h *MaxHeap[T]) Push(x any) {
	*h = append(*h, x.(T))
}
func (h *MaxHeap[T]) Pop() any {
	old := *h
	n := len(old)
	x := old[n-1]
	*h = old[0 : n-1]
	return x
}

type MinHeap[T constraints.Ordered] []T

func (h MinHeap[T]) Len() int { return len(h) }
func (h MinHeap[T]) Swap(i, j int) {
	h[i], h[j] = h[j], h[i]
}
func (h MinHeap[T]) Less(i, j int) bool {
	return h[i] < h[j]
}
func (h *MinHeap[T]) Push(x any) {
	*h = append(*h, x.(T))
}
func (h *MinHeap[T]) Pop() any {
	old := *h
	n := len(old)
	x := old[n-1]
	*h = old[0 : n-1]
	return x
}
