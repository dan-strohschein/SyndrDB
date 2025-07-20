package btreeindex

// heapItem represents an item in the priority queue
type heapItem struct {
	kv      DocIndexKeyValue
	runIdx  int
	compare func(a, b DocIndexKeyValue) bool
}

// runHeap implements the heap.Interface for tournament tree
type runHeap []*heapItem

func (h runHeap) Len() int { return len(h) }

func (h runHeap) Less(i, j int) bool {
	return h[i].compare(h[i].kv, h[j].kv)
}

func (h runHeap) Swap(i, j int) { h[i], h[j] = h[j], h[i] }

func (h *runHeap) Push(x interface{}) {
	*h = append(*h, x.(*heapItem))
}

func (h *runHeap) Pop() interface{} {
	old := *h
	n := len(old)
	item := old[n-1]
	*h = old[0 : n-1]
	return item
}
