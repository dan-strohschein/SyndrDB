package btreeindex

import "container/heap"

// SortedIterator provides an iterator interface to the sorted results
type SortedIterator struct {
	items    []DocIndexKeyValue
	position int
	pq       *runHeap
	runs     []*sortRun
	compare  func(a, b DocIndexKeyValue) bool
	fillRun  func(*sortRun) error
}

// newInMemoryIterator creates an iterator for in-memory results
func newInMemoryIterator(items []DocIndexKeyValue) *SortedIterator {
	return &SortedIterator{
		items:    items,
		position: 0,
	}
}

// newMergeIterator creates an iterator that merges multiple runs
func newMergeIterator(pq *runHeap, runs []*sortRun,
	compare func(a, b DocIndexKeyValue) bool,
	fillRun func(*sortRun) error) *SortedIterator {

	return &SortedIterator{
		pq:      pq,
		runs:    runs,
		compare: compare,
		fillRun: fillRun,
	}
}

// Next returns the next item in the sorted sequence
func (si *SortedIterator) Next() (DocIndexKeyValue, bool) {
	// In-memory case
	if si.pq == nil {
		if si.position >= len(si.items) {
			return DocIndexKeyValue{}, false
		}
		item := si.items[si.position]
		si.position++
		return item, true
	}

	// Multi-way merge case
	if si.pq.Len() == 0 {
		return DocIndexKeyValue{}, false
	}

	// Get the smallest item from the priority queue
	item := heap.Pop(si.pq).(*heapItem)
	kv := item.kv
	runIdx := item.runIdx

	// Get the corresponding run
	run := si.runs[runIdx]

	// If we need more items from this run
	if run.position < len(run.buffer) {
		// Push the next item from this run onto the heap
		heap.Push(si.pq, &heapItem{
			kv:      run.buffer[run.position],
			runIdx:  runIdx,
			compare: si.compare,
		})
		run.position++
	} else if run.position < run.size {
		// If buffer is empty but run has more items, refill the buffer
		if err := si.fillRun(run); err != nil {
			// Handle error -
			// TODO log this error appropriately
			return DocIndexKeyValue{}, false
		}

		if len(run.buffer) > 0 {
			heap.Push(si.pq, &heapItem{
				kv:      run.buffer[0],
				runIdx:  runIdx,
				compare: si.compare,
			})
			run.position = 1
		}
	}

	return kv, true
}

// Close closes the iterator and releases resources
func (si *SortedIterator) Close() error {
	var lastErr error
	if si.runs != nil {
		for _, run := range si.runs {
			if run.file != nil {
				if err := run.file.Close(); err != nil {
					lastErr = err
				}
			}
		}
	}
	return lastErr
}
