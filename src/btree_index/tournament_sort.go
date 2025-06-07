package btreeindex

import (
	"bufio"
	"container/heap"
	"encoding/binary"
	"fmt"

	"os"
	"sort"
)

// DocIndexKeyValue represents a key-value pair in our index
type DocIndexKeyValue struct {
	Key       []byte // The key to be indexed (could be a field value from document)
	DocID     string // Document ID
	ExtraData []byte // Additional data to store with the index entry (optional)
}

// TournamentSorter handles the tournament tree sort process
type TournamentSorter struct {
	maxMemorySize int64                            // Maximum memory to use in bytes
	tempDir       string                           // Directory for temporary files
	runs          []*sortRun                       // Sorted runs on disk
	currentItems  []DocIndexKeyValue               // Current items in memory
	comparator    func(a, b DocIndexKeyValue) bool // Custom comparison function
}

// sortRun represents a single sorted run stored on disk
type sortRun struct {
	path     string             // Path to the file containing the run
	file     *os.File           // File handle
	buffer   []DocIndexKeyValue // Read buffer
	position int                // Current position in buffer
	size     int                // Total number of items in the run
}

// NewTournamentSorter creates a new sorter with specified memory limit
func NewTournamentSorter(maxMemoryBytes int64, tempDir string,
	comparator func(a, b DocIndexKeyValue) bool) *TournamentSorter {

	// If no directory specified, use system temp directory
	if tempDir == "" {
		tempDir = os.TempDir()
	}

	if comparator == nil {
		// Default comparator uses lexicographic comparison of keys
		comparator = func(a, b DocIndexKeyValue) bool {
			return string(a.Key) < string(b.Key)
		}
	}

	return &TournamentSorter{
		maxMemorySize: maxMemoryBytes,
		tempDir:       tempDir,
		currentItems:  make([]DocIndexKeyValue, 0, 10000), // Initial capacity
		comparator:    comparator,
	}
}

// Add adds a key-value pair to the sorter
func (ts *TournamentSorter) Add(key []byte, docID string, extraData []byte) error {
	item := DocIndexKeyValue{
		Key:       key,
		DocID:     docID,
		ExtraData: extraData,
	}

	// Simple estimation of item memory size
	itemSize := int64(len(key) + len(docID) + len(extraData) + 32) // 32 bytes for struct overhead

	// If adding this item would exceed memory limit, flush to disk (slightly expensive operation)
	currentSize := int64(len(ts.currentItems)) * itemSize
	if currentSize+itemSize > ts.maxMemorySize && len(ts.currentItems) > 0 {
		if err := ts.flushToDisk(); err != nil {
			return err
		}
	}

	ts.currentItems = append(ts.currentItems, item)
	return nil
}

// flushToDisk sorts the current items and writes them to a temporary file
func (ts *TournamentSorter) flushToDisk() error {
	if len(ts.currentItems) == 0 {
		return nil
	}

	// Sort the current batch in memory
	sort.Slice(ts.currentItems, func(i, j int) bool {
		return ts.comparator(ts.currentItems[i], ts.currentItems[j])
	})

	// Create a temporary file for this run
	tmpFile, err := os.CreateTemp(ts.tempDir, "index-run-*.dat")
	if err != nil {
		return fmt.Errorf("failed to create temp file: %w", err)
	}

	// Write the sorted items to the file
	run := &sortRun{
		path: tmpFile.Name(),
		size: len(ts.currentItems),
	}

	bufWriter := bufio.NewWriterSize(tmpFile, 64*1024) // 64KB buffer

	buffer := make([]byte, 0, 64*1024)
	for _, item := range ts.currentItems {
		appendBytes(&buffer, item.Key)
		appendString(&buffer, item.DocID)
		appendBytes(&buffer, item.ExtraData)
	}
	bufWriter.Write(buffer)
	// TODO In the final implementation, we will serialize the items efficiently
	// For now, we'll just write the length and bytes for each field
	// for _, item := range ts.currentItems {
	// 	// TODO use proper binary encoding
	// 	writeBytes(tmpFile, item.Key)
	// 	writeString(tmpFile, item.DocID)
	// 	writeBytes(tmpFile, item.ExtraData)
	// }

	bufWriter.Flush()
	tmpFile.Close()
	ts.runs = append(ts.runs, run)

	// Clear current items
	ts.currentItems = ts.currentItems[:0]
	return nil
}

// Sort performs the complete sort operation and returns an iterator for the results
func (ts *TournamentSorter) Sort() (*SortedIterator, error) {
	// Flush any remaining items to disk
	if len(ts.currentItems) > 0 {
		if err := ts.flushToDisk(); err != nil {
			return nil, err
		}
	}

	// If there are no runs, the data was small enough to fit in memory
	if len(ts.runs) == 0 {
		// Create an in-memory iterator
		return newInMemoryIterator(ts.currentItems), nil
	}

	// Perform the multi-way merge of all runs
	return ts.mergeRuns()
}

// mergeRuns merges all the sorted runs using a tournament tree approach
func (ts *TournamentSorter) mergeRuns() (*SortedIterator, error) {
	// Open all run files
	for _, run := range ts.runs {
		file, err := os.Open(run.path)
		if err != nil {
			return nil, fmt.Errorf("failed to open run file: %w", err)
		}
		run.file = file

		// Initialize the buffer for each run
		run.buffer = make([]DocIndexKeyValue, 0, 1000)
		run.position = 0

		// Read initial batch of items
		if err := ts.fillRunBuffer(run); err != nil {
			return nil, err
		}
	}

	// Create a priority queue (min-heap) using the first item from each run
	pq := make(runHeap, 0, len(ts.runs))
	heap.Init(&pq)

	for i, run := range ts.runs {
		if len(run.buffer) > 0 {
			heap.Push(&pq, &heapItem{
				kv:      run.buffer[0],
				runIdx:  i,
				compare: ts.comparator,
			})
			run.position++
		}
	}

	// Create the final merge iterator
	return newMergeIterator(&pq, ts.runs, ts.comparator, ts.fillRunBuffer), nil
}

// fillRunBuffer reads more items into a run's buffer
func (ts *TournamentSorter) fillRunBuffer(run *sortRun) error {
	// Clear the buffer but keep capacity
	run.buffer = run.buffer[:0]

	// Read up to 1000 items
	const batchSize = 1000
	for i := 0; i < batchSize; i++ {
		// Check if we've read all items in this run
		if run.position >= run.size {
			break
		}

		// TODO properly deserialize from binary format
		// For now, we assume readKeyValue works for mvp
		kv, err := readKeyValue(run.file)
		if err != nil {
			return err
		}

		run.buffer = append(run.buffer, kv)
	}

	return nil
}

// Cleanup removes all temporary files
func (ts *TournamentSorter) Cleanup() error {
	var lastErr error
	for _, run := range ts.runs {
		if run.file != nil {
			run.file.Close()
		}
		if err := os.Remove(run.path); err != nil {
			lastErr = err
		}
	}
	return lastErr
}

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

// Helper functions to append to the buffer
func appendBytes(buffer *[]byte, data []byte) {
	// Append length then data
	lenBuf := make([]byte, 4)
	binary.LittleEndian.PutUint32(lenBuf, uint32(len(data)))
	*buffer = append(*buffer, lenBuf...)
	*buffer = append(*buffer, data...)
}

// appendString appends a string to the buffer with length prefix
func appendString(buffer *[]byte, s string) {
	// Convert string to bytes only once
	data := []byte(s)

	// Append length then string data
	lenBuf := make([]byte, 4)
	binary.LittleEndian.PutUint32(lenBuf, uint32(len(data)))
	*buffer = append(*buffer, lenBuf...)
	*buffer = append(*buffer, data...)
}
