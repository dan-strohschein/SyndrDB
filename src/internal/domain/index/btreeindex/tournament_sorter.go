package btreeindex

import (
	"bufio"
	"container/heap"
	"fmt"
	"os"
	"sort"
)

// TournamentSorter handles the tournament tree sort process
type TournamentSorter struct {
	maxMemorySize int64                            // Maximum memory to use in bytes
	tempDir       string                           // Directory for temporary files
	runs          []*sortRun                       // Sorted runs on disk
	currentItems  []DocIndexKeyValue               // Current items in memory
	comparator    func(a, b DocIndexKeyValue) bool // Custom comparison function
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
