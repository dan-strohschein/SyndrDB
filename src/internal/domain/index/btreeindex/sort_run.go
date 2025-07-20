package btreeindex

import "os"

// sortRun represents a single sorted run stored on disk
type sortRun struct {
	path     string             // Path to the file containing the run
	file     *os.File           // File handle
	buffer   []DocIndexKeyValue // Read buffer
	position int                // Current position in buffer
	size     int                // Total number of items in the run
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
		kv, err := ReadKeyValue(run.file)
		if err != nil {
			return err
		}

		run.buffer = append(run.buffer, kv)
	}

	return nil
}
