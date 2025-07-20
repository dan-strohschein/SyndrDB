package btreeindex

import (
	"os"
)

// // TournamentSorter handles the tournament tree sort process
// type TournamentSorter struct {
// 	maxMemorySize int64                            // Maximum memory to use in bytes
// 	tempDir       string                           // Directory for temporary files
// 	runs          []*sortRun                       // Sorted runs on disk
// 	currentItems  []DocIndexKeyValue               // Current items in memory
// 	comparator    func(a, b DocIndexKeyValue) bool // Custom comparison function
// }

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
