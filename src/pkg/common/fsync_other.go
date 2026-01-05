//go:build !linux && !darwin && !windows

package common

import (
	"fmt"
	"os"
)

// fdatasyncImpl is the fallback implementation for other platforms
// Falls back to standard file.Sync() for safety
func fdatasyncImpl(file *os.File) error {
	return file.Sync()
}

// openFileOptimizedImpl is the fallback implementation for other platforms
// No special optimizations, use standard open
func openFileOptimizedImpl(path string, baseFlags int, perm os.FileMode) (*os.File, error) {
	file, err := os.OpenFile(path, baseFlags, perm)
	if err != nil {
		return nil, fmt.Errorf("failed to open file %s: %w", path, err)
	}
	return file, nil
}
