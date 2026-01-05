//go:build darwin

package common

import (
	"fmt"
	"os"

	"golang.org/x/sys/unix"
)

// fdatasyncImpl is the macOS implementation using F_FULLFSYNC
// macOS requires F_FULLFSYNC to bypass disk cache for true durability
// This is slower than fsync but necessary because macOS fsync is async
// PostgreSQL and SQLite use this approach on macOS
func fdatasyncImpl(file *os.File) error {
	// Use F_FULLFSYNC to bypass disk cache for true durability
	_, err := unix.FcntlInt(file.Fd(), unix.F_FULLFSYNC, 0)
	if err != nil {
		return fmt.Errorf("fcntl F_FULLFSYNC failed: %w", err)
	}
	return nil
}

// openFileOptimizedImpl is the macOS implementation
// No special optimizations available on macOS, use standard open
func openFileOptimizedImpl(path string, baseFlags int, perm os.FileMode) (*os.File, error) {
	file, err := os.OpenFile(path, baseFlags, perm)
	if err != nil {
		return nil, fmt.Errorf("failed to open file %s: %w", path, err)
	}
	return file, nil
}
