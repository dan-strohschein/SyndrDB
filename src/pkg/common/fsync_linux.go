//go:build linux

package common

import (
	"fmt"
	"os"
	"syscall"
)

// O_NOATIME is Linux-specific - prevents access time updates on reads
// This reduces metadata write overhead without sacrificing durability
const O_NOATIME = 0x40000

// fdatasyncImpl is the Linux implementation using SYS_FDATASYNC syscall
// This is 2-3x faster than fsync() because it only syncs data, skipping metadata
// updates like access time, modification time, and inode changes when file size hasn't changed.
func fdatasyncImpl(file *os.File) error {
	// Use fdatasync on Linux - only syncs data, skips unnecessary metadata
	// This is safe because we don't need atime/mtime updates for durability
	// File size changes are still synced via fdatasync
	_, _, errno := syscall.Syscall(syscall.SYS_FDATASYNC, file.Fd(), 0, 0)
	if errno != 0 {
		return fmt.Errorf("fdatasync failed: %w", errno)
	}
	return nil
}

// openFileOptimizedImpl is the Linux implementation with O_NOATIME optimization
func openFileOptimizedImpl(path string, baseFlags int, perm os.FileMode) (*os.File, error) {
	// O_NOATIME: Don't update file access time on reads
	// This reduces metadata writes and improves performance
	// Requires file ownership or CAP_FOWNER capability
	flags := baseFlags | O_NOATIME

	// Attempt to open with O_NOATIME
	file, err := os.OpenFile(path, flags, perm)
	if err != nil {
		// If O_NOATIME fails (permission issue), retry without it
		flags = baseFlags
		file, err = os.OpenFile(path, flags, perm)
		if err != nil {
			return nil, fmt.Errorf("failed to open file %s: %w", path, err)
		}
		// Successfully opened without O_NOATIME
		return file, nil
	}

	return file, nil
}
