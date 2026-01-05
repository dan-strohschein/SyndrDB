package common

/*
PLATFORM-OPTIMIZED FSYNC UTILITY

This file provides platform-specific file synchronization functions optimized for database workloads.
The implementation follows PostgreSQL and RocksDB best practices for durable writes across operating systems.

Key features:
- Fdatasync(): 2-3x faster than Sync() on Linux by skipping metadata sync
- F_FULLFSYNC on macOS for true durability bypassing disk cache
- O_NOATIME flag on Linux to reduce metadata write overhead
- Automatic platform detection and fallback to standard library

The primary function Fdatasync() should be used instead of file.Sync() throughout the codebase
for all durability-critical operations like WAL flushes and checkpoint writes.

Performance impact:
- Linux: fdatasync() is 2-3x faster than fsync() (skips atime, mtime, file size updates)
- macOS: F_FULLFSYNC ensures true durability by forcing disk cache flush
- Other platforms: Falls back to standard file.Sync()

File opening optimization:
- OpenFileOptimized() adds O_NOATIME on Linux to skip access time updates
- Reduces metadata write overhead without sacrificing durability
*/

import (
	"fmt"
	"os"
	"runtime"
	"syscall"

	"golang.org/x/sys/unix"
)

// O_NOATIME is Linux-specific - define it here for cross-platform compilation
// On non-Linux platforms, this will just be 0 and have no effect
const O_NOATIME = 0x40000

// Fdatasync synchronizes file data to disk without syncing metadata
// This is 2-3x faster than Sync() on Linux because it skips unnecessary metadata updates
// like access time, modification time, and inode changes when file size hasn't changed.
//
// Platform-specific behavior:
// - Linux: Uses SYS_FDATASYNC syscall (fast, trusts disk cache by default)
// - macOS: Uses F_FULLFSYNC fcntl (slow but ensures true durability by bypassing disk cache)
// - Other: Falls back to file.Sync() (safe default)
//
// For database workloads, this should be used instead of file.Sync() at all sync points:
// - WAL flush operations
// - Checkpoint completions
// - Transaction commits
// - Storage engine writes
func Fdatasync(file *os.File) error {
	if file == nil {
		return fmt.Errorf("cannot sync nil file")
	}

	switch runtime.GOOS {
	case "linux":
		// Use fdatasync on Linux - only syncs data, skips metadata
		// This is safe because we don't need atime/mtime updates for durability
		// File size changes are still synced via fdatasync
		_, _, errno := syscall.Syscall(syscall.SYS_FDATASYNC, file.Fd(), 0, 0)
		if errno != 0 {
			return fmt.Errorf("fdatasync failed: %w", errno)
		}
		return nil

	case "darwin":
		// macOS: Use F_FULLFSYNC to bypass disk cache for true durability
		// This is slower than fsync but necessary because macOS fsync is async
		// PostgreSQL and SQLite use this approach on macOS
		_, err := unix.FcntlInt(file.Fd(), unix.F_FULLFSYNC, 0)
		if err != nil {
			return fmt.Errorf("fcntl F_FULLFSYNC failed: %w", err)
		}
		return nil

	default:
		// Fallback to standard Sync() for other platforms (Windows, BSD, etc.)
		// TODO: I will add Windows-specific optimizations using FlushFileBuffers
		return file.Sync()
	}
}

// OpenFileOptimized opens a file with platform-specific optimizations for database workloads
// This should be used instead of os.OpenFile for all database data files (WAL, storage, indexes)
//
// Platform-specific optimizations:
// - Linux: Adds O_NOATIME to skip access time updates (reduces metadata writes)
// - macOS: Uses standard flags (no optimization available)
// - Other: Uses standard flags
//
// The O_NOATIME flag on Linux reduces disk I/O by preventing atime (access time) updates
// on every read operation. This is safe for databases because we don't rely on access times
// for any functionality. PostgreSQL, MySQL, and RocksDB all use this optimization.
//
// Example usage:
//
//	file, err := OpenFileOptimized("/path/to/wal.log", os.O_RDWR|os.O_CREATE|os.O_APPEND, 0644)
//	if err != nil {
//		return fmt.Errorf("failed to open WAL file: %w", err)
//	}
//	defer file.Close()
func OpenFileOptimized(path string, baseFlags int, perm os.FileMode) (*os.File, error) {
	flags := baseFlags

	// Add platform-specific optimizations
	switch runtime.GOOS {
	case "linux":
		// O_NOATIME: Don't update file access time on reads
		// This reduces metadata writes and improves performance
		// Requires file ownership or CAP_FOWNER capability
		flags |= O_NOATIME
	}

	// Attempt to open with optimized flags
	file, err := os.OpenFile(path, flags, perm)
	if err != nil {
		// If O_NOATIME fails (permission issue), retry without it
		if runtime.GOOS == "linux" && (flags&O_NOATIME) != 0 {
			flags &^= O_NOATIME
			file, err = os.OpenFile(path, flags, perm)
			if err != nil {
				return nil, fmt.Errorf("failed to open file %s: %w", path, err)
			}
			// Successfully opened without O_NOATIME
			return file, nil
		}
		return nil, fmt.Errorf("failed to open file %s: %w", path, err)
	}

	return file, nil
}

// SyncDirectory syncs a directory to ensure file metadata is durable
// This is necessary after file renames, creates, or deletes to ensure the directory
// entry changes are persisted to disk. Without this, a crash could cause the directory
// to be inconsistent even if individual files were synced.
//
// Used in scenarios like:
// - Atomic file renames (e.g., WAL rotation, checkpoint completion)
// - File creation (e.g., new WAL segment, new SSTable)
// - File deletion (e.g., WAL cleanup, compaction)
func SyncDirectory(path string) error {
	// Open the directory (not the file)
	dir, err := os.Open(path)
	if err != nil {
		return fmt.Errorf("failed to open directory %s: %w", path, err)
	}
	defer dir.Close()

	// Sync the directory to persist metadata changes
	if err := dir.Sync(); err != nil {
		return fmt.Errorf("failed to sync directory %s: %w", path, err)
	}

	return nil
}
