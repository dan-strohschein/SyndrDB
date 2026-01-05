//go:build windows

package common

import (
	"fmt"
	"os"
	"syscall"
	"unsafe"
)

var (
	kernel32         = syscall.NewLazyDLL("kernel32.dll")
	procFlushFileBuffers = kernel32.NewProc("FlushFileBuffers")
)

// fdatasyncImpl is the Windows implementation using FlushFileBuffers
// Windows doesn't have fdatasync, so we use FlushFileBuffers which is equivalent to fsync
// This ensures all buffered data is written to disk
func fdatasyncImpl(file *os.File) error {
	// Get the Windows file handle
	handle := syscall.Handle(file.Fd())
	
	// Call FlushFileBuffers to ensure durability
	r1, _, err := procFlushFileBuffers.Call(uintptr(handle))
	if r1 == 0 {
		return fmt.Errorf("FlushFileBuffers failed: %w", err)
	}
	return nil
}

// openFileOptimizedImpl is the Windows implementation
// No special optimizations available on Windows, use standard open
func openFileOptimizedImpl(path string, baseFlags int, perm os.FileMode) (*os.File, error) {
	file, err := os.OpenFile(path, baseFlags, perm)
	if err != nil {
		return nil, fmt.Errorf("failed to open file %s: %w", path, err)
	}
	return file, nil
}

// Suppress unused import error
var _ = unsafe.Pointer(nil)
