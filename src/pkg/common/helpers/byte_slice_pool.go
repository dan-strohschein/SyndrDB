// Package helpers provides high-performance utilities for SyndrDB.
//
// This file implements a byte slice pool for reducing allocations during
// serialization and deserialization operations. The pool provides pre-allocated
// byte slices of various sizes to avoid repeated allocations.
//
// Usage Pattern:
//
//	buf := helpers.GetPooledByteSlice(1024)
//	defer helpers.ReturnPooledByteSlice(buf)
//	// Use buf[:0] and append to it
//	result := append(buf[:0], data...)
package helpers

import (
	"sync"
)

// ByteSlicePool provides pooling for byte slices of various sizes.
// Uses multiple pools for different size classes to minimize wasted space.
type ByteSlicePool struct {
	// Size classes: 1KB, 4KB, 16KB, 64KB, 256KB, 1MB
	pool1K   sync.Pool
	pool4K   sync.Pool
	pool16K  sync.Pool
	pool64K  sync.Pool
	pool256K sync.Pool
	pool1M   sync.Pool
}

// Global byte slice pool instance
var globalByteSlicePool = &ByteSlicePool{
	pool1K: sync.Pool{
		New: func() interface{} { return make([]byte, 0, 1024) },
	},
	pool4K: sync.Pool{
		New: func() interface{} { return make([]byte, 0, 4096) },
	},
	pool16K: sync.Pool{
		New: func() interface{} { return make([]byte, 0, 16384) },
	},
	pool64K: sync.Pool{
		New: func() interface{} { return make([]byte, 0, 65536) },
	},
	pool256K: sync.Pool{
		New: func() interface{} { return make([]byte, 0, 262144) },
	},
	pool1M: sync.Pool{
		New: func() interface{} { return make([]byte, 0, 1048576) },
	},
}

// GetPooledByteSlice returns a byte slice with at least minCapacity bytes.
// The returned slice has length 0 but capacity >= minCapacity.
// CRITICAL: Caller must call ReturnPooledByteSlice when done.
func GetPooledByteSlice(minCapacity int) []byte {
	switch {
	case minCapacity <= 1024:
		return globalByteSlicePool.pool1K.Get().([]byte)[:0]
	case minCapacity <= 4096:
		return globalByteSlicePool.pool4K.Get().([]byte)[:0]
	case minCapacity <= 16384:
		return globalByteSlicePool.pool16K.Get().([]byte)[:0]
	case minCapacity <= 65536:
		return globalByteSlicePool.pool64K.Get().([]byte)[:0]
	case minCapacity <= 262144:
		return globalByteSlicePool.pool256K.Get().([]byte)[:0]
	case minCapacity <= 1048576:
		return globalByteSlicePool.pool1M.Get().([]byte)[:0]
	default:
		// Too large for pool - allocate directly
		return make([]byte, 0, minCapacity)
	}
}

// ReturnPooledByteSlice returns a byte slice to the appropriate pool.
// Safe to call with nil or slices that weren't from the pool.
func ReturnPooledByteSlice(buf []byte) {
	if buf == nil {
		return
	}

	cap := cap(buf)
	switch {
	case cap == 1024:
		globalByteSlicePool.pool1K.Put(buf[:0])
	case cap == 4096:
		globalByteSlicePool.pool4K.Put(buf[:0])
	case cap == 16384:
		globalByteSlicePool.pool16K.Put(buf[:0])
	case cap == 65536:
		globalByteSlicePool.pool64K.Put(buf[:0])
	case cap == 262144:
		globalByteSlicePool.pool256K.Put(buf[:0])
	case cap == 1048576:
		globalByteSlicePool.pool1M.Put(buf[:0])
		// else: not from our pool, let GC handle it
	}
}

// GetPooledByteSliceWithData returns a pooled byte slice containing a copy of data.
// Useful for cases where you need to return data that might outlive the source.
func GetPooledByteSliceWithData(data []byte) []byte {
	buf := GetPooledByteSlice(len(data))
	return append(buf, data...)
}

// PooledBuffer wraps a byte slice with automatic pool return on Release.
// Provides a safer API that tracks ownership.
type PooledBuffer struct {
	Data     []byte
	released bool
}

// NewPooledBuffer creates a new pooled buffer with at least minCapacity.
func NewPooledBuffer(minCapacity int) *PooledBuffer {
	return &PooledBuffer{
		Data:     GetPooledByteSlice(minCapacity),
		released: false,
	}
}

// Write appends data to the buffer, growing if needed.
func (pb *PooledBuffer) Write(data []byte) {
	pb.Data = append(pb.Data, data...)
}

// Bytes returns the current buffer contents.
func (pb *PooledBuffer) Bytes() []byte {
	return pb.Data
}

// Len returns the current length of data in the buffer.
func (pb *PooledBuffer) Len() int {
	return len(pb.Data)
}

// Reset clears the buffer but keeps capacity.
func (pb *PooledBuffer) Reset() {
	pb.Data = pb.Data[:0]
}

// Release returns the buffer to the pool. Safe to call multiple times.
func (pb *PooledBuffer) Release() {
	if pb.released {
		return
	}
	ReturnPooledByteSlice(pb.Data)
	pb.Data = nil
	pb.released = true
}

// CopyAndRelease returns a copy of the data and releases the buffer.
// Use this when you need the data to outlive the pooled buffer.
func (pb *PooledBuffer) CopyAndRelease() []byte {
	if pb.released {
		return nil
	}
	result := make([]byte, len(pb.Data))
	copy(result, pb.Data)
	pb.Release()
	return result
}
