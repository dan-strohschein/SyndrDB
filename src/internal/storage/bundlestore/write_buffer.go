package bundlestore

import (
	"os"
	"sync"
	"time"
)

// WriteBuffer provides batched write operations for optimal I/O performance
// Reduces syscalls by buffering multiple document writes before flushing
// Designed for append-only operations with configurable flush triggers
type WriteBuffer struct {
	file         *os.File
	buffer       []byte
	bufferSize   int
	flushSize    int
	lastFlush    time.Time
	flushTimeout time.Duration
	mutex        sync.Mutex
}

// NewWriteBuffer creates a new write buffer for the specified file
func NewWriteBuffer(file *os.File, bufferSize int) *WriteBuffer {
	return &WriteBuffer{
		file:         file,
		buffer:       make([]byte, 0, bufferSize),
		bufferSize:   bufferSize,
		flushSize:    bufferSize / 2, // Flush when half full
		lastFlush:    time.Now(),
		flushTimeout: 100 * time.Millisecond, // Flush after 100ms
	}
}

// Write adds data to the buffer and flushes if necessary
func (wb *WriteBuffer) Write(data []byte) error {
	wb.mutex.Lock()
	defer wb.mutex.Unlock()

	// Check if we need to flush before adding data
	if len(wb.buffer)+len(data) > wb.bufferSize {
		if err := wb.flushInternal(); err != nil {
			return err
		}
	}

	// Add data to buffer
	wb.buffer = append(wb.buffer, data...)

	// Check flush conditions
	shouldFlush := len(wb.buffer) >= wb.flushSize ||
		time.Since(wb.lastFlush) >= wb.flushTimeout

	if shouldFlush {
		return wb.flushInternal()
	}

	return nil
}

// Flush forces all buffered data to be written to disk
func (wb *WriteBuffer) Flush() error {
	wb.mutex.Lock()
	defer wb.mutex.Unlock()
	return wb.flushInternal()
}

// flushInternal performs the actual flush operation (must be called with mutex held)
func (wb *WriteBuffer) flushInternal() error {
	if len(wb.buffer) == 0 {
		return nil
	}

	_, err := wb.file.Write(wb.buffer)
	if err != nil {
		return err
	}

	// Force OS to flush to disk - CRITICAL for durability
	err = wb.file.Sync()
	if err != nil {
		return err
	}

	// Reset buffer
	wb.buffer = wb.buffer[:0]
	wb.lastFlush = time.Now()

	return nil
}

// Close flushes any remaining data and closes the underlying file
func (wb *WriteBuffer) Close() error {
	if err := wb.Flush(); err != nil {
		return err
	}
	return wb.file.Close()
}

// Size returns the current buffer size
func (wb *WriteBuffer) Size() int {
	wb.mutex.Lock()
	defer wb.mutex.Unlock()
	return len(wb.buffer)
}
