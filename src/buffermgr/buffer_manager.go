package buffermgr

import (
	"errors"
	"fmt"
	"sync"
	"time"

	"go.uber.org/zap"
)

const (
	// DefaultPageSize is 8KB, matching PostgreSQL's default
	DefaultPageSize = 8 * 1024

	// DefaultBufferPoolSize is the default number of buffers in the pool
	DefaultBufferPoolSize = 1000

	// BufferStateInvalid indicates the buffer doesn't contain valid data
	BufferStateInvalid = 0

	// BufferStateValid indicates the buffer contains valid data
	BufferStateValid = 1

	// BufferStateDirty indicates the buffer has been modified and needs writing
	BufferStateDirty = 2
)

// BufferTag uniquely identifies a disk page
type BufferTag struct {
	FileID      uint32 // Equivalent to PostgreSQL's RelFileNode
	BlockNumber uint32 // Page number within the file
}

// DBPageBuffer represents a single buffer in the buffer pool
type DBPageBuffer struct {
	// Buffer state and lock management
	Mu         sync.RWMutex
	State      int
	RefCount   int
	UsageCount int

	// Buffer identification
	Tag BufferTag
	ID  int

	// Buffer Data
	Data []byte

	// For dirty buffer management
	IsDirty      bool
	LastModified time.Time

	// For clock sweep algorithm
	Referenced bool
}

// BufferDescriptor holds metadata about a buffer
type BufferDescriptor struct {
	ID         int       // Buffer ID
	Tag        BufferTag // Identifies which disk page the buffer contains
	State      int       // Buffer state (invalid, valid, dirty)
	RefCount   int       // Number of current users of the buffer
	UsageCount int       // For clock-sweep eviction
	Pinned     bool      // If true, don't evict this buffer
}

// BufferPool manages a collection of buffers
type BufferPool struct {
	mu          sync.Mutex
	buffers     []*DBPageBuffer
	descriptors []BufferDescriptor
	hashTable   map[BufferTag]int // Maps BufferTag to buffer index

	// For clock sweep algorithm
	clockHand int

	// Configuration
	pageSize   int
	maxBuffers int

	// Stats
	hits         uint64
	misses       uint64
	evictions    uint64
	writeCount   uint64 // Track total writes
	syncInterval int    // How often to sync (every N writes)

	// File management
	fileRegistry *FileRegistry

	logger *zap.SugaredLogger
}

// NewBufferPool creates a new buffer pool with the given size
func NewBufferPool(bufferCount int, pageSize int, fileRegistry *FileRegistry, logger *zap.SugaredLogger) *BufferPool {
	if pageSize <= 0 {
		pageSize = DefaultPageSize
	}

	if bufferCount <= 0 {
		bufferCount = DefaultBufferPoolSize
	}

	pool := &BufferPool{
		buffers:      make([]*DBPageBuffer, bufferCount),
		descriptors:  make([]BufferDescriptor, bufferCount),
		hashTable:    make(map[BufferTag]int),
		pageSize:     pageSize,
		maxBuffers:   bufferCount,
		clockHand:    0,
		syncInterval: fileRegistry.GetSyncInterval(),
		fileRegistry: fileRegistry,
		logger:       logger,
	}

	// Initialize all buffers
	for i := 0; i < bufferCount; i++ {
		pool.buffers[i] = &DBPageBuffer{
			State:    BufferStateInvalid,
			ID:       i,
			Data:     make([]byte, pageSize),
			RefCount: 0,
		}

		pool.descriptors[i] = BufferDescriptor{
			ID:       i,
			State:    BufferStateInvalid,
			RefCount: 0,
		}
	}

	return pool
}

// GetPage retrieves a page from the buffer pool, reading from disk if necessary
func (bp *BufferPool) GetPage(fileID uint32, blockNum uint32) (*DBPageBuffer, error) {
	tag := BufferTag{
		FileID:      fileID,
		BlockNumber: blockNum,
	}

	// First, try to find the page in the buffer pool
	buffer, found := bp.lookupBuffer(tag)
	if found {
		bp.hits++
		buffer.Referenced = true
		buffer.UsageCount++
		return buffer, nil
	}

	bp.misses++

	// Page not found in the buffer pool, need to read it from disk
	// First, find a buffer to use (either free or by eviction)
	bufferID, err := bp.findFreeBuffer()
	if err != nil {
		return nil, fmt.Errorf("could not find free buffer: %w", err)
	}

	buffer = bp.buffers[bufferID]

	// Mark it as in use to prevent concurrent eviction
	buffer.Mu.Lock()
	defer buffer.Mu.Unlock()

	// If the buffer contains dirty data, write it back to disk
	if buffer.IsDirty {
		err := bp.writeBufferToDisk(buffer)
		if err != nil {
			return nil, fmt.Errorf("could not write dirty buffer to disk: %w", err)
		}
	}

	// Update the buffer's tag
	oldTag := buffer.Tag
	buffer.Tag = tag

	// Update the hash table
	if buffer.State != BufferStateInvalid {
		delete(bp.hashTable, oldTag)
	}
	bp.hashTable[tag] = bufferID

	// Read the page from disk
	err = bp.readPageFromDisk(fileID, blockNum, buffer)
	if err != nil {
		// Revert the hash table changes on failure
		delete(bp.hashTable, tag)
		if buffer.State != BufferStateInvalid {
			bp.hashTable[oldTag] = bufferID
		}
		return nil, fmt.Errorf("could not read page from disk: %w", err)
	}

	// Update buffer state
	buffer.State = BufferStateValid
	buffer.RefCount = 1
	buffer.UsageCount = 1
	buffer.Referenced = true
	buffer.IsDirty = false

	// Update descriptor
	bp.descriptors[bufferID].Tag = tag
	bp.descriptors[bufferID].State = BufferStateValid
	bp.descriptors[bufferID].RefCount = 1

	return buffer, nil
}

// lookupBuffer checks if a page is already in the buffer pool
func (bp *BufferPool) lookupBuffer(tag BufferTag) (*DBPageBuffer, bool) {
	bp.mu.Lock()
	defer bp.mu.Unlock()

	bufferID, found := bp.hashTable[tag]
	if !found {
		return nil, false
	}

	buffer := bp.buffers[bufferID]

	// Make sure the buffer still contains this page (double-check)
	if buffer.Tag != tag || buffer.State == BufferStateInvalid {
		// This shouldn't happen with proper locking, but let's be defensive
		delete(bp.hashTable, tag)
		return nil, false
	}

	// Increment the reference count
	buffer.RefCount++
	bp.descriptors[bufferID].RefCount++

	return buffer, true
}

// findFreeBuffer finds a free buffer to use, potentially evicting if necessary
func (bp *BufferPool) findFreeBuffer() (int, error) {
	bp.mu.Lock()
	defer bp.mu.Unlock()

	// First pass: look for an invalid (unused) buffer
	for i := 0; i < bp.maxBuffers; i++ {
		if bp.buffers[i].State == BufferStateInvalid {
			return i, nil
		}
	}

	// Second pass: use clock sweep to find a victim
	startHand := bp.clockHand

	for {
		bufferID := bp.clockHand

		// Move the clock hand
		bp.clockHand = (bp.clockHand + 1) % bp.maxBuffers

		buffer := bp.buffers[bufferID]

		// Skip buffers that are currently in use
		if buffer.RefCount > 0 {
			continue
		}

		// If the buffer was recently referenced, give it another chance
		if buffer.Referenced {
			buffer.Referenced = false
			continue
		}

		// Found a victim
		bp.evictions++

		// If we've gone through all buffers and found none to evict
		if bp.clockHand == startHand {
			return 0, errors.New("all buffers are in use, cannot evict any")
		}

		return bufferID, nil
	}
}

// writeBufferToDisk writes a dirty buffer back to its file
func (bp *BufferPool) writeBufferToDisk(buffer *DBPageBuffer) error {
	bp.logger.Debugf("Writing buffer %d (file %d, block %d) to disk",
		buffer.ID, buffer.Tag.FileID, buffer.Tag.BlockNumber)

	// Get the file handle from the file registry
	file, err := bp.fileRegistry.GetFile(buffer.Tag.FileID)
	if err != nil {
		return fmt.Errorf("failed to get file handle for fileID %d: %w",
			buffer.Tag.FileID, err)
	}
	// We don't need to close the file as it's managed by the registry

	// Acquire a write lock on the file
	file.Lock()
	defer file.Unlock()

	// Seek to the correct position
	offset := int64(buffer.Tag.BlockNumber) * int64(bp.pageSize)
	_, err = file.Seek(offset, 0)
	if err != nil {
		return fmt.Errorf("failed to seek to block %d: %w",
			buffer.Tag.BlockNumber, err)
	}

	// Write the buffer data
	n, err := file.Write(buffer.Data)
	if err != nil {
		return fmt.Errorf("failed to write buffer data: %w", err)
	}

	if n < bp.pageSize {
		return fmt.Errorf("incomplete write: only wrote %d of %d bytes",
			n, bp.pageSize)
	}

	// Mark buffer as no longer dirty
	buffer.IsDirty = false
	buffer.LastModified = time.Now()

	// Update write statistics
	bp.writeCount++

	// Sync based on policy
	if bp.fileRegistry.ShouldSyncWrites() {
		// Always sync
		if err := file.Sync(); err != nil {
			return fmt.Errorf("failed to sync file: %w", err)
		}
	} else if bp.syncInterval > 0 && bp.writeCount%uint64(bp.syncInterval) == 0 {
		// Sync every N writes
		if err := file.Sync(); err != nil {
			bp.logger.Warnf("Failed to perform interval sync on fileID %d: %v",
				buffer.Tag.FileID, err)
		}
	}

	bp.logger.Debugf("Successfully wrote buffer %d to disk", buffer.ID)
	return nil
}

// readPageFromDisk reads a page from disk into a buffer
func (bp *BufferPool) readPageFromDisk(fileID uint32, blockNum uint32, buffer *DBPageBuffer) error {
	// Get the file handle from the file registry
	managed_file, err := bp.fileRegistry.GetFile(fileID)
	if err != nil {
		return fmt.Errorf("failed to get file handle for fileID %d: %w", fileID, err)
	}

	// Acquire a read lock on the file
	managed_file.RLock()
	defer managed_file.RUnlock()

	offset := int64(blockNum) * int64(bp.pageSize)

	// Seek to the correct position
	_, err = managed_file.Seek(offset, 0)
	if err != nil {
		return fmt.Errorf("could not seek to block %d: %w", blockNum, err)
	}

	// Read the page
	n, err := managed_file.Read(buffer.Data)
	if err != nil {
		return fmt.Errorf("could not read block %d: %w", blockNum, err)
	}

	if n < bp.pageSize {
		// If we read less than a full page, zero the rest
		for i := n; i < bp.pageSize; i++ {
			buffer.Data[i] = 0
		}

		bp.logger.Warnf("Partial read for block %d: got %d bytes, expected %d",
			blockNum, n, bp.pageSize)
	}

	return nil
}

// ReleaseBuffer decreases the reference count of a buffer
func (bp *BufferPool) ReleaseBuffer(buffer *DBPageBuffer) {
	bp.mu.Lock()
	defer bp.mu.Unlock()

	if buffer.RefCount > 0 {
		buffer.RefCount--
		bp.descriptors[buffer.ID].RefCount--
	}
}

// MarkBufferDirty marks a buffer as dirty, requiring a future write
func (bp *BufferPool) MarkBufferDirty(buffer *DBPageBuffer) {
	buffer.Mu.Lock()
	defer buffer.Mu.Unlock()

	buffer.IsDirty = true
	buffer.LastModified = time.Now()
}

// FlushAllDirty writes all dirty buffers to disk
func (bp *BufferPool) FlushAllDirty() error {
	bp.mu.Lock()
	defer bp.mu.Unlock()

	for i := 0; i < bp.maxBuffers; i++ {
		buffer := bp.buffers[i]

		if buffer.State != BufferStateInvalid && buffer.IsDirty {
			err := bp.writeBufferToDisk(buffer)
			if err != nil {
				return fmt.Errorf("error flushing buffer %d: %w", i, err)
			}
		}
	}

	return nil
}

// Stats returns statistics about the buffer pool
type BufferStats struct {
	TotalBuffers int
	UsedBuffers  int
	DirtyBuffers int
	Hits         uint64
	Misses       uint64
	HitRatio     float64
	Evictions    uint64
}

// GetStats returns statistics about the buffer pool
func (bp *BufferPool) GetStats() BufferStats {
	bp.mu.Lock()
	defer bp.mu.Unlock()

	stats := BufferStats{
		TotalBuffers: bp.maxBuffers,
		UsedBuffers:  0,
		DirtyBuffers: 0,
		Hits:         bp.hits,
		Misses:       bp.misses,
		Evictions:    bp.evictions,
	}

	for i := 0; i < bp.maxBuffers; i++ {
		if bp.buffers[i].State != BufferStateInvalid {
			stats.UsedBuffers++
			if bp.buffers[i].IsDirty {
				stats.DirtyBuffers++
			}
		}
	}

	totalRequests := stats.Hits + stats.Misses
	if totalRequests > 0 {
		stats.HitRatio = float64(stats.Hits) / float64(totalRequests)
	}

	return stats
}

// ClearBuffer invalidates a buffer and releases its memory
func (bp *BufferPool) ClearBuffer(bufferID int) error {
	bp.mu.Lock()
	defer bp.mu.Unlock()

	buffer := bp.buffers[bufferID]

	// Check if buffer is currently in use
	if buffer.RefCount > 0 {
		return fmt.Errorf("cannot clear buffer %d: still in use (refCount: %d)",
			bufferID, buffer.RefCount)
	}

	// If buffer is dirty, write it to disk first
	if buffer.IsDirty {
		if err := bp.writeBufferToDisk(buffer); err != nil {
			return fmt.Errorf("failed to write dirty buffer %d to disk: %w", bufferID, err)
		}
	}

	// Remove from hash table
	if buffer.State != BufferStateInvalid {
		delete(bp.hashTable, buffer.Tag)
	}

	// Reset buffer to invalid state
	buffer.State = BufferStateInvalid
	buffer.RefCount = 0
	buffer.UsageCount = 0
	buffer.Referenced = false
	buffer.IsDirty = false

	// Update descriptor
	bp.descriptors[bufferID].State = BufferStateInvalid
	bp.descriptors[bufferID].RefCount = 0
	bp.descriptors[bufferID].Tag = BufferTag{} // Zero value

	// Reset data array (optional - helps garbage collector)
	for i := range buffer.Data {
		buffer.Data[i] = 0
	}

	return nil
}

// ClearBufferPool invalidates all buffers and releases memory
func (bp *BufferPool) ClearBufferPool() error {
	bp.logger.Info("Clearing all buffers from pool")

	// First flush all dirty buffers
	if err := bp.FlushAllDirty(); err != nil {
		return fmt.Errorf("error flushing dirty buffers: %w", err)
	}

	bp.mu.Lock()
	defer bp.mu.Unlock()

	// Check for in-use buffers
	inUseCount := 0
	for i := 0; i < bp.maxBuffers; i++ {
		if bp.buffers[i].RefCount > 0 {
			inUseCount++
		}
	}

	if inUseCount > 0 {
		return fmt.Errorf("%d buffers still in use, cannot clear pool", inUseCount)
	}

	// Clear all buffers
	bp.hashTable = make(map[BufferTag]int) // Reset hash table

	for i := 0; i < bp.maxBuffers; i++ {
		buffer := bp.buffers[i]

		// Reset buffer state
		buffer.State = BufferStateInvalid
		buffer.RefCount = 0
		buffer.UsageCount = 0
		buffer.Referenced = false
		buffer.IsDirty = false
		buffer.Tag = BufferTag{} // Zero value

		// Reset descriptor
		bp.descriptors[i].State = BufferStateInvalid
		bp.descriptors[i].RefCount = 0
		bp.descriptors[i].Tag = BufferTag{} // Zero value

		// Clear data (optional)
		for j := range buffer.Data {
			buffer.Data[j] = 0
		}
	}

	// Reset statistics
	bp.clockHand = 0

	bp.logger.Info("Buffer pool cleared successfully")
	return nil
}

// ShutDown performs clean shutdown of the buffer pool
func (bp *BufferPool) ShutDown() error {
	bp.logger.Info("Shutting down buffer pool")

	// Log final statistics
	stats := bp.GetStats()
	bp.logger.Infof("Buffer pool stats at shutdown: hits=%d, misses=%d, ratio=%.2f, evictions=%d, writes=%d",
		stats.Hits, stats.Misses, stats.HitRatio, stats.Evictions, bp.writeCount)

	// Flush all dirty buffers
	if err := bp.FlushAllDirty(); err != nil {
		return fmt.Errorf("error flushing dirty buffers during shutdown: %w", err)
	}

	// Close all open files
	if bp.fileRegistry != nil {
		if err := bp.fileRegistry.CloseAllFiles(); err != nil {
			bp.logger.Warnf("Error closing files during shutdown: %v", err)
		}
	}

	// Check for any buffers still in use
	bp.mu.Lock()
	defer bp.mu.Unlock()

	inUseCount := 0
	for i := 0; i < bp.maxBuffers; i++ {
		if bp.buffers[i].RefCount > 0 {
			inUseCount++
			bp.logger.Warnf("Buffer %d still has refCount %d during shutdown",
				i, bp.buffers[i].RefCount)
		}
	}

	if inUseCount > 0 {
		bp.logger.Warnf("%d buffers still in use during shutdown", inUseCount)
	}

	// Reset all buffers to help garbage collection
	for i := 0; i < bp.maxBuffers; i++ {
		bp.buffers[i].Data = nil // Allow memory to be reclaimed
	}

	bp.buffers = nil     // Release slice
	bp.descriptors = nil // Release slice
	bp.hashTable = nil   // Release map

	bp.logger.Info("Buffer pool shut down successfully")
	return nil
}
