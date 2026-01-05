package buffer

import (
	"errors"
	"fmt"
	"time"

	"go.uber.org/zap"
)

type BufferManager interface {
	GetPage(fileID, pageNum uint32) (*DBPageBuffer, error)
	ReleasePage(page *DBPageBuffer)
	MarkDirty(page *DBPageBuffer)
	FlushAllDirty() error
}

const (
	// DefaultPageSize is 8KB, matching PostgreSQL's default
	DefaultPageSize = 8 * 1024

	// DefaultBufferPoolSize is the default number of Buffers in the pool
	DefaultBufferPoolSize = 1000

	// BufferStateInvalid indicates the buffer doesn't contain valid data
	BufferStateInvalid = 0

	// BufferStateValid indicates the buffer contains valid data
	BufferStateValid = 1

	// BufferStateDirty indicates the buffer has been modified and needs writing
	BufferStateDirty = 2
)

// BufferTag uniquely identifies a disk page
/*type BufferTag struct {
	FileID      uint32 // Equivalent to PostgreSQL's RelFileNode
	BlockNumber uint32 // Page number within the file
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
*/
// BufferPool manages a collection of Buffers
// type BufferPool struct {
// 	MU          sync.Mutex
// 	Buffers     []*models.DBPageBuffer
// 	Descriptors []models.BufferDescriptor
// 	HashTable   map[models.BufferTag]int // Maps BufferTag to buffer index

// 	// For clock sweep algorithm
// 	ClockHand int

// 	// Configuration
// 	pageSize   int
// 	MaxBuffers int

// 	// Stats
// 	hits         uint64
// 	misses       uint64
// 	evictions    uint64
// 	WriteCount   uint64 // Track total writes
// 	syncInterval int    // How often to sync (every N writes)

// 	// File management
// 	fileRegistry *file.FileRegistry

// 	Logger *zap.SugaredLogger
// }

// NewBufferPool creates a new buffer pool with the given size
func NewBufferPool(bufferCount int, pageSize int, fileRegistry *FileRegistry, Logger *zap.SugaredLogger) *BufferPool {
	if pageSize <= 0 {
		pageSize = DefaultPageSize
	}

	if bufferCount <= 0 {
		bufferCount = DefaultBufferPoolSize
	}

	pool := &BufferPool{
		Buffers:      make([]*DBPageBuffer, bufferCount),
		Descriptors:  make([]BufferDescriptor, bufferCount),
		HashTable:    make(map[BufferTag]int),
		PageSize:     pageSize,
		MaxBuffers:   bufferCount,
		ClockHand:    0,
		SyncInterval: fileRegistry.GetSyncInterval(),
		FileRegistry: fileRegistry,
		Logger:       Logger,
	}

	// Initialize all Buffers
	for i := 0; i < bufferCount; i++ {
		pool.Buffers[i] = &DBPageBuffer{
			State:    BufferStateInvalid,
			ID:       i,
			Data:     make([]byte, pageSize),
			RefCount: 0,
		}

		pool.Descriptors[i] = BufferDescriptor{
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
		bp.Hits++
		buffer.Referenced = true
		buffer.UsageCount++
		return buffer, nil
	}

	bp.Misses++

	// Page not found in the buffer pool, need to read it from disk
	// First, find a buffer to use (either free or by eviction)
	bufferID, err := bp.findFreeBuffer()
	if err != nil {
		return nil, fmt.Errorf("could not find free buffer: %w", err)
	}

	buffer = bp.Buffers[bufferID]

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
		delete(bp.HashTable, oldTag)
	}
	bp.HashTable[tag] = bufferID

	// Read the page from disk
	err = bp.readPageFromDisk(fileID, blockNum, buffer)
	if err != nil {
		// Revert the hash table changes on failure
		delete(bp.HashTable, tag)
		if buffer.State != BufferStateInvalid {
			bp.HashTable[oldTag] = bufferID
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
	bp.Descriptors[bufferID].Tag = tag
	bp.Descriptors[bufferID].State = BufferStateValid
	bp.Descriptors[bufferID].RefCount = 1

	return buffer, nil
}

// lookupBuffer checks if a page is already in the buffer pool
func (bp *BufferPool) lookupBuffer(tag BufferTag) (*DBPageBuffer, bool) {
	bp.MU.Lock()
	defer bp.MU.Unlock()

	bufferID, found := bp.HashTable[tag]
	if !found {
		return nil, false
	}

	buffer := bp.Buffers[bufferID]

	// Make sure the buffer still contains this page (double-check)
	if buffer.Tag != tag || buffer.State == BufferStateInvalid {
		// This shouldn't happen with proper locking, but let's be defensive
		delete(bp.HashTable, tag)
		return nil, false
	}

	// Increment the reference count
	buffer.RefCount++
	bp.Descriptors[bufferID].RefCount++

	return buffer, true
}

// findFreeBuffer finds a free buffer to use, potentially evicting if necessary
func (bp *BufferPool) findFreeBuffer() (int, error) {
	bp.MU.Lock()
	defer bp.MU.Unlock()

	// First pass: look for an invalid (unused) buffer
	for i := 0; i < bp.MaxBuffers; i++ {
		if bp.Buffers[i].State == BufferStateInvalid {
			return i, nil
		}
	}

	// Second pass: use clock sweep to find a victim
	startHand := bp.ClockHand

	for {
		bufferID := bp.ClockHand

		// Move the clock hand
		bp.ClockHand = (bp.ClockHand + 1) % bp.MaxBuffers

		buffer := bp.Buffers[bufferID]

		// Skip Buffers that are currently in use
		if buffer.RefCount > 0 {
			continue
		}

		// If the buffer was recently referenced, give it another chance
		if buffer.Referenced {
			buffer.Referenced = false
			continue
		}

		// Found a victim
		bp.Evictions++

		// If we've gone through all Buffers and found none to evict
		if bp.ClockHand == startHand {
			return 0, errors.New("all Buffers are in use, cannot evict any")
		}

		return bufferID, nil
	}
}

// writeBufferToDisk writes a dirty buffer back to its file
func (bp *BufferPool) writeBufferToDisk(buffer *DBPageBuffer) error {
	bp.Logger.Debugf("Writing buffer %d (file %d, block %d) to disk",
		buffer.ID, buffer.Tag.FileID, buffer.Tag.BlockNumber)

	// Get the file handle from the file registry
	file, err := bp.FileRegistry.GetFile(buffer.Tag.FileID)
	if err != nil {
		return fmt.Errorf("failed to get file handle for fileID %d: %w",
			buffer.Tag.FileID, err)
	}
	// We don't need to close the file as it's managed by the registry

	// Acquire a write lock on the file
	file.Lock()
	defer file.Unlock()

	// Seek to the correct position
	offset := int64(buffer.Tag.BlockNumber) * int64(bp.PageSize)
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

	if n < bp.PageSize {
		return fmt.Errorf("incomplete write: only wrote %d of %d bytes",
			n, bp.PageSize)
	}

	// Mark buffer as no longer dirty
	buffer.IsDirty = false
	buffer.LastModified = time.Now()

	// Update write statistics
	bp.WriteCount++

	// Sync based on policy
	if bp.FileRegistry.ShouldSyncWrites() {
		// Always sync
		if err := file.Sync(); err != nil {
			return fmt.Errorf("failed to sync file: %w", err)
		}
	} else if bp.SyncInterval > 0 && bp.WriteCount%uint64(bp.SyncInterval) == 0 {
		// Sync every N writes
		if err := file.Sync(); err != nil {
			bp.Logger.Warnf("Failed to perform interval sync on fileID %d: %v",
				buffer.Tag.FileID, err)
		}
	}

	bp.Logger.Debugf("Successfully wrote buffer %d to disk", buffer.ID)
	return nil
}

// readPageFromDisk reads a page from disk into a buffer
func (bp *BufferPool) readPageFromDisk(fileID uint32, blockNum uint32, buffer *DBPageBuffer) error {
	// Get the file handle from the file registry
	managed_file, err := bp.FileRegistry.GetFile(fileID)
	if err != nil {
		return fmt.Errorf("failed to get file handle for fileID %d: %w", fileID, err)
	}

	// Acquire a read lock on the file
	managed_file.RLock()
	defer managed_file.RUnlock()

	offset := int64(blockNum) * int64(bp.PageSize)

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

	if n < bp.PageSize {
		// If we read less than a full page, zero the rest
		for i := n; i < bp.PageSize; i++ {
			buffer.Data[i] = 0
		}

		bp.Logger.Warnf("Partial read for block %d: got %d bytes, expected %d",
			blockNum, n, bp.PageSize)
	}

	return nil
}

// ReleaseBuffer decreases the reference count of a buffer
func (bp *BufferPool) ReleaseBuffer(buffer *DBPageBuffer) {
	bp.MU.Lock()
	defer bp.MU.Unlock()

	if buffer.RefCount > 0 {
		buffer.RefCount--
		bp.Descriptors[buffer.ID].RefCount--
	}
}

// MarkBufferDirty marks a buffer as dirty, requiring a future write
func (bp *BufferPool) MarkBufferDirty(buffer *DBPageBuffer) {
	buffer.Mu.Lock()
	defer buffer.Mu.Unlock()

	buffer.IsDirty = true
	buffer.LastModified = time.Now()
}

// FlushAllDirty writes all dirty Buffers to disk
func (bp *BufferPool) FlushAllDirty() error {
	bp.MU.Lock()
	defer bp.MU.Unlock()

	for i := 0; i < bp.MaxBuffers; i++ {
		buffer := bp.Buffers[i]

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
	bp.MU.Lock()
	defer bp.MU.Unlock()

	stats := BufferStats{
		TotalBuffers: bp.MaxBuffers,
		UsedBuffers:  0,
		DirtyBuffers: 0,
		Hits:         bp.Hits,
		Misses:       bp.Misses,
		Evictions:    bp.Evictions,
	}

	for i := 0; i < bp.MaxBuffers; i++ {
		if bp.Buffers[i].State != BufferStateInvalid {
			stats.UsedBuffers++
			if bp.Buffers[i].IsDirty {
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
	bp.MU.Lock()
	defer bp.MU.Unlock()

	buffer := bp.Buffers[bufferID]

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
		delete(bp.HashTable, buffer.Tag)
	}

	// Reset buffer to invalid state
	buffer.State = BufferStateInvalid
	buffer.RefCount = 0
	buffer.UsageCount = 0
	buffer.Referenced = false
	buffer.IsDirty = false

	// Update descriptor
	bp.Descriptors[bufferID].State = BufferStateInvalid
	bp.Descriptors[bufferID].RefCount = 0
	bp.Descriptors[bufferID].Tag = BufferTag{} // Zero value

	// Reset data array (optional - helps garbage collector)
	for i := range buffer.Data {
		buffer.Data[i] = 0
	}

	return nil
}

// ClearBufferPool invalidates all Buffers and releases memory
func (bp *BufferPool) ClearBufferPool() error {
	bp.Logger.Info("Clearing all Buffers from pool")

	// First flush all dirty Buffers
	if err := bp.FlushAllDirty(); err != nil {
		return fmt.Errorf("error flushing dirty Buffers: %w", err)
	}

	bp.MU.Lock()
	defer bp.MU.Unlock()

	// Check for in-use Buffers
	inUseCount := 0
	for i := 0; i < bp.MaxBuffers; i++ {
		if bp.Buffers[i].RefCount > 0 {
			inUseCount++
		}
	}

	if inUseCount > 0 {
		return fmt.Errorf("%d Buffers still in use, cannot clear pool", inUseCount)
	}

	// Clear all Buffers
	bp.HashTable = make(map[BufferTag]int) // Reset hash table

	for i := 0; i < bp.MaxBuffers; i++ {
		buffer := bp.Buffers[i]

		// Reset buffer state
		buffer.State = BufferStateInvalid
		buffer.RefCount = 0
		buffer.UsageCount = 0
		buffer.Referenced = false
		buffer.IsDirty = false
		buffer.Tag = BufferTag{} // Zero value

		// Reset descriptor
		bp.Descriptors[i].State = BufferStateInvalid
		bp.Descriptors[i].RefCount = 0
		bp.Descriptors[i].Tag = BufferTag{} // Zero value

		// Clear data (optional)
		for j := range buffer.Data {
			buffer.Data[j] = 0
		}
	}

	// Reset statistics
	bp.ClockHand = 0

	bp.Logger.Info("Buffer pool cleared successfully")
	return nil
}

// ShutDown performs clean shutdown of the buffer pool
func (bp *BufferPool) ShutDown() error {
	bp.Logger.Info("Shutting down buffer pool")

	// Log final statistics
	stats := bp.GetStats()
	bp.Logger.Infof("Buffer pool stats at shutdown: hits=%d, misses=%d, ratio=%.2f, evictions=%d, writes=%d",
		stats.Hits, stats.Misses, stats.HitRatio, stats.Evictions, bp.WriteCount)

	// Flush all dirty Buffers
	if err := bp.FlushAllDirty(); err != nil {
		return fmt.Errorf("error flushing dirty Buffers during shutdown: %w", err)
	}

	// Close all open files
	if bp.FileRegistry != nil {
		if err := bp.FileRegistry.CloseAllFiles(); err != nil {
			bp.Logger.Warnf("Error closing files during shutdown: %v", err)
		}
	}

	// Check for any Buffers still in use
	bp.MU.Lock()
	defer bp.MU.Unlock()

	inUseCount := 0
	for i := 0; i < bp.MaxBuffers; i++ {
		if bp.Buffers[i].RefCount > 0 {
			inUseCount++
			bp.Logger.Warnf("Buffer %d still has refCount %d during shutdown",
				i, bp.Buffers[i].RefCount)
		}
	}

	if inUseCount > 0 {
		bp.Logger.Warnf("%d Buffers still in use during shutdown", inUseCount)
	}

	// Reset all Buffers to help garbage collection
	for i := 0; i < bp.MaxBuffers; i++ {
		bp.Buffers[i].Data = nil // Allow memory to be reclaimed
	}

	bp.Buffers = nil     // Release slice
	bp.Descriptors = nil // Release slice
	bp.HashTable = nil   // Release map

	bp.Logger.Info("Buffer pool shut down successfully")
	return nil
}
