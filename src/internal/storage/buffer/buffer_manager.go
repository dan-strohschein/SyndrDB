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

	// Initialize all Buffers (State/RefCount/Tag live in Descriptor only)
	for i := 0; i < bufferCount; i++ {
		pool.Buffers[i] = &DBPageBuffer{
			ID:   i,
			Data: make([]byte, pageSize),
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
		bp.Hits.Add(1)
		buffer.Referenced = true
		buffer.UsageCount++
		return buffer, nil
	}

	bp.Misses.Add(1)

	// Page not found in the buffer pool, need to read it from disk.
	// Find a buffer (either free or by eviction), then pin it so no other goroutine can evict it.
	bufferID, err := bp.findFreeBuffer()
	if err != nil {
		return nil, fmt.Errorf("could not find free buffer: %w", err)
	}

	buffer = bp.Buffers[bufferID]

	// Pin the buffer under pool mutex so findFreeBuffer will not hand it out again.
	bp.MU.Lock()
	bp.Descriptors[bufferID].RefCount = 1
	bp.MU.Unlock()

	// Do I/O without holding pool mutex and without adding new tag to HashTable,
	// so no concurrent lookup can return this buffer until it is fully loaded.
	// writeBufferToDisk takes buffer.Mu internally; we do not hold it here.
	if buffer.IsDirty {
		if err := bp.writeBufferToDisk(buffer); err != nil {
			bp.unpinBuffer(bufferID)
			return nil, fmt.Errorf("could not write dirty buffer to disk: %w", err)
		}
	}

	oldTag := bp.Descriptors[bufferID].Tag
	// Do not add tag to HashTable or set descriptor.Tag/buffer.Tag until after read completes.

	if err := bp.readPageFromDisk(fileID, blockNum, buffer); err != nil {
		bp.unpinBuffer(bufferID)
		return nil, fmt.Errorf("could not read page from disk: %w", err)
	}

	// Now update hash table and state under pool mutex so the buffer is visible for the new tag.
	bp.MU.Lock()
	bp.Descriptors[bufferID].Tag = tag
	buffer.Tag = tag
	if bp.Descriptors[bufferID].State != BufferStateInvalid {
		delete(bp.HashTable, oldTag)
	}
	bp.HashTable[tag] = bufferID
	bp.Descriptors[bufferID].State = BufferStateValid
	bp.Descriptors[bufferID].RefCount = 1
	buffer.UsageCount = 1
	buffer.Referenced = true
	buffer.IsDirty = false
	bp.MU.Unlock()

	return buffer, nil
}

// unpinBuffer releases the pin set on a buffer during the GetPage miss path (so it can be evicted again).
// Call on miss-path failure after we pinned with RefCount=1 but before adding the tag to HashTable.
func (bp *BufferPool) unpinBuffer(bufferID int) {
	bp.MU.Lock()
	bp.Descriptors[bufferID].RefCount = 0
	bp.MU.Unlock()
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
	if bp.Descriptors[bufferID].Tag != tag || bp.Descriptors[bufferID].State == BufferStateInvalid {
		delete(bp.HashTable, tag)
		return nil, false
	}

	// Increment the reference count
	bp.Descriptors[bufferID].RefCount++

	return buffer, true
}

// findFreeBuffer finds a free buffer to use, potentially evicting if necessary
func (bp *BufferPool) findFreeBuffer() (int, error) {
	bp.MU.Lock()
	defer bp.MU.Unlock()

	// First pass: look for an invalid (unused) buffer
	for i := 0; i < bp.MaxBuffers; i++ {
		if bp.Descriptors[i].State == BufferStateInvalid {
			return i, nil
		}
	}

	// Second pass: use clock sweep to find a victim
	startHand := bp.ClockHand

	for first := true; ; first = false {
		bufferID := bp.ClockHand

		// Detect full circle without finding any victim (only after first iteration)
		if !first && bufferID == startHand {
			return 0, errors.New("all Buffers are in use, cannot evict any")
		}

		// Advance the clock hand
		bp.ClockHand = (bp.ClockHand + 1) % bp.MaxBuffers

		buffer := bp.Buffers[bufferID]

		// Skip buffers that are currently in use
		if bp.Descriptors[bufferID].RefCount > 0 {
			continue
		}

		// If the buffer was recently referenced, give it another chance
		if buffer.Referenced {
			buffer.Referenced = false
			continue
		}

		// Found a victim; return immediately (do not check full circle after finding one)
		bp.Evictions++
		return bufferID, nil
	}
}

// writeBufferToDisk writes a dirty buffer back to its file.
// It holds buffer.Mu for the duration of the write so a consistent snapshot is written
// (avoids torn or stale writes if the buffer is in use). Callers must not hold buffer.Mu.
func (bp *BufferPool) writeBufferToDisk(buffer *DBPageBuffer) error {
	buffer.Mu.Lock()
	defer buffer.Mu.Unlock()

	tag := bp.Descriptors[buffer.ID].Tag
	bp.Logger.Debugf("Writing buffer %d (file %d, block %d) to disk",
		buffer.ID, tag.FileID, tag.BlockNumber)

	// Get the file handle from the file registry (increments refCount)
	file, err := bp.FileRegistry.GetFile(tag.FileID)
	if err != nil {
		return fmt.Errorf("failed to get file handle for fileID %d: %w", tag.FileID, err)
	}
	defer bp.FileRegistry.CloseFile(tag.FileID)

	// Acquire a write lock on the file
	file.Lock()
	defer file.Unlock()

	// Seek to the correct position
	offset := int64(tag.BlockNumber) * int64(bp.PageSize)
	_, err = file.Seek(offset, 0)
	if err != nil {
		return fmt.Errorf("failed to seek to block %d: %w", tag.BlockNumber, err)
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
	bp.WriteCount.Add(1)

	// Sync based on policy
	if bp.FileRegistry.ShouldSyncWrites() {
		// Always sync
		if err := file.Sync(); err != nil {
			return fmt.Errorf("failed to sync file: %w", err)
		}
		} else if bp.SyncInterval > 0 && bp.WriteCount.Load()%uint64(bp.SyncInterval) == 0 {
		// Sync every N writes
		if err := file.Sync(); err != nil {
			bp.Logger.Warnf("Failed to perform interval sync on fileID %d: %v", tag.FileID, err)
		}
	}

	bp.Logger.Debugf("Successfully wrote buffer %d to disk", buffer.ID)
	return nil
}

// readPageFromDisk reads a page from disk into a buffer
func (bp *BufferPool) readPageFromDisk(fileID uint32, blockNum uint32, buffer *DBPageBuffer) error {
	// Get the file handle from the file registry (increments refCount)
	managed_file, err := bp.FileRegistry.GetFile(fileID)
	if err != nil {
		return fmt.Errorf("failed to get file handle for fileID %d: %w", fileID, err)
	}
	defer bp.FileRegistry.CloseFile(fileID)

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

	if bp.Descriptors[buffer.ID].RefCount > 0 {
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

// FlushAllDirty writes all dirty buffers to disk without holding the pool mutex across I/O,
// so GetPage/ReleaseBuffer remain responsive. Each write holds only the buffer's lock.
func (bp *BufferPool) FlushAllDirty() error {
	bp.MU.Lock()
	var toFlush []int
	for i := 0; i < bp.MaxBuffers; i++ {
		b := bp.Buffers[i]
		if bp.Descriptors[i].State != BufferStateInvalid && b.IsDirty {
			toFlush = append(toFlush, i)
		}
	}
	bp.MU.Unlock()

	for _, i := range toFlush {
		if err := bp.writeBufferToDisk(bp.Buffers[i]); err != nil {
			return fmt.Errorf("error flushing buffer %d: %w", i, err)
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
		Hits:         bp.Hits.Load(),
		Misses:       bp.Misses.Load(),
		Evictions:    bp.Evictions,
	}

	for i := 0; i < bp.MaxBuffers; i++ {
		if bp.Descriptors[i].State != BufferStateInvalid {
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
	if bp.Descriptors[bufferID].RefCount > 0 {
		return fmt.Errorf("cannot clear buffer %d: still in use (refCount: %d)",
			bufferID, bp.Descriptors[bufferID].RefCount)
	}

	// If buffer is dirty, write it to disk first
	if buffer.IsDirty {
		if err := bp.writeBufferToDisk(buffer); err != nil {
			return fmt.Errorf("failed to write dirty buffer %d to disk: %w", bufferID, err)
		}
	}

	// Remove from hash table
	if bp.Descriptors[bufferID].State != BufferStateInvalid {
		delete(bp.HashTable, bp.Descriptors[bufferID].Tag)
	}

	// Reset buffer and descriptor to invalid state
	buffer.UsageCount = 0
	buffer.Referenced = false
	buffer.IsDirty = false
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
		if bp.Descriptors[i].RefCount > 0 {
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

		// Reset buffer and descriptor
		buffer.UsageCount = 0
		buffer.Referenced = false
		buffer.IsDirty = false
		buffer.Tag = BufferTag{}
		bp.Descriptors[i].State = BufferStateInvalid
		bp.Descriptors[i].RefCount = 0
		bp.Descriptors[i].Tag = BufferTag{}

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
		stats.Hits, stats.Misses, stats.HitRatio, stats.Evictions, bp.WriteCount.Load())

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
		if bp.Descriptors[i].RefCount > 0 {
			inUseCount++
			bp.Logger.Warnf("Buffer %d still has refCount %d during shutdown",
				i, bp.Descriptors[i].RefCount)
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
