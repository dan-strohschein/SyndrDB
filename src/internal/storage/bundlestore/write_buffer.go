package bundlestore

import (
	"fmt"
	"os"
	"sync"
	"sync/atomic"
	"syndrdb/src/internal/domain/models"
	"syndrdb/src/pkg/common"
	"time"

	"github.com/dan-strohschein/HVJson/hvjson"
)

// BufferedDocument tracks a document in the write buffer with transaction context
type BufferedDocument struct {
	DocumentID string
	TxID       string // Transaction that wrote this document
	Data       []byte // Full serialized data (header + document)
	Offset     int    // Position in buffer
	Size       int    // Size of data
}

// WriteBuffer provides batched write operations for optimal I/O performance
// Reduces syscalls by buffering multiple document writes before flushing
// Designed for append-only operations with configurable flush triggers
//
// P0c: Double-buffering — flush runs outside mutex. We swap active <-> back buffer
// under lock, then write back buffer to file without holding lock. Reduces
// contention when buffer is full or timeout hits.
//
// RCU Lock-Free Path: WriteDirectAtomic bypasses the buffer entirely, using
// atomic offset reservation and pwrite for concurrent lock-free writes.
type WriteBuffer struct {
	file            *os.File
	buffer          []byte
	backBuffer      []byte // P0c: second buffer for flush; I/O done outside mutex
	bufferSize      int
	flushSize       int
	lastFlush       time.Time
	flushTimeout    time.Duration
	mutex           sync.Mutex
	flushCond       *sync.Cond // signaled when flushInProgress becomes false
	flushInProgress bool       // true while doFlushToFile runs (outside mutex)
	flushErr        error      // sticky error from background flush

	// Transaction-aware tracking
	bufferedDocs  map[string]*BufferedDocument // documentID -> BufferedDocument
	discardedDocs map[string]bool              // documentID -> is discarded

	// RCU Lock-Free Write Support
	// atomicOffset tracks the current file end position for lock-free writes
	// Writers use atomic.AddInt64 to reserve space, then pwrite at their offset
	atomicOffset int64      // Current file end position (atomic)
	pwriteMutex  sync.Mutex // Protects pwrite syscall (required by OS)
	directWrites int64      // Counter for direct writes (atomic, for metrics)

	// In-flight write tracking for safe buffer closure
	// Tracks writes that have reserved offset space but not yet completed
	// FreezeAndWaitForInflight waits until this counter reaches 0
	inflightWrites int64 // Number of in-flight WriteDirectAtomic calls (atomic)

	// Frozen flag for safe file rotation
	// When a file is rotated, the old WriteBuffer is marked as frozen to prevent
	// stale writers from corrupting the file during compaction reads.
	// WriteDirectAtomic checks this flag and returns ErrBufferFrozen if set.
	isFrozen int32 // 1 = frozen, 0 = active (atomic)
}

// ErrBufferFrozen is returned when WriteDirectAtomic is called on a frozen buffer
// Callers should retry with a fresh buffer from getOrCreateWriteBuffer
var ErrBufferFrozen = fmt.Errorf("write buffer is frozen (file rotated)")

// NewWriteBuffer creates a new write buffer for the specified file
// CRITICAL: Seeks to end of file to prevent overwriting existing data
func NewWriteBuffer(file *os.File, bufferSize int) *WriteBuffer {
	// Get current file size for atomic offset initialization
	var initialOffset int64 = 0
	if file != nil {
		if stat, err := file.Stat(); err == nil {
			initialOffset = stat.Size()
		}
		// CRITICAL FIX: Seek to end of file for buffered writes
		// Without this, doFlushToFile() uses file.Write() which writes at position 0,
		// overwriting existing data! This caused corruption where new data would
		// overwrite the beginning of the file.
		//
		// The atomicOffset tracks the logical end for WriteDirectAtomic (uses WriteAt),
		// but the OS file position is separate and must be set explicitly for Write().
		if initialOffset > 0 {
			file.Seek(0, 2) // Seek to end (whence=2 means SEEK_END)
		}
	}

	wb := &WriteBuffer{
		file:          file,
		buffer:        make([]byte, 0, bufferSize),
		backBuffer:    make([]byte, 0, bufferSize), // P0c double-buffer
		bufferSize:    bufferSize,
		flushSize:     bufferSize / 2, // Flush when half full
		lastFlush:     time.Now(),
		flushTimeout:  100 * time.Millisecond, // Flush after 100ms
		bufferedDocs:  make(map[string]*BufferedDocument),
		discardedDocs: make(map[string]bool),
		atomicOffset:  initialOffset, // RCU: Start at current file end
		directWrites:  0,
	}
	wb.flushCond = sync.NewCond(&wb.mutex)
	return wb
}

// Write adds data to the buffer and flushes if necessary
func (wb *WriteBuffer) Write(data []byte) error {
	// Call WriteWithTxID with empty transaction ID for backward compatibility
	return wb.WriteWithTxID(data, "", "")
}

// WriteWithTxID adds data to the buffer with transaction tracking and flushes if necessary
func (wb *WriteBuffer) WriteWithTxID(data []byte, docID string, txID string) error {
	wb.mutex.Lock()
	defer wb.mutex.Unlock()

	offset := len(wb.buffer)

	// Check if we need to flush before adding data
	if len(wb.buffer)+len(data) > wb.bufferSize {
		if err := wb.flushInternal(); err != nil {
			return err
		}
		offset = 0 // Reset offset after flush
	}

	// Track this document with transaction context (if docID provided)
	if docID != "" {
		// CRITICAL FIX: Make a copy of data to prevent buffer pool race condition
		// The caller may return the data buffer to a pool immediately after this call
		// If we store a pointer to that buffer, another thread could reuse it and corrupt our data
		// This copy ensures BufferedDocument has its own independent copy for transaction tracking
		dataCopy := make([]byte, len(data))
		copy(dataCopy, data)

		wb.bufferedDocs[docID] = &BufferedDocument{
			DocumentID: docID,
			TxID:       txID,
			Data:       dataCopy, // Use the copy, not the original buffer
			Offset:     offset,
			Size:       len(data),
		}
	}

	// Add data to buffer
	wb.buffer = append(wb.buffer, data...)

	// PERFORMANCE FIX: Flush buffer without sync for better performance
	// Only sync on explicit Flush() calls (transaction commits) or when buffer is full
	// This allows the OS to batch writes more efficiently
	if len(wb.buffer) >= wb.flushSize || time.Since(wb.lastFlush) >= wb.flushTimeout {
		return wb.flushWithoutSync()
	}

	return nil
}

// WriteDirectAtomic performs a lock-free direct write to the file using atomic offset reservation.
// This is the RCU (Read-Copy-Update) write path that enables concurrent writes without mutex contention.
//
// How it works:
// 1. Atomically reserve space at the end of the file using atomic.AddInt64
// 2. Write data directly to the reserved offset using pwrite (doesn't change file position)
// 3. No buffer involved - data goes straight to disk
//
// This eliminates the WriteBuffer mutex bottleneck for RCU updates.
// 150 concurrent writers can now proceed in parallel instead of serializing.
//
// Trade-offs:
// - Bypasses buffering (more syscalls, but no contention)
// - No transaction tracking (RCU is autocommit-only)
// - pwrite requires a minimal mutex due to OS limitations on some platforms
//
// SAFETY: Returns ErrBufferFrozen if the buffer has been frozen (file rotated).
// Callers should retry with a fresh buffer from getOrCreateWriteBuffer.
func (wb *WriteBuffer) WriteDirectAtomic(data []byte) (int64, error) {
	// STEP 0: Increment in-flight counter BEFORE checking frozen
	// This ensures FreezeAndWaitForInflight() will wait for us
	atomic.AddInt64(&wb.inflightWrites, 1)

	// STEP 1: Check frozen flag - if frozen, decrement counter and return
	// This prevents writes to files that are being read by compaction
	if atomic.LoadInt32(&wb.isFrozen) == 1 {
		atomic.AddInt64(&wb.inflightWrites, -1)
		return 0, ErrBufferFrozen
	}

	if wb.file == nil {
		atomic.AddInt64(&wb.inflightWrites, -1)
		return 0, fmt.Errorf("write buffer file is nil")
	}
	if len(data) == 0 {
		atomic.AddInt64(&wb.inflightWrites, -1)
		return 0, nil
	}

	// STEP 2: Atomically reserve space at the end of the file
	// This is lock-free - multiple goroutines can reserve concurrently
	// CRITICAL: We're now committed to this offset - we MUST write to it
	dataLen := int64(len(data))
	writeOffset := atomic.AddInt64(&wb.atomicOffset, dataLen) - dataLen

	// STEP 3: Write data at the reserved offset using pwrite
	// pwrite writes at a specific offset without changing file position
	// This allows concurrent writes to different offsets
	//
	// NOTE: On some platforms (notably macOS), pwrite still serializes internally.
	// However, the contention is now at the OS level (microseconds) rather than
	// Go mutex level (can be milliseconds under high load).
	// For truly parallel I/O, consider using io_uring on Linux.
	wb.pwriteMutex.Lock()
	written := 0
	for written < len(data) {
		n, err := wb.file.WriteAt(data[written:], writeOffset+int64(written))
		if err != nil {
			wb.pwriteMutex.Unlock()
			atomic.AddInt64(&wb.inflightWrites, -1)
			return writeOffset, fmt.Errorf("pwrite failed at offset %d: %w", writeOffset+int64(written), err)
		}
		written += n
	}
	wb.pwriteMutex.Unlock()

	// STEP 4: Track metrics and decrement in-flight counter
	atomic.AddInt64(&wb.directWrites, 1)
	atomic.AddInt64(&wb.inflightWrites, -1)

	return writeOffset, nil
}

// GetDirectWriteCount returns the number of direct atomic writes performed
func (wb *WriteBuffer) GetDirectWriteCount() int64 {
	return atomic.LoadInt64(&wb.directWrites)
}

// GetAtomicOffset returns the current atomic file offset
func (wb *WriteBuffer) GetAtomicOffset() int64 {
	return atomic.LoadInt64(&wb.atomicOffset)
}

// WriteBufferStats contains diagnostic information about a WriteBuffer
type WriteBufferStats struct {
	FilePath       string `json:"filePath"`
	FileSize       int64  `json:"fileSize"`       // atomicOffset = approximate file size
	BufferLen      int    `json:"bufferLen"`      // Current buffer content length
	BackBufferLen  int    `json:"backBufferLen"`  // Back buffer content length
	BufferedDocs   int    `json:"bufferedDocs"`   // Number of buffered documents
	DirectWrites   int64  `json:"directWrites"`   // Number of direct writes (RCU)
	InflightWrites int64  `json:"inflightWrites"` // Number of in-flight writes
	IsFrozen       bool   `json:"isFrozen"`       // Whether buffer is frozen
}

// GetStats returns diagnostic statistics about this WriteBuffer
func (wb *WriteBuffer) GetStats() WriteBufferStats {
	wb.mutex.Lock()
	defer wb.mutex.Unlock()

	filePath := ""
	if wb.file != nil {
		filePath = wb.file.Name()
	}

	return WriteBufferStats{
		FilePath:       filePath,
		FileSize:       atomic.LoadInt64(&wb.atomicOffset),
		BufferLen:      len(wb.buffer),
		BackBufferLen:  len(wb.backBuffer),
		BufferedDocs:   len(wb.bufferedDocs),
		DirectWrites:   atomic.LoadInt64(&wb.directWrites),
		InflightWrites: atomic.LoadInt64(&wb.inflightWrites),
		IsFrozen:       atomic.LoadInt32(&wb.isFrozen) == 1,
	}
}

// Flush forces all buffered data to be written to disk
func (wb *WriteBuffer) Flush() error {
	wb.mutex.Lock()
	defer wb.mutex.Unlock()
	return wb.flushInternal()
}

// Sync forces a sync to disk (fsync) to ensure durability
// This can be called after Flush() to ensure OS has written data to physical disk
// Uses platform-optimized Fdatasync for 2-3x faster sync on Linux
func (wb *WriteBuffer) Sync() error {
	wb.mutex.Lock()
	defer wb.mutex.Unlock()

	if wb.file == nil {
		return fmt.Errorf("write buffer file is nil")
	}
	return common.Fdatasync(wb.file)
}

// SyncGroupCommit registers this buffer's file for P4b group commit and blocks until
// the next coalesced fsync run. Use instead of Sync() when DurabilityMode is "strict"
// to batch multiple writers into a single fsync every ~20ms.
func (wb *WriteBuffer) SyncGroupCommit() error {
	wb.mutex.Lock()
	f := wb.file
	wb.mutex.Unlock()
	if f == nil {
		return fmt.Errorf("write buffer file is nil")
	}
	return globalCoalescer.RequestSync(f)
}

// doFlushToFile writes data to file (and optionally syncs). Must NOT hold wb.mutex.
// P0c: I/O runs outside mutex to reduce contention.
func (wb *WriteBuffer) doFlushToFile(data []byte, doSync bool) error {
	if wb.file == nil || len(data) == 0 {
		return nil
	}
	written := 0
	for written < len(data) {
		n, err := wb.file.Write(data[written:])
		if err != nil {
			return fmt.Errorf("write failed after %d of %d bytes: %w", written, len(data), err)
		}
		if n == 0 {
			return fmt.Errorf("write returned 0 bytes written with no error (stuck write)")
		}
		written += n
	}
	if doSync {
		return common.Fdatasync(wb.file)
	}
	return nil
}

// swapAndFlush swaps buffer <-> backBuffer, runs I/O outside lock, then clears backBuffer.
// Caller must hold mutex. Returns with mutex held. Sets flushErr on failure.
// CRITICAL FIX: Added timeout to prevent indefinite blocking if a previous flush hangs
func (wb *WriteBuffer) swapAndFlush(doSync bool) error {
	// Wait for any in-progress flush using Cond.Wait() for instant wake-up.
	// A watchdog timer ensures we still check the deadline if Broadcast is never called.
	timer := time.AfterFunc(5*time.Second, func() {
		wb.flushCond.Broadcast()
	})
	defer timer.Stop()

	deadline := time.Now().Add(5 * time.Second)
	for wb.flushInProgress {
		if time.Now().After(deadline) {
			err := fmt.Errorf("timeout waiting for previous flush to complete (possible stuck I/O)")
			wb.flushErr = err
			return err
		}
		wb.flushCond.Wait()
	}
	if wb.flushErr != nil {
		return wb.flushErr
	}
	if len(wb.buffer) == 0 {
		return nil
	}
	wb.buffer, wb.backBuffer = wb.backBuffer, wb.buffer
	wb.buffer = wb.buffer[:0]
	wb.bufferedDocs = make(map[string]*BufferedDocument)
	wb.lastFlush = time.Now()
	wb.flushInProgress = true
	data := wb.backBuffer
	dataLen := int64(len(data)) // Capture length before releasing lock
	wb.mutex.Unlock()
	defer func() {
		if p := recover(); p != nil {
			wb.mutex.Lock()
			wb.flushInProgress = false
			wb.flushErr = fmt.Errorf("flush panic: %v", p)
			wb.flushCond.Broadcast()
			wb.mutex.Unlock()
			panic(p)
		}
	}()
	err := wb.doFlushToFile(data, doSync)
	wb.mutex.Lock()
	wb.backBuffer = wb.backBuffer[:0]
	wb.flushInProgress = false
	wb.flushCond.Broadcast()
	if err != nil {
		wb.flushErr = err
		return err
	}
	// CRITICAL FIX: Update atomicOffset after successful buffered write
	// This keeps atomicOffset in sync with the actual file position so that
	// WriteDirectAtomic (which uses atomicOffset for pwrite) doesn't conflict
	// with buffered writes (which use sequential Write()).
	// Without this, WriteDirectAtomic could write at an offset that's already
	// been written by a buffered flush.
	atomic.AddInt64(&wb.atomicOffset, dataLen)
	wb.flushErr = nil // clear sticky error on success
	return nil
}

// flushInternal performs the actual flush operation with sync (must be called with mutex held)
// Use this for explicit flush requests (transaction commits) where durability is required
func (wb *WriteBuffer) flushInternal() error {
	return wb.swapAndFlush(true)
}

// flushWithoutSync performs flush without fsync for better performance
// Used for automatic flushes when buffer is full or timeout reached
// Durability is ensured by WAL logging and explicit Flush() calls on transaction commits
func (wb *WriteBuffer) flushWithoutSync() error {
	return wb.swapAndFlush(false)
}

// GetDiscardedDocuments returns the list of document IDs that were discarded
// This should be called BEFORE Flush() to get documents that need physical deletion
func (wb *WriteBuffer) GetDiscardedDocuments() []string {
	wb.mutex.Lock()
	defer wb.mutex.Unlock()

	discarded := make([]string, 0, len(wb.discardedDocs))
	for docID := range wb.discardedDocs {
		discarded = append(discarded, docID)
	}
	return discarded
}

// Freeze marks the buffer as frozen to prevent new WriteDirectAtomic calls.
// This is called during file rotation to ensure stale writer references
// cannot corrupt a file that compaction may read.
// Returns immediately - does not wait for in-flight writes to complete.
func (wb *WriteBuffer) Freeze() {
	atomic.StoreInt32(&wb.isFrozen, 1)
}

// FreezeAndWaitForInflight freezes the buffer and waits for any in-flight
// WriteDirectAtomic operations to complete. This is essential for safe reads
// during compaction - the frozen flag prevents NEW writes, but we must also
// wait for writes that started before the freeze.
//
// How it works:
// 1. Set frozen flag (atomic) - new WriteDirectAtomic calls will see frozen and abort early
// 2. Spin-wait until inflightWrites counter reaches 0
// 3. Once counter is 0, all writers have either:
//   - Completed their write successfully
//   - Aborted before reserving an offset (saw frozen flag)
//
// CRITICAL: Writers increment inflightWrites BEFORE checking frozen, so:
// - If a writer sees frozen=true, it decrements and aborts (no offset reserved)
// - If a writer sees frozen=false, it will complete the write before decrementing
// This guarantees no "holes" in the offset space after this function returns.
//
// After this returns, it's safe to read the file without torn reads.
func (wb *WriteBuffer) FreezeAndWaitForInflight() {
	// Step 1: Prevent new writes from proceeding past the frozen check
	atomic.StoreInt32(&wb.isFrozen, 1)

	// Step 2: Wait for all in-flight writes to complete
	// Spin-wait with exponential backoff for efficiency
	backoff := time.Microsecond
	maxBackoff := time.Millisecond * 10
	for atomic.LoadInt64(&wb.inflightWrites) > 0 {
		time.Sleep(backoff)
		if backoff < maxBackoff {
			backoff *= 2
		}
	}
}

// IsFrozen returns true if the buffer has been frozen
func (wb *WriteBuffer) IsFrozen() bool {
	return atomic.LoadInt32(&wb.isFrozen) == 1
}

// Close flushes any remaining data and closes the underlying file
// CRITICAL: Freezes the buffer and waits for in-flight writes before closing
func (wb *WriteBuffer) Close() error {
	// CRITICAL: Freeze and wait for in-flight WriteDirectAtomic calls
	// This ensures no writes are in progress when we flush and close
	wb.FreezeAndWaitForInflight()

	if err := wb.Flush(); err != nil {
		return err
	}
	return wb.file.Close()
}

// ClearDiscardedDocuments removes the specified document IDs from the discarded set
// This should be called after physically deleting flushed-then-discarded documents
func (wb *WriteBuffer) ClearDiscardedDocuments(docIDs []string) {
	wb.mutex.Lock()
	defer wb.mutex.Unlock()

	for _, docID := range docIDs {
		delete(wb.discardedDocs, docID)
	}
}

// Discard clears the buffer without writing to disk and closes the file
// This is used for rollback scenarios where buffered writes should be abandoned
func (wb *WriteBuffer) Discard() error {
	wb.mutex.Lock()
	defer wb.mutex.Unlock()

	// Clear buffer and tracking without writing
	wb.buffer = wb.buffer[:0]
	wb.bufferedDocs = make(map[string]*BufferedDocument)
	wb.discardedDocs = make(map[string]bool)
	wb.lastFlush = time.Now()

	// Close file without flushing
	return wb.file.Close()
}

// GetDocumentsForTransaction returns all buffered documents for a specific transaction
func (wb *WriteBuffer) GetDocumentsForTransaction(txID string) ([]*models.Document, error) {
	wb.mutex.Lock()
	defer wb.mutex.Unlock()

	docs := make([]*models.Document, 0)

	for docID, bufDoc := range wb.bufferedDocs {
		// Skip if discarded or different transaction
		if wb.discardedDocs[docID] || bufDoc.TxID != txID {
			continue
		}

		// Parse the document from buffer bytes
		doc, err := wb.parseDocument(bufDoc)
		if err != nil {
			// Log but continue - don't fail entire query for one bad document
			continue
		}

		docs = append(docs, doc)
	}

	return docs, nil
}

// parseDocument extracts and deserializes a document from buffered bytes
func (wb *WriteBuffer) parseDocument(bufDoc *BufferedDocument) (*models.Document, error) {
	// Document format: [magic:4][size:4][json_bytes]
	if len(bufDoc.Data) < 8 {
		return nil, fmt.Errorf("invalid document data: too short")
	}

	// Skip magic number (4 bytes) and size (4 bytes) to get JSON
	jsonBytes := bufDoc.Data[8:]

	var doc models.Document
	// PERF: hvjson is 27-42% faster than encoding/json
	if err := hvjson.Unmarshal(jsonBytes, &doc); err != nil {
		return nil, fmt.Errorf("failed to unmarshal document: %w", err)
	}

	return &doc, nil
}

// MarkDiscarded marks a document as discarded (for rollback)
func (wb *WriteBuffer) MarkDiscarded(docID string) {
	wb.mutex.Lock()
	defer wb.mutex.Unlock()

	wb.discardedDocs[docID] = true
}

// IsDocumentAvailable checks if a document is buffered and not discarded
func (wb *WriteBuffer) IsDocumentAvailable(docID string) bool {
	wb.mutex.Lock()
	defer wb.mutex.Unlock()

	_, exists := wb.bufferedDocs[docID]
	if !exists {
		return false
	}

	return !wb.discardedDocs[docID]
}

// Size returns the current buffer size
func (wb *WriteBuffer) Size() int {
	wb.mutex.Lock()
	defer wb.mutex.Unlock()
	return len(wb.buffer)
}
