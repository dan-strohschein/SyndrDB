package bundlestore

import (
	"fmt"
	"os"
	"sync"
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
}

// NewWriteBuffer creates a new write buffer for the specified file
func NewWriteBuffer(file *os.File, bufferSize int) *WriteBuffer {
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
func (wb *WriteBuffer) swapAndFlush(doSync bool) error {
	for wb.flushInProgress {
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
	wb.mutex.Unlock()
	err := wb.doFlushToFile(data, doSync)
	wb.mutex.Lock()
	wb.backBuffer = wb.backBuffer[:0]
	wb.flushInProgress = false
	wb.flushCond.Broadcast()
	if err != nil {
		wb.flushErr = err
		return err
	}
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

// Close flushes any remaining data and closes the underlying file
func (wb *WriteBuffer) Close() error {
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
