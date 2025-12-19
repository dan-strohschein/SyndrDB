package bundlestore

import (
	"encoding/json"
	"fmt"
	"os"
	"sync"
	"syndrdb/src/internal/domain/models"
	"time"
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
type WriteBuffer struct {
	file         *os.File
	buffer       []byte
	bufferSize   int
	flushSize    int
	lastFlush    time.Time
	flushTimeout time.Duration
	mutex        sync.Mutex

	// Transaction-aware tracking
	bufferedDocs  map[string]*BufferedDocument // documentID -> BufferedDocument
	discardedDocs map[string]bool              // documentID -> is discarded
}

// NewWriteBuffer creates a new write buffer for the specified file
func NewWriteBuffer(file *os.File, bufferSize int) *WriteBuffer {
	return &WriteBuffer{
		file:          file,
		buffer:        make([]byte, 0, bufferSize),
		bufferSize:    bufferSize,
		flushSize:     bufferSize / 2, // Flush when half full
		lastFlush:     time.Now(),
		flushTimeout:  100 * time.Millisecond, // Flush after 100ms
		bufferedDocs:  make(map[string]*BufferedDocument),
		discardedDocs: make(map[string]bool),
	}
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

	// Flush if buffer is getting full or timeout reached
	if len(wb.buffer) >= wb.flushSize || time.Since(wb.lastFlush) >= wb.flushTimeout {
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

// Sync forces a sync to disk (fsync) to ensure durability
// This can be called after Flush() to ensure OS has written data to physical disk
func (wb *WriteBuffer) Sync() error {
	wb.mutex.Lock()
	defer wb.mutex.Unlock()

	if wb.file == nil {
		return fmt.Errorf("write buffer file is nil")
	}
	return wb.file.Sync()
}

// flushInternal performs the actual flush operation (must be called with mutex held)
func (wb *WriteBuffer) flushInternal() error {
	if len(wb.buffer) == 0 {
		return nil
	}

	// CRITICAL: Handle short writes - file.Write() can return n < len(buffer) with err == nil
	// We must loop until all bytes are written to prevent data corruption from partial writes
	toWrite := wb.buffer
	written := 0
	for written < len(toWrite) {
		n, err := wb.file.Write(toWrite[written:])
		if err != nil {
			return fmt.Errorf("write failed after %d of %d bytes: %w", written, len(toWrite), err)
		}
		if n == 0 {
			// Write returned 0 with no error - this should never happen but signals a problem
			return fmt.Errorf("write returned 0 bytes written with no error (stuck write)")
		}
		written += n
	}

	// Force OS to flush to disk - CRITICAL for durability
	err := wb.file.Sync()
	if err != nil {
		return err
	}

	// Reset buffer and transaction tracking
	wb.buffer = wb.buffer[:0]
	wb.bufferedDocs = make(map[string]*BufferedDocument)
	// NOTE: Don't clear discardedDocs - these need to persist for post-rollback cleanup
	// They'll be cleared after physical deletion in post-rollback cleanup
	wb.lastFlush = time.Now()

	return nil
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
	if err := json.Unmarshal(jsonBytes, &doc); err != nil {
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
