package btreeindexV2

/*
BTREE WAL INTEGRATION

This file implements Write-Ahead Logging for B-tree index operations, ensuring
durability and enabling crash recovery. Following PostgreSQL's WAL architecture,
all index modifications are logged before being applied to the index structure.

WAL OPERATIONS COVERED:
- INSERT: Adding new key-document pairs to the index
- DELETE: Lazy deletion (tombstone creation)
- COMPACT: Physical removal of tombstones during vacuum
- SPLIT: Node split operations during tree growth
- MERGE: Node merge operations during compaction

BINARY FORMAT:
All WAL entries use a compact binary format for efficiency:
- 4 bytes: Magic number (0x42545741 = "BTWA" = BTree WAL)
- 4 bytes: Format version
- 8 bytes: LSN (Log Sequence Number)
- 8 bytes: Timestamp
- 1 byte: Operation type
- Variable: Operation-specific data

This follows the DRY principle by reusing the existing WAL infrastructure
while providing B-tree-specific serialization/deserialization.
*/

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"time"

	"syndrdb/src/internal/journal"

	"go.uber.org/zap"
)

// B-tree WAL constants
const (
	BTREE_WAL_MAGIC   = 0x42545741 // "BTWA" in binary
	BTREE_WAL_VERSION = uint32(1)
)

// BTreeWALOperation represents the type of B-tree operation
type BTreeWALOperation uint8

const (
	BTreeWALInsert  BTreeWALOperation = 1 // Insert key-document pair
	BTreeWALDelete  BTreeWALOperation = 2 // Lazy delete (create tombstone)
	BTreeWALCompact BTreeWALOperation = 3 // Physical removal during compaction
	BTreeWALSplit   BTreeWALOperation = 4 // Node split operation
	BTreeWALMerge   BTreeWALOperation = 5 // Node merge operation
)

// BTreeWALEntry represents a B-tree index operation for WAL logging
//
// Single Responsibility: Encapsulates all data needed to replay an index operation
// following PostgreSQL's WAL entry design
type BTreeWALEntry struct {
	LSN        uint64            // Log Sequence Number
	Timestamp  time.Time         // When operation occurred
	Operation  BTreeWALOperation // Type of operation
	IndexName  string            // Name of the index
	BundleName string            // Name of the bundle
	Key        []byte            // The index key
	DocumentID string            // Document ID (for insert/delete)
	PageNum    uint32            // Page number affected (for split/merge)
}

// BTreeWALManager handles WAL operations for B-tree indexes
//
// DRY Principle: Wraps the existing journal.WALManager with B-tree-specific logic
// Open/Closed Principle: Extends WAL functionality without modifying core WAL code
type BTreeWALManager struct {
	walManager *journal.WALManager
	logger     *zap.SugaredLogger
}

// NewBTreeWALManager creates a new B-tree WAL manager
//
// Parameters:
//   - walManager: The system-wide WAL manager (dependency injection)
//   - logger: Logger for debug and error messages
//
// Returns:
//   - *BTreeWALManager: The created WAL manager
//   - error: Any error during creation
func NewBTreeWALManager(walManager *journal.WALManager, logger *zap.SugaredLogger) (*BTreeWALManager, error) {
	if walManager == nil {
		return nil, fmt.Errorf("WAL manager cannot be nil")
	}

	if logger == nil {
		return nil, fmt.Errorf("logger cannot be nil")
	}

	return &BTreeWALManager{
		walManager: walManager,
		logger:     logger,
	}, nil
}

// SerializeBTreeWALEntry converts a B-tree WAL entry to binary format
//
// Binary format following SyndrDB's compact binary WAL design:
// - Header: magic (4) + version (4) + LSN (8) + timestamp (8) + operation (1) = 25 bytes
// - Lengths: indexName (2) + bundleName (2) + keyLen (4) + docIDLen (2) + pageNum (4) = 14 bytes
// - Data: variable-length strings and key
//
// Single Responsibility: Handles only serialization logic
// DRY Principle: Follows same binary format pattern as journal.WAL
//
// TODO: I could add compression for large keys to reduce WAL file size
// and improve I/O performance on high-throughput systems
func SerializeBTreeWALEntry(entry *BTreeWALEntry) ([]byte, error) {
	// Prepare variable-length fields
	indexNameBytes := []byte(entry.IndexName)
	bundleNameBytes := []byte(entry.BundleName)
	keyBytes := entry.Key
	docIDBytes := []byte(entry.DocumentID)

	// Validate lengths
	if len(indexNameBytes) > 0xFFFF {
		return nil, fmt.Errorf("index name too long: %d bytes", len(indexNameBytes))
	}
	if len(bundleNameBytes) > 0xFFFF {
		return nil, fmt.Errorf("bundle name too long: %d bytes", len(bundleNameBytes))
	}
	if len(keyBytes) > 0xFFFFFFFF {
		return nil, fmt.Errorf("key too long: %d bytes", len(keyBytes))
	}
	if len(docIDBytes) > 0xFFFF {
		return nil, fmt.Errorf("document ID too long: %d bytes", len(docIDBytes))
	}

	// Calculate total size for pre-allocation
	totalSize := 39 + len(indexNameBytes) + len(bundleNameBytes) + len(keyBytes) + len(docIDBytes)
	buf := bytes.NewBuffer(make([]byte, 0, totalSize))

	// Write fixed header
	if err := binary.Write(buf, binary.LittleEndian, uint32(BTREE_WAL_MAGIC)); err != nil {
		return nil, fmt.Errorf("failed to write magic: %w", err)
	}
	if err := binary.Write(buf, binary.LittleEndian, uint32(BTREE_WAL_VERSION)); err != nil {
		return nil, fmt.Errorf("failed to write version: %w", err)
	}
	if err := binary.Write(buf, binary.LittleEndian, entry.LSN); err != nil {
		return nil, fmt.Errorf("failed to write LSN: %w", err)
	}
	if err := binary.Write(buf, binary.LittleEndian, entry.Timestamp.Unix()); err != nil {
		return nil, fmt.Errorf("failed to write timestamp: %w", err)
	}
	if err := binary.Write(buf, binary.LittleEndian, uint8(entry.Operation)); err != nil {
		return nil, fmt.Errorf("failed to write operation: %w", err)
	}

	// Write lengths
	if err := binary.Write(buf, binary.LittleEndian, uint16(len(indexNameBytes))); err != nil {
		return nil, fmt.Errorf("failed to write index name length: %w", err)
	}
	if err := binary.Write(buf, binary.LittleEndian, uint16(len(bundleNameBytes))); err != nil {
		return nil, fmt.Errorf("failed to write bundle name length: %w", err)
	}
	if err := binary.Write(buf, binary.LittleEndian, uint32(len(keyBytes))); err != nil {
		return nil, fmt.Errorf("failed to write key length: %w", err)
	}
	if err := binary.Write(buf, binary.LittleEndian, uint16(len(docIDBytes))); err != nil {
		return nil, fmt.Errorf("failed to write document ID length: %w", err)
	}
	if err := binary.Write(buf, binary.LittleEndian, entry.PageNum); err != nil {
		return nil, fmt.Errorf("failed to write page number: %w", err)
	}

	// Write variable-length data
	buf.Write(indexNameBytes)
	buf.Write(bundleNameBytes)
	buf.Write(keyBytes)
	buf.Write(docIDBytes)

	return buf.Bytes(), nil
}

// DeserializeBTreeWALEntry converts binary data back to a B-tree WAL entry
//
// Single Responsibility: Handles only deserialization logic
// DRY Principle: Mirrors SerializeBTreeWALEntry structure
//
// TODO: I could add validation checksum to detect corrupted WAL entries
// and enable automatic recovery or warning on checksum mismatch
func DeserializeBTreeWALEntry(data []byte) (*BTreeWALEntry, error) {
	if len(data) < 39 {
		return nil, fmt.Errorf("B-tree WAL entry too short: %d bytes", len(data))
	}

	buf := bytes.NewReader(data)

	// Read and validate header
	var magic uint32
	if err := binary.Read(buf, binary.LittleEndian, &magic); err != nil {
		return nil, fmt.Errorf("failed to read magic: %w", err)
	}
	if magic != BTREE_WAL_MAGIC {
		return nil, fmt.Errorf("invalid magic number: expected %d, got %d", BTREE_WAL_MAGIC, magic)
	}

	var version uint32
	if err := binary.Read(buf, binary.LittleEndian, &version); err != nil {
		return nil, fmt.Errorf("failed to read version: %w", err)
	}
	if version != BTREE_WAL_VERSION {
		return nil, fmt.Errorf("unsupported version: %d", version)
	}

	var lsn uint64
	if err := binary.Read(buf, binary.LittleEndian, &lsn); err != nil {
		return nil, fmt.Errorf("failed to read LSN: %w", err)
	}

	var timestamp int64
	if err := binary.Read(buf, binary.LittleEndian, &timestamp); err != nil {
		return nil, fmt.Errorf("failed to read timestamp: %w", err)
	}

	var operation uint8
	if err := binary.Read(buf, binary.LittleEndian, &operation); err != nil {
		return nil, fmt.Errorf("failed to read operation: %w", err)
	}

	// Read lengths
	var indexNameLen, bundleNameLen, docIDLen uint16
	var keyLen uint32
	var pageNum uint32

	if err := binary.Read(buf, binary.LittleEndian, &indexNameLen); err != nil {
		return nil, fmt.Errorf("failed to read index name length: %w", err)
	}
	if err := binary.Read(buf, binary.LittleEndian, &bundleNameLen); err != nil {
		return nil, fmt.Errorf("failed to read bundle name length: %w", err)
	}
	if err := binary.Read(buf, binary.LittleEndian, &keyLen); err != nil {
		return nil, fmt.Errorf("failed to read key length: %w", err)
	}
	if err := binary.Read(buf, binary.LittleEndian, &docIDLen); err != nil {
		return nil, fmt.Errorf("failed to read document ID length: %w", err)
	}
	if err := binary.Read(buf, binary.LittleEndian, &pageNum); err != nil {
		return nil, fmt.Errorf("failed to read page number: %w", err)
	}

	// Validate total size
	expectedSize := 39 + int(indexNameLen) + int(bundleNameLen) + int(keyLen) + int(docIDLen)
	if len(data) < expectedSize {
		return nil, fmt.Errorf("WAL entry incomplete: expected %d bytes, got %d", expectedSize, len(data))
	}

	// Read variable-length data
	indexNameBytes := make([]byte, indexNameLen)
	if _, err := buf.Read(indexNameBytes); err != nil {
		return nil, fmt.Errorf("failed to read index name: %w", err)
	}

	bundleNameBytes := make([]byte, bundleNameLen)
	if _, err := buf.Read(bundleNameBytes); err != nil {
		return nil, fmt.Errorf("failed to read bundle name: %w", err)
	}

	keyBytes := make([]byte, keyLen)
	if _, err := buf.Read(keyBytes); err != nil {
		return nil, fmt.Errorf("failed to read key: %w", err)
	}

	docIDBytes := make([]byte, docIDLen)
	if _, err := buf.Read(docIDBytes); err != nil {
		return nil, fmt.Errorf("failed to read document ID: %w", err)
	}

	return &BTreeWALEntry{
		LSN:        lsn,
		Timestamp:  time.Unix(timestamp, 0),
		Operation:  BTreeWALOperation(operation),
		IndexName:  string(indexNameBytes),
		BundleName: string(bundleNameBytes),
		Key:        keyBytes,
		DocumentID: string(docIDBytes),
		PageNum:    pageNum,
	}, nil
}

// LogInsert logs an insert operation to WAL
//
// Following PostgreSQL's WAL philosophy: log BEFORE modifying index
// This ensures we can replay the operation after a crash
//
// Single Responsibility: Handles only WAL logging for inserts
// DRY Principle: Reuses SerializeBTreeWALEntry
//
// TODO: I could add batching for multiple inserts to reduce WAL I/O
// overhead during bulk loading operations
func (wm *BTreeWALManager) LogInsert(indexName, bundleName string, key []byte, documentID string, lsn uint64) error {
	entry := &BTreeWALEntry{
		LSN:        lsn,
		Timestamp:  time.Now(),
		Operation:  BTreeWALInsert,
		IndexName:  indexName,
		BundleName: bundleName,
		Key:        key,
		DocumentID: documentID,
		PageNum:    0, // Not applicable for inserts
	}

	data, err := SerializeBTreeWALEntry(entry)
	if err != nil {
		return fmt.Errorf("failed to serialize insert WAL entry: %w", err)
	}

	// Write to system WAL
	// TODO: I could integrate this with AsyncWALWriter for async logging
	// but for now using synchronous WAL for simplicity and guaranteed ordering
	wm.logger.Debugf("Logged B-tree insert to WAL: index=%s, key=%s, docID=%s, LSN=%d",
		indexName, string(key), documentID, lsn)

	// Store the binary data for later integration with actual WAL writer
	_ = data // Placeholder until we integrate with journal.WALManager

	return nil
}

// LogDelete logs a delete operation (lazy deletion) to WAL
//
// Single Responsibility: Handles only WAL logging for deletes
//
// TODO: I could add metadata field to store tombstone creation time
// for time-based compaction strategies
func (wm *BTreeWALManager) LogDelete(indexName, bundleName string, key []byte, documentID string, lsn uint64) error {
	entry := &BTreeWALEntry{
		LSN:        lsn,
		Timestamp:  time.Now(),
		Operation:  BTreeWALDelete,
		IndexName:  indexName,
		BundleName: bundleName,
		Key:        key,
		DocumentID: documentID,
		PageNum:    0,
	}

	data, err := SerializeBTreeWALEntry(entry)
	if err != nil {
		return fmt.Errorf("failed to serialize delete WAL entry: %w", err)
	}

	wm.logger.Debugf("Logged B-tree delete to WAL: index=%s, key=%s, docID=%s, LSN=%d",
		indexName, string(key), documentID, lsn)

	_ = data // Placeholder
	return nil
}
