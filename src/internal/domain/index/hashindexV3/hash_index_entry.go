package hashindexV3

/*
HASH INDEX ENTRY - LSM-STYLE APPEND-ONLY ENTRY STRUCTURE

This file defines the core data structure for LSM-style hash index entries.
Unlike traditional hash indexes that use file offsets, this implementation uses
append-only entries with temporal ordering, similar to bundle storage.

KEY CONCEPTS:
- Append-only: Entries are never modified in place, only appended
- Temporal ordering: Each entry has a timestamp and sequence number
- Tombstones: Deleted entries are marked with tombstone flag, not removed
- No file offsets: References documents by UUID, not file position

DESIGN PRINCIPLES:
- Single Responsibility: This file only defines entry data representation
- Open/Closed: New entry types can extend the base structure
- DRY: Reuses patterns from bundle append-only storage

ENTRY LIFECYCLE:
1. Insert: Append new entry with current timestamp
2. Update: Append new version with higher sequence number
3. Delete: Append tombstone entry (Deleted=true)
4. Compaction: Remove old versions and tombstones during merge

TODO: Future extensions
- Add compression flag for large entries
- Add encryption metadata for secure indexes
- Add entry size field for variable-length optimization
- Support batch entry writes
*/

import (
	"encoding/binary"
	"fmt"
	"io"
	"syndrdb/src/pkg/constants"
	"time"

	"github.com/cespare/xxhash/v2"
)

// Magic numbers for entry identification (follows bundle storage pattern)
const (
	// EntryMagicNumber identifies a valid hash index entry (0xFEEDBEEF)
	EntryMagicNumber uint32 = 0xFEEDBEEF

	// TombstoneMagicNumber identifies a deleted entry (0xDEADDEAD)
	// This matches the tombstone pattern used in bundle storage for consistency
	TombstoneMagicNumber uint32 = 0xDEADDEAD

	// EntryHeaderSize is the fixed size of the entry header in bytes
	// Magic(4) + EntryID(8) + Timestamp(8) + Sequence(8) + HashValue(4) +
	// BucketNum(4) + PageID(4) + Checksum(4) + Deleted(1) + Reserved(3) = 48 bytes
	EntryHeaderSize = 48

	// MaxKeySize is the maximum size for a key value (64KB)
	MaxKeySize = 65536

	// MaxDocumentIDSize is the maximum size for a document ID (typically UUID = 36 bytes)
	MaxDocumentIDSize = 256
)

// HashIndexEntry represents a single append-only index entry
// This is the fundamental unit of the LSM-style hash index
type HashIndexEntry struct {
	// Entry identification (8 bytes total)
	EntryID uint64 // Sequential entry number within the index file

	// Key-value mapping (variable length)
	KeyValue   string // The indexed key value (e.g., field value being indexed)
	DocumentID string // Target document UUID

	// Document location tracking (4 bytes)
	// CRITICAL: Stores the physical page ID where the document resides
	// This eliminates unreliable hash-based page calculations and enables O(1) document lookups
	PageID uint32 // Physical page number in bundle storage (0-based)

	// Temporal information for LSM ordering (16 bytes total)
	Timestamp time.Time // When this entry was written (8 bytes as Unix nano)
	Sequence  uint64    // Global sequence number for ordering conflicts (8 bytes)

	// Lifecycle management (1 byte + 3 padding = 4 bytes)
	Deleted bool // True if this is a tombstone entry

	// Hashing information (8 bytes total)
	HashValue uint32 // Cached hash value for the key (4 bytes)
	BucketNum uint32 // Target bucket number for locality (4 bytes)

	// Integrity check (4 bytes)
	Checksum uint32 // CRC32 checksum of entry data
}

// NewHashIndexEntry creates a new hash index entry
// Parameters:
//   - keyValue: The value being indexed
//   - documentID: The UUID of the document
//   - pageID: The physical page number where the document resides
//   - sequence: Global sequence number for ordering
//
// Returns a new HashIndexEntry with computed hash and timestamp
func NewHashIndexEntry(keyValue, documentID string, pageID uint32, sequence uint64) *HashIndexEntry {
	entry := &HashIndexEntry{
		KeyValue:   keyValue,
		DocumentID: documentID,
		PageID:     pageID,
		Timestamp:  time.Now(),
		Sequence:   sequence,
		Deleted:    false,
	}

	// Compute hash and bucket for this entry
	entry.HashValue = ComputeHash(keyValue)
	entry.BucketNum = 0 // Will be set by bucket manager

	// Compute checksum of entry data
	entry.Checksum = entry.computeChecksum()

	return entry
}

// NewTombstoneEntry creates a tombstone entry for deletion
// Parameters:
//   - keyValue: The value being deleted
//   - sequence: Global sequence number
//
// Returns a new HashIndexEntry marked as deleted
func NewTombstoneEntry(keyValue string, sequence uint64) *HashIndexEntry {
	entry := &HashIndexEntry{
		KeyValue:   keyValue,
		DocumentID: "", // No document ID for tombstones
		Timestamp:  time.Now(),
		Sequence:   sequence,
		Deleted:    true,
	}

	entry.HashValue = ComputeHash(keyValue)
	entry.BucketNum = 0 // Will be set by bucket manager
	entry.Checksum = entry.computeChecksum()

	return entry
}

// Serialize converts the entry to binary format for disk storage
// Format:
//   - Magic number (4 bytes) - EntryMagicNumber or TombstoneMagicNumber
//   - EntryID (8 bytes)
//   - Timestamp (8 bytes) - Unix nanoseconds
//   - Sequence (8 bytes)
//   - HashValue (4 bytes)
//   - BucketNum (4 bytes)
//   - Checksum (4 bytes)
//   - Deleted flag (1 byte)
//   - Reserved (3 bytes) - for future use/alignment
//   - KeyLength (4 bytes)
//   - Key data (variable)
//   - DocumentIDLength (4 bytes)
//   - DocumentID data (variable)
//
// Returns byte slice containing serialized entry
func (e *HashIndexEntry) Serialize() ([]byte, error) {
	// Validate sizes
	if len(e.KeyValue) > MaxKeySize {
		return nil, fmt.Errorf("key size %d exceeds maximum %d", len(e.KeyValue), MaxKeySize)
	}
	if len(e.DocumentID) > MaxDocumentIDSize {
		return nil, fmt.Errorf("document ID size %d exceeds maximum %d", len(e.DocumentID), MaxDocumentIDSize)
	}

	// Recompute checksum before serialization (in case entry was modified)
	e.Checksum = e.computeChecksum()

	// Calculate total size
	keyBytes := []byte(e.KeyValue)
	docIDBytes := []byte(e.DocumentID)
	totalSize := EntryHeaderSize + 4 + len(keyBytes) + 4 + len(docIDBytes)

	buffer := make([]byte, totalSize)
	offset := 0

	// Write magic number (determines if entry or tombstone)
	magic := EntryMagicNumber
	if e.Deleted {
		magic = TombstoneMagicNumber
	}
	binary.LittleEndian.PutUint32(buffer[offset:], magic)
	offset += 4

	// Write fixed-size header fields
	binary.LittleEndian.PutUint64(buffer[offset:], e.EntryID)
	offset += 8

	binary.LittleEndian.PutUint64(buffer[offset:], uint64(e.Timestamp.UnixNano()))
	offset += 8

	binary.LittleEndian.PutUint64(buffer[offset:], e.Sequence)
	offset += 8

	binary.LittleEndian.PutUint32(buffer[offset:], e.HashValue)
	offset += 4

	binary.LittleEndian.PutUint32(buffer[offset:], e.BucketNum)
	offset += 4

	binary.LittleEndian.PutUint32(buffer[offset:], e.PageID)
	offset += 4

	binary.LittleEndian.PutUint32(buffer[offset:], e.Checksum)
	offset += 4

	// Write deleted flag (1 byte) + 3 reserved bytes
	if e.Deleted {
		buffer[offset] = 1
	} else {
		buffer[offset] = 0
	}
	offset += 4 // Include 3 reserved bytes

	// Write variable-length key
	binary.LittleEndian.PutUint32(buffer[offset:], uint32(len(keyBytes)))
	offset += 4
	copy(buffer[offset:], keyBytes)
	offset += len(keyBytes)

	// Write variable-length document ID
	binary.LittleEndian.PutUint32(buffer[offset:], uint32(len(docIDBytes)))
	offset += 4
	//docIDStart := offset
	copy(buffer[offset:], docIDBytes)

	// DEBUG: Verify what we're about to write
	// fmt.Printf("SERIALIZE DEBUG:\n")
	// fmt.Printf("  KeyValue: %q (len=%d)\n", e.KeyValue, len(e.KeyValue))
	// fmt.Printf("  DocumentID: %q (len=%d)\n", e.DocumentID, len(e.DocumentID))
	// fmt.Printf("  docIDBytes: %q (len=%d)\n", string(docIDBytes), len(docIDBytes))
	// fmt.Printf("  Buffer total size: %d\n", len(buffer))
	// fmt.Printf("  DocumentID written at offset %d for %d bytes\n", docIDStart, len(docIDBytes))
	// fmt.Printf("  Buffer[%d:%d] (docID in buffer): %q\n", docIDStart, docIDStart+len(docIDBytes), string(buffer[docIDStart:docIDStart+len(docIDBytes)]))

	// Show tail of buffer safely
	tailSize := 30
	if len(buffer) < tailSize {
		tailSize = len(buffer)
	}
	//fmt.Printf("  Buffer tail (last %d bytes): % X\n", tailSize, buffer[len(buffer)-tailSize:])

	return buffer, nil
}

// Deserialize reconstructs an entry from binary data
// Returns the deserialized entry and the number of bytes consumed
func DeserializeEntry(data []byte) (*HashIndexEntry, int, error) {
	if len(data) < EntryHeaderSize {
		return nil, 0, fmt.Errorf("insufficient data for entry header: need %d, got %d", EntryHeaderSize, len(data))
	}

	offset := 0
	entry := &HashIndexEntry{}

	// Read and validate magic number
	magic := binary.LittleEndian.Uint32(data[offset:])
	offset += 4

	if magic == TombstoneMagicNumber {
		entry.Deleted = true
	} else if magic == EntryMagicNumber {
		entry.Deleted = false
	} else {
		return nil, 0, fmt.Errorf("invalid magic number: 0x%X (expected 0x%X or 0x%X)",
			magic, EntryMagicNumber, TombstoneMagicNumber)
	}

	// Read fixed-size header fields
	entry.EntryID = binary.LittleEndian.Uint64(data[offset:])
	offset += 8

	timestampNano := binary.LittleEndian.Uint64(data[offset:])
	entry.Timestamp = time.Unix(0, int64(timestampNano))
	offset += 8

	entry.Sequence = binary.LittleEndian.Uint64(data[offset:])
	offset += 8

	entry.HashValue = binary.LittleEndian.Uint32(data[offset:])
	offset += 4

	entry.BucketNum = binary.LittleEndian.Uint32(data[offset:])
	offset += 4

	entry.PageID = binary.LittleEndian.Uint32(data[offset:])
	offset += 4

	entry.Checksum = binary.LittleEndian.Uint32(data[offset:])
	offset += 4

	// Read deleted flag (skip 3 reserved bytes)
	deletedFlag := data[offset]
	offset += 4 // Skip 1 byte flag + 3 reserved

	// Verify deleted flag matches magic number
	if (deletedFlag == 1) != entry.Deleted {
		return nil, 0, fmt.Errorf("deleted flag mismatch: flag=%v, magic indicates=%v",
			deletedFlag == 1, entry.Deleted)
	}

	// Read variable-length key
	if len(data) < offset+4 {
		return nil, 0, fmt.Errorf("insufficient data for key length")
	}
	keyLen := binary.LittleEndian.Uint32(data[offset:])
	offset += 4

	if keyLen > MaxKeySize {
		return nil, 0, fmt.Errorf("key length %d exceeds maximum %d", keyLen, MaxKeySize)
	}

	if len(data) < offset+int(keyLen) {
		return nil, 0, fmt.Errorf("insufficient data for key: need %d, got %d", keyLen, len(data)-offset)
	}
	entry.KeyValue = string(data[offset : offset+int(keyLen)])
	offset += int(keyLen)

	// Read variable-length document ID
	if len(data) < offset+4 {
		return nil, 0, fmt.Errorf("insufficient data for document ID length")
	}
	docIDLen := binary.LittleEndian.Uint32(data[offset:])
	offset += 4

	if docIDLen > MaxDocumentIDSize {
		return nil, 0, fmt.Errorf("document ID length %d exceeds maximum %d", docIDLen, MaxDocumentIDSize)
	}

	if len(data) < offset+int(docIDLen) {
		return nil, 0, fmt.Errorf("insufficient data for document ID: need %d, got %d", docIDLen, len(data)-offset)
	}
	entry.DocumentID = string(data[offset : offset+int(docIDLen)])
	offset += int(docIDLen)

	// Verify checksum
	expectedChecksum := entry.computeChecksum()
	if entry.Checksum != expectedChecksum {
		// DEBUG: Log all fields used in checksum computation
		//fmt.Printf("CHECKSUM MISMATCH DEBUG:\n")
		//fmt.Printf("  Stored checksum: 0x%X\n", entry.Checksum)
		//fmt.Printf("  Computed checksum: 0x%X\n", expectedChecksum)
		//fmt.Printf("  Sequence: %d\n", entry.Sequence)
		//fmt.Printf("  HashValue: 0x%X\n", entry.HashValue)
		//fmt.Printf("  BucketNum: %d\n", entry.BucketNum)
		//fmt.Printf("  PageID: %d\n", entry.PageID)
		//fmt.Printf("  Deleted: %v\n", entry.Deleted)
		//fmt.Printf("  KeyValue: %q (len=%d)\n", entry.KeyValue, len(entry.KeyValue))
		//fmt.Printf("  DocumentID: %q (len=%d)\n", entry.DocumentID, len(entry.DocumentID))

		return nil, 0, fmt.Errorf("checksum mismatch: stored=0x%X, computed=0x%X",
			entry.Checksum, expectedChecksum)
	}

	return entry, offset, nil
}

// DeserializeEntryFromReader reads and deserializes an entry from an io.Reader
// This is useful for streaming reads during compaction
//
// Returns:
//   - entry: The deserialized entry
//   - bytesRead: Number of bytes consumed
//   - error: Any error encountered (including io.EOF)
func DeserializeEntryFromReader(reader io.Reader) (*HashIndexEntry, int, error) {
	// Read header first to determine total size
	headerBuf := make([]byte, EntryHeaderSize)
	n, err := io.ReadFull(reader, headerBuf)
	if err != nil {
		return nil, n, err
	}

	// Read key length
	keyLenBuf := make([]byte, 4)
	n2, err := io.ReadFull(reader, keyLenBuf)
	if err != nil {
		return nil, n + n2, err
	}
	keyLen := binary.LittleEndian.Uint32(keyLenBuf)

	if keyLen > MaxKeySize {
		return nil, n + n2, fmt.Errorf("key length %d exceeds maximum %d", keyLen, MaxKeySize)
	}

	// Read key data
	keyBuf := make([]byte, keyLen)
	n3, err := io.ReadFull(reader, keyBuf)
	if err != nil {
		return nil, n + n2 + n3, err
	}

	// Read document ID length
	docLenBuf := make([]byte, 4)
	n4, err := io.ReadFull(reader, docLenBuf)
	if err != nil {
		return nil, n + n2 + n3 + n4, err
	}
	docLen := binary.LittleEndian.Uint32(docLenBuf)

	if docLen > MaxDocumentIDSize {
		return nil, n + n2 + n3 + n4, fmt.Errorf("document ID length %d exceeds maximum %d", docLen, MaxDocumentIDSize)
	}

	// Read document ID data
	docBuf := make([]byte, docLen)
	n5, err := io.ReadFull(reader, docBuf)
	if err != nil {
		return nil, n + n2 + n3 + n4 + n5, err
	}

	// Reconstruct full buffer and use existing DeserializeEntry
	totalSize := n + n2 + int(keyLen) + n3 + n4 + int(docLen) + n5
	fullBuf := make([]byte, totalSize)

	copy(fullBuf[0:], headerBuf)
	copy(fullBuf[n:], keyLenBuf)
	copy(fullBuf[n+n2:], keyBuf)
	copy(fullBuf[n+n2+n3:], docLenBuf)
	copy(fullBuf[n+n2+n3+n4:], docBuf)

	entry, _, err := DeserializeEntry(fullBuf)
	return entry, totalSize, err
}

// computeChecksum calculates a CRC32 checksum of entry data
// This helps detect corruption in stored entries
//
// NOTE: EntryID is NOT included because it's storage metadata, not entry data
// EntryID can vary across compactions/rewrites while entry content remains the same
//
// NOTE: Timestamp is NOT included because it's temporal metadata, not logical data
// Timestamp changes on every write but doesn't affect data integrity
//
// IMPORTANT: Uses same encoding as Serialize() to ensure checksums match
// Must use PutUint* methods with temp buffer, not binary.Write (which uses reflection)
func (e *HashIndexEntry) computeChecksum() uint32 {
	h := xxhash.New()
	buf := make([]byte, 8) // Reusable buffer for encoding

	// Hash only the logical entry data (not storage/temporal metadata)
	// This ensures the checksum validates data corruption, not timing differences

	// Sequence
	binary.LittleEndian.PutUint64(buf, e.Sequence)
	h.Write(buf)

	// HashValue (4 bytes)
	binary.LittleEndian.PutUint32(buf[:4], e.HashValue)
	h.Write(buf[:4])

	// BucketNum (4 bytes)
	binary.LittleEndian.PutUint32(buf[:4], e.BucketNum)
	h.Write(buf[:4])

	// PageID (4 bytes)
	binary.LittleEndian.PutUint32(buf[:4], e.PageID)
	h.Write(buf[:4])

	// Deleted flag (1 byte) + 3 reserved bytes = 4 bytes total (matching serialization)
	deletedBuf := make([]byte, 4)
	if e.Deleted {
		deletedBuf[0] = 1
	} else {
		deletedBuf[0] = 0
	}
	// deletedBuf[1], [2], [3] are already 0 (reserved bytes)
	h.Write(deletedBuf)

	// Variable-length fields (raw bytes, no length prefix in checksum)
	h.Write([]byte(e.KeyValue))
	h.Write([]byte(e.DocumentID))

	// xxHash returns uint64, truncate to uint32 for compatibility
	return uint32(h.Sum64())
}

// IsNewer returns true if this entry is newer than the other entry
// Comparison is based on sequence number (primary) and timestamp (tie-breaker)
func (e *HashIndexEntry) IsNewer(other *HashIndexEntry) bool {
	if e.Sequence != other.Sequence {
		return e.Sequence > other.Sequence
	}
	return e.Timestamp.After(other.Timestamp)
}

// String returns a human-readable representation of the entry
func (e *HashIndexEntry) String() string {
	status := "ACTIVE"
	if e.Deleted {
		status = "TOMBSTONE"
	}
	return fmt.Sprintf("Entry[%d] %s: key=%q doc=%s seq=%d hash=0x%X bucket=%d ts=%s",
		e.EntryID, status, e.KeyValue, e.DocumentID, e.Sequence,
		e.HashValue, e.BucketNum, e.Timestamp.Format(time.RFC3339))
}

// ComputeHash computes a 32-bit hash value for the given key using xxHash.
// xxHash is chosen for its exceptional speed (<10ns per hash) while maintaining
// excellent distribution properties for hash table applications.
//
// The function uses xxHash64 and truncates to uint32 for compatibility with
// the HashIndexEntry.HashValue field. This provides good distribution across
// 2^32 buckets while keeping the hash value compact.
//
// This function is inline-able and has zero allocations.
func ComputeHash(key string) uint32 {
	// Use xxHash64 for speed, truncate to uint32
	// Lower 32 bits maintain excellent distribution properties
	return uint32(xxhash.Sum64String(key))
}

// ComputeBucketNum determines which bucket a hash value belongs to.
// Uses simple modulo operation to map hash values to bucket range [0, numBuckets).
//
// MED-011: Added input validation for bucket count to prevent invalid operations.
// The modulo operation itself is safe and cannot overflow.
//
// Parameters:
//   - hashValue: The 32-bit hash of the key (from ComputeHash)
//   - numBuckets: Total number of buckets (typically 256)
//
// Returns:
//   - Bucket number in range [0, numBuckets)
//   - Error if numBuckets is zero or invalid
func ComputeBucketNum(hashValue uint32, numBuckets uint32) (uint32, error) {
	// MED-011: Validate bucket count (cannot be zero for modulo operation)
	if numBuckets == 0 {
		return 0, fmt.Errorf("bucket count cannot be zero")
	}
	if err := constants.CheckBucketCount(numBuckets); err != nil {
		return 0, fmt.Errorf("invalid bucket count: %w", err)
	}
	// Modulo operation is safe - it always returns a value in [0, numBuckets)
	return hashValue % numBuckets, nil
}

// ComputeHashAndBucket is a convenience function that computes both hash and bucket
// in a single call. Useful for operations that need both values.
//
// Parameters:
//   - key: The key to hash
//   - numBuckets: Total number of buckets
//
// Returns:
//   - hashValue: 32-bit hash of the key
//   - bucketNum: Bucket number in range [0, numBuckets)
func ComputeHashAndBucket(key string, numBuckets uint32) (hashValue uint32, bucketNum uint32, err error) {
	hashValue = ComputeHash(key)
	bucketNum, err = ComputeBucketNum(hashValue, numBuckets)
	return
}

// TODO: Future extensions
// - Add method to compare entries for merge/compaction
// - Add method to validate entry consistency
// - Add support for multi-field composite keys
// - Add entry size calculation for storage optimization
