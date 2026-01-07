package journal

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"time"
)

// WAL_BINARY_MAGIC is the magic number identifying binary WAL entries
const WAL_BINARY_MAGIC = 0x57414C42 // "WALB" in binary

// WAL_BINARY_VERSION is the current binary format version
const WAL_BINARY_VERSION = uint32(1)

// SerializeWALEntryBinary converts a WALEntry to compact binary format
// This replaces the JSON marshaling for significantly better performance
func (wal *WriteAheadLog) SerializeWALEntryBinary(entry WALEntry) ([]byte, error) {
	// Calculate variable field lengths
	txIDBytes := []byte(entry.TxID)
	bundleNameBytes := []byte(entry.BundleName)
	docIDBytes := []byte(entry.DocumentID)
	beforeBytes := []byte(entry.BeforeData)
	afterBytes := []byte(entry.AfterData)
	metaBytes := []byte(entry.Metadata)

	// Validate field lengths
	if len(txIDBytes) > 0xFFFF {
		return nil, fmt.Errorf("transaction ID too long: %d bytes", len(txIDBytes))
	}
	if len(bundleNameBytes) > 0xFFFF {
		return nil, fmt.Errorf("bundle name too long: %d bytes", len(bundleNameBytes))
	}
	if len(docIDBytes) > 0xFFFF {
		return nil, fmt.Errorf("document ID too long: %d bytes", len(docIDBytes))
	}
	if len(beforeBytes) > 0xFFFFFFFF {
		return nil, fmt.Errorf("before data too long: %d bytes", len(beforeBytes))
	}
	if len(afterBytes) > 0xFFFFFFFF {
		return nil, fmt.Errorf("after data too long: %d bytes", len(afterBytes))
	}
	if len(metaBytes) > 0xFFFF {
		return nil, fmt.Errorf("metadata too long: %d bytes", len(metaBytes))
	}

	// Calculate total size for buffer allocation
	totalSize := 53 + len(txIDBytes) + len(bundleNameBytes) + len(docIDBytes) +
		len(beforeBytes) + len(afterBytes) + len(metaBytes)

	// Pre-allocate buffer for efficiency
	buf := bytes.NewBuffer(make([]byte, 0, totalSize))

	// Write fixed header fields individually to avoid struct padding issues
	if err := binary.Write(buf, binary.LittleEndian, uint32(WAL_BINARY_MAGIC)); err != nil {
		return nil, fmt.Errorf("failed to write magic: %w", err)
	}
	if err := binary.Write(buf, binary.LittleEndian, uint32(WAL_BINARY_VERSION)); err != nil {
		return nil, fmt.Errorf("failed to write version: %w", err)
	}
	if err := binary.Write(buf, binary.LittleEndian, entry.LSN); err != nil {
		return nil, fmt.Errorf("failed to write LSN: %w", err)
	}
	if err := binary.Write(buf, binary.LittleEndian, entry.Timestamp.Unix()); err != nil {
		return nil, fmt.Errorf("failed to write timestamp: %w", err)
	}
	if err := binary.Write(buf, binary.LittleEndian, int32(entry.Timestamp.Nanosecond())); err != nil {
		return nil, fmt.Errorf("failed to write timestamp ns: %w", err)
	}
	if err := binary.Write(buf, binary.LittleEndian, uint16(len(txIDBytes))); err != nil {
		return nil, fmt.Errorf("failed to write TxID len: %w", err)
	}
	if err := binary.Write(buf, binary.LittleEndian, uint8(entry.Operation)); err != nil {
		return nil, fmt.Errorf("failed to write operation: %w", err)
	}
	if err := binary.Write(buf, binary.LittleEndian, uint16(len(bundleNameBytes))); err != nil {
		return nil, fmt.Errorf("failed to write bundle name len: %w", err)
	}
	if err := binary.Write(buf, binary.LittleEndian, uint16(len(docIDBytes))); err != nil {
		return nil, fmt.Errorf("failed to write doc ID len: %w", err)
	}
	if err := binary.Write(buf, binary.LittleEndian, uint32(len(beforeBytes))); err != nil {
		return nil, fmt.Errorf("failed to write before data len: %w", err)
	}
	if err := binary.Write(buf, binary.LittleEndian, uint32(len(afterBytes))); err != nil {
		return nil, fmt.Errorf("failed to write after data len: %w", err)
	}
	if err := binary.Write(buf, binary.LittleEndian, uint16(len(metaBytes))); err != nil {
		return nil, fmt.Errorf("failed to write metadata len: %w", err)
	}
	if err := binary.Write(buf, binary.LittleEndian, entry.Checksum); err != nil {
		return nil, fmt.Errorf("failed to write checksum: %w", err)
	}
	// Write 4 bytes padding for alignment
	if err := binary.Write(buf, binary.LittleEndian, uint32(0)); err != nil {
		return nil, fmt.Errorf("failed to write padding: %w", err)
	}

	// Write variable-length fields in order
	buf.Write(txIDBytes)
	buf.Write(bundleNameBytes)
	buf.Write(docIDBytes)
	buf.Write(beforeBytes)
	buf.Write(afterBytes)
	buf.Write(metaBytes)

	return buf.Bytes(), nil
}

// DeserializeWALEntryBinary converts binary data back to WALEntry
// This replaces JSON unmarshaling for faster WAL recovery
func (wal *WriteAheadLog) DeserializeWALEntryBinary(data []byte) (*WALEntry, error) {
	if len(data) < 53 {
		return nil, fmt.Errorf("binary WAL entry too short: %d bytes", len(data))
	}

	buf := bytes.NewReader(data)

	// Read fixed header fields individually
	var magic uint32
	if err := binary.Read(buf, binary.LittleEndian, &magic); err != nil {
		return nil, fmt.Errorf("failed to read magic: %w", err)
	}
	if magic != WAL_BINARY_MAGIC {
		return nil, fmt.Errorf("invalid magic number: expected %d, got %d", WAL_BINARY_MAGIC, magic)
	}

	var version uint32
	if err := binary.Read(buf, binary.LittleEndian, &version); err != nil {
		return nil, fmt.Errorf("failed to read version: %w", err)
	}
	if version != WAL_BINARY_VERSION {
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

	var timestampNs int32
	if err := binary.Read(buf, binary.LittleEndian, &timestampNs); err != nil {
		return nil, fmt.Errorf("failed to read timestamp ns: %w", err)
	}

	var txIDLen uint16
	if err := binary.Read(buf, binary.LittleEndian, &txIDLen); err != nil {
		return nil, fmt.Errorf("failed to read TxID len: %w", err)
	}

	var operation uint8
	if err := binary.Read(buf, binary.LittleEndian, &operation); err != nil {
		return nil, fmt.Errorf("failed to read operation: %w", err)
	}

	var bundleNameLen uint16
	if err := binary.Read(buf, binary.LittleEndian, &bundleNameLen); err != nil {
		return nil, fmt.Errorf("failed to read bundle name len: %w", err)
	}

	var docIDLen uint16
	if err := binary.Read(buf, binary.LittleEndian, &docIDLen); err != nil {
		return nil, fmt.Errorf("failed to read doc ID len: %w", err)
	}

	var beforeLen uint32
	if err := binary.Read(buf, binary.LittleEndian, &beforeLen); err != nil {
		return nil, fmt.Errorf("failed to read before data len: %w", err)
	}

	var afterLen uint32
	if err := binary.Read(buf, binary.LittleEndian, &afterLen); err != nil {
		return nil, fmt.Errorf("failed to read after data len: %w", err)
	}

	var metaLen uint16
	if err := binary.Read(buf, binary.LittleEndian, &metaLen); err != nil {
		return nil, fmt.Errorf("failed to read metadata len: %w", err)
	}

	var checksum uint32
	if err := binary.Read(buf, binary.LittleEndian, &checksum); err != nil {
		return nil, fmt.Errorf("failed to read checksum: %w", err)
	}

	// Skip padding
	var padding uint32
	if err := binary.Read(buf, binary.LittleEndian, &padding); err != nil {
		return nil, fmt.Errorf("failed to read padding: %w", err)
	}

	// Calculate expected size for validation
	expectedSize := 53 + int(txIDLen) + int(bundleNameLen) + int(docIDLen) +
		int(beforeLen) + int(afterLen) + int(metaLen)
	if len(data) != expectedSize {
		return nil, fmt.Errorf("data size mismatch: expected %d, got %d", expectedSize, len(data))
	}

	// Read variable-length fields
	txIDBytes := make([]byte, txIDLen)
	if _, err := buf.Read(txIDBytes); err != nil {
		return nil, fmt.Errorf("failed to read TxID: %w", err)
	}

	bundleNameBytes := make([]byte, bundleNameLen)
	if _, err := buf.Read(bundleNameBytes); err != nil {
		return nil, fmt.Errorf("failed to read bundle name: %w", err)
	}

	docIDBytes := make([]byte, docIDLen)
	if _, err := buf.Read(docIDBytes); err != nil {
		return nil, fmt.Errorf("failed to read document ID: %w", err)
	}

	beforeBytes := make([]byte, beforeLen)
	if _, err := buf.Read(beforeBytes); err != nil {
		return nil, fmt.Errorf("failed to read before data: %w", err)
	}

	afterBytes := make([]byte, afterLen)
	if _, err := buf.Read(afterBytes); err != nil {
		return nil, fmt.Errorf("failed to read after data: %w", err)
	}

	metaBytes := make([]byte, metaLen)
	if _, err := buf.Read(metaBytes); err != nil {
		return nil, fmt.Errorf("failed to read metadata: %w", err)
	}

	// Reconstruct timestamp with nanosecond precision
	ts := time.Unix(timestamp, int64(timestampNs))

	return &WALEntry{
		LSN:        lsn,
		Timestamp:  ts,
		TxID:       string(txIDBytes),
		Operation:  OperationType(operation),
		BundleName: string(bundleNameBytes),
		DocumentID: string(docIDBytes),
		BeforeData: string(beforeBytes),
		AfterData:  string(afterBytes),
		Metadata:   string(metaBytes),
		Checksum:   checksum,
	}, nil
}

// LogOperationBinary logs a database operation using efficient binary format
// This replaces the ASCII JSON approach with high-performance binary serialization
func (wal *WriteAheadLog) LogOperationBinary(txID string, operation OperationType, bundleName, documentID, beforeData, afterData, metadata string) error {
	wal.mutex.Lock()
	defer wal.mutex.Unlock()

	// Ensure correct file is open
	if err := wal.ensureCorrectFileOpen(); err != nil {
		return fmt.Errorf("failed to ensure WAL file open: %w", err)
	}

	// Check if we need to rotate the file
	if err := wal.checkFileRotation(); err != nil {
		return fmt.Errorf("failed to check file rotation: %w", err)
	}

	// Increment LSN
	wal.currentLSN++

	// Create WAL entry
	entry := WALEntry{
		LSN:        wal.currentLSN,
		Timestamp:  time.Now(),
		TxID:       txID,
		Operation:  operation,
		BundleName: bundleName,
		DocumentID: documentID,
		BeforeData: beforeData,
		AfterData:  afterData,
		Metadata:   metadata,
	}

	// Calculate checksum for integrity
	entry.Checksum = wal.calculateChecksum(entry)

	// Serialize entry to binary format
	binaryData, err := wal.SerializeWALEntryBinary(entry)
	if err != nil {
		return fmt.Errorf("failed to serialize WAL entry to binary: %w", err)
	}

	// Write entry length header (4 bytes) followed by binary data
	// This allows for easy reading of variable-length entries
	entryLen := uint32(len(binaryData))
	if err := binary.Write(wal.buffer, binary.LittleEndian, entryLen); err != nil {
		return fmt.Errorf("failed to write entry length: %w", err)
	}

	// Write binary data
	if _, err := wal.buffer.Write(binaryData); err != nil {
		return fmt.Errorf("failed to write binary WAL entry: %w", err)
	}

	// PERFORMANCE OPTIMIZATION: Batch flushing (PostgreSQL-style group commit)
	// Increment pending operations counter
	wal.pendingOps++

	// Determine if we should flush based on multiple triggers:
	// 1. Transaction commit with fsyncOnCommit=true (zero data loss guarantee)
	// 2. Checkpoint markers (OpCheckpointBegin, OpCheckpointComplete)
	// 3. Batch size reached (configurable, default: 100 operations)
	// 4. Time threshold exceeded (configurable, default: 100ms since last flush)
	// 5. Durability mode: strict=every op, balanced=batch with forced commits, performance=batch only

	isCommitOp := operation == OpCommitTx || operation == OpCheckpointBegin || operation == OpCheckpointComplete
	forceFlush := false

	// Strict mode: sync every operation
	if wal.durabilityMode == "strict" {
		forceFlush = true
		wal.logger.Debugf("WAL flush triggered: strict mode")
	}

	// Balanced mode: sync on commits (zero data loss) or when batch full
	if wal.durabilityMode == "balanced" {
		if wal.fsyncOnCommit && isCommitOp {
			forceFlush = true // Force immediate sync on transaction boundaries
			wal.logger.Debugf("WAL flush triggered: balanced mode commit")
		} else if wal.pendingOps >= wal.walBatchSize || time.Since(wal.lastFlush) >= wal.walMaxFlushDelay {
			forceFlush = true // Batch threshold reached
			wal.logger.Debugf("WAL flush triggered: balanced mode batch (pendingOps=%d, timeSince=%v)", wal.pendingOps, time.Since(wal.lastFlush))
		}
	}

	// Performance mode: only sync when batch full or time exceeded (no forced commits)
	if wal.durabilityMode == "performance" {
		if wal.pendingOps >= wal.walBatchSize || time.Since(wal.lastFlush) >= wal.walMaxFlushDelay {
			forceFlush = true
			wal.logger.Warnf("⚠️  WAL flush triggered in PERFORMANCE mode: pendingOps=%d, batchSize=%d, timeSince=%v, maxDelay=%v",
				wal.pendingOps, wal.walBatchSize, time.Since(wal.lastFlush), wal.walMaxFlushDelay)
		}
	}

	if forceFlush {
		flushStart := time.Now()
		if err := wal.flushUnsafe(); err != nil {
			return err
		}
		flushDuration := time.Since(flushStart)
		if flushDuration > 10*time.Millisecond {
			wal.logger.Warnf("⚠️  WAL flushUnsafe() took %v (mode=%s)", flushDuration, wal.durabilityMode)
		}
		wal.pendingOps = 0
		wal.lastFlush = time.Now()
		return nil
	}

	// Legacy fallback: Check if buffer is getting large
	if wal.buffer.Buffered() > 8192 { // 8KB buffer
		if err := wal.flushUnsafe(); err != nil {
			return err
		}
		wal.pendingOps = 0
		wal.lastFlush = time.Now()
	}

	return nil
}

// ReplayOperationsBinary replays WAL operations from binary format for recovery
// This provides much faster recovery compared to JSON parsing
func (wal *WriteAheadLog) ReplayOperationsBinary(filePath string, fromLSN uint64, replayFunc func(WALEntry) error) error {
	file, err := os.Open(filePath)
	if err != nil {
		return fmt.Errorf("failed to open WAL file: %w", err)
	}
	defer file.Close()

	reader := bufio.NewReader(file)

	for {
		// Read entry length
		var entryLen uint32
		if err := binary.Read(reader, binary.LittleEndian, &entryLen); err != nil {
			if err == io.EOF {
				break // End of file
			}
			return fmt.Errorf("failed to read entry length: %w", err)
		}

		// Validate entry length
		if entryLen == 0 || entryLen > 1024*1024*10 { // Max 10MB per entry
			return fmt.Errorf("invalid entry length: %d", entryLen)
		}

		// Read entry data
		entryData := make([]byte, entryLen)
		if _, err := io.ReadFull(reader, entryData); err != nil {
			return fmt.Errorf("failed to read entry data: %w", err)
		}

		// Deserialize entry
		entry, err := wal.DeserializeWALEntryBinary(entryData)
		if err != nil {
			wal.logger.Warnf("Failed to deserialize binary WAL entry: %v", err)
			continue
		}

		// Skip entries before the specified LSN
		if entry.LSN < fromLSN {
			continue
		}

		// Verify checksum
		expectedChecksum := wal.calculateChecksum(*entry)
		if entry.Checksum != expectedChecksum {
			wal.logger.Warnf("Checksum mismatch for LSN %d, skipping", entry.LSN)
			continue
		}

		// Call replay function
		if err := replayFunc(*entry); err != nil {
			return fmt.Errorf("replay failed for LSN %d: %w", entry.LSN, err)
		}
	}

	return nil
}

// ValidateBinaryWALFile validates the integrity of a binary WAL file
// This can be used for WAL file health checks and corruption detection
func (wal *WriteAheadLog) ValidateBinaryWALFile(filePath string) error {
	file, err := os.Open(filePath)
	if err != nil {
		return fmt.Errorf("failed to open WAL file: %w", err)
	}
	defer file.Close()

	reader := bufio.NewReader(file)
	entryCount := 0
	var lastLSN uint64

	for {
		// Read entry length
		var entryLen uint32
		if err := binary.Read(reader, binary.LittleEndian, &entryLen); err != nil {
			if err == io.EOF {
				break // End of file
			}
			return fmt.Errorf("failed to read entry length at entry %d: %w", entryCount, err)
		}

		// Validate entry length
		if entryLen == 0 || entryLen > 1024*1024*10 { // Max 10MB per entry
			return fmt.Errorf("invalid entry length at entry %d: %d", entryCount, entryLen)
		}

		// Read entry data
		entryData := make([]byte, entryLen)
		if _, err := io.ReadFull(reader, entryData); err != nil {
			return fmt.Errorf("failed to read entry data at entry %d: %w", entryCount, err)
		}

		// Deserialize and validate entry
		entry, err := wal.DeserializeWALEntryBinary(entryData)
		if err != nil {
			return fmt.Errorf("failed to deserialize entry %d: %w", entryCount, err)
		}

		// Verify checksum
		expectedChecksum := wal.calculateChecksum(*entry)
		if entry.Checksum != expectedChecksum {
			return fmt.Errorf("checksum mismatch at entry %d (LSN %d)", entryCount, entry.LSN)
		}

		// Verify LSN ordering
		if entry.LSN <= lastLSN && entryCount > 0 {
			return fmt.Errorf("LSN out of order at entry %d: %d <= %d", entryCount, entry.LSN, lastLSN)
		}
		lastLSN = entry.LSN

		entryCount++
	}

	wal.logger.Infof("Binary WAL file validation successful: %s (%d entries)", filePath, entryCount)
	return nil
}
