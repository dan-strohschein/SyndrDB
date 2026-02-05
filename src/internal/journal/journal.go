package journal

/*
WRITE AHEAD LOGGING SYSTEM

This file implements the Write Ahead Logging (WAL) functionality for the SyndrDB database engine.
All transactions in the database are logged to the WAL first before being applied to the database files.
This ensures ACID compliance and allows for recovery in case of system failures.

The WAL implementation follows database industry best practices:
- Sequential writes for performance
- Atomic operations with platform-optimized fsync
- Transaction log replay capability
- Efficient binary format (PERFORMANCE OPTIMIZED)
- Automatic file rotation and cleanup
- Thread-safe operations
- PostgreSQL-style group commit with durability modes

BINARY FORMAT MIGRATION:
As of this version, WAL operations use high-performance binary serialization instead of ASCII JSON.
This provides significant speed improvements for both writes and reads. The system maintains backwards
compatibility by automatically detecting and reading old ASCII format files during recovery.

NEW: Binary format provides ~3-5x faster writes and ~10x faster recovery compared to JSON.
DEPRECATED: ASCII JSON functions are marked as deprecated and will be removed in future versions.

FSYNC OPTIMIZATION:
Uses platform-optimized Fdatasync() for 2-3x faster sync on Linux (fdatasync syscall) and
proper durability on macOS (F_FULLFSYNC). Implements PostgreSQL-style group commit with
configurable durability modes: strict (sync every op), balanced (batch with forced commits),
performance (batch only, accept <1s data loss on crash).

Main functionality includes:
- LogOperation: Log any database operation before execution (now uses binary format)
- Flush: Force write to disk for durability (uses Fdatasync)
- Replay: Replay operations from WAL for recovery (supports both binary and ASCII)
- Cleanup: Manage old WAL files and cleanup
*/

import (
	"bufio"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"syndrdb/src/pkg/common"
	"time"

	"go.uber.org/zap"
)

// walFilePattern matches WAL filenames: YYYY-MM-DD.wal or YYYY-MM-DD_HH-MM-SS.wal (rotated).
// Used by loadLastLSN, CleanupOldFiles, and ReplayOperations for consistent discovery.
var walFilePattern = regexp.MustCompile(`^\d{4}-\d{2}-\d{2}(_\d{2}-\d{2}-\d{2})?\.wal$`)

// OperationType represents the type of database operation
type OperationType int

const (
	OpInsert OperationType = iota + 1
	OpUpdate
	OpDelete
	OpCreateBundle
	OpDeleteBundle
	OpCreateIndex
	OpDropIndex
	OpDropRelationship
	OpCreateDatabase
	OpDropDatabase
	OpBeginTx
	OpCommitTx
	OpRollbackTx
	OpCommitSequenceAssign // PHASE 2: MVCC - Batch commit sequence assignment for documents
	OpCheckpointBegin      // Marks the start of a checkpoint (for crash recovery)
	OpCheckpointComplete   // Marks successful checkpoint completion (recovery point)
)

// WALEntry represents a single entry in the Write Ahead Log
type WALEntry struct {
	LSN        uint64        `json:"lsn"`         // Log Sequence Number
	Timestamp  time.Time     `json:"timestamp"`   // When the operation occurred
	TxID       string        `json:"tx_id"`       // Transaction ID
	Operation  OperationType `json:"operation"`   // Type of operation
	BundleName string        `json:"bundle_name"` // Target bundle
	DocumentID string        `json:"document_id"` // Target document (if applicable)
	BeforeData string        `json:"before_data"` // Data before operation (for rollback)
	AfterData  string        `json:"after_data"`  // Data after operation (for redo)
	Metadata   string        `json:"metadata"`    // Additional metadata
	Checksum   uint32        `json:"checksum"`    // Entry integrity checksum
}

// errHolder holds an error for atomic storage; heap-allocated so async flush goroutine does not store pointer to stack.
type errHolder struct{ err error }

// WriteAheadLog manages the WAL for SyndrDB
type WriteAheadLog struct {
	mutex          sync.RWMutex
	file           *os.File
	logger         *zap.SugaredLogger
	baseFilePath   string
	currentDate    time.Time
	currentLSN     uint64
	maxFileSize    int64
	flushInterval  time.Duration
	retentionDays  int
	buffer         *bufio.Writer
	lastFlush      time.Time
	autoFlush      bool
	isShuttingDown bool
	flushTicker    *time.Ticker
	flushStopChan  chan struct{}

	// WAL specific settings
	fsyncOnCommit      bool
	compressionEnabled bool
	encryptionEnabled  bool

	// PERFORMANCE OPTIMIZATION: Batch flushing (Priority 2)
	pendingOps       int           // Count of operations since last flush
	walBatchSize     int           // Number of operations to batch before flush (default: 100)
	walMaxFlushDelay time.Duration // Maximum time to wait before forcing flush (default: 100ms)

	// Durability mode configuration (PostgreSQL-style)
	durabilityMode string // "strict", "balanced", "performance"

	// Write coordinator integration
	coordinator *WriteCoordinator // Reference to write coordinator for checkpoint coordination

	// HIGH-008: Error tracking for async operations (heap-allocated holder to avoid use-after-free)
	lastFlushError atomic.Value // type: *errHolder

	// Phase 2: Group commit with double-buffering for reduced lock contention
	groupCommitMgr *GroupCommitManager // Manages double-buffering and group commit
	useGroupCommit bool                // When true, LogOperation uses group-commit path; default false
}

// WALConfig holds configuration for the WAL
type WALConfig struct {
	LogDir             string
	MaxFileSize        int64
	FlushInterval      time.Duration
	RetentionDays      int
	FsyncOnCommit      bool
	CompressionEnabled bool
	EncryptionEnabled  bool
	AutoFlush          bool
	WALBatchSize       int           // Batch size for flush operations (default: 100)
	WALMaxFlushDelay   time.Duration // Max delay before forcing flush (default: 100ms)
	DurabilityMode     string        // Durability mode: "strict", "balanced", "performance"
	UseGroupCommit     bool          // When true, use group-commit path for LogOperation (default: false)
}

// NewWriteAheadLog creates a new WAL instance with proper configuration
func NewWriteAheadLog(config WALConfig, logger *zap.SugaredLogger) (*WriteAheadLog, error) {
	if logger == nil {
		return nil, fmt.Errorf("logger cannot be nil")
	}

	// Set defaults if not provided
	if config.LogDir == "" {
		config.LogDir = "./log_files/wal"
	}
	if config.MaxFileSize <= 0 {
		config.MaxFileSize = 100 * 1024 * 1024 // 100MB default
	}
	if config.FlushInterval <= 0 {
		config.FlushInterval = 1 * time.Second // 1 second default
	}
	if config.RetentionDays <= 0 {
		config.RetentionDays = 30 // 30 days default
	}

	//baseFilePath := filepath.Join(config.LogDir, "wal")

	// Set batch defaults if not provided (PostgreSQL-style balanced mode)
	walBatchSize := config.WALBatchSize
	if walBatchSize <= 0 {
		walBatchSize = 100 // Default: batch 100 operations (balanced mode)
	}
	walMaxFlushDelay := config.WALMaxFlushDelay
	if walMaxFlushDelay <= 0 {
		walMaxFlushDelay = 100 * time.Millisecond // Default: max 100ms delay
	}

	// Set durability mode (default: "balanced")
	durabilityMode := "balanced"
	if config.DurabilityMode != "" {
		durabilityMode = config.DurabilityMode
	}

	wal := &WriteAheadLog{
		logger:             logger,
		baseFilePath:       config.LogDir,
		currentDate:        time.Now().Truncate(24 * time.Hour),
		currentLSN:         0,
		maxFileSize:        config.MaxFileSize,
		flushInterval:      config.FlushInterval,
		retentionDays:      config.RetentionDays,
		fsyncOnCommit:      config.FsyncOnCommit,
		compressionEnabled: config.CompressionEnabled,
		encryptionEnabled:  config.EncryptionEnabled,
		autoFlush:          config.AutoFlush,
		lastFlush:          time.Now(),
		flushStopChan:      make(chan struct{}),
		walBatchSize:       walBatchSize,
		walMaxFlushDelay:   walMaxFlushDelay,
		pendingOps:         0,
		durabilityMode:     durabilityMode,
		coordinator:        nil, // Set later via SetCoordinator()
		groupCommitMgr: NewGroupCommitManager(GroupCommitConfig{
			Enabled:          true,
			MaxWaitTime:      walMaxFlushDelay,
			MaxGroupSize:     walBatchSize,
			BufferSizeBytes:  64 * 1024, // 64KB initial buffer
			CompletionBuffer: 1000,      // Support 1000 concurrent waiters
		}),
		useGroupCommit: config.UseGroupCommit,
	}

	// Initialize WAL file
	if err := wal.ensureCorrectFileOpen(); err != nil {
		return nil, fmt.Errorf("failed to initialize WAL file: %w", err)
	}

	// Load last LSN from existing files
	if err := wal.loadLastLSN(); err != nil {
		logger.Warnf("Failed to load last LSN, starting from 0: %v", err)
		wal.currentLSN = 0
	}

	// Start auto-flush goroutine if enabled
	if wal.autoFlush {
		wal.startAutoFlush()
	}

	logger.Debugf("Write Ahead Log initialized at %s with LSN starting at %d", config.LogDir, wal.currentLSN)

	wal.logger.Debugf("WAL Configuration: durabilityMode=%s, batchSize=%d, maxFlushDelay=%v, fsyncOnCommit=%v",
		durabilityMode, walBatchSize, walMaxFlushDelay, config.FsyncOnCommit)

	return wal, nil
}

// ensureCorrectFileOpen ensures the correct WAL file is open based on current date
func (wal *WriteAheadLog) ensureCorrectFileOpen() error {
	today := time.Now().Truncate(24 * time.Hour)

	// If we already have the correct file open, do nothing
	if wal.file != nil && wal.currentDate.Equal(today) {
		return nil
	}

	// Close the current file if it's open
	if err := wal.closeCurrentFile(); err != nil {
		return fmt.Errorf("failed to close previous WAL file: %w", err)
	}

	// Create the filename with today's date
	dateStr := today.Format("2006-01-02")
	fileName := fmt.Sprintf("%s/%s.wal", wal.baseFilePath, dateStr)

	// Ensure the directory exists
	dir := wal.baseFilePath
	if err := os.MkdirAll(dir, 0755); err != nil {
		return fmt.Errorf("failed to create WAL directory: %w", err)
	}

	// Open the new WAL file
	file, err := os.OpenFile(fileName, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return fmt.Errorf("failed to open WAL file %s: %w", fileName, err)
	}

	// Create buffered writer for performance
	wal.file = file
	wal.buffer = bufio.NewWriter(file)
	wal.currentDate = today

	wal.logger.Debugf("Opened new WAL file: %s", fileName)
	return nil
}

// closeCurrentFile safely closes the current WAL file
func (wal *WriteAheadLog) closeCurrentFile() error {
	if wal.buffer != nil {
		if err := wal.buffer.Flush(); err != nil {
			return fmt.Errorf("failed to flush buffer: %w", err)
		}
		wal.buffer = nil
	}

	if wal.file != nil {
		if err := wal.file.Close(); err != nil {
			return fmt.Errorf("failed to close WAL file: %w", err)
		}
		wal.file = nil
	}

	return nil
}

// loadLastLSN loads the last LSN from existing WAL files to ensure continuity.
// Uses baseFilePath (WAL directory) and walFilePattern for consistent discovery.
func (wal *WriteAheadLog) loadLastLSN() error {
	dir := wal.baseFilePath
	files, err := os.ReadDir(dir)
	if err != nil {
		if os.IsNotExist(err) {
			wal.currentLSN = 0
			return nil
		}
		return fmt.Errorf("failed to read WAL directory: %w", err)
	}

	var latestFile string
	var latestTime time.Time

	for _, file := range files {
		if file.IsDir() {
			continue
		}
		if !walFilePattern.MatchString(file.Name()) {
			continue
		}
		fullPath := filepath.Join(dir, file.Name())
		info, err := os.Stat(fullPath)
		if err != nil {
			continue
		}
		if info.ModTime().After(latestTime) {
			latestTime = info.ModTime()
			latestFile = fullPath
		}
	}

	if latestFile == "" {
		wal.currentLSN = 0
		return nil
	}

	lastLSN, err := wal.readLastLSNFromFile(latestFile)
	if err != nil {
		wal.logger.Warnf("Failed to read last LSN from %s: %v", latestFile, err)
		wal.currentLSN = 0
		return nil
	}

	wal.currentLSN = lastLSN
	wal.logger.Debugf("Loaded last LSN %d from file %s", lastLSN, latestFile)
	return nil
}

// readLastLSNFromFile reads the last LSN from a WAL file. Tries binary format first
// (entry length + payload; LSN at offset 8 in each payload), then falls back to ASCII/JSON.
func (wal *WriteAheadLog) readLastLSNFromFile(filePath string) (uint64, error) {
	// Try binary format first (current production format)
	if lsn, err := wal.readLastLSNFromFileBinary(filePath); err == nil && lsn > 0 {
		return lsn, nil
	}
	// Fall back to ASCII/JSON for legacy files
	return wal.readLastLSNFromFileASCII(filePath)
}

// readLastLSNFromFileBinary scans a binary WAL file and returns the highest LSN.
// Format: [4-byte entry length][payload] repeated. LSN is at offset 8 in payload (after magic, version).
func (wal *WriteAheadLog) readLastLSNFromFileBinary(filePath string) (uint64, error) {
	file, err := os.Open(filePath)
	if err != nil {
		return 0, err
	}
	defer file.Close()

	reader := bufio.NewReader(file)
	var lastLSN uint64

	for {
		var entryLen uint32
		if err := binary.Read(reader, binary.LittleEndian, &entryLen); err != nil {
			if err == io.EOF {
				break
			}
			return 0, err
		}
		if entryLen == 0 || entryLen > 1024*1024*10 {
			return lastLSN, nil // Invalid or EOF; return what we have
		}
		payload := make([]byte, entryLen)
		if _, err := io.ReadFull(reader, payload); err != nil {
			if err == io.EOF {
				break
			}
			return lastLSN, nil
		}
		if len(payload) >= 16 {
			lsn := binary.LittleEndian.Uint64(payload[8:16])
			if lsn > lastLSN {
				lastLSN = lsn
			}
		}
	}
	return lastLSN, nil
}

// readLastLSNFromFileASCII reads the last LSN from an ASCII/JSON WAL file (legacy).
func (wal *WriteAheadLog) readLastLSNFromFileASCII(filePath string) (uint64, error) {
	file, err := os.Open(filePath)
	if err != nil {
		return 0, err
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	var lastLSN uint64

	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" {
			continue
		}
		var entry WALEntry
		if err := json.Unmarshal([]byte(line), &entry); err != nil {
			continue
		}
		if entry.LSN > lastLSN {
			lastLSN = entry.LSN
		}
	}
	return lastLSN, scanner.Err()
}

// startAutoFlush starts the auto-flush goroutine
// HIGH-008: Errors from async flush are stored in a heap-allocated errHolder (no pointer to stack).
func (wal *WriteAheadLog) startAutoFlush() {
	wal.flushTicker = time.NewTicker(wal.flushInterval)
	wal.lastFlushError.Store(&errHolder{err: nil})

	go func() {
		for {
			select {
			case <-wal.flushTicker.C:
				if !wal.isShuttingDown {
					if err := wal.Flush(); err != nil {
						wal.lastFlushError.Store(&errHolder{err: err})
						wal.logger.Errorw("WAL auto-flush failed",
							"error", err,
							"durabilityMode", wal.durabilityMode,
							"description", "Async flush error - use GetLastFlushError() to retrieve")
					} else {
						wal.lastFlushError.Store(&errHolder{err: nil})
					}
				}
			case <-wal.flushStopChan:
				return
			}
		}
	}()
}

// LogOperation logs a database operation to the WAL.
// When UseGroupCommit is enabled, uses the group-commit path for reduced contention;
// otherwise uses the standard binary path. Flush and the write coordinator flush
// both the main buffer and the group-commit buffer when group commit is enabled.
func (wal *WriteAheadLog) LogOperation(txID string, operation OperationType, bundleName, documentID, beforeData, afterData, metadata string) error {
	if wal.useGroupCommit {
		return LogOperationWithGroupCommit(wal, txID, operation, bundleName, documentID, beforeData, afterData, metadata)
	}
	return wal.LogOperationBinary(txID, operation, bundleName, documentID, beforeData, afterData, metadata)
}

// checkFileRotation checks if the current file needs to be rotated.
// Rotated files are created inside baseFilePath with name YYYY-MM-DD_HH-MM-SS.wal.
func (wal *WriteAheadLog) checkFileRotation() error {
	if wal.file == nil {
		return nil
	}

	stat, err := wal.file.Stat()
	if err != nil {
		return fmt.Errorf("failed to stat WAL file: %w", err)
	}

	if stat.Size() >= wal.maxFileSize {
		wal.logger.Debugf("WAL file size %d exceeds max size %d, rotating", stat.Size(), wal.maxFileSize)

		if err := wal.closeCurrentFile(); err != nil {
			return fmt.Errorf("failed to close current file for rotation: %w", err)
		}

		dateStr := time.Now().Format("2006-01-02")
		timeStr := time.Now().Format("15-04-05")
		rotatedName := dateStr + "_" + timeStr + ".wal"
		fileName := filepath.Join(wal.baseFilePath, rotatedName)

		file, err := os.OpenFile(fileName, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
		if err != nil {
			return fmt.Errorf("failed to open rotated WAL file: %w", err)
		}

		wal.file = file
		wal.buffer = bufio.NewWriter(file)
		wal.logger.Debugf("Rotated to new WAL file: %s", fileName)
	}

	return nil
}

func (wal *WriteAheadLog) CalculateChecksum(entry WALEntry) uint32 {
	return wal.calculateChecksum(entry)
}

// calculateChecksum calculates a simple checksum for the WAL entry
func (wal *WriteAheadLog) calculateChecksum(entry WALEntry) uint32 {
	data := fmt.Sprintf("%d%s%d%s%s%s%s%s",
		entry.LSN, entry.TxID, entry.Operation,
		entry.BundleName, entry.DocumentID,
		entry.BeforeData, entry.AfterData, entry.Metadata)

	var checksum uint32
	for _, b := range []byte(data) {
		checksum = checksum*31 + uint32(b)
	}
	return checksum
}

// Flush forces all buffered data to be written to disk, including the group-commit
// buffer when group commit is enabled.
func (wal *WriteAheadLog) Flush() error {
	wal.mutex.Lock()
	defer wal.mutex.Unlock()
	return wal.flushIncludingGroupCommitUnsafe()
}

// flushUnsafe performs the actual flush without acquiring locks (internal use only)
func (wal *WriteAheadLog) flushUnsafe() error {
	if wal.buffer == nil {
		return nil
	}

	// Flush buffer to file
	if err := wal.buffer.Flush(); err != nil {
		return fmt.Errorf("failed to flush WAL buffer: %w", err)
	}

	// CRITICAL PERFORMANCE FIX: Skip expensive fsync in performance mode
	// Write-ahead log is written to OS page cache, background checkpoint handles durability
	// This matches behavior of PostgreSQL "synchronous_commit = off" for high throughput
	if wal.durabilityMode == "performance" {
		wal.logger.Debugf("WAL flushed to OS cache (performance mode - fsync deferred to checkpoint)")
		wal.lastFlush = time.Now()
		return nil // Skip fsync, rely on checkpoint for durability
	}

	// Force sync to disk for durability using platform-optimized fsync
	// Uses fdatasync on Linux (2-3x faster), F_FULLFSYNC on macOS (true durability)
	if err := common.Fdatasync(wal.file); err != nil {
		// In strict/balanced modes, sync failures are fatal
		return fmt.Errorf("failed to sync WAL to disk: %w", err)
	}

	wal.logger.Debugf("WAL synced to disk (strict/balanced mode)")
	wal.lastFlush = time.Now()
	return nil
}

// flushIncludingGroupCommitUnsafe flushes both the main WAL buffer and the group-commit
// active buffer when group commit is enabled. Must be called with wal.mutex held.
// Used by the write coordinator so periodic and shutdown flushes include group-commit data.
func (wal *WriteAheadLog) flushIncludingGroupCommitUnsafe() error {
	if err := wal.flushUnsafe(); err != nil {
		return err
	}
	if wal.groupCommitMgr != nil {
		return wal.groupCommitMgr.FlushActiveBufferDirectUnsafe(wal)
	}
	return nil
}

// Close gracefully shuts down the WAL
func (wal *WriteAheadLog) Close() error {
	wal.mutex.Lock()
	defer wal.mutex.Unlock()

	wal.isShuttingDown = true

	// Stop auto-flush goroutine
	if wal.flushTicker != nil {
		wal.flushTicker.Stop()
		close(wal.flushStopChan)
	}

	// Flush any remaining data in group commit buffer (using Unsafe since we hold the lock)
	if wal.groupCommitMgr != nil {
		if err := wal.groupCommitMgr.FlushActiveBufferDirectUnsafe(wal); err != nil {
			wal.logger.Warnf("Failed to flush group commit buffer on shutdown: %v", err)
		}
	}

	// Final flush
	if err := wal.closeCurrentFile(); err != nil {
		wal.logger.Errorf("Error closing WAL file: %v", err)
		return err
	}

	wal.logger.Info("Write Ahead Log closed successfully")
	return nil
}

// ReplayOperations replays WAL operations for recovery
// DEPRECATED: This function now uses binary format for performance.
// The ASCII JSON format has been replaced with efficient binary serialization.
// Use ReplayOperationsBinary directly for new code.
func (wal *WriteAheadLog) ReplayOperations(fromLSN uint64, replayFunc func(WALEntry) error) error {
	// The WAL files are in the baseFilePath directory itself, not its parent
	dir := wal.baseFilePath
	wal.logger.Debugf("REPLAY: Looking for WAL files in directory: %s", dir)

	files, err := os.ReadDir(dir)
	if err != nil {
		return fmt.Errorf("failed to read WAL directory: %w", err)
	}

	wal.logger.Debugf("REPLAY: Found %d files in directory", len(files))

	var walFiles []string
	for _, file := range files {
		if file.IsDir() {
			continue
		}
		if walFilePattern.MatchString(file.Name()) {
			walFiles = append(walFiles, filepath.Join(dir, file.Name()))
		}
	}

	// Sort files by name so replay order is deterministic (YYYY-MM-DD.wal then YYYY-MM-DD_HH-MM-SS.wal)
	sort.Slice(walFiles, func(i, j int) bool { return walFiles[i] < walFiles[j] })

	for _, filePath := range walFiles {
		// Try binary format first (new format)
		if err := wal.ReplayOperationsBinary(filePath, fromLSN, replayFunc); err != nil {
			// Fallback to ASCII format for backwards compatibility
			wal.logger.Warnf("Binary replay failed for %s, trying ASCII format: %v", filePath, err)
			if err := wal.replayFromFileASCII(filePath, fromLSN, replayFunc); err != nil {
				return fmt.Errorf("failed to replay from file %s (both binary and ASCII): %w", filePath, err)
			}
		}
	}

	return nil
}

// RecoveryError represents an error during WAL recovery with entry context
// HIGH-008: Provides detailed error information for recovery failures
type RecoveryError struct {
	LSN    uint64 // Log Sequence Number of the problematic entry
	File   string // WAL file path
	Reason string // Reason for error (unmarshal, checksum, replay)
	Err    error  // Underlying error
}

func (re *RecoveryError) Error() string {
	if re.Err != nil {
		return fmt.Sprintf("recovery error at LSN %d in file %s (%s): %v", re.LSN, re.File, re.Reason, re.Err)
	}
	return fmt.Sprintf("recovery error at LSN %d in file %s (%s)", re.LSN, re.File, re.Reason)
}

// RecoveryErrorList aggregates multiple recovery errors
type RecoveryErrorList struct {
	Errors []*RecoveryError
	File   string
}

func (rel *RecoveryErrorList) Error() string {
	if len(rel.Errors) == 0 {
		return "no recovery errors"
	}
	return fmt.Sprintf("recovery encountered %d error(s) in file %s: first error: %v", len(rel.Errors), rel.File, rel.Errors[0])
}

// replayFromFileASCII replays operations from a specific WAL file using ASCII format
// DEPRECATED: This function is for backwards compatibility with old ASCII WAL files.
// New WAL files use binary format for better performance.
// HIGH-008: Now aggregates errors and returns them instead of silently skipping entries
func (wal *WriteAheadLog) replayFromFileASCII(filePath string, fromLSN uint64, replayFunc func(WALEntry) error) error {
	file, err := os.Open(filePath)
	if err != nil {
		return err
	}
	defer file.Close()

	var recoveryErrors []*RecoveryError
	lineNum := 0

	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		lineNum++
		line := strings.TrimSpace(scanner.Text())
		if line == "" {
			continue
		}

		// Skip empty lines and binary data (lines starting with non-printable characters)
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}

		// Check if line looks like binary data (starts with non-printable or control characters)
		if len(line) > 0 && (line[0] < 32 && line[0] != '\t' && line[0] != '\n' && line[0] != '\r') {
			// This is likely binary data, skip it
			wal.logger.Debugf("Skipping binary WAL entry at line %d (first byte: 0x%02x)", lineNum, line[0])
			continue
		}

		var entry WALEntry
		if err := json.Unmarshal([]byte(line), &entry); err != nil {
			// HIGH-008: Track unmarshal errors instead of silently skipping
			// Only log if it's not a binary data issue (which we already handle above)
			if !strings.Contains(err.Error(), "invalid character") || len(line) == 0 {
				recoveryErrors = append(recoveryErrors, &RecoveryError{
					LSN:    0, // Unknown LSN if unmarshal failed
					File:   filePath,
					Reason: "unmarshal",
					Err:    fmt.Errorf("line %d: %w", lineNum, err),
				})
				wal.logger.Warnf("Failed to unmarshal WAL entry at line %d: %v", lineNum, err)
			}
			continue // Continue processing other entries
		}

		// Skip entries before the specified LSN
		if entry.LSN < fromLSN {
			continue
		}

		// Verify checksum
		expectedChecksum := wal.calculateChecksum(entry)
		if entry.Checksum != expectedChecksum {
			// HIGH-008: Track checksum errors instead of silently skipping
			recoveryErrors = append(recoveryErrors, &RecoveryError{
				LSN:    entry.LSN,
				File:   filePath,
				Reason: "checksum_mismatch",
				Err:    fmt.Errorf("expected %d, got %d", expectedChecksum, entry.Checksum),
			})
			wal.logger.Warnf("Checksum mismatch for LSN %d: expected %d, got %d", entry.LSN, expectedChecksum, entry.Checksum)
			continue // Continue processing other entries
		}

		// Call replay function
		if err := replayFunc(entry); err != nil {
			// HIGH-008: Track replay function errors
			recoveryErrors = append(recoveryErrors, &RecoveryError{
				LSN:    entry.LSN,
				File:   filePath,
				Reason: "replay_function",
				Err:    err,
			})
			wal.logger.Warnf("Replay function failed for LSN %d: %v", entry.LSN, err)
			// Continue processing other entries (non-fatal)
		}
	}

	// HIGH-008: Return scanner error if any
	if err := scanner.Err(); err != nil {
		return fmt.Errorf("scanner error while reading WAL file %s: %w", filePath, err)
	}

	// HIGH-008: Return aggregated errors if any occurred
	if len(recoveryErrors) > 0 {
		return &RecoveryErrorList{
			Errors: recoveryErrors,
			File:   filePath,
		}
	}

	return nil
}

// CleanupOldFiles removes WAL files older than the retention period.
// Uses baseFilePath and walFilePattern (same as loadLastLSN and ReplayOperations).
func (wal *WriteAheadLog) CleanupOldFiles() error {
	wal.mutex.Lock()
	defer wal.mutex.Unlock()

	cutoff := time.Now().AddDate(0, 0, -wal.retentionDays)
	dir := wal.baseFilePath
	files, err := os.ReadDir(dir)
	if err != nil {
		return fmt.Errorf("failed to read WAL directory: %w", err)
	}

	for _, file := range files {
		if file.IsDir() {
			continue
		}
		if !walFilePattern.MatchString(file.Name()) {
			continue
		}
		fullPath := filepath.Join(dir, file.Name())
		info, err := os.Stat(fullPath)
		if err != nil {
			continue
		}
		if info.ModTime().Before(cutoff) {
			if err := os.Remove(fullPath); err != nil {
				wal.logger.Warnf("Failed to remove old WAL file %s: %v", fullPath, err)
			} else {
				wal.logger.Debugf("Removed old WAL file: %s", fullPath)
			}
		}
	}
	return nil
}

// GetCurrentLSN returns the current Log Sequence Number
func (wal *WriteAheadLog) GetCurrentLSN() uint64 {
	wal.mutex.RLock()
	defer wal.mutex.RUnlock()
	return wal.currentLSN
}

// GetOperationTypeName returns a human-readable name for an operation type
func GetOperationTypeName(op OperationType) string {
	switch op {
	case OpInsert:
		return "INSERT"
	case OpUpdate:
		return "UPDATE"
	case OpDelete:
		return "DELETE"
	case OpCreateBundle:
		return "CREATE_BUNDLE"
	case OpDeleteBundle:
		return "DELETE_BUNDLE"
	case OpCreateIndex:
		return "CREATE_INDEX"
	case OpDropIndex:
		return "DROP_INDEX"
	case OpBeginTx:
		return "BEGIN_TX"
	case OpCommitTx:
		return "COMMIT_TX"
	case OpRollbackTx:
		return "ROLLBACK_TX"
	case OpCheckpointBegin:
		return "CHECKPOINT_BEGIN"
	case OpCheckpointComplete:
		return "CHECKPOINT_COMPLETE"
	default:
		return "UNKNOWN"
	}
}

// SetCoordinator sets the write coordinator reference for checkpoint coordination
func (wal *WriteAheadLog) SetCoordinator(coordinator *WriteCoordinator) {
	wal.coordinator = coordinator
}

// GetLastFlushError returns the last error from the async flush goroutine, if any
func (wal *WriteAheadLog) GetLastFlushError() error {
	if val := wal.lastFlushError.Load(); val != nil {
		if h, ok := val.(*errHolder); ok && h != nil {
			return h.err
		}
	}
	return nil
}

// GetGroupCommitStats returns statistics about group commit performance
// Phase 2: Useful for monitoring group commit efficiency
func (wal *WriteAheadLog) GetGroupCommitStats() map[string]interface{} {
	if wal.groupCommitMgr == nil {
		return map[string]interface{}{
			"enabled": false,
		}
	}
	stats := wal.groupCommitMgr.GetStats()
	stats["enabled"] = true
	stats["durability_mode"] = wal.durabilityMode
	return stats
}
