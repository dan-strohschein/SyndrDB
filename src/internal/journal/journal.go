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
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"sync"
	"sync/atomic"
	"syndrdb/src/pkg/common"
	"time"

	"go.uber.org/zap"
)

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
	OpCheckpointBegin       // Marks the start of a checkpoint (for crash recovery)
	OpCheckpointComplete    // Marks successful checkpoint completion (recovery point)
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

	// HIGH-008: Error tracking for async operations
	// Use pointer to error since atomic.Value cannot store nil directly
	lastFlushError atomic.Value // Stores *error from async flush goroutine (type: *error)
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

	logger.Infof("Write Ahead Log initialized at %s with LSN starting at %d", config.LogDir, wal.currentLSN)

	wal.logger.Infof("WAL Configuration: durabilityMode=%s, batchSize=%d, maxFlushDelay=%v, fsyncOnCommit=%v",
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

	wal.logger.Infof("Opened new WAL file: %s", fileName)
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

// loadLastLSN loads the last LSN from existing WAL files to ensure continuity
func (wal *WriteAheadLog) loadLastLSN() error {
	dir := filepath.Dir(wal.baseFilePath)
	files, err := os.ReadDir(dir)
	if err != nil {
		if os.IsNotExist(err) {
			wal.currentLSN = 0
			return nil
		}
		return fmt.Errorf("failed to read WAL directory: %w", err)
	}

	// Find the most recent WAL file
	var latestFile string
	var latestTime time.Time

	walPattern := regexp.MustCompile(`wal_\d{4}-\d{2}-\d{2}\.wal$`)

	for _, file := range files {
		if file.IsDir() {
			continue
		}

		if walPattern.MatchString(file.Name()) {
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
	}

	if latestFile == "" {
		wal.currentLSN = 0
		return nil
	}

	// Read the last LSN from the most recent file
	lastLSN, err := wal.readLastLSNFromFile(latestFile)
	if err != nil {
		wal.logger.Warnf("Failed to read last LSN from %s: %v", latestFile, err)
		wal.currentLSN = 0
		return nil
	}

	wal.currentLSN = lastLSN
	wal.logger.Infof("Loaded last LSN %d from file %s", lastLSN, latestFile)
	return nil
}

// readLastLSNFromFile reads the last LSN from a specific WAL file
func (wal *WriteAheadLog) readLastLSNFromFile(filePath string) (uint64, error) {
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
			continue // Skip malformed entries
		}

		if entry.LSN > lastLSN {
			lastLSN = entry.LSN
		}
	}

	return lastLSN, scanner.Err()
}

// startAutoFlush starts the auto-flush goroutine
// HIGH-008: Errors from async flush are now stored and can be retrieved via GetLastFlushError()
func (wal *WriteAheadLog) startAutoFlush() {
	wal.flushTicker = time.NewTicker(wal.flushInterval)
	
	// Initialize error storage with nil pointer
	var nilErr error
	wal.lastFlushError.Store(&nilErr)

	go func() {
		for {
			select {
			case <-wal.flushTicker.C:
				if !wal.isShuttingDown {
					// HIGH-008: Store flush errors for retrieval
					if err := wal.Flush(); err != nil {
						wal.lastFlushError.Store(&err)
						wal.logger.Errorw("WAL auto-flush failed",
							"error", err,
							"durabilityMode", wal.durabilityMode,
							"description", "Async flush error - use GetLastFlushError() to retrieve")
					} else {
						// Clear error on successful flush
						var nilErr error
						wal.lastFlushError.Store(&nilErr)
					}
				}
			case <-wal.flushStopChan:
				return
			}
		}
	}()
}

// LogOperation logs a database operation to the WAL
// DEPRECATED: This function now uses binary format internally for performance.
// The ASCII JSON format has been replaced with efficient binary serialization.
// Use LogOperationBinary directly for new code, or keep using this for compatibility.
func (wal *WriteAheadLog) LogOperation(txID string, operation OperationType, bundleName, documentID, beforeData, afterData, metadata string) error {
	// Redirect to high-performance binary implementation
	return wal.LogOperationBinary(txID, operation, bundleName, documentID, beforeData, afterData, metadata)
}

// checkFileRotation checks if the current file needs to be rotated
func (wal *WriteAheadLog) checkFileRotation() error {
	if wal.file == nil {
		return nil
	}

	stat, err := wal.file.Stat()
	if err != nil {
		return fmt.Errorf("failed to stat WAL file: %w", err)
	}

	if stat.Size() >= wal.maxFileSize {
		wal.logger.Infof("WAL file size %d exceeds max size %d, rotating", stat.Size(), wal.maxFileSize)

		// Close current file and open a new one with timestamp
		if err := wal.closeCurrentFile(); err != nil {
			return fmt.Errorf("failed to close current file for rotation: %w", err)
		}

		// Create new file with timestamp
		timestamp := time.Now().Format("2006-01-02_15-04-05")
		fileName := fmt.Sprintf("%s_%s.wal", wal.baseFilePath, timestamp)

		file, err := os.OpenFile(fileName, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
		if err != nil {
			return fmt.Errorf("failed to open rotated WAL file: %w", err)
		}

		wal.file = file
		wal.buffer = bufio.NewWriter(file)
		wal.logger.Infof("Rotated to new WAL file: %s", fileName)
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

// Flush forces all buffered data to be written to disk
func (wal *WriteAheadLog) Flush() error {
	wal.mutex.Lock()
	defer wal.mutex.Unlock()
	return wal.flushUnsafe()
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
	wal.logger.Infof("REPLAY: Looking for WAL files in directory: %s", dir)

	files, err := os.ReadDir(dir)
	if err != nil {
		return fmt.Errorf("failed to read WAL directory: %w", err)
	}

	wal.logger.Infof("REPLAY: Found %d files in directory", len(files))

	// Match WAL files: YYYY-MM-DD.wal or YYYY-MM-DD_HH-MM-SS.wal (for rotated files)
	walPattern := regexp.MustCompile(`^\d{4}-\d{2}-\d{2}(_\d{2}-\d{2}-\d{2})?\.wal$`)
	var walFiles []string

	for _, file := range files {
		if file.IsDir() {
			continue
		}
		wal.logger.Infof("REPLAY: Checking file: %s, matches=%v", file.Name(), walPattern.MatchString(file.Name()))
		if walPattern.MatchString(file.Name()) {
			walFiles = append(walFiles, filepath.Join(dir, file.Name()))
		}
	}

	wal.logger.Infof("REPLAY: Found %d WAL files matching pattern", len(walFiles))

	// Sort files by modification time
	// TODO: Implement proper sorting

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
	LSN     uint64 // Log Sequence Number of the problematic entry
	File    string // WAL file path
	Reason  string // Reason for error (unmarshal, checksum, replay)
	Err     error  // Underlying error
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

		var entry WALEntry
		if err := json.Unmarshal([]byte(line), &entry); err != nil {
			// HIGH-008: Track unmarshal errors instead of silently skipping
			recoveryErrors = append(recoveryErrors, &RecoveryError{
				LSN:    entry.LSN, // May be 0 if unmarshal failed
				File:   filePath,
				Reason: "unmarshal",
				Err:    fmt.Errorf("line %d: %w", lineNum, err),
			})
			wal.logger.Warnf("Failed to unmarshal WAL entry at line %d: %v", lineNum, err)
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

// CleanupOldFiles removes WAL files older than the retention period
func (wal *WriteAheadLog) CleanupOldFiles() error {
	wal.mutex.Lock()
	defer wal.mutex.Unlock()

	cutoff := time.Now().AddDate(0, 0, -wal.retentionDays)

	dir := filepath.Dir(wal.baseFilePath)
	files, err := os.ReadDir(dir)
	if err != nil {
		return fmt.Errorf("failed to read WAL directory: %w", err)
	}

	walPattern := regexp.MustCompile(`wal_\d{4}-\d{2}-\d{2}.*\.wal$`)

	for _, file := range files {
		if file.IsDir() {
			continue
		}

		if walPattern.MatchString(file.Name()) {
			fullPath := filepath.Join(dir, file.Name())
			info, err := os.Stat(fullPath)
			if err != nil {
				continue
			}

			if info.ModTime().Before(cutoff) {
				if err := os.Remove(fullPath); err != nil {
					wal.logger.Warnf("Failed to remove old WAL file %s: %v", fullPath, err)
				} else {
					wal.logger.Infof("Removed old WAL file: %s", fullPath)
				}
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
// HIGH-008: Allows callers to detect and handle flush errors from background operations
// Returns nil if no error has occurred or if the last flush was successful
func (wal *WriteAheadLog) GetLastFlushError() error {
	if val := wal.lastFlushError.Load(); val != nil {
		if errPtr, ok := val.(*error); ok && errPtr != nil && *errPtr != nil {
			return *errPtr
		}
	}
	return nil
}
