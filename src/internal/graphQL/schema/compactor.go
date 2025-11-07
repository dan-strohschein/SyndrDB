package schema

// compactor.go
//
// This file implements compaction for GraphQL schema files in SyndrDB.
// Compaction is the process of rewriting a schema file to remove expired
// tombstones and reclaim disk space while preserving all active schemas.
//
// Phase 3 functionality:
// - Atomic compaction (write to temp file, then rename)
// - Retention-aware tombstone purging
// - Background compaction support
// - File size reduction tracking
//
// Design Principles:
// - Single Responsibility: Handles only compaction operations
// - Open/Closed: Uses existing file operations without modification
// - DRY: Reuses schema file I/O operations

import (
	"fmt"
	"os"
	"time"
)

// Compactor handles schema file compaction
type Compactor struct {
	filePath string
	dbName   string
	dbID     string
}

// NewCompactor creates a new compactor for a schema file
func NewCompactor(filePath, databaseName, databaseID string) *Compactor {
	return &Compactor{
		filePath: filePath,
		dbName:   databaseName,
		dbID:     databaseID,
	}
}

// CompactResult contains statistics about a compaction operation
type CompactResult struct {
	OriginalSize       int64         // Original file size in bytes
	CompactedSize      int64         // Compacted file size in bytes
	BytesSaved         int64         // Bytes reclaimed
	PercentReduction   float64       // Percentage reduction
	RecordsBefore      int64         // Total records before
	RecordsAfter       int64         // Total records after
	RecordsPurged      int64         // Records purged
	ActivePreserved    int64         // Active schemas preserved
	TombstonesRetained int64         // Tombstones retained (within window)
	TombstonesPurged   int64         // Tombstones purged (expired)
	Duration           time.Duration // Time taken
	Timestamp          time.Time     // When compaction completed
}

// Compact performs compaction on the schema file
// This is an atomic operation - either succeeds completely or leaves original file unchanged
func (c *Compactor) Compact() (*CompactResult, error) {
	startTime := time.Now()

	// Open the existing file
	schemaFile, err := OpenSchemaFile(c.filePath)
	if err != nil {
		return nil, fmt.Errorf("failed to open schema file: %w", err)
	}
	defer schemaFile.Close()

	// Check if compaction is needed
	if !schemaFile.NeedsCompaction() {
		// Get current stats for the no-op result
		allSchemas, _ := schemaFile.ReadAllSchemas()
		activeSchemas, _ := schemaFile.ReadActiveSchemas()
		fileInfo, _ := os.Stat(c.filePath)

		return &CompactResult{
			OriginalSize:     fileInfo.Size(),
			CompactedSize:    fileInfo.Size(),
			BytesSaved:       0,
			PercentReduction: 0,
			RecordsBefore:    int64(len(allSchemas)),
			RecordsAfter:     int64(len(allSchemas)),
			RecordsPurged:    0,
			ActivePreserved:  int64(len(activeSchemas)),
			Timestamp:        time.Now(),
			Duration:         time.Since(startTime),
		}, nil // No compaction needed
	}

	// Get original file size
	originalFileInfo, err := os.Stat(c.filePath)
	if err != nil {
		return nil, fmt.Errorf("failed to stat original file: %w", err)
	}
	originalSize := originalFileInfo.Size()

	// Read all records (raw, without tombstone filtering)
	allRecords, err := schemaFile.ReadAllSchemas()
	if err != nil {
		return nil, fmt.Errorf("failed to read schemas: %w", err)
	}

	// Get active schemas (with tombstone awareness)
	activeSchemas, err := schemaFile.ReadActiveSchemas()
	if err != nil {
		return nil, fmt.Errorf("failed to read active schemas: %w", err)
	}

	// Build map of active bundle+version combinations to keep
	activeKeys := make(map[string]bool)
	for _, record := range activeSchemas {
		key := record.GetBundleName() + ":" + fmt.Sprintf("%d", record.SchemaVersion)
		activeKeys[key] = true
	}

	// Filter records for compaction
	now := time.Now().Unix()
	recordsToKeep := make([]*SchemaRecord, 0)
	stats := &CompactResult{
		OriginalSize:  originalSize,
		RecordsBefore: int64(len(allRecords)),
		Timestamp:     time.Now(),
	}

	for _, record := range allRecords {
		key := record.GetBundleName() + ":" + fmt.Sprintf("%d", record.SchemaVersion)

		// Keep if it's an active schema (not tombstoned)
		if record.IsActive() && activeKeys[key] {
			recordsToKeep = append(recordsToKeep, record)
			stats.ActivePreserved++
			continue
		}

		// For tombstones, check if they should be purged
		if record.IsTombstone() {
			if record.ShouldPurge(now) {
				// Expired tombstone - purge it
				stats.TombstonesPurged++
				stats.RecordsPurged++
				continue
			} else {
				// Within retention window - keep it
				recordsToKeep = append(recordsToKeep, record)
				stats.TombstonesRetained++
				continue
			}
		}

		// Don't keep active records that have been tombstoned
		if record.IsActive() && !activeKeys[key] {
			stats.RecordsPurged++
			continue
		}
	}

	stats.RecordsAfter = int64(len(recordsToKeep))

	// Create temporary file for compacted data
	tempFile := c.filePath + ".compact.tmp"
	compactedFile, err := CreateSchemaFile(tempFile, c.dbName, c.dbID)
	if err != nil {
		return nil, fmt.Errorf("failed to create temp file: %w", err)
	}

	// Copy header settings from original file
	compactedFile.Header.CompactionThreshold = schemaFile.Header.CompactionThreshold
	compactedFile.Header.RetentionSeconds = schemaFile.Header.RetentionSeconds
	compactedFile.Header.CreatedAt = schemaFile.Header.CreatedAt // Preserve original creation time

	// Write all records to keep
	for _, record := range recordsToKeep {
		if err := compactedFile.AppendSchema(record); err != nil {
			compactedFile.Close()
			os.Remove(tempFile)
			return nil, fmt.Errorf("failed to write record during compaction: %w", err)
		}
	}

	// Update compaction timestamp
	compactedFile.Header.LastCompactedAt = time.Now().Unix()
	if err := compactedFile.writeHeader(); err != nil {
		compactedFile.Close()
		os.Remove(tempFile)
		return nil, fmt.Errorf("failed to update header: %w", err)
	}

	// Sync and close compacted file
	if err := compactedFile.Close(); err != nil {
		os.Remove(tempFile)
		return nil, fmt.Errorf("failed to close compacted file: %w", err)
	}

	// Close original file before replacing
	schemaFile.Close()

	// Get compacted file size
	compactedFileInfo, err := os.Stat(tempFile)
	if err != nil {
		os.Remove(tempFile)
		return nil, fmt.Errorf("failed to stat compacted file: %w", err)
	}
	stats.CompactedSize = compactedFileInfo.Size()
	stats.BytesSaved = originalSize - stats.CompactedSize
	if originalSize > 0 {
		stats.PercentReduction = float64(stats.BytesSaved) / float64(originalSize) * 100.0
	}

	// Atomic rename: replace original with compacted file
	backupFile := c.filePath + ".backup"

	// First, rename original to backup
	if err := os.Rename(c.filePath, backupFile); err != nil {
		os.Remove(tempFile)
		return nil, fmt.Errorf("failed to create backup: %w", err)
	}

	// Then rename temp to original
	if err := os.Rename(tempFile, c.filePath); err != nil {
		// Try to restore backup
		os.Rename(backupFile, c.filePath)
		os.Remove(tempFile)
		return nil, fmt.Errorf("failed to rename compacted file: %w", err)
	}

	// Success! Remove backup
	os.Remove(backupFile)

	stats.Duration = time.Since(startTime)
	return stats, nil
}

// CompactIfNeeded performs compaction only if the tombstone ratio exceeds threshold
func (c *Compactor) CompactIfNeeded() (*CompactResult, error) {
	schemaFile, err := OpenSchemaFile(c.filePath)
	if err != nil {
		return nil, fmt.Errorf("failed to open schema file: %w", err)
	}
	defer schemaFile.Close()

	if !schemaFile.NeedsCompaction() {
		return &CompactResult{
			Timestamp: time.Now(),
		}, nil // No compaction needed
	}

	schemaFile.Close()
	return c.Compact()
}

// ValidateCompaction verifies the compacted file has the same active schemas
func (c *Compactor) ValidateCompaction(originalRecords, compactedRecords []*SchemaRecord) error {
	// Build maps of active schemas by bundle+version
	originalActive := make(map[string]*SchemaRecord)
	compactedActive := make(map[string]*SchemaRecord)

	for _, record := range originalRecords {
		if record.IsActive() {
			key := record.GetBundleName() + ":" + fmt.Sprintf("%d", record.SchemaVersion)
			originalActive[key] = record
		}
	}

	for _, record := range compactedRecords {
		if record.IsActive() {
			key := record.GetBundleName() + ":" + fmt.Sprintf("%d", record.SchemaVersion)
			compactedActive[key] = record
		}
	}

	// Verify all original active schemas are in compacted file
	for key := range originalActive {
		if _, exists := compactedActive[key]; !exists {
			return fmt.Errorf("active schema missing after compaction: %s", key)
		}
	}

	// Verify no extra active schemas in compacted file
	for key := range compactedActive {
		if _, exists := originalActive[key]; !exists {
			return fmt.Errorf("unexpected active schema after compaction: %s", key)
		}
	}

	return nil
}

// GetCompactionStats returns statistics without performing compaction
func (c *Compactor) GetCompactionStats() (CompactionStats, error) {
	schemaFile, err := OpenSchemaFile(c.filePath)
	if err != nil {
		return CompactionStats{}, fmt.Errorf("failed to open schema file: %w", err)
	}
	defer schemaFile.Close()

	fileInfo, err := os.Stat(c.filePath)
	if err != nil {
		return CompactionStats{}, fmt.Errorf("failed to stat file: %w", err)
	}

	allRecords, err := schemaFile.ReadAllSchemas()
	if err != nil {
		return CompactionStats{}, fmt.Errorf("failed to read schemas: %w", err)
	}

	// Get truly active schemas (tombstone-aware)
	activeSchemas, err := schemaFile.ReadActiveSchemas()
	if err != nil {
		return CompactionStats{}, fmt.Errorf("failed to read active schemas: %w", err)
	}

	now := time.Now().Unix()
	stats := CompactionStats{
		FilePath:        c.filePath,
		FileSize:        fileInfo.Size(),
		TotalRecords:    int64(len(allRecords)),
		ActiveRecords:   int64(len(activeSchemas)),
		NeedsCompaction: schemaFile.NeedsCompaction(),
		TombstoneRatio:  schemaFile.Header.GetTombstoneRatio(),
		LastCompactedAt: time.Unix(schemaFile.Header.LastCompactedAt, 0),
	}

	for _, record := range allRecords {
		if record.IsTombstone() {
			stats.TombstoneRecords++
			if record.ShouldPurge(now) {
				stats.ExpiredTombstones++
			}
		}
	}

	return stats, nil
}

// CompactionStats provides information about compaction potential
type CompactionStats struct {
	FilePath          string
	FileSize          int64
	TotalRecords      int64
	ActiveRecords     int64
	TombstoneRecords  int64
	ExpiredTombstones int64
	NeedsCompaction   bool
	TombstoneRatio    float64
	LastCompactedAt   time.Time
}

// EstimatedSavings estimates how much space could be saved by compaction
func (stats CompactionStats) EstimatedSavings() int64 {
	if stats.TotalRecords == 0 {
		return 0
	}
	// Rough estimate: assume each record is approximately the same size
	avgRecordSize := stats.FileSize / stats.TotalRecords
	return avgRecordSize * stats.ExpiredTombstones
}

// BackgroundCompactor manages background compaction operations
type BackgroundCompactor struct {
	compactor *Compactor
	interval  time.Duration
	stopChan  chan struct{}
	doneChan  chan struct{}
}

// NewBackgroundCompactor creates a new background compactor
func NewBackgroundCompactor(filePath, databaseName, databaseID string, interval time.Duration) *BackgroundCompactor {
	return &BackgroundCompactor{
		compactor: NewCompactor(filePath, databaseName, databaseID),
		interval:  interval,
		stopChan:  make(chan struct{}),
		doneChan:  make(chan struct{}),
	}
}

// Start begins background compaction loop
func (bc *BackgroundCompactor) Start() {
	go bc.run()
}

// Stop halts background compaction
func (bc *BackgroundCompactor) Stop() {
	close(bc.stopChan)
	<-bc.doneChan
}

// run is the background compaction loop
func (bc *BackgroundCompactor) run() {
	defer close(bc.doneChan)

	ticker := time.NewTicker(bc.interval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			// Attempt compaction if needed
			_, _ = bc.compactor.CompactIfNeeded()
			// Errors are silently ignored in background mode
			// In production, you'd want to log these
		case <-bc.stopChan:
			return
		}
	}
}
