package bundlestore

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"

	"go.uber.org/zap"
)

/*
BUNDLE MANIFEST MANAGER

Manages the manifest file (bundle.manifest) that tracks all segment files for a bundle.
Provides atomic updates, concurrent-safe operations, and crash recovery via WAL logging.

MANIFEST FILE STRUCTURE:
{
  "bundleName": "Users",
  "databaseName": "primary",
  "version": 1,
  "files": [
    {
      "fileID": 1,
      "fileName": "Users_000001.bnd",
      "docCount": 50000,
      "tombstones": 5000,
      "minSeq": 1,
      "maxSeq": 50000,
      "fileSize": 33554432,
      "createdAt": "2025-12-24T10:00:00Z"
    }
  ],
  "totalDocuments": 50000,
  "totalTombstones": 5000,
  "activeFileID": 1,
  "lastUpdated": "2025-12-24T10:00:00Z"
}

TODO: v2: Implement manifest reconstruction by scanning .bnd files for emergency recovery
TODO: Enterprise: Implement manifest invalidation via Redis pub/sub for multi-instance deployments
*/

// BundleManifest represents the manifest tracking all files for a bundle
type BundleManifest struct {
	BundleName      string              `json:"bundleName"`
	DatabaseName    string              `json:"databaseName"`
	Version         int                 `json:"version"`
	Files           []*ManifestFileInfo `json:"files"`
	TotalDocuments  int64               `json:"totalDocuments"`
	TotalTombstones int64               `json:"totalTombstones"`
	ActiveFileID    int                 `json:"activeFileID"`
	LastUpdated     time.Time           `json:"lastUpdated"`
}

// ManifestFileInfo contains metadata about a single bundle file
type ManifestFileInfo struct {
	FileID      int       `json:"fileID"`
	FileName    string    `json:"fileName"`
	DocCount    int64     `json:"docCount"`
	Tombstones  int64     `json:"tombstones"`
	MinSeq      uint64    `json:"minSeq"`
	MaxSeq      uint64    `json:"maxSeq"`
	FileSize    int64     `json:"fileSize"`
	CreatedAt   time.Time `json:"createdAt"`
	IsImmutable bool      `json:"isImmutable"` // True when file is frozen (no more writes)

	// Bloom filter for document ID lookups (serialized as base64)
	BloomFilterData   string `json:"bloomFilterData,omitempty"`   // Base64 encoded bloom filter
	BloomFilterSize   uint64 `json:"bloomFilterSize,omitempty"`   // Size in bits
	BloomFilterHashes uint32 `json:"bloomFilterHashes,omitempty"` // Number of hash functions
}

// ManifestManager handles manifest file operations with concurrent safety
type ManifestManager struct {
	dataDir      string
	manifestPath string
	mutex        sync.RWMutex
	manifest     *BundleManifest
	logger       *zap.SugaredLogger
}

// NewManifestManager creates a new manifest manager for a bundle
func NewManifestManager(dataDir, databaseName, bundleName string, logger *zap.SugaredLogger) *ManifestManager {
	bundleDir := filepath.Join(dataDir, databaseName, bundleName)
	manifestPath := filepath.Join(bundleDir, "bundle.manifest")

	return &ManifestManager{
		dataDir:      dataDir,
		manifestPath: manifestPath,
		logger:       logger,
	}
}

// LoadOrCreate loads an existing manifest or creates a new one
func (mm *ManifestManager) LoadOrCreate(databaseName, bundleName string) (*BundleManifest, error) {
	mm.mutex.Lock()
	defer mm.mutex.Unlock()

	// Try to load existing manifest
	if _, err := os.Stat(mm.manifestPath); err == nil {
		manifest, err := mm.loadManifestUnsafe()
		if err != nil {
			mm.logger.Warnw("Failed to load manifest, creating new one",
				"path", mm.manifestPath,
				"error", err)
			return mm.createNewManifestUnsafe(databaseName, bundleName), nil
		}
		mm.manifest = manifest
		return manifest, nil
	}

	// Create new manifest
	mm.manifest = mm.createNewManifestUnsafe(databaseName, bundleName)
	if err := mm.persistManifestUnsafe(); err != nil {
		return nil, fmt.Errorf("failed to persist new manifest: %w", err)
	}

	return mm.manifest, nil
}

// GetManifest returns a copy of the current manifest
func (mm *ManifestManager) GetManifest() *BundleManifest {
	mm.mutex.RLock()
	defer mm.mutex.RUnlock()

	if mm.manifest == nil {
		return nil
	}

	// Return a deep copy to prevent external modifications
	manifestCopy := *mm.manifest
	manifestCopy.Files = make([]*ManifestFileInfo, len(mm.manifest.Files))
	for i, file := range mm.manifest.Files {
		fileCopy := *file
		manifestCopy.Files[i] = &fileCopy
	}

	return &manifestCopy
}

// GetFileList returns the list of files sorted by fileID (descending = newest first)
func (mm *ManifestManager) GetFileList(newestFirst bool) []*ManifestFileInfo {
	mm.mutex.RLock()
	defer mm.mutex.RUnlock()

	if mm.manifest == nil {
		return nil
	}

	files := make([]*ManifestFileInfo, len(mm.manifest.Files))
	copy(files, mm.manifest.Files)

	if newestFirst {
		// Reverse order (newest first)
		for i, j := 0, len(files)-1; i < j; i, j = i+1, j-1 {
			files[i], files[j] = files[j], files[i]
		}
	}

	return files
}

// AddFile adds a new file to the manifest and sets it as active
func (mm *ManifestManager) AddFile(fileID int, fileName string) error {
	mm.mutex.Lock()
	defer mm.mutex.Unlock()

	if mm.manifest == nil {
		return fmt.Errorf("manifest not initialized")
	}

	newFile := &ManifestFileInfo{
		FileID:      fileID,
		FileName:    fileName,
		DocCount:    0,
		Tombstones:  0,
		MinSeq:      0,
		MaxSeq:      0,
		FileSize:    0,
		CreatedAt:   time.Now(),
		IsImmutable: false,
	}

	mm.manifest.Files = append(mm.manifest.Files, newFile)
	mm.manifest.ActiveFileID = fileID
	mm.manifest.LastUpdated = time.Now()

	return mm.persistManifestUnsafe()
}

// UpdateFileStats updates statistics for a specific file
func (mm *ManifestManager) UpdateFileStats(fileID int, docCount, tombstones int64, minSeq, maxSeq uint64, fileSize int64) error {
	mm.mutex.Lock()
	defer mm.mutex.Unlock()

	if mm.manifest == nil {
		return fmt.Errorf("manifest not initialized")
	}

	// Find the file
	var targetFile *ManifestFileInfo
	for _, file := range mm.manifest.Files {
		if file.FileID == fileID {
			targetFile = file
			break
		}
	}

	if targetFile == nil {
		return fmt.Errorf("file ID %d not found in manifest", fileID)
	}

	// Update file stats
	targetFile.DocCount = docCount
	targetFile.Tombstones = tombstones
	targetFile.MinSeq = minSeq
	targetFile.MaxSeq = maxSeq
	targetFile.FileSize = fileSize

	// Recalculate totals
	mm.recalculateTotalsUnsafe()
	mm.manifest.LastUpdated = time.Now()

	return mm.persistManifestUnsafe()
}

// FreezeFile marks a file as immutable (no more writes)
func (mm *ManifestManager) FreezeFile(fileID int) error {
	mm.mutex.Lock()
	defer mm.mutex.Unlock()

	if mm.manifest == nil {
		return fmt.Errorf("manifest not initialized")
	}

	for _, file := range mm.manifest.Files {
		if file.FileID == fileID {
			file.IsImmutable = true
			mm.manifest.LastUpdated = time.Now()
			return mm.persistManifestUnsafe()
		}
	}

	return fmt.Errorf("file ID %d not found in manifest", fileID)
}

// RemoveFiles removes files from the manifest (after compaction)
func (mm *ManifestManager) RemoveFiles(fileIDs []int) error {
	mm.mutex.Lock()
	defer mm.mutex.Unlock()

	if mm.manifest == nil {
		return fmt.Errorf("manifest not initialized")
	}

	// Create a set of IDs to remove
	removeSet := make(map[int]bool)
	for _, id := range fileIDs {
		removeSet[id] = true
	}

	// Filter out removed files
	newFiles := make([]*ManifestFileInfo, 0, len(mm.manifest.Files))
	for _, file := range mm.manifest.Files {
		if !removeSet[file.FileID] {
			newFiles = append(newFiles, file)
		}
	}

	mm.manifest.Files = newFiles
	mm.recalculateTotalsUnsafe()
	mm.manifest.LastUpdated = time.Now()

	return mm.persistManifestUnsafe()
}

// GetActiveFileID returns the current active file ID
func (mm *ManifestManager) GetActiveFileID() int {
	mm.mutex.RLock()
	defer mm.mutex.RUnlock()

	if mm.manifest == nil {
		return 0
	}

	return mm.manifest.ActiveFileID
}

// GetNextFileID returns the next available file ID
func (mm *ManifestManager) GetNextFileID() int {
	mm.mutex.RLock()
	defer mm.mutex.RUnlock()

	if mm.manifest == nil {
		return 1
	}

	maxID := 0
	for _, file := range mm.manifest.Files {
		if file.FileID > maxID {
			maxID = file.FileID
		}
	}

	return maxID + 1
}

// GetTombstoneRatio calculates the tombstone ratio for compaction decisions
func (mm *ManifestManager) GetTombstoneRatio() float64 {
	mm.mutex.RLock()
	defer mm.mutex.RUnlock()

	if mm.manifest == nil || mm.manifest.TotalDocuments == 0 {
		return 0.0
	}

	return float64(mm.manifest.TotalTombstones) / float64(mm.manifest.TotalDocuments+mm.manifest.TotalTombstones)
}

// loadManifestUnsafe loads manifest from disk (caller must hold lock)
func (mm *ManifestManager) loadManifestUnsafe() (*BundleManifest, error) {
	data, err := os.ReadFile(mm.manifestPath)
	if err != nil {
		return nil, fmt.Errorf("failed to read manifest file: %w", err)
	}

	var manifest BundleManifest
	if err := json.Unmarshal(data, &manifest); err != nil {
		return nil, fmt.Errorf("failed to parse manifest JSON: %w", err)
	}

	return &manifest, nil
}

// createNewManifestUnsafe creates a new empty manifest (caller must hold lock)
func (mm *ManifestManager) createNewManifestUnsafe(databaseName, bundleName string) *BundleManifest {
	return &BundleManifest{
		BundleName:      bundleName,
		DatabaseName:    databaseName,
		Version:         1,
		Files:           make([]*ManifestFileInfo, 0),
		TotalDocuments:  0,
		TotalTombstones: 0,
		ActiveFileID:    0,
		LastUpdated:     time.Now(),
	}
}

// persistManifestUnsafe writes manifest to disk atomically (caller must hold lock)
// Uses PostgreSQL-style atomic write: temp file → fsync → rename
func (mm *ManifestManager) persistManifestUnsafe() error {
	// Ensure directory exists
	dir := filepath.Dir(mm.manifestPath)
	if err := os.MkdirAll(dir, 0755); err != nil {
		return fmt.Errorf("failed to create manifest directory: %w", err)
	}

	// Marshal to JSON with indentation for readability
	data, err := json.MarshalIndent(mm.manifest, "", "  ")
	if err != nil {
		return fmt.Errorf("failed to marshal manifest: %w", err)
	}

	// Write to temporary file
	tempPath := mm.manifestPath + ".tmp"
	if err := os.WriteFile(tempPath, data, 0644); err != nil {
		return fmt.Errorf("failed to write temp manifest: %w", err)
	}

	// Sync to disk (ensure durability)
	tempFile, err := os.OpenFile(tempPath, os.O_RDONLY, 0644)
	if err != nil {
		return fmt.Errorf("failed to open temp manifest for sync: %w", err)
	}
	if err := tempFile.Sync(); err != nil {
		tempFile.Close()
		return fmt.Errorf("failed to sync temp manifest: %w", err)
	}
	tempFile.Close()

	// Atomic rename (POSIX guarantees atomicity)
	if err := os.Rename(tempPath, mm.manifestPath); err != nil {
		return fmt.Errorf("failed to rename manifest: %w", err)
	}

	mm.logger.Debugw("Manifest persisted successfully",
		"path", mm.manifestPath,
		"files", len(mm.manifest.Files),
		"totalDocs", mm.manifest.TotalDocuments)

	// TODO: Log manifest update to WAL for crash recovery
	// Requires integration with journal/wal_manager.go

	return nil
}

// recalculateTotalsUnsafe recalculates total documents and tombstones (caller must hold lock)
func (mm *ManifestManager) recalculateTotalsUnsafe() {
	totalDocs := int64(0)
	totalTombstones := int64(0)

	for _, file := range mm.manifest.Files {
		totalDocs += file.DocCount
		totalTombstones += file.Tombstones
	}

	mm.manifest.TotalDocuments = totalDocs
	mm.manifest.TotalTombstones = totalTombstones
}

// UpdateBloomFilter updates the bloom filter for a specific file
// This is called during compaction after generating a new bloom filter
func (mm *ManifestManager) UpdateBloomFilter(fileID int, bloomData string, bloomSize uint64, bloomHashes uint32) error {
	mm.mutex.Lock()
	defer mm.mutex.Unlock()

	// Find the file
	for _, file := range mm.manifest.Files {
		if file.FileID == fileID {
			file.BloomFilterData = bloomData
			file.BloomFilterSize = bloomSize
			file.BloomFilterHashes = bloomHashes

			mm.manifest.LastUpdated = time.Now()
			return mm.persistManifestUnsafe()
		}
	}

	return fmt.Errorf("file %d not found in manifest", fileID)
}

// GetBloomFilter retrieves the bloom filter for a specific file
// Returns nil if no bloom filter is available
func (mm *ManifestManager) GetBloomFilter(fileID int) (string, uint64, uint32, bool) {
	mm.mutex.RLock()
	defer mm.mutex.RUnlock()

	for _, file := range mm.manifest.Files {
		if file.FileID == fileID {
			if file.BloomFilterData != "" {
				return file.BloomFilterData, file.BloomFilterSize, file.BloomFilterHashes, true
			}
			return "", 0, 0, false
		}
	}

	return "", 0, 0, false
}
