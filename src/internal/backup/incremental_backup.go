package backup

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"syndrdb/src/internal/storage/bundlestore"
	"syndrdb/src/pkg/common/helpers"
	"syndrdb/src/pkg/settings"
)

// walArchiveManifest mirrors the WAL archiver's on-disk manifest format.
type walArchiveManifest struct {
	Files []walArchiveFileEntry `json:"files"`
}

type walArchiveFileEntry struct {
	FileName  string `json:"file_name"`
	FirstLSN  uint64 `json:"first_lsn"`
	LastLSN   uint64 `json:"last_lsn"`
	SizeBytes int64  `json:"size_bytes"`
}

// collectChangedFiles walks the database directory and returns segment files
// whose MaxSeq exceeds lastBackupCommitSeq. Bundle manifests are always
// included so the restore layer can reconstruct the directory layout.
func collectChangedFiles(dbPath string, lastBackupCommitSeq uint64, includeIndexes bool) ([]string, []string, error) {
	var changedFiles []string
	var changedBundles []string

	entries, err := os.ReadDir(dbPath)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to read database directory: %w", err)
	}

	for _, entry := range entries {
		if !entry.IsDir() {
			continue
		}

		bundleDir := filepath.Join(dbPath, entry.Name())
		manifestPath := filepath.Join(bundleDir, "bundle.manifest")

		data, err := os.ReadFile(manifestPath)
		if err != nil {
			// Not a bundle directory — skip
			continue
		}

		var bm bundlestore.BundleManifest
		if err := json.Unmarshal(data, &bm); err != nil {
			continue
		}

		// Always include bundle.manifest
		changedFiles = append(changedFiles, manifestPath)

		// Check each segment file for changes
		bundleHasChanges := false
		for _, fi := range bm.Files {
			if fi.MaxSeq > lastBackupCommitSeq {
				segPath := filepath.Join(bundleDir, fi.FileName)
				if _, err := os.Stat(segPath); err == nil {
					changedFiles = append(changedFiles, segPath)
					bundleHasChanges = true
				}
			}
		}

		if bundleHasChanges {
			changedBundles = append(changedBundles, entry.Name())

			// Include index files if requested
			if includeIndexes {
				idxEntries, _ := os.ReadDir(bundleDir)
				for _, ie := range idxEntries {
					ext := filepath.Ext(ie.Name())
					if ext == ".hidx" || ext == ".btidx" || ext == ".sidx" {
						changedFiles = append(changedFiles, filepath.Join(bundleDir, ie.Name()))
					}
				}
			}
		}
	}

	return changedFiles, changedBundles, nil
}

// collectWALFiles reads the WAL archive manifest and returns WAL files
// whose LastLSN >= lastBackupLSN.
func collectWALFiles(lastBackupLSN uint64) ([]string, []string, error) {
	args := settings.GetSettings()
	archiveDir := args.WALArchiveDir
	manifestPath := filepath.Join(archiveDir, "wal_archive_manifest.json")

	data, err := os.ReadFile(manifestPath)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil, nil
		}
		return nil, nil, fmt.Errorf("failed to read WAL archive manifest: %w", err)
	}

	var manifest walArchiveManifest
	if err := json.Unmarshal(data, &manifest); err != nil {
		return nil, nil, fmt.Errorf("failed to parse WAL archive manifest: %w", err)
	}

	var filePaths, fileNames []string
	for _, f := range manifest.Files {
		if f.LastLSN >= lastBackupLSN {
			// Try both plain and compressed paths
			path := filepath.Join(archiveDir, f.FileName)
			if _, err := os.Stat(path); err != nil {
				path = filepath.Join(archiveDir, f.FileName+".compressed")
				if _, err := os.Stat(path); err != nil {
					continue
				}
			}
			filePaths = append(filePaths, path)
			fileNames = append(fileNames, f.FileName)
		}
	}

	return filePaths, fileNames, nil
}

// CreateIncrementalBackup creates an incremental backup containing only
// changed segments since the parent backup.
func (bs *BackupService) CreateIncrementalBackup(dbName string, options BackupOptions) (string, error) {
	startTime := time.Now()

	bs.logger.Infow("Starting incremental backup",
		"database", dbName,
		"output", options.OutputPath,
	)

	// Step 1: Validate database exists
	db, err := bs.databaseService.GetDatabaseByName(dbName)
	if err != nil {
		return "", fmt.Errorf("database '%s' not found: %w", dbName, err)
	}

	// Step 2: Load catalog and find parent backup
	catalogPath := filepath.Join(bs.settings.BackupDir, "backup_catalog.json")
	catalog, err := LoadCatalog(catalogPath)
	if err != nil {
		bs.logger.Warnw("Failed to load backup catalog, will try explicit parent", "error", err)
		catalog = &BackupCatalog{Version: "1.0", Entries: []BackupCatalogEntry{}}
	}

	var parent *BackupCatalogEntry
	if options.ParentBackupPath != "" {
		// Find parent by explicit path
		parent = catalog.FindEntryByPath(options.ParentBackupPath)
		if parent == nil {
			// Try reading manifest from the archive
			manifest, readErr := ReadManifestFromArchive(options.ParentBackupPath)
			if readErr != nil {
				return "", fmt.Errorf("parent backup '%s' not found and cannot read archive: %w", options.ParentBackupPath, readErr)
			}
			entry := catalogEntryFromManifest(manifest, options.ParentBackupPath)
			parent = &entry
		}
	} else {
		parent = catalog.FindLatestBackupForDB(dbName)
	}

	if parent == nil {
		return "", fmt.Errorf("no previous backup found for database '%s'. Run a full backup first", dbName)
	}

	if parent.DatabaseName != dbName {
		return "", fmt.Errorf("parent backup is for database '%s', not '%s'", parent.DatabaseName, dbName)
	}

	// Check chain depth
	maxDepth := bs.settings.BackupMaxChainDepth
	if maxDepth <= 0 {
		maxDepth = 7
	}
	if parent.ChainDepth+1 >= maxDepth {
		return "", fmt.Errorf("incremental chain depth would exceed maximum of %d. Run a full backup", maxDepth)
	}

	// Step 3: Checkpoint
	if err := bs.executeCheckpoint(); err != nil {
		return "", fmt.Errorf("checkpoint failed: %w", err)
	}

	// Flush metadata to ensure bundle manifests on disk are up to date
	if bs.bundleService != nil {
		bs.bundleService.FlushMetadataUpdates()
	}

	// Step 4: Capture current LSN + CommitSeq
	var startLSN, endLSN uint64
	var startCommitSeq, endCommitSeq uint64
	pitrCapable := bs.walManager != nil && bs.settings.WALEnabled && bs.settings.PITREnabled
	if pitrCapable {
		startLSN = bs.walManager.GetCurrentLSN()
		startCommitSeq = bs.walManager.GetSnapshotManager().GetCurrentSequence()
	}

	// After checkpoint, capture end values
	if pitrCapable {
		endLSN = bs.walManager.GetCurrentLSN()
		endCommitSeq = bs.walManager.GetSnapshotManager().GetCurrentSequence()
	}

	// Step 5: Collect changed files
	dbPath := helpers.GetDatabaseFolderPath(db.Name)
	changedFiles, changedBundles, err := collectChangedFiles(dbPath, parent.EndCommitSeq, options.IncludeIndexes)
	if err != nil {
		return "", fmt.Errorf("failed to collect changed files: %w", err)
	}

	bs.logger.Infow("Collected changed files for incremental backup",
		"database", dbName,
		"changed_files", len(changedFiles),
		"changed_bundles", changedBundles,
	)

	// Step 6: Collect WAL files if PITR enabled
	var walFilePaths []string
	var walFileNames []string
	if pitrCapable {
		walFilePaths, walFileNames, err = collectWALFiles(parent.EndLSN)
		if err != nil {
			bs.logger.Warnw("Failed to collect WAL files", "error", err)
		}
	}

	// Step 7: Build manifest
	backupID := helpers.GenerateUUID()
	baseBackupID := parent.BaseBackupID
	if baseBackupID == "" {
		baseBackupID = parent.BackupID
	}

	manifest := &Manifest{
		BackupVersion:    "2.0",
		Timestamp:        time.Now(),
		DatabaseName:     db.Name,
		ServerVersion:    bs.settings.Version,
		Compression:      options.Compression,
		IncludesIndexes:  options.IncludeIndexes,
		Files:            make([]FileEntry, 0, len(changedFiles)),
		PrimaryDBDocuments: make([]PrimaryDBDoc, 0),
		BackupID:         backupID,
		BackupType:       "incremental",
		ParentBackupID:   parent.BackupID,
		ParentBackupPath: parent.FilePath,
		BaseBackupID:     baseBackupID,
		ChainDepth:       parent.ChainDepth + 1,
		ChangedBundles:   changedBundles,
		WALFiles:         walFileNames,
		StartLSN:         startLSN,
		EndLSN:           endLSN,
		StartCommitSeq:   startCommitSeq,
		EndCommitSeq:     endCommitSeq,
		PITREnabled:      pitrCapable,
	}

	// Step 8: Create temp directory and copy changed files
	tempDir, err := os.MkdirTemp(bs.settings.TempDir, "incr_backup_*")
	if err != nil {
		return "", fmt.Errorf("failed to create temp directory: %w", err)
	}
	defer os.RemoveAll(tempDir)

	for _, srcPath := range changedFiles {
		relPath, err := filepath.Rel(dbPath, srcPath)
		if err != nil {
			return "", fmt.Errorf("failed to get relative path for %s: %w", srcPath, err)
		}

		data, err := os.ReadFile(srcPath)
		if err != nil {
			return "", fmt.Errorf("failed to read %s: %w", srcPath, err)
		}

		crc, err := CalculateFileCRC(srcPath)
		if err != nil {
			return "", fmt.Errorf("failed to calculate CRC for %s: %w", srcPath, err)
		}

		destPath := filepath.Join(tempDir, relPath)
		if err := os.MkdirAll(filepath.Dir(destPath), 0755); err != nil {
			return "", fmt.Errorf("failed to create directory: %w", err)
		}
		if err := os.WriteFile(destPath, data, 0644); err != nil {
			return "", fmt.Errorf("failed to write %s: %w", destPath, err)
		}

		manifest.Files = append(manifest.Files, FileEntry{
			Path:      relPath,
			SizeBytes: int64(len(data)),
			CRC32:     crc,
		})
		manifest.TotalSizeBytes += int64(len(data))
	}

	// Step 9: Copy WAL files to wal/ subdirectory
	for _, walPath := range walFilePaths {
		walName := filepath.Base(walPath)
		destPath := filepath.Join(tempDir, "wal", walName)
		if err := os.MkdirAll(filepath.Dir(destPath), 0755); err != nil {
			return "", fmt.Errorf("failed to create WAL directory: %w", err)
		}

		data, err := os.ReadFile(walPath)
		if err != nil {
			bs.logger.Warnw("Failed to read WAL file", "path", walPath, "error", err)
			continue
		}
		if err := os.WriteFile(destPath, data, 0644); err != nil {
			return "", fmt.Errorf("failed to write WAL file: %w", err)
		}

		manifest.Files = append(manifest.Files, FileEntry{
			Path:      filepath.Join("wal", walName),
			SizeBytes: int64(len(data)),
		})
		manifest.TotalSizeBytes += int64(len(data))
	}

	// Step 10: Collect Primary DB documents
	if err := bs.collectPrimaryDBDocuments(db, manifest); err != nil {
		bs.logger.Warnw("Failed to collect Primary DB documents for incremental", "error", err)
	}

	// Step 11: Write manifest
	manifestFilePath := filepath.Join(tempDir, "manifest.json")
	if err := bs.writeManifestFile(manifest, manifestFilePath); err != nil {
		return "", fmt.Errorf("failed to write manifest: %w", err)
	}

	// Step 12: Create archive
	if err := os.MkdirAll(filepath.Dir(options.OutputPath), 0755); err != nil {
		return "", fmt.Errorf("failed to create backup directory: %w", err)
	}
	if err := bs.createArchive(tempDir, options.OutputPath, options.Compression); err != nil {
		return "", fmt.Errorf("failed to create archive: %w", err)
	}

	// Step 13: Update catalog
	backupInfo, _ := os.Stat(options.OutputPath)
	var sizeBytes int64
	if backupInfo != nil {
		sizeBytes = backupInfo.Size()
		manifest.CompressedSize = sizeBytes
	}

	if bs.settings.BackupCatalogEnabled {
		catalog.AddEntry(BackupCatalogEntry{
			BackupID:       backupID,
			BackupType:     "incremental",
			ParentBackupID: parent.BackupID,
			BaseBackupID:   baseBackupID,
			ChainDepth:     parent.ChainDepth + 1,
			DatabaseName:   dbName,
			Timestamp:      manifest.Timestamp,
			StartLSN:       startLSN,
			EndLSN:         endLSN,
			StartCommitSeq: startCommitSeq,
			EndCommitSeq:   endCommitSeq,
			FilePath:       options.OutputPath,
			SizeBytes:      sizeBytes,
			FileCount:      len(manifest.Files),
			ChangedBundles: changedBundles,
		})

		if err := SaveCatalog(catalog, catalogPath); err != nil {
			bs.logger.Warnw("Failed to save backup catalog", "error", err)
		}
	}

	duration := time.Since(startTime)
	bs.logger.Infow("Incremental backup completed",
		"database", dbName,
		"output", options.OutputPath,
		"size_bytes", sizeBytes,
		"changed_files", len(changedFiles),
		"changed_bundles", changedBundles,
		"chain_depth", parent.ChainDepth+1,
		"duration", duration,
	)

	return options.OutputPath, nil
}

// getIncrementalInfo returns a human-readable summary string for an incremental backup
func getIncrementalInfo(entry *BackupCatalogEntry) string {
	var parts []string
	parts = append(parts, fmt.Sprintf("type=%s", entry.BackupType))
	parts = append(parts, fmt.Sprintf("chain_depth=%d", entry.ChainDepth))
	if entry.ParentBackupID != "" {
		parts = append(parts, fmt.Sprintf("parent=%s", entry.ParentBackupID[:8]))
	}
	return strings.Join(parts, ", ")
}
