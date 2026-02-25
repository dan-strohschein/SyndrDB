package backup

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"syndrdb/src/pkg/common/helpers"
	"syndrdb/src/pkg/settings"
)

// RestoreFromIncremental restores a database from an incremental backup by
// rebuilding the full chain: restoring the base full backup first, then
// overlaying each incremental in order.
func (rs *RestoreService) RestoreFromIncremental(backupPath string, options RestoreOptions) error {
	startTime := time.Now()

	rs.logger.Infow("Starting incremental restore",
		"backup", backupPath,
		"target_database", options.TargetDBName,
	)

	// Step 1: Read target backup manifest to get chain info
	targetManifest, err := ReadManifestFromArchive(backupPath)
	if err != nil {
		return fmt.Errorf("failed to read manifest from '%s': %w", backupPath, err)
	}

	// Step 2: Build chain
	chain, err := rs.buildRestoreChain(backupPath, targetManifest)
	if err != nil {
		return fmt.Errorf("failed to build backup chain: %w", err)
	}

	rs.logger.Infow("Built backup chain for restore",
		"chain_length", len(chain),
		"base_backup", chain[0].FilePath,
	)

	// Step 3: Validate chain — all archives must exist
	if err := ValidateChain(chain); err != nil {
		return fmt.Errorf("chain validation failed: %w", err)
	}

	// Determine target database name
	targetDBName := options.TargetDBName
	if targetDBName == "" {
		targetDBName = targetManifest.DatabaseName
	}

	// Step 4: Check if target database exists
	existingDB, err := rs.databaseService.GetDatabaseByName(targetDBName)
	if err == nil && existingDB != nil {
		if !options.Force {
			return fmt.Errorf("database '%s' already exists (use force option to overwrite)", targetDBName)
		}
		rs.logger.Warnw("Overwriting existing database", "database", targetDBName)
		if err := rs.databaseService.DeleteDatabase(targetDBName); err != nil {
			return fmt.Errorf("failed to delete existing database: %w", err)
		}
	}

	// Step 5: Create database directory
	dbPath := helpers.GetDatabaseFolderPath(targetDBName)
	if err := os.MkdirAll(dbPath, 0755); err != nil {
		return fmt.Errorf("failed to create database directory: %w", err)
	}

	// Step 6: Apply chain in order: base first, then each incremental
	var lastManifest *Manifest
	for i, entry := range chain {
		layerType := "full"
		if i > 0 {
			layerType = "incremental"
		}

		rs.logger.Infow("Applying backup layer",
			"layer", i,
			"type", layerType,
			"path", entry.FilePath,
		)

		manifest, applyErr := rs.applyBackupLayer(entry.FilePath, dbPath, targetDBName, targetManifest.DatabaseName)
		if applyErr != nil {
			// Rollback
			os.RemoveAll(dbPath)
			return fmt.Errorf("failed to apply %s layer '%s': %w", layerType, entry.FilePath, applyErr)
		}
		lastManifest = manifest
	}

	if lastManifest == nil {
		os.RemoveAll(dbPath)
		return fmt.Errorf("no backup layers were applied")
	}

	// Step 7: Create database and register bundles
	if err := rs.createLockedDatabase(targetDBName); err != nil {
		os.RemoveAll(dbPath)
		return fmt.Errorf("failed to create database (rolled back): %w", err)
	}

	targetDB, err := rs.databaseService.GetDatabaseByName(targetDBName)
	if err != nil {
		rs.databaseService.DeleteDatabase(targetDBName)
		os.RemoveAll(dbPath)
		return fmt.Errorf("failed to get restored database: %w", err)
	}
	if err := rs.bundleService.AddDatabaseToCatalog(targetDB); err != nil {
		rs.logger.Warnw("Failed to register database in catalog", "error", err)
	}

	// Use the last manifest (incremental) but with all files from the full chain
	if err := rs.loadAndRegisterBundles(targetDBName, lastManifest); err != nil {
		rs.logger.Warnw("Failed to load/register bundles", "error", err)
	}

	// Step 8: Validate
	if err := rs.validateIncrementalRestore(targetDBName, dbPath); err != nil {
		rs.databaseService.DeleteDatabase(targetDBName)
		os.RemoveAll(dbPath)
		return fmt.Errorf("validation failed (rolled back): %w", err)
	}

	duration := time.Since(startTime)
	rs.logger.Infow("Incremental restore completed successfully",
		"database", targetDBName,
		"duration", duration,
		"chain_length", len(chain),
		"status", "LOCKED - run UNLOCK DATABASE to enable access",
	)

	return nil
}

// buildRestoreChain builds the restore chain from catalog or path-based discovery.
func (rs *RestoreService) buildRestoreChain(backupPath string, manifest *Manifest) ([]BackupCatalogEntry, error) {
	// Try catalog-based chain first
	if manifest.BackupID != "" {
		catalogPath := filepath.Join(settings.GetSettings().BackupDir, "backup_catalog.json")
		catalog, err := LoadCatalog(catalogPath)
		if err == nil {
			chain, err := catalog.BuildChain(manifest.BackupID)
			if err == nil && len(chain) > 0 {
				return chain, nil
			}
		}
	}

	// Fallback: path-based chain discovery
	return BuildChainFromPath(backupPath)
}

// applyBackupLayer extracts a single backup archive and copies its files to
// the target database directory, overwriting existing files (overlay).
func (rs *RestoreService) applyBackupLayer(archivePath, dbPath, targetDBName, originalDBName string) (*Manifest, error) {
	tempDir, err := os.MkdirTemp(rs.settings.TempDir, "restore_layer_*")
	if err != nil {
		return nil, fmt.Errorf("failed to create temp directory: %w", err)
	}
	defer os.RemoveAll(tempDir)

	bs := &BackupService{
		settings: rs.settings,
		logger:   rs.logger,
	}

	if err := bs.extractArchive(archivePath, tempDir); err != nil {
		return nil, fmt.Errorf("failed to extract archive: %w", err)
	}

	// Read manifest
	manifestFile, err := os.Open(filepath.Join(tempDir, "manifest.json"))
	if err != nil {
		return nil, fmt.Errorf("manifest not found: %w", err)
	}
	manifest, err := ReadManifest(manifestFile)
	manifestFile.Close()
	if err != nil {
		return nil, fmt.Errorf("failed to read manifest: %w", err)
	}

	// Verify CRCs
	for _, fileEntry := range manifest.Files {
		// Skip WAL files from CRC verification (they may not have CRCs)
		if strings.HasPrefix(fileEntry.Path, "wal/") || strings.HasPrefix(fileEntry.Path, "wal\\") {
			continue
		}
		filePath := filepath.Join(tempDir, fileEntry.Path)
		if err := VerifyFileCRC(filePath, fileEntry.CRC32); err != nil {
			return nil, fmt.Errorf("CRC32 checksum mismatch in chain backup '%s': file '%s'", archivePath, fileEntry.Path)
		}
	}

	// Copy files to database directory (overlay — overwrites existing)
	needsRename := originalDBName != targetDBName
	oldPrefix := originalDBName + "_"
	newPrefix := targetDBName + "_"

	for _, fileEntry := range manifest.Files {
		// Skip WAL files — they go to a separate location
		if strings.HasPrefix(fileEntry.Path, "wal/") || strings.HasPrefix(fileEntry.Path, "wal\\") {
			continue
		}

		srcPath := filepath.Join(tempDir, fileEntry.Path)

		destRelPath := fileEntry.Path
		if needsRename {
			fileName := filepath.Base(destRelPath)
			if strings.HasPrefix(fileName, oldPrefix) {
				newName := newPrefix + strings.TrimPrefix(fileName, oldPrefix)
				destRelPath = filepath.Join(filepath.Dir(destRelPath), newName)
			}
		}
		destPath := filepath.Join(dbPath, destRelPath)

		if err := os.MkdirAll(filepath.Dir(destPath), 0755); err != nil {
			return nil, fmt.Errorf("failed to create directory: %w", err)
		}

		data, err := os.ReadFile(srcPath)
		if err != nil {
			return nil, fmt.Errorf("failed to read %s: %w", srcPath, err)
		}

		// Rewrite databaseName inside bundle.manifest files if renamed
		if needsRename && filepath.Base(destPath) == "bundle.manifest" {
			rewritten, rwErr := rewriteManifestDatabaseName(data, targetDBName)
			if rwErr == nil {
				data = rewritten
			}
		}

		if err := os.WriteFile(destPath, data, 0644); err != nil {
			return nil, fmt.Errorf("failed to write %s: %w", destPath, err)
		}
	}

	return manifest, nil
}

// validateIncrementalRestore checks that the restored database has bundle directories.
func (rs *RestoreService) validateIncrementalRestore(dbName, dbPath string) error {
	rs.logger.Info("Validating incremental restore...")

	// Verify database was created
	_, err := rs.databaseService.GetDatabaseByName(dbName)
	if err != nil {
		return fmt.Errorf("database not found after restore: %w", err)
	}

	// Verify database is locked
	if !rs.lockService.IsLocked(dbName) {
		return fmt.Errorf("database not locked after restore")
	}

	// Verify at least one bundle directory exists
	entries, err := os.ReadDir(dbPath)
	if err != nil {
		return fmt.Errorf("failed to read database directory: %w", err)
	}

	hasBundles := false
	for _, entry := range entries {
		if entry.IsDir() {
			manifestPath := filepath.Join(dbPath, entry.Name(), "bundle.manifest")
			if _, err := os.Stat(manifestPath); err == nil {
				hasBundles = true
				break
			}
		}
	}

	if !hasBundles {
		rs.logger.Warnw("No bundle directories found after restore", "database", dbName)
	}

	rs.logger.Infow("Incremental restore validation passed", "database", dbName)
	return nil
}
