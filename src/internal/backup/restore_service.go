package backup

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"go.uber.org/zap"

	"syndrdb/src/internal/domain/bundle"
	"syndrdb/src/internal/domain/database"
	"syndrdb/src/internal/domain/models"
	"syndrdb/src/internal/lock"
	"syndrdb/src/pkg/common/helpers"
	"syndrdb/src/pkg/settings"
)

// RestoreService handles database restore operations
type RestoreService struct {
	databaseService *database.DatabaseService
	bundleService   *bundle.BundleService
	lockService     *lock.LockService
	settings        *settings.Arguments
	logger          *zap.SugaredLogger
}

// RestoreOptions configures restore behavior
type RestoreOptions struct {
	TargetDBName string // Name for restored database (can differ from backup)
	Force        bool   // Overwrite existing database
}

// NewRestoreService creates a new restore service instance
func NewRestoreService(
	databaseService *database.DatabaseService,
	bundleService *bundle.BundleService,
	lockService *lock.LockService,
	logger *zap.SugaredLogger,
) *RestoreService {
	return &RestoreService{
		databaseService: databaseService,
		bundleService:   bundleService,
		lockService:     lockService,
		settings:        settings.GetSettings(),
		logger:          logger,
	}
}

// RestoreBackup restores a database from a backup file
func (rs *RestoreService) RestoreBackup(backupPath string, options RestoreOptions) error {
	startTime := time.Now()

	rs.logger.Infow("Starting database restore",
		"backup", backupPath,
		"target_database", options.TargetDBName,
		"force", options.Force,
	)

	// Step 1: Extract backup to temp directory
	tempDir, manifest, err := rs.extractAndValidate(backupPath)
	if err != nil {
		return fmt.Errorf("failed to extract backup: %w", err)
	}
	defer os.RemoveAll(tempDir)

	// Use target database name if provided, otherwise use original name
	targetDBName := options.TargetDBName
	if targetDBName == "" {
		targetDBName = manifest.DatabaseName
	}

	// Step 2: Check if target database exists
	existingDB, err := rs.databaseService.GetDatabaseByName(targetDBName)
	if err == nil && existingDB != nil {
		if !options.Force {
			return fmt.Errorf("database '%s' already exists (use force option to overwrite)", targetDBName)
		}

		// Delete existing database
		rs.logger.Warnw("Overwriting existing database",
			"database", targetDBName,
		)
		if err := rs.databaseService.DeleteDatabase(targetDBName); err != nil {
			return fmt.Errorf("failed to delete existing database: %w", err)
		}
	}

	// Step 3: Verify CRCs for all files
	if err := rs.verifyCRCs(tempDir, manifest); err != nil {
		return fmt.Errorf("CRC verification failed: %w", err)
	}

	// Step 4: Check compatibility
	if err := rs.checkCompatibility(manifest); err != nil {
		return fmt.Errorf("compatibility check failed: %w", err)
	}

	// Step 5: Create database directory
	dbPath := helpers.GetDatabaseFolderPath(targetDBName)
	if err := os.MkdirAll(dbPath, 0755); err != nil {
		return fmt.Errorf("failed to create database directory: %w", err)
	}

	// Step 6: Copy files from temp to database directory (renaming if target differs from original)
	if err := rs.copyFiles(tempDir, dbPath, manifest, manifest.DatabaseName, targetDBName); err != nil {
		// Rollback: Delete database directory
		os.RemoveAll(dbPath)
		return fmt.Errorf("failed to copy files (rolled back): %w", err)
	}

	// Step 7: Create database in locked state
	if err := rs.createLockedDatabase(targetDBName); err != nil {
		// Rollback: Delete database directory
		os.RemoveAll(dbPath)
		return fmt.Errorf("failed to create database (rolled back): %w", err)
	}

	// Step 8: Register restored database in the system catalog.
	// Use the standard catalog API (AddDatabaseToCatalog) which creates a
	// fresh catalog document with its own UUID — no risk of overwriting
	// existing catalog entries for other databases.
	targetDB, err := rs.databaseService.GetDatabaseByName(targetDBName)
	if err != nil {
		rs.databaseService.DeleteDatabase(targetDBName)
		os.RemoveAll(dbPath)
		return fmt.Errorf("failed to get restored database: %w", err)
	}
	if err := rs.bundleService.AddDatabaseToCatalog(targetDB); err != nil {
		rs.logger.Warnw("Failed to register database in catalog (may already exist)", "error", err)
	}

	// Step 9: Load restored bundles into memory and register each in the catalog.
	// For each bundle in the backup, load its metadata from disk via
	// GetBundleMetadata, then register in catalog via RegisterBundleInCatalog.
	// This uses the standard catalog APIs which create fresh documents with
	// their own UUIDs — they never modify existing catalog entries.
	if err := rs.loadAndRegisterBundles(targetDBName, manifest); err != nil {
		rs.logger.Warnw("Failed to load/register restored bundles (bundles will lazy-load on first query)",
			"error", err,
		)
	}

	// Step 10: Validate restored database
	if err := rs.validateDatabase(targetDBName, manifest, manifest.DatabaseName); err != nil {
		// Rollback: Delete database and directory
		rs.databaseService.DeleteDatabase(targetDBName)
		os.RemoveAll(dbPath)
		return fmt.Errorf("validation failed (rolled back): %w", err)
	}

	duration := time.Since(startTime)
	rs.logger.Infow("Restore completed successfully",
		"database", targetDBName,
		"duration", duration,
		"file_count", len(manifest.Files),
		"status", "LOCKED - run UNLOCK DATABASE to enable access",
	)

	return nil
}

// extractAndValidate extracts the backup and reads the manifest
func (rs *RestoreService) extractAndValidate(backupPath string) (string, *Manifest, error) {
	// Check if backup file exists
	if _, err := os.Stat(backupPath); err != nil {
		return "", nil, fmt.Errorf("backup file not found: %w", err)
	}

	// Create temp directory
	tempDir, err := os.MkdirTemp(rs.settings.TempDir, "restore_*")
	if err != nil {
		return "", nil, fmt.Errorf("failed to create temp directory: %w", err)
	}

	// Create a backup service to use extraction logic
	bs := &BackupService{
		settings: rs.settings,
		logger:   rs.logger,
	}

	// Extract archive
	if err := bs.extractArchive(backupPath, tempDir); err != nil {
		os.RemoveAll(tempDir)
		return "", nil, fmt.Errorf("failed to extract archive: %w", err)
	}

	// Read manifest
	manifestPath := filepath.Join(tempDir, "manifest.json")
	manifestFile, err := os.Open(manifestPath)
	if err != nil {
		os.RemoveAll(tempDir)
		return "", nil, fmt.Errorf("manifest not found: %w", err)
	}
	defer manifestFile.Close()

	manifest, err := ReadManifest(manifestFile)
	if err != nil {
		os.RemoveAll(tempDir)
		return "", nil, fmt.Errorf("failed to read manifest: %w", err)
	}

	return tempDir, manifest, nil
}

// verifyCRCs validates file integrity
func (rs *RestoreService) verifyCRCs(tempDir string, manifest *Manifest) error {
	rs.logger.Info("Verifying file integrity...")

	for _, fileEntry := range manifest.Files {
		filePath := filepath.Join(tempDir, fileEntry.Path)
		if err := VerifyFileCRC(filePath, fileEntry.CRC32); err != nil {
			return err
		}
	}

	rs.logger.Infow("CRC verification passed",
		"file_count", len(manifest.Files),
	)
	return nil
}

// checkCompatibility verifies backup is compatible with current server
func (rs *RestoreService) checkCompatibility(manifest *Manifest) error {
	// Check server version compatibility
	// For now, we allow any version (TODO: add strict version checking)
	if manifest.ServerVersion != rs.settings.Version {
		rs.logger.Warnw("Server version mismatch",
			"backup_version", manifest.ServerVersion,
			"current_version", rs.settings.Version,
		)
	}

	// Check backup version
	if manifest.BackupVersion != "1.0" {
		return fmt.Errorf("unsupported backup version: %s (expected 1.0)", manifest.BackupVersion)
	}

	rs.logger.Info("Compatibility check passed")
	return nil
}

// copyFiles copies files from temp to database directory.
// When originalDBName differs from targetDBName it renames legacy files
// (<originalDB>_<bundle>.*) and rewrites the databaseName field inside
// bundle.manifest files so the storage engine picks up the new name.
func (rs *RestoreService) copyFiles(srcDir, destDir string, manifest *Manifest, originalDBName, targetDBName string) error {
	rs.logger.Info("Copying database files...")

	needsRename := originalDBName != targetDBName
	oldPrefix := originalDBName + "_"
	newPrefix := targetDBName + "_"

	for _, fileEntry := range manifest.Files {
		srcPath := filepath.Join(srcDir, fileEntry.Path)

		// Compute destination path, renaming legacy files if needed
		destRelPath := fileEntry.Path
		if needsRename {
			fileName := filepath.Base(destRelPath)
			if strings.HasPrefix(fileName, oldPrefix) {
				newName := newPrefix + strings.TrimPrefix(fileName, oldPrefix)
				destRelPath = filepath.Join(filepath.Dir(destRelPath), newName)
			}
		}
		destPath := filepath.Join(destDir, destRelPath)

		// Create parent directories
		if err := os.MkdirAll(filepath.Dir(destPath), 0755); err != nil {
			return fmt.Errorf("failed to create directory: %w", err)
		}

		// Read source file
		data, err := os.ReadFile(srcPath)
		if err != nil {
			return fmt.Errorf("failed to read %s: %w", srcPath, err)
		}

		// Rewrite databaseName inside bundle.manifest files
		if needsRename && filepath.Base(destPath) == "bundle.manifest" {
			data, err = rewriteManifestDatabaseName(data, targetDBName)
			if err != nil {
				rs.logger.Warnf("Failed to rewrite bundle.manifest %s: %v", destPath, err)
				// Continue with original data rather than failing the entire restore
			}
		}

		// Write destination file
		if err := os.WriteFile(destPath, data, 0644); err != nil {
			return fmt.Errorf("failed to write %s: %w", destPath, err)
		}
	}

	rs.logger.Infow("Files copied successfully",
		"file_count", len(manifest.Files),
		"renamed", needsRename,
	)
	return nil
}

// rewriteManifestDatabaseName updates the databaseName field in a bundle.manifest JSON blob.
func rewriteManifestDatabaseName(data []byte, newDBName string) ([]byte, error) {
	var m map[string]interface{}
	if err := json.Unmarshal(data, &m); err != nil {
		return nil, err
	}
	m["databaseName"] = newDBName
	return json.MarshalIndent(m, "", "  ")
}

// createLockedDatabase creates the database in locked state
func (rs *RestoreService) createLockedDatabase(dbName string) error {
	// Create database using DatabaseService
	dbCommand := models.DatabaseCommand{
		DatabaseName: dbName,
	}

	_, err := rs.databaseService.AddDatabase(dbCommand)
	if err != nil {
		return fmt.Errorf("failed to create database: %w", err)
	}

	// Lock the database
	if err := rs.lockService.LockDatabase(dbName, "system", "RESTORE", "Database restored from backup - verification required"); err != nil {
		return fmt.Errorf("failed to lock database: %w", err)
	}

	rs.logger.Infow("Database created and locked",
		"database", dbName,
	)
	return nil
}

// restorePrimaryDBDocs restores metadata documents to Primary database
func (rs *RestoreService) restorePrimaryDBDocs(targetDBName string, manifest *Manifest) error {
	rs.logger.Infow("[RESTORE-DIAG] Restoring Primary database metadata",
		"target_db", targetDBName,
		"original_db", manifest.DatabaseName,
		"primary_db_docs_in_manifest", len(manifest.PrimaryDBDocuments),
	)

	if len(manifest.PrimaryDBDocuments) == 0 {
		rs.logger.Warnw("[RESTORE-DIAG] Manifest has 0 PrimaryDBDocuments — SHOW BUNDLES will return empty for restored database")
		return nil
	}

	primaryDB, err := rs.databaseService.GetDatabaseByName("primary")
	if err != nil {
		return fmt.Errorf("Primary database not found: %w", err)
	}

	// When restoring to a different name, we must generate new DocumentIDs
	// for catalog entries. Reusing the original IDs would overwrite the
	// source database's entries in the Primary DB (same page cache key).
	renamed := targetDBName != manifest.DatabaseName

	// Restore documents to appropriate bundles
	for _, doc := range manifest.PrimaryDBDocuments {
		bundle, err := rs.bundleService.GetBundleByName(primaryDB, doc.Bundle)
		if err != nil {
			return fmt.Errorf("bundle '%s' not found: %w", doc.Bundle, err)
		}

		// Update database name in document if it changed
		if doc.Bundle == "Databases" || doc.Bundle == "Bundles" {
			if originalName, ok := doc.Data["DatabaseName"]; ok && originalName != targetDBName {
				doc.Data["DatabaseName"] = targetDBName
			}
			if originalName, ok := doc.Data["Name"]; ok && doc.Bundle == "Databases" && originalName != targetDBName {
				doc.Data["Name"] = targetDBName
			}
		}

		// Assign DocumentID: use a fresh UUID when restoring under a new name
		// to avoid overwriting the original database's catalog entries.
		docID := doc.DocumentID
		if renamed {
			docID = helpers.GenerateUUID()
		}

		// Create document with both Data and Values populated.
		// Values must be populated because GetFieldValue (used by
		// GetBundlesFromCatalogByDatabaseName) only reads from Values,
		// and the write-through page cache stores the document as-is.
		document := &models.Document{
			DocumentID: docID,
			Data:       doc.Data,
		}
		schema := bundle.DocumentStructure.FieldSchema()
		if schema != nil && len(doc.Data) > 0 {
			document.Values = make([]models.FieldValue, len(schema.Names))
			for i, name := range schema.Names {
				if val, ok := doc.Data[name]; ok {
					document.Values[i] = models.NewInterfaceValue(val)
				}
			}
		}

		// Add document to bundle
		if err := rs.bundleService.AddDocumentToBundleByStruct(primaryDB, bundle, document); err != nil {
			return fmt.Errorf("failed to add document to %s: %w", doc.Bundle, err)
		}
	}

	rs.logger.Infow("Primary DB metadata restored",
		"document_count", len(manifest.PrimaryDBDocuments),
	)
	return nil
}

// loadAndRegisterBundles discovers restored bundles on disk, loads their metadata
// into the BundleService cache, and registers each in the system catalog using
// the standard RegisterBundleInCatalog API. This creates fresh catalog documents
// with unique UUIDs and never touches existing catalog entries for other databases.
func (rs *RestoreService) loadAndRegisterBundles(targetDBName string, manifest *Manifest) error {
	db, err := rs.databaseService.GetDatabaseByName(targetDBName)
	if err != nil {
		return fmt.Errorf("failed to get restored database: %w", err)
	}

	// Discover bundle names from the manifest's PrimaryDBDocuments.
	bundleNames := make([]string, 0)
	for _, doc := range manifest.PrimaryDBDocuments {
		if doc.Bundle != "Bundles" {
			continue
		}
		bundleName, _ := doc.Data["Name"].(string)
		if bundleName != "" {
			bundleNames = append(bundleNames, bundleName)
		}
	}

	// If manifest had no PrimaryDBDocuments, fall back to disk discovery
	if len(bundleNames) == 0 {
		dbPath := settings.GetSettings().DataDir + "/" + targetDBName + "/"
		entries, dirErr := os.ReadDir(dbPath)
		if dirErr == nil {
			seen := make(map[string]bool)
			for _, entry := range entries {
				name := entry.Name()
				if entry.IsDir() {
					manifestPath := filepath.Join(dbPath, name, "bundle.manifest")
					if _, err := os.Stat(manifestPath); err == nil && !seen[name] {
						seen[name] = true
						bundleNames = append(bundleNames, name)
					}
				} else if strings.HasSuffix(name, ".bnd") {
					bName := strings.TrimSuffix(name, ".bnd")
					bName = strings.TrimPrefix(bName, targetDBName+"_")
					if bName != "" && !seen[bName] {
						seen[bName] = true
						bundleNames = append(bundleNames, bName)
					}
				}
			}
		}
	}

	var loaded int
	for _, bundleName := range bundleNames {
		// Register bundle file in Database.BundleFiles
		legacyFileName := fmt.Sprintf("%s_%s.bnd", targetDBName, bundleName)
		db.BundleFiles = append(db.BundleFiles, legacyFileName)

		// Load bundle metadata from disk
		bundle, err := rs.bundleService.GetBundleMetadata(db, bundleName)
		if err != nil {
			rs.logger.Warnw("Failed to load restored bundle metadata",
				"bundle", bundleName,
				"error", err,
			)
			continue
		}

		// Always set database reference to the TARGET database so that
		// RegisterBundleInCatalog uses the new database's ID/Name, not
		// the original source database that was backed up.
		bundle.Database = db

		// Register in catalog using the standard API (creates fresh document, never overwrites)
		if err := rs.bundleService.RegisterBundleInCatalog(bundle); err != nil {
			rs.logger.Warnw("Failed to register restored bundle in catalog",
				"bundle", bundleName,
				"error", err,
			)
			continue
		}
		loaded++
	}

	rs.logger.Infow("Loaded and registered restored bundles",
		"database", targetDBName,
		"bundles_registered", loaded,
	)
	return nil
}

// validateDatabase performs post-restore validation.
// originalDBName is the database name from the backup manifest so that
// renamed files can be located correctly on disk.
func (rs *RestoreService) validateDatabase(dbName string, manifest *Manifest, originalDBName string) error {
	rs.logger.Info("Validating restored database...")

	// Verify database was created
	_, err := rs.databaseService.GetDatabaseByName(dbName)
	if err != nil {
		return fmt.Errorf("database not found after restore: %w", err)
	}

	// Verify database is locked
	if !rs.lockService.IsLocked(dbName) {
		return fmt.Errorf("database not locked after restore")
	}

	// Verify all manifest files exist on disk.
	// We check presence rather than exact count because the restore process
	// itself may create additional files (e.g. lock metadata).
	// When the database was renamed, file names with the old DB prefix were
	// changed during copy, so we apply the same rename logic here.
	dbPath := helpers.GetDatabaseFolderPath(dbName)
	needsRename := originalDBName != dbName
	oldPrefix := originalDBName + "_"
	newPrefix := dbName + "_"

	var missingFiles []string
	for _, fileEntry := range manifest.Files {
		checkPath := fileEntry.Path
		if needsRename {
			fileName := filepath.Base(checkPath)
			if strings.HasPrefix(fileName, oldPrefix) {
				newName := newPrefix + strings.TrimPrefix(fileName, oldPrefix)
				checkPath = filepath.Join(filepath.Dir(checkPath), newName)
			}
		}
		filePath := filepath.Join(dbPath, checkPath)
		if _, err := os.Stat(filePath); err != nil {
			missingFiles = append(missingFiles, fileEntry.Path)
		}
	}
	if len(missingFiles) > 0 {
		return fmt.Errorf("missing %d files after restore: %v", len(missingFiles), missingFiles)
	}

	rs.logger.Infow("Validation passed",
		"database", dbName,
		"manifest_files", len(manifest.Files),
	)
	return nil
}

// TODO: I will implement selective restore (specific bundles only)
// TODO: I will add point-in-time recovery using WAL replay
// TODO: I will support restoring to different server versions with migration
// TODO: I will add dry-run mode to preview restore without executing
// TODO: I will implement parallel file copying for faster restore
// TODO: I will add restore progress reporting
