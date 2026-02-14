package backup

import (
	"fmt"
	"os"
	"path/filepath"
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

	// Step 6: Copy files from temp to database directory
	if err := rs.copyFiles(tempDir, dbPath, manifest); err != nil {
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

	// Step 8: Restore Primary DB documents (metadata)
	if err := rs.restorePrimaryDBDocs(targetDBName, manifest); err != nil {
		// Rollback: Delete database and directory
		rs.databaseService.DeleteDatabase(targetDBName)
		os.RemoveAll(dbPath)
		return fmt.Errorf("failed to restore metadata (rolled back): %w", err)
	}

	// Step 9: Validate restored database
	if err := rs.validateDatabase(targetDBName, manifest); err != nil {
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

// copyFiles copies files from temp to database directory
func (rs *RestoreService) copyFiles(srcDir, destDir string, manifest *Manifest) error {
	rs.logger.Info("Copying database files...")

	for _, fileEntry := range manifest.Files {
		srcPath := filepath.Join(srcDir, fileEntry.Path)
		destPath := filepath.Join(destDir, fileEntry.Path)

		// Create parent directories
		if err := os.MkdirAll(filepath.Dir(destPath), 0755); err != nil {
			return fmt.Errorf("failed to create directory: %w", err)
		}

		// Read source file
		data, err := os.ReadFile(srcPath)
		if err != nil {
			return fmt.Errorf("failed to read %s: %w", srcPath, err)
		}

		// Write destination file
		if err := os.WriteFile(destPath, data, 0644); err != nil {
			return fmt.Errorf("failed to write %s: %w", destPath, err)
		}
	}

	rs.logger.Infow("Files copied successfully",
		"file_count", len(manifest.Files),
	)
	return nil
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
	rs.logger.Info("Restoring Primary database metadata...")

	primaryDB, err := rs.databaseService.GetDatabaseByName("primary")
	if err != nil {
		return fmt.Errorf("Primary database not found: %w", err)
	}

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

		// Create document from data (Option B: Data-only for restore; bundle add path will use Data when Values is nil)
		document := &models.Document{
			DocumentID: doc.DocumentID,
			Data:       doc.Data,
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

// validateDatabase performs post-restore validation
func (rs *RestoreService) validateDatabase(dbName string, manifest *Manifest) error {
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

	// Verify file count matches manifest
	dbPath := helpers.GetDatabaseFolderPath(dbName)
	var actualFileCount int
	err = filepath.Walk(dbPath, func(path string, info os.FileInfo, err error) error {
		if err == nil && !info.IsDir() {
			actualFileCount++
		}
		return nil
	})
	if err != nil {
		return fmt.Errorf("failed to count files: %w", err)
	}

	expectedFileCount := len(manifest.Files)
	if actualFileCount != expectedFileCount {
		return fmt.Errorf("file count mismatch: expected %d, got %d", expectedFileCount, actualFileCount)
	}

	rs.logger.Infow("Validation passed",
		"database", dbName,
		"file_count", actualFileCount,
	)
	return nil
}

// TODO: I will implement selective restore (specific bundles only)
// TODO: I will add point-in-time recovery using WAL replay
// TODO: I will support restoring to different server versions with migration
// TODO: I will add dry-run mode to preview restore without executing
// TODO: I will implement parallel file copying for faster restore
// TODO: I will add restore progress reporting
