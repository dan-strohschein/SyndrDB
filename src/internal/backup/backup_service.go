package backup

import (
	"archive/tar"
	"compress/gzip"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/klauspost/compress/zstd"
	"go.uber.org/zap"

	"syndrdb/src/internal/domain/bundle"
	"syndrdb/src/internal/domain/database"
	"syndrdb/src/internal/domain/models"
	"syndrdb/src/internal/journal"
	"syndrdb/src/internal/lock"
	"syndrdb/src/pkg/common/helpers"
	"syndrdb/src/pkg/settings"
)

// BackupService handles database backup operations
type BackupService struct {
	databaseService *database.DatabaseService
	bundleService   *bundle.BundleService
	lockService     *lock.LockService
	walManager      *journal.WALManager
	settings        *settings.Arguments
	logger          *zap.SugaredLogger
}

// BackupOptions configures backup behavior
type BackupOptions struct {
	Compression    string // "gzip", "zstd", "none"
	IncludeIndexes bool   // Include index files
	OutputPath     string // Full path to output file
}

// NewBackupService creates a new backup service instance
func NewBackupService(
	databaseService *database.DatabaseService,
	bundleService *bundle.BundleService,
	lockService *lock.LockService,
	walManager *journal.WALManager,
	logger *zap.SugaredLogger,
) *BackupService {
	return &BackupService{
		databaseService: databaseService,
		bundleService:   bundleService,
		lockService:     lockService,
		walManager:      walManager,
		settings:        settings.GetSettings(),
		logger:          logger,
	}
}

// CreateBackup creates a backup of the specified database
func (bs *BackupService) CreateBackup(dbName string, options BackupOptions) (string, error) {
	startTime := time.Now()

	bs.logger.Infow("Starting database backup",
		"database", dbName,
		"output", options.OutputPath,
		"compression", options.Compression,
		"include_indexes", options.IncludeIndexes,
	)

	// Step 1: Validate database exists
	db, err := bs.databaseService.GetDatabaseByName(dbName)
	if err != nil {
		return "", fmt.Errorf("database '%s' not found: %w", dbName, err)
	}

	// Step 2: Execute CHECKPOINT to flush all data
	if err := bs.executeCheckpoint(); err != nil {
		return "", fmt.Errorf("checkpoint failed: %w", err)
	}

	// Step 3: Collect all database files
	files, err := bs.collectDatabaseFiles(db, options.IncludeIndexes)
	if err != nil {
		return "", fmt.Errorf("failed to collect database files: %w", err)
	}

	bs.logger.Infow("Collected database files",
		"database", dbName,
		"file_count", len(files),
	)

	// Step 4: Build manifest
	manifest := bs.buildManifest(db, files, options)

	// Step 5: Create temporary directory for staging
	tempDir, err := os.MkdirTemp(bs.settings.TempDir, "backup_*")
	if err != nil {
		return "", fmt.Errorf("failed to create temp directory: %w", err)
	}
	defer os.RemoveAll(tempDir)

	// Step 6: Copy files to temp and calculate CRCs
	if err := bs.copyFilesWithCRC(db, files, tempDir, manifest); err != nil {
		return "", fmt.Errorf("failed to copy files: %w", err)
	}

	// Step 7: Collect Primary DB metadata documents
	if err := bs.collectPrimaryDBDocuments(db, manifest); err != nil {
		return "", fmt.Errorf("failed to collect Primary DB documents: %w", err)
	}

	// Step 8: Write manifest to temp directory
	manifestPath := filepath.Join(tempDir, "manifest.json")
	if err := bs.writeManifestFile(manifest, manifestPath); err != nil {
		return "", fmt.Errorf("failed to write manifest: %w", err)
	}

	// Step 9: Create compressed archive
	if err := bs.createArchive(tempDir, options.OutputPath, options.Compression); err != nil {
		return "", fmt.Errorf("failed to create archive: %w", err)
	}

	// Step 10: Calculate final backup size
	backupInfo, err := os.Stat(options.OutputPath)
	if err != nil {
		return "", fmt.Errorf("failed to stat backup file: %w", err)
	}
	manifest.CompressedSize = backupInfo.Size()

	duration := time.Since(startTime)
	bs.logger.Infow("Backup completed successfully",
		"database", dbName,
		"output", options.OutputPath,
		"size_bytes", backupInfo.Size(),
		"duration", duration,
		"file_count", len(files),
	)

	return options.OutputPath, nil
}

// executeCheckpoint flushes all in-memory data to disk
func (bs *BackupService) executeCheckpoint() error {
	// Flush WAL if enabled
	if bs.walManager != nil && bs.settings.WALEnabled {
		if err := bs.walManager.Flush(); err != nil {
			return fmt.Errorf("WAL flush failed: %w", err)
		}
		bs.logger.Debug("WAL flushed successfully")
	}

	// TODO: I will add BundleService.FlushAllBundles() to flush dirty bundle pages
	// TODO: I will add IndexManager.FlushAll() to flush index buffers
	// TODO: I will add sync() syscall for OS-level buffer flush

	bs.logger.Info("Checkpoint completed")
	return nil
}

// collectDatabaseFiles gathers all files for a database
func (bs *BackupService) collectDatabaseFiles(db *models.Database, includeIndexes bool) ([]string, error) {
	dbPath := helpers.GetDatabaseFolderPath(db.Name)

	var files []string

	err := filepath.Walk(dbPath, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}

		// Skip directories
		if info.IsDir() {
			return nil
		}

		// Skip temporary and lock files
		if strings.HasSuffix(path, ".lock") || strings.HasSuffix(path, ".tmp") {
			return nil
		}

		// Skip index files if not included
		if !includeIndexes {
			ext := filepath.Ext(path)
			if ext == ".hidx" || ext == ".btidx" {
				return nil
			}
		}

		files = append(files, path)
		return nil
	})

	if err != nil {
		return nil, err
	}

	return files, nil
}

// buildManifest creates the backup manifest structure
func (bs *BackupService) buildManifest(db *models.Database, files []string, options BackupOptions) *Manifest {
	return &Manifest{
		BackupVersion:      "1.0",
		Timestamp:          time.Now(),
		DatabaseName:       db.Name,
		ServerVersion:      bs.settings.Version,
		Compression:        options.Compression,
		IncludesIndexes:    options.IncludeIndexes,
		Files:              make([]FileEntry, 0, len(files)),
		PrimaryDBDocuments: make([]PrimaryDBDoc, 0),
		TotalSizeBytes:     0,
	}
}

// copyFilesWithCRC copies files to temp directory and calculates CRCs
func (bs *BackupService) copyFilesWithCRC(db *models.Database, files []string, tempDir string, manifest *Manifest) error {
	dbPath := helpers.GetDatabaseFolderPath(db.Name)

	for _, srcPath := range files {
		// Read file
		data, err := os.ReadFile(srcPath)
		if err != nil {
			return fmt.Errorf("failed to read %s: %w", srcPath, err)
		}

		// Calculate CRC
		crc, err := CalculateFileCRC(srcPath)
		if err != nil {
			return fmt.Errorf("failed to calculate CRC for %s: %w", srcPath, err)
		}

		// Calculate relative path
		relPath, err := filepath.Rel(dbPath, srcPath)
		if err != nil {
			return fmt.Errorf("failed to get relative path for %s: %w", srcPath, err)
		}

		// Create destination path
		destPath := filepath.Join(tempDir, relPath)

		// Create parent directories
		if err := os.MkdirAll(filepath.Dir(destPath), 0755); err != nil {
			return fmt.Errorf("failed to create directory: %w", err)
		}

		// Write file to temp
		if err := os.WriteFile(destPath, data, 0644); err != nil {
			return fmt.Errorf("failed to write %s: %w", destPath, err)
		}

		// Add to manifest
		fileEntry := FileEntry{
			Path:      relPath,
			SizeBytes: int64(len(data)),
			CRC32:     crc,
		}
		manifest.Files = append(manifest.Files, fileEntry)
		manifest.TotalSizeBytes += int64(len(data))
	}

	return nil
}

// collectPrimaryDBDocuments retrieves metadata from Primary database
func (bs *BackupService) collectPrimaryDBDocuments(db *models.Database, manifest *Manifest) error {
	primaryDB, err := bs.databaseService.GetDatabaseByName("primary")
	if err != nil {
		return fmt.Errorf("Primary database not found: %w", err)
	}

	// Get Databases bundle to find this database's metadata
	databasesBundle, err := bs.bundleService.GetBundleByName(primaryDB, "Databases")
	if err != nil {
		return fmt.Errorf("Databases bundle not found: %w", err)
	}

	// Find the document for this database
	// TODO: I need to iterate through documents using page cache via BundleService.GetDocumentPage()
	// For now, we scan through all document IDs and load each document
	docIDs := databasesBundle.SortedIndex.GetAllDocumentIDs()
	for _, docID := range docIDs {
		doc, err := bs.bundleService.GetDocument(databasesBundle.Name, primaryDB.Name, docID)
		if err != nil {
			continue
		}
		if nameField, ok := doc.Data["Name"]; ok {
			if nameField.(string) == db.Name {
				manifest.PrimaryDBDocuments = append(manifest.PrimaryDBDocuments, PrimaryDBDoc{
					Bundle:     "Databases",
					DocumentID: doc.DocumentID,
					Data:       doc.Data,
				})
				break
			}
		}
	}

	// Get Bundles bundle to collect all bundle metadata for this database
	bundlesBundle, err := bs.bundleService.GetBundleByName(primaryDB, "Bundles")
	if err != nil {
		return fmt.Errorf("Bundles bundle not found: %w", err)
	}

	// Collect all bundle documents for this database
	// TODO: I need to iterate through documents using page cache via BundleService.GetDocumentPage()
	// For now, we scan through all document IDs and load each document
	bundleDocIDs := bundlesBundle.SortedIndex.GetAllDocumentIDs()
	for _, docID := range bundleDocIDs {
		doc, err := bs.bundleService.GetDocument(bundlesBundle.Name, primaryDB.Name, docID)
		if err != nil {
			continue
		}
		if dbNameField, ok := doc.Data["DatabaseName"]; ok {
			if dbNameField.(string) == db.Name {
				manifest.PrimaryDBDocuments = append(manifest.PrimaryDBDocuments, PrimaryDBDoc{
					Bundle:     "Bundles",
					DocumentID: doc.DocumentID,
					Data:       doc.Data,
				})
			}
		}
	}

	bs.logger.Infow("Collected Primary DB documents",
		"database", db.Name,
		"document_count", len(manifest.PrimaryDBDocuments),
	)

	return nil
}

// writeManifestFile writes the manifest to a JSON file
func (bs *BackupService) writeManifestFile(manifest *Manifest, path string) error {
	file, err := os.Create(path)
	if err != nil {
		return err
	}
	defer file.Close()

	return WriteManifest(manifest, file)
}

// createArchive creates a compressed tar archive from the temp directory
func (bs *BackupService) createArchive(srcDir, outputPath, compression string) error {
	// Create output file
	outFile, err := os.Create(outputPath)
	if err != nil {
		return fmt.Errorf("failed to create output file: %w", err)
	}
	defer outFile.Close()

	// Create compression writer
	var compWriter io.WriteCloser
	switch compression {
	case "gzip":
		compWriter = gzip.NewWriter(outFile)
	case "zstd":
		compWriter, err = zstd.NewWriter(outFile)
		if err != nil {
			return fmt.Errorf("failed to create zstd writer: %w", err)
		}
	case "none":
		compWriter = &nopWriteCloser{outFile}
	default:
		return fmt.Errorf("unsupported compression: %s", compression)
	}
	defer compWriter.Close()

	// Create tar writer
	tarWriter := tar.NewWriter(compWriter)
	defer tarWriter.Close()

	// Walk source directory and add files to archive
	return filepath.Walk(srcDir, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}

		// Skip directories
		if info.IsDir() {
			return nil
		}

		// Create tar header
		header, err := tar.FileInfoHeader(info, "")
		if err != nil {
			return fmt.Errorf("failed to create tar header: %w", err)
		}

		// Set relative path
		relPath, err := filepath.Rel(srcDir, path)
		if err != nil {
			return err
		}
		header.Name = relPath

		// Write header
		if err := tarWriter.WriteHeader(header); err != nil {
			return fmt.Errorf("failed to write tar header: %w", err)
		}

		// Write file data
		data, err := os.ReadFile(path)
		if err != nil {
			return fmt.Errorf("failed to read file: %w", err)
		}

		if _, err := tarWriter.Write(data); err != nil {
			return fmt.Errorf("failed to write file data: %w", err)
		}

		return nil
	})
}

// ValidateBackup verifies the integrity of a backup file
func (bs *BackupService) ValidateBackup(backupPath string) error {
	// Extract to temp directory
	tempDir, err := os.MkdirTemp(bs.settings.TempDir, "validate_*")
	if err != nil {
		return fmt.Errorf("failed to create temp directory: %w", err)
	}
	defer os.RemoveAll(tempDir)

	// Extract archive
	if err := bs.extractArchive(backupPath, tempDir); err != nil {
		return fmt.Errorf("failed to extract backup: %w", err)
	}

	// Read manifest
	manifestPath := filepath.Join(tempDir, "manifest.json")
	manifestFile, err := os.Open(manifestPath)
	if err != nil {
		return fmt.Errorf("manifest not found: %w", err)
	}
	defer manifestFile.Close()

	manifest, err := ReadManifest(manifestFile)
	if err != nil {
		return fmt.Errorf("failed to read manifest: %w", err)
	}

	// Verify CRCs for all files
	for _, fileEntry := range manifest.Files {
		filePath := filepath.Join(tempDir, fileEntry.Path)
		if err := VerifyFileCRC(filePath, fileEntry.CRC32); err != nil {
			return err
		}
	}

	bs.logger.Infow("Backup validation successful",
		"backup", backupPath,
		"file_count", len(manifest.Files),
	)

	return nil
}

// GetBackupInfo extracts and returns manifest information without full restore
func (bs *BackupService) GetBackupInfo(backupPath string) (*Manifest, error) {
	// Extract to temp directory
	tempDir, err := os.MkdirTemp(bs.settings.TempDir, "info_*")
	if err != nil {
		return nil, fmt.Errorf("failed to create temp directory: %w", err)
	}
	defer os.RemoveAll(tempDir)

	// Extract archive
	if err := bs.extractArchive(backupPath, tempDir); err != nil {
		return nil, fmt.Errorf("failed to extract backup: %w", err)
	}

	// Read manifest
	manifestPath := filepath.Join(tempDir, "manifest.json")
	manifestFile, err := os.Open(manifestPath)
	if err != nil {
		return nil, fmt.Errorf("manifest not found: %w", err)
	}
	defer manifestFile.Close()

	return ReadManifest(manifestFile)
}

// extractArchive extracts a backup archive to a directory
func (bs *BackupService) extractArchive(archivePath, destDir string) error {
	// Open archive file
	archiveFile, err := os.Open(archivePath)
	if err != nil {
		return fmt.Errorf("failed to open archive: %w", err)
	}
	defer archiveFile.Close()

	// Detect compression based on file extension
	var decompReader io.Reader
	ext := filepath.Ext(archivePath)

	if strings.Contains(ext, "gz") || strings.Contains(archivePath, ".tar.gz") {
		gzReader, err := gzip.NewReader(archiveFile)
		if err != nil {
			return fmt.Errorf("failed to create gzip reader: %w", err)
		}
		defer gzReader.Close()
		decompReader = gzReader
	} else if strings.Contains(ext, "zst") || strings.Contains(archivePath, ".tar.zst") {
		zstReader, err := zstd.NewReader(archiveFile)
		if err != nil {
			return fmt.Errorf("failed to create zstd reader: %w", err)
		}
		defer zstReader.Close()
		decompReader = zstReader
	} else {
		decompReader = archiveFile
	}

	// Create tar reader
	tarReader := tar.NewReader(decompReader)

	// Extract files
	for {
		header, err := tarReader.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			return fmt.Errorf("failed to read tar header: %w", err)
		}

		// Skip directories
		if header.Typeflag == tar.TypeDir {
			continue
		}

		// Create destination path
		destPath := filepath.Join(destDir, header.Name)

		// Create parent directories
		if err := os.MkdirAll(filepath.Dir(destPath), 0755); err != nil {
			return fmt.Errorf("failed to create directory: %w", err)
		}

		// Create file
		outFile, err := os.Create(destPath)
		if err != nil {
			return fmt.Errorf("failed to create file: %w", err)
		}

		// Copy data
		if _, err := io.Copy(outFile, tarReader); err != nil {
			outFile.Close()
			return fmt.Errorf("failed to extract file: %w", err)
		}
		outFile.Close()
	}

	return nil
}

// nopWriteCloser wraps an io.Writer to add a no-op Close method
type nopWriteCloser struct {
	io.Writer
}

func (nwc *nopWriteCloser) Close() error {
	return nil
}

// TODO: I will implement incremental backups (track LSN/checkpoint numbers)
// TODO: I will add backup streaming to avoid temp directory for large databases
// TODO: I will add multi-threaded compression for better performance
// TODO: I will implement background backup (async operation)
// TODO: I will add progress callbacks for long-running backups
// TODO: I will implement backup verification without full extraction
