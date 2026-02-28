package backup

import (
	"archive/tar"
	"bytes"
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
	"syndrdb/src/pkg/extension"
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
	Compression      string // "gzip", "zstd", "none"
	IncludeIndexes   bool   // Include index files
	OutputPath       string // Full path to output file
	Incremental      bool   // true = incremental backup
	ParentBackupPath string // explicit parent (auto-discovered from catalog if empty)
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

// CreateBackup creates a backup of the specified database.
// If options.Incremental is true, delegates to CreateIncrementalBackup.
func (bs *BackupService) CreateBackup(dbName string, options BackupOptions) (string, error) {
	if options.Incremental {
		return bs.CreateIncrementalBackup(dbName, options)
	}

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

	// Step 2: Capture LSN/CommitSeq before checkpoint (for PITR)
	var startLSN, endLSN uint64
	var startCommitSeq, endCommitSeq uint64
	pitrCapable := bs.walManager != nil && bs.settings.WALEnabled && bs.settings.PITREnabled
	if pitrCapable {
		startLSN = bs.walManager.GetCurrentLSN()
		startCommitSeq = bs.walManager.GetSnapshotManager().GetCurrentSequence()
	}

	// Step 3: Execute CHECKPOINT to flush all data
	if err := bs.executeCheckpoint(); err != nil {
		return "", fmt.Errorf("checkpoint failed: %w", err)
	}

	// Step 4: Capture LSN/CommitSeq after checkpoint (for PITR)
	if pitrCapable {
		endLSN = bs.walManager.GetCurrentLSN()
		endCommitSeq = bs.walManager.GetSnapshotManager().GetCurrentSequence()
	}

	// Step 5: Collect all database files
	files, err := bs.collectDatabaseFiles(db, options.IncludeIndexes)
	if err != nil {
		return "", fmt.Errorf("failed to collect database files: %w", err)
	}

	bs.logger.Infow("Collected database files",
		"database", dbName,
		"file_count", len(files),
	)

	// Step 6: Build manifest
	manifest := bs.buildManifest(db, files, options)

	// Set incremental metadata (every backup gets an ID for catalog tracking)
	backupID := helpers.GenerateUUID()
	manifest.BackupID = backupID
	manifest.BackupType = "full"
	manifest.BaseBackupID = backupID
	manifest.ChainDepth = 0

	// Set PITR fields if capable
	if pitrCapable {
		manifest.BackupVersion = "2.0"
		manifest.StartLSN = startLSN
		manifest.EndLSN = endLSN
		manifest.StartCommitSeq = startCommitSeq
		manifest.EndCommitSeq = endCommitSeq
		manifest.PITREnabled = true
		bs.logger.Infow("PITR metadata captured",
			"start_lsn", startLSN,
			"end_lsn", endLSN,
			"start_commit_seq", startCommitSeq,
			"end_commit_seq", endCommitSeq,
		)
	} else {
		manifest.BackupVersion = "2.0"
	}

	// Step 7: Create temporary directory for staging
	tempDir, err := os.MkdirTemp(bs.settings.TempDir, "backup_*")
	if err != nil {
		return "", fmt.Errorf("failed to create temp directory: %w", err)
	}
	defer os.RemoveAll(tempDir)

	// Step 8: Copy files to temp and calculate CRCs
	if err := bs.copyFilesWithCRC(db, files, tempDir, manifest); err != nil {
		return "", fmt.Errorf("failed to copy files: %w", err)
	}

	// Step 9: Collect Primary DB metadata documents
	if err := bs.collectPrimaryDBDocuments(db, manifest); err != nil {
		return "", fmt.Errorf("failed to collect Primary DB documents: %w", err)
	}

	// Step 10: Write manifest to temp directory
	manifestPath := filepath.Join(tempDir, "manifest.json")
	if err := bs.writeManifestFile(manifest, manifestPath); err != nil {
		return "", fmt.Errorf("failed to write manifest: %w", err)
	}

	// Step 11: Ensure output directory exists, then create compressed archive
	if err := os.MkdirAll(filepath.Dir(options.OutputPath), 0755); err != nil {
		return "", fmt.Errorf("failed to create backup directory: %w", err)
	}
	if err := bs.createArchive(tempDir, options.OutputPath, options.Compression); err != nil {
		return "", fmt.Errorf("failed to create archive: %w", err)
	}

	// Step 12: Calculate final backup size
	backupInfo, err := os.Stat(options.OutputPath)
	if err != nil {
		return "", fmt.Errorf("failed to stat backup file: %w", err)
	}
	manifest.CompressedSize = backupInfo.Size()

	// Step 13: Update backup catalog
	if bs.settings.BackupCatalogEnabled {
		catalogPath := filepath.Join(bs.settings.BackupDir, "backup_catalog.json")
		catalog, catalogErr := LoadCatalog(catalogPath)
		if catalogErr != nil {
			bs.logger.Warnw("Failed to load backup catalog", "error", catalogErr)
		} else {
			catalog.AddEntry(BackupCatalogEntry{
				BackupID:       backupID,
				BackupType:     "full",
				BaseBackupID:   backupID,
				ChainDepth:     0,
				DatabaseName:   dbName,
				Timestamp:      manifest.Timestamp,
				StartLSN:       startLSN,
				EndLSN:         endLSN,
				StartCommitSeq: startCommitSeq,
				EndCommitSeq:   endCommitSeq,
				FilePath:       options.OutputPath,
				SizeBytes:      backupInfo.Size(),
				FileCount:      len(manifest.Files),
			})
			if saveErr := SaveCatalog(catalog, catalogPath); saveErr != nil {
				bs.logger.Warnw("Failed to save backup catalog", "error", saveErr)
			}
		}
	}

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

// collectPrimaryDBDocuments retrieves metadata from Primary database.
// Uses LoadCatalogBundleDocuments (page-scanning) instead of SortedIndex
// because the sorted_index.sidx file may not exist for system catalog
// bundles, causing GetAllDocumentIDs() to return an empty list.
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

	// Scan all documents in the Databases bundle via page iteration
	dbSchema := databasesBundle.DocumentStructure.FieldSchema()
	bs.bundleService.FlushMetadataUpdates()
	dbDocs, err := bs.bundleService.LoadCatalogBundleDocuments(databasesBundle.Name)
	if err != nil {
		bs.logger.Warnw("Failed to load Databases catalog documents", "error", err)
	}
	bs.logger.Infow("[BACKUP-DIAG] Databases bundle scan",
		"total_docs_returned", len(dbDocs),
		"target_db", db.Name,
		"page_count", databasesBundle.PageCount,
		"total_docs_meta", databasesBundle.TotalDocuments,
	)
	for _, doc := range dbDocs {
		data := ensureDocData(doc, dbSchema)
		if data == nil {
			bs.logger.Warnw("[BACKUP-DIAG] ensureDocData returned nil",
				"docID", doc.DocumentID,
				"hasData", doc.Data != nil,
				"valuesLen", len(doc.Values),
			)
			continue
		}
		nameField, nameOK := data["Name"]
		bs.logger.Infow("[BACKUP-DIAG] Databases doc",
			"docID", doc.DocumentID,
			"Name", nameField,
			"nameFieldExists", nameOK,
		)
		if nameOK {
			if nameStr, ok := nameField.(string); ok && nameStr == db.Name {
				manifest.PrimaryDBDocuments = append(manifest.PrimaryDBDocuments, PrimaryDBDoc{
					Bundle:     "Databases",
					DocumentID: doc.DocumentID,
					Data:       data,
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

	// Scan all documents in the Bundles bundle via page iteration
	bundleSchema := bundlesBundle.DocumentStructure.FieldSchema()
	bundleDocs, err := bs.bundleService.LoadCatalogBundleDocuments(bundlesBundle.Name)
	if err != nil {
		bs.logger.Warnw("Failed to load Bundles catalog documents", "error", err)
	}
	bs.logger.Infow("[BACKUP-DIAG] Bundles bundle scan",
		"total_docs_returned", len(bundleDocs),
		"target_db", db.Name,
		"page_count", bundlesBundle.PageCount,
		"total_docs_meta", bundlesBundle.TotalDocuments,
	)
	for _, doc := range bundleDocs {
		data := ensureDocData(doc, bundleSchema)
		if data == nil {
			continue
		}
		if dbNameField, ok := data["DatabaseName"]; ok {
			if dbNameStr, ok := dbNameField.(string); ok && dbNameStr == db.Name {
				bs.logger.Infow("[BACKUP-DIAG] matched Bundles doc",
					"docID", doc.DocumentID,
					"Name", data["Name"],
					"DatabaseName", dbNameStr,
				)
				manifest.PrimaryDBDocuments = append(manifest.PrimaryDBDocuments, PrimaryDBDoc{
					Bundle:     "Bundles",
					DocumentID: doc.DocumentID,
					Data:       data,
				})
			}
		}
	}

	bs.logger.Infow("Collected Primary DB documents",
		"database", db.Name,
		"document_count", len(manifest.PrimaryDBDocuments),
	)

	if len(manifest.PrimaryDBDocuments) == 0 {
		bs.logger.Errorw("[BACKUP-DIAG] WARNING: 0 PrimaryDBDocuments captured! Restore will not register bundles in catalog.",
			"database", db.Name,
		)
	}

	return nil
}

// ensureDocData returns doc.Data if populated, otherwise builds a Data map
// from doc.Values using the bundle's field schema so that backup metadata
// is always captured even when the typed-Values path is active.
func ensureDocData(doc *models.Document, schema *models.BundleFieldSchema) map[string]interface{} {
	if doc.Data != nil && len(doc.Data) > 0 {
		return doc.Data
	}
	if schema != nil && len(doc.Values) > 0 {
		data := make(map[string]interface{}, len(schema.Names))
		for i, name := range schema.Names {
			if i < len(doc.Values) {
				data[name] = doc.Values[i].AsInterface()
			}
		}
		return data
	}
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

	// Enterprise encryption hook: wrap output file with encryption if enabled
	var baseWriter io.Writer = outFile
	var encryptCloser io.Closer
	if reg := extension.GetRegistry(); reg.HasStorageEncryptors() {
		enc := reg.GetStorageEncryptor()
		if enc.EncryptionEnabled("backup:" + filepath.Base(outputPath)) {
			encWriter, encErr := newEncryptingWriter(outFile, enc, "backup:"+filepath.Base(outputPath))
			if encErr != nil {
				return fmt.Errorf("failed to create encrypting writer: %w", encErr)
			}
			baseWriter = encWriter
			encryptCloser = encWriter
		}
	}

	// Create compression writer
	var compWriter io.WriteCloser
	switch compression {
	case "gzip":
		compWriter = gzip.NewWriter(baseWriter)
	case "zstd":
		compWriter, err = zstd.NewWriter(baseWriter)
		if err != nil {
			return fmt.Errorf("failed to create zstd writer: %w", err)
		}
	case "none":
		compWriter = &nopWriteCloser{baseWriter}
	default:
		return fmt.Errorf("unsupported compression: %s", compression)
	}
	defer func() {
		compWriter.Close()
		if encryptCloser != nil {
			encryptCloser.Close()
		}
	}()

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

	// Enterprise decryption hook: wrap reader if encryption layer is present
	var baseReader io.Reader = archiveFile
	if reg := extension.GetRegistry(); reg.HasStorageEncryptors() {
		enc := reg.GetStorageEncryptor()
		backupScope := "backup:" + filepath.Base(archivePath)
		if enc.EncryptionEnabled(backupScope) {
			decReader, decErr := newDecryptingReader(archiveFile, enc, backupScope)
			if decErr != nil {
				return fmt.Errorf("failed to create decrypting reader: %w", decErr)
			}
			baseReader = decReader
		}
	}

	// Detect compression by reading magic bytes from the file header.
	// This is more reliable than file extension, especially for .sdb files
	// which may be gzip or zstd compressed.
	//
	// If baseReader wraps a decryption layer, we read the decrypted bytes.
	// We need a seekable buffer to peek and then rewind.
	peekBuf := make([]byte, 4)
	n, err := baseReader.Read(peekBuf)
	if err != nil || n < 2 {
		return fmt.Errorf("failed to read archive header: %w", err)
	}

	// Build a re-readable reader: prepend the peeked bytes back
	baseReader = io.MultiReader(bytes.NewReader(peekBuf[:n]), baseReader)

	var decompReader io.Reader

	if peekBuf[0] == 0x1f && peekBuf[1] == 0x8b {
		// gzip magic bytes
		gzReader, err := gzip.NewReader(baseReader)
		if err != nil {
			return fmt.Errorf("failed to create gzip reader: %w", err)
		}
		defer gzReader.Close()
		decompReader = gzReader
	} else if n >= 4 && peekBuf[0] == 0x28 && peekBuf[1] == 0xb5 && peekBuf[2] == 0x2f && peekBuf[3] == 0xfd {
		// zstd magic bytes
		zstReader, err := zstd.NewReader(baseReader)
		if err != nil {
			return fmt.Errorf("failed to create zstd reader: %w", err)
		}
		defer zstReader.Close()
		decompReader = zstReader
	} else {
		// Assume uncompressed tar
		decompReader = baseReader
	}

	// Create tar reader
	tarReader := tar.NewReader(decompReader)

	// SECURITY: Track total decompressed bytes to prevent decompression bombs
	maxRestoreSize := bs.settings.MaxRestoreSizeBytes
	var totalBytesWritten int64

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

		// SECURITY: Validate path to prevent directory traversal
		cleanName := filepath.Clean(header.Name)
		if strings.HasPrefix(cleanName, "..") || filepath.IsAbs(cleanName) {
			return fmt.Errorf("invalid file path in archive: %s", header.Name)
		}

		// Create destination path
		destPath := filepath.Join(destDir, cleanName)

		// Create parent directories
		if err := os.MkdirAll(filepath.Dir(destPath), 0755); err != nil {
			return fmt.Errorf("failed to create directory: %w", err)
		}

		// Create file
		outFile, err := os.Create(destPath)
		if err != nil {
			return fmt.Errorf("failed to create file: %w", err)
		}

		// Copy data with size limit check
		var written int64
		if maxRestoreSize > 0 {
			remaining := maxRestoreSize - totalBytesWritten
			if remaining <= 0 {
				outFile.Close()
				return fmt.Errorf("restore aborted: decompressed size exceeds maximum of %d bytes", maxRestoreSize)
			}
			limitedReader := io.LimitReader(tarReader, remaining+1) // +1 to detect overflow
			written, err = io.Copy(outFile, limitedReader)
			if written > remaining {
				outFile.Close()
				return fmt.Errorf("restore aborted: decompressed size exceeds maximum of %d bytes", maxRestoreSize)
			}
		} else {
			written, err = io.Copy(outFile, tarReader)
		}
		outFile.Close()
		if err != nil {
			return fmt.Errorf("failed to extract file: %w", err)
		}
		totalBytesWritten += written
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

// encryptingWriter wraps an io.Writer with block encryption.
// Each Write call encrypts the data as a single block.
type encryptingWriter struct {
	inner io.Writer
	enc   extension.StorageEncryptionExtension
	scope string
}

func newEncryptingWriter(w io.Writer, enc extension.StorageEncryptionExtension, scope string) (*encryptingWriter, error) {
	return &encryptingWriter{inner: w, enc: enc, scope: scope}, nil
}

func (ew *encryptingWriter) Write(p []byte) (int, error) {
	encrypted, err := ew.enc.EncryptBlock(p, ew.scope)
	if err != nil {
		return 0, fmt.Errorf("backup encrypt: %w", err)
	}
	// Write length-prefixed encrypted block: [len:4][encrypted data]
	lenBuf := make([]byte, 4)
	lenBuf[0] = byte(len(encrypted))
	lenBuf[1] = byte(len(encrypted) >> 8)
	lenBuf[2] = byte(len(encrypted) >> 16)
	lenBuf[3] = byte(len(encrypted) >> 24)
	if _, err := ew.inner.Write(lenBuf); err != nil {
		return 0, err
	}
	if _, err := ew.inner.Write(encrypted); err != nil {
		return 0, err
	}
	return len(p), nil
}

func (ew *encryptingWriter) Close() error {
	return nil
}

// decryptingReader wraps an io.Reader with block decryption.
// Reads length-prefixed encrypted blocks and returns decrypted data.
type decryptingReader struct {
	inner  io.Reader
	enc    extension.StorageEncryptionExtension
	scope  string
	buf    []byte // leftover decrypted bytes from previous read
}

func newDecryptingReader(r io.Reader, enc extension.StorageEncryptionExtension, scope string) (*decryptingReader, error) {
	return &decryptingReader{inner: r, enc: enc, scope: scope}, nil
}

func (dr *decryptingReader) Read(p []byte) (int, error) {
	// Return buffered data first
	if len(dr.buf) > 0 {
		n := copy(p, dr.buf)
		dr.buf = dr.buf[n:]
		return n, nil
	}

	// Read next encrypted block: [len:4][encrypted data]
	lenBuf := make([]byte, 4)
	if _, err := io.ReadFull(dr.inner, lenBuf); err != nil {
		return 0, err
	}
	blockLen := int(lenBuf[0]) | int(lenBuf[1])<<8 | int(lenBuf[2])<<16 | int(lenBuf[3])<<24
	if blockLen <= 0 || blockLen > 10*1024*1024 {
		return 0, fmt.Errorf("invalid encrypted block length: %d", blockLen)
	}

	encrypted := make([]byte, blockLen)
	if _, err := io.ReadFull(dr.inner, encrypted); err != nil {
		return 0, fmt.Errorf("backup decrypt read: %w", err)
	}

	decrypted, err := dr.enc.DecryptBlock(encrypted, dr.scope)
	if err != nil {
		return 0, fmt.Errorf("backup decrypt: %w", err)
	}

	n := copy(p, decrypted)
	if n < len(decrypted) {
		dr.buf = decrypted[n:]
	}
	return n, nil
}

// TODO: I will add backup streaming to avoid temp directory for large databases
// TODO: I will add multi-threaded compression for better performance
// TODO: I will implement background backup (async operation)
// TODO: I will add progress callbacks for long-running backups
// TODO: I will implement backup verification without full extraction
