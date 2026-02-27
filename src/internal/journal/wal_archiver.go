package journal

import (
	"compress/gzip"
	"encoding/json"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"time"

	"github.com/klauspost/compress/zstd"
	"go.uber.org/zap"
)

// WALArchiverBackend defines the interface for WAL archive storage.
// The default implementation is LocalFilesystemBackend; future backends
// (S3, GCS, NFS) can implement this interface without refactoring the archiver.
type WALArchiverBackend interface {
	Store(fileName string, reader io.Reader, metadata WALArchiveFileEntry) error
	List() ([]WALArchiveFileEntry, error)
	Retrieve(fileName string) (io.ReadCloser, error)
	Delete(fileName string) error
	Close() error
}

// WALArchiveFileEntry tracks metadata for a single archived WAL file.
type WALArchiveFileEntry struct {
	FileName       string    `json:"file_name"`
	FirstLSN       uint64    `json:"first_lsn"`
	LastLSN        uint64    `json:"last_lsn"`
	FirstTimestamp time.Time `json:"first_timestamp"`
	LastTimestamp   time.Time `json:"last_timestamp"`
	CRC32          uint32    `json:"crc32"`
	SizeBytes      int64     `json:"size_bytes"`
	Compressed     bool      `json:"compressed,omitempty"`
	ArchivedAt     time.Time `json:"archived_at"`
}

// WALArchiveManifest tracks all archived WAL files.
type WALArchiveManifest struct {
	Files   []WALArchiveFileEntry `json:"files"`
	Updated time.Time             `json:"updated"`
}

// WALArchiver manages continuous archiving of completed WAL files.
type WALArchiver struct {
	mu            sync.Mutex
	walDir        string // Source: WAL directory
	backend       WALArchiverBackend
	compression   string // "none", "gzip", "zstd"
	pollInterval  time.Duration
	retentionDays int
	logger        *zap.SugaredLogger
	wal           *WriteAheadLog // Reference to active WAL (to skip current file)

	stopCh chan struct{}
	wg     sync.WaitGroup
}

// WALArchiverConfig holds configuration for the WAL archiver.
type WALArchiverConfig struct {
	WALDir        string
	ArchiveDir    string
	Compression   string
	PollInterval  time.Duration
	RetentionDays int
	Backend       WALArchiverBackend // If nil, uses LocalFilesystemBackend
}

// NewWALArchiver creates a new WAL archiver. If config.Backend is nil,
// a LocalFilesystemBackend is created using config.ArchiveDir.
func NewWALArchiver(config WALArchiverConfig, wal *WriteAheadLog, logger *zap.SugaredLogger) (*WALArchiver, error) {
	backend := config.Backend
	if backend == nil {
		var err error
		backend, err = NewLocalFilesystemBackend(config.ArchiveDir)
		if err != nil {
			return nil, fmt.Errorf("failed to create local filesystem backend: %w", err)
		}
	}

	return &WALArchiver{
		walDir:        config.WALDir,
		backend:       backend,
		compression:   config.Compression,
		pollInterval:  config.PollInterval,
		retentionDays: config.RetentionDays,
		logger:        logger,
		wal:           wal,
		stopCh:        make(chan struct{}),
	}, nil
}

// Start begins the archiver polling loop in a background goroutine.
func (a *WALArchiver) Start() {
	a.wg.Add(1)
	go a.archiveLoop()
	a.logger.Infow("WAL archiver started",
		"wal_dir", a.walDir,
		"poll_interval", a.pollInterval,
		"compression", a.compression,
		"retention_days", a.retentionDays,
	)
}

// Stop signals the archiver to stop and waits for completion.
func (a *WALArchiver) Stop() {
	close(a.stopCh)
	a.wg.Wait()
	if err := a.backend.Close(); err != nil {
		a.logger.Warnw("Failed to close archiver backend", "error", err)
	}
	a.logger.Info("WAL archiver stopped")
}

func (a *WALArchiver) archiveLoop() {
	defer a.wg.Done()

	// Run once immediately on start
	a.archivePendingFiles()

	ticker := time.NewTicker(a.pollInterval)
	defer ticker.Stop()

	for {
		select {
		case <-a.stopCh:
			// Final archive pass before stopping
			a.archivePendingFiles()
			return
		case <-ticker.C:
			a.archivePendingFiles()
			a.cleanupExpiredArchives()
		}
	}
}

// archivePendingFiles discovers completed WAL files and archives them.
func (a *WALArchiver) archivePendingFiles() {
	a.mu.Lock()
	defer a.mu.Unlock()

	// Discover WAL files in the WAL directory
	entries, err := os.ReadDir(a.walDir)
	if err != nil {
		a.logger.Warnw("Failed to read WAL directory", "dir", a.walDir, "error", err)
		return
	}

	// Get the active WAL file name to skip it
	activeFileName := a.getActiveWALFileName()

	// Get already-archived files to avoid re-archiving
	archived, err := a.backend.List()
	if err != nil {
		a.logger.Warnw("Failed to list archived files", "error", err)
		return
	}
	archivedSet := make(map[string]bool, len(archived))
	for _, entry := range archived {
		archivedSet[entry.FileName] = true
	}

	// Find completed WAL files to archive
	var toArchive []string
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		name := entry.Name()
		if !walFilePattern.MatchString(name) {
			continue
		}
		// Skip the currently active WAL file
		if name == activeFileName {
			continue
		}
		// Skip already-archived files
		if archivedSet[name] {
			continue
		}
		toArchive = append(toArchive, name)
	}

	// Sort by name for deterministic ordering
	sort.Strings(toArchive)

	for _, name := range toArchive {
		if err := a.archiveFile(name); err != nil {
			a.logger.Warnw("Failed to archive WAL file", "file", name, "error", err)
		}
	}
}

// getActiveWALFileName returns the filename of the currently active WAL file.
func (a *WALArchiver) getActiveWALFileName() string {
	if a.wal == nil || a.wal.file == nil {
		return ""
	}
	a.wal.mutex.RLock()
	defer a.wal.mutex.RUnlock()
	if a.wal.file == nil {
		return ""
	}
	return filepath.Base(a.wal.file.Name())
}

// archiveFile archives a single WAL file.
func (a *WALArchiver) archiveFile(fileName string) error {
	srcPath := filepath.Join(a.walDir, fileName)

	// Get file info for size
	info, err := os.Stat(srcPath)
	if err != nil {
		return fmt.Errorf("stat failed: %w", err)
	}

	// Calculate CRC32 of the source file
	crc, err := calculateFileCRC32(srcPath)
	if err != nil {
		return fmt.Errorf("CRC calculation failed: %w", err)
	}

	// Extract LSN/timestamp bounds by scanning the WAL file
	firstLSN, lastLSN, firstTS, lastTS := a.extractBounds(srcPath)

	// Build metadata entry
	meta := WALArchiveFileEntry{
		FileName:       fileName,
		FirstLSN:       firstLSN,
		LastLSN:        lastLSN,
		FirstTimestamp: firstTS,
		LastTimestamp:   lastTS,
		CRC32:          crc,
		SizeBytes:      info.Size(),
		Compressed:     a.compression != "none" && a.compression != "",
		ArchivedAt:     time.Now(),
	}

	// Open source file
	src, err := os.Open(srcPath)
	if err != nil {
		return fmt.Errorf("open failed: %w", err)
	}
	defer src.Close()

	// Optionally wrap in compression
	var reader io.Reader = src
	if a.compression == "gzip" || a.compression == "zstd" {
		pr, pw := io.Pipe()
		go func() {
			var compWriter io.WriteCloser
			var compErr error
			switch a.compression {
			case "gzip":
				compWriter = gzip.NewWriter(pw)
			case "zstd":
				compWriter, compErr = zstd.NewWriter(pw)
				if compErr != nil {
					pw.CloseWithError(compErr)
					return
				}
			}
			if _, err := io.Copy(compWriter, src); err != nil {
				compWriter.Close()
				pw.CloseWithError(err)
				return
			}
			compWriter.Close()
			pw.Close()
		}()
		reader = pr
	}

	// Store via backend
	if err := a.backend.Store(fileName, reader, meta); err != nil {
		return fmt.Errorf("store failed: %w", err)
	}

	a.logger.Infow("Archived WAL file",
		"file", fileName,
		"size", info.Size(),
		"first_lsn", firstLSN,
		"last_lsn", lastLSN,
		"compressed", meta.Compressed,
	)

	return nil
}

// extractBounds scans a WAL file to extract LSN and timestamp bounds.
func (a *WALArchiver) extractBounds(filePath string) (firstLSN, lastLSN uint64, firstTS, lastTS time.Time) {
	if a.wal == nil {
		return
	}

	first := true
	_ = a.wal.ReplayOperationsBinary(filePath, 0, func(entry WALEntry) error {
		if first {
			firstLSN = entry.LSN
			firstTS = entry.Timestamp
			first = false
		}
		if entry.LSN > lastLSN {
			lastLSN = entry.LSN
		}
		lastTS = entry.Timestamp
		return nil
	})

	return
}

// cleanupExpiredArchives removes archived files older than the retention period.
func (a *WALArchiver) cleanupExpiredArchives() {
	if a.retentionDays <= 0 {
		return
	}

	cutoff := time.Now().AddDate(0, 0, -a.retentionDays)
	entries, err := a.backend.List()
	if err != nil {
		a.logger.Warnw("Failed to list archives for cleanup", "error", err)
		return
	}

	for _, entry := range entries {
		if entry.ArchivedAt.Before(cutoff) {
			if err := a.backend.Delete(entry.FileName); err != nil {
				a.logger.Warnw("Failed to delete expired archive", "file", entry.FileName, "error", err)
			} else {
				a.logger.Infow("Deleted expired WAL archive", "file", entry.FileName, "archived_at", entry.ArchivedAt)
			}
		}
	}
}

// calculateFileCRC32 computes a CRC32 checksum for a file.
func calculateFileCRC32(filePath string) (uint32, error) {
	f, err := os.Open(filePath)
	if err != nil {
		return 0, err
	}
	defer f.Close()

	h := crc32.NewIEEE()
	if _, err := io.Copy(h, f); err != nil {
		return 0, err
	}
	return h.Sum32(), nil
}

// --- LocalFilesystemBackend ---

// LocalFilesystemBackend stores archived WAL files on the local filesystem.
type LocalFilesystemBackend struct {
	archiveDir   string
	manifestPath string
	mu           sync.Mutex
}

// NewLocalFilesystemBackend creates a local filesystem backend.
func NewLocalFilesystemBackend(archiveDir string) (*LocalFilesystemBackend, error) {
	if err := os.MkdirAll(archiveDir, 0755); err != nil {
		return nil, fmt.Errorf("failed to create archive directory: %w", err)
	}
	return &LocalFilesystemBackend{
		archiveDir:   archiveDir,
		manifestPath: filepath.Join(archiveDir, "wal_archive.manifest"),
	}, nil
}

func (b *LocalFilesystemBackend) Store(fileName string, reader io.Reader, metadata WALArchiveFileEntry) error {
	destPath := filepath.Join(b.archiveDir, fileName)
	if metadata.Compressed {
		destPath += ".compressed"
	}

	f, err := os.Create(destPath)
	if err != nil {
		return fmt.Errorf("create failed: %w", err)
	}

	if _, err := io.Copy(f, reader); err != nil {
		f.Close()
		os.Remove(destPath)
		return fmt.Errorf("copy failed: %w", err)
	}

	if err := f.Close(); err != nil {
		return fmt.Errorf("close failed: %w", err)
	}

	// Update manifest
	return b.addToManifest(metadata)
}

func (b *LocalFilesystemBackend) List() ([]WALArchiveFileEntry, error) {
	manifest, err := b.loadManifest()
	if err != nil {
		return nil, err
	}
	return manifest.Files, nil
}

func (b *LocalFilesystemBackend) Retrieve(fileName string) (io.ReadCloser, error) {
	// Try compressed first, then uncompressed
	compressedPath := filepath.Join(b.archiveDir, fileName+".compressed")
	if _, err := os.Stat(compressedPath); err == nil {
		return os.Open(compressedPath)
	}
	return os.Open(filepath.Join(b.archiveDir, fileName))
}

func (b *LocalFilesystemBackend) Delete(fileName string) error {
	// Remove file (compressed or not)
	compressedPath := filepath.Join(b.archiveDir, fileName+".compressed")
	if _, err := os.Stat(compressedPath); err == nil {
		os.Remove(compressedPath)
	}
	plainPath := filepath.Join(b.archiveDir, fileName)
	if _, err := os.Stat(plainPath); err == nil {
		os.Remove(plainPath)
	}

	// Remove from manifest
	return b.removeFromManifest(fileName)
}

func (b *LocalFilesystemBackend) Close() error {
	return nil
}

func (b *LocalFilesystemBackend) addToManifest(entry WALArchiveFileEntry) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	manifest, err := b.loadManifest()
	if err != nil {
		manifest = &WALArchiveManifest{}
	}

	manifest.Files = append(manifest.Files, entry)
	manifest.Updated = time.Now()

	return b.saveManifest(manifest)
}

func (b *LocalFilesystemBackend) removeFromManifest(fileName string) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	manifest, err := b.loadManifest()
	if err != nil {
		return nil // Nothing to remove
	}

	filtered := make([]WALArchiveFileEntry, 0, len(manifest.Files))
	for _, f := range manifest.Files {
		if f.FileName != fileName {
			filtered = append(filtered, f)
		}
	}
	manifest.Files = filtered
	manifest.Updated = time.Now()

	return b.saveManifest(manifest)
}

func (b *LocalFilesystemBackend) loadManifest() (*WALArchiveManifest, error) {
	data, err := os.ReadFile(b.manifestPath)
	if err != nil {
		if os.IsNotExist(err) {
			return &WALArchiveManifest{}, nil
		}
		return nil, fmt.Errorf("read manifest failed: %w", err)
	}

	var manifest WALArchiveManifest
	if err := json.Unmarshal(data, &manifest); err != nil {
		return nil, fmt.Errorf("unmarshal manifest failed: %w", err)
	}
	return &manifest, nil
}

func (b *LocalFilesystemBackend) saveManifest(manifest *WALArchiveManifest) error {
	data, err := json.MarshalIndent(manifest, "", "  ")
	if err != nil {
		return fmt.Errorf("marshal manifest failed: %w", err)
	}

	return os.WriteFile(b.manifestPath, data, 0644)
}

// GetArchiveDir returns the archive directory path.
func (b *LocalFilesystemBackend) GetArchiveDir() string {
	return b.archiveDir
}
