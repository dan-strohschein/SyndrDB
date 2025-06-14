package buffermgr

import (
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"

	"go.uber.org/zap"
)

/*

This is exclusively for managing file access in the buffer pool.
It provides a registry for files, allowing them to be opened, closed, and managed with reference counting.

*/

// ManagedFile wraps an os.File with additional management capabilities
type ManagedFile struct {
	mu         sync.RWMutex
	file       *os.File
	path       string
	refCount   int
	lastAccess int64
}

// Lock acquires an exclusive lock on the file
func (mf *ManagedFile) Lock() {
	mf.mu.Lock()
}

// Unlock releases the exclusive lock on the file
func (mf *ManagedFile) Unlock() {
	mf.mu.Unlock()
}

// RLock acquires a read lock on the file
func (mf *ManagedFile) RLock() {
	mf.mu.RLock()
}

// RUnlock releases the read lock on the file
func (mf *ManagedFile) RUnlock() {
	mf.mu.RUnlock()
}

// Seek seeks to the specified position in the file
func (mf *ManagedFile) Seek(offset int64, whence int) (int64, error) {
	return mf.file.Seek(offset, whence)
}

// Read reads data from the file
func (mf *ManagedFile) Read(b []byte) (int, error) {
	return mf.file.Read(b)
}

// Write writes data to the file
func (mf *ManagedFile) Write(b []byte) (int, error) {
	return mf.file.Write(b)
}

// Sync synchronizes the file data to disk
func (mf *ManagedFile) Sync() error {
	return mf.file.Sync()
}

// FileRegistry manages access to files used by the buffer pool
type FileRegistry struct {
	mu         sync.Mutex
	files      map[uint32]*ManagedFile
	dataDir    string
	fileIDMap  map[string]uint32 // Maps filenames to fileIDs
	nextFileID uint32
	syncPolicy SyncPolicy
	logger     *zap.SugaredLogger
}

// SyncPolicy defines when files should be synchronized to disk
type SyncPolicy int

const (
	// SyncNever never automatically syncs files
	SyncNever SyncPolicy = iota

	// SyncAlways syncs after every write
	SyncAlways

	// SyncInterval syncs every N writes
	SyncInterval
)

// NewFileRegistry creates a new file registry
func NewFileRegistry(dataDir string, syncPolicy SyncPolicy, logger *zap.SugaredLogger) (*FileRegistry, error) {
	// Create data directory if it doesn't exist
	if err := os.MkdirAll(dataDir, 0755); err != nil {
		return nil, err
	}

	return &FileRegistry{
		files:      make(map[uint32]*ManagedFile),
		fileIDMap:  make(map[string]uint32),
		dataDir:    dataDir,
		nextFileID: 1, // Start from 1, 0 reserved
		syncPolicy: syncPolicy,
		logger:     logger,
	}, nil
}

// GetFile returns a managed file for the given fileID
func (fr *FileRegistry) GetFile(fileID uint32) (*ManagedFile, error) {
	fr.mu.Lock()
	defer fr.mu.Unlock()

	// Check if we already have the file open
	file, exists := fr.files[fileID]
	if exists {
		file.refCount++
		return file, nil
	}

	// We need to find the path for this fileID
	var filePath string
	found := false

	for path, id := range fr.fileIDMap {
		if id == fileID {
			filePath = path
			found = true
			break
		}
	}

	if !found {
		return nil, fmt.Errorf("no file path found for fileID %d", fileID)
	}

	// Open the file
	fullPath := filepath.Join(fr.dataDir, filePath)
	osFile, err := os.OpenFile(fullPath, os.O_RDWR|os.O_CREATE, 0644)
	if err != nil {
		return nil, fmt.Errorf("failed to open file %s: %w", fullPath, err)
	}

	// Create a managed file
	managedFile := &ManagedFile{
		file:       osFile,
		path:       filePath,
		refCount:   1,
		lastAccess: time.Now().UnixNano(),
	}

	// Store it
	fr.files[fileID] = managedFile

	return managedFile, nil
}

// RegisterFile registers a file with the registry and returns its fileID
func (fr *FileRegistry) RegisterFile(filePath string) (uint32, error) {
	fr.mu.Lock()
	defer fr.mu.Unlock()

	// Check if file is already registered
	if fileID, exists := fr.fileIDMap[filePath]; exists {
		return fileID, nil
	}

	// Assign a new fileID
	fileID := fr.nextFileID
	fr.nextFileID++

	// Register it
	fr.fileIDMap[filePath] = fileID

	return fileID, nil
}

// CloseFile decrements the reference count for a file and closes it if no longer in use
func (fr *FileRegistry) CloseFile(fileID uint32) error {
	fr.mu.Lock()
	defer fr.mu.Unlock()

	file, exists := fr.files[fileID]
	if !exists {
		return fmt.Errorf("no open file with ID %d", fileID)
	}

	file.refCount--
	if file.refCount <= 0 {
		// Close the file
		if err := file.file.Close(); err != nil {
			return fmt.Errorf("failed to close file %d: %w", fileID, err)
		}

		delete(fr.files, fileID)
	}

	return nil
}

// CloseAllFiles closes all open files
func (fr *FileRegistry) CloseAllFiles() error {
	fr.mu.Lock()
	defer fr.mu.Unlock()

	var lastErr error
	for fileID, file := range fr.files {
		if err := file.file.Close(); err != nil {
			lastErr = fmt.Errorf("failed to close file %d: %w", fileID, err)
			fr.logger.Errorf("Failed to close file %d: %v", fileID, err)
		}
	}

	fr.files = make(map[uint32]*ManagedFile)

	return lastErr
}

// ShouldSyncWrites returns whether writes should be synced according to policy
func (fr *FileRegistry) ShouldSyncWrites() bool {
	return fr.syncPolicy == SyncAlways
}

// GetSyncInterval returns the configured sync interval
func (fr *FileRegistry) GetSyncInterval() int {
	return 100 // Default to 100 writes between syncs
}
