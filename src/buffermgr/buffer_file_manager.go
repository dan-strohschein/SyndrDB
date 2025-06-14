package buffermgr

import (
	"fmt"
	"os"
	"path/filepath"
	"sync"

	"go.uber.org/zap"
)

// FileManager handles files on disk
type FileManager struct {
	mu            sync.Mutex
	openFiles     map[uint32]*os.File
	dataDir       string
	bufferPool    *BufferPool
	fileIDCounter uint32
	fileIDMap     map[string]uint32 // Maps filenames to fileIDs
	logger        *zap.SugaredLogger
}

// NewFileManager creates a new file manager
func NewFileManager(dataDir string, bufferPool *BufferPool, logger *zap.SugaredLogger) (*FileManager, error) {
	// Create data directory if it doesn't exist
	err := os.MkdirAll(dataDir, 0755)
	if err != nil {
		return nil, fmt.Errorf("could not create data directory: %w", err)
	}

	return &FileManager{
		openFiles:     make(map[uint32]*os.File),
		dataDir:       dataDir,
		bufferPool:    bufferPool,
		fileIDCounter: 1, // Start from 1, 0 is reserved
		fileIDMap:     make(map[string]uint32),
		logger:        logger,
	}, nil
}

// GetFileID returns the fileID for a given filename, creating it if necessary
func (fm *FileManager) GetFileID(filename string) (uint32, error) {
	fm.mu.Lock()
	defer fm.mu.Unlock()

	// Check if we already have a fileID for this filename
	fileID, exists := fm.fileIDMap[filename]
	if exists {
		return fileID, nil
	}

	// Assign a new fileID
	fileID = fm.fileIDCounter
	fm.fileIDCounter++

	// Add to the map
	fm.fileIDMap[filename] = fileID

	return fileID, nil
}

// GetFilePath returns the full path for a file
func (fm *FileManager) GetFilePath(filename string) string {
	return filepath.Join(fm.dataDir, filename)
}

// OpenFile opens a file and returns its fileID
func (fm *FileManager) OpenFile(filename string) (uint32, error) {
	fm.mu.Lock()
	defer fm.mu.Unlock()

	// Get or create a fileID
	fileID, exists := fm.fileIDMap[filename]
	if !exists {
		fileID = fm.fileIDCounter
		fm.fileIDCounter++
		fm.fileIDMap[filename] = fileID
	}

	// Check if file is already open
	_, fileOpen := fm.openFiles[fileID]
	if fileOpen {
		return fileID, nil
	}

	// Open the file
	filePath := filepath.Join(fm.dataDir, filename)
	file, err := os.OpenFile(filePath, os.O_RDWR|os.O_CREATE, 0644)
	if err != nil {
		return 0, fmt.Errorf("could not open file %s: %w", filePath, err)
	}

	fm.openFiles[fileID] = file

	return fileID, nil
}

// ReadPage reads a page through the buffer manager
func (fm *FileManager) ReadPage(fileID uint32, blockNum uint32) (*DBPageBuffer, error) {
	// Get the file handle
	fm.mu.Lock()
	_, exists := fm.openFiles[fileID]
	fm.mu.Unlock()

	if !exists {
		return nil, fmt.Errorf("no open file with ID %d", fileID)
	}

	// Get the page through the buffer pool
	return fm.bufferPool.GetPage(fileID, blockNum)
}

// ReleasePage decrements the reference count for a buffer
func (fm *FileManager) ReleasePage(buffer *DBPageBuffer) {
	fm.bufferPool.ReleaseBuffer(buffer)
}

// WritePage marks a page as dirty
func (fm *FileManager) WritePage(buffer *DBPageBuffer) {
	fm.bufferPool.MarkBufferDirty(buffer)
}

// Close closes all open files
func (fm *FileManager) Close() error {
	fm.mu.Lock()
	defer fm.mu.Unlock()

	// Flush any dirty buffers
	err := fm.bufferPool.FlushAllDirty()
	if err != nil {
		return fmt.Errorf("could not flush dirty buffers: %w", err)
	}

	// Close all open files
	for fileID, file := range fm.openFiles {
		err := file.Close()
		if err != nil {
			return fmt.Errorf("could not close file ID %d: %w", fileID, err)
		}
		delete(fm.openFiles, fileID)
	}

	return nil
}
