package buffer

// import (
// 	"fmt"
// 	"os"
// 	"path/filepath"
// 	"sync"
// 	"syndrdb/src/internal/domain/models"

// 	"go.uber.org/zap"
// )

// FileManager handles files on disk
// type FileManager struct {
// 	mu            sync.Mutex
// 	openFiles     map[uint32]*os.File
// 	dataDir       string
// 	bufferPool    *BufferPool
// 	fileIDCounter uint32
// 	fileIDMap     map[string]uint32 // Maps filenames to fileIDs
// 	logger        *zap.SugaredLogger

// 	freeSpaceMap *FreeSpaceMap
// 	// Table tracking info
// 	tablePageCounts map[uint32]uint32 // FileID -> Page Count

// }

// // NewFileManager creates a new file manager
// func NewFileManager(dataDir string, bufferPool *BufferPool, logger *zap.SugaredLogger) (*FileManager, error) {
// 	// Create data directory if it doesn't exist
// 	err := os.MkdirAll(dataDir, 0755)
// 	if err != nil {
// 		return nil, fmt.Errorf("could not create data directory: %w", err)
// 	}

// 	fsm := NewFreeSpaceMap(bufferPool.pageSize, bufferPool, logger)

// 	return &FileManager{
// 		openFiles:       make(map[uint32]*os.File),
// 		dataDir:         dataDir,
// 		bufferPool:      bufferPool,
// 		fileIDCounter:   1, // Start from 1, 0 is reserved
// 		fileIDMap:       make(map[string]uint32),
// 		logger:          logger,
// 		freeSpaceMap:    fsm,
// 		tablePageCounts: make(map[uint32]uint32),
// 	}, nil
// }

// // GetFileID returns the fileID for a given filename, creating it if necessary
// func (fm *FileManager) GetFileID(filename string) (uint32, error) {
// 	fm.mu.Lock()
// 	defer fm.mu.Unlock()

// 	// Check if we already have a fileID for this filename
// 	fileID, exists := fm.fileIDMap[filename]
// 	if exists {
// 		return fileID, nil
// 	}

// 	// Assign a new fileID
// 	fileID = fm.fileIDCounter
// 	fm.fileIDCounter++

// 	// Add to the map
// 	fm.fileIDMap[filename] = fileID

// 	return fileID, nil
// }

// // GetFilePath returns the full path for a file
// func (fm *FileManager) GetFilePath(filename string) string {
// 	return filepath.Join(fm.dataDir, filename)
// }

// // OpenFile opens a file and returns its fileID
// func (fm *FileManager) OpenFile(filename string) (uint32, error) {
// 	fm.mu.Lock()
// 	defer fm.mu.Unlock()

// 	// Get or create a fileID
// 	fileID, exists := fm.fileIDMap[filename]
// 	if !exists {
// 		fileID = fm.fileIDCounter
// 		fm.fileIDCounter++
// 		fm.fileIDMap[filename] = fileID
// 	}

// 	// Check if file is already open
// 	_, fileOpen := fm.openFiles[fileID]
// 	if fileOpen {
// 		return fileID, nil
// 	}

// 	// Open the file
// 	filePath := filepath.Join(fm.dataDir, filename)
// 	file, err := os.OpenFile(filePath, os.O_RDWR|os.O_CREATE, 0644)
// 	if err != nil {
// 		return 0, fmt.Errorf("could not open file %s: %w", filePath, err)
// 	}

// 	fm.openFiles[fileID] = file

// 	return fileID, nil
// }

// // ReadPage reads a page through the buffer manager
// func (fm *FileManager) ReadPage(fileID uint32, blockNum uint32) (*models.DBPageBuffer, error) {
// 	// Get the file handle
// 	fm.mu.Lock()
// 	_, exists := fm.openFiles[fileID]
// 	fm.mu.Unlock()

// 	if !exists {
// 		return nil, fmt.Errorf("no open file with ID %d", fileID)
// 	}

// 	// Get the page through the buffer pool
// 	return fm.bufferPool.GetPage(fileID, blockNum)
// }

// // ReleasePage decrements the reference count for a buffer
// func (fm *FileManager) ReleasePage(buffer *models.DBPageBuffer) {
// 	fm.bufferPool.ReleaseBuffer(buffer)
// }

// // WritePage marks a page as dirty
// func (fm *FileManager) WritePage(buffer *models.DBPageBuffer) {
// 	fm.bufferPool.MarkBufferDirty(buffer)
// }

// // Close closes all open files
// func (fm *FileManager) Close() error {
// 	fm.mu.Lock()
// 	defer fm.mu.Unlock()

// 	// Flush any dirty buffers
// 	err := fm.bufferPool.FlushAllDirty()
// 	if err != nil {
// 		return fmt.Errorf("could not flush dirty buffers: %w", err)
// 	}

// 	// Close all open files
// 	for fileID, file := range fm.openFiles {
// 		err := file.Close()
// 		if err != nil {
// 			return fmt.Errorf("could not close file ID %d: %w", fileID, err)
// 		}
// 		delete(fm.openFiles, fileID)
// 	}

// 	return nil
// }

// // AllocatePage allocates a new page in the file
// func (fm *FileManager) AllocatePage(fileID uint32) (uint32, error) {
// 	fm.mu.Lock()
// 	defer fm.mu.Unlock()

// 	// Get the current page count
// 	pageCount, exists := fm.tablePageCounts[fileID]
// 	if !exists {
// 		pageCount = 0
// 	}

// 	// Allocate the new page
// 	newPageNum := pageCount
// 	fm.tablePageCounts[fileID] = pageCount + 1

// 	// Get the buffer for the new page
// 	bufferPage, err := fm.bufferPool.GetPage(fileID, newPageNum)
// 	if err != nil {
// 		return 0, fmt.Errorf("failed to get buffer for new page: %w", err)
// 	}

// 	// Initialize the page
// 	bufferPage.InitializePage()

// 	// Mark it as dirty so it will be written
// 	fm.bufferPool.MarkBufferDirty(bufferPage)

// 	// Register in FSM
// 	fm.freeSpaceMap.RegisterNewPage(fileID, newPageNum)

// 	// Release the buffer
// 	fm.bufferPool.ReleaseBuffer(bufferPage)

// 	return newPageNum, nil
// }

// // FindPageWithSpace finds a page with enough free space
// func (fm *FileManager) FindPageWithSpace(fileID uint32, requiredBytes int) (uint32, bool, error) {
// 	// Check FSM for a page with enough space
// 	if pageNum, found := fm.freeSpaceMap.FindPageWithSpace(fileID, requiredBytes); found {
// 		return pageNum, true, nil
// 	}

// 	// If no suitable page found, allocate a new one
// 	newPage, err := fm.AllocatePage(fileID)
// 	if err != nil {
// 		return 0, false, fmt.Errorf("failed to allocate new page: %w", err)
// 	}

// 	return newPage, false, nil
// }

// // InsertData inserts data into a page with enough free space
// func (fm *FileManager) InsertData(fileID uint32, data []byte) (uint32, uint16, error) {
// 	// Find a page with enough space
// 	pageNum, _, err := fm.FindPageWithSpace(fileID, len(data))
// 	if err != nil {
// 		return 0, 0, err
// 	}

// 	// Get the buffer for this page
// 	bufferPage, err := fm.bufferPool.GetPage(fileID, pageNum)
// 	if err != nil {
// 		return 0, 0, fmt.Errorf("failed to get page buffer: %w", err)
// 	}
// 	defer fm.bufferPool.ReleaseBuffer(bufferPage)

// 	// Add the data to the page
// 	itemOffset, err := bufferPage.AddItem(data)
// 	if err != nil {
// 		return 0, 0, fmt.Errorf("failed to add item to page: %w", err)
// 	}

// 	// Update free space map
// 	fm.freeSpaceMap.UpdateFromBuffer(bufferPage)

// 	return pageNum, itemOffset, nil
// }
