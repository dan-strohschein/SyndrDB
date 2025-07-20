package buffer

import (
	"sync"

	"go.uber.org/zap"
)

const (
	// FSM_BITS_PER_PAGE is the number of bits used to represent free space in a page
	FSM_BITS_PER_PAGE = 4

	// FSM_MAX_VALUE is the maximum value that can be stored in FSM_BITS_PER_PAGE bits
	FSM_MAX_VALUE = (1 << FSM_BITS_PER_PAGE) - 1
)

// FreeSpaceCategory converts bytes to a category from 0-15
// 0 means no space, 15 means more than 3/4 of page is free
func FreeSpaceCategory(freeBytes, pageSize int) uint8 {
	// Convert free space to a percentage
	if freeBytes <= 0 {
		return 0
	}

	// Map to values 0-15 based on percentage of free space
	// This is a simplified version of PostgreSQL's FSM categories
	percentage := float64(freeBytes) / float64(pageSize)

	switch {
	case percentage >= 0.75:
		return 15
	case percentage >= 0.5:
		return 10
	case percentage >= 0.25:
		return 5
	case percentage > 0:
		return 1
	default:
		return 0
	}
}

// FreeSpaceMap tracks free space in pages for a relation
type FreeSpaceMap struct {
	mu         sync.RWMutex
	fileIDMap  map[uint32]map[uint32]uint8 // FileID -> PageNum -> FreeSpaceCategory
	pageSize   int
	bufferPool *BufferPool
	logger     *zap.SugaredLogger
}

// NewFreeSpaceMap creates a new free space map
func NewFreeSpaceMap(pageSize int, bufferPool *BufferPool, logger *zap.SugaredLogger) *FreeSpaceMap {
	return &FreeSpaceMap{
		fileIDMap:  make(map[uint32]map[uint32]uint8),
		pageSize:   pageSize,
		bufferPool: bufferPool,
		logger:     logger,
	}
}

// UpdateFreeSpace updates the free space information for a page
func (fsm *FreeSpaceMap) UpdateFreeSpace(fileID, pageNum uint32, freeBytes int) {
	fsm.mu.Lock()
	defer fsm.mu.Unlock()

	// Ensure the file map exists
	fileMap, exists := fsm.fileIDMap[fileID]
	if !exists {
		fileMap = make(map[uint32]uint8)
		fsm.fileIDMap[fileID] = fileMap
	}

	// Update the free space category
	category := FreeSpaceCategory(freeBytes, fsm.pageSize)
	fileMap[pageNum] = category
}

// FindPageWithSpace finds a page with enough free space
func (fsm *FreeSpaceMap) FindPageWithSpace(fileID uint32, requiredBytes int) (uint32, bool) {
	fsm.mu.RLock()
	defer fsm.mu.RUnlock()

	// Get the file map
	fileMap, exists := fsm.fileIDMap[fileID]
	if !exists {
		return 0, false
	}

	// Calculate the required category
	requiredCategory := FreeSpaceCategory(requiredBytes, fsm.pageSize)

	// Find the first page with enough space
	for pageNum, category := range fileMap {
		if category >= requiredCategory {
			return pageNum, true
		}
	}

	return 0, false
}

// RegisterNewPage registers a new page with maximum free space
func (fsm *FreeSpaceMap) RegisterNewPage(fileID, pageNum uint32) {
	fsm.mu.Lock()
	defer fsm.mu.Unlock()

	// Ensure the file map exists
	fileMap, exists := fsm.fileIDMap[fileID]
	if !exists {
		fileMap = make(map[uint32]uint8)
		fsm.fileIDMap[fileID] = fileMap
	}

	// Register with maximum free space
	fileMap[pageNum] = FSM_MAX_VALUE
}

// RemovePage removes a page from the free space map
func (fsm *FreeSpaceMap) RemovePage(fileID, pageNum uint32) {
	fsm.mu.Lock()
	defer fsm.mu.Unlock()

	if fileMap, exists := fsm.fileIDMap[fileID]; exists {
		delete(fileMap, pageNum)
	}
}

// UpdateFromBuffer updates free space info by reading the page header
func (fsm *FreeSpaceMap) UpdateFromBuffer(buffer *DBPageBuffer) error {
	// Read the header from the buffer
	header := ReadPageHeader(buffer.Data)

	// Calculate free space
	freeSpace := GetFreeSpace(header)

	// Update the map
	fsm.UpdateFreeSpace(buffer.Tag.FileID, buffer.Tag.BlockNumber, int(freeSpace))

	return nil
}

// ScanRelation scans all pages in a relation to rebuild free space info
func (fsm *FreeSpaceMap) ScanRelation(fileID uint32, pageCount uint32) error {
	fsm.logger.Debugf("Scanning relation file %d for free space (%d pages)", fileID, pageCount)

	// Clear existing entries
	fsm.mu.Lock()
	fsm.fileIDMap[fileID] = make(map[uint32]uint8)
	fsm.mu.Unlock()

	// Scan each page
	for i := uint32(0); i < pageCount; i++ {
		buffer, err := fsm.bufferPool.GetPage(fileID, i)
		if err != nil {
			fsm.logger.Warnf("Error reading page %d of file %d: %v", i, fileID, err)
			continue
		}

		// Update free space info
		fsm.UpdateFromBuffer(buffer)

		// Release the buffer
		fsm.bufferPool.ReleaseBuffer(buffer)
	}

	return nil
}
