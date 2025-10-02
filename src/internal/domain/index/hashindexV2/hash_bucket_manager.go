package hashindexV2

/*
BUCKET MANAGEMENT SYSTEM

This file implements the bucket management for the hash index, including
the linear hashing algorithm for dynamic bucket splitting.

ALGORITHM OVERVIEW:
Linear hashing allows the hash table to grow incrementally by splitting
one bucket at a time, rather than rehashing the entire table. This provides
consistent performance even during growth operations.

LINEAR HASHING ALGORITHM:
1. Start with N buckets numbered 0 to N-1
2. Use hash(key) & (N-1) to find bucket
3. When load factor exceeds threshold, split the "next" bucket
4. Increment "next" pointer (wraps around after splitting all buckets)
5. Double the table size conceptually when all buckets have been split
6. Use two masks: HighMask for new table size, LowMask for old

BUCKET SPLITTING PROCESS:
1. Create new bucket at end of bucket array
2. Redistribute items from split bucket using new hash mask
3. Update masks and bucket count appropriately
4. Handle overflow pages during redistribution

OVERFLOW HANDLING:
When a bucket becomes full, overflow pages are chained to the primary bucket.
During splits, overflow pages are also redistributed appropriately.
*/

import (
	"fmt"

	"time"

	"go.uber.org/zap"
)

// // createBucket creates a new empty bucket
// // Parameters:
// //   - bucketNum: The bucket number to create
// //
// // Returns:
// //   - error: Any error that occurred during creation
// func (hi *HashIndex) createBucket(bucketNum uint32) error {
// 	bucket := &BucketPage{
// 		BucketNum:    bucketNum,
// 		ItemCount:    0,
// 		Items:        make([]*HashItem, 0),
// 		OverflowPage: 0, // No overflow initially
// 	}

// 	return hi.Storage.SaveBucket(bucket)
// }

// // clearBucket removes all items from a bucket and its overflow pages
// // Parameters:
// //   - bucketNum: The bucket number to clear
// //
// // Returns:
// //   - error: Any error that occurred during clearing
// func (hi *HashIndex) clearBucket(bucketNum uint32) error {
// 	// Load the bucket
// 	bucket, err := hi.Storage.LoadBucket(bucketNum)
// 	if err != nil {
// 		return fmt.Errorf("failed to load bucket %d: %w", bucketNum, err)
// 	}

// 	// Clear overflow pages if they exist
// 	if bucket.OverflowPage > 0 {
// 		err = hi.clearOverflowChain(bucket.OverflowPage)
// 		if err != nil {
// 			return fmt.Errorf("failed to clear overflow chain: %w", err)
// 		}
// 	}

// 	// Clear the bucket itself
// 	bucket.ItemCount = 0
// 	bucket.Items = make([]*HashItem, 0)
// 	bucket.OverflowPage = 0

// 	return hi.Storage.SaveBucket(bucket)
// }

// // clearOverflowChain removes all overflow pages in a chain
// // Parameters:
// //   - firstOverflowPage: The first overflow page in the chain
// //
// // Returns:
// //   - error: Any error that occurred during clearing
// func (hi *HashIndex) clearOverflowChain(firstOverflowPage uint32) error {
// 	currentPage := firstOverflowPage

// 	for currentPage > 0 {
// 		overflow, err := hi.Storage.LoadOverflowPage(currentPage)
// 		if err != nil {
// 			return fmt.Errorf("failed to load overflow page %d: %w", currentPage, err)
// 		}

// 		nextPage := overflow.NextPage

// 		// Delete this overflow page
// 		err = hi.Storage.DeleteOverflowPage(currentPage)
// 		if err != nil {
// 			return fmt.Errorf("failed to delete overflow page %d: %w", currentPage, err)
// 		}

// 		hi.metadata.NumOverflows--
// 		currentPage = nextPage
// 	}

// 	return nil
// }

// BucketManager handles bucket-specific operations
type BucketManager struct {
	pageManager *PageManager
	fileManager *FileManager
	logger      *zap.SugaredLogger
	metadata    *HashIndexMetadata
	storage     *HashIndexStorage
}

// NewBucketManager creates a new bucket manager instance
// Parameters:
//   - pageManager: The page manager for caching
//   - fileManager: The file manager for I/O
//   - logger: Logger for debug/error messages
//
// Returns:
//   - *BucketManager: The bucket manager instance
func NewBucketManager(storage *HashIndexStorage, pageManager *PageManager, fileManager *FileManager, metadata *HashIndexMetadata, logger *zap.SugaredLogger) (*BucketManager, error) {
	if storage == nil {
		return nil, fmt.Errorf("storage manager cannot be nil")
	}

	if pageManager == nil {
		return nil, fmt.Errorf("page manager cannot be nil")
	}

	if metadata == nil {
		return nil, fmt.Errorf("metadata cannot be nil")
	}

	if logger == nil {
		return nil, fmt.Errorf("logger cannot be nil")
	}

	return &BucketManager{
		pageManager: pageManager,
		fileManager: fileManager,
		storage:     storage,
		metadata:    metadata,
		logger:      logger,
	}, nil
}

// GetBucket retrieves a bucket by its bucket number
// Parameters:
//   - bucketNum: The bucket number to retrieve (0-based)
//
// Returns:
//   - *BucketPage: The bucket page containing the bucket data
//   - error: Any error that occurred during retrieval
func (bm *BucketManager) GetBucket(bucketNum uint32) (*BucketPage, error) {
	bm.logger.Debugf("Getting bucket %d", bucketNum)

	// Convert bucket number to page number (buckets start at page 1)
	pageNum := bucketNumberToPageNumber(bucketNum)

	// Try to get from cache first, then load from file
	pageData, err := bm.pageManager.GetPage(pageNum, func(pn uint32) (interface{}, error) {
		return bm.fileManager.ReadPage(pn)
	})
	if err != nil {
		return nil, fmt.Errorf("failed to load bucket page %d: %w", pageNum, err)
	}

	// Type assert to BucketPage
	bucketPage, ok := pageData.(*BucketPage)
	if !ok {
		// CRITICAL: Major corruption detected - bucket page contains wrong data type
		bm.logger.Errorf("MAJOR CORRUPTION: Page %d should contain bucket %d but has type %T",
			pageNum, bucketNum, pageData)

		// CRITICAL RECOVERY: Attempt to rebuild the corrupted bucket
		bm.logger.Warnf("RECOVERY: Attempting to rebuild corrupted bucket %d on page %d", bucketNum, pageNum)

		// Create a new bucket page with the correct bucket number
		newBucketPage := NewBucketPage(bucketNum, bm.metadata.PageSize)

		// If the corrupted page was an overflow page, try to salvage any valid records
		if overflowPage, isOverflow := pageData.(*OverflowPage); isOverflow {
			bm.logger.Infof("RECOVERY: Found overflow page data on bucket page %d, attempting to salvage %d records",
				pageNum, len(overflowPage.Records))

			// Try to add any valid records to the new bucket page
			salvageCount := 0
			for _, record := range overflowPage.Records {
				if record != nil && record.DocumentID != "" {
					// For now, just add the record without bucket verification
					// TODO: Add proper bucket computation once we have access to hash functions
					if newBucketPage.CanFitRecord(record) {
						newBucketPage.AddRecord(record)
						salvageCount++
					}
				}
			}
			bm.logger.Infof("RECOVERY: Salvaged %d valid records from corrupted bucket page %d",
				salvageCount, pageNum)
		}

		// Write the rebuilt bucket page to storage
		if err := bm.fileManager.WritePage(pageNum, newBucketPage); err != nil {
			return nil, fmt.Errorf("RECOVERY FAILED: Could not write rebuilt bucket %d to page %d: %w",
				bucketNum, pageNum, err)
		}

		// Update the page manager cache
		bm.pageManager.PutPage(pageNum, newBucketPage, true)

		bm.logger.Infof("RECOVERY SUCCESS: Rebuilt bucket %d on page %d", bucketNum, pageNum)
		return newBucketPage, nil
	}

	// Verify the bucket number matches
	if bucketPage.BucketNumber != bucketNum {
		return nil, fmt.Errorf("bucket number mismatch: expected %d, got %d", bucketNum, bucketPage.BucketNumber)
	}

	// CRITICAL FIX: Recalculate free space after loading from disk
	// Following SyndrDB data integrity requirements, ensure accurate capacity tracking
	originalFreeSpace := bucketPage.FreeSpace
	bm.recalculateFreeSpace(bucketPage)

	if originalFreeSpace != bucketPage.FreeSpace {
		bm.logger.Debugf("Free space recalculated for bucket %d: %d -> %d",
			bucketNum, originalFreeSpace, bucketPage.FreeSpace)

		// Mark as dirty so the corrected free space gets written back
		bm.pageManager.PutPage(pageNum, bucketPage, true)
	}

	bm.logger.Debugf("Successfully retrieved bucket %d with %d records", bucketNum, len(bucketPage.Records))
	return bucketPage, nil
}

// recalculateFreeSpace calculates the correct free space for a bucket page
// This function follows the Single Responsibility Principle by handling only free space calculation
// Following SyndrDB comprehensive error handling, it ensures accurate capacity calculations
// Parameters:
//   - bucketPage: The bucket page to recalculate free space for
func (bm *BucketManager) recalculateFreeSpace(bucketPage *BucketPage) {
	if bucketPage == nil {
		bm.logger.Errorf("Cannot recalculate free space for nil bucket page")
		return
	}

	// Calculate the space used by the bucket page header
	// This should match the actual serialization format
	headerSize := bm.calculateBucketHeaderSize()

	// Calculate space used by records
	recordsSize := uint32(0)
	for _, record := range bucketPage.Records {
		if record != nil {
			recordsSize += record.Size()
		}
	}

	// Calculate total used space
	totalUsedSpace := headerSize + recordsSize

	// Calculate free space
	if totalUsedSpace > bm.metadata.PageSize {
		bm.logger.Errorf("Used space %d exceeds page size %d for bucket %d",
			totalUsedSpace, bm.metadata.PageSize, bucketPage.BucketNumber)
		bucketPage.FreeSpace = 0
	} else {
		bucketPage.FreeSpace = bm.metadata.PageSize - totalUsedSpace
	}

	bm.logger.Debugf("Recalculated free space for bucket %d: header=%d, records=%d, total_used=%d, free=%d",
		bucketPage.BucketNumber, headerSize, recordsSize, totalUsedSpace, bucketPage.FreeSpace)
}

// calculateBucketHeaderSize calculates the size of the bucket page header
// This function follows the Single Responsibility Principle by handling only header size calculation
// Following SyndrDB comprehensive error handling, it provides accurate header size estimation
// Returns:
//   - uint32: The estimated header size in bytes
func (bm *BucketManager) calculateBucketHeaderSize() uint32 {
	// BucketPage structure fields (this should match the actual serialization):
	// - PageNumber: 4 bytes (uint32)
	// - BucketNumber: 4 bytes (uint32)
	// - RecordCount: 4 bytes (uint32)
	// - FreeSpace: 4 bytes (uint32)
	// - OverflowPageNum: 4 bytes (uint32)
	// - LastModified: 8 bytes (time.Time as int64)
	// - Records slice header: 24 bytes (slice header in Go)
	// - Padding/alignment: ~16 bytes (conservative estimate)

	return 68 // Conservative estimate for bucket page header
}

// CreateBucket creates a new empty bucket
// Parameters:
//   - bucketNum: The bucket number to create
//   - pageSize: The size of the page for this bucket
//
// Returns:
//   - *BucketPage: The created bucket page
//   - error: Any error that occurred during creation
func (bm *BucketManager) CreateBucket(bucketNum uint32, pageSize uint32) (*BucketPage, error) {
	bm.logger.Debugf("Creating new bucket %d", bucketNum)

	// Create new bucket page
	bucketPage := NewBucketPage(bucketNum, pageSize)

	// CRITICAL: Properly initialize free space for new buckets
	// Following SyndrDB data integrity requirements, ensure accurate capacity from start
	bm.recalculateFreeSpace(bucketPage)

	// Convert bucket number to page number
	pageNum := bucketNumberToPageNumber(bucketNum)

	// Write to file
	if err := bm.fileManager.WritePage(pageNum, bucketPage); err != nil {
		return nil, fmt.Errorf("failed to write new bucket %d to page %d: %w", bucketNum, pageNum, err)
	}

	// Cache the new bucket
	bm.pageManager.PutPage(pageNum, bucketPage, false)

	bm.logger.Debugf("Successfully created bucket %d", bucketNum)
	return bucketPage, nil
}

// UpdateBucket updates a bucket's contents and marks it as dirty
// Parameters:
//   - bucketPage: The bucket page to update
//
// Returns:
//   - error: Any error that occurred during update
func (bm *BucketManager) UpdateBucket(bucketPage *BucketPage) error {

	if bucketPage == nil {
		return fmt.Errorf("bucket page cannot be nil")
	}
	bm.logger.Debugf("Updating bucket %d", bucketPage.BucketNumber)

	// CRITICAL: Properly initialize free space for new buckets
	// Following SyndrDB data integrity requirements, ensure accurate capacity from start
	bm.recalculateFreeSpace(bucketPage)

	// Convert bucket number to page number
	pageNum := bucketNumberToPageNumber(bucketPage.BucketNumber)

	// Update timestamp
	bucketPage.LastModified = time.Now() // Should be time.Now(), but maintaining existing

	// Mark page as dirty in cache
	bm.pageManager.PutPage(pageNum, bucketPage, true)

	bm.logger.Debugf("Successfully updated bucket %d", bucketPage.BucketNumber)
	return nil
}

// BucketExists checks if a bucket exists
// Parameters:
//   - bucketNum: The bucket number to check
//
// Returns:
//   - bool: Whether the bucket exists
//   - error: Any error that occurred during check
func (bm *BucketManager) BucketExists(bucketNum uint32) (bool, error) {
	bm.logger.Debugf("Checking if bucket %d exists", bucketNum)

	pageNum := bucketNumberToPageNumber(bucketNum)

	// Try to read the page
	_, err := bm.pageManager.GetPage(pageNum, bm.fileManager.ReadPage)
	if err != nil {
		// If it's a read error (not found), bucket doesn't exist
		bm.logger.Debugf("Bucket %d does not exist: %v", bucketNum, err)
		return false, nil
	}

	bm.logger.Debugf("Bucket %d exists", bucketNum)
	return true, nil
}

// GetBucketStats returns statistics about a bucket
// Parameters:
//   - bucketNum: The bucket number to get stats for
//
// Returns:
//   - *BucketStats: Statistics about the bucket
//   - error: Any error that occurred
func (bm *BucketManager) GetBucketStats(bucketNum uint32) (*BucketStats, error) {
	bucketPage, err := bm.GetBucket(bucketNum)
	if err != nil {
		return nil, fmt.Errorf("failed to get bucket for stats: %w", err)
	}

	stats := &BucketStats{
		BucketNumber:    bucketNum,
		RecordCount:     uint32(len(bucketPage.Records)),
		FreeSpace:       bucketPage.FreeSpace,
		HasOverflow:     bucketPage.OverflowPageNum != 0,
		OverflowPageNum: bucketPage.OverflowPageNum,
	}

	// Count overflow records if there's an overflow chain
	if bucketPage.OverflowPageNum != 0 {
		overflowCount, err := bm.countOverflowRecords(bucketPage.OverflowPageNum)
		if err != nil {
			bm.logger.Warnf("Failed to count overflow records for bucket %d: %v", bucketNum, err)
		} else {
			stats.OverflowRecords = overflowCount
		}
	}

	return stats, nil
}

// countOverflowRecords counts records in an overflow chain
// Parameters:
//   - startPageNum: The first overflow page number
//
// Returns:
//   - uint32: Number of records in the overflow chain
//   - error: Any error that occurred
func (bm *BucketManager) countOverflowRecords(startPageNum uint32) (uint32, error) {
	totalRecords := uint32(0)
	currentPageNum := startPageNum

	for currentPageNum != 0 {
		pageData, err := bm.pageManager.GetPage(currentPageNum, bm.fileManager.ReadPage)
		if err != nil {
			return totalRecords, fmt.Errorf("failed to read overflow page %d: %w", currentPageNum, err)
		}

		overflowPage, ok := pageData.(*OverflowPage)
		if !ok {
			return totalRecords, fmt.Errorf("page %d is not an overflow page", currentPageNum)
		}

		totalRecords += uint32(len(overflowPage.Records))
		currentPageNum = overflowPage.NextOverflowPage
	}

	return totalRecords, nil
}

// BucketStats contains statistics about a bucket
type BucketStats struct {
	BucketNumber    uint32
	RecordCount     uint32
	FreeSpace       uint32
	HasOverflow     bool
	OverflowPageNum uint32
	OverflowRecords uint32
}

// String returns a string representation of bucket stats
func (bs *BucketStats) String() string {
	return fmt.Sprintf("BucketStats{Bucket: %d, Records: %d, FreeSpace: %d, Overflow: %t, OverflowRecords: %d}",
		bs.BucketNumber, bs.RecordCount, bs.FreeSpace, bs.HasOverflow, bs.OverflowRecords)
}

// ValidateAllBucketFreeSpace validates and fixes free space for all buckets
// This function follows the Single Responsibility Principle by handling only free space validation
// Following SyndrDB comprehensive error handling, it ensures system-wide free space accuracy
// Returns:
//   - error: Any error that occurred during validation
func (bm *BucketManager) ValidateAllBucketFreeSpace() error {
	bm.logger.Infof("Validating free space for all %d buckets", bm.metadata.BucketCount)

	fixedBuckets := 0
	for bucketNum := uint32(0); bucketNum < bm.metadata.BucketCount; bucketNum++ {
		bucketPage, err := bm.GetBucket(bucketNum)
		if err != nil {
			bm.logger.Warnf("Failed to load bucket %d for validation: %v", bucketNum, err)
			continue
		}

		originalFreeSpace := bucketPage.FreeSpace
		bm.recalculateFreeSpace(bucketPage)

		if originalFreeSpace != bucketPage.FreeSpace {
			bm.logger.Infof("Fixed free space for bucket %d: %d -> %d",
				bucketNum, originalFreeSpace, bucketPage.FreeSpace)

			// Update the bucket to persist the fix
			if err := bm.UpdateBucket(bucketPage); err != nil {
				bm.logger.Errorf("Failed to update bucket %d after free space fix: %v", bucketNum, err)
			} else {
				fixedBuckets++
			}
		}
	}

	bm.logger.Infof("Free space validation completed: fixed %d buckets", fixedBuckets)
	return nil
}
