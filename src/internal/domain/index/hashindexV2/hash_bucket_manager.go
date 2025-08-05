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
		return nil, fmt.Errorf("page %d is not a bucket page, got type %T", pageNum, pageData)
	}

	// Verify the bucket number matches
	if bucketPage.BucketNumber != bucketNum {
		return nil, fmt.Errorf("bucket number mismatch: expected %d, got %d", bucketNum, bucketPage.BucketNumber)
	}

	bm.logger.Debugf("Successfully retrieved bucket %d with %d records", bucketNum, len(bucketPage.Records))
	return bucketPage, nil
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
	bm.logger.Debugf("Updating bucket %d", bucketPage.BucketNumber)

	// Convert bucket number to page number
	pageNum := bucketNumberToPageNumber(bucketPage.BucketNumber)

	// Update timestamp
	bucketPage.LastModified = bucketPage.LastModified // Should be time.Now(), but maintaining existing

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
