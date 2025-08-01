/*
HASH INDEX INITIALIZATION AND MAINTENANCE

This file contains functions for initializing new hash indexes and performing
maintenance operations like compaction, repair, and optimization.

ALGORITHM OVERVIEW:
Initialization creates the basic structure of a hash index including metadata
page and initial bucket pages. Maintenance operations ensure the index remains
efficient and consistent over time.

INITIALIZATION PROCESS:
1. Create metadata page with index configuration
2. Create initial bucket pages (empty)
3. Write all pages to storage
4. Verify index integrity

MAINTENANCE OPERATIONS:
- Compaction: Removes fragmentation and reclaims space
- Repair: Fixes corruption and inconsistencies
- Optimization: Adjusts parameters for better performance
- Verification: Checks index integrity and consistency
*/

package hashindexV2

import (
	"fmt"
	"time"
)

// initializeIndex creates the initial structure of a new hash index
// Returns:
//   - error: Any error that occurred during initialization
func (hi *HashIndex) initializeIndex() error {
	hi.logger.Debugf("Initializing hash index with %d buckets", hi.metadata.BucketCount)

	// Write metadata to page 0
	if err := hi.Storage.SaveMetadata(hi.metadata); err != nil {
		return fmt.Errorf("failed to save initial metadata: %w", err)
	}

	// Create initial bucket pages
	for i := uint32(0); i < hi.metadata.BucketCount; i++ {
		bucketPage := NewBucketPage(i, hi.metadata.PageSize)

		pageNum := bucketNumberToPageNumber(i)
		if err := hi.fileManager.WritePage(pageNum, bucketPage); err != nil {
			return fmt.Errorf("failed to write initial bucket page %d: %w", i, err)
		}

		// Cache the empty bucket
		hi.pageManager.PutPage(pageNum, bucketPage, false)
	}

	hi.logger.Debugf("Successfully initialized hash index")
	return nil
}

// loadBucketPage loads a bucket page from storage or cache
// Parameters:
//   - bucketNum: The bucket number to load
//
// Returns:
//   - *BucketPage: The loaded bucket page
//   - error: Any error that occurred during loading
func (hi *HashIndex) loadBucketPage(bucketNum uint32) (*BucketPage, error) {
	return hi.bucketManager.GetBucket(bucketNum)
}

// documentExistsInBucket checks if a document exists in a bucket (including overflow chain)
// Parameters:
//   - bucketPage: The bucket page to search
//   - documentID: The document ID to search for
//
// Returns:
//   - bool: Whether the document exists
func (hi *HashIndex) documentExistsInBucket(bucketPage *BucketPage, documentID string) bool {
	return hi.searchInBucketChain(bucketPage, documentID)
}

// searchInBucketChain searches for a document in a bucket and its overflow chain
// Parameters:
//   - bucketPage: The bucket page to start searching from
//   - documentID: The document ID to search for
//
// Returns:
//   - bool: Whether the document was found
func (hi *HashIndex) searchInBucketChain(bucketPage *BucketPage, documentID string) bool {
	// Search in main bucket page
	for _, record := range bucketPage.Records {
		if record.DocumentID == documentID {
			return true
		}
	}

	// Search in overflow chain
	currentOverflowPageNum := bucketPage.OverflowPageNum
	for currentOverflowPageNum != 0 {
		overflowPageData, err := hi.pageManager.GetPage(currentOverflowPageNum, hi.fileManager.ReadPage)
		if err != nil {
			hi.logger.Errorf("Failed to read overflow page %d: %v", currentOverflowPageNum, err)
			return false
		}

		overflowPage := overflowPageData.(*OverflowPage)
		for _, record := range overflowPage.Records {
			if record.DocumentID == documentID {
				return true
			}
		}

		currentOverflowPageNum = overflowPage.NextPageNum
	}

	return false
}

// addToOverflow adds a record to the overflow chain of a bucket
// Parameters:
//   - bucketPage: The bucket page
//   - bucketNum: The bucket number
//   - record: The record to add
//
// Returns:
//   - error: Any error that occurred
func (hi *HashIndex) addToOverflow(bucketPage *BucketPage, bucketNum uint32, record *IndexRecord) error {
	if bucketPage.OverflowPageNum == 0 {
		// Create first overflow page
		overflowPageNum, err := hi.allocateNewPage()
		if err != nil {
			return fmt.Errorf("failed to allocate overflow page: %w", err)
		}

		overflowPage := NewOverflowPage(overflowPageNum, hi.metadata.PageSize)
		overflowPage.AddRecord(record)

		bucketPage.OverflowPageNum = overflowPageNum

		// Write pages
		hi.pageManager.PutPage(bucketNumberToPageNumber(bucketNum), bucketPage, true)
		hi.pageManager.PutPage(overflowPageNum, overflowPage, true)

		return nil
	}

	// Find the last overflow page with space or create a new one
	return hi.addToOverflowChain(bucketPage.OverflowPageNum, record)
}

// addToOverflowChain adds a record to an overflow chain
// Parameters:
//   - startPageNum: The first overflow page number
//   - record: The record to add
//
// Returns:
//   - error: Any error that occurred
func (hi *HashIndex) addToOverflowChain(startPageNum uint32, record *IndexRecord) error {
	currentPageNum := startPageNum

	for {
		overflowPageData, err := hi.pageManager.GetPage(currentPageNum, hi.fileManager.ReadPage)
		if err != nil {
			return fmt.Errorf("failed to read overflow page %d: %w", currentPageNum, err)
		}

		overflowPage := overflowPageData.(*OverflowPage)

		// Try to add to current page
		if overflowPage.CanFitRecord(record) {
			overflowPage.AddRecord(record)
			hi.pageManager.PutPage(currentPageNum, overflowPage, true)
			return nil
		}

		// Move to next page or create new one
		if overflowPage.NextPageNum == 0 {
			// Create new overflow page
			newPageNum, err := hi.allocateNewPage()
			if err != nil {
				return fmt.Errorf("failed to allocate new overflow page: %w", err)
			}

			newOverflowPage := NewOverflowPage(newPageNum, hi.metadata.PageSize)
			newOverflowPage.AddRecord(record)

			overflowPage.NextPageNum = newPageNum

			// Write both pages
			hi.pageManager.PutPage(currentPageNum, overflowPage, true)
			hi.pageManager.PutPage(newPageNum, newOverflowPage, true)

			return nil
		}

		currentPageNum = overflowPage.NextPageNum
	}
}

// removeFromBucketChain removes a document from a bucket chain
// Parameters:
//   - bucketPage: The bucket page to start from
//   - bucketNum: The bucket number
//   - documentID: The document ID to remove
//
// Returns:
//   - bool: Whether the document was found and removed
//   - error: Any error that occurred
func (hi *HashIndex) removeFromBucketChain(bucketPage *BucketPage, bucketNum uint32, documentID string) (bool, error) {
	// Try to remove from main bucket page
	for i, record := range bucketPage.Records {
		if record.DocumentID == documentID {
			// Remove record
			bucketPage.Records = append(bucketPage.Records[:i], bucketPage.Records[i+1:]...)
			bucketPage.RecordCount--
			hi.pageManager.PutPage(bucketNumberToPageNumber(bucketNum), bucketPage, true)
			return true, nil
		}
	}

	// Search and remove from overflow chain
	if bucketPage.OverflowPageNum != 0 {
		return hi.removeFromOverflowChain(bucketPage.OverflowPageNum, documentID)
	}

	return false, nil
}

// removeFromOverflowChain removes a document from an overflow chain
// Parameters:
//   - startPageNum: The first overflow page number
//   - documentID: The document ID to remove
//
// Returns:
//   - bool: Whether the document was found and removed
//   - error: Any error that occurred
func (hi *HashIndex) removeFromOverflowChain(startPageNum uint32, documentID string) (bool, error) {
	currentPageNum := startPageNum

	for currentPageNum != 0 {
		overflowPageData, err := hi.pageManager.GetPage(currentPageNum, hi.fileManager.ReadPage)
		if err != nil {
			return false, fmt.Errorf("failed to read overflow page %d: %w", currentPageNum, err)
		}

		overflowPage := overflowPageData.(*OverflowPage)

		// Try to remove from current page
		for i, record := range overflowPage.Records {
			if record.DocumentID == documentID {
				// Remove record
				overflowPage.Records = append(overflowPage.Records[:i], overflowPage.Records[i+1:]...)
				overflowPage.RecordCount--
				hi.pageManager.PutPage(currentPageNum, overflowPage, true)
				return true, nil
			}
		}

		currentPageNum = overflowPage.NextPageNum
	}

	return false, nil
}

// allocateNewPage allocates a new page number for overflow pages
// Returns:
//   - uint32: The new page number
//   - error: Any error that occurred
func (hi *HashIndex) allocateNewPage() (uint32, error) {
	// Simple allocation strategy: use next available page number
	// In a production system, this would track free pages

	hi.metadata.NextPageNum++
	return hi.metadata.NextPageNum, nil
}

// VerifyIndex performs integrity checks on the hash index
// Returns:
//   - *VerificationResult: Results of the verification
//   - error: Any error that occurred during verification
func (hi *HashIndex) VerifyIndex() (*VerificationResult, error) {
	hi.mutex.RLock()
	defer hi.mutex.RUnlock()

	if !hi.isOpen {
		return nil, fmt.Errorf("hash index is closed")
	}

	result := &VerificationResult{
		StartTime:    time.Now(),
		BucketCount:  hi.metadata.BucketCount,
		TotalRecords: 0,
		Errors:       make([]string, 0),
		Warnings:     make([]string, 0),
	}

	hi.logger.Infof("Starting index verification for bundle '%s'", hi.bundleName)

	// Verify each bucket
	for bucketNum := uint32(0); bucketNum < hi.metadata.BucketCount; bucketNum++ {
		bucketPage, err := hi.loadBucketPage(bucketNum)
		if err != nil {
			result.Errors = append(result.Errors, fmt.Sprintf("Failed to load bucket %d: %v", bucketNum, err))
			continue
		}

		// Count records in bucket
		bucketRecords := len(bucketPage.Records)
		result.TotalRecords += uint64(bucketRecords)

		// Verify each record
		for _, record := range bucketPage.Records {
			if err := hi.verifyRecord(record, bucketNum); err != nil {
				result.Errors = append(result.Errors, fmt.Sprintf("Invalid record in bucket %d: %v", bucketNum, err))
			}
		}

		// Verify overflow chain
		if bucketPage.OverflowPageNum != 0 {
			overflowRecords, err := hi.verifyOverflowChain(bucketPage.OverflowPageNum, bucketNum)
			if err != nil {
				result.Errors = append(result.Errors, fmt.Sprintf("Overflow chain error in bucket %d: %v", bucketNum, err))
			} else {
				result.TotalRecords += uint64(overflowRecords)
			}
		}
	}

	// Check if record count matches metadata
	if result.TotalRecords != hi.metadata.TotalRecords {
		result.Errors = append(result.Errors,
			fmt.Sprintf("Record count mismatch: found %d, metadata says %d",
				result.TotalRecords, hi.metadata.TotalRecords))
	}

	result.EndTime = time.Now()
	result.Duration = result.EndTime.Sub(result.StartTime)
	result.IsValid = len(result.Errors) == 0

	hi.logger.Infof("Index verification completed in %v. Valid: %t, Errors: %d, Warnings: %d",
		result.Duration, result.IsValid, len(result.Errors), len(result.Warnings))

	return result, nil
}

// verifyRecord verifies that a record is valid and in the correct bucket
// Parameters:
//   - record: The record to verify
//   - expectedBucket: The bucket number where the record was found
//
// Returns:
//   - error: Validation error if any
func (hi *HashIndex) verifyRecord(record *IndexRecord, expectedBucket uint32) error {
	if err := validateDocumentID(record.DocumentID); err != nil {
		return fmt.Errorf("invalid document ID: %w", err)
	}

	// Verify record is in correct bucket
	calculatedBucket := hi.calculateBucket(record.HashValue)
	if calculatedBucket != expectedBucket {
		return fmt.Errorf("record in wrong bucket: expected %d, found in %d",
			calculatedBucket, expectedBucket)
	}

	// Verify hash value is correct
	expectedHash := calculateHash([]byte(record.DocumentID), hi.metadata.HashSeed)
	if record.HashValue != expectedHash {
		return fmt.Errorf("incorrect hash value: expected %d, found %d",
			expectedHash, record.HashValue)
	}

	return nil
}

// verifyOverflowChain verifies an overflow chain and counts records
// Parameters:
//   - startPageNum: The first overflow page number
//   - bucketNum: The bucket number this chain belongs to
//
// Returns:
//   - int: Number of records found in the chain
//   - error: Any error that occurred
func (hi *HashIndex) verifyOverflowChain(startPageNum uint32, bucketNum uint32) (int, error) {
	recordCount := 0
	currentPageNum := startPageNum
	visitedPages := make(map[uint32]bool)

	for currentPageNum != 0 {
		// Check for cycles
		if visitedPages[currentPageNum] {
			return recordCount, fmt.Errorf("cycle detected in overflow chain at page %d", currentPageNum)
		}
		visitedPages[currentPageNum] = true

		overflowPageData, err := hi.pageManager.GetPage(currentPageNum, hi.fileManager.ReadPage)
		if err != nil {
			return recordCount, fmt.Errorf("failed to read overflow page %d: %w", currentPageNum, err)
		}

		overflowPage := overflowPageData.(*OverflowPage)
		recordCount += len(overflowPage.Records)

		// Verify each record
		for _, record := range overflowPage.Records {
			if err := hi.verifyRecord(record, bucketNum); err != nil {
				return recordCount, fmt.Errorf("invalid record in overflow page %d: %w", currentPageNum, err)
			}
		}

		currentPageNum = overflowPage.NextPageNum
	}

	return recordCount, nil
}

// VerificationResult contains the results of index verification
type VerificationResult struct {
	StartTime    time.Time
	EndTime      time.Time
	Duration     time.Duration
	IsValid      bool
	BucketCount  uint32
	TotalRecords uint64
	Errors       []string
	Warnings     []string
}
