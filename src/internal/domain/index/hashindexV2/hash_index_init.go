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

		currentOverflowPageNum = overflowPage.NextOverflowPage
	}

	return false
}

// addToOverflow adds a record to the overflow chain of a bucket with enhanced validation
// This function follows the Single Responsibility Principle by handling only bucket overflow operations
// Following SyndrDB comprehensive error handling, it prevents duplicate records and maintains data integrity
// Parameters:
//   - bucketPage: The bucket page
//   - bucketNum: The bucket number
//   - record: The record to add
//
// Returns:
//   - error: Any error that occurred during overflow addition
func (hi *HashIndex) addToOverflow(bucketPage *BucketPage, bucketNum uint32, record *IndexRecord) error {
	// Validate parameters following SyndrDB defensive programming practices
	if bucketPage == nil {
		return fmt.Errorf("bucket page cannot be nil")
	}

	if record == nil {
		return fmt.Errorf("record cannot be nil")
	}

	hi.logger.Debugf("Adding record to overflow for bucket %d: DocumentID=%s", bucketNum, record.DocumentID)

	// CRITICAL: Check if record already exists in the bucket before adding to overflow
	// Following SyndrDB data integrity requirements
	for _, existingRecord := range bucketPage.Records {
		if existingRecord != nil && existingRecord.DocumentID == record.DocumentID {
			return fmt.Errorf("duplicate record: DocumentID %s already exists in bucket %d", record.DocumentID, bucketNum)
		}
	}

	if bucketPage.OverflowPageNum == 0 {
		// Create first overflow page
		overflowPageNum, err := hi.allocateNewPage()
		if err != nil {
			return fmt.Errorf("failed to allocate overflow page: %w", err)
		}

		hi.logger.Debugf("Creating first overflow page %d for bucket %d", overflowPageNum, bucketNum)

		overflowPage := NewOverflowPage(overflowPageNum, hi.metadata.PageSize)
		overflowPage.ParentBucket = bucketNum
		overflowPage.AddRecord(record)

		bucketPage.OverflowPageNum = overflowPageNum

		// Write pages following SyndrDB ACID compliance
		hi.pageManager.PutPage(bucketNumberToPageNumber(bucketNum), bucketPage, true)
		hi.pageManager.PutPage(overflowPageNum, overflowPage, true)

		if err := hi.flushBucketToDisk(bucketNum, bucketPage); err != nil {
			return fmt.Errorf("failed to persist updated bucket page %d to disk: %w", bucketNum, err)
		}

		if err := hi.fileManager.WritePage(overflowPageNum, overflowPage); err != nil {
			return fmt.Errorf("failed to write overflow page %d to disk: %w", overflowPageNum, err)
		}

		if err := hi.fileManager.Sync(); err != nil {
			return fmt.Errorf("failed to sync overflow page %d to disk - data integrity cannot be guaranteed: %w", overflowPageNum, err)
		}

		hi.logger.Debugf("Successfully created first overflow page %d for bucket %d", overflowPageNum, bucketNum)
		return nil
	}

	// Add to existing overflow chain with duplicate detection
	return hi.addToOverflowChain(bucketPage.OverflowPageNum, record)
}

// addToOverflowChain adds a record to an overflow chain with duplicate detection
// This function follows the Single Responsibility Principle by handling only overflow chain insertion
// Following SyndrDB comprehensive error handling, it prevents duplicate records and maintains chain integrity
// Parameters:
//   - startPageNum: The first overflow page number
//   - record: The record to add
//
// Returns:
//   - error: Any error that occurred during insertion
func (hi *HashIndex) addToOverflowChain(startPageNum uint32, record *IndexRecord) error {
	// Validate parameters following SyndrDB defensive programming practices
	if startPageNum == 0 {
		return fmt.Errorf("invalid start page number: 0")
	}

	if record == nil {
		return fmt.Errorf("record cannot be nil")
	}

	if hi.fileManager == nil {
		return fmt.Errorf("file manager is nil, cannot persist overflow chain changes")
	}

	hi.logger.Debugf("Adding record to overflow chain starting at page %d: DocumentID=%s, HashValue=%d",
		startPageNum, record.DocumentID, record.HashValue)

	// CRITICAL: Check for duplicate records before adding
	// Following SyndrDB data integrity requirements, prevent duplicate entries
	exists, err := hi.recordExistsInOverflowChain(startPageNum, record.DocumentID)
	if err != nil {
		return fmt.Errorf("failed to check for duplicate record in overflow chain: %w", err)
	}

	if exists {
		hi.logger.Warnf("Record with DocumentID %s already exists in overflow chain starting at page %d",
			record.DocumentID, startPageNum)
		return fmt.Errorf("duplicate record: DocumentID %s already exists in overflow chain", record.DocumentID)
	}

	currentPageNum := startPageNum
	visitedPages := make(map[uint32]bool) // Prevent infinite loops

	for {
		// Check for cycles following SyndrDB comprehensive error handling
		if visitedPages[currentPageNum] {
			return fmt.Errorf("cycle detected in overflow chain at page %d", currentPageNum)
		}
		visitedPages[currentPageNum] = true

		// Load current overflow page
		overflowPageData, err := hi.pageManager.GetPage(currentPageNum, hi.fileManager.ReadPage)
		if err != nil {
			return fmt.Errorf("failed to read overflow page %d: %w", currentPageNum, err)
		}

		overflowPage, ok := overflowPageData.(*OverflowPage)
		if !ok {
			return fmt.Errorf("page %d is not an overflow page, got type %T", currentPageNum, overflowPageData)
		}

		hi.logger.Debugf("Checking overflow page %d: RecordCount=%d, CanFit=%v",
			currentPageNum, overflowPage.RecordCount, overflowPage.CanFitRecord(record))

		// Try to add to current page if it has space
		if overflowPage.CanFitRecord(record) {
			overflowPage.AddRecord(record)
			hi.pageManager.PutPage(currentPageNum, overflowPage, true)

			// CRITICAL: Persist changes immediately following ACID compliance
			if err := hi.fileManager.WritePage(currentPageNum, overflowPage); err != nil {
				return fmt.Errorf("failed to write updated overflow page %d to disk: %w", currentPageNum, err)
			}

			if err := hi.fileManager.Sync(); err != nil {
				return fmt.Errorf("failed to sync overflow page %d to disk - data integrity cannot be guaranteed: %w", currentPageNum, err)
			}

			hi.logger.Debugf("Successfully added record to existing overflow page %d", currentPageNum)
			return nil
		}

		// Current page is full, check if there's a next page
		if overflowPage.NextOverflowPage == 0 {
			// Need to create a new overflow page
			newPageNum, err := hi.allocateNewPage()
			if err != nil {
				return fmt.Errorf("failed to allocate new overflow page: %w", err)
			}

			hi.logger.Debugf("Creating new overflow page %d for record", newPageNum)

			// Create new overflow page
			newOverflowPage := NewOverflowPage(newPageNum, hi.metadata.PageSize)
			newOverflowPage.ParentBucket = overflowPage.ParentBucket
			newOverflowPage.AddRecord(record)

			// Link the new page to the chain
			overflowPage.NextOverflowPage = newPageNum

			// Write both pages following SyndrDB ACID compliance
			hi.pageManager.PutPage(currentPageNum, overflowPage, true)
			hi.pageManager.PutPage(newPageNum, newOverflowPage, true)

			if err := hi.fileManager.WritePage(currentPageNum, overflowPage); err != nil {
				return fmt.Errorf("failed to write updated overflow page %d to disk: %w", currentPageNum, err)
			}

			if err := hi.fileManager.WritePage(newPageNum, newOverflowPage); err != nil {
				return fmt.Errorf("failed to write new overflow page %d to disk: %w", newPageNum, err)
			}

			if err := hi.fileManager.Sync(); err != nil {
				return fmt.Errorf("failed to sync overflow pages to disk - data integrity cannot be guaranteed: %w", err)
			}

			hi.logger.Debugf("Successfully created and linked new overflow page %d", newPageNum)
			return nil
		}

		// Move to next page in the chain
		currentPageNum = overflowPage.NextOverflowPage
	}
}

// recordExistsInOverflowChain checks if a record already exists in an overflow chain
// This function follows the Single Responsibility Principle by handling only duplicate detection
// Following SyndrDB comprehensive error handling, it safely traverses overflow chains
// Parameters:
//   - startPageNum: The first overflow page number
//   - documentID: The document ID to search for
//
// Returns:
//   - bool: Whether the record exists in the chain
//   - error: Any error that occurred during search
func (hi *HashIndex) recordExistsInOverflowChain(startPageNum uint32, documentID string) (bool, error) {
	currentPageNum := startPageNum
	visitedPages := make(map[uint32]bool)

	hi.logger.Debugf("Checking for existing record with DocumentID %s in overflow chain starting at page %d",
		documentID, startPageNum)

	for currentPageNum != 0 {
		// Check for cycles
		if visitedPages[currentPageNum] {
			return false, fmt.Errorf("cycle detected in overflow chain at page %d while checking for duplicates", currentPageNum)
		}
		visitedPages[currentPageNum] = true

		// Load overflow page
		overflowPageData, err := hi.pageManager.GetPage(currentPageNum, hi.fileManager.ReadPage)
		if err != nil {
			return false, fmt.Errorf("failed to read overflow page %d while checking for duplicates: %w", currentPageNum, err)
		}

		overflowPage, ok := overflowPageData.(*OverflowPage)
		if !ok {
			return false, fmt.Errorf("page %d is not an overflow page while checking for duplicates", currentPageNum)
		}

		// Check records in this page
		for _, record := range overflowPage.Records {
			if record != nil && record.DocumentID == documentID {
				hi.logger.Debugf("Found existing record with DocumentID %s in overflow page %d", documentID, currentPageNum)
				return true, nil
			}
		}

		// Move to next overflow page
		currentPageNum = overflowPage.NextOverflowPage
	}

	hi.logger.Debugf("No existing record found with DocumentID %s in overflow chain", documentID)
	return false, nil
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

		currentPageNum = overflowPage.NextOverflowPage
	}

	return false, nil
}

// allocateNewPage allocates a new page number for overflow pages
// Returns:
//   - uint32: The new page number
//   - error: Any error that occurred
func (hi *HashIndex) allocateNewPage() (uint32, error) {
	hi.logger.Debugf("ENTER allocateNewPage: current NextPageNum=%d", hi.metadata.NextPageNum)

	// Validate metadata following SyndrDB defensive programming practices
	if hi.metadata == nil {
		return 0, fmt.Errorf("metadata is nil, cannot allocate page")
	}

	if hi.fileManager == nil {
		return 0, fmt.Errorf("file manager is nil, cannot persist allocation")
	}

	// CRITICAL: Validate NextPageNum is reasonable following SyndrDB data integrity requirements
	expectedMinimum := hi.metadata.BucketCount + 1 // Should be at least after all bucket pages
	if hi.metadata.NextPageNum < expectedMinimum {
		hi.metadata.NextPageNum = expectedMinimum
	}

	// Check if we have free pages to reuse (following efficient storage practices)
	if len(hi.metadata.FreePageList) > 0 {
		// Reuse a free page
		pageNum := hi.metadata.FreePageList[0]
		hi.metadata.FreePageList = hi.metadata.FreePageList[1:]
		hi.logger.Debugf("Reusing free page %d from free page list", pageNum)

		// CRITICAL: Persist the updated free page list immediately
		if err := hi.updateMetadataOnDisk(); err != nil {
			// Rollback the free page list change
			hi.metadata.FreePageList = append([]uint32{pageNum}, hi.metadata.FreePageList...)
			hi.logger.Errorf("FAILED to persist free page list update, rolled back: %v", err)
			return 0, fmt.Errorf("failed to persist free page list update: %w", err)
		}

		hi.logger.Debugf("Successfully reused and persisted free page %d", pageNum)
		return pageNum, nil
	}

	// Store the current NextPageNum before incrementing (for rollback purposes)
	originalNextPageNum := hi.metadata.NextPageNum

	// Allocate new page number - this should be consecutive and valid
	newPageNum := hi.metadata.NextPageNum
	hi.metadata.NextPageNum++

	hi.logger.Debugf("Allocated new page %d, incremented NextPageNum from %d to %d",
		newPageNum, originalNextPageNum, hi.metadata.NextPageNum)

	// CRITICAL: Immediately persist the updated NextPageNum to prevent desynchronization
	// Following SyndrDB ACID compliance requirements - metadata changes must be durable
	if err := hi.updateMetadataOnDisk(); err != nil {
		// Rollback the NextPageNum increment
		hi.metadata.NextPageNum = originalNextPageNum
		hi.logger.Errorf("FAILED to persist NextPageNum update, rolled back from %d to %d: %v",
			hi.metadata.NextPageNum+1, originalNextPageNum, err)
		return 0, fmt.Errorf("failed to persist page allocation metadata: %w", err)
	}

	hi.logger.Debugf("Successfully allocated and persisted page %d, NextPageNum now %d",
		newPageNum, hi.metadata.NextPageNum)
	return newPageNum, nil
}

// updateMetadataOnDisk immediately writes metadata changes to disk
// This function follows the Single Responsibility Principle by handling only metadata disk persistence
// Returns:
//   - error: Any error that occurred during persistence
func (hi *HashIndex) updateMetadataOnDisk() error {
	hi.logger.Debugf("ENTER updateMetadataOnDisk: NextPageNum=%d, DocumentCount=%d",
		hi.metadata.NextPageNum, hi.metadata.DocumentCount)

	// Validate components following SyndrDB defensive programming practices
	if hi.fileManager == nil {
		return fmt.Errorf("file manager is nil, cannot persist metadata")
	}

	if hi.metadata == nil {
		return fmt.Errorf("metadata is nil, cannot persist")
	}

	// Write metadata to disk (page 0)
	if err := hi.fileManager.WriteMetadata(hi.metadata); err != nil {
		return fmt.Errorf("failed to write metadata to disk: %w", err)
	}

	// CRITICAL: Sync changes to ensure durability following SyndrDB ACID compliance
	if err := hi.fileManager.Sync(); err != nil {
		return fmt.Errorf("failed to sync metadata to disk - data integrity cannot be guaranteed: %w", err)
	}

	hi.logger.Debugf("Successfully persisted metadata to disk")
	return nil
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

		currentPageNum = overflowPage.NextOverflowPage
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
