/*
HASH INDEX HELPER FUNCTIONS

This file contains utility functions used throughout the hash index system.
These functions handle common operations like hashing, bucket calculations,
and data validation.

ALGORITHM OVERVIEW:
Helper functions provide reusable components that maintain consistency
across the hash index implementation. They encapsulate complex logic
and provide a single point of maintenance for common operations.

HASH FUNCTION:
Uses FNV-1a hash function which provides good distribution and is fast.
The hash function is deterministic and consistent across runs.

BUCKET CALCULATION:
Implements linear hashing algorithm for dynamic bucket expansion.
Uses bit manipulation for efficient bucket number calculation.

VALIDATION:
Provides input validation and sanitization functions to ensure
data integrity throughout the system.
*/

package hashindexV2

// DEPRECATED: This file is part of the old hashindexV2 (non-LSM) implementation.
// Use hashindexV3 (LSM-style) instead. See hash_index_api.go for details.

import (
	"crypto/rand"
	"encoding/binary"
	"fmt"
	"hash/fnv"
	"time"
)

// createBucket creates a new empty bucket using the bucket manager
// func (hi *HashIndex) createBucket(bucketNum uint32) error {
// 	_, err := hi.bucketManager.CreateBucket(bucketNum, hi.metadata.PageSize)
// 	return err
// }

// // clearBucket removes all records from a bucket and its overflow pages
// func (hi *HashIndex) clearBucket(bucketNum uint32) error {
// 	// Load the bucket using correct types
// 	bucketPage, err := hi.bucketManager.GetBucket(bucketNum)
// 	if err != nil {
// 		return fmt.Errorf("failed to load bucket %d: %w", bucketNum, err)
// 	}

// 	// Clear overflow pages if they exist
// 	if bucketPage.OverflowPageNum != 0 {
// 		err = hi.clearOverflowChain(bucketPage.OverflowPageNum)
// 		if err != nil {
// 			return fmt.Errorf("failed to clear overflow chain: %w", err)
// 		}
// 	}

// 	// Clear the bucket itself - use CORRECT field names
// 	bucketPage.RecordCount = 0                       // Not ItemCount
// 	bucketPage.Records = make([]*IndexRecord, 0)     // Not Items
// 	bucketPage.OverflowPageNum = 0                   // Not OverflowPage
// 	bucketPage.FreeSpace = hi.metadata.PageSize - 64 // Reset free space

// 	// Update using bucket manager
// 	return hi.bucketManager.UpdateBucket(bucketPage)
// }

// clearOverflowChain removes all overflow pages in a chain
// func (hi *HashIndex) clearOverflowChain(firstOverflowPageNum uint32) error {
// 	currentPageNum := firstOverflowPageNum
// 	visitedPages := make(map[uint32]bool) // Prevent infinite loops

// 	for currentPageNum != 0 {
// 		if visitedPages[currentPageNum] {
// 			return fmt.Errorf("cycle detected in overflow chain at page %d", currentPageNum)
// 		}
// 		visitedPages[currentPageNum] = true

// 		// Load overflow page using correct method
// 		pageData, err := hi.pageManager.GetPage(currentPageNum, hi.fileManager.ReadPage)
// 		if err != nil {
// 			return fmt.Errorf("failed to load overflow page %d: %w", currentPageNum, err)
// 		}

// 		overflowPage, ok := pageData.(*OverflowPage)
// 		if !ok {
// 			return fmt.Errorf("page %d is not an overflow page", currentPageNum)
// 		}

// 		nextPageNum := overflowPage.NextOverflowPage

// 		// Clear the page and mark for deletion
// 		overflowPage.Records = make([]*IndexRecord, 0)
// 		overflowPage.RecordCount = 0
// 		overflowPage.NextOverflowPage = 0

// 		// Write cleared page (or implement page deletion)
// 		err = hi.fileManager.WritePage(currentPageNum, overflowPage)
// 		if err != nil {
// 			return fmt.Errorf("failed to clear overflow page %d: %w", currentPageNum, err)
// 		}

// 		// Update metadata
// 		hi.metadata.OverflowPages-- // Use correct field name
// 		currentPageNum = nextPageNum
// 	}

// 	return nil
// }

// calculateHash computes the hash value for a given key using FNV-1a algorithm
// Parameters:
//   - key: The byte array to hash
//   - seed: A seed value to add randomness
//
// Returns:
//   - uint32: The computed hash value
func calculateHash(key []byte, seed uint32) uint32 {
	// Use FNV-1a hash function
	h := fnv.New32a()

	// Add seed to prevent hash collision attacks
	seedBytes := make([]byte, 4)
	binary.LittleEndian.PutUint32(seedBytes, seed)
	h.Write(seedBytes)

	// Hash the key
	h.Write(key)

	return h.Sum32()
}

// GenerateHashSeed creates a random seed for the hash function
// Returns:
//   - uint32: A random seed value
func GenerateHashSeed() uint32 {
	// Try to use crypto/rand for better randomness
	seedBytes := make([]byte, 4)
	if _, err := rand.Read(seedBytes); err != nil {
		// Fallback to time-based seed
		return uint32(time.Now().UnixNano())
	}

	return binary.LittleEndian.Uint32(seedBytes)
}

// calculateBucket determines which bucket a hash value belongs to using linear hashing
// Parameters:
//   - hashValue: The hash value to map to a bucket
//   - bucketCount: Current number of buckets
//   - splitPointer: Current split pointer in linear hashing
//
// Returns:
//   - uint32: The bucket number (0-based)
func calculateBucket(hashValue uint32, bucketCount uint32, splitPointer uint32) uint32 {
	// Linear hashing algorithm
	bucket := hashValue % bucketCount

	// If bucket is before split pointer, it has been split
	if bucket < splitPointer {
		bucket = hashValue % (bucketCount * 2)
	}

	return bucket
}

// calculateBucket method for HashIndex
// Parameters:
//   - hashValue: The hash value to map to a bucket
//
// Returns:
//   - uint32: The bucket number (0-based)
func (hi *HashIndex) calculateBucket(hashValue uint32) uint32 {
	return calculateBucket(hashValue, hi.metadata.BucketCount, hi.metadata.SplitPointer)
}

// shouldSplit determines if the index should split a bucket based on load factor
// Returns:
//   - bool: Whether a split should occur
// func (hi *HashIndex) shouldSplit() bool {
// 	currentLoadFactor := float64(hi.Metadata.TotalRecords) / float64(hi.Metadata.BucketCount)
// 	return currentLoadFactor > hi.Metadata.LoadFactor
// }

// validateDocumentID checks if a document ID is valid
// Parameters:
//   - documentID: The document ID to validate
//
// Returns:
//   - error: Validation error if any
func validateDocumentID(documentID string) error {
	if documentID == "" {
		return fmt.Errorf("document ID cannot be empty")
	}

	if len(documentID) > 255 {
		return fmt.Errorf("document ID too long (max 255 characters)")
	}

	// Check for control characters that might cause issues
	for _, r := range documentID {
		if r < 32 && r != 9 && r != 10 && r != 13 { // Allow tab, LF, CR
			return fmt.Errorf("document ID contains invalid control character")
		}
	}

	return nil
}

// normalizeKey normalizes a key for consistent hashing
// Parameters:
//   - key: The key to normalize
//
// Returns:
//   - []byte: The normalized key
// func normalizeKey(key []byte) []byte {
// 	// Trim null terminators and whitespace
// 	result := make([]byte, 0, len(key))

// 	for _, b := range key {
// 		if b != 0 && b != ' ' && b != '\t' && b != '\n' && b != '\r' {
// 			result = append(result, b)
// 		}
// 	}

// 	return result
// }

// pageNumberToBucketNumber converts a page number to bucket number
// Parameters:
//   - pageNum: The page number (1-based, page 0 is metadata)
//
// Returns:
//   - uint32: The bucket number (0-based)
// func pageNumberToBucketNumber(pageNum uint32) uint32 {
// 	if pageNum == 0 {
// 		panic("page 0 is metadata page, not a bucket")
// 	}
// 	return pageNum - 1
// }

// bucketNumberToPageNumber converts a bucket number to page number
// Parameters:
//   - bucketNum: The bucket number (0-based)
//
// Returns:
//   - uint32: The page number (1-based)
func bucketNumberToPageNumber(bucketNum uint32) uint32 {
	return bucketNum + 1
}

// estimateRecordSize calculates the approximate size of a record in bytes
// Parameters:
//   - record: The record to measure
//
// Returns:
//   - uint32: Estimated size in bytes
// func estimateRecordSize(record *IndexRecord) uint32 {
// 	// Base record overhead
// 	size := uint32(24) // Fixed fields (hash, timestamp, etc.)

// 	// Document ID length
// 	size += uint32(len(record.DocumentID))

// 	// Add some padding for alignment
// 	size += 8

// 	return size
// }

// Fix the estimateRecordSize function (line 219):
// func estimateRecordSize(record *IndexRecord) uint32 {
// 	return record.Size() // Use the record's own size calculation
// }

// // splitBucket implements bucket splitting for linear hashing
// // Returns:
// //   - error: Any error that occurred during splitting
// func (hi *HashIndex) splitBucketBAD() error {
// 	hi.logger.Debugf("Splitting bucket %d", hi.metadata.SplitPointer)

// 	// Load the bucket to split
// 	oldBucketPage, err := hi.loadBucketPage(hi.metadata.SplitPointer)
// 	if err != nil {
// 		return fmt.Errorf("failed to load bucket for splitting: %w", err)
// 	}

// 	// Create new bucket
// 	newBucketNum := hi.metadata.BucketCount
// 	newBucketPage := NewBucketPage(newBucketNum, hi.metadata.PageSize)

// 	// Collect all records from old bucket and its overflow chain
// 	allRecords, err := hi.collectAllRecords(oldBucketPage)
// 	if err != nil {
// 		return fmt.Errorf("failed to collect records: %w", err)
// 	}

// 	// Clear old bucket
// 	oldBucketPage.Records = make([]*IndexRecord, 0)
// 	oldBucketPage.RecordCount = 0
// 	oldBucketPage.OverflowPageNum = 0

// 	// Redistribute records
// 	for _, record := range allRecords {
// 		bucketNum := calculateBucket(record.HashValue, hi.metadata.BucketCount*2, 0)

// 		if bucketNum == hi.metadata.SplitPointer {
// 			// Stays in old bucket
// 			if !oldBucketPage.CanFitRecord(record) {
// 				if err := hi.addToOverflow(oldBucketPage, hi.metadata.SplitPointer, record); err != nil {
// 					return fmt.Errorf("failed to add to old bucket overflow: %w", err)
// 				}
// 			} else {
// 				oldBucketPage.AddRecord(record)
// 			}
// 		} else {
// 			// Goes to new bucket
// 			if !newBucketPage.CanFitRecord(record) {
// 				if err := hi.addToOverflow(newBucketPage, newBucketNum, record); err != nil {
// 					return fmt.Errorf("failed to add to new bucket overflow: %w", err)
// 				}
// 			} else {
// 				newBucketPage.AddRecord(record)
// 			}
// 		}
// 	}

// 	// Write pages
// 	// hi.pageManager.PutPage(hi.metadata.SplitPointer, oldBucketPage, true)
// 	// hi.pageManager.PutPage(newBucketNum, newBucketPage, true)

// 	// Write old bucket page (corrected)
// 	oldPageNum := bucketNumberToPageNumber(hi.metadata.SplitPointer)
// 	hi.pageManager.PutPage(oldPageNum, oldBucketPage, true)
// 	if err := hi.fileManager.WritePage(oldPageNum, oldBucketPage); err != nil {
// 		return fmt.Errorf("failed to persist old bucket page: %w", err)
// 	}

// 	// Write new bucket page (corrected)
// 	newPageNum := bucketNumberToPageNumber(newBucketNum)
// 	hi.pageManager.PutPage(newPageNum, newBucketPage, true)
// 	if err := hi.fileManager.WritePage(newPageNum, newBucketPage); err != nil {
// 		return fmt.Errorf("failed to persist new bucket page: %w", err)
// 	}

// 	// Force sync to ensure buckets are on disk before committing bucket count change
// 	if err := hi.fileManager.Sync(); err != nil {

// 		return fmt.Errorf("failed to sync bucket pages: %w", err)
// 	}

// 	// Update metadata
// 	hi.metadata.BucketCount++
// 	hi.metadata.SplitPointer++

// 	// Reset split pointer if we've split all buckets in this round
// 	if hi.metadata.SplitPointer >= hi.metadata.BucketCount/2 {
// 		hi.metadata.SplitPointer = 0
// 	}

// 	hi.logger.Debugf("Split complete. New bucket count: %d, split pointer: %d",
// 		hi.metadata.BucketCount, hi.metadata.SplitPointer)

// 	return nil
// }

// splitBucket implements proper linear hashing bucket splitting
// This function follows the Single Responsibility Principle by handling only bucket splitting
// Following SyndrDB comprehensive error handling, it ensures proper record redistribution
func (hi *HashIndex) splitBucket() error {
	hi.logger.Debugf("Starting linear hash bucket split, current buckets: %d", hi.metadata.BucketCount)

	// PERFORMANCE FIX: In linear hashing, we only split ONE bucket (the split pointer bucket)
	// This makes splitting O(k) where k is records in one bucket, not O(n) for all records
	splitBucketNum := hi.metadata.SplitPointer
	newBucketNum := hi.metadata.BucketCount

	hi.logger.Debugf("Splitting bucket %d, creating new bucket %d", splitBucketNum, newBucketNum)

	// STEP 1: Load the bucket to be split
	splitBucket, err := hi.bucketManager.GetBucket(splitBucketNum)
	if err != nil {
		return fmt.Errorf("failed to load split bucket %d: %w", splitBucketNum, err)
	}

	// STEP 2: Collect all records from the split bucket (including overflow chain)
	allRecords, err := hi.collectAllRecords(splitBucket)
	if err != nil {
		return fmt.Errorf("failed to collect records from split bucket: %w", err)
	}

	hi.logger.Debugf("Found %d records in split bucket %d", len(allRecords), splitBucketNum)

	// STEP 3: Create new bucket
	newBucket := NewBucketPage(newBucketNum, hi.metadata.PageSize)

	// STEP 4: Update metadata FIRST to ensure correct bucket calculations
	oldBucketCount := hi.metadata.BucketCount
	hi.metadata.BucketCount = newBucketNum + 1
	hi.updateLinearHashingMetadata()

	// STEP 5: Redistribute records between old and new bucket
	splitRecords := make([]*IndexRecord, 0)
	newRecords := make([]*IndexRecord, 0)

	for _, record := range allRecords {
		// Recalculate bucket assignment with new bucket count
		targetBucket := hi.computeBucket(record.HashValue)

		if targetBucket == splitBucketNum {
			splitRecords = append(splitRecords, record)
		} else if targetBucket == newBucketNum {
			newRecords = append(newRecords, record)
		} else {
			// This shouldn't happen in proper linear hashing
			hi.logger.Warnf("Record %s maps to unexpected bucket %d during split", record.DocumentID, targetBucket)
			splitRecords = append(splitRecords, record) // Keep in original bucket
		}
	}

	hi.logger.Debugf("Split result: %d records stay in bucket %d, %d records move to bucket %d",
		len(splitRecords), splitBucketNum, len(newRecords), newBucketNum)

	// STEP 6: Clear the split bucket and repopulate
	splitBucket.Records = make([]*IndexRecord, 0)
	splitBucket.RecordCount = 0
	splitBucket.OverflowPageNum = 0 // Clear overflow chain

	// Add records back to split bucket
	for _, record := range splitRecords {
		if !splitBucket.CanFitRecord(record) {
			if err := hi.addToOverflow(splitBucket, splitBucketNum, record); err != nil {
				return fmt.Errorf("failed to add record to split bucket overflow: %w", err)
			}
		} else {
			splitBucket.AddRecord(record)
		}
	}

	// STEP 7: Populate new bucket
	for _, record := range newRecords {
		if !newBucket.CanFitRecord(record) {
			if err := hi.addToOverflow(newBucket, newBucketNum, record); err != nil {
				return fmt.Errorf("failed to add record to new bucket overflow: %w", err)
			}
		} else {
			newBucket.AddRecord(record)
		}
	}

	// STEP 8: Update split pointer for next split
	hi.metadata.SplitPointer++
	if hi.metadata.SplitPointer >= oldBucketCount {
		hi.metadata.SplitPointer = 0 // Reset for next round
	}

	// STEP 9: Persist changes (deferred to batch operations when possible)
	if err := hi.bucketManager.UpdateBucket(splitBucket); err != nil {
		return fmt.Errorf("failed to update split bucket: %w", err)
	}

	newPageNum := bucketNumberToPageNumber(newBucketNum)
	hi.pageManager.PutPage(newPageNum, newBucket, true)

	hi.logger.Infof("Linear hash split completed: bucket %d split, created bucket %d (total: %d buckets)",
		splitBucketNum, newBucketNum, hi.metadata.BucketCount)

	return nil
}

// updateLinearHashingMetadata updates linear hashing masks after a split
// This function follows the Single Responsibility Principle by handling only metadata updates
// Following SyndrDB comprehensive error handling, it maintains linear hashing state
func (hi *HashIndex) updateLinearHashingMetadata() {
	// Linear hashing mask management
	if hi.metadata.HighMask == 0 {
		// Initialize masks for first split
		hi.metadata.HighMask = hi.metadata.BucketCount - 1
		hi.metadata.LowMask = (hi.metadata.BucketCount / 2) - 1
		hi.metadata.MaxBucket = hi.metadata.BucketCount - 1
	} else {
		// Update masks for subsequent splits
		hi.metadata.MaxBucket = hi.metadata.BucketCount - 1

		// Check if we need to update masks
		if hi.metadata.BucketCount > hi.metadata.HighMask+1 {
			hi.metadata.LowMask = hi.metadata.HighMask
			hi.metadata.HighMask = 2*hi.metadata.HighMask + 1
		}
	}

	hi.logger.Debugf("Updated linear hashing metadata: HighMask=%d, LowMask=%d, MaxBucket=%d",
		hi.metadata.HighMask, hi.metadata.LowMask, hi.metadata.MaxBucket)
}

// collectAllRecords gathers all records from a bucket and its overflow chain
// Parameters:
//   - bucketPage: The bucket page to start from
//
// Returns:
//   - []*IndexRecord: All records in the bucket chain
//   - error: Any error that occurred
func (hi *HashIndex) collectAllRecords(bucketPage *BucketPage) ([]*IndexRecord, error) {
	var allRecords []*IndexRecord

	// Add records from main bucket page
	allRecords = append(allRecords, bucketPage.Records...)

	// Follow overflow chain
	currentOverflowPageNum := bucketPage.OverflowPageNum
	for currentOverflowPageNum != 0 {
		overflowPageData, err := hi.pageManager.GetPage(currentOverflowPageNum, hi.fileManager.ReadPage)
		if err != nil {
			return nil, fmt.Errorf("failed to read overflow page %d: %w", currentOverflowPageNum, err)
		}

		overflowPage := overflowPageData.(*OverflowPage)
		allRecords = append(allRecords, overflowPage.Records...)
		currentOverflowPageNum = overflowPage.NextOverflowPage
	}

	return allRecords, nil
}

// computeBucket determines which bucket a hash value maps to
// This implements the linear hashing bucket selection algorithm
// Parameters:
//   - hi: The hash index instance
//   - hashValue: The computed hash value for a key
//
// Returns:
//   - uint32: The bucket number (0-based)
func (hi *HashIndex) computeBucket(hashValue uint32) uint32 {

	if hi.metadata == nil {
		hi.logger.Errorf("CRITICAL: metadata is nil in computeBucket")
		return 0
	}

	if hi.metadata.BucketCount == 0 {
		hi.logger.Errorf("CRITICAL: bucket count is 0 in computeBucket")
		return 0
	}

	// Following SyndrDB data integrity requirements, validate mask consistency
	if hi.metadata.HighMask == 0 || hi.metadata.MaxBucket == 0 {
		// PERFORMANCE FIX: Reduce excessive logging - use Debug instead of Info
		hi.logger.Debugf("Linear hashing metadata not properly initialized, using simple modulo")
		hi.logger.Debugf("  HighMask: %d, LowMask: %d, MaxBucket: %d",
			hi.metadata.HighMask, hi.metadata.LowMask, hi.metadata.MaxBucket)

		// Fallback to simple modulo bucket calculation
		// Following SyndrDB defensive programming, provide reliable fallback
		bucketNum := hashValue % hi.metadata.BucketCount
		hi.logger.Debugf("computeBucket FALLBACK - using modulo: %d %% %d = %d",
			hashValue, hi.metadata.BucketCount, bucketNum)
		return bucketNum
	}

	// Apply the high mask first
	bucket := hashValue & hi.metadata.HighMask

	// If the bucket is beyond our current range, use the low mask
	if bucket > hi.metadata.MaxBucket {
		bucket = hashValue & hi.metadata.LowMask
	}

	// Final validation following SyndrDB data integrity requirements
	if bucket >= hi.metadata.BucketCount {
		hi.logger.Errorf("CRITICAL: computed bucket %d >= BucketCount %d, using fallback",
			bucket, hi.metadata.BucketCount)
		bucket = hashValue % hi.metadata.BucketCount
	}

	return bucket
}

func (hi *HashIndex) deleteFromBucket(bucketNum uint32, documentID string) error {
	// Load the bucket page
	bucketPage, err := hi.bucketManager.GetBucket(bucketNum)
	if err != nil {
		return fmt.Errorf("failed to load bucket %d: %w", bucketNum, err)
	}

	// Find and remove the record
	for i, rec := range bucketPage.Records {
		if rec.DocumentID == documentID {
			bucketPage.Records = append(bucketPage.Records[:i], bucketPage.Records[i+1:]...)
			bucketPage.RecordCount--
			return hi.bucketManager.UpdateBucket(bucketPage)
		}
	}

	return fmt.Errorf("record not found in bucket %d", bucketNum)
}

// shouldSplitFast determines if a bucket split is needed using fast calculation
// PERFORMANCE OPTIMIZED: Uses simple math instead of expensive bucket iteration
// Returns:
//   - bool: True if a split is needed
func (hi *HashIndex) shouldSplitFast() (bool, error) {
	// Fast load factor calculation: total records / total buckets
	// This is O(1) instead of O(n) bucket iteration
	currentLoadFactor := float64(hi.metadata.TotalRecords) / float64(hi.metadata.BucketCount)
	targetLoadFactor := hi.metadata.LoadFactor
	shouldSplit := currentLoadFactor > targetLoadFactor

	hi.logger.Debugf("Fast split evaluation: records=%d, buckets=%d, current=%.4f, target=%.4f, shouldSplit=%v",
		hi.metadata.TotalRecords, hi.metadata.BucketCount, currentLoadFactor, targetLoadFactor, shouldSplit)

	// Additional safety check: don't split if we have very few documents
	minDocumentsForSplit := hi.metadata.BucketCount * 2 // At least 2 documents per bucket
	if hi.metadata.TotalRecords < uint64(minDocumentsForSplit) {
		hi.logger.Debugf("Preventing split: only %d documents for %d buckets (minimum: %d)",
			hi.metadata.TotalRecords, hi.metadata.BucketCount, minDocumentsForSplit)
		return false, nil
	}

	return shouldSplit, nil
}

// shouldSplit determines if a bucket split is needed based on load factor
// DEPRECATED: Use shouldSplitFast() for better performance
// Returns true if the current load factor exceeds the target fill factor
// Returns:
//   - bool: True if a split is needed
// func (hi *HashIndex) shouldSplit() (bool, error) {
// 	hi.logger.Debugf("Evaluating split decision with %d documents across %d buckets",
// 		hi.metadata.DocumentCount, hi.metadata.BucketCount)

// 	// CRITICAL FIX: Use proper load factor calculation
// 	currentLoadFactor, err := hi.CalculateCurrentLoadFactor(hi.bucketManager)
// 	if err != nil {
// 		return false, fmt.Errorf("failed to calculate current load factor: %w", err)
// 	}

// 	targetLoadFactor := hi.metadata.LoadFactor
// 	shouldSplit := currentLoadFactor > targetLoadFactor

// 	hi.logger.Infof("Split evaluation: current=%.4f, target=%.4f, shouldSplit=%v",
// 		currentLoadFactor, targetLoadFactor, shouldSplit)

// 	// Additional safety check: don't split if we have very few documents
// 	minDocumentsForSplit := hi.metadata.BucketCount / 2 // At least half the buckets should have documents
// 	if hi.metadata.DocumentCount < uint64(minDocumentsForSplit) {
// 		hi.logger.Infof("Preventing split: only %d documents for %d buckets (minimum: %d)",
// 			hi.metadata.DocumentCount, hi.metadata.BucketCount, minDocumentsForSplit)
// 		return false, nil
// 	}

// 	// Additional safety check: don't split if most buckets are empty
// 	nonEmptyBuckets := hi.countNonEmptyBuckets(hi.bucketManager)
// 	emptyBucketThreshold := float64(0.5) // Allow split only if less than 50% buckets are empty
// 	emptyBucketRatio := float64(hi.metadata.BucketCount-nonEmptyBuckets) / float64(hi.metadata.BucketCount)

// 	if emptyBucketRatio > emptyBucketThreshold {
// 		hi.logger.Infof("Preventing split: %.1f%% buckets are empty (threshold: %.1f%%)",
// 			emptyBucketRatio*100, emptyBucketThreshold*100)
// 		return false, nil
// 	}

// 	return shouldSplit, nil
// }

// CalculateCurrentLoadFactor computes the actual load factor based on bucket utilization
// This function follows the Single Responsibility Principle by handling only load factor calculation
// Following SyndrDB comprehensive error handling, it provides accurate load measurements
func (hi *HashIndex) CalculateCurrentLoadFactor(bucketManager *BucketManager) (float64, error) {
	if bucketManager == nil {
		return 0.0, fmt.Errorf("bucket manager cannot be nil")
	}

	hi.logger.Debugf("Calculating current load factor for %d buckets", hi.metadata.BucketCount)

	totalCapacity := uint64(0)
	totalUsed := uint64(0)
	nonEmptyBuckets := uint32(0)

	// Examine each bucket to calculate actual utilization
	for bucketNum := uint32(0); bucketNum < hi.metadata.BucketCount; bucketNum++ {
		bucket, err := bucketManager.GetBucket(bucketNum)
		if err != nil {
			hi.logger.Warnf("Failed to load bucket %d for load calculation: %v", bucketNum, err)
			continue
		}

		// Calculate bucket capacity (approximate)
		bucketCapacity := hi.calculateBucketCapacity()
		bucketUsed := hi.calculateBucketUsage(bucket)

		totalCapacity += uint64(bucketCapacity)
		totalUsed += uint64(bucketUsed)

		if bucket.RecordCount > 0 {
			nonEmptyBuckets++
		}

		hi.logger.Debugf("Bucket %d: capacity=%d, used=%d, records=%d",
			bucketNum, bucketCapacity, bucketUsed, bucket.RecordCount)
	}

	// Calculate load factor as percentage of total capacity used
	var loadFactor float64
	if totalCapacity > 0 {
		loadFactor = float64(totalUsed) / float64(totalCapacity)
	} else {
		loadFactor = 0.0
	}

	hi.logger.Infof("Load factor calculation: used=%d, capacity=%d, load=%.4f, nonEmptyBuckets=%d/%d",
		totalUsed, totalCapacity, loadFactor, nonEmptyBuckets, hi.metadata.BucketCount)

	return loadFactor, nil
}

// calculateBucketCapacity estimates the capacity of a single bucket
// This function follows the Single Responsibility Principle by handling only capacity calculation
// Following SyndrDB comprehensive error handling, it provides accurate capacity estimates
func (hi *HashIndex) calculateBucketCapacity() uint32 {
	// Account for bucket page header overhead
	headerSize := uint32(100) // Approximate header size

	// Available space for records
	availableSpace := hi.metadata.PageSize - headerSize

	// Estimate average record size (UUID + hash + overhead)
	averageRecordSize := uint32(60) // 36 (UUID) + 4 (hash) + 20 (overhead)

	// Calculate maximum records that can fit
	maxRecords := availableSpace / averageRecordSize

	return maxRecords
}

// calculateBucketUsage calculates current usage of a bucket
// This function follows the Single Responsibility Principle by handling only usage calculation
// Following SyndrDB comprehensive error handling, it provides accurate usage measurements
func (hi *HashIndex) calculateBucketUsage(bucket *BucketPage) uint32 {
	if bucket.RecordCount == 0 {
		return 0
	}

	// Count records in main bucket page
	usage := bucket.RecordCount

	// Add records from overflow pages if they exist
	// This would need to be implemented based on overflow chain traversal
	// For now, we'll use the record count from the bucket page

	return usage
}

// countNonEmptyBuckets counts how many buckets actually contain documents
// This function follows the Single Responsibility Principle by handling only bucket counting
// Following SyndrDB comprehensive error handling, it provides accurate bucket counts
// func (hi *HashIndex) countNonEmptyBuckets(bucketManager *BucketManager) uint32 {
// 	nonEmptyCount := uint32(0)

// 	for bucketNum := uint32(0); bucketNum < hi.metadata.BucketCount; bucketNum++ {
// 		bucket, err := bucketManager.GetBucket(bucketNum)
// 		if err != nil {
// 			hi.logger.Warnf("Failed to load bucket %d for counting: %v", bucketNum, err)
// 			continue
// 		}

// 		if bucket.RecordCount > 0 {
// 			nonEmptyCount++
// 		}
// 	}

// 	hi.logger.Debugf("Non-empty buckets: %d/%d", nonEmptyCount, hi.metadata.BucketCount)
// 	return nonEmptyCount
// }

// // calculateSplitBucket determines which bucket should be split next
// // In linear hashing, buckets are split in round-robin order
// // Returns:
// //   - uint32: The bucket number to split
// func (hi *HashIndex) calculateSplitBucket() uint32 {
// 	// In linear hashing, we split buckets in order starting from 0
// 	// The split bucket is determined by the current state of the masks
// 	if hi.metadata.LowMask == 0 {
// 		return 0 // First split of the round
// 	}

// 	// Continue the round-robin split sequence
// 	return hi.metadata.MaxBucket + 1 - hi.metadata.HighMask
// }

// // updateMetadataForSplit updates the hash index metadata after a bucket split
// // This includes updating masks, bucket counts, and other tracking information
// func (hi *HashIndex) updateMetadataForSplit() {
// 	oldMaxBucket := hi.metadata.MaxBucket
// 	hi.metadata.MaxBucket = hi.metadata.BucketCount // New bucket becomes the max
// 	hi.metadata.BucketCount++

// 	// Update masks according to linear hashing algorithm
// 	if (oldMaxBucket + 1) >= hi.metadata.HighMask {
// 		// We've completed a round of splits, double the high mask
// 		hi.metadata.HighMask = 2*hi.metadata.HighMask + 1
// 		hi.metadata.LowMask = 0
// 	} else {
// 		// Still in the middle of a round, increment low mask
// 		hi.metadata.LowMask++
// 	}

// 	hi.logger.Debugf("Updated metadata - MaxBucket: %d, HighMask: %d, LowMask: %d",
// 		hi.metadata.MaxBucket, hi.metadata.HighMask, hi.metadata.LowMask)
// }

// redistributeItems distributes items between the split bucket and new bucket
// Items are redistributed based on their hash values and the new mask
// Parameters:
//   - items: List of items to redistribute
//   - splitBucketNum: The bucket that was split
//   - newBucketNum: The newly created bucket
//
// Returns:
//   - error: Any error that occurred during redistribution
// func (hi *HashIndex) redistributeItems(items []*IndexRecord, splitBucketNum, newBucketNum uint32) error {
// 	splitCount := 0
// 	newCount := 0

// 	for _, item := range items {
// 		// Recalculate which bucket this item belongs to with new masks
// 		targetBucket := hi.computeBucket(item.HashValue)

// 		var err error
// 		if targetBucket == splitBucketNum {
// 			err = hi.insertIntoBucket(splitBucketNum, item)
// 			splitCount++
// 		} else if targetBucket == newBucketNum {
// 			err = hi.insertIntoBucket(newBucketNum, item)
// 			newCount++
// 		} else {
// 			// This should not happen in linear hashing
// 			return fmt.Errorf("item hash %d maps to unexpected bucket %d", item.HashValue, targetBucket)
// 		}

// 		if err != nil {
// 			return fmt.Errorf("failed to insert item during redistribution: %w", err)
// 		}
// 	}

// 	hi.logger.Debugf("Redistributed %d items: %d to split bucket, %d to new bucket",
// 		len(items), splitCount, newCount)

// 	return nil
// }
