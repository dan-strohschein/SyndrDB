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

import (
	"crypto/rand"
	"encoding/binary"
	"fmt"
	"hash/fnv"
	"time"
)

// createBucket creates a new empty bucket using the bucket manager
func (hi *HashIndex) createBucket(bucketNum uint32) error {
	_, err := hi.bucketManager.CreateBucket(bucketNum, hi.metadata.PageSize)
	return err
}

// clearBucket removes all records from a bucket and its overflow pages
func (hi *HashIndex) clearBucket(bucketNum uint32) error {
	// Load the bucket using correct types
	bucketPage, err := hi.bucketManager.GetBucket(bucketNum)
	if err != nil {
		return fmt.Errorf("failed to load bucket %d: %w", bucketNum, err)
	}

	// Clear overflow pages if they exist
	if bucketPage.OverflowPageNum != 0 {
		err = hi.clearOverflowChain(bucketPage.OverflowPageNum)
		if err != nil {
			return fmt.Errorf("failed to clear overflow chain: %w", err)
		}
	}

	// Clear the bucket itself - use CORRECT field names
	bucketPage.RecordCount = 0                       // Not ItemCount
	bucketPage.Records = make([]*IndexRecord, 0)     // Not Items
	bucketPage.OverflowPageNum = 0                   // Not OverflowPage
	bucketPage.FreeSpace = hi.metadata.PageSize - 64 // Reset free space

	// Update using bucket manager
	return hi.bucketManager.UpdateBucket(bucketPage)
}

// clearOverflowChain removes all overflow pages in a chain
func (hi *HashIndex) clearOverflowChain(firstOverflowPageNum uint32) error {
	currentPageNum := firstOverflowPageNum
	visitedPages := make(map[uint32]bool) // Prevent infinite loops

	for currentPageNum != 0 {
		if visitedPages[currentPageNum] {
			return fmt.Errorf("cycle detected in overflow chain at page %d", currentPageNum)
		}
		visitedPages[currentPageNum] = true

		// Load overflow page using correct method
		pageData, err := hi.pageManager.GetPage(currentPageNum, hi.fileManager.ReadPage)
		if err != nil {
			return fmt.Errorf("failed to load overflow page %d: %w", currentPageNum, err)
		}

		overflowPage, ok := pageData.(*OverflowPage)
		if !ok {
			return fmt.Errorf("page %d is not an overflow page", currentPageNum)
		}

		nextPageNum := overflowPage.NextPageNum

		// Clear the page and mark for deletion
		overflowPage.Records = make([]*IndexRecord, 0)
		overflowPage.RecordCount = 0
		overflowPage.NextPageNum = 0

		// Write cleared page (or implement page deletion)
		err = hi.fileManager.WritePage(currentPageNum, overflowPage)
		if err != nil {
			return fmt.Errorf("failed to clear overflow page %d: %w", currentPageNum, err)
		}

		// Update metadata
		hi.metadata.OverflowPages-- // Use correct field name
		currentPageNum = nextPageNum
	}

	return nil
}

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

// generateHashSeed creates a random seed for the hash function
// Returns:
//   - uint32: A random seed value
func generateHashSeed() uint32 {
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
func normalizeKey(key []byte) []byte {
	// Trim null terminators and whitespace
	result := make([]byte, 0, len(key))

	for _, b := range key {
		if b != 0 && b != ' ' && b != '\t' && b != '\n' && b != '\r' {
			result = append(result, b)
		}
	}

	return result
}

// pageNumberToBucketNumber converts a page number to bucket number
// Parameters:
//   - pageNum: The page number (1-based, page 0 is metadata)
//
// Returns:
//   - uint32: The bucket number (0-based)
func pageNumberToBucketNumber(pageNum uint32) uint32 {
	if pageNum == 0 {
		panic("page 0 is metadata page, not a bucket")
	}
	return pageNum - 1
}

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
func estimateRecordSize(record *IndexRecord) uint32 {
	return record.Size() // Use the record's own size calculation
}

// splitBucket implements bucket splitting for linear hashing
// Returns:
//   - error: Any error that occurred during splitting
func (hi *HashIndex) splitBucket() error {
	hi.logger.Debugf("Splitting bucket %d", hi.metadata.SplitPointer)

	// Load the bucket to split
	oldBucketPage, err := hi.loadBucketPage(hi.metadata.SplitPointer)
	if err != nil {
		return fmt.Errorf("failed to load bucket for splitting: %w", err)
	}

	// Create new bucket
	newBucketNum := hi.metadata.BucketCount
	newBucketPage := NewBucketPage(newBucketNum, hi.metadata.PageSize)

	// Collect all records from old bucket and its overflow chain
	allRecords, err := hi.collectAllRecords(oldBucketPage)
	if err != nil {
		return fmt.Errorf("failed to collect records: %w", err)
	}

	// Clear old bucket
	oldBucketPage.Records = make([]*IndexRecord, 0)
	oldBucketPage.RecordCount = 0
	oldBucketPage.OverflowPageNum = 0

	// Redistribute records
	for _, record := range allRecords {
		bucketNum := calculateBucket(record.HashValue, hi.metadata.BucketCount*2, 0)

		if bucketNum == hi.metadata.SplitPointer {
			// Stays in old bucket
			if !oldBucketPage.CanFitRecord(record) {
				if err := hi.addToOverflow(oldBucketPage, hi.metadata.SplitPointer, record); err != nil {
					return fmt.Errorf("failed to add to old bucket overflow: %w", err)
				}
			} else {
				oldBucketPage.AddRecord(record)
			}
		} else {
			// Goes to new bucket
			if !newBucketPage.CanFitRecord(record) {
				if err := hi.addToOverflow(newBucketPage, newBucketNum, record); err != nil {
					return fmt.Errorf("failed to add to new bucket overflow: %w", err)
				}
			} else {
				newBucketPage.AddRecord(record)
			}
		}
	}

	// Write pages
	hi.pageManager.PutPage(hi.metadata.SplitPointer, oldBucketPage, true)
	hi.pageManager.PutPage(newBucketNum, newBucketPage, true)

	// Update metadata
	hi.metadata.BucketCount++
	hi.metadata.SplitPointer++

	// Reset split pointer if we've split all buckets in this round
	if hi.metadata.SplitPointer >= hi.metadata.BucketCount/2 {
		hi.metadata.SplitPointer = 0
	}

	hi.logger.Debugf("Split complete. New bucket count: %d, split pointer: %d",
		hi.metadata.BucketCount, hi.metadata.SplitPointer)

	return nil
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
		currentOverflowPageNum = overflowPage.NextPageNum
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
	// Apply the high mask first
	bucket := hashValue & hi.metadata.HighMask

	// If the bucket is beyond our current range, use the low mask
	if bucket > hi.metadata.MaxBucket {
		bucket = hashValue & hi.metadata.LowMask
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

// shouldSplit determines if a bucket split is needed based on load factor
// Returns true if the current load factor exceeds the target fill factor
// Returns:
//   - bool: True if a split is needed
func (hi *HashIndex) shouldSplit() bool {
	if hi.metadata.BucketCount == 0 {
		return false
	}

	// Calculate current load factor
	loadFactor := float64(hi.metadata.TotalRecords) / float64(hi.metadata.BucketCount)
	targetLoad := float64(hi.metadata.FillFactor) / 100.0

	hi.logger.Debugf("Load factor: %.2f, Target: %.2f", loadFactor, targetLoad)

	return loadFactor > targetLoad
}

// splitBucket implements the linear hashing bucket split algorithm
// This splits the "next" bucket in the round-robin sequence
// Returns:
//   - error: Any error that occurred during the split
// func (hi *HashIndex) splitBucket() error {
// 	hi.Logger.Debugf("Starting bucket split, current buckets: %d", hi.Metadata.NumBuckets)

// 	// Determine which bucket to split (the "next" bucket in linear hashing)
// 	splitBucketNum := hi.calculateSplitBucket()
// 	newBucketNum := hi.Metadata.NumBuckets

// 	hi.Logger.Debugf("Splitting bucket %d, creating bucket %d", splitBucketNum, newBucketNum)

// 	// Read all items from the bucket being split (including overflow pages)
// 	items, err := hi.readAllBucketItems(splitBucketNum)
// 	if err != nil {
// 		return fmt.Errorf("failed to read bucket items: %w", err)
// 	}

// 	hi.Logger.Debugf("Read %d items from bucket %d", len(items), splitBucketNum)

// 	// Clear the split bucket
// 	err = hi.clearBucket(splitBucketNum)
// 	if err != nil {
// 		return fmt.Errorf("failed to clear split bucket: %w", err)
// 	}

// 	// Create the new bucket
// 	err = hi.createBucket(newBucketNum)
// 	if err != nil {
// 		return fmt.Errorf("failed to create new bucket: %w", err)
// 	}

// 	// Update metadata for the split
// 	hi.updateMetadataForSplit()

// 	// Redistribute items between old and new buckets
// 	err = hi.redistributeItems(items, splitBucketNum, newBucketNum)
// 	if err != nil {
// 		return fmt.Errorf("failed to redistribute items: %w", err)
// 	}

// 	hi.Logger.Debugf("Split complete, now have %d buckets", hi.Metadata.NumBuckets)
// 	return nil
// }

// calculateSplitBucket determines which bucket should be split next
// In linear hashing, buckets are split in round-robin order
// Returns:
//   - uint32: The bucket number to split
func (hi *HashIndex) calculateSplitBucket() uint32 {
	// In linear hashing, we split buckets in order starting from 0
	// The split bucket is determined by the current state of the masks
	if hi.metadata.LowMask == 0 {
		return 0 // First split of the round
	}

	// Continue the round-robin split sequence
	return hi.metadata.MaxBucket + 1 - hi.metadata.HighMask
}

// updateMetadataForSplit updates the hash index metadata after a bucket split
// This includes updating masks, bucket counts, and other tracking information
func (hi *HashIndex) updateMetadataForSplit() {
	oldMaxBucket := hi.metadata.MaxBucket
	hi.metadata.MaxBucket = hi.metadata.BucketCount // New bucket becomes the max
	hi.metadata.BucketCount++

	// Update masks according to linear hashing algorithm
	if (oldMaxBucket + 1) >= hi.metadata.HighMask {
		// We've completed a round of splits, double the high mask
		hi.metadata.HighMask = 2*hi.metadata.HighMask + 1
		hi.metadata.LowMask = 0
	} else {
		// Still in the middle of a round, increment low mask
		hi.metadata.LowMask++
	}

	hi.logger.Debugf("Updated metadata - MaxBucket: %d, HighMask: %d, LowMask: %d",
		hi.metadata.MaxBucket, hi.metadata.HighMask, hi.metadata.LowMask)
}

// redistributeItems distributes items between the split bucket and new bucket
// Items are redistributed based on their hash values and the new mask
// Parameters:
//   - items: List of items to redistribute
//   - splitBucketNum: The bucket that was split
//   - newBucketNum: The newly created bucket
//
// Returns:
//   - error: Any error that occurred during redistribution
func (hi *HashIndex) redistributeItems(items []*IndexRecord, splitBucketNum, newBucketNum uint32) error {
	splitCount := 0
	newCount := 0

	for _, item := range items {
		// Recalculate which bucket this item belongs to with new masks
		targetBucket := hi.computeBucket(item.HashValue)

		var err error
		if targetBucket == splitBucketNum {
			err = hi.insertIntoBucket(splitBucketNum, item)
			splitCount++
		} else if targetBucket == newBucketNum {
			err = hi.insertIntoBucket(newBucketNum, item)
			newCount++
		} else {
			// This should not happen in linear hashing
			return fmt.Errorf("item hash %d maps to unexpected bucket %d", item.HashValue, targetBucket)
		}

		if err != nil {
			return fmt.Errorf("failed to insert item during redistribution: %w", err)
		}
	}

	hi.logger.Debugf("Redistributed %d items: %d to split bucket, %d to new bucket",
		len(items), splitCount, newCount)

	return nil
}
