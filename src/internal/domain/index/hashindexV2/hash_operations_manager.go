/*
HASH INDEX OPERATIONS

This file implements the core operations for the hash index including
insertion, search, and bucket management. These operations work together
to provide the main functionality of the hash index system.

OPERATION TYPES:
- Insert operations: Add new key-value pairs to the index
- Search operations: Find values associated with keys
- Bucket operations: Manage primary and overflow buckets
- Maintenance operations: Split buckets and handle overflow

INSERTION ALGORITHM:
1. Compute hash value for the key
2. Determine target bucket using linear hashing
3. Check for duplicates if unique index
4. Add item to bucket or overflow page
5. Split bucket if load factor exceeded

SEARCH ALGORITHM:
1. Compute hash value for the key
2. Determine target bucket using linear hashing
3. Search bucket and overflow chain for matching keys
4. Return all matching values

BUCKET OVERFLOW HANDLING:
When a bucket becomes full, overflow pages are allocated and chained
to the primary bucket. This allows unlimited growth while maintaining
reasonable performance characteristics.
*/

package hashindexV2

import (
	"fmt"
)

// Maximum items per bucket page (approximate based on page size)
const MaxItemsPerBucket = 100

// insertIntoBucket adds an item to the specified bucket
// Parameters:
//   - hi: The hash index instance
//   - bucketNum: The bucket number to insert into
//   - item: The item to insert
//
// Returns:
//   - error: Any error that occurred during insertion
func (hi *HashIndex) insertIntoBucket(bucketNum uint32, record *IndexRecord) error {
	hi.logger.Debugf("Inserting record into bucket %d: %+v", bucketNum, record)

	// Load the bucket page
	bucketPage, err := hi.loadBucketPage(bucketNum)
	if err != nil {
		return fmt.Errorf("failed to get bucket %d: %w", bucketNum, err)
	}

	// Check if bucket can fit the record
	if !bucketPage.CanFitRecord(record) {
		// Need to handle overflow
		return hi.addToOverflow(bucketPage, bucketNum, record)
	}

	// Add record to bucket
	bucketPage.AddRecord(record)

	// Update the bucket in storage
	pageNum := bucketNumberToPageNumber(bucketNum)
	hi.pageManager.PutPage(pageNum, bucketPage, true)

	hi.logger.Debugf("Record inserted into bucket %d", bucketNum)
	return nil
}

// searchInBucket searches for a key in the specified bucket and its overflow chain
// Parameters:
//   - bucketNum: The bucket number to search in
//   - key: The key to search for
//
// Returns:
//   - []string: List of values (document IDs) found for the key
//   - error: Any error that occurred during search
func (hi *HashIndex) searchInBucket(bucketNum uint32, key string) ([]string, error) {
	hi.logger.Debugf("Searching for key '%s' in bucket %d", key, bucketNum)

	var results []string

	// Load the bucket page
	bucketPage, err := hi.loadBucketPage(bucketNum)
	if err != nil {
		return nil, fmt.Errorf("failed to load bucket %d: %w", bucketNum, err)
	}

	// Search in main bucket page
	results = append(results, hi.searchRecordsInPage(bucketPage.Records, key)...)

	// Search in overflow chain if it exists
	if bucketPage.OverflowPageNum != 0 {
		overflowResults, err := hi.searchInOverflowChain(bucketPage.OverflowPageNum, key)
		if err != nil {
			return results, fmt.Errorf("failed to search overflow chain: %w", err)
		}
		results = append(results, overflowResults...)
	}

	hi.logger.Debugf("Found %d results for key '%s' in bucket %d", len(results), key, bucketNum)
	return results, nil
}

// searchRecordsInPage searches for a key within a slice of records
// Parameters:
//   - records: The records to search through
//   - key: The key to search for
//
// Returns:
//   - []string: List of matching document IDs
func (hi *HashIndex) searchRecordsInPage(records []*IndexRecord, key string) []string {
	var results []string

	for _, record := range records {
		if record.DocumentID == key {
			results = append(results, record.DocumentID)
			hi.logger.Debugf("Found matching record: %s", record.DocumentID)
		}
	}

	return results
}

// searchInOverflowChain searches for a key in an overflow page chain
// Parameters:
//   - startPageNum: The first overflow page number
//   - key: The key to search for
//
// Returns:
//   - []string: List of matching document IDs
//   - error: Any error that occurred during search
func (hi *HashIndex) searchInOverflowChain(startPageNum uint32, key string) ([]string, error) {
	var results []string
	currentPageNum := startPageNum
	visitedPages := make(map[uint32]bool) // Prevent infinite loops

	hi.logger.Debugf("Searching overflow chain starting at page %d", startPageNum)

	for currentPageNum != 0 {
		// Check for cycles
		if visitedPages[currentPageNum] {
			return results, fmt.Errorf("cycle detected in overflow chain at page %d", currentPageNum)
		}
		visitedPages[currentPageNum] = true

		// Load overflow page
		overflowPageData, err := hi.pageManager.GetPage(currentPageNum, hi.fileManager.ReadPage)
		if err != nil {
			return results, fmt.Errorf("failed to read overflow page %d: %w", currentPageNum, err)
		}

		overflowPage, ok := overflowPageData.(*OverflowPage)
		if !ok {
			return results, fmt.Errorf("page %d is not an overflow page, got type %T", currentPageNum, overflowPageData)
		}

		// Search records in this overflow page
		pageResults := hi.searchRecordsInPage(overflowPage.Records, key)
		results = append(results, pageResults...)

		// Move to next overflow page
		currentPageNum = overflowPage.NextPageNum
	}

	hi.logger.Debugf("Found %d results in overflow chain", len(results))
	return results, nil
}

// keyExists checks if a key already exists in the index (for unique constraint checking)
// Parameters:
//   - key: The key to check
//
// Returns:
//   - bool: Whether the key exists
//   - error: Any error that occurred during check
func (hi *HashIndex) keyExists(key string) (bool, error) {
	results, err := hi.Search(key)
	if err != nil {
		return false, err
	}
	return len(results) > 0, nil
}

// findRecordInBucket finds a specific record in a bucket (used for deletion)
// Parameters:
//   - bucketNum: The bucket number to search in
//   - key: The key to find
//
// Returns:
//   - *IndexRecord: The found record (nil if not found)
//   - uint32: Page number where the record was found (0 if not found)
//   - int: Index of the record in the page (-1 if not found)
//   - error: Any error that occurred during search
func (hi *HashIndex) findRecordInBucket(bucketNum uint32, key string) (*IndexRecord, uint32, int, error) {
	hi.logger.Debugf("Finding record for key '%s' in bucket %d", key, bucketNum)

	// Load bucket page
	bucketPage, err := hi.loadBucketPage(bucketNum)
	if err != nil {
		return nil, 0, -1, fmt.Errorf("failed to load bucket %d: %w", bucketNum, err)
	}

	// Search in main bucket page
	for i, record := range bucketPage.Records {
		if record.DocumentID == key {
			pageNum := bucketNumberToPageNumber(bucketNum)
			hi.logger.Debugf("Found record in main bucket page %d at index %d", pageNum, i)
			return record, pageNum, i, nil
		}
	}

	// Search in overflow chain
	if bucketPage.OverflowPageNum != 0 {
		return hi.findRecordInOverflowChain(bucketPage.OverflowPageNum, key)
	}

	hi.logger.Debugf("Record not found for key '%s' in bucket %d", key, bucketNum)
	return nil, 0, -1, nil
}

// findRecordInOverflowChain finds a record in an overflow chain
// Parameters:
//   - startPageNum: The first overflow page number
//   - key: The key to find
//
// Returns:
//   - *IndexRecord: The found record (nil if not found)
//   - uint32: Page number where the record was found (0 if not found)
//   - int: Index of the record in the page (-1 if not found)
//   - error: Any error that occurred during search
func (hi *HashIndex) findRecordInOverflowChain(startPageNum uint32, key string) (*IndexRecord, uint32, int, error) {
	currentPageNum := startPageNum
	visitedPages := make(map[uint32]bool)

	for currentPageNum != 0 {
		// Check for cycles
		if visitedPages[currentPageNum] {
			return nil, 0, -1, fmt.Errorf("cycle detected in overflow chain at page %d", currentPageNum)
		}
		visitedPages[currentPageNum] = true

		// Load overflow page
		overflowPageData, err := hi.pageManager.GetPage(currentPageNum, hi.fileManager.ReadPage)
		if err != nil {
			return nil, 0, -1, fmt.Errorf("failed to read overflow page %d: %w", currentPageNum, err)
		}

		overflowPage, ok := overflowPageData.(*OverflowPage)
		if !ok {
			return nil, 0, -1, fmt.Errorf("page %d is not an overflow page", currentPageNum)
		}

		// Search records in this overflow page
		for i, record := range overflowPage.Records {
			if record.DocumentID == key {
				hi.logger.Debugf("Found record in overflow page %d at index %d", currentPageNum, i)
				return record, currentPageNum, i, nil
			}
		}

		// Move to next overflow page
		currentPageNum = overflowPage.NextPageNum
	}

	return nil, 0, -1, nil
}
