package hashindex

import (
	"bytes"
	"fmt"
)

// Insert adds a key to the hash index
func (hi *HashIndex) Insert(key []byte, docID string, tid uint64) error {
	hi.Lock()
	defer hi.Unlock()

	// Compute hash value
	hashValue := hashKey(key)

	// Find the bucket
	bucketNum := hi.computeBucket(hashValue)

	// Read the bucket page
	bucketPage, err := hi.readPage(bucketNum)
	if err != nil {
		return fmt.Errorf("failed to read bucket page: %w", err)
	}

	// Check if key already exists (for uniqueness)
	if hi.metadata.IsUnique {
		existingPage := bucketPage
		for {
			for _, item := range existingPage.Items {
				if bytes.Equal(item.Key, key) {
					return fmt.Errorf("duplicate key detected in unique index")
				}
			}

			// Check overflow pages
			if existingPage.NextPage == 0 {
				break
			}

			existingPage, err = hi.readPage(existingPage.NextPage)
			if err != nil {
				return fmt.Errorf("failed to read overflow page: %w", err)
			}
		}
	}

	// Create new index item
	item := HashIndexItem{
		HashValue: hashValue,
		Key:       key,
		DocID:     docID,
		TID:       tid,
	}

	// Calculate item size (approximate)
	itemSize := 12 + len(key) + len(docID) // header + key + docID

	// Check if item fits in the current bucket page
	if int(bucketPage.FreeSpace) < itemSize {
		// Need to use or create an overflow page
		if bucketPage.NextPage == 0 {
			// Create a new overflow page
			overflowPageNum := hi.allocateNewPage()
			overflowPage := &HashIndexPage{
				PageType:  HashOverflowPage,
				PageNum:   overflowPageNum,
				NextPage:  0,
				ItemCount: 0,
				FreeSpace: HashPageSize - 32, // Approximate header size
				Items:     make([]HashIndexItem, 0),
			}

			// Link it to the bucket
			bucketPage.NextPage = overflowPageNum

			// Write the updated bucket page
			if err := hi.writePage(bucketNum, bucketPage); err != nil {
				return fmt.Errorf("failed to update bucket page: %w", err)
			}

			// Add item to the overflow page
			overflowPage.Items = append(overflowPage.Items, item)
			overflowPage.ItemCount++
			overflowPage.FreeSpace -= uint16(itemSize)

			// Write the overflow page
			if err := hi.writePage(overflowPageNum, overflowPage); err != nil {
				return fmt.Errorf("failed to write overflow page: %w", err)
			}

			hi.metadata.OverflowPages++
		} else {
			// Use existing overflow chain
			// Find the last page with enough space
			currentPageNum := bucketPage.NextPage
			for {
				currentPage, err := hi.readPage(currentPageNum)
				if err != nil {
					return fmt.Errorf("failed to read overflow page: %w", err)
				}

				// If this page has enough space or is the last page
				if int(currentPage.FreeSpace) >= itemSize || currentPage.NextPage == 0 {
					if int(currentPage.FreeSpace) >= itemSize {
						// Add item to this page
						currentPage.Items = append(currentPage.Items, item)
						currentPage.ItemCount++
						currentPage.FreeSpace -= uint16(itemSize)

						// Write the updated page
						if err := hi.writePage(currentPageNum, currentPage); err != nil {
							return fmt.Errorf("failed to update overflow page: %w", err)
						}
					} else {
						// Need another overflow page
						overflowPageNum := hi.allocateNewPage()
						overflowPage := &HashIndexPage{
							PageType:  HashOverflowPage,
							PageNum:   overflowPageNum,
							NextPage:  0,
							ItemCount: 1,
							FreeSpace: HashPageSize - 32 - uint16(itemSize),
							Items:     []HashIndexItem{item},
						}

						// Link it to the chain
						currentPage.NextPage = overflowPageNum

						// Write the updated pages
						if err := hi.writePage(currentPageNum, currentPage); err != nil {
							return fmt.Errorf("failed to update overflow page: %w", err)
						}

						if err := hi.writePage(overflowPageNum, overflowPage); err != nil {
							return fmt.Errorf("failed to write new overflow page: %w", err)
						}

						hi.metadata.OverflowPages++
					}
					break
				}

				currentPageNum = currentPage.NextPage
			}
		}
	} else {
		// Add item to the bucket page
		bucketPage.Items = append(bucketPage.Items, item)
		bucketPage.ItemCount++
		bucketPage.FreeSpace -= uint16(itemSize)

		// Write the updated page
		if err := hi.writePage(bucketNum, bucketPage); err != nil {
			return fmt.Errorf("failed to update bucket page: %w", err)
		}
	}

	// Update metadata
	hi.metadata.NumTuples++
	hi.dirty = true

	// Check if we need to split a bucket
	fillPercentage := (float64(hi.metadata.NumTuples) / float64(hi.metadata.MaxBucket+1)) /
		(float64(HashPageSize) * float64(hi.metadata.FillFactor) / 100.0)

	if fillPercentage > 1.0 {
		return hi.splitBucket()
	}

	return nil
}

// allocateNewPage finds the next available page number
func (hi *HashIndex) allocateNewPage() uint32 {
	// Simple implementation: just return the next page number
	// In a real implementation, you'd track free/used pages
	return uint32(hi.metadata.MaxBucket + hi.metadata.OverflowPages + 2) // +2 because page 0 is meta, and bucket pages start at 1
}

// splitBucket implements the linear hashing bucket split algorithm
func (hi *HashIndex) splitBucket() error {
	// Determine which bucket to split (round-robin)
	splitBucket := uint32(0)
	if hi.metadata.LowMask > 0 {
		// After first round of splits
		splitBucket = hi.metadata.MaxBucket + 1 - hi.metadata.HighMask
	}

	hi.logger.Infof("Splitting bucket %d", splitBucket)

	// Create a new bucket
	newBucketNum := hi.metadata.MaxBucket + 1
	newBucketPage := &HashIndexPage{
		PageType:  HashBucketPage,
		PageNum:   newBucketNum + 1, // Convert to 1-based page numbers
		ItemCount: 0,
		FreeSpace: HashPageSize - 32,
		Items:     make([]HashIndexItem, 0),
	}

	// Read the bucket being split
	splitBucketPage, err := hi.readPage(splitBucket + 1) // Convert to 1-based
	if err != nil {
		return fmt.Errorf("failed to read split bucket: %w", err)
	}

	// Collect all items from the bucket and its overflow chain
	var allItems []HashIndexItem
	currentPage := splitBucketPage
	for {
		allItems = append(allItems, currentPage.Items...)

		if currentPage.NextPage == 0 {
			break
		}

		currentPage, err = hi.readPage(currentPage.NextPage)
		if err != nil {
			return fmt.Errorf("failed to read overflow page: %w", err)
		}
	}

	// Reset the split bucket
	splitBucketPage.Items = make([]HashIndexItem, 0)
	splitBucketPage.ItemCount = 0
	splitBucketPage.FreeSpace = HashPageSize - 32
	splitBucketPage.NextPage = 0

	// Write the empty bucket
	if err := hi.writePage(splitBucket+1, splitBucketPage); err != nil {
		return fmt.Errorf("failed to write reset bucket: %w", err)
	}

	// Delete all overflow pages (in a real implementation, you'd recycle these)
	// For this example, we'll just "forget" them

	// Write the new bucket
	if err := hi.writePage(newBucketNum+1, newBucketPage); err != nil {
		return fmt.Errorf("failed to write new bucket: %w", err)
	}

	// Update metadata for the split
	oldMaxBucket := hi.metadata.MaxBucket
	hi.metadata.MaxBucket = newBucketNum

	// Update masks based on PostgreSQL's linear hashing algorithm
	if (oldMaxBucket + 1) >= (hi.metadata.HighMask + 1) {
		// We've completed a round of splits, double the high mask
		hi.metadata.HighMask = 2*hi.metadata.HighMask + 1
		hi.metadata.LowMask = 0
	} else {
		// Still in the middle of a round
		hi.metadata.LowMask = hi.metadata.LowMask + 1
	}

	// Redistribute items between the split and new buckets
	for _, item := range allItems {
		bucketNum := hi.computeBucket(item.HashValue)
		targetPage, err := hi.readPage(bucketNum)
		if err != nil {
			return fmt.Errorf("failed to read target bucket: %w", err)
		}

		// Insert the item (simplified - doesn't handle overflow during redistribution)
		targetPage.Items = append(targetPage.Items, item)
		targetPage.ItemCount++

		// Write the updated page
		if err := hi.writePage(bucketNum, targetPage); err != nil {
			return fmt.Errorf("failed to update bucket during redistribution: %w", err)
		}
	}

	hi.dirty = true
	return nil
}

// Find searches for a key in the hash index
func (hi *HashIndex) Find(key []byte) (*IndexTuple, error) {
	hi.RLock()
	defer hi.RUnlock()

	// Compute hash value
	hashValue := hashKey(key)

	// Find the bucket
	bucketNum := hi.computeBucket(hashValue)

	// Read the bucket page
	bucketPage, err := hi.readPage(bucketNum)
	if err != nil {
		return nil, fmt.Errorf("failed to read bucket page: %w", err)
	}

	// Search for the key in this bucket and its overflow chain
	currentPage := bucketPage
	for {
		for _, item := range currentPage.Items {
			if bytes.Equal(item.Key, key) {
				// Found the key
				return &IndexTuple{
					Key:   key,
					DocID: item.DocID,
					TID:   item.TID,
				}, nil
			}
		}

		// Check overflow pages
		if currentPage.NextPage == 0 {
			break
		}

		currentPage, err = hi.readPage(currentPage.NextPage)
		if err != nil {
			return nil, fmt.Errorf("failed to read overflow page: %w", err)
		}
	}

	// Key not found
	return nil, nil
}

// ScanAll scans the entire hash index
func (hi *HashIndex) ScanAll() ([]*IndexTuple, error) {
	hi.RLock()
	defer hi.RUnlock()

	var results []*IndexTuple

	// Scan all buckets
	for bucketNum := uint32(1); bucketNum <= hi.metadata.MaxBucket+1; bucketNum++ {
		bucketPage, err := hi.readPage(bucketNum)
		if err != nil {
			return nil, fmt.Errorf("failed to read bucket page: %w", err)
		}

		// Process this bucket and its overflow chain
		currentPage := bucketPage
		for {
			for _, item := range currentPage.Items {
				results = append(results, &IndexTuple{
					Key:   item.Key,
					DocID: item.DocID,
					TID:   item.TID,
				})
			}

			// Process overflow pages
			if currentPage.NextPage == 0 {
				break
			}

			currentPage, err = hi.readPage(currentPage.NextPage)
			if err != nil {
				return nil, fmt.Errorf("failed to read overflow page: %w", err)
			}
		}
	}

	return results, nil
}
