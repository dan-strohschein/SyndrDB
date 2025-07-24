package hashindex

import (
	"bytes"
	"crypto/rand"
	"fmt"
	"os"
	"syndrdb/src/internal/storage/hash"
	"time"
)

func generateSeed() uint32 {
	var seedBytes [4]byte
	_, err := rand.Read(seedBytes[:])
	if err != nil {
		// Fall back to time-based seed
		return uint32(time.Now().UnixNano())
	}
	return uint32(seedBytes[0]) |
		uint32(seedBytes[1])<<8 |
		uint32(seedBytes[2])<<16 |
		uint32(seedBytes[3])<<24
}

func (hi *HashIndex) AddDocumentByField(fieldName string, fieldValue string, docID string) error {
	// Prepare the key (could be just the field value, or fieldName+value for composite keys)
	key := []byte(fieldValue)

	// Use the default HashIndexFile (assume hi.File is set up)
	hf := hi.File
	if hf == nil {
		return fmt.Errorf("HashIndexFile not initialized")
	}

	hif := &hash.HashIndexFile{
		FilePath:     hi.FilePath,
		File:         hi.File,
		PageCache:    hi.PageCache,
		CacheSize:    hi.CacheSize,
		MaxCacheSize: hi.MaxCacheSize,
		Logger:       hi.Logger,
		Dirty:        hi.Dirty,
	}

	// Use default TID (0 for first version)
	tid := uint64(0)

	// Call the main Insert function
	return hi.Insert(hif, key, docID, tid)
}

// Insert adds a key to the hash index
func (hi *HashIndex) Insert(hif *hash.HashIndexFile, key []byte, docID string, tid uint64) error {
	hi.Lock()
	defer hi.Unlock()

	// Compute hash value
	//hashValue := hashKey(key)

	hashValue := jenkinsHash(key, hi.Metadata.Seed)

	// Find the bucket
	bucketNum := hi.ComputeBucket(hashValue)

	// check to see if the hi.File is open
	if hif.File == nil {
		var file_err error
		hif.File, file_err = os.OpenFile(hif.FilePath, os.O_RDWR|os.O_CREATE, 0644)
		if file_err != nil {
			return fmt.Errorf("INSERT:: failed to open hash index file: %w", file_err)
		}
	}

	if hif.File == nil {
		return fmt.Errorf("INSERT:: hash index file is not open, COULD NOT BECAUSE")
	}

	// Read the bucket page
	bucketPage, err := hif.ReadPage(bucketNum)
	if err != nil {
		return fmt.Errorf("INSERT::failed to read bucket page: %w", err)
	}

	// Check if key already exists (for uniqueness)
	if hi.Metadata.IsUnique {
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

			existingPage, err = hif.ReadPage(existingPage.NextPage)
			if err != nil {
				return fmt.Errorf("failed to read overflow page: %w", err)
			}
		}
	}

	// Create new index item
	item := hash.HashIndexItem{
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
			overflowPage := &hash.HashIndexPage{
				PageType:  HashOverflowPage,
				PageNum:   overflowPageNum,
				NextPage:  0,
				ItemCount: 0,
				FreeSpace: HashPageSize - 32, // Approximate header size
				Items:     make([]hash.HashIndexItem, 0),
			}

			// Link it to the bucket
			bucketPage.NextPage = overflowPageNum

			// Write the updated bucket page
			if err := hif.WritePage(bucketNum, bucketPage); err != nil {
				return fmt.Errorf("failed to update bucket page: %w", err)
			}

			// Add item to the overflow page
			overflowPage.Items = append(overflowPage.Items, item)
			overflowPage.ItemCount++
			overflowPage.FreeSpace -= uint16(itemSize)

			// Write the overflow page
			if err := hif.WritePage(overflowPageNum, overflowPage); err != nil {
				return fmt.Errorf("failed to write overflow page: %w", err)
			}

			hi.Metadata.OverflowPages++
		} else {
			// Use existing overflow chain
			// Find the last page with enough space
			currentPageNum := bucketPage.NextPage
			for {
				currentPage, err := hif.ReadPage(currentPageNum)
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
						if err := hif.WritePage(currentPageNum, currentPage); err != nil {
							return fmt.Errorf("failed to update overflow page: %w", err)
						}
					} else {
						// Need another overflow page
						overflowPageNum := hi.allocateNewPage()
						overflowPage := &hash.HashIndexPage{
							PageType:  HashOverflowPage,
							PageNum:   overflowPageNum,
							NextPage:  0,
							ItemCount: 1,
							FreeSpace: HashPageSize - 32 - uint16(itemSize),
							Items:     []hash.HashIndexItem{item},
						}

						// Link it to the chain
						currentPage.NextPage = overflowPageNum

						// Write the updated pages
						if err := hif.WritePage(currentPageNum, currentPage); err != nil {
							return fmt.Errorf("failed to update overflow page: %w", err)
						}

						if err := hif.WritePage(overflowPageNum, overflowPage); err != nil {
							return fmt.Errorf("failed to write new overflow page: %w", err)
						}

						hi.Metadata.OverflowPages++
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
		if err := hif.WritePage(bucketNum, bucketPage); err != nil {
			return fmt.Errorf("failed to update bucket page: %w", err)
		}
	}

	// Update metadata
	hi.Metadata.NumTuples++
	hi.Dirty = true

	// Check if we need to split a bucket
	fillPercentage := (float64(hi.Metadata.NumTuples) / float64(hi.Metadata.MaxBucket+1)) /
		(float64(HashPageSize) * float64(hi.Metadata.FillFactor) / 100.0)

	if fillPercentage > 1.0 {
		return hi.SplitBucket(hif)
	}

	return nil
}

// allocateNewPage finds the next available page number
func (hi *HashIndex) allocateNewPage() uint32 {
	// Simple implementation: just return the next page number
	// In a real implementation, you'd track free/used pages
	return uint32(hi.Metadata.MaxBucket + hi.Metadata.OverflowPages + 2) // +2 because page 0 is meta, and bucket pages start at 1
}

// splitBucket implements the linear hashing bucket split algorithm
func (hi *HashIndex) SplitBucket(hif *hash.HashIndexFile) error {
	// Determine which bucket to split (round-robin)
	splitBucket := uint32(0)
	if hi.Metadata.LowMask > 0 {
		// After first round of splits
		splitBucket = hi.Metadata.MaxBucket + 1 - hi.Metadata.HighMask
	}

	hi.Logger.Infof("Splitting bucket %d", splitBucket)

	// Create a new bucket
	newBucketNum := hi.Metadata.MaxBucket + 1
	newBucketPage := &hash.HashIndexPage{
		PageType:  HashBucketPage,
		PageNum:   newBucketNum + 1, // Convert to 1-based page numbers
		ItemCount: 0,
		FreeSpace: HashPageSize - 32,
		Items:     make([]hash.HashIndexItem, 0),
	}

	// Read the bucket being split
	splitBucketPage, err := hif.ReadPage(splitBucket + 1) // Convert to 1-based
	if err != nil {
		return fmt.Errorf("failed to read split bucket: %w", err)
	}

	// Collect all items from the bucket and its overflow chain
	var allItems []hash.HashIndexItem
	currentPage := splitBucketPage
	for {
		allItems = append(allItems, currentPage.Items...)

		if currentPage.NextPage == 0 {
			break
		}

		currentPage, err = hif.ReadPage(currentPage.NextPage)
		if err != nil {
			return fmt.Errorf("failed to read overflow page: %w", err)
		}
	}

	// Reset the split bucket
	splitBucketPage.Items = make([]hash.HashIndexItem, 0)
	splitBucketPage.ItemCount = 0
	splitBucketPage.FreeSpace = HashPageSize - 32
	splitBucketPage.NextPage = 0

	// Write the empty bucket
	if err := hif.WritePage(splitBucket+1, splitBucketPage); err != nil {
		return fmt.Errorf("failed to write reset bucket: %w", err)
	}

	// Delete all overflow pages (in a full implementation, we will recycle these)
	// For this example, we'll just "forget" them

	// Write the new bucket
	if err := hif.WritePage(newBucketNum+1, newBucketPage); err != nil {
		return fmt.Errorf("failed to write new bucket: %w", err)
	}

	// Update metadata for the split
	oldMaxBucket := hi.Metadata.MaxBucket
	hi.Metadata.MaxBucket = newBucketNum

	// Update masks based on PostgreSQL's linear hashing algorithm
	if (oldMaxBucket + 1) >= (hi.Metadata.HighMask + 1) {
		// We've completed a round of splits, double the high mask
		hi.Metadata.HighMask = 2*hi.Metadata.HighMask + 1
		hi.Metadata.LowMask = 0
	} else {
		// Still in the middle of a round
		hi.Metadata.LowMask = hi.Metadata.LowMask + 1
	}

	// Redistribute items between the split and new buckets
	for _, item := range allItems {
		bucketNum := hi.ComputeBucket(item.HashValue)
		targetPage, err := hif.ReadPage(bucketNum)
		if err != nil {
			return fmt.Errorf("failed to read target bucket: %w", err)
		}

		// Insert the item (simplified - doesn't handle overflow during redistribution)
		targetPage.Items = append(targetPage.Items, item)
		targetPage.ItemCount++

		// Write the updated page
		if err := hif.WritePage(bucketNum, targetPage); err != nil {
			return fmt.Errorf("failed to update bucket during redistribution: %w", err)
		}
	}

	hi.Dirty = true
	return nil
}

// computeBucket determines which bucket a hash value belongs to
func (hi *HashIndex) ComputeBucket(hashValue uint32) uint32 {
	bucket := hashValue & hi.Metadata.HighMask

	// Check if this bucket has been split
	if bucket > hi.Metadata.MaxBucket {
		// Apply the "linear hashing" algorithm logic
		bucket = hashValue & hi.Metadata.LowMask
	}

	return bucket + 1 // Convert to 1-based page numbers
}

// Find searches for a key in the hash index
func (hi *HashIndex) Find(hif *hash.HashIndexFile, key []byte) (*hash.IndexTuple, error) {
	hi.RLock()
	defer hi.RUnlock()

	// Compute hash value
	//hashValue := hashKey(key)
	hashValue := jenkinsHash(key, hi.Metadata.Seed) // Using the new Jenkins hash function
	// Find the bucket
	bucketNum := hi.ComputeBucket(hashValue)

	// check to see if the hi.File is open
	if hif.File == nil {
		var file_err error
		hif.File, file_err = os.OpenFile(hif.FilePath, os.O_RDWR|os.O_CREATE, 0644)
		if file_err != nil {
			return nil, fmt.Errorf("failed to open hash index file: %w", file_err)
		}
	}

	// Read the bucket page
	bucketPage, err := hif.ReadPage(bucketNum)
	if err != nil {
		return nil, fmt.Errorf("FIND::failed to read bucket page: %w", err)
	}

	// Search for the key in this bucket and its overflow chain
	currentPage := bucketPage
	for {
		for _, item := range currentPage.Items {
			if bytes.Equal(item.Key, key) {
				// Found the key
				return &hash.IndexTuple{
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

		currentPage, err = hif.ReadPage(currentPage.NextPage)
		if err != nil {
			return nil, fmt.Errorf("failed to read overflow page: %w", err)
		}
	}

	// Key not found
	return nil, nil
}

// ScanAll scans the entire hash index
func (hi *HashIndex) ScanAll(hif *hash.HashIndexFile) ([]*hash.IndexTuple, error) {
	hi.RLock()
	defer hi.RUnlock()

	var results []*hash.IndexTuple

	// Scan all buckets
	for bucketNum := uint32(1); bucketNum <= hi.Metadata.MaxBucket+1; bucketNum++ {
		bucketPage, err := hif.ReadPage(bucketNum)
		if err != nil {
			return nil, fmt.Errorf("failed to read bucket page: %w", err)
		}

		// Process this bucket and its overflow chain
		currentPage := bucketPage
		for {
			for _, item := range currentPage.Items {
				results = append(results, &hash.IndexTuple{
					Key:   item.Key,
					DocID: item.DocID,
					TID:   item.TID,
				})
			}

			// Process overflow pages
			if currentPage.NextPage == 0 {
				break
			}

			currentPage, err = hif.ReadPage(currentPage.NextPage)
			if err != nil {
				return nil, fmt.Errorf("failed to read overflow page: %w", err)
			}
		}
	}

	return results, nil
}
