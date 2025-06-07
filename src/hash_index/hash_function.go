package hashindex

import "hash/fnv"

// hashKey computes the hash value of a key
func hashKey(key []byte) uint32 {
	// Use FNV hash for simplicity in this example
	// PostgreSQL uses a more sophisticated hash function
	h := fnv.New32a()
	h.Write(key)
	return h.Sum32()
}

// computeBucket determines which bucket a hash value belongs to
func (hi *HashIndex) computeBucket(hashValue uint32) uint32 {
	bucket := hashValue & hi.metadata.HighMask

	// Check if this bucket has been split
	if bucket > hi.metadata.MaxBucket {
		// Apply the "linear hashing" algorithm logic
		bucket = hashValue & hi.metadata.LowMask
	}

	return bucket + 1 // Convert to 1-based page numbers
}
