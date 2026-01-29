package joinexecutor

import (
	"fmt"
	"sync"
	"sync/atomic"

	"syndrdb/src/internal/domain/models"
	"syndrdb/src/pkg/common/conversion"

	syndrdbsimd "github.com/dan-strohschein/syndrdb-simd"
)

// InMemoryHashTable implements the HashTable interface using an in-memory hash table
// This is optimized for SyndrDB's document storage with support for multiple values per key
type InMemoryHashTable struct {
	buckets    []bucket     // Array of buckets for hash table storage
	size       int          // Number of unique keys stored
	capacity   int          // Current capacity (number of buckets)
	loadFactor float64      // Target load factor before resizing
	memoryUsed int64        // Estimated memory usage in bytes
	mutex      sync.RWMutex // Protects concurrent access during build phase
	frozen     atomic.Bool  // Once true, hash table is read-only and Get() is lock-free

	// PHASE 2: Disk spillover support
	// spillManager *DiskSpillManager
	// isSpilled    bool
	// spillThreshold int64
}

// bucket represents a single bucket in the hash table
type bucket struct {
	entries []hashEntry // Entries in this bucket (for collision resolution)
}

// hashEntry represents a single key-value entry
type hashEntry struct {
	key       interface{}        // The join key
	documents []*models.Document // Documents with this key (supports multiple docs per key)
	keyHash   uint64             // Cached hash value for the key
}

// NewInMemoryHashTable creates a new in-memory hash table
// initialCapacity: Initial number of buckets
// loadFactor: Target load factor before resizing (typically 0.75)
func NewInMemoryHashTable(initialCapacity int, loadFactor float64) HashTable {
	if initialCapacity <= 0 {
		initialCapacity = 16 // Default minimum capacity
	}
	if loadFactor <= 0 || loadFactor > 1.0 {
		loadFactor = 0.75 // Default load factor
	}

	return &InMemoryHashTable{
		buckets:    make([]bucket, initialCapacity),
		size:       0,
		capacity:   initialCapacity,
		loadFactor: loadFactor,
		memoryUsed: int64(initialCapacity * 64), // Estimate initial memory
	}
}

// Put stores a key-value pair in the hash table
// Panics if called on a frozen hash table (hash tables must be frozen after build phase)
func (ht *InMemoryHashTable) Put(key interface{}, value *models.Document) error {
	if ht.frozen.Load() {
		panic("Put called on frozen hash table - hash tables are immutable after Freeze()")
	}
	if key == nil || value == nil {
		return fmt.Errorf("key and value cannot be nil")
	}

	ht.mutex.Lock()
	defer ht.mutex.Unlock()

	// Check if we need to resize
	if float64(ht.size) >= float64(ht.capacity)*ht.loadFactor {
		ht.resize()
	}

	// Calculate hash and bucket index
	keyHash := ht.hashKey(key)
	bucketIndex := int(keyHash % uint64(ht.capacity))
	bucket := &ht.buckets[bucketIndex]

	// Look for existing entry with this key
	for i := range bucket.entries {
		entry := &bucket.entries[i]
		if entry.keyHash == keyHash && ht.keysEqual(entry.key, key) {
			// Key already exists, add document to existing entry
			entry.documents = append(entry.documents, value)
			ht.memoryUsed += ht.estimateDocumentSize(value)
			return nil
		}
	}

	// Key doesn't exist, create new entry
	newEntry := hashEntry{
		key:       key,
		documents: []*models.Document{value},
		keyHash:   keyHash,
	}
	bucket.entries = append(bucket.entries, newEntry)
	ht.size++
	ht.memoryUsed += ht.estimateEntrySize(key, value)

	return nil
}

// Get retrieves all documents associated with a key
// When frozen, this is lock-free and returns the original slice (zero-copy).
// When not frozen, acquires RLock and returns a defensive copy.
func (ht *InMemoryHashTable) Get(key interface{}) ([]*models.Document, bool) {
	if key == nil {
		return nil, false
	}

	// Fast path: frozen hash table - lock-free, zero-copy access
	if ht.frozen.Load() {
		return ht.getUnsafe(key)
	}

	// Slow path: mutable hash table - need lock and defensive copy
	ht.mutex.RLock()
	defer ht.mutex.RUnlock()

	// Calculate hash and bucket index
	keyHash := ht.hashKey(key)
	bucketIndex := int(keyHash % uint64(ht.capacity))
	bucket := &ht.buckets[bucketIndex]

	// Search for entry with this key
	for _, entry := range bucket.entries {
		if entry.keyHash == keyHash && ht.keysEqual(entry.key, key) {
			// Return copy of documents slice to prevent external modification
			docs := make([]*models.Document, len(entry.documents))
			copy(docs, entry.documents)
			return docs, true
		}
	}

	return nil, false
}

// getUnsafe performs lock-free lookup. Only safe when hash table is frozen.
// Returns the original slice directly (zero-copy) for maximum performance.
func (ht *InMemoryHashTable) getUnsafe(key interface{}) ([]*models.Document, bool) {
	// Calculate hash and bucket index
	keyHash := ht.hashKey(key)
	bucketIndex := int(keyHash % uint64(ht.capacity))
	bucket := &ht.buckets[bucketIndex]

	// Search for entry with this key
	for _, entry := range bucket.entries {
		if entry.keyHash == keyHash && ht.keysEqual(entry.key, key) {
			// Return original slice directly - safe because hash table is frozen
			return entry.documents, true
		}
	}

	return nil, false
}

// TryGet attempts to retrieve documents without blocking.
// Returns (documents, found, acquired). If acquired is false, the read lock was
// contended and the caller should retry or use Get with blocking.
// When frozen, always succeeds immediately with lock-free access.
func (ht *InMemoryHashTable) TryGet(key interface{}) ([]*models.Document, bool, bool) {
	if key == nil {
		return nil, false, true
	}

	// Fast path: frozen hash table - always succeeds, lock-free
	if ht.frozen.Load() {
		docs, found := ht.getUnsafe(key)
		return docs, found, true
	}

	// Slow path: mutable hash table - try to acquire lock
	metrics := GetLockMetrics()
	if !ht.mutex.TryRLock() {
		// Lock contended - record failure and return without blocking
		metrics.RecordHashTableAttempt(true)
		return nil, false, false
	}
	defer ht.mutex.RUnlock()
	metrics.RecordHashTableAttempt(false)

	// Calculate hash and bucket index
	keyHash := ht.hashKey(key)
	bucketIndex := int(keyHash % uint64(ht.capacity))
	bucket := &ht.buckets[bucketIndex]

	// Search for entry with this key
	for _, entry := range bucket.entries {
		if entry.keyHash == keyHash && ht.keysEqual(entry.key, key) {
			// Return copy of documents slice to prevent external modification
			docs := make([]*models.Document, len(entry.documents))
			copy(docs, entry.documents)
			return docs, true, true
		}
	}

	return nil, false, true
}

// Contains checks if a key exists in the hash table
func (ht *InMemoryHashTable) Contains(key interface{}) bool {
	_, found := ht.Get(key)
	return found
}

// Size returns the number of unique keys in the hash table
func (ht *InMemoryHashTable) Size() int {
	ht.mutex.RLock()
	defer ht.mutex.RUnlock()
	return ht.size
}

// GetMemoryUsage returns current memory usage in bytes
func (ht *InMemoryHashTable) GetMemoryUsage() int64 {
	ht.mutex.RLock()
	defer ht.mutex.RUnlock()
	return ht.memoryUsed
}

// Clear removes all entries from the hash table and resets frozen state.
// After Clear(), the hash table can be rebuilt and frozen again.
func (ht *InMemoryHashTable) Clear() {
	// Reset frozen flag first so we can acquire lock
	ht.frozen.Store(false)

	ht.mutex.Lock()
	defer ht.mutex.Unlock()

	// Reset all buckets
	for i := range ht.buckets {
		ht.buckets[i].entries = nil
	}

	ht.size = 0
	ht.memoryUsed = int64(ht.capacity * 64) // Reset to base memory usage
}

// Freeze marks the hash table as read-only after build phase completes.
// Once frozen, Get() becomes lock-free and returns the original slice (zero-copy).
// Put() will panic if called after Freeze().
// This eliminates RWMutex contention during concurrent probe phase reads.
func (ht *InMemoryHashTable) Freeze() {
	ht.frozen.Store(true)
}

// IsFrozen returns true if the hash table has been frozen.
// Frozen hash tables provide lock-free, zero-copy reads.
func (ht *InMemoryHashTable) IsFrozen() bool {
	return ht.frozen.Load()
}

// resize increases the capacity of the hash table and rehashes all entries
func (ht *InMemoryHashTable) resize() {
	oldBuckets := ht.buckets
	oldCapacity := ht.capacity

	// Double the capacity
	ht.capacity = oldCapacity * 2
	ht.buckets = make([]bucket, ht.capacity)
	ht.size = 0
	oldMemoryUsed := ht.memoryUsed
	ht.memoryUsed = int64(ht.capacity * 64) // Reset base memory

	// Rehash all entries
	for _, bucket := range oldBuckets {
		for _, entry := range bucket.entries {
			// Recalculate bucket index with new capacity
			bucketIndex := int(entry.keyHash % uint64(ht.capacity))
			newBucket := &ht.buckets[bucketIndex]

			// Add entry to new bucket
			newBucket.entries = append(newBucket.entries, entry)
			ht.size++
		}
	}

	// Update memory usage (structure overhead may have changed)
	ht.memoryUsed = oldMemoryUsed + int64((ht.capacity-oldCapacity)*64)
}

// hashKey computes the hash value for a key using SIMD-accelerated hashing when available
func (ht *InMemoryHashTable) hashKey(key interface{}) uint64 {
	// Use SIMD-accelerated hashing for common types
	switch v := key.(type) {
	case string:
		// SIMD XXHash for strings (UUIDs, etc.) - 4x faster than FNV
		// XXHash64Bytes accepts []byte and returns uint64
		return syndrdbsimd.XXHash64Bytes([]byte(v))
	case int64, int, int32, int16, int8:
		// For integer types, convert to string and use XXHash
		// (CRC32Int64 is batch-only, needs slice input)
		keyStr := conversion.ValueToString(key)
		return syndrdbsimd.XXHash64Bytes([]byte(keyStr))
	default:
		// Fallback: Convert to string and use SIMD XXHash
		keyStr := conversion.ValueToString(key)
		return syndrdbsimd.XXHash64Bytes([]byte(keyStr))
	}
}

// keysEqual compares two keys for equality using SIMD when available
func (ht *InMemoryHashTable) keysEqual(key1, key2 interface{}) bool {
	// Fast path: use SIMD equality for common types
	switch v1 := key1.(type) {
	case string:
		if v2, ok := key2.(string); ok {
			// SIMD string equality - 4-6x faster for UUIDs
			return syndrdbsimd.StrEq([]byte(v1), []byte(v2))
		}
	case int64:
		// For now, use standard comparison for integers (batch SIMD not beneficial for single values)
		if v2, ok := key2.(int64); ok {
			return v1 == v2
		}
	case int:
		if v2, ok := key2.(int); ok {
			return v1 == v2
		}
	case int32:
		if v2, ok := key2.(int32); ok {
			return v1 == v2
		}
	}

	// Fallback to string conversion for other types
	return conversion.ValueToString(key1) == conversion.ValueToString(key2)
}

// estimateEntrySize estimates the memory size of a hash table entry
func (ht *InMemoryHashTable) estimateEntrySize(key interface{}, doc *models.Document) int64 {
	// Rough estimate: key + document + overhead
	keySize := int64(len(fmt.Sprintf("%v", key)))
	docSize := ht.estimateDocumentSize(doc)
	overhead := int64(64) // Struct overhead, pointers, etc.

	return keySize + docSize + overhead
}

// estimateDocumentSize estimates the memory size of a document
func (ht *InMemoryHashTable) estimateDocumentSize(doc *models.Document) int64 {
	// Rough estimate based on document fields
	// This is a simplified calculation - could be enhanced for better accuracy
	baseSize := int64(200) // Base struct size

	// Add estimated size of string fields
	baseSize += int64(len(doc.DocumentID) * 2) // Unicode strings are roughly 2 bytes per char

	// Add size of other fields (rough estimates)
	baseSize += int64(100) // CreatedAt, UpdatedAt, other fields

	return baseSize
}

// GetAllKeys returns all unique keys in the hash table
// This is used for index-assisted probing where we need to query the index with all keys
// Returns a slice of keys (caller must type-assert to appropriate type)
// TODO: Consider returning a more specific type or using generics if performance becomes an issue
func (ht *InMemoryHashTable) GetAllKeys() []interface{} {
	ht.mutex.RLock()
	defer ht.mutex.RUnlock()

	// Pre-allocate slice with exact capacity to avoid reallocation
	keys := make([]interface{}, 0, ht.size)

	// Iterate through all buckets and collect keys
	for i := range ht.buckets {
		bucket := &ht.buckets[i]
		for j := range bucket.entries {
			entry := &bucket.entries[j]
			keys = append(keys, entry.key)
		}
	}

	return keys
}

// PHASE 2: Disk spillover methods (to be implemented)
/*
func (ht *InMemoryHashTable) SpillToDisk() error {
	// Implementation for spilling hash table to disk when memory pressure is high
	// This will use the DiskSpillManager to write buckets to cloud storage
	return fmt.Errorf("disk spillover not yet implemented")
}

func (ht *InMemoryHashTable) LoadFromDisk() error {
	// Implementation for loading hash table from disk
	// This will read spilled buckets from cloud storage back into memory
	return fmt.Errorf("disk spillover not yet implemented")
}

func (ht *InMemoryHashTable) IsSpilled() bool {
	// Returns whether this hash table has been spilled to disk
	return false // Will be implemented in Phase 2
}
*/

// PHASE 4: Advanced optimization methods (to be implemented)
/*
func (ht *InMemoryHashTable) CreateBloomFilter() BloomFilter {
	// Creates a Bloom filter from the keys in this hash table
	// This can be used to quickly filter probe side documents
	return nil
}

func (ht *InMemoryHashTable) GetKeyDistribution() KeyDistribution {
	// Returns statistics about key distribution for optimization
	return KeyDistribution{}
}

func (ht *InMemoryHashTable) Compress() error {
	// Compresses the hash table to reduce memory usage
	// Useful when memory is constrained but CPU is available
	return fmt.Errorf("compression not yet implemented")
}
*/
