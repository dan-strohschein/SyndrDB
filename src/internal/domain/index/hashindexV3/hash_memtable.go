package hashindexV3

/*
HASH MEMTABLE - IN-MEMORY LSM CACHE LAYER

This file implements the in-memory portion of the LSM-style hash index.
The MemTable serves as a write cache and fast lookup layer, maintaining
the latest version of each key in memory.

KEY RESPONSIBILITIES:
- Store latest entry for each key in memory
- Provide O(1) lookup for recent operations
- Buffer writes before flushing to disk
- Track unflushed entries for durability

DESIGN PRINCIPLES:
- Single Responsibility: Only handles in-memory cache operations
- Thread-safe: All operations protected by RWMutex
- Size-bounded: Flushes to disk when reaching capacity

LSM INTEGRATION:
- Write: Update MemTable + append to disk immediately
- Read: Check MemTable first, then scan disk backward
- Delete: Store tombstone in MemTable
- Flush: Periodically consolidate MemTable to optimize memory

CONCURRENCY:
- Reads use RLock for concurrent access
- Writes use Lock for exclusive access
- No blocking between readers

TODO: Future extensions
- LRU eviction policy for memory management
- Bloom filter integration for negative lookups
- MVCC (Multi-Version Concurrency Control) support
- Statistics tracking for cache hit rates
*/

import (
	"fmt"
	"sync"
	"time"
)

// HashMemTable maintains in-memory latest values for hash index entries
// This provides fast lookups without requiring disk I/O for recent operations
type HashMemTable struct {
	// Core storage: key → latest entry mapping
	entries map[string]*HashIndexEntry

	// Concurrency control
	mutex sync.RWMutex

	// Size management
	maxSize     int // Maximum number of entries before flush recommended
	currentSize int // Current number of entries

	// Durability tracking
	// Note: In LSM-style, entries are written to disk immediately,
	// so walBuffer is only for crash recovery optimization
	walBuffer []HashIndexEntry // Unflushed entries (future WAL support)

	// Statistics
	hits   uint64 // Number of cache hits
	misses uint64 // Number of cache misses

	// Lifecycle
	created time.Time
}

// NewHashMemTable creates a new in-memory table
// Parameters:
//   - maxSize: Maximum number of entries before flush recommended
//
// Returns initialized MemTable
func NewHashMemTable(maxSize int) *HashMemTable {
	return &HashMemTable{
		entries:     make(map[string]*HashIndexEntry),
		maxSize:     maxSize,
		currentSize: 0,
		walBuffer:   make([]HashIndexEntry, 0, 1000), // Pre-allocate for WAL
		created:     time.Now(),
	}
}

// Put adds or updates an entry in the MemTable
// If an entry already exists for this key, it's replaced with the new entry
// (following LSM semantics where latest write wins)
//
// Parameters:
//   - entry: The entry to store
//
// Returns error if operation fails
func (mt *HashMemTable) Put(entry *HashIndexEntry) error {
	if entry == nil {
		return fmt.Errorf("cannot put nil entry")
	}

	mt.mutex.Lock()
	defer mt.mutex.Unlock()

	// Check if this key already exists
	existingEntry, exists := mt.entries[entry.KeyValue]

	if exists {
		// Verify new entry is actually newer
		if !entry.IsNewer(existingEntry) {
			// This is an older entry, ignore it (shouldn't happen in normal operation)
			return fmt.Errorf("attempted to insert older entry: seq=%d vs existing=%d",
				entry.Sequence, existingEntry.Sequence)
		}
		// Replace with newer entry (no size change)
		mt.entries[entry.KeyValue] = entry
	} else {
		// New key, increment size
		mt.entries[entry.KeyValue] = entry
		mt.currentSize++
	}

	// Add to WAL buffer for durability (future feature)
	mt.walBuffer = append(mt.walBuffer, *entry)

	return nil
}

// Get retrieves the latest entry for a key
// Returns the entry if found, or nil if not in MemTable
//
// Parameters:
//   - key: The key to look up
//
// Returns entry and boolean indicating if found
func (mt *HashMemTable) Get(key string) (*HashIndexEntry, bool) {
	mt.mutex.RLock()
	defer mt.mutex.RUnlock()

	entry, found := mt.entries[key]

	// Update statistics
	if found {
		mt.hits++
	} else {
		mt.misses++
	}

	return entry, found
}

// Delete marks a key as deleted by storing a tombstone
// This follows LSM semantics where deletes are writes
//
// Parameters:
//   - key: The key to delete
//   - sequence: Global sequence number for the delete operation
//   - commitSequence: Commit sequence when deletion was committed (0 = uncommitted)
//
// Returns error if operation fails
// PHASE 4: MVCC - Added commitSequence parameter
func (mt *HashMemTable) Delete(key string, sequence uint64, commitSequence uint64) error {
	mt.mutex.Lock()
	defer mt.mutex.Unlock()

	// Create tombstone entry with commit sequence
	tombstone := NewTombstoneEntry(key, sequence, commitSequence)

	// Store tombstone in MemTable
	mt.entries[key] = tombstone

	// Add to WAL buffer
	mt.walBuffer = append(mt.walBuffer, *tombstone)

	// Note: Size doesn't change - we're replacing with tombstone
	// Tombstones will be removed during compaction

	return nil
}

// Contains checks if a key exists in the MemTable
// This is faster than Get when you only need existence check
//
// Parameters:
//   - key: The key to check
//
// Returns true if key exists (including tombstones)
func (mt *HashMemTable) Contains(key string) bool {
	mt.mutex.RLock()
	defer mt.mutex.RUnlock()

	_, exists := mt.entries[key]
	return exists
}

// IsDeleted checks if a key is marked as deleted (tombstone)
//
// Parameters:
//   - key: The key to check
//
// Returns true if key has tombstone, false otherwise
func (mt *HashMemTable) IsDeleted(key string) bool {
	mt.mutex.RLock()
	defer mt.mutex.RUnlock()

	entry, exists := mt.entries[key]
	if !exists {
		return false
	}

	return entry.Deleted
}

// Size returns the current number of entries in MemTable
func (mt *HashMemTable) Size() int {
	mt.mutex.RLock()
	defer mt.mutex.RUnlock()

	return mt.currentSize
}

// IsFull checks if MemTable has reached capacity
// Returns true if a flush is recommended
func (mt *HashMemTable) IsFull() bool {
	mt.mutex.RLock()
	defer mt.mutex.RUnlock()

	return mt.currentSize >= mt.maxSize
}

// Clear removes all entries from the MemTable
// This is typically called after flushing to disk
//
// Returns number of entries cleared
func (mt *HashMemTable) Clear() int {
	mt.mutex.Lock()
	defer mt.mutex.Unlock()

	count := mt.currentSize

	// Clear all data structures
	mt.entries = make(map[string]*HashIndexEntry)
	mt.walBuffer = make([]HashIndexEntry, 0, 1000)
	mt.currentSize = 0

	return count
}

// Snapshot returns a copy of all entries in the MemTable
// This is used for flushing to disk or during compaction
//
// Returns slice of all entries (not in any particular order)
func (mt *HashMemTable) Snapshot() []*HashIndexEntry {
	mt.mutex.RLock()
	defer mt.mutex.RUnlock()

	entries := make([]*HashIndexEntry, 0, mt.currentSize)
	for _, entry := range mt.entries {
		// Make a copy to prevent external modification
		entryCopy := *entry
		entries = append(entries, &entryCopy)
	}

	return entries
}

// GetUnflushedEntries returns entries that haven't been persisted
// This is used for WAL and crash recovery
//
// Returns slice of unflushed entries in write order
func (mt *HashMemTable) GetUnflushedEntries() []HashIndexEntry {
	mt.mutex.RLock()
	defer mt.mutex.RUnlock()

	// Return copy to prevent external modification
	buffer := make([]HashIndexEntry, len(mt.walBuffer))
	copy(buffer, mt.walBuffer)

	return buffer
}

// ClearWALBuffer clears the unflushed entries buffer
// Called after successful persistence to disk
func (mt *HashMemTable) ClearWALBuffer() {
	mt.mutex.Lock()
	defer mt.mutex.Unlock()

	mt.walBuffer = make([]HashIndexEntry, 0, 1000)
}

// Stats returns statistics about MemTable usage
type MemTableStats struct {
	Size           int       // Current number of entries
	MaxSize        int       // Maximum capacity
	Hits           uint64    // Number of cache hits
	Misses         uint64    // Number of cache misses
	HitRate        float64   // Cache hit rate percentage
	Created        time.Time // When MemTable was created
	MemoryUsage    int64     // Approximate memory usage in bytes
	TombstoneCount int       // Number of tombstone entries
}

// GetStats returns current statistics
func (mt *HashMemTable) GetStats() MemTableStats {
	mt.mutex.RLock()
	defer mt.mutex.RUnlock()

	// Calculate hit rate
	total := mt.hits + mt.misses
	hitRate := 0.0
	if total > 0 {
		hitRate = (float64(mt.hits) / float64(total)) * 100.0
	}

	// Count tombstones
	tombstones := 0
	for _, entry := range mt.entries {
		if entry.Deleted {
			tombstones++
		}
	}

	// Estimate memory usage (rough approximation)
	// Each map entry: ~48 bytes overhead + key string + entry struct (~150 bytes)
	avgKeySize := 32 // Average key size estimate
	memUsage := int64(mt.currentSize * (48 + avgKeySize + 150))

	return MemTableStats{
		Size:           mt.currentSize,
		MaxSize:        mt.maxSize,
		Hits:           mt.hits,
		Misses:         mt.misses,
		HitRate:        hitRate,
		Created:        mt.created,
		MemoryUsage:    memUsage,
		TombstoneCount: tombstones,
	}
}

// Merge combines another MemTable into this one
// Used during compaction or recovery operations
// Only keeps newer entries based on sequence numbers
//
// Parameters:
//   - other: Another MemTable to merge from
//
// Returns number of entries merged
func (mt *HashMemTable) Merge(other *HashMemTable) int {
	if other == nil {
		return 0
	}

	mt.mutex.Lock()
	defer mt.mutex.Unlock()

	other.mutex.RLock()
	defer other.mutex.RUnlock()

	merged := 0

	for key, otherEntry := range other.entries {
		existingEntry, exists := mt.entries[key]

		if !exists {
			// New key, add it
			mt.entries[key] = otherEntry
			mt.currentSize++
			merged++
		} else if otherEntry.IsNewer(existingEntry) {
			// Other entry is newer, replace
			mt.entries[key] = otherEntry
			merged++
		}
		// If existing entry is newer, keep it (do nothing)
	}

	return merged
}

// String returns a human-readable representation of MemTable state
func (mt *HashMemTable) String() string {
	stats := mt.GetStats()
	return fmt.Sprintf("MemTable[size=%d/%d hitRate=%.1f%% tombstones=%d mem=%dKB]",
		stats.Size, stats.MaxSize, stats.HitRate, stats.TombstoneCount, stats.MemoryUsage/1024)
}

// TODO: Future extensions
// - Implement LRU eviction policy when memory constrained
// - Add Bloom filter for fast negative lookups
// - Support MVCC with multiple entry versions
// - Add background thread for periodic statistics collection
// - Implement adaptive sizing based on workload patterns
// - Add metrics export for monitoring systems
