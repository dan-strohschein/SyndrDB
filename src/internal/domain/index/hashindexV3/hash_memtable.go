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
	"container/list"
	"fmt"
	"sync"
	"time"
)

// HashMemTable maintains in-memory latest values for hash index entries
// This provides fast lookups without requiring disk I/O for recent operations
type HashMemTable struct {
	// Core storage: key → latest entry mapping
	entries map[string]*HashIndexEntry

	// LRU eviction tracking
	lruOrder    *list.List               // Doubly-linked list for O(1) LRU eviction
	lruElements map[string]*list.Element // key → list element for O(1) promotion

	// Concurrency control
	mutex sync.RWMutex

	// Size management
	maxSize     int // Maximum number of entries before eviction starts
	currentSize int // Current number of entries

	// Durability tracking
	// Note: In LSM-style, entries are written to disk immediately,
	// so walBuffer is only for crash recovery optimization
	walBuffer []HashIndexEntry // Unflushed entries (future WAL support)

	// Statistics
	hits      uint64 // Number of cache hits
	misses    uint64 // Number of cache misses
	evictions uint64 // Number of entries evicted

	// Lifecycle
	created time.Time

	// Activity tracking for time-based compaction
	lastActivity       time.Time // Last Put/Delete operation
	lastEntriesCompact time.Time // Last time entries map was compacted
}

// NewHashMemTable creates a new in-memory table with LRU eviction
// Parameters:
//   - maxSize: Maximum number of entries before eviction starts
//
// Returns initialized MemTable
func NewHashMemTable(maxSize int) *HashMemTable {
	now := time.Now()
	return &HashMemTable{
		entries:            make(map[string]*HashIndexEntry),
		lruOrder:           list.New(),
		lruElements:        make(map[string]*list.Element),
		maxSize:            maxSize,
		currentSize:        0,
		walBuffer:          make([]HashIndexEntry, 0, 1000), // Pre-allocate for WAL
		created:            now,
		lastActivity:       now,
		lastEntriesCompact: now,
	}
}

// Put adds or updates an entry in the MemTable
// If an entry already exists for this key, it's replaced with the new entry
// (following LSM semantics where latest write wins)
// Automatically evicts oldest entries when maxSize is exceeded.
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

	// Track activity for idle-based compaction
	mt.lastActivity = time.Now()

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
		// Move to front of LRU (most recently used)
		if elem, ok := mt.lruElements[entry.KeyValue]; ok {
			mt.lruOrder.MoveToFront(elem)
		}
	} else {
		// Evict oldest entries if we're at capacity
		// Evict 10% of entries to avoid frequent evictions
		if mt.currentSize >= mt.maxSize {
			evictCount := mt.maxSize / 10
			if evictCount < 1 {
				evictCount = 1
			}
			mt.evictOldestLocked(evictCount)
		}

		// New key, add to entries and LRU
		mt.entries[entry.KeyValue] = entry
		elem := mt.lruOrder.PushFront(entry.KeyValue)
		mt.lruElements[entry.KeyValue] = elem
		mt.currentSize++
	}

	// Add to WAL buffer for durability (future feature)
	mt.walBuffer = append(mt.walBuffer, *entry)

	return nil
}

// evictOldestLocked removes the N oldest entries from the MemTable.
// Caller must hold mutex.Lock().
// Entries are safe to evict because they're already persisted to disk.
func (mt *HashMemTable) evictOldestLocked(count int) {
	for i := 0; i < count && mt.lruOrder.Len() > 0; i++ {
		// Get oldest entry (back of list)
		oldest := mt.lruOrder.Back()
		if oldest == nil {
			break
		}
		key := oldest.Value.(string)

		// Remove from all data structures
		delete(mt.entries, key)
		delete(mt.lruElements, key)
		mt.lruOrder.Remove(oldest)
		mt.currentSize--
		mt.evictions++
	}
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

	// Track activity for idle-based compaction
	mt.lastActivity = time.Now()

	// Create tombstone entry with commit sequence
	tombstone := NewTombstoneEntry(key, sequence, commitSequence)

	// Check if key already exists to update LRU correctly
	_, exists := mt.entries[key]

	// Store tombstone in MemTable
	mt.entries[key] = tombstone

	if exists {
		// Move to front of LRU (most recently used)
		if elem, ok := mt.lruElements[key]; ok {
			mt.lruOrder.MoveToFront(elem)
		}
	} else {
		// Evict oldest entries if we're at capacity
		if mt.currentSize >= mt.maxSize {
			evictCount := mt.maxSize / 10
			if evictCount < 1 {
				evictCount = 1
			}
			mt.evictOldestLocked(evictCount)
		}

		// New key, add to LRU
		elem := mt.lruOrder.PushFront(key)
		mt.lruElements[key] = elem
		mt.currentSize++
	}

	// Add to WAL buffer
	mt.walBuffer = append(mt.walBuffer, *tombstone)

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

	// Clear all data structures including LRU
	mt.entries = make(map[string]*HashIndexEntry)
	mt.lruOrder = list.New()
	mt.lruElements = make(map[string]*list.Element)
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

// CompactSafe performs a concurrent-safe compaction of the MemTable.
// This method is designed to be called during high-concurrency writes without
// blocking writers for extended periods or causing crashes.
//
// ALGORITHM (Double-Buffer Swap):
// 1. Take brief write lock
// 2. Swap walBuffer with fresh empty buffer (O(1) pointer swap)
// 3. Optionally compact entries map if it exceeds threshold
// 4. Release lock - writers immediately resume on fresh buffers
// 5. Return old buffers for offline processing (caller can discard or persist)
//
// THREAD SAFETY:
// - Writers blocked only for ~microseconds during swap
// - No data loss: entries map retains latest values for lookups
// - walBuffer cleared safely: old buffer returned to caller
//
// PERFORMANCE:
// - O(1) swap operation under lock
// - Zero allocation under lock (pre-allocated buffers)
// - Concurrent reads continue during compaction
//
// Parameters:
//   - compactEntries: If true, also rebuild entries map to reclaim memory
//   - entryThreshold: Only compact entries if size exceeds this (0 = always compact)
//
// Returns:
//   - oldWALBuffer: The old WAL buffer entries (can be discarded or persisted)
//   - entriesCompacted: Number of entries in map after compaction
//   - error: Any error during compaction
func (mt *HashMemTable) CompactSafe(compactEntries bool, entryThreshold int) ([]HashIndexEntry, int, error) {
	mt.mutex.Lock()

	// Swap WAL buffer atomically - this is the critical memory-saving operation
	oldWALBuffer := mt.walBuffer
	mt.walBuffer = make([]HashIndexEntry, 0, 1000) // Fresh pre-allocated buffer

	entriesCount := mt.currentSize

	// Optionally compact entries map to reclaim map bucket memory
	// Only if requested AND size exceeds threshold
	if compactEntries && (entryThreshold == 0 || mt.currentSize > entryThreshold) {
		// Create new map with exact size (no over-allocation)
		// This reclaims memory from deleted map buckets
		newEntries := make(map[string]*HashIndexEntry, mt.currentSize)
		for k, v := range mt.entries {
			newEntries[k] = v
		}
		mt.entries = newEntries
		entriesCount = len(newEntries)
		mt.lastEntriesCompact = time.Now()
	}

	mt.mutex.Unlock()

	// Return old buffer - caller can discard (GC will reclaim) or process
	return oldWALBuffer, entriesCount, nil
}

// TrimWALBuffer safely reduces the WAL buffer size without full compaction.
// This is a lighter-weight operation for when only the walBuffer is causing
// memory pressure, not the entries map.
//
// ALGORITHM:
// 1. Take brief write lock
// 2. If buffer exceeds threshold, replace with smaller pre-allocated buffer
// 3. Release lock
//
// Parameters:
//   - threshold: Only trim if buffer length exceeds this value
//
// Returns:
//   - trimmed: True if buffer was trimmed
//   - oldSize: Size of buffer before trim
func (mt *HashMemTable) TrimWALBuffer(threshold int) (bool, int) {
	mt.mutex.Lock()
	defer mt.mutex.Unlock()

	oldSize := len(mt.walBuffer)
	if oldSize <= threshold {
		return false, oldSize
	}

	// Replace with fresh smaller buffer - old buffer will be GC'd
	mt.walBuffer = make([]HashIndexEntry, 0, 1000)
	return true, oldSize
}

// NeedsEntriesCompaction checks if the entries map should be compacted
// based on time interval or idle timeout.
//
// COMPACTION TRIGGERS:
// - Time interval (60s): Forces periodic compaction to reclaim memory
// - Idle timeout (30s): Compacts when no writes for 30s to avoid mid-burst compaction
//
// CONCURRENCY: Thread-safe, uses RLock for minimal blocking
//
// Parameters:
//   - intervalSeconds: Force compaction after this many seconds since last compaction
//   - idleSeconds: Compact if no activity for this many seconds
//
// Returns:
//   - needsCompaction: True if compaction should be triggered
//   - reason: Description of why compaction is needed (for logging)
func (mt *HashMemTable) NeedsEntriesCompaction(intervalSeconds, idleSeconds int) (bool, string) {
	mt.mutex.RLock()
	defer mt.mutex.RUnlock()

	now := time.Now()
	sinceLastCompact := now.Sub(mt.lastEntriesCompact)
	sinceLastActivity := now.Sub(mt.lastActivity)

	// Check time interval trigger (60s default)
	if sinceLastCompact >= time.Duration(intervalSeconds)*time.Second {
		return true, fmt.Sprintf("interval exceeded (%.0fs since last compact)", sinceLastCompact.Seconds())
	}

	// Check idle timeout trigger (30s default)
	// Only compact if there's been at least some activity since creation
	// and we've been idle for the threshold
	if sinceLastActivity >= time.Duration(idleSeconds)*time.Second &&
		mt.lastActivity.After(mt.created) {
		return true, fmt.Sprintf("idle timeout (%.0fs since last activity)", sinceLastActivity.Seconds())
	}

	return false, ""
}

// Stats returns statistics about MemTable usage
type MemTableStats struct {
	Size           int       // Current number of entries
	MaxSize        int       // Maximum capacity
	Hits           uint64    // Number of cache hits
	Misses         uint64    // Number of cache misses
	Evictions      uint64    // Number of entries evicted by LRU
	HitRate        float64   // Cache hit rate percentage
	Created        time.Time // When MemTable was created
	MemoryUsage    int64     // Approximate memory usage in bytes
	TombstoneCount int       // Number of tombstone entries
	WALBufferSize  int       // Current size of WAL buffer (diagnostic)
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
	// LRU overhead: ~64 bytes per entry (list element + map entry)
	avgKeySize := 32 // Average key size estimate
	memUsage := int64(mt.currentSize * (48 + avgKeySize + 150 + 64))

	return MemTableStats{
		Size:           mt.currentSize,
		MaxSize:        mt.maxSize,
		Hits:           mt.hits,
		Misses:         mt.misses,
		Evictions:      mt.evictions,
		HitRate:        hitRate,
		Created:        mt.created,
		MemoryUsage:    memUsage,
		TombstoneCount: tombstones,
		WALBufferSize:  len(mt.walBuffer),
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
