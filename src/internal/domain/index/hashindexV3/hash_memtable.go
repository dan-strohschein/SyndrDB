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
	"sync/atomic"
	"time"
	"unsafe"
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

	// WAL buffer cap (0 = no cap; when exceeded, buffer is swapped to bound memory)
	walBufferMaxSize int

	// Durability tracking
	// Note: In LSM-style, entries are written to disk immediately,
	// so walBuffer is only for crash recovery optimization
	walBuffer []HashIndexEntry // Unflushed entries (future WAL support)

	// Statistics (hits/misses updated concurrently by readers; use atomics to avoid data race)
	hits      atomic.Uint64 // Number of cache hits
	misses    atomic.Uint64 // Number of cache misses
	evictions uint64        // Number of entries evicted (only under Lock in evictOldestLocked)
	tombstoneCount int      // Number of tombstone entries (O(1) for GetStats; maintained under Lock)

	// Lifecycle
	created time.Time

	// Activity tracking for time-based compaction
	lastActivity       time.Time // Last Put/Delete operation
	lastEntriesCompact time.Time // Last time entries map was compacted
}

// NewHashMemTable creates a new in-memory table with LRU eviction.
// Parameters:
//   - maxSize: Maximum number of entries before eviction starts
//   - walBufferMaxSize: optional; max WAL buffer length before swap (0 = no cap). When provided and > 0, Put/Delete will swap the buffer when exceeded to bound memory.
//
// Returns initialized MemTable
func NewHashMemTable(maxSize int, walBufferMaxSize ...int) *HashMemTable {
	now := time.Now()
	cap := 0
	if len(walBufferMaxSize) > 0 && walBufferMaxSize[0] > 0 {
		cap = walBufferMaxSize[0]
	}
	return &HashMemTable{
		entries:            make(map[string]*HashIndexEntry),
		lruOrder:           list.New(),
		lruElements:        make(map[string]*list.Element),
		maxSize:            maxSize,
		currentSize:        0,
		walBufferMaxSize:   cap,
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
		// Update tombstone count: replacing existing with new entry
		if existingEntry.Deleted && !entry.Deleted {
			mt.tombstoneCount--
		} else if !existingEntry.Deleted && entry.Deleted {
			mt.tombstoneCount++
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
		if entry.Deleted {
			mt.tombstoneCount++
		}
		elem := mt.lruOrder.PushFront(entry.KeyValue)
		mt.lruElements[entry.KeyValue] = elem
		mt.currentSize++
	}

	// Add to WAL buffer for durability (future feature)
	mt.walBuffer = append(mt.walBuffer, *entry)
	// Bound WAL buffer growth: swap with fresh slice when over cap (Issue 5)
	if mt.walBufferMaxSize > 0 && len(mt.walBuffer) > mt.walBufferMaxSize {
		mt.walBuffer = make([]HashIndexEntry, 0, 1000)
	}

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
		if entry, ok := mt.entries[key]; ok && entry.Deleted {
			mt.tombstoneCount--
		}

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
	entry, found := mt.entries[key]
	mt.mutex.RUnlock()

	// Update statistics outside lock using atomics to avoid data race with concurrent readers
	if found {
		mt.hits.Add(1)
	} else {
		mt.misses.Add(1)
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
	existingEntry, exists := mt.entries[key]

	// Store tombstone in MemTable
	mt.entries[key] = tombstone
	// Update tombstone count: we added a tombstone; if it was new or replaced a non-tombstone, count++
	if !exists {
		mt.tombstoneCount++
	} else if !existingEntry.Deleted {
		mt.tombstoneCount++
	}

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
	// Bound WAL buffer growth: swap with fresh slice when over cap (Issue 5)
	if mt.walBufferMaxSize > 0 && len(mt.walBuffer) > mt.walBufferMaxSize {
		mt.walBuffer = make([]HashIndexEntry, 0, 1000)
	}

	return nil
}

// IterateNonDeleted calls f for each non-deleted entry; iteration stops if f returns false.
// Caller must not hold the MemTable lock. Used by GetAllDocumentIDs and other index APIs.
func (mt *HashMemTable) IterateNonDeleted(f func(key string, entry *HashIndexEntry) bool) {
	mt.mutex.RLock()
	defer mt.mutex.RUnlock()
	for key, entry := range mt.entries {
		if entry.Deleted {
			continue
		}
		if !f(key, entry) {
			return
		}
	}
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
	mt.tombstoneCount = 0

	return count
}

// Snapshot returns a copy of all entries in the MemTable.
// This is used for flushing to disk or during compaction.
// Short critical section: copy keys under RLock, then build entry copies under a second RLock to minimize lock hold.
//
// Returns slice of all entries (not in any particular order)
func (mt *HashMemTable) Snapshot() []*HashIndexEntry {
	// Phase 1: copy keys only (minimal allocation under lock)
	mt.mutex.RLock()
	size := mt.currentSize
	keys := make([]string, 0, size+8) // +8 to avoid realloc if size is slightly off
	for k := range mt.entries {
		keys = append(keys, k)
	}
	mt.mutex.RUnlock()

	// Phase 2: build result; one more RLock to read entries by key
	result := make([]*HashIndexEntry, 0, len(keys))
	mt.mutex.RLock()
	for _, k := range keys {
		if entry := mt.entries[k]; entry != nil {
			entryCopy := *entry
			result = append(result, &entryCopy)
		}
	}
	mt.mutex.RUnlock()
	return result
}

// GetUnflushedEntries returns entries that haven't been persisted.
// This is used for WAL and crash recovery.
// Uses swap under Lock so the critical section is O(1); caller receives ownership of the returned slice.
//
// Returns slice of unflushed entries in write order. Caller must not modify the returned slice.
func (mt *HashMemTable) GetUnflushedEntries() []HashIndexEntry {
	mt.mutex.Lock()
	// Swap with fresh buffer so we hold the lock only for O(1)
	old := mt.walBuffer
	mt.walBuffer = make([]HashIndexEntry, 0, 1000)
	mt.mutex.Unlock()
	return old
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
	// Read atomic stats and O(1) tombstone count (no full iteration)
	hits := mt.hits.Load()
	misses := mt.misses.Load()
	tombstones := mt.tombstoneCount
	currentSize := mt.currentSize
	maxSize := mt.maxSize
	evictions := mt.evictions
	created := mt.created
	walBufferLen := len(mt.walBuffer)
	mt.mutex.RUnlock()

	// Calculate hit rate and memory outside lock
	total := hits + misses
	hitRate := 0.0
	if total > 0 {
		hitRate = (float64(hits) / float64(total)) * 100.0
	}
	avgKeySize := 32 // Average key size estimate
	memUsage := int64(currentSize * (48 + avgKeySize + 150 + 64))

	return MemTableStats{
		Size:           currentSize,
		MaxSize:        maxSize,
		Hits:           hits,
		Misses:         misses,
		Evictions:      evictions,
		HitRate:        hitRate,
		Created:        created,
		MemoryUsage:    memUsage,
		TombstoneCount: tombstones,
		WALBufferSize:  walBufferLen,
	}
}

// Merge combines another MemTable into this one.
// Used during compaction or recovery operations.
// Only keeps newer entries based on sequence numbers.
//
// LOCK ORDERING (canonical by address to avoid deadlock): The MemTable with the
// smaller pointer address is locked first (Lock), then the other (RLock).
// Callers (e.g. compaction) must use the same order when calling Merge(A, B).
//
// Parameters:
//   - other: Another MemTable to merge from
//
// Returns number of entries merged
func (mt *HashMemTable) Merge(other *HashMemTable) int {
	if other == nil {
		return 0
	}

	// Canonical lock order by address to prevent deadlock when A.Merge(B) and B.Merge(A) run concurrently
	mtAddr := uintptr(unsafe.Pointer(mt))
	otherAddr := uintptr(unsafe.Pointer(other))
	if mtAddr == otherAddr {
		return 0
	}
	if mtAddr < otherAddr {
		mt.mutex.Lock()
		defer mt.mutex.Unlock()
		other.mutex.RLock()
		defer other.mutex.RUnlock()
	} else {
		other.mutex.RLock()
		defer other.mutex.RUnlock()
		mt.mutex.Lock()
		defer mt.mutex.Unlock()
	}

	merged := 0

	for key, otherEntry := range other.entries {
		existingEntry, exists := mt.entries[key]

		if !exists {
			// New key: add to entries and to LRU so it can be evicted later
			mt.entries[key] = otherEntry
			if otherEntry.Deleted {
				mt.tombstoneCount++
			}
			elem := mt.lruOrder.PushFront(key)
			mt.lruElements[key] = elem
			mt.currentSize++
			merged++
		} else if otherEntry.IsNewer(existingEntry) {
			// Other entry is newer: replace and promote in LRU
			if existingEntry.Deleted && !otherEntry.Deleted {
				mt.tombstoneCount--
			} else if !existingEntry.Deleted && otherEntry.Deleted {
				mt.tombstoneCount++
			}
			mt.entries[key] = otherEntry
			if elem, ok := mt.lruElements[key]; ok {
				mt.lruOrder.MoveToFront(elem)
			}
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
