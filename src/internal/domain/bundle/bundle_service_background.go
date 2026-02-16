package bundle

import (
	"container/list"
	"context"
	"sync"
	"time"

	"syndrdb/src/internal/domain/models"
	"syndrdb/src/pkg/settings"

	hashindex "syndrdb/src/internal/domain/index/hashindexV3"
)

// startCOWSnapshotCleaner starts a background goroutine that periodically sweeps
// and removes stale COW snapshots from all page cache shards.
// This prevents stale snapshot accumulation and eliminates hot-path cleanup overhead.
//
// PERFORMANCE FIX: Root cause of 17ms → 128ms latency degradation (7.5x) in write-heavy workloads.
// Go's sync.Map.Delete() doesn't free memory - it marks entries as "expunged" but they remain
// in internal structures. The old cleanStaleCOWSnapshots() approach used Delete() every 5s,
// which added tombstones without removing them. Over time, Load() operations had to scan
// through accumulated expunged entries, causing severe performance degradation.
//
// COMBINED APPROACH (replaces cleanStaleCOWSnapshots):
// - Rebuilds sync.Map every 30 seconds with only fresh entries
// - Eliminates expunged tombstones and cleans stale entries in one operation
// - Similar to fastLookup compaction but for cowSnapshot cache
// - 30s interval balances freshness (5s staleness threshold) with compaction overhead
func (s *BundleService) startCOWSnapshotCompactor(ctx context.Context) {
	ticker := time.NewTicker(30 * time.Second) // Compact every 30 seconds
	s.logger.Debug("Background COW snapshot compactor started (30s interval)")

	go func() {
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				s.logger.Debug("Background COW snapshot compactor stopped")
				return
			case <-ticker.C:
				s.compactAllCOWSnapshots()
			}
		}
	}()
}

// compactAllCOWSnapshots compacts the cowSnapshot sync.Map in all page cache shards.
// This combines cleanup (remove stale entries) and compaction (remove expunged tombstones)
// in one operation by rebuilding each sync.Map with only fresh, non-stale entries.
//
// PERFORMANCE FIX: Root cause of 17ms → 128ms latency degradation in write-heavy workloads.
// The old cleanStaleCOWSnapshots() approach used Delete() every 5s, which marked entries as
// "expunged" but didn't free memory. This caused Load() operations to scan through accumulated
// tombstones, degrading performance on subsequent runs.
//
// COMBINED APPROACH (replaces cleanStaleCOWSnapshots):
// - Rebuilds sync.Map with only fresh entries (age ≤ GroupBySnapshotStalenessMs)
// - Eliminates expunged tombstones from previous Delete() operations
// - Cleans stale entries and compacts in one operation
// - Runs every 30 seconds (vs 5s for old cleaner) to balance freshness and overhead
func (s *BundleService) compactAllCOWSnapshots() {
	stalenessMs := settings.GetSettings().GroupBySnapshotStalenessMs
	now := time.Now().UnixMilli()
	totalBefore := 0
	totalAfter := 0

	for _, shard := range s.pageShards {
		before, after := shard.compactCOWSnapshot(int64(stalenessMs), now)
		totalBefore += before
		totalAfter += after
	}

	if totalBefore > 0 {
		removed := totalBefore - totalAfter
		s.logger.Debugf("COW snapshot compactor: compacted %d shards, %d entries → %d entries (%d removed, threshold: %dms)",
			PageCacheShardCount, totalBefore, totalAfter, removed, stalenessMs)
	}
}

// startFastLookupCompactor starts a background goroutine that periodically compacts
// the fastLookup sync.Map in all page cache shards.
//
// PERFORMANCE FIX: Root cause of remaining 50ms latency degradation after first run
// Go's sync.Map.Delete() doesn't free memory - it marks entries as "expunged" but they
// remain in internal structures. After many page evictions, the sync.Map accumulates
// cruft that degrades Load() performance:
// - First run: Fresh sync.Map, fast Load() operations
// - Second run: Accumulated expunged entries cause slower Load() operations
// - Solution: Periodically recreate sync.Map with only current entries
func (s *BundleService) startFastLookupCompactor(ctx context.Context) {
	ticker := time.NewTicker(60 * time.Second) // Compact every 60 seconds
	s.logger.Debug("Background fastLookup compactor started (60s interval)")

	go func() {
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				s.logger.Debug("Background fastLookup compactor stopped")
				return
			case <-ticker.C:
				s.compactAllFastLookups()
			}
		}
	}()
}

// compactAllFastLookups compacts the fastLookup sync.Map and regular maps in all page cache shards.
// This recreates each sync.Map with only current entries, removing accumulated cruft
// from deleted entries that slow down Load() operations.
// Also compacts regular Go maps (pages, lruElements) to reclaim bucket memory from deletions.
func (s *BundleService) compactAllFastLookups() {
	totalPages := 0
	regularMapEntries := 0
	for _, shard := range s.pageShards {
		shard.compactFastLookup()
		// Also compact regular maps to reclaim bucket memory
		regularMapEntries += shard.compactRegularMaps()
		// Count pages for logging (requires lock, but we just released it)
		shard.mu.RLock()
		totalPages += len(shard.pages)
		shard.mu.RUnlock()
	}
	s.logger.Debugf("FastLookup compactor: compacted %d shards (%d total pages, %d regular map entries)",
		PageCacheShardCount, totalPages, regularMapEntries)
}

// CompactAllCaches compacts all caches in BundleService and the underlying store.
// PERFORMANCE FIX: Go's map delete() doesn't shrink the bucket array. After many
// connect/disconnect cycles with writes, caches accumulate empty bucket slots that
// degrade iteration and memory performance. Call this periodically (every 60s) to
// reclaim memory and restore lookup speed.
// Returns total entries across all caches after compaction.
func (s *BundleService) CompactAllCaches() int {
	total := 0

	// Compact page shards (both sync.Map and regular maps)
	for _, shard := range s.pageShards {
		shard.compactFastLookup()
		total += shard.compactRegularMaps()
	}

	// Compact underlying store's caches
	total += s.store.CompactAllCaches()

	return total
}

// FlushAllDocumentCaches aggressively clears ALL document-holding caches.
// This is more aggressive than CompactAllCaches - it completely removes cached
// document data rather than just compacting map structures.
//
// PERFORMANCE FIX: When all clients disconnect between test runs, document objects
// accumulate in caches (COW snapshots, page cache, file cache). While map compaction
// reclaims bucket memory, the actual document data remains. This method provides
// a "fresh start" equivalent to server restart, preventing latency degradation
// across consecutive test runs.
//
// This method should be called when all clients have disconnected to prepare for
// the next session cycle with clean caches.
func (s *BundleService) FlushAllDocumentCaches() {
	s.logger.Info("Flushing all document caches for clean state")

	// DIAGNOSTIC: Log cache sizes before flushing to track what's accumulating
	totalPageCacheEntries := 0
	for _, shard := range s.pageShards {
		shard.mu.RLock()
		totalPageCacheEntries += len(shard.pages)
		shard.mu.RUnlock()
	}
	loadedIndexCount := 0
	if s.loadedIndexes != nil {
		s.loadedIndexes.ForEach(func(bundleName string, indexes map[string]interface{}) bool {
			loadedIndexCount += len(indexes)
			return true
		})
	}
	s.logger.Infof("Cache state before flush: pageCache=%d entries, loadedIndexes=%d, bundleMetadata=%d bundles",
		totalPageCacheEntries, loadedIndexCount, len(s.bundleMetadata))

	// Clear page cache shards - these hold DocumentPage objects with document data
	for _, shard := range s.pageShards {
		shard.mu.Lock()
		// Clear authoritative page cache
		shard.pages = make(map[string]*models.DocumentPage)
		// Clear LRU tracking
		shard.lruOrder = list.New()
		shard.lruElements = make(map[string]*list.Element)
		// Clear fast lookup sync.Map by atomically swapping in a fresh one
		shard.fastLookup.Store(&sync.Map{})
		// Clear COW snapshot cache by atomically swapping in a fresh one
		shard.cowSnapshot.Store(&sync.Map{})
		shard.mu.Unlock()
	}

	// Clear document-to-page mapping cache (documentPageCache)
	// This cache maps documentID -> pageID and grows during document operations
	if s.documentPageCache != nil {
		s.documentPageCache.Flush()
	}

	// PERFORMANCE FIX: Compact MemTables instead of closing indexes
	// Closing indexes causes expensive reload on next access (open files, read headers,
	// restore sequences). Instead, we compact the MemTables which clears accumulated
	// data while keeping indexes hot in memory.
	//
	// The compactAllHashIndexMemTables function:
	// - Clears walBuffer (main memory hog during writes)
	// - Optionally rebuilds entries map to reclaim memory
	// - Keeps file handles open for fast subsequent access
	s.logger.Info("Compacting hash index MemTables instead of closing indexes")
	s.compactAllHashIndexMemTables()

	// For indexes loaded via EnsureHashIndexV3Loaded (bundleMetadata path),
	// we also need to compact their MemTables. Since they might not be tracked
	// in bundleMetadata's iteration, just force a full compaction.
	memtablesCompacted := 0
	if s.loadedIndexes != nil {
		s.loadedIndexes.ForEach(func(bundleName string, indexes map[string]interface{}) bool {
			for indexName, idx := range indexes {
				if hashIdx, ok := idx.(*hashindex.HashIndexV3); ok {
					// Force compaction with entries cleanup
					walCleared, _, err := hashIdx.CompactMemTableSafe(true)
					if err != nil {
						s.logger.Warnf("Error compacting MemTable for '%s.%s': %v", bundleName, indexName, err)
					} else if walCleared > 0 {
						memtablesCompacted++
					}
				}
			}
			return true
		})
	}
	if memtablesCompacted > 0 {
		s.logger.Infof("Compacted %d index MemTables during cache flush", memtablesCompacted)
	}

	// For bundleMetadata path indexes, compact but don't close
	for _, bundle := range s.bundleMetadata {
		if bundle.Indexes != nil {
			for indexName := range bundle.Indexes {
				indexRef := bundle.Indexes[indexName]
				if indexRef.IndexInstance != nil {
					if hashIdx, ok := indexRef.IndexInstance.(*hashindex.HashIndexV3); ok {
						hashIdx.CompactMemTableSafe(true)
					}
				}
			}
		}
	}

	// Flush underlying store's document caches
	s.store.FlushAllDocumentCaches()

	// Call external flush callback if registered (e.g., JOIN hash table cache)
	if s.onCacheFlush != nil {
		s.onCacheFlush()
	}

	s.logger.Info("All document caches flushed")
}

// startMemTableCompactor starts a background goroutine that periodically compacts
// all loaded hash index MemTables to prevent unbounded memory growth.
//
// PERFORMANCE FIX: Root cause of sustained write workload latency degradation.
// Hash index MemTable's walBuffer grows unboundedly during continuous writes:
// - Each Put() appends to walBuffer for WAL tracking
// - During sustained writes, walBuffer can grow to millions of entries
// - This causes memory pressure and GC overhead
//
// SOLUTION:
// - Every 30 seconds, iterate all loaded hash indexes
// - Call CompactMemTableSafe() which atomically swaps walBuffer with fresh empty buffer
// - Writers blocked only for microseconds during swap
// - Old buffer is GC'd after swap completes
//
// THREAD SAFETY:
// - Uses CompactMemTableSafe() which only holds lock briefly for O(1) swap
// - Writers continue immediately on new buffer
// - No risk of data loss - disk writes happen during normal Put() operations
func (s *BundleService) startMemTableCompactor(ctx context.Context) {
	ticker := time.NewTicker(30 * time.Second) // Compact every 30 seconds
	s.logger.Debug("Background hash index MemTable compactor started (30s interval)")

	go func() {
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				s.logger.Debug("Background hash index MemTable compactor stopped")
				return
			case <-ticker.C:
				s.compactAllHashIndexMemTables()
			}
		}
	}()
}

// compactAllHashIndexMemTables compacts the MemTable in all loaded hash indexes.
// This clears walBuffer and optionally rebuilds entries map to reclaim memory.
//
// DESIGN:
// - Always compacts walBuffer (main memory hog during sustained writes)
// - Entries map compaction triggered by time interval (60s) or idle timeout (30s)
// - Time-based compaction ensures memory reclamation without impacting active bursts
//
// ENTRIES COMPACTION TRIGGERS:
// - Interval (60s): Force entries compaction every 60s to prevent unbounded growth
// - Idle (30s): Compact entries after 30s of idle (burst has ended)
const (
	entriesCompactIntervalSec = 60 // Force entries compaction every 60s
	entriesCompactIdleSec     = 30 // Compact entries after 30s of idle
)

func (s *BundleService) compactAllHashIndexMemTables() {
	totalWALCleared := 0
	indexesCompacted := 0
	entriesCompacted := 0

	// Iterate through all bundles and their loaded hash indexes
	for bundleName, bundle := range s.bundleMetadata {
		if bundle.Indexes == nil {
			continue
		}

		for indexName, indexRef := range bundle.Indexes {
			if indexRef.IndexInstance == nil {
				continue // Index not loaded in memory
			}

			if indexRef.IndexType != "hash" {
				continue // Only compact hash indexes
			}

			// Cast to HashIndexV3
			hashIndex, ok := indexRef.IndexInstance.(*hashindex.HashIndexV3)
			if !ok {
				continue
			}

			// Check if entries map needs compaction (60s interval OR 30s idle)
			shouldCompactEntries, reason := hashIndex.NeedsEntriesCompaction(
				entriesCompactIntervalSec,
				entriesCompactIdleSec,
			)

			// Perform safe compaction
			// - Always clears walBuffer
			// - Conditionally rebuilds entries map based on time/idle triggers
			walCleared, entriesCount, err := hashIndex.CompactMemTableSafe(shouldCompactEntries)
			if err != nil {
				// Log but continue - don't let one index failure stop others
				s.logger.Warnf("Failed to compact MemTable for index '%s.%s': %v",
					bundleName, indexName, err)
				continue
			}

			if walCleared > 0 {
				totalWALCleared += walCleared
				indexesCompacted++
			}

			if shouldCompactEntries {
				entriesCompacted++
				s.logger.Debugf("Entries compaction triggered for '%s.%s': %s (entries=%d)",
					bundleName, indexName, reason, entriesCount)
			}
		}
	}

	if totalWALCleared > 0 || entriesCompacted > 0 {
		s.logger.Debugf("MemTable compactor: compacted %d indexes, cleared %d WAL entries, entries compacted=%d",
			indexesCompacted, totalWALCleared, entriesCompacted)
	}
}

// startIdleBufferFlusher starts a background goroutine that flushes all WriteBuffers
// after a period of idle (no write activity).
//
// ROOT CAUSE FIX: WriteBuffer.flushTimeout (100ms) only triggers on the NEXT write.
// If no writes come after data is buffered, the buffer stays full forever.
// This caused stuck buffers in `order_items` (26KB) and `cart_items` (12KB) that
// never flushed because those bundles weren't being written to anymore.
//
// DESIGN:
// - Checks every 2 seconds if there's been 5+ seconds of idle time
// - If idle threshold exceeded, flushes all WriteBuffers
// - Uses lastWriteActivity from RecordWriteActivity() to detect idle
// - Safe to flush at any time (idempotent operation)
//
// THREAD SAFETY: Uses atomic reads for activity timestamp
func (s *BundleService) startIdleBufferFlusher(ctx context.Context) {
	const (
		checkInterval = 2 * time.Second // How often to check for idle
		idleThreshold = 5 * time.Second // Flush after this much idle time
	)

	ticker := time.NewTicker(checkInterval)
	s.logger.Debug("Background idle buffer flusher started (5s idle threshold)")

	go func() {
		defer ticker.Stop()
		var lastFlushTime time.Time

		for {
			select {
			case <-ctx.Done():
				s.logger.Debug("Background idle buffer flusher stopped")
				return
			case <-ticker.C:
				lastActivity := time.Unix(0, s.lastWriteActivity.Load())
				idleTime := time.Since(lastActivity)

				// Flush if we've been idle for 5+ seconds AND haven't flushed since last activity
				if idleTime >= idleThreshold && lastFlushTime.Before(lastActivity) {
					if err := s.store.FlushAllWriteBuffers(); err != nil {
						s.logger.Warnf("Background idle buffer flush failed: %v", err)
					} else {
						s.logger.Debug("Background idle buffer flush completed")
					}
					lastFlushTime = time.Now()
				}
			}
		}
	}()
}

// startIdleCacheFlusher starts a background goroutine that flushes all document caches
// when the server has been idle for 30 seconds.
//
// PERFORMANCE FIX: Test run isolation
// When running consecutive test runs, document data accumulates in caches causing
// latency degradation (e.g., 15ms → 46ms → 66ms across runs). This was originally
// triggered when all sessions disconnected, but that approach had race conditions
// with rapid reconnects causing inconsistent results (sometimes 20ms, sometimes 60ms).
//
// DESIGN:
// - Checks every 5 seconds if there's been 30+ seconds of idle time
// - If idle threshold exceeded AND haven't flushed since last activity, flush all caches
// - Uses lastActivity to detect true server idle (reads AND writes, not just writes)
// - Closes indexes properly to release file handles
// - Safe to flush at any time (next access reloads from disk)
//
// THREAD SAFETY: Uses atomic reads for activity timestamp
func (s *BundleService) startIdleCacheFlusher(ctx context.Context) {
	const checkInterval = 5 * time.Second // How often to check for idle

	ticker := time.NewTicker(checkInterval)
	s.logger.Info("Background idle cache flusher started (30s idle threshold)")

	go func() {
		defer ticker.Stop()

		for {
			select {
			case <-ctx.Done():
				s.logger.Debug("Background idle cache flusher stopped")
				return
			case <-ticker.C:
				// Use lastActivity (reads + writes) for cache flush detection,
				// not lastWriteActivity which only tracks writes.
				// This prevents false idle detection during read-only workloads.
				lastActivityTime := time.Unix(0, s.lastActivity.Load())
				lastFlush := time.Unix(0, s.lastCacheFlushTime.Load())
				idleTimeNs := time.Since(lastActivityTime).Nanoseconds()

				// Flush if:
				// 1. We've been idle for 30+ seconds, AND
				// 2. We haven't already flushed since the last activity
				if idleTimeNs >= s.idleCacheFlushThresholdNs && lastFlush.Before(lastActivityTime) {
					s.logger.Info("Server idle for 30s - flushing all document caches for clean state")
					s.FlushAllDocumentCaches()
					s.lastCacheFlushTime.Store(time.Now().UnixNano())
				}
			}
		}
	}()
}
