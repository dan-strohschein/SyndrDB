package bundle

import (
	"context"
	"sync/atomic"
	"time"

	hashindex "syndrdb/src/internal/domain/index/hashindexV3"
	"syndrdb/src/internal/storage/bundlestore"
)

// StalePageIDFallbackCounter tracks how often the cache-scan fallback is triggered
// due to stale pageID entries in indexes. High values indicate need for index rebuild.
var StalePageIDFallbackCounter atomic.Uint64

// GetStalePageIDFallbackCount returns the current count of stale pageID fallbacks
func GetStalePageIDFallbackCount() uint64 {
	return StalePageIDFallbackCounter.Load()
}

// ResetStalePageIDFallbackCount resets the counter (for testing or periodic reset)
func ResetStalePageIDFallbackCount() {
	StalePageIDFallbackCounter.Store(0)
}

// BufferDiagnostics contains a snapshot of all buffer sizes at a point in time
type BufferDiagnostics struct {
	Timestamp             time.Time                      `json:"timestamp"`
	IndexUpdateBufferSize int                            `json:"indexUpdateBufferSize"`
	MetadataBufferSize    int                            `json:"metadataBufferSize"`
	PageCacheStats        PageCacheDiagnostics           `json:"pageCacheStats"`
	HashIndexStats        []HashIndexDiagnostics         `json:"hashIndexStats"`
	WriteBufferStats      []bundlestore.WriteBufferStats `json:"writeBufferStats"`
	TotalDataFileSize     int64                          `json:"totalDataFileSize"`
}

// PageCacheDiagnostics contains page cache shard statistics
type PageCacheDiagnostics struct {
	TotalPages        int   `json:"totalPages"`
	TotalCOWSnapshots int   `json:"totalCOWSnapshots"`
	TotalFastLookup   int   `json:"totalFastLookup"`
	ShardSizes        []int `json:"shardSizes"`
}

// HashIndexDiagnostics contains MemTable statistics for a single hash index
type HashIndexDiagnostics struct {
	BundleName    string `json:"bundleName"`
	IndexName     string `json:"indexName"`
	EntriesCount  int    `json:"entriesCount"`
	WALBufferSize int    `json:"walBufferSize"`
	MaxSize       int    `json:"maxSize"`
}

// GetBufferDiagnostics returns a snapshot of all buffer sizes for debugging.
// Call this periodically or on-demand to track memory growth patterns.
//
// USAGE:
//
//	diag := service.GetBufferDiagnostics()
//	logger.Infof("Buffer diagnostics: %+v", diag)
//
// THREAD SAFETY: Acquires necessary locks briefly to read sizes.
func (s *BundleService) GetBufferDiagnostics() BufferDiagnostics {
	diag := BufferDiagnostics{
		Timestamp: time.Now(),
	}

	// Get indexUpdateBuffer size
	s.indexUpdateMutex.Lock()
	diag.IndexUpdateBufferSize = len(s.indexUpdateBuffer)
	s.indexUpdateMutex.Unlock()

	// Get metadataUpdateBuffer size
	diag.MetadataBufferSize = int(s.metadataBufferLen.Load())

	// Get page cache statistics from all shards
	diag.PageCacheStats.ShardSizes = make([]int, PageCacheShardCount)
	for i := 0; i < PageCacheShardCount; i++ {
		shard := s.pageShards[i]
		shard.mu.RLock()
		shardSize := len(shard.pages)
		diag.PageCacheStats.ShardSizes[i] = shardSize
		diag.PageCacheStats.TotalPages += shardSize

		shard.mu.RUnlock()

		// Count cowSnapshot entries (sync.Map doesn't have Len())
		// Safe without lock: cowSnapshot is atomic.Pointer[sync.Map], so Load() always
		// returns a consistent *sync.Map. No torn-read risk.
		cowCount := 0
		shard.cowSnapshot.Load().Range(func(_, _ interface{}) bool {
			cowCount++
			return true
		})
		diag.PageCacheStats.TotalCOWSnapshots += cowCount

		// Count fastLookup entries (same: atomic pointer, safe without lock)
		fastCount := 0
		shard.fastLookup.Load().Range(func(_, _ interface{}) bool {
			fastCount++
			return true
		})
		diag.PageCacheStats.TotalFastLookup += fastCount
	}

	// Get hash index MemTable statistics
	for bundleName, bundle := range s.bundleMetadata {
		if bundle.Indexes == nil {
			continue
		}

		for indexName, indexRef := range bundle.Indexes {
			if indexRef.IndexInstance == nil {
				continue
			}
			if indexRef.IndexType != "hash" {
				continue
			}

			hashIndex, ok := indexRef.IndexInstance.(*hashindex.HashIndexV3)
			if !ok {
				continue
			}

			stats := hashIndex.MemTable.GetStats()
			diag.HashIndexStats = append(diag.HashIndexStats, HashIndexDiagnostics{
				BundleName:    bundleName,
				IndexName:     indexName,
				EntriesCount:  stats.Size,
				WALBufferSize: stats.WALBufferSize,
				MaxSize:       stats.MaxSize,
			})
		}
	}

	// Get write buffer statistics (file sizes)
	diag.WriteBufferStats = s.store.GetAllWriteBufferStats()
	for _, wbStats := range diag.WriteBufferStats {
		diag.TotalDataFileSize += wbStats.FileSize
	}

	return diag
}

// LogBufferDiagnostics logs current buffer sizes at INFO level.
// Call this when investigating latency degradation.
func (s *BundleService) LogBufferDiagnostics() {
	diag := s.GetBufferDiagnostics()

	s.logger.Infof("=== BUFFER DIAGNOSTICS at %s ===", diag.Timestamp.Format(time.RFC3339))
	s.logger.Infof("  IndexUpdateBuffer: %d entries", diag.IndexUpdateBufferSize)
	s.logger.Infof("  MetadataBuffer: %d entries", diag.MetadataBufferSize)
	s.logger.Infof("  PageCache: %d pages, %d COW snapshots, %d fastLookup entries",
		diag.PageCacheStats.TotalPages,
		diag.PageCacheStats.TotalCOWSnapshots,
		diag.PageCacheStats.TotalFastLookup)

	// Log write buffer (data file) statistics
	s.logger.Infof("  WriteBuffers: %d files, total size: %.2f MB",
		len(diag.WriteBufferStats), float64(diag.TotalDataFileSize)/(1024*1024))
	for _, wb := range diag.WriteBufferStats {
		s.logger.Infof("    %s: size=%.2f MB, buffer=%d, directWrites=%d",
			wb.FilePath, float64(wb.FileSize)/(1024*1024), wb.BufferLen, wb.DirectWrites)
	}

	for _, idx := range diag.HashIndexStats {
		s.logger.Infof("  HashIndex %s.%s: entries=%d, walBuffer=%d, maxSize=%d",
			idx.BundleName, idx.IndexName, idx.EntriesCount, idx.WALBufferSize, idx.MaxSize)
	}
	s.logger.Infof("=== END BUFFER DIAGNOSTICS ===")
}

// RecordWriteActivity updates the last write activity timestamp.
// Call this on every write operation to reset the idle timer for diagnostics.
// Thread-safe: Uses atomic operation.
func (s *BundleService) RecordWriteActivity() {
	s.lastWriteActivity.Store(time.Now().UnixNano())
}

// RecordActivity updates the last activity timestamp for any server activity.
// Call this on every command execution (read or write) to reset the idle cache flush timer.
// This ensures the server correctly detects idle state during read-only workloads.
// Thread-safe: Uses atomic operation.
func (s *BundleService) RecordActivity() {
	s.lastActivity.Store(time.Now().UnixNano())
}

// startDiagnosticsLogger starts a background goroutine that logs buffer diagnostics
// after 30 seconds of idle (no write activity).
//
// DESIGN:
// - Checks every 5 seconds if there's been 30+ seconds of idle time
// - If idle threshold exceeded, logs diagnostics and resets the timer
// - Does NOT log during active write bursts to avoid log spam
// - Useful for debugging latency degradation after workload ends
//
// THREAD SAFETY: Uses atomic reads for activity timestamp
func (s *BundleService) startDiagnosticsLogger(ctx context.Context) {
	const (
		checkInterval = 5 * time.Second  // How often to check for idle
		idleThreshold = 30 * time.Second // Log after this much idle time
	)

	ticker := time.NewTicker(checkInterval)
	s.logger.Debug("Background buffer diagnostics logger started (30s idle threshold)")

	go func() {
		defer ticker.Stop()
		var lastLoggedForActivity int64 // Track which activity timestamp we last logged for

		for {
			select {
			case <-ctx.Done():
				s.logger.Debug("Background buffer diagnostics logger stopped")
				return
			case <-ticker.C:
				activityNano := s.lastWriteActivity.Load()
				lastActivity := time.Unix(0, activityNano)
				idleTime := time.Since(lastActivity)

				// Log if we've been idle for 30+ seconds AND haven't logged for this activity period
				// This ensures we log once per "burst" of activity after 30s idle
				if idleTime >= idleThreshold && lastLoggedForActivity != activityNano {
					s.LogBufferDiagnostics()
					lastLoggedForActivity = activityNano
				}
			}
		}
	}()
}
