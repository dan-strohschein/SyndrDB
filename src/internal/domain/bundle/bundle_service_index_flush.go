package bundle

import (
	"fmt"
	"strings"
	"sync/atomic"
	"time"

	"syndrdb/src/internal/domain/index/brinindex"
	hashindex "syndrdb/src/internal/domain/index/hashindexV3"
	"syndrdb/src/internal/domain/models"
	"syndrdb/src/pkg/constants"
	"syndrdb/src/pkg/settings"

	"go.uber.org/zap"
)

// scheduleIndexUpdate adds an index update to the deferred update buffer
// This optimizes write performance by batching index updates
// Parameters:
//   - pageID: Physical page number where the document resides (use 0 if unknown, will need update later)
//   - docMetadata: optional [commitSeq, versionSeq]; when len>=2, hash index uses these instead of GetDocument
func (s *BundleService) scheduleIndexUpdate(bundleName, indexName, indexType, operation, documentID string, fieldValue interface{}, pageID uint32, oldValue interface{}, deferred bool, docMetadata ...uint64) {
	// DIAGNOSTICS: Record write activity for idle-based diagnostics logging
	s.RecordWriteActivity()

	update := IndexUpdate{
		BundleName:  bundleName,
		IndexName:   indexName,
		IndexType:   indexType,
		Operation:   operation,
		DocumentID:  documentID,
		FieldValue:  fieldValue,
		PageID:      pageID,
		OldValue:    oldValue,
		Timestamp:   time.Now(),
		AppliedSync: !deferred, // Mark as synchronously applied if not deferred
	}

	// CRITICAL FIX: For hash indexes, update MemTable IMMEDIATELY for read-your-own-writes consistency
	// This ensures LSM semantics where reads always see recent writes via MemTable
	if indexType == "hash" {
		// Get the bundle to access the index
		bundle, exists := s.bundleMetadata[bundleName]
		if exists {
			indexRef, indexExists := bundle.Indexes[indexName]
			if indexExists {
				// Load or get the hash index
				hashIndex, err := s.GetOrLoadHashIndex(bundle, indexName, indexRef)
				if err == nil {
					// Update MemTable synchronously (in-memory operation, very fast)
					// Use fieldValueToIndexKeyString so document FieldValue unwraps to same string as query lookups
					keyValue := fieldValueToIndexKeyString(fieldValue)
					if keyValue == "" || keyValue == "<nil>" {
						keyValue = documentID // Fallback for DocumentID indexes
					}

					// Single-write path: assign sequence once; same entry goes to MemTable and disk
					currentSequence := atomic.LoadUint64(&hashIndex.GlobalSequence)
					if err := constants.CheckUint64Increment(currentSequence, "GlobalSequence"); err != nil {
						s.logger.Warnw("GlobalSequence overflow, skipping hash index update",
							zap.String("bundle", bundleName),
							zap.String("index", indexName),
							zap.Error(err))
					} else {
						sequence := atomic.AddUint64(&hashIndex.GlobalSequence, 1)

						// PHASE 4: MVCC - Get document's version metadata
						var commitSeq, versionSeq uint64
						if len(docMetadata) >= 2 {
							commitSeq, versionSeq = docMetadata[0], docMetadata[1]
						} else if doc, err := s.GetDocument(bundleName, bundle.Database.Name, documentID); err == nil {
							commitSeq = doc.CommitSequence
							versionSeq = doc.VersionSequence
						}

						entry := hashindex.NewHashIndexEntry(keyValue, documentID, pageID, sequence, commitSeq, versionSeq)
						if operation == "delete" {
							entry.Deleted = true
						}
						// Set BucketNum so WriteEntryToDiskOnly (processHashIndexBatch) routes to the correct
						// bucket file; otherwise entry.BucketNum stays 0 and all entries land in bucket 0,
						// causing lookups for other buckets to miss (index appears empty for those keys).
						numBkts := hashIndex.NumBuckets()
						bucketNum, bucketErr := hashindex.ComputeBucketNum(entry.HashValue, numBkts)
						if bucketErr == nil {
							entry.BucketNum = bucketNum
						}

						err = hashIndex.MemTable.Put(entry)
						if err != nil {
							s.logger.Warnw("Failed to update MemTable immediately",
								zap.String("bundle", bundleName),
								zap.String("index", indexName),
								zap.Error(err))
						} else {
							// DIAG: Log scheduled index update with bucket assignment
							s.logger.Warnw("[BUCKET-DIAG] scheduleIndexUpdate: entry queued",
								"key", keyValue,
								"docID", documentID,
								"bucketNum", entry.BucketNum,
								"hashValue", entry.HashValue,
								"index", indexName,
								"operation", operation)
							update.HashEntry = entry // processHashIndexBatch will write this to disk only
							if operation == "insert" {
								s.logger.Debugw("Immediately updated MemTable for key",
									zap.String("key", keyValue),
									zap.String("index", indexName))
							} else {
								s.logger.Debugw("Immediately updated MemTable with tombstone",
									zap.String("key", keyValue),
									zap.String("index", indexName))
							}
						}

						if trimmed, oldSize := hashIndex.TrimMemTableWAL(10000); trimmed {
							s.logger.Debugf("Aggressive MemTable trim: cleared %d WAL entries for %s.%s",
								oldSize, bundleName, indexName)
						}
					}
				} else {
					s.logger.Warnw("Failed to load hash index for immediate MemTable update",
						zap.String("bundle", bundleName),
						zap.String("index", indexName),
						zap.Error(err))
				}
			}
		}
	}

	// CRITICAL FIX: For B-tree indexes, update in-memory cache IMMEDIATELY for read-your-own-writes consistency
	// This ensures PostgreSQL-style semantics where reads always see recent writes via page cache
	// PERFORMANCE: Skip synchronous updates when deferred=true (batch UPDATE operations)
	// Deferred operations will be applied in processBTreeIndexBatch without duplicate checking
	if indexType == "btree" && !deferred {
		// Get the bundle to access the index
		bundle, exists := s.bundleMetadata[bundleName]
		if exists {
			indexRef, indexExists := bundle.Indexes[indexName]
			if indexExists {
				// Load or get the B-tree index
				btreeIndex, err := s.getOrLoadBTreeIndex(bundle, indexName, indexRef)
				if err == nil {
					// Convert field value to bytes for B-tree key
					keyBytes, err := convertValueToBytes(fieldValue)
					if err != nil {
						s.logger.Warnw("Failed to convert field value to bytes for B-tree",
							zap.String("bundle", bundleName),
							zap.String("index", indexName),
							zap.Error(err))
					} else {
						// Measure insert time (PostgreSQL baseline + 15% margin = 500μs target)
						insertStart := time.Now()

						// Attempt insert with retry logic (fixed 1ms backoff)
						var insertErr error
						switch operation {
						case "insert":
							insertErr = btreeIndex.Insert(keyBytes, documentID)
							if insertErr != nil {
								// Retry once after 1ms
								time.Sleep(1 * time.Millisecond)
								insertErr = btreeIndex.Insert(keyBytes, documentID)
								if insertErr != nil {
									s.logger.Warnw("Failed to insert into B-tree after retry",
										zap.String("bundle", bundleName),
										zap.String("index", indexName),
										zap.String("documentID", documentID),
										zap.Error(insertErr))
								}
							}
						case "delete":
							insertErr = btreeIndex.Delete(keyBytes, documentID)
							if insertErr != nil {
								// Retry once after 1ms
								time.Sleep(1 * time.Millisecond)
								insertErr = btreeIndex.Delete(keyBytes, documentID)
								if insertErr != nil {
									s.logger.Warnw("Failed to delete from B-tree after retry",
										zap.String("bundle", bundleName),
										zap.String("index", indexName),
										zap.String("documentID", documentID),
										zap.Error(insertErr))
								}
							}
						}

						insertDuration := time.Since(insertStart)

						// Log performance warning if insert exceeds PostgreSQL baseline + 15% (500μs)
						if insertErr == nil && insertDuration > 500*time.Microsecond {
							s.logger.Warnw("⚠️  B-tree synchronous insert exceeded performance target",
								zap.String("index", indexName),
								zap.String("operation", operation),
								zap.Duration("duration", insertDuration),
								zap.Duration("target", 500*time.Microsecond))
						} else if insertErr == nil {
							s.logger.Debugw("⚡ B-tree synchronous insert completed",
								zap.String("index", indexName),
								zap.String("operation", operation),
								zap.Duration("duration", insertDuration))
						}
					}
				} else {
					s.logger.Warnw("Failed to load B-tree index for immediate insert",
						zap.String("bundle", bundleName),
						zap.String("index", indexName),
						zap.Error(err))
				}
			}
		}
	}

	// BRIN indexes: update min/max in-memory immediately (very cheap O(1) operation)
	if indexType == "brin" {
		bundle, exists := s.bundleMetadata[bundleName]
		if exists {
			indexRef, indexExists := bundle.Indexes[indexName]
			if indexExists {
				if brinIdx, ok := indexRef.IndexInstance.(*brinindex.BRINIndex); ok {
					brinIdx.UpdateRange(update.PageID, fieldValue)
				}
			}
		}
	}

	// Schedule disk persistence (deferred for performance)
	// FIX: Protect indexUpdateBuffer with mutex to prevent data race
	// CONSOLIDATED: Single lock acquisition for append + idle check
	s.indexUpdateMutex.Lock()
	s.indexUpdateBuffer = append(s.indexUpdateBuffer, update)
	bufferLen := len(s.indexUpdateBuffer)
	lastFlush := s.lastIndexFlush
	idleFlushNeeded := bufferLen > 0 && time.Since(lastFlush) >= (s.indexUpdateInterval*5)
	s.indexUpdateMutex.Unlock()

	// Check if we should flush updates to disk
	shouldFlush := bufferLen >= s.indexUpdateBatchSize ||
		time.Since(lastFlush) >= s.indexUpdateInterval ||
		idleFlushNeeded

	// ASYNC: Trigger background flush goroutine (non-blocking)
	// The ADD caller returns immediately after buffer append (~1μs)
	// instead of blocking on index I/O (100ms-3s).
	if shouldFlush {
		select {
		case s.indexFlushTrigger <- struct{}{}:
		default:
			// Flush already pending — skip (coalescing)
		}
	}
}

// scheduleMetadataUpdate adds a metadata update to the deferred update buffer
// This optimizes write performance by batching metadata calculations
// Thread-safe: Protected by metadataUpdateMutex
func (s *BundleService) scheduleMetadataUpdate(bundleName, operation string, value int64) {
	s.metadataUpdateMutex.Lock()
	defer s.metadataUpdateMutex.Unlock()

	update := MetadataUpdate{
		BundleName: bundleName,
		Operation:  operation,
		Value:      value,
		Timestamp:  time.Now(),
	}

	s.metadataUpdateBuffer = append(s.metadataUpdateBuffer, update)
	s.metadataBufferLen.Store(int32(len(s.metadataUpdateBuffer)))

	// PHASE 1 OPTIMIZATION: Track operations for deferred persistence
	s.metadataOperationCount++

	// Check if we should flush metadata updates
	// Release lock before flushing to prevent deadlock (flush will acquire its own lock)
	shouldFlush := len(s.metadataUpdateBuffer) >= s.indexUpdateBatchSize ||
		time.Since(s.lastMetadataFlush) >= s.indexUpdateInterval

	shouldIdleFlush := len(s.metadataUpdateBuffer) > 0 &&
		time.Since(s.lastMetadataFlush) >= (s.indexUpdateInterval*5)

	if shouldFlush || shouldIdleFlush {
		// Must unlock before calling flush to avoid deadlock
		s.metadataUpdateMutex.Unlock()
		s.FlushMetadataUpdates()
		s.metadataUpdateMutex.Lock() // Re-acquire for defer
	}
}

// startIndexFlushLoop runs the background index flusher goroutine.
//
// Design: Two-tier flush to avoid B-tree lock contention with inline Insert:
//   - On trigger: lightweight flush — hash disk writes only (no B-tree page lock contention)
//   - On periodic timer: B-tree FlushDirtyPages (batches dirty pages, brief contention every 2s)
//
// The inline scheduleIndexUpdate already applies B-tree Insert and hash MemTable.Put
// synchronously for read-your-own-writes consistency. This goroutine only handles
// disk persistence, which is safe to defer slightly.
func (s *BundleService) startIndexFlushLoop() {
	go func() {
		defer close(s.indexFlushDone)
		ticker := time.NewTicker(2 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case _, ok := <-s.indexFlushTrigger:
				if !ok {
					return // Channel closed — shutdown
				}
				s.flushIndexUpdatesHashOnly()
			case <-ticker.C:
				s.flushBTreeDirtyPages()
			}
		}
	}()
}

// flushIndexUpdatesHashOnly drains the index update buffer and processes ONLY hash index
// disk writes. B-tree updates are skipped because:
//   - In-memory B-tree is already updated by inline Insert in scheduleIndexUpdate
//   - B-tree FlushDirtyPages contends with inline Insert for page-level locks
//   - Dirty pages are flushed periodically by the timer in startIndexFlushLoop
func (s *BundleService) flushIndexUpdatesHashOnly() {
	s.indexUpdateMutex.Lock()
	if len(s.indexUpdateBuffer) == 0 {
		s.indexUpdateMutex.Unlock()
		return
	}

	// Group updates by bundle and index
	updateGroups := make(map[string]map[string][]IndexUpdate)
	for _, update := range s.indexUpdateBuffer {
		if updateGroups[update.BundleName] == nil {
			updateGroups[update.BundleName] = make(map[string][]IndexUpdate)
		}
		updateGroups[update.BundleName][update.IndexName] = append(
			updateGroups[update.BundleName][update.IndexName], update)
	}

	s.indexUpdateBuffer = s.indexUpdateBuffer[:0]
	s.lastIndexFlush = time.Now()
	s.indexUpdateMutex.Unlock()

	// Process ONLY hash index updates (disk persistence)
	// B-tree is already updated in-memory; disk flush happens via periodic timer
	for bundleName, indexGroups := range updateGroups {
		bundle, exists := s.bundleMetadata[bundleName]
		if !exists {
			continue
		}
		for indexName, updates := range indexGroups {
			indexRef, exists := bundle.Indexes[indexName]
			if !exists {
				continue
			}
			if indexRef.IndexType == "hash" {
				if err := s.processHashIndexBatch(bundle, indexName, indexRef, updates); err != nil {
					s.logger.Errorf("Failed to process hash index batch for %s.%s: %v", bundleName, indexName, err)
				}
			}
			// B-tree: skip — FlushDirtyPages handled by periodic timer
		}
	}
}

// flushBTreeDirtyPages flushes dirty B-tree pages to disk for all loaded indexes.
// Called periodically (every 2s) by the background goroutine.
// Brief contention with inline Insert is acceptable at this frequency.
func (s *BundleService) flushBTreeDirtyPages() {
	for bundleName, bundle := range s.bundleMetadata {
		if bundle.Indexes == nil {
			continue
		}
		for indexName, indexRef := range bundle.Indexes {
			if indexRef.IndexType != "btree" || indexRef.IndexInstance == nil {
				continue
			}
			btreeIndex, err := s.getOrLoadBTreeIndex(bundle, indexName, indexRef)
			if err != nil || btreeIndex == nil {
				continue
			}
			if err := btreeIndex.FlushDirtyPages(); err != nil {
				s.logger.Warnf("Periodic B-tree flush failed for %s.%s: %v", bundleName, indexName, err)
			}
			if err := btreeIndex.PersistMetadata(); err != nil {
				s.logger.Warnf("Periodic B-tree metadata persist failed for %s.%s: %v", bundleName, indexName, err)
			}
		}
	}
}

// flushIndexUpdates processes all pending index updates in a batch
// This significantly improves write performance by reducing I/O operations
// FIX: Now protected by indexUpdateMutex to prevent data race
func (s *BundleService) flushIndexUpdates() {
	s.indexUpdateMutex.Lock()
	if len(s.indexUpdateBuffer) == 0 {
		s.indexUpdateMutex.Unlock()
		return
	}

	startTime := time.Now()
	s.logger.Debugw("Flushing pending index updates",
		zap.Int("count", len(s.indexUpdateBuffer)))

	// Group updates by bundle and index for efficient processing
	updateGroups := make(map[string]map[string][]IndexUpdate)

	for _, update := range s.indexUpdateBuffer {
		if updateGroups[update.BundleName] == nil {
			updateGroups[update.BundleName] = make(map[string][]IndexUpdate)
		}
		updateGroups[update.BundleName][update.IndexName] = append(
			updateGroups[update.BundleName][update.IndexName], update)
	}

	// Clear the buffer and update flush time BEFORE processing (release lock faster)
	s.indexUpdateBuffer = s.indexUpdateBuffer[:0] // Reset slice but keep capacity
	s.lastIndexFlush = time.Now()
	s.indexUpdateMutex.Unlock()

	// Process updates in batches (outside lock to avoid blocking other operations)
	for bundleName, indexGroups := range updateGroups {
		bundle, exists := s.bundleMetadata[bundleName]
		if !exists {
			s.logger.Warnf("Bundle '%s' not found in metadata during index update flush", bundleName)
			continue
		}

		for indexName, updates := range indexGroups {
			indexRef, exists := bundle.Indexes[indexName]
			if !exists {
				s.logger.Warnf("Index '%s' not found in bundle '%s' during flush", indexName, bundleName)
				continue
			}

			// Process updates for this specific index
			err := s.processIndexUpdateBatch(bundle, indexName, indexRef, updates)
			if err != nil {
				s.logger.Errorf("Failed to process index update batch for %s.%s: %v", bundleName, indexName, err)
			}
		}
	}

	flushTime := time.Since(startTime)
	s.logger.Debugw("Index update flush completed",
		zap.Duration("duration", flushTime))
}

// flushIndexUpdatesForBundle flushes only index updates for the given bundle (P4a scoped flush).
// Reduces tail latency by avoiding processing the entire global buffer when only this bundle's
// indexes were touched by the current UPDATE.
// FIX: Now protected by indexUpdateMutex to prevent data race
func (s *BundleService) flushIndexUpdatesForBundle(bundleName string) {
	s.indexUpdateMutex.Lock()
	var match, keep []IndexUpdate
	for _, u := range s.indexUpdateBuffer {
		if u.BundleName == bundleName {
			match = append(match, u)
		} else {
			keep = append(keep, u)
		}
	}
	if len(match) == 0 {
		s.indexUpdateMutex.Unlock()
		return
	}

	// Update buffer and flush time BEFORE processing (release lock faster)
	s.indexUpdateBuffer = keep
	s.lastIndexFlush = time.Now()
	s.indexUpdateMutex.Unlock()

	startTime := time.Now()
	s.logger.Debugw("Flushing scoped index updates for bundle",
		zap.String("bundle", bundleName),
		zap.Int("count", len(match)))

	updateGroups := make(map[string][]IndexUpdate)
	for _, u := range match {
		updateGroups[u.IndexName] = append(updateGroups[u.IndexName], u)
	}
	bundle, exists := s.bundleMetadata[bundleName]
	if !exists {
		s.logger.Warnf("Bundle '%s' not found during scoped index flush", bundleName)
		return
	}
	for indexName, updates := range updateGroups {
		indexRef, ok := bundle.Indexes[indexName]
		if !ok {
			s.logger.Warnf("Index '%s' not found in bundle '%s' during scoped flush", indexName, bundleName)
			continue
		}
		if err := s.processIndexUpdateBatch(bundle, indexName, indexRef, updates); err != nil {
			s.logger.Errorf("Failed to process index update batch for %s.%s: %v", bundleName, indexName, err)
		}
	}
	s.logger.Debugw("Scoped index flush completed",
		zap.String("bundle", bundleName),
		zap.Duration("duration", time.Since(startTime)))
}

// FlushMetadataUpdates processes all pending metadata updates in a batch
// This significantly improves write performance by reducing metadata calculation overhead
// Thread-safe: Protected by metadataUpdateMutex
//
// DUAL PERSISTENCE STRATEGY:
//  1. ALWAYS apply updates to in-memory bundle metadata (consistency)
//  2. Mark affected bundles as dirty (IsDirty = true)
//  3. Persist to disk when EITHER condition is met:
//     a) Bundle is dirty AND flush triggered by time/size thresholds (efficiency)
//     b) Global operation counter >= metadataPersistInterval (safety net)
//
// This approach provides:
// - Single-bundle heavy writes: Immediate persistence after each flush
// - Multi-bundle operations: Batched persistence for performance
// - Safety guarantee: Operation counter ensures eventual persistence
func (s *BundleService) FlushMetadataUpdates() {
	s.metadataUpdateMutex.Lock()
	if len(s.metadataUpdateBuffer) == 0 {
		s.metadataUpdateMutex.Unlock()
		return
	}

	startTime := time.Now()
	bufferSize := len(s.metadataUpdateBuffer)
	s.logger.Debugf("Flushing %d pending metadata updates", bufferSize)

	// Group updates by bundle for efficient processing
	bundleUpdates := make(map[string][]MetadataUpdate)
	for _, update := range s.metadataUpdateBuffer {
		bundleUpdates[update.BundleName] = append(bundleUpdates[update.BundleName], update)
	}

	// Clear buffer and capture state before releasing lock
	s.metadataUpdateBuffer = s.metadataUpdateBuffer[:0]
	s.metadataBufferLen.Store(0)
	s.lastMetadataFlush = time.Now()

	// Release lock before expensive I/O operations
	s.metadataUpdateMutex.Unlock()

	// Process updates for each bundle
	for bundleName, updates := range bundleUpdates {
		bundle, exists := s.bundleMetadata[bundleName]
		if !exists {
			s.logger.Warnf("Bundle '%s' not found in metadata during metadata update flush", bundleName)
			continue
		}

		// Apply all updates for this bundle
		docCountDelta := int64(0)
		for _, update := range updates {
			switch update.Operation {
			case "increment_docs":
				docCountDelta += update.Value
			case "decrement_docs":
				// CRITICAL FIX: Ignore decrement_docs operations to prevent corruption
				// In append-only storage, tombstones are still entries on disk, so TotalDocuments
				// should represent total document entries (including tombstones), not active documents.
				// Active document count is calculated dynamically by filtering tombstones during queries.
				// Decrementing TotalDocuments causes corruption when documents exist that were never counted.
				// docCountDelta -= update.Value // REMOVED: Causes corruption
				// Silently ignore - no logging needed for performance
			}
		}

		// Apply the accumulated changes
		bundle.TotalDocuments += docCountDelta

		// Mark bundle as dirty - needs persistence
		// This flag is cleared only after successful disk write
		bundle.IsDirty = true

		// Recalculate page count if documents changed
		if docCountDelta != 0 {
			// Ensure PageSize is never zero to prevent divide by zero
			// Use consistent PageSize with BundleService and factory defaults
			if bundle.PageSize == 0 {
				bundle.PageSize = s.defaultPageSize // Use service default (4096)
				s.logger.Debugf("Set default PageSize of %d for bundle '%s'", s.defaultPageSize, bundleName)
			}

			// CRITICAL: Proper virtual pagination calculation
			// PageCount = ceil(TotalDocuments / PageSize)
			newPageCount := (bundle.TotalDocuments + int64(bundle.PageSize) - 1) / int64(bundle.PageSize)
			if newPageCount != bundle.PageCount {
				s.logger.Debugf("Updated PageCount for bundle '%s': %d -> %d (TotalDocuments: %d, PageSize: %d)",
					bundleName, bundle.PageCount, newPageCount, bundle.TotalDocuments, bundle.PageSize)
				bundle.PageCount = newPageCount
			}
		}
	}

	// DUAL PERSISTENCE TRIGGERS:
	// Trigger 1: Dirty bundles on flush (efficiency - immediate persistence for active bundles)
	// Trigger 2: Operation counter threshold (safety - eventual persistence for all)
	s.metadataUpdateMutex.Lock()
	shouldPersistToDisk := s.metadataOperationCount >= s.metadataPersistInterval
	currentOperationCount := s.metadataOperationCount
	s.metadataUpdateMutex.Unlock()

	// Collect dirty bundles that need persistence
	var bundlesToPersist []*models.Bundle
	for bundleName := range bundleUpdates {
		bundle, exists := s.bundleMetadata[bundleName]
		if exists && bundle.IsDirty {
			bundlesToPersist = append(bundlesToPersist, bundle)
		}
	}

	s.logger.Debugf("METADATA FLUSH: operationCount=%d, threshold=%d, shouldPersist=%v, dirtyBundles=%d, bufferSize=%d",
		currentOperationCount, s.metadataPersistInterval, shouldPersistToDisk, len(bundlesToPersist), bufferSize)

	// Persist if EITHER dirty bundles exist OR threshold reached
	if len(bundlesToPersist) > 0 || shouldPersistToDisk {
		if shouldPersistToDisk {
			// Threshold reached - collect ALL dirty bundles across entire service
			bundlesToPersist = s.getAllDirtyBundles()
		}

		// Persist all dirty bundles
		successCount := 0
		for _, bundle := range bundlesToPersist {

			err := s.store.UpdateBundleFile(bundle.Database, bundle)
			if err != nil {
				s.logger.Errorf("Failed to persist metadata updates for bundle '%s': %v", bundle.Name, err)
				// TODO: Implement retry queue for failed persistence operations
				// Keep IsDirty = true on failure so next cycle will retry
			} else {
				// Clear dirty flag only on successful persistence
				bundle.IsDirty = false
				successCount++

			}
		}

		// Reset operation counter after persistence
		if shouldPersistToDisk {
			s.metadataUpdateMutex.Lock()
			s.metadataOperationCount = 0
			s.metadataUpdateMutex.Unlock()
			s.logger.Debugf("Performed deferred metadata persistence after %d operations", s.metadataPersistInterval)
		}
	} else {
		s.logger.Debugf("Skipping disk persistence - %d operations remaining until next persist (threshold: %d)",
			s.metadataPersistInterval-currentOperationCount, s.metadataPersistInterval)
	}

	flushTime := time.Since(startTime)
	s.logger.Debugf("Metadata update flush completed in %v", flushTime)
}

// ForceMetadataPersistence forces immediate persistence of all metadata updates to disk
// This should be called during shutdown, explicit flush requests, or before critical operations
// Thread-safe: Uses metadataUpdateMutex for operation count reset
func (s *BundleService) ForceMetadataPersistence() {
	// First flush any pending updates to memory
	s.FlushMetadataUpdates()

	// Now persist ALL dirty bundles regardless of operation count
	// This ensures all metadata is saved during shutdown
	s.logger.Info("Forcing metadata persistence for shutdown")

	// Get all dirty bundles across entire service
	dirtyBundles := s.getAllDirtyBundles()
	if len(dirtyBundles) == 0 {
		s.logger.Info("No dirty bundles to persist")
		return
	}

	s.logger.Infow("Persisting dirty bundles on shutdown",
		"bundleCount", len(dirtyBundles))

	successCount := 0
	for _, bundle := range dirtyBundles {

		err := s.store.UpdateBundleFile(bundle.Database, bundle)
		if err != nil {
			s.logger.Errorf("Failed to force persist bundle metadata: %v", err)
		} else {
			// Clear dirty flag only on success
			bundle.IsDirty = false
			successCount++
		}
	}

	s.logger.Infow("Shutdown metadata persistence complete",
		"attempted", len(dirtyBundles),
		"succeeded", successCount,
		"failed", len(dirtyBundles)-successCount)

	// Reset operation counter
	s.metadataUpdateMutex.Lock()
	s.metadataOperationCount = 0
	s.metadataUpdateMutex.Unlock()
}

// getAllDirtyBundles returns all bundles with IsDirty = true across all databases
// Thread-safe: Only reads bundle metadata, no lock needed (bundles accessed through factory)
// TODO: Consider adding a dirty bundle tracking map for O(1) access instead of O(n) scan
func (s *BundleService) getAllDirtyBundles() []*models.Bundle {
	var dirtyBundles []*models.Bundle

	// Iterate through all bundles in metadata map
	for _, bundle := range s.bundleMetadata {
		if bundle.IsDirty {
			dirtyBundles = append(dirtyBundles, bundle)
		}
	}

	return dirtyBundles
}

// trackOperationForBulkDetection tracks write operations to detect bulk scenarios
// Returns true if WAL should be bypassed due to bulk mode detection
func (s *BundleService) trackOperationForBulkDetection() bool {
	// Get global settings for WAL bulk operation configuration
	globalSettings := settings.GetSettings()

	// Skip tracking if bulk operation detection is disabled
	if !globalSettings.BulkOperationDetection {
		return false
	}

	now := time.Now()
	s.operationCount++

	// Check if we're in a new time window (1 second)
	windowDuration := now.Sub(s.operationWindow)
	if windowDuration >= time.Second {
		// Calculate operations per second in the previous window
		opsPerSecond := float64(s.operationCount) / windowDuration.Seconds()

		// Check if we should enter or exit bulk mode
		if opsPerSecond >= float64(s.bulkThresholdOpsPerSec) {
			if !s.bulkModeEnabled {
				s.bulkModeEnabled = true
				s.logger.Debugf("Entering bulk mode - detected %.1f ops/sec (threshold: %d)",
					opsPerSecond, s.bulkThresholdOpsPerSec)
			}
		} else {
			if s.bulkModeEnabled {
				s.bulkModeEnabled = false
				s.logger.Debugf("Exiting bulk mode - detected %.1f ops/sec (threshold: %d)",
					opsPerSecond, s.bulkThresholdOpsPerSec)

				// CRITICAL: Flush all buffers when exiting bulk mode
				// This ensures that any pending operations are persisted to disk
				if err := s.FlushAllBuffers(); err != nil {
					s.logger.Errorf("BULK END: Failed to flush buffers: %v", err)
				} else {
					s.logger.Debugf("BULK END: Successfully flushed all pending operations")
				}
			}
		}

		// Reset counters for new window
		s.operationCount = 0
		s.operationWindow = now
	}

	// Return true if WAL should be disabled due to bulk mode
	return s.bulkModeEnabled && globalSettings.WALDisableForBulkOps
}

// ShouldBypassWAL returns true if WAL should be bypassed for the current operation
// This method should be called by external services before WAL operations
func (s *BundleService) ShouldBypassWAL() bool {
	return s.trackOperationForBulkDetection()
}

// GetBulkModeStatus returns the current bulk mode status for monitoring
func (s *BundleService) GetBulkModeStatus() (bool, int, float64) {
	globalSettings := settings.GetSettings()
	if !globalSettings.BulkOperationDetection {
		return false, 0, 0
	}

	// Calculate current operations per second
	windowDuration := time.Since(s.operationWindow)
	var opsPerSecond float64
	if windowDuration > 0 {
		opsPerSecond = float64(s.operationCount) / windowDuration.Seconds()
	}

	return s.bulkModeEnabled, s.operationCount, opsPerSecond
}

// FlushAllIndexesToDisk forces all loaded hash and BTree indexes to flush their memtables to disk
// This ensures durability even for indexes that don't have pending updates in the buffer
// CRITICAL for test reliability and data consistency after bulk operations
func (s *BundleService) FlushAllIndexesToDisk() error {
	var errors []error
	flushedCount := 0

	// Iterate through all bundles and flush their loaded indexes
	for bundleName, bundle := range s.bundleMetadata {
		if bundle.Indexes == nil {
			continue
		}

		for indexName, indexRef := range bundle.Indexes {
			if indexRef.IndexInstance == nil {
				continue // Index not loaded in memory, skip
			}

			switch indexRef.IndexType {
			case "hash":
				// Flush hash index V3 (LSM-style)
				if hashIndex, ok := indexRef.IndexInstance.(*hashindex.HashIndexV3); ok {
					if err := hashIndex.Flush(); err != nil {
						// Skip closed indexes - they were already flushed when closed
						if strings.Contains(err.Error(), "index is closed") {
							s.logger.Debugf("Skipping flush for closed hash index '%s' in bundle '%s'", indexName, bundleName)
							continue
						}
						errorMsg := fmt.Sprintf("failed to flush hash index '%s' in bundle '%s': %v", indexName, bundleName, err)
						s.logger.Warnf(errorMsg)
						errors = append(errors, fmt.Errorf("%s", errorMsg))
					} else {
						flushedCount++
						s.logger.Debugf("Flushed hash index '%s' in bundle '%s' to disk", indexName, bundleName)
					}
				}

			case "btree":
				// Flush BTree index if it has a Flush method
				if btreeIndex, ok := indexRef.IndexInstance.(interface{ Flush() error }); ok {
					if err := btreeIndex.Flush(); err != nil {
						// Skip closed indexes - they were already flushed when closed
						if strings.Contains(err.Error(), "index is closed") {
							s.logger.Debugf("Skipping flush for closed BTree index '%s' in bundle '%s'", indexName, bundleName)
							continue
						}
						errorMsg := fmt.Sprintf("failed to flush BTree index '%s' in bundle '%s': %v", indexName, bundleName, err)
						s.logger.Warnf(errorMsg)
						errors = append(errors, fmt.Errorf("%s", errorMsg))
					} else {
						flushedCount++
						s.logger.Debugf("Flushed BTree index '%s' in bundle '%s' to disk", indexName, bundleName)
					}
				}
			}
		}
	}

	if len(errors) > 0 {
		return fmt.Errorf("failed to flush %d of %d indexes: %v", len(errors), flushedCount+len(errors), errors)
	}

	if flushedCount > 0 {
		s.logger.Debugf("Successfully flushed %d indexes to disk", flushedCount)
	}

	return nil
}

// FlushAllBuffers forces immediate flush of all pending operations to disk
// This should be called at the end of bulk operations to ensure data persistence
func (s *BundleService) FlushAllBuffers() error {

	var errors []error

	// 1. Flush index updates first (they may affect metadata)
	// FIX: Use mutex to safely check buffer length
	s.indexUpdateMutex.Lock()
	hasIndexUpdates := len(s.indexUpdateBuffer) > 0
	s.indexUpdateMutex.Unlock()
	if hasIndexUpdates {
		s.flushIndexUpdates()
	}

	// 2. CRITICAL: Flush all loaded indexes to ensure memtables are persisted
	// This is essential for test reliability and durability after document operations
	if err := s.FlushAllIndexesToDisk(); err != nil {
		s.logger.Warnf("Failed to flush all indexes to disk: %v", err)
		errors = append(errors, err)
	}

	// 3. Force metadata persistence regardless of thresholds
	if s.metadataBufferLen.Load() > 0 {
		s.ForceMetadataPersistence()
	}

	// 4. Sync any file system buffers
	// Note: Individual stores should handle their own sync operations
	if err := s.store.FlushAllWriteBuffers(); err != nil {
		errors = append(errors, err)
	}

	// 5. Log completion
	if len(errors) > 0 {
		s.logger.Errorf("FLUSH: Completed with %d errors", len(errors))
		return fmt.Errorf("flush completed with %d errors", len(errors))
	}

	return nil
}

// IsDocumentBuffered checks if a document is currently in the write buffer
func (s *BundleService) IsDocumentBuffered(bundleName string, docID string) bool {
	return s.store.IsDocumentBuffered(bundleName, docID)
}

// MarkDocumentDiscarded marks a document as discarded (for rollback)
func (s *BundleService) MarkDocumentDiscarded(bundleName string, docID string) error {
	return s.store.MarkDocumentDiscarded(bundleName, docID)
}

// GetDiscardedDocuments returns list of document IDs marked as discarded in a bundle
func (s *BundleService) GetDiscardedDocuments(bundleName string) []string {
	return s.store.GetDiscardedDocuments(bundleName)
}

// ClearDiscardedDocuments removes document IDs from the discarded set after successful deletion
func (s *BundleService) ClearDiscardedDocuments(bundleName string, docIDs []string) {
	s.store.ClearDiscardedDocuments(bundleName, docIDs)
}

// DeleteDiscardedDocuments physically deletes documents that were marked as discarded
// This is called after FlushAllBuffers during rollback cleanup
func (s *BundleService) DeleteDiscardedDocuments(database *models.Database, bundleName string, docIDs []string) error {
	if len(docIDs) == 0 {
		return nil
	}

	bundle, err := s.GetBundleByName(database, bundleName)
	if err != nil {
		return fmt.Errorf("failed to get bundle %s: %w", bundleName, err)
	}

	// Create a minimal DocumentDeleteCommand for internal use
	docCommand := &models.DocumentDeleteCommand{
		BundleName: bundleName,
	}

	// Use internal deletion without metadata updates (we'll batch those)
	return s.deleteDocumentsInternal(bundle, docCommand, docIDs, true, nil)
}

// processIndexUpdateBatch handles a batch of updates for a specific index
func (s *BundleService) processIndexUpdateBatch(bundle *models.Bundle, indexName string, indexRef models.IndexReference, updates []IndexUpdate) error {
	switch indexRef.IndexType {
	case "hash":
		return s.processHashIndexBatch(bundle, indexName, indexRef, updates)
	case "btree":
		return s.processBTreeIndexBatch(bundle, indexName, indexRef, updates)
	default:
		return fmt.Errorf("unsupported index type: %s", indexRef.IndexType)
	}
}

// processHashIndexBatch optimizes hash index updates by batching operations.
// Single-write path: when update.HashEntry is set, we write that entry to disk only (no second MemTable/sequence).
// Otherwise we fall back to Put/Delete for backward compatibility.
func (s *BundleService) processHashIndexBatch(bundle *models.Bundle, indexName string, indexRef models.IndexReference, updates []IndexUpdate) error {
	hashIndex, err := s.GetOrLoadHashIndex(bundle, indexName, indexRef)
	if err != nil {
		return fmt.Errorf("failed to load hash index: %w", err)
	}

	// CRITICAL FIX: Deduplicate updates to prevent processing the same document multiple times
	seen := make(map[string]bool)
	deduplicatedUpdates := make([]IndexUpdate, 0, len(updates))

	for _, update := range updates {
		key := update.Operation + ":" + update.DocumentID
		if !seen[key] {
			seen[key] = true
			deduplicatedUpdates = append(deduplicatedUpdates, update)
		} else {
			s.logger.Debugf("Skipping duplicate update for document '%s' in index '%s'", update.DocumentID, indexName)
		}
	}

	// Process all deduplicated updates for disk persistence
	// NOTE: MemTable was already updated synchronously in scheduleIndexUpdate()
	// We use Put/Delete here which will update MemTable again (idempotent) and persist to disk
	successCount := 0
	errorCount := 0

	for _, update := range deduplicatedUpdates {
		if update.HashEntry != nil {
			// Single-write path: same entry already applied to MemTable; write to disk only
			err := hashIndex.WriteEntryToDiskOnly(update.HashEntry)
			if err != nil {
				errorCount++
				s.logger.Warnf("Failed to persist entry to disk (doc '%s') in index V3 '%s': %v",
					update.DocumentID, indexName, err)
			} else {
				successCount++
			}
			continue
		}

		// Fallback: no pre-built entry (e.g. legacy or overflow skip)
		// Use fieldValueToIndexKeyString so document FieldValue unwraps to same string as query lookups
		keyValue := fieldValueToIndexKeyString(update.FieldValue)
		if keyValue == "" || keyValue == "<nil>" {
			keyValue = update.DocumentID
		}

		switch update.Operation {
		case "insert":
			var commitSeq, versionSeq uint64
			if bundle.Database != nil {
				if doc, err := s.GetDocument(update.BundleName, bundle.Database.Name, update.DocumentID); err == nil {
					commitSeq = doc.CommitSequence
					versionSeq = doc.VersionSequence
				}
			}
			err := hashIndex.Put(keyValue, update.DocumentID, update.PageID, commitSeq, versionSeq)
			if err != nil {
				errorCount++
				s.logger.Warnf("Failed to persist insert to disk for key '%s' (doc '%s') in index V3 '%s': %v",
					keyValue, update.DocumentID, indexName, err)
			} else {
				successCount++
			}

		case "delete":
			var commitSeq uint64
			if bundle.Database != nil {
				if doc, err := s.GetDocument(update.BundleName, bundle.Database.Name, update.DocumentID); err == nil {
					commitSeq = doc.CommitSequence
				}
			}
			_, err := hashIndex.Delete(keyValue, commitSeq)
			if err != nil {
				errorCount++
				s.logger.Warnf("Failed to persist delete to disk for key '%s' (doc '%s') in index V3 '%s': %v",
					keyValue, update.DocumentID, indexName, err)
			} else {
				successCount++
			}
		}
	}

	// Log batch processing results
	if errorCount > 0 {
		s.logger.Warnf("Hash index batch processing completed: %d successes, %d errors for index '%s'",
			successCount, errorCount, indexName)
	} else {
		s.logger.Debugf("Hash index batch processing completed: %d disk operations successful for index '%s'",
			successCount, indexName)
	}

	// Flush disk writes
	if err := hashIndex.Flush(); err != nil {
		s.logger.Warnf("Failed to flush hash index V3 '%s' to disk: %v", indexName, err)
	}

	return nil
}

// processBTreeIndexBatch optimizes BTree index updates by batching operations
//
// IMPORTANT: B-tree inserts now happen SYNCHRONOUSLY in scheduleIndexUpdate() for
// immediate visibility (read-your-own-writes consistency). This batch processing
// ONLY handles async disk persistence via Flush().
//
// The in-memory page cache is already updated during the synchronous insert in
// scheduleIndexUpdate(), so this function primarily ensures dirty pages are
// written to disk for durability.
//
// TODO: Potential optimization - track which keys are already in cache and skip
// redundant inserts during batch processing since they were applied synchronously.
//
// TODO: Expose cache metrics for production monitoring:
//   - Cache hit ratio (should be >95% with synchronous inserts)
//   - Dirty page ratio (should stay <80% with auto-flush)
//   - Sync insert latency (target: <500μs, PostgreSQL baseline + 15%)
func (s *BundleService) processBTreeIndexBatch(bundle *models.Bundle, indexName string, indexRef models.IndexReference, updates []IndexUpdate) error {
	btreeIndex, err := s.getOrLoadBTreeIndex(bundle, indexName, indexRef)
	if err != nil {
		// If index file doesn't exist, log warning and skip updates gracefully
		// This can happen during index initialization or if file was deleted
		s.logger.Warnf("Cannot process BTree index updates for '%s': %v", indexName, err)
		return nil // Don't propagate error - just skip these updates
	}

	if btreeIndex == nil {
		s.logger.Warnf("BTree index '%s' is nil, skipping updates", indexName)
		return nil
	}

	// OPTIMIZATION: Deduplicate updates to avoid redundant inserts
	// Since synchronous inserts in scheduleIndexUpdate() already applied these updates
	// to the in-memory page cache, we only need to apply updates that aren't cached.
	// This eliminates ~10ms of redundant work per batch (50x performance improvement).
	skippedCount := 0
	appliedCount := 0

	for _, update := range updates {
		switch update.Operation {
		case "insert":
			keyBytes, err := convertValueToBytes(update.FieldValue)
			if err != nil {
				s.logger.Warnf("Failed to convert field value to bytes: %v", err)
				continue
			}

			// PERFORMANCE: Skip dedup check if update was deferred (not applied synchronously)
			// Deferred updates haven't been applied yet, so no need to check for duplicates
			if update.AppliedSync {
				// Check if key+docID already exists in cache (applied synchronously)
				// Search() is fast (~100μs) because it checks PageManager cache first
				existingDocs, searchErr := btreeIndex.Search(keyBytes)
				if searchErr == nil {
					// Check if this specific docID is already present
					alreadyExists := false
					for _, existingDocID := range existingDocs {
						if existingDocID == update.DocumentID {
							alreadyExists = true
							break
						}
					}

					if alreadyExists {
						// Skip redundant insert - already applied synchronously
						skippedCount++
						s.logger.Debugf("Skipped duplicate insert for key in index '%s' (already in cache)", indexName)
						continue
					}
				}
			}

			// Apply insert (directly for deferred, or if not in cache for sync)
			err = btreeIndex.Insert(keyBytes, update.DocumentID)
			if err != nil {
				s.logger.Warnf("Failed to insert into BTree index '%s': %v", indexName, err)
			} else {
				appliedCount++
			}

		case "delete":
			keyBytes, err := convertValueToBytes(update.FieldValue)
			if err != nil {
				s.logger.Warnf("Failed to convert field value to bytes: %v", err)
				continue
			}

			// For deferred deletes, apply directly (no dedup needed for deletes)
			// For sync deletes, they were already applied but we need to persist
			if update.AppliedSync {
				// Already applied synchronously - skip to avoid double-delete errors
				skippedCount++
				continue
			}

			err = btreeIndex.Delete(keyBytes, update.DocumentID)
			if err != nil {
				s.logger.Warnf("Failed to delete from BTree index '%s': %v", indexName, err)
			} else {
				appliedCount++
			}

		case "update":
			// Delete old value
			if update.OldValue != nil {
				oldKeyBytes, err := convertValueToBytes(update.OldValue)
				if err == nil {
					btreeIndex.Delete(oldKeyBytes, update.DocumentID)
				}
			}

			// Insert new value
			keyBytes, err := convertValueToBytes(update.FieldValue)
			if err != nil {
				s.logger.Warnf("Failed to convert field value to bytes: %v", err)
				continue
			}

			err = btreeIndex.Insert(keyBytes, update.DocumentID)
			if err != nil {
				s.logger.Warnf("Failed to update BTree index '%s': %v", indexName, err)
			} else {
				appliedCount++
			}
		}
	}

	// Log deduplication statistics
	if skippedCount > 0 {
		s.logger.Debugw("B-tree batch deduplication summary",
			zap.String("index", indexName),
			zap.Int("skipped", skippedCount),
			zap.Int("applied", appliedCount),
			zap.Int("total", len(updates)))
	}

	// Flush dirty pages to disk with single fdatasync for durability
	// This uses batched mode: writes all pages without sync, then one fdatasync at the end
	// Much faster than individual fsync per page (8 pages = 1 sync vs 8 syncs)
	flushStart := time.Now()
	if err := btreeIndex.FlushDirtyPages(); err != nil {
		s.logger.Warnw("Failed to flush B-tree index to disk",
			zap.String("index", indexName),
			zap.Error(err))
	} else {
		flushDuration := time.Since(flushStart)
		if flushDuration > 10*time.Millisecond {
			s.logger.Warnw("⚠️  B-tree disk flush took longer than expected",
				zap.String("index", indexName),
				zap.Duration("duration", flushDuration))
		} else {
			s.logger.Debugw("✓ B-tree disk flush completed",
				zap.String("index", indexName),
				zap.Duration("duration", flushDuration))
		}
	}

	// Persist metadata once per batch (Insert/Delete no longer do it on the hot path)
	if err := btreeIndex.PersistMetadata(); err != nil {
		s.logger.Warnw("Failed to persist B-tree index metadata",
			zap.String("index", indexName),
			zap.Error(err))
	}

	return nil
}

// ForceFlushIndexUpdates is the per-command flush entrypoint.
// Processes hash index disk writes synchronously (fast for 1-few items).
// B-tree FlushDirtyPages is intentionally deferred to the periodic background timer
// to avoid page-lock contention with inline btreeIndex.Insert() calls.
func (s *BundleService) ForceFlushIndexUpdates() {
	s.indexUpdateMutex.Lock()
	indexCount := len(s.indexUpdateBuffer)
	s.indexUpdateMutex.Unlock()
	if indexCount > 0 {
		s.flushIndexUpdatesHashOnly()
	}
	if s.metadataBufferLen.Load() > 0 {
		s.FlushMetadataUpdates()
	}
}

// ForceFlushIndexUpdatesFull is the COMMIT/shutdown flush entrypoint.
// Processes ALL pending index updates including B-tree FlushDirtyPages
// for full durability before response.
func (s *BundleService) ForceFlushIndexUpdatesFull() {
	s.forceFlushIndexUpdatesFull()
}

// forceFlushIndexUpdatesFull ensures all pending updates are fully processed to disk.
// This should be called on COMMIT, shutdown, and bulk operation completion.
func (s *BundleService) forceFlushIndexUpdatesFull() {
	s.indexUpdateMutex.Lock()
	indexCount := len(s.indexUpdateBuffer)
	s.indexUpdateMutex.Unlock()
	if indexCount > 0 {
		s.logger.Debugf("Force flushing %d pending index updates (full)", indexCount)
		s.flushIndexUpdates()
	}
	if s.metadataBufferLen.Load() > 0 {
		s.logger.Debugf("Force flushing %d pending metadata updates", s.metadataBufferLen.Load())
		s.FlushMetadataUpdates()
	}
}
