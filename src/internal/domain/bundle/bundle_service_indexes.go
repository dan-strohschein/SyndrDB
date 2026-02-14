package bundle

import (
	"encoding/binary"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	indexutils "syndrdb/src/internal/domain/index"
	"syndrdb/src/internal/domain/models"
	"syndrdb/src/internal/registry"
	syndrQL "syndrdb/src/internal/syndrQL"
	"syndrdb/src/pkg/common/conversion"
	"syndrdb/src/pkg/common/helpers"
	"syndrdb/src/pkg/settings"

	"syndrdb/src/internal/domain/index/brinindex"
	"syndrdb/src/internal/domain/index/btreeindexV2"
	hashindex "syndrdb/src/internal/domain/index/hashindexV3"
)

func (s *BundleService) AddIndexToBundle(database *models.Database, bundle *models.Bundle, indexCommand *models.CreateIndexCommand) error {
	//args := settings.GetSettings()

	// Check if the bundle exists
	if bundle == nil {
		s.logger.Errorf("Bundle is nil, cannot add index")
		return fmt.Errorf("bundle '%s' is nil, cannot add index", indexCommand.BundleName)
	}

	bundle, err := s.GetBundleByName(database, bundle.Name)
	if err != nil {
		return fmt.Errorf("bundle '%s' not found", indexCommand.BundleName)
	}

	// Invalidate plan cache before index creation (schema change)
	// This ensures queries use fresh plans after index becomes available
	s.invalidatePlanCacheForBundle(bundle.Name)

	// Create the index based on the command type

	switch indexCommand.IndexType {
	case "btree":

		err1 := CreateBTreeIndex(s, bundle, indexCommand)

		return err1

		// Record the created index
		// bundle.Indexes[indexCommand.IndexName] = indexRef
		// err = s.store.UpdateBundleFile(bundle.Database, bundle)
		// if err != nil {
		// 	s.logger.Errorf("Failed to update bundle file after creating index: %v", err)
		// 	return fmt.Errorf("failed to update bundle file after creating index: %w", err)
		// }
	case "hash":
		err1 := CreateHashIndex(s, bundle, indexCommand)
		return err1

	case "brin":
		err1 := CreateBRINIndex(s, bundle, indexCommand)
		return err1

	default:
		return fmt.Errorf("unknown index type: %s", indexCommand.IndexType)
	}
}

// getBRINIndexForField looks up a BRIN index on the given field in the bundle's indexes.
func (s *BundleService) getBRINIndexForField(bundle *models.Bundle, fieldName string) *brinindex.BRINIndex {
	if bundle.Indexes == nil {
		return nil
	}
	for _, indexRef := range bundle.Indexes {
		if indexRef.IndexType == "brin" && len(indexRef.Fields) > 0 && indexRef.Fields[0].Name == fieldName {
			if brin, ok := indexRef.IndexInstance.(*brinindex.BRINIndex); ok {
				return brin
			}
		}
	}
	return nil
}

// runBTreeRollback undoes B-tree index updates when UpdateDocumentsBatch fails.
// For each op: Delete(newKey, documentID) then Insert(oldKey) to restore pre-update state.
// Logs and continues on individual op failures since we are already in an error path.
func (s *BundleService) runBTreeRollback(ops []btreeRollbackOp) {
	for _, op := range ops {
		if err := op.idx.Delete(op.newKey, op.documentID); err != nil {
			s.logger.Warnf("rollback: B-tree Delete failed for doc %s: %v", op.documentID, err)
		}
		if err := op.idx.Insert(op.oldKey, op.documentID); err != nil {
			s.logger.Warnf("rollback: B-tree Insert failed for doc %s: %v", op.documentID, err)
		}
	}
}

// GetOrLoadHashIndex retrieves or loads a hash index instance for the specified bundle and index name
// This function follows the Single Responsibility Principle by handling only hash index loading
// Parameters:
//   - bundle: The bundle containing the index reference
//   - indexName: The name of the index to load
//   - indexRef: The index reference containing metadata
//
// Returns:
//   - *hashindex.HashIndexV3: The loaded hash index instance (V3 LSM-style)
//   - error: Any error that occurred during loading
func (s *BundleService) GetOrLoadHashIndex(bundle *models.Bundle, indexName string, indexRef models.IndexReference) (*hashindex.HashIndexV3, error) {
	// CRITICAL FIX: Use dedicated in-memory cache instead of bundle.Indexes[].IndexInstance
	// The IndexInstance field has `json:"-"` tag so it's never persisted to disk
	// This caused the cache to be empty on every operation, forcing disk loads

	// Check the sharded cache first (fast path with read lock per shard)
	if cachedIndex, found := s.loadedIndexes.Get(bundle.Name, indexName); found {
		if hashIndex, ok := cachedIndex.(*hashindex.HashIndexV3); ok {
			s.logger.Debugf("✓ Hash index V3 '%s' CACHE HIT (already in memory)", indexName)
			return hashIndex, nil
		}
	}

	s.logger.Debugf("⚠️  Hash index V3 '%s' CACHE MISS - loading from disk for bundle '%s'", indexName, bundle.Name)

	// === OLD V2 IMPLEMENTATION (Commented out) ===
	// args := settings.GetSettings()
	// databasePath := helpers.GetDatabaseFolderPath(bundle.Database.Name)
	// indexFilePath := fmt.Sprintf("%s%s_%s.hidx", databasePath, bundle.Name, indexRef.HashIndexField.FieldName)
	// hashIndex, err := hashindexV2.OpenHashIndex(indexFilePath, args.Debug, s.logger)

	// === NEW V3 IMPLEMENTATION (Sprint 5: LSM-style) ===
	databasePath := helpers.GetDatabaseFolderPath(bundle.Database.Name)
	indexesPath := filepath.Join(databasePath, bundle.Name, "indexes")

	walBufferMax := s.settings.Storage.MemTableWALBufferMaxSize
	if walBufferMax <= 0 {
		walBufferMax = 50000 // default to bound memory
	}
	config := hashindex.IndexConfig{
		IndexName:                indexName,
		BundleName:               bundle.Name,
		DatabaseName:             bundle.Database.Name,
		FieldName:                indexRef.HashIndexField.FieldName,
		DataDir:                  indexesPath,
		MaxFileSize:              128 * 1024 * 1024,
		WriteBufferSize:          64 * 1024,
		MemTableMaxSize:          100000,
		MemTableWALBufferMaxSize: walBufferMax,
		CompactionEnabled:        true,
		CompactionMaxFiles:       10,
		Logger:                   s.logger,
	}

	hashIndex, err := hashindex.OpenHashIndexV3(config)
	if err != nil {
		return nil, fmt.Errorf("failed to load hash index V3 '%s' from disk: %w", indexName, err)
	}

	// Store the loaded instance in the sharded cache (thread-safe with GetOrSet)
	s.loadedIndexes.Set(bundle.Name, indexName, hashIndex)

	s.logger.Debugf("✅ Successfully loaded and cached hash index V3 '%s' from disk", indexName)
	return hashIndex, nil
}

// getOrLoadBTreeIndex retrieves or loads a BTree index instance for the specified bundle and index name
// This function follows the Single Responsibility Principle by handling only BTree index loading
// Uses persistent loadedIndexes cache (like hash indexes) to avoid reload overhead
// Parameters:
//   - bundle: The bundle containing the index reference
//   - indexName: The name of the index to load
//   - indexRef: The index reference containing metadata
//
// Returns:
//   - *btreeindexV2.BTreeIndex: The loaded BTree index instance
//   - error: Any error that occurred during loading
func (s *BundleService) getOrLoadBTreeIndex(bundle *models.Bundle, indexName string, indexRef models.IndexReference) (*btreeindexV2.BTreeIndex, error) {
	// FIX: Use proper double-checked locking pattern to prevent race condition
	// where multiple goroutines load separate BTreeIndex instances for the same file,
	// causing tree corruption (each instance has its own mutex but shares the file)
	// PERFORMANCE: Uses sharded cache (64 shards) with per-shard locking

	// Use GetOrLoad which provides atomic load-or-create semantics per shard
	// This prevents multiple goroutines from loading the same index simultaneously
	loaded, err := s.loadedIndexes.GetOrLoad(bundle.Name, indexName, func() (interface{}, error) {
		s.logger.Debugf("⚠️  BTree index '%s' CACHE MISS - loading from disk for bundle '%s'", indexName, bundle.Name)

		// TODO This is should be in a separate centrailized location so we can alter folders later
		// Construct proper B-tree index file path
		// Format: /data_dir/<database>/<bundle>/indexes/btree/<btree-index-file-name>.btidx
		databasePath := helpers.GetDatabaseFolderPath(bundle.Database.Name)
		btreeIndexesPath := filepath.Join(databasePath, bundle.Name, "indexes", "btree")
		indexFilePath := filepath.Join(btreeIndexesPath, fmt.Sprintf("%s.btidx", indexName))

		// Check if the index file exists before trying to open it
		if _, err := os.Stat(indexFilePath); os.IsNotExist(err) {
			// Index file doesn't exist - this can happen if:
			// 1. Index was just created but file creation failed
			// 2. Index metadata exists but file was deleted
			// 3. Race condition during index creation
			s.logger.Warnf("BTree index file '%s' does not exist for index '%s', skipping updates", indexFilePath, indexName)
			return nil, fmt.Errorf("index file does not exist: %s (index may still be initializing)", indexFilePath)
		}

		args := settings.GetSettings()
		btreeIndex, err := btreeindexV2.OpenBTreeIndex(indexFilePath, args.Debug, s.logger)
		if err != nil {
			return nil, fmt.Errorf("failed to load BTree index '%s' from disk: %w", indexName, err)
		}

		s.logger.Debugf("✅ Successfully loaded and cached BTree index '%s' from disk", indexName)
		return btreeIndex, nil
	})

	if err != nil {
		return nil, err
	}

	return loaded.(*btreeindexV2.BTreeIndex), nil
}

// GetOrLoadBTreeIndex is a public wrapper for getOrLoadBTreeIndex to support query planner
func (s *BundleService) GetOrLoadBTreeIndex(bundle *models.Bundle, indexName string, indexRef models.IndexReference) (interface{}, error) {
	return s.getOrLoadBTreeIndex(bundle, indexName, indexRef)
}

// GetOrLoadHashIndexInterface is a wrapper to support query planner interface compatibility
func (s *BundleService) GetOrLoadHashIndexInterface(bundle *models.Bundle, indexName string, indexRef models.IndexReference) (interface{}, error) {
	return s.GetOrLoadHashIndex(bundle, indexName, indexRef)
}

// LoadDatabaseIndexes loads all unique constraint B-tree indexes for a database into memory
// This implements PostgreSQL-style in-memory index caching with LRU eviction on idle timeout
// Called automatically on database context switches (connection/USE command)
// PHASE 5: Refactored to use atomic counter and sharded map for concurrent access.
// Parameters:
//   - databaseName: The name of the database to load indexes for
//
// Returns:
//   - error: Any error that occurred during loading or LRU eviction
func (s *BundleService) LoadDatabaseIndexes(databaseName string) error {
	// PHASE 5: Fast path using sharded map - Touch updates timestamp if exists
	if s.loadedDatabases.Touch(databaseName) {
		s.logger.Debugf("Database '%s' indexes already loaded (touch updated access time)", databaseName)
		return nil
	}

	// Database not loaded yet - check again with get to avoid race
	if _, exists := s.loadedDatabases.Get(databaseName); exists {
		s.loadedDatabases.Set(databaseName, time.Now())
		s.logger.Debugf("Database '%s' indexes already loaded (race condition avoided)", databaseName)
		return nil
	}

	// LRU EVICTION: Find databases idle for more than 10 minutes
	idleTimeout := 10 * time.Minute
	evictedDatabases := s.loadedDatabases.ForEachWithEviction(idleTimeout)

	// Evict idle databases and free their memory
	for _, dbName := range evictedDatabases {
		lastAccess, _ := s.loadedDatabases.Get(dbName)
		s.logger.Debugf("📤 Evicting idle database '%s' indexes (idle for %v)", dbName, time.Since(lastAccess))

		// Find all bundles for this database and unload their unique indexes
		// Use ForEach to safely iterate over the sharded cache
		s.loadedIndexes.ForEach(func(bundleName string, indexes map[string]interface{}) bool {
			// TODO: I need to add database info to bundle name or use catalog to map bundle->database
			// For now, we'll unload all indexes for the evicted database (conservative approach)
			for indexName, indexInstance := range indexes {
				if btreeIndex, ok := indexInstance.(*btreeindexV2.BTreeIndex); ok {
					// Check if this is a unique index for the evicted database
					// Close the index to free file handles and memory
					if err := btreeIndex.Close(); err != nil {
						s.logger.Warnf("Failed to close B-tree index '%s' during eviction: %v", indexName, err)
					}
					// PHASE 5: Use atomic subtraction for memory tracking
					meta := btreeIndex.Metadata
					s.currentIndexMemoryUsage.Add(-meta.EstimatedMemorySizeBytes)
					// Delete from sharded cache (ForEach gives us a copy, so we delete separately)
					s.loadedIndexes.DeleteIndex(bundleName, indexName)
					s.logger.Debugf("  Unloaded B-tree index '%s' from bundle '%s' (%d MB freed)",
						indexName, bundleName, meta.EstimatedMemorySizeBytes/(1024*1024))
				}
			}
			return true // continue iteration
		})

		s.loadedDatabases.Delete(dbName)
	}

	// Find all bundles in this database and load unique constraint B-tree indexes
	var totalIndexes int
	var totalMemory int64
	var skippedIndexes int

	// Iterate through all bundles to find ones belonging to this database
	for bundleName, bundle := range s.bundleMetadata {
		if bundle.Database == nil || bundle.Database.Name != databaseName {
			continue
		}

		// Iterate through bundle indexes to find unique B-tree indexes
		for indexName, indexRef := range bundle.Indexes {
			// Only load B-tree indexes with unique constraints
			if indexRef.IndexType != "btree" || !indexRef.BTreeIndexField.IsUnique {
				continue
			}

			// Load the B-tree index to check memory size
			btreeIndex, err := s.getOrLoadBTreeIndex(bundle, indexName, indexRef)
			if err != nil {
				s.logger.Warnf("Failed to load unique B-tree index '%s' for memory check: %v", indexName, err)
				continue
			}

			// Check if we have budget for this index
			meta := btreeIndex.Metadata
			indexSize := meta.EstimatedMemorySizeBytes

			// PHASE 5: Use atomic Load for budget check
			currentUsage := s.currentIndexMemoryUsage.Load()
			if currentUsage+indexSize > s.uniqueIndexMemoryBudgetBytes {
				s.logger.Warnf("⚠️  Memory budget exceeded, skipping B-tree index '%s' (would use %d MB, budget: %d MB used / %d MB total)",
					indexName,
					indexSize/(1024*1024),
					currentUsage/(1024*1024),
					s.uniqueIndexMemoryBudgetBytes/(1024*1024))
				skippedIndexes++

				// Close the index since we won't keep it in memory
				if err := btreeIndex.Close(); err != nil {
					s.logger.Warnf("Failed to close B-tree index '%s': %v", indexName, err)
				}

				// Remove from sharded cache to force disk-based fallback
				s.loadedIndexes.Delete(bundleName, indexName)

				continue
			}

			// PHASE 5: Use atomic addition for memory tracking
			s.currentIndexMemoryUsage.Add(indexSize)
			totalIndexes++
			totalMemory += indexSize
			s.logger.Debugf("  ✓ Loaded unique B-tree index '%s.%s' (%d MB, %d records)",
				bundleName, indexName, indexSize/(1024*1024), meta.TotalRecords)
		}
	}

	// Mark database as loaded using sharded map
	s.loadedDatabases.Set(databaseName, time.Now())

	// Log summary at INFO level for visibility
	if skippedIndexes > 0 {
		s.logger.Debugf("📊 Loaded %d unique indexes for database '%s', using %d MB / %d MB budget (%d indexes skipped due to budget)",
			totalIndexes, databaseName,
			totalMemory/(1024*1024),
			s.uniqueIndexMemoryBudgetBytes/(1024*1024),
			skippedIndexes)
	} else {
		s.logger.Debugf("📊 Loaded %d unique indexes for database '%s', using %d MB / %d MB budget",
			totalIndexes, databaseName,
			totalMemory/(1024*1024),
			s.uniqueIndexMemoryBudgetBytes/(1024*1024))
	}

	return nil
}

// min returns the minimum of two integers
// I should probably redo this in assembly for speed
func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

func CreateHashIndex(s *BundleService, bundle *models.Bundle, indexCommand *models.CreateIndexCommand) error {
	// === OLD V2 IMPLEMENTATION (Commented out) ===
	// config := hashindexV2.IndexConfig{
	// 	BundleName:  bundle.Name,
	// 	FieldName:   indexCommand.Fields[0].Name,
	// 	IsUnique:    indexCommand.Fields[0].IsUnique,
	// 	DataDir:     args.DataDir,
	// 	DebugMode:   args.Debug,
	// 	InitialSize: 16,
	// 	PageSize:    8192,
	// 	LoadFactor:  0.75,
	// 	CacheSize:   100,
	// }
	// hashIndex, err := hashindexV2.CreateHashIndex(&config, s.logger)

	// === NEW V3 IMPLEMENTATION (LSM-style) ===
	// Create configuration for hashindexV3
	databasePath := helpers.GetDatabaseFolderPath(bundle.Database.Name)
	indexesPath := filepath.Join(databasePath, bundle.Name, "indexes")

	// Check if this field is a foreign key
	isForeignKey, referencedBundle, referencedField := IsFieldForeignKey(bundle, indexCommand.Fields[0].Name)

	// Get global settings for sequence safety margin
	globalSettings := settings.GetSettings()

	config := hashindex.IndexConfig{
		IndexName:            indexCommand.IndexName,
		BundleName:           bundle.Name,
		DatabaseName:         bundle.Database.Name,
		FieldName:            indexCommand.Fields[0].Name,
		DataDir:              indexesPath,
		MaxFileSize:          128 * 1024 * 1024, // 128MB per entry file
		WriteBufferSize:      64 * 1024,         // 64KB write buffer
		MemTableMaxSize:      100000,            // 100K entries in MemTable
		SequenceSafetyMargin: globalSettings.IndexSequenceSafetyMargin,
		CompactionEnabled:    true,
		CompactionMaxFiles:   10,
		Logger:               s.logger,
		IsForeignKey:         isForeignKey,
		ReferencedBundle:     referencedBundle,
		ReferencedField:      referencedField,
	}

	// Create the hash index using hashindexV3 LSM implementation
	hashIndex, err := hashindex.NewHashIndexV3(config)
	if err != nil {
		s.logger.Errorf("Failed to create hash index V3: %v", err)
		return fmt.Errorf("failed to create hash index: %w", err)
	}

	// Backfill: Populate the hash index with existing documents from the bundle.
	// Without this, indexes created AFTER documents are inserted would remain empty,
	// causing all hash index lookups to miss (returning 0 results).
	// This mirrors the backfill logic in CreateBTreeIndex.
	//
	// We iterate page-by-page so we can record the correct pageID per document,
	// which the query planner uses to skip directly to the right storage page.
	fieldName := indexCommand.Fields[0].Name
	schema := bundle.DocumentStructure.FieldSchema()
	if bundle.PageCount > 0 || bundle.TotalDocuments > 0 {
		s.logger.Infof("Backfilling hash index '%s' on field '%s' for bundle '%s' (%d pages)",
			indexCommand.IndexName, fieldName, bundle.Name, bundle.PageCount)

		insertedCount := 0
		skippedCount := 0

		if s.metadataBufferLen.Load() > 0 {
			s.FlushMetadataUpdates()
		}

		pageCount := uint32(bundle.PageCount)
		if pageCount == 0 {
			pageCount = 1
		}

		for pageID := uint32(0); pageID < pageCount; pageID++ {
			docs, err := s.SnapshotPageDocuments(bundle.Name, bundle.Database.Name, pageID)
			if err != nil {
				continue
			}

			for _, doc := range docs {
				fieldValue, err := extractFieldValueForIndex(doc, fieldName, schema)
				if err != nil {
					// Field may not exist on every document (sparse fields); skip gracefully.
					skippedCount++
					continue
				}

				// Convert field value to the same key string used by query lookups
				keyValue := fieldValueToIndexKeyString(fieldValue)
				if keyValue == "" || keyValue == "<nil>" {
					skippedCount++
					continue
				}

				// Insert into the hash index using Put(), which correctly sets BucketNum
				err = hashIndex.Put(keyValue, doc.DocumentID, pageID, doc.CommitSequence, doc.VersionSequence)
				if err != nil {
					s.logger.Warnf("Failed to backfill hash index entry for doc '%s' key '%s': %v",
						doc.DocumentID, keyValue, err)
					skippedCount++
					continue
				}
				insertedCount++
			}
		}

		s.logger.Infof("Hash index '%s' backfill complete: %d inserted, %d skipped",
			indexCommand.IndexName, insertedCount, skippedCount)

		// Flush to ensure all backfilled entries are persisted to disk
		if flushErr := hashIndex.Flush(); flushErr != nil {
			s.logger.Warnf("Failed to flush hash index '%s' after backfill: %v", indexCommand.IndexName, flushErr)
		}
	}

	// Create the index field structure for compatibility
	indexField := models.IndexField{
		FieldName: fieldName,
		IsUnique:  indexCommand.Fields[0].IsUnique,
		Collation: "",
	}

	// Create the index reference
	indexRef := models.IndexReference{
		IndexName:      indexCommand.IndexName,
		Fields:         indexCommand.Fields,
		IndexType:      indexCommand.IndexType,
		CreateTime:     time.Now(),
		IndexInstance:  hashIndex, // Store the V3 hash index instance
		HashIndexField: indexField,
		WherePredicate: indexCommand.WherePredicate,
	}

	// Add the index to the bundle
	bundle.Indexes[indexCommand.IndexName] = indexRef
	bundle.IndexNames = append(bundle.IndexNames, indexCommand.IndexName)

	// Update the bundle file
	err = s.store.UpdateBundleFile(bundle.Database, bundle)
	if err != nil {
		s.logger.Errorf("Failed to update bundle file after creating index: %v", err)
		return fmt.Errorf("failed to update bundle file after creating index: %w", err)
	}

	s.logger.Debugf("Successfully created V3 hash index '%s' on field '%s' for bundle '%s'",
		indexCommand.IndexName, fieldName, bundle.Name)
	return nil
}

func createHashIndexInternal(s *BundleService, bundle *models.Bundle, name string) error {
	databasePath := helpers.GetDatabaseFolderPath(bundle.Database.Name)
	indexesPath := filepath.Join(databasePath, bundle.Name, "indexes")

	// === OLD V2 IMPLEMENTATION (Sprint 5: Commented out) ===
	// config := hashindexV2.IndexConfig{
	// 	DatabaseName: bundle.Database.Name,
	// 	BundleName:   bundle.Name,
	// 	FieldName:    name,
	// 	IsUnique:     true,
	// 	DataDir:      databasePath,
	// 	DebugMode:    args.Debug,
	// 	InitialSize:  16,
	// 	PageSize:     8192,
	// 	LoadFactor:   0.75,
	// 	CacheSize:    100,
	// }
	// hashIndex, err := hashindexV2.CreateHashIndex(&config, s.logger)

	// === NEW V3 IMPLEMENTATION (Sprint 5: LSM-style) ===
	// Check if this field is a foreign key
	isForeignKey, referencedBundle, referencedField := IsFieldForeignKey(bundle, name)

	// Get global settings for sequence safety margin
	globalSettings := settings.GetSettings()

	config := hashindex.IndexConfig{
		IndexName:            name, //name + "_idx",
		BundleName:           bundle.Name,
		DatabaseName:         bundle.Database.Name,
		FieldName:            name,
		DataDir:              indexesPath,
		MaxFileSize:          128 * 1024 * 1024,
		WriteBufferSize:      64 * 1024,
		MemTableMaxSize:      100000,
		SequenceSafetyMargin: globalSettings.IndexSequenceSafetyMargin,
		CompactionEnabled:    true,
		CompactionMaxFiles:   10,
		Logger:               s.logger,
		IsForeignKey:         isForeignKey,
		ReferencedBundle:     referencedBundle,
		ReferencedField:      referencedField,
	}

	// Create the hash index using hashindexV3
	hashIndex, err := hashindex.NewHashIndexV3(config)
	if err != nil {
		s.logger.Errorf("Failed to create hash index V3: %v", err)
		return fmt.Errorf("failed to create hash index: %w", err)
	}

	// Create the index field structure for compatibility
	indexField := models.IndexField{
		FieldName: name,
		IsUnique:  true,
		Collation: "",
	}

	// Create the index reference
	indexRef := models.IndexReference{
		IndexName:      name,
		Fields:         []models.FieldDefinition{bundle.DocumentStructure.FieldDefinitions["DocumentID"]},
		IndexType:      "hash",
		CreateTime:     time.Now(),
		IndexInstance:  hashIndex, // Store the V2 hash index instance
		HashIndexField: indexField,
	}

	// Initialize indexes map if nil
	if bundle.Indexes == nil {
		bundle.Indexes = make(map[string]models.IndexReference)
	}

	// Add the index to the bundle
	bundle.Indexes[name] = indexRef
	bundle.IndexNames = append(bundle.IndexNames, name)

	// Update the bundle file
	err = s.store.UpdateBundleFile(bundle.Database, bundle)
	if err != nil {
		s.logger.Errorf("Failed to update bundle file after creating index: %v", err)
		return fmt.Errorf("failed to update bundle file after creating index: %w", err)
	}

	// Update the cache with the modified bundle
	s.bundleMetadata[bundle.Name] = bundle

	s.logger.Debugf("Successfully created hash index '%s' on field '%s' for bundle '%s'", name, name, bundle.Name)
	return nil
}

// CreateBTreeIndex creates a new BTree index for the specified bundle and field
// This function follows the same pattern as the hash index creation but uses
// the btreeindexV2 implementation for optimal B+ tree performance
// Parameters:
//   - s: The BundleService instance for logging and storage operations
//   - bundle: The bundle to create the index for
//   - indexCommand: The command containing index configuration details
//
// Returns:
//   - error: Any error that occurred during index creation
func CreateBTreeIndex(s *BundleService, bundle *models.Bundle, indexCommand *models.CreateIndexCommand) error {
	args := settings.GetSettings()

	// Validate input parameters
	if len(indexCommand.Fields) == 0 {
		return fmt.Errorf("no fields specified for BTree index creation")
	}

	fieldDef := indexCommand.Fields[0]

	// Validate that all indexed fields exist in the bundle structure
	// Skip validation for expression indexes (the field is derived from the expression)
	if indexCommand.Expression == "" {
		for _, fd := range indexCommand.Fields {
			if _, exists := bundle.DocumentStructure.FieldDefinitions[fd.Name]; !exists {
				return fmt.Errorf("field '%s' does not exist in bundle '%s'", fd.Name, bundle.Name)
			}
		}
	}
	isComposite := len(indexCommand.Fields) > 1
	indexSchema := bundle.DocumentStructure.FieldSchema()

	var partialPredicate func(*models.Document) bool
	if indexCommand.WherePredicate != "" {
		pred, predErr := compilePartialIndexPredicate(indexCommand.WherePredicate, indexSchema)
		if predErr != nil {
			return fmt.Errorf("failed to compile partial index predicate: %w", predErr)
		}
		partialPredicate = pred
		s.logger.Debugf("Partial index predicate compiled for '%s': %s", indexCommand.IndexName, indexCommand.WherePredicate)
	}

	// Compile expression index function if present
	var exprFn func(interface{}) interface{}
	if indexCommand.Expression != "" {
		_, fn, exprErr := indexutils.CompileIndexExpression(indexCommand.Expression)
		if exprErr != nil {
			return fmt.Errorf("failed to compile index expression: %w", exprErr)
		}
		exprFn = fn
		s.logger.Debugf("Expression index function compiled for '%s': %s", indexCommand.IndexName, indexCommand.Expression)
	}

	s.logger.Debugf("Creating BTree index '%s' on field '%s' for bundle '%s'",
		indexCommand.IndexName, fieldDef.Name, bundle.Name)

	// Then in the CreateBTreeIndex function:
	splitRatio := calculateOptimalSplitRatio(fieldDef, fieldDef.IsUnique)

	// Create configuration for the new BTree index
	config := btreeindexV2.IndexConfig{
		DatabaseName: bundle.Database.Name,
		BundleName:   bundle.Name,
		FieldName:    fieldDef.Name,
		IsUnique:     fieldDef.IsUnique,
		// IndexDir removed - use proper database/bundle/indexes/btree path structure
		DebugMode:    args.Debug,
		PageSize:     8192,       // 8KB pages (PostgreSQL-style)
		CacheSize:    100,        // Cache 100 pages for performance
		FillFactor:   0.7,        // 70% fill factor for optimal balance between space and performance
		MaxKeyLength: 2048,       // Set maximum key length to 2KB
		SplitRatio:   splitRatio, // Use the calculated split ratio
	}

	// Configure WAL manager for durability using dependency injection
	// DRY Principle: Use shared service registry to access WAL without circular dependencies
	// Open/Closed: Registry pattern allows adding new services without modifying existing code
	serviceRegistry := registry.GetRegistry()
	if serviceRegistry.IsWALAvailable() {
		config.WALManager = serviceRegistry.GetWALManager()
		s.logger.Debugf("WAL enabled for B-tree index '%s' on field '%s'", indexCommand.IndexName, fieldDef.Name)
	} else {
		s.logger.Debugf("WAL not available for B-tree index '%s' (proceeding without durability)", indexCommand.IndexName)
	}

	// Set IndexName for proper file path construction
	config.IndexName = indexCommand.IndexName

	// Get proper database path structure (same as hash index)
	databasePath := helpers.GetDatabaseFolderPath(bundle.Database.Name)

	// CRITICAL: Construct full B-tree indexes path to match bundle structure
	// B-tree indexes must be stored in: database/bundle/indexes/btree/
	// Format: /data_dir/<database>/<bundle>/indexes/btree/<btree-index-file-name>.btidx
	btreeIndexesPath := filepath.Join(databasePath, bundle.Name, "indexes", "btree")

	// Ensure the btree indexes directory exists before creating the index
	if err := os.MkdirAll(btreeIndexesPath, 0755); err != nil {
		s.logger.Errorf("Failed to create btree indexes directory: %v", err)
		return fmt.Errorf("failed to create btree indexes directory: %w", err)
	}

	// Create the BTree index using the V2 implementation
	btreeIndex, err := btreeindexV2.CreateBTreeIndex(&config, s.logger)
	if err != nil {
		s.logger.Errorf("Failed to create BTree index: %v", err)
		return fmt.Errorf("failed to create BTree index: %w", err)
	}

	// Populate the index with existing documents from the bundle
	// TODO: Optimize this to work with paginated documents
	s.logger.Debugf("Populating BTree index with documents from bundle '%s'", bundle.Name)

	// For now, we need to load all documents to build the index
	// In the future, this should be done incrementally as pages are loaded
	allDocuments, err := s.getAllDocumentsForIndexing(bundle.Name, 0, 0, nil)
	if err != nil {
		s.logger.Warnf("Failed to load documents for indexing: %v", err)
		return err
	}

	if len(allDocuments) > 0 {
		s.logger.Debugf("Populating BTree index with %d existing documents", len(allDocuments))

		insertedCount := 0
		skippedCount := 0
		for documentID, document := range allDocuments {
			// Partial index: skip documents that don't match the predicate
			if partialPredicate != nil && !partialPredicate(document) {
				skippedCount++
				continue
			}

			var keyBytes []byte

			if exprFn != nil {
				fieldValue, fErr := extractFieldValueForIndex(*document, fieldDef.Name, indexSchema)
				if fErr != nil {
					s.logger.Warnf("Failed to extract field value for document '%s': %v", documentID, fErr)
					continue
				}
				transformedValue := exprFn(fieldValue)
				keyBytes, err = convertValueToBytes(transformedValue)
				if err != nil {
					s.logger.Warnf("Failed to convert expression result to bytes for document '%s': %v", documentID, err)
					continue
				}
			} else if isComposite {
				keyBytes, err = encodeCompositeKeyForIndex(*document, indexCommand.Fields, indexSchema)
				if err != nil {
					s.logger.Warnf("Failed to encode composite key for document '%s': %v", documentID, err)
					continue
				}
			} else {
				fieldValue, fErr := extractFieldValueForIndex(*document, fieldDef.Name, indexSchema)
				if fErr != nil {
					s.logger.Warnf("Failed to extract field value for document '%s': %v", documentID, fErr)
					continue
				}
				keyBytes, err = convertValueToBytes(fieldValue)
				if err != nil {
					s.logger.Warnf("Failed to convert field value to bytes for document '%s': %v", documentID, err)
					continue
				}
			}

			// Insert into the BTree index
			// CRITICAL FIX: Make population idempotent - skip duplicates gracefully
			// This allows index creation to succeed even if:
			// - Index was partially populated from a previous failed attempt
			// - Same document appears multiple times in the document set
			// - Concurrent operations added entries
			err = btreeIndex.Insert(keyBytes, document.DocumentID)
			if err != nil {
				// Check if this is a duplicate document ID error (expected during idempotent population)
				if strings.Contains(err.Error(), "document ID already exists for this key") {
					// Document already in index - skip gracefully (idempotent behavior)
					skippedCount++
					s.logger.Debugf("Skipping document '%s' - already exists in index (idempotent population)", documentID)
					continue
				}
				// For other errors (e.g., unique constraint violations), log and fail
				s.logger.Errorf("Failed to insert document '%s' into BTree index: %v", documentID, err)
				btreeIndex.Close()
				return fmt.Errorf("failed to populate BTree index with existing documents: %w", err)
			}
			insertedCount++
		}

		s.logger.Debugf("BTree index population complete: inserted %d documents, skipped %d duplicates", insertedCount, skippedCount)

		if err := btreeIndex.PersistMetadata(); err != nil {
			s.logger.Warnf("Failed to persist B-tree index metadata after population: %v", err)
		}
		s.logger.Debugf("Successfully populated BTree index with existing documents")
	}

	// Create the index field structure for compatibility
	indexField := models.IndexField{
		FieldName: fieldDef.Name,
		IsUnique:  fieldDef.IsUnique,
		Collation: "",
	}

	// Create the index reference
	indexRef := models.IndexReference{
		IndexName:       indexCommand.IndexName,
		Fields:          indexCommand.Fields,
		IndexType:       indexCommand.IndexType,
		CreateTime:      time.Now(),
		IndexInstance:   btreeIndex, // Store the V2 BTree index instance
		BTreeIndexField: indexField, // Add this field to models.IndexReference if not exists
		IncludeFields:   indexCommand.IncludeFields,
		WherePredicate:  indexCommand.WherePredicate,
		Expression:      indexCommand.Expression,
	}

	// Initialize indexes map if nil
	if bundle.Indexes == nil {
		bundle.Indexes = make(map[string]models.IndexReference)
	}

	// Add the index to the bundle
	bundle.Indexes[indexCommand.IndexName] = indexRef
	bundle.IndexNames = append(bundle.IndexNames, indexCommand.IndexName)

	// Update the bundle file with the new index information
	err = s.store.UpdateBundleFile(bundle.Database, bundle)
	if err != nil {
		s.logger.Errorf("Failed to update bundle file after creating BTree index: %v", err)
		// Close the index since we couldn't save the bundle state
		btreeIndex.Close()
		return fmt.Errorf("failed to update bundle file after creating BTree index: %w", err)
	}

	s.logger.Debugf("Successfully created BTree index '%s' on field '%s' for bundle '%s'",
		indexCommand.IndexName, fieldDef.Name, bundle.Name)

	return nil
}

// CreateBRINIndex creates a BRIN (Block Range INdex) on a field.
// BRIN indexes store min/max per page range and are ideal for naturally ordered columns
// (timestamps, sequential IDs). Tiny index size (~few KB for millions of docs).
func CreateBRINIndex(s *BundleService, bundle *models.Bundle, indexCommand *models.CreateIndexCommand) error {
	if len(indexCommand.Fields) == 0 {
		return fmt.Errorf("no fields specified for BRIN index creation")
	}
	fieldDef := indexCommand.Fields[0]

	// Validate that the field exists in the bundle structure
	if bundle.DocumentStructure.FieldDefinitions != nil {
		if _, exists := bundle.DocumentStructure.FieldDefinitions[fieldDef.Name]; !exists {
			return fmt.Errorf("field '%s' does not exist in bundle '%s'", fieldDef.Name, bundle.Name)
		}
	}

	s.logger.Debugf("Creating BRIN index '%s' on field '%s' for bundle '%s'",
		indexCommand.IndexName, fieldDef.Name, bundle.Name)

	databasePath := helpers.GetDatabaseFolderPath(bundle.Database.Name)
	indexesPath := filepath.Join(databasePath, bundle.Name, "indexes", "brin")
	if err := os.MkdirAll(indexesPath, 0755); err != nil {
		return fmt.Errorf("failed to create BRIN indexes directory: %w", err)
	}

	ppr := indexCommand.PagesPerRange
	if ppr == 0 {
		ppr = 128
	}

	config := brinindex.BRINConfig{
		IndexName:     indexCommand.IndexName,
		BundleName:    bundle.Name,
		DatabaseName:  bundle.Database.Name,
		FieldName:     fieldDef.Name,
		DataDir:       indexesPath,
		PagesPerRange: ppr,
		Logger:        s.logger,
	}

	brinIdx, err := brinindex.NewBRINIndex(config)
	if err != nil {
		return fmt.Errorf("failed to create BRIN index: %w", err)
	}

	// Backfill: iterate pages to build min/max per range (page-aware)
	s.logger.Debugf("Populating BRIN index with documents from bundle '%s' (%d pages)", bundle.Name, bundle.PageCount)
	insertedCount := 0
	for pageID := uint32(0); pageID < uint32(bundle.PageCount); pageID++ {
		docs, snapErr := s.SnapshotPageDocuments(bundle.Name, bundle.Database.Name, pageID)
		if snapErr != nil {
			continue // page may not exist
		}
		schema := bundle.DocumentStructure.FieldSchema()
		for i := range docs {
			doc := &docs[i]
			fieldValue, fErr := extractFieldValueForIndex(*doc, fieldDef.Name, schema)
			if fErr != nil {
				continue
			}
			brinIdx.UpdateRange(pageID, fieldValue)
			insertedCount++
		}
	}

	s.logger.Debugf("BRIN index population complete: processed %d documents", insertedCount)

	if err := brinIdx.Flush(); err != nil {
		return fmt.Errorf("failed to persist BRIN index: %w", err)
	}

	// Register in bundle metadata
	if bundle.Indexes == nil {
		bundle.Indexes = make(map[string]models.IndexReference)
	}

	indexRef := models.IndexReference{
		IndexName:     indexCommand.IndexName,
		Fields:        indexCommand.Fields,
		IndexType:     "brin",
		CreateTime:    time.Now(),
		IndexInstance: brinIdx,
	}
	bundle.Indexes[indexCommand.IndexName] = indexRef
	bundle.IndexNames = append(bundle.IndexNames, indexCommand.IndexName)

	err = s.store.UpdateBundleFile(bundle.Database, bundle)
	if err != nil {
		s.logger.Errorf("Failed to update bundle file after creating BRIN index: %v", err)
		return fmt.Errorf("failed to update bundle file after creating BRIN index: %w", err)
	}

	s.logger.Debugf("Successfully created BRIN index '%s' on field '%s' for bundle '%s'",
		indexCommand.IndexName, fieldDef.Name, bundle.Name)
	return nil
}

// extractFieldValueForIndex extracts the value of a specific field from a document
// This function handles the document field structure and returns the raw value
// for index key generation
// Parameters:
//   - document: The document to extract the field value from
//   - fieldName: The name of the field to extract
//
// Returns:
//   - interface{}: The field value
//   - error: Any error that occurred during extraction
func compilePartialIndexPredicate(predicate string, schema *models.BundleFieldSchema) (func(*models.Document) bool, error) {
	expr, err := syndrQL.ParseExpression(predicate)
	if err != nil {
		return nil, fmt.Errorf("failed to parse partial index predicate '%s': %w", predicate, err)
	}
	return func(doc *models.Document) bool {
		if doc == nil {
			return false
		}
		return evaluatePartialIndexExpr(expr, doc, schema)
	}, nil
}

func evaluatePartialIndexExpr(expr syndrQL.Expression, doc *models.Document, schema *models.BundleFieldSchema) bool {
	switch e := expr.(type) {
	case *syndrQL.BinaryExpression:
		if e.Operator == syndrQL.TOKEN_AND {
			return evaluatePartialIndexExpr(e.Left, doc, schema) && evaluatePartialIndexExpr(e.Right, doc, schema)
		}
		if e.Operator == syndrQL.TOKEN_OR {
			return evaluatePartialIndexExpr(e.Left, doc, schema) || evaluatePartialIndexExpr(e.Right, doc, schema)
		}
		var fieldName string
		switch left := e.Left.(type) {
		case *syndrQL.IdentifierExpression:
			fieldName = strings.Trim(left.Name, "\"")
		case *syndrQL.QualifiedIdentifierExpression:
			fieldName = strings.Trim(left.Field, "\"")
		default:
			return false
		}
		var docValue models.FieldValue
		var exists bool
		if schema != nil && len(doc.Values) > 0 {
			docValue, exists = doc.GetFieldValue(schema, fieldName)
		} else if doc.Data != nil {
			if v, ok := doc.Data[fieldName]; ok {
				docValue = models.NewInterfaceValue(v)
				exists = true
			}
		}
		if !exists {
			return false
		}
		literal, isLiteral := e.Right.(*syndrQL.LiteralExpression)
		if !isLiteral {
			return false
		}
		compValue := literal.Value
		switch e.Operator {
		case syndrQL.TOKEN_EQ, syndrQL.TOKEN_ASSIGN:
			return fmt.Sprintf("%v", docValue.AsInterface()) == fmt.Sprintf("%v", compValue)
		case syndrQL.TOKEN_NEQ:
			return fmt.Sprintf("%v", docValue.AsInterface()) != fmt.Sprintf("%v", compValue)
		default:
			return false
		}
	case *syndrQL.GroupedExpression:
		return evaluatePartialIndexExpr(e.Expression, doc, schema)
	default:
		return false
	}
}

func extractFieldValueForIndex(document models.Document, fieldName string, schema *models.BundleFieldSchema) (interface{}, error) {
	if schema != nil && len(document.Values) > 0 {
		if fv, ok := document.GetFieldValue(schema, fieldName); ok {
			return fv.AsInterface(), nil
		}
	}
	if document.Data != nil {
		if v, ok := document.Data[fieldName]; ok {
			return v, nil
		}
	}
	return nil, fmt.Errorf("field '%s' not found in document", fieldName)
}

// encodeCompositeKeyForIndex encodes multiple field values into a single composite key.
// Each component is length-prefixed (4 bytes big-endian) for unambiguous decoding
// and correct lexicographic ordering.
func encodeCompositeKeyForIndex(document models.Document, fields []models.FieldDefinition, schema *models.BundleFieldSchema) ([]byte, error) {
	var buf []byte
	for _, fd := range fields {
		fieldValue, err := extractFieldValueForIndex(document, fd.Name, schema)
		if err != nil {
			// Missing field: encode as 0-length component
			lenBytes := make([]byte, 4)
			binary.BigEndian.PutUint32(lenBytes, 0)
			buf = append(buf, lenBytes...)
			continue
		}
		encoded, err := convertValueToBytes(fieldValue)
		if err != nil {
			return nil, fmt.Errorf("failed to encode composite key component '%s': %w", fd.Name, err)
		}
		lenBytes := make([]byte, 4)
		binary.BigEndian.PutUint32(lenBytes, uint32(len(encoded)))
		buf = append(buf, lenBytes...)
		buf = append(buf, encoded...)
	}
	return buf, nil
}

// fieldValueToIndexKeyString converts a value (possibly models.FieldValue from a document field)
// to the same string representation used by query lookups. Query literals are converted with
// conversion.ValueToString(int64(6775)) -> "6775". Document fields are models.FieldValue; passing
// that directly to ValueToString would hit the default case and produce a struct dump (e.g.
// "{Type:2 IntVal:6775}"), so lookups for "6775" would miss. This helper unwraps FieldValue via
// AsInterface() so the key matches query lookups.
func fieldValueToIndexKeyString(v interface{}) string {
	if v == nil {
		return "<nil>"
	}
	if fv, ok := v.(models.FieldValue); ok {
		return conversion.ValueToString(fv.AsInterface())
	}
	return conversion.ValueToString(v)
}

// convertValueToBytes converts a field value to bytes for BTree key storage
// This function handles different data types and converts them to a consistent
// byte representation for use as BTree keys
// Parameters:
//   - value: The field value to convert
//
// Returns:
//   - []byte: The value converted to bytes
//   - error: Any error that occurred during conversion
func convertValueToBytes(value interface{}) ([]byte, error) {
	if value == nil {
		return []byte{}, nil
	}

	// Handle models.FieldValue explicitly — unwrap to the concrete Go type
	// so that the type switch below encodes it consistently with EncodeKey in key_encoding.go.
	if fv, ok := value.(models.FieldValue); ok {
		value = fv.AsInterface()
		if value == nil {
			return []byte{}, nil
		}
	}

	switch v := value.(type) {
	case string:
		// Magic values (SYNDR_NULL, SYNDR_MISSING, etc.) get consistent byte representation
		// This ensures they sort predictably in BTree indexes and can be efficiently queried
		if strings.HasPrefix(v, "::SYNDR_") {
			// Store magic values as-is for consistent indexing and querying
			return []byte(v), nil
		}
		return []byte(v), nil
	case []byte:
		return v, nil
	case int:
		return []byte(fmt.Sprintf("%d", v)), nil
	case int32:
		return []byte(fmt.Sprintf("%d", v)), nil
	case int64:
		return []byte(fmt.Sprintf("%d", v)), nil
	case float32:
		return []byte(fmt.Sprintf("%.6f", v)), nil
	case float64:
		return []byte(fmt.Sprintf("%.6f", v)), nil
	case bool:
		if v {
			return []byte("true"), nil
		}
		return []byte("false"), nil
	default:
		// For complex types, convert to string representation
		return []byte(conversion.ValueToString(v)), nil
	}
}

// calculateOptimalSplitRatio determines the best split ratio based on field characteristics
// This function follows the Single Responsibility Principle for split ratio calculation
// Parameters:
//   - fieldDef: The field definition to analyze
//   - isUnique: Whether this is a unique index
//
// Returns:
//   - float64: The optimal split ratio for this index
func calculateOptimalSplitRatio(fieldDef models.FieldDefinition, isUnique bool) float64 {
	// For unique indexes, use 50% split for balanced structure
	if isUnique {
		return 0.5
	}

	/*
		Split Ratio = 0.5 (50%) is the recommended value from Copilot because:

		1.Balanced Tree Structure: When a node becomes full and needs to split, a 50% ratio creates two nodes
		that are equally balanced, maintaining optimal B+ tree characteristics.

		2.PostgreSQL Standard: PostgreSQL uses a similar 50% split ratio for B-tree indexes, which provides
		excellent performance characteristics.

		3.Optimal Performance: Equal splits minimize tree height and provide consistent performance for both
		insertions and searches.

		4.Space Efficiency: Balanced splits ensure good space utilization without excessive fragmentation.

		I will use this for now, but will eventually make it more intelligent, despite copilots claims
	*/

	// For non-unique indexes with potential duplicates, slightly favor left split
	// This can help with sequential insertion patterns
	switch fieldDef.Type {
	case "string":
		return 0.5 // Balanced split for string fields
	case "int", "int32", "int64":
		return 0.6 // Slightly favor left for numeric sequences
	case "float32", "float64":
		return 0.5 // Balanced split for floating point
	case "bool":
		return 0.5 // Balanced split for boolean fields
	default:
		return 0.5 // Default to balanced split
	}
}

// updatesIndexedField returns true if any field in the update touches a non-DocumentID
// indexed field (B-tree, hash on user field, or BRIN). When this returns false, a
// HOT-like update is possible: the hash index pointer doesn't need updating (same
// pageID since DocumentID is unchanged), and no secondary index maintenance is needed.
//
// This is the SyndrDB equivalent of PostgreSQL's Heap-Only Tuple (HOT) detection.
func updatesIndexedField(bundle *models.Bundle, updates []models.KeyValue) bool {
	if bundle.Indexes == nil {
		return false
	}
	for _, kv := range updates {
		for _, indexRef := range bundle.Indexes {
			switch indexRef.IndexType {
			case "hash":
				if indexRef.HashIndexField.FieldName == kv.Key && indexRef.HashIndexField.FieldName != "DocumentID" {
					return true
				}
			case "btree":
				if indexRef.BTreeIndexField.FieldName == kv.Key {
					return true
				}
				// Check multi-column btree indexes
				for _, fieldDef := range indexRef.Fields {
					if fieldDef.Name == kv.Key {
						return true
					}
				}
			case "brin":
				for _, fieldDef := range indexRef.Fields {
					if fieldDef.Name == kv.Key {
						return true
					}
				}
			}
		}
	}
	return false
}
