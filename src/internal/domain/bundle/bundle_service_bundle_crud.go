package bundle

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"syndrdb/src/internal/domain/database"
	"syndrdb/src/internal/domain/models"
	"syndrdb/src/internal/storage/bundlestore"
	"syndrdb/src/pkg/common/helpers"
	"syndrdb/src/pkg/errors"
	"syndrdb/src/pkg/settings"

	"syndrdb/src/internal/domain/index/brinindex"
	"syndrdb/src/internal/domain/index/btreeindexV2"
	hashindex "syndrdb/src/internal/domain/index/hashindexV3"
)

func (s *BundleService) AddBundle(databaseService *database.DatabaseService, db *models.Database, bundleCommand *models.BundleCommand) (*models.Bundle, error) {
	args := settings.GetSettings()

	// Validate bundle name (includes _mv_ prefix check)
	if err := s.validateBundleName(bundleCommand.BundleName); err != nil {
		return nil, errors.NewValidationError(
			errors.ERR_VALIDATION_FIELD,
			fmt.Sprintf("Invalid bundle name '%s'", bundleCommand.BundleName),
			errors.LayerDomain,
			&errors.ValidationErrorDetails{
				SubmittedInput: bundleCommand.BundleName,
				ExpectedFormat: "valid bundle name (alphanumeric, underscores, no spaces)",
				Suggestions:    []string{"Bundle names must start with a letter and contain only alphanumeric characters and underscores"},
			},
		).WithContext("bundle_name", bundleCommand.BundleName)
	}

	// Check if the bundle already exists
	if _, err := s.GetBundleByName(db, bundleCommand.BundleName); err == nil {
		return nil, errors.New(errors.ERR_VALIDATION_CONSTRAINT, fmt.Sprintf("Bundle '%s' already exists", bundleCommand.BundleName), errors.LayerDomain).WithContext("bundle_name", bundleCommand.BundleName)
	}

	// Create a new bundle
	bundle := s.factory.NewBundle(bundleCommand.BundleName, "")
	bundle.Database = db

	// Automatically add a DocumentID field to the bundle structure for all bundles
	bundle.DocumentStructure.FieldDefinitions["DocumentID"] = models.FieldDefinition{
		Name:         "DocumentID",
		Type:         "string",
		IsRequired:   true,
		IsUnique:     true,
		DefaultValue: "",
	}

	// Initialize the Document structure in the bundle
	for _, fieldDef := range bundleCommand.Fields {
		bundle.DocumentStructure.FieldDefinitions[fieldDef.Name] = models.FieldDefinition{
			Name:         fieldDef.Name,
			Type:         fieldDef.Type,
			IsRequired:   fieldDef.IsRequired,
			IsUnique:     fieldDef.IsUnique,
			DefaultValue: fieldDef.DefaultValue,
		}
		if args.Debug {
			s.logger.Debugf("Added field '%s' to bundle '%s'", fieldDef.Name, bundleCommand.BundleName)
		}

	}

	// Add the bundle to the database
	db.Bundles[bundle.Name] = *bundle

	//This needs to be added to a bundle file
	err := s.store.CreateBundleFile(db, bundle)
	if err != nil {
		return nil, errors.WrapWithMessage(err, errors.ERR_INTERNAL_STORAGE, fmt.Sprintf("Error creating bundle file for bundle '%s'", bundle.Name), errors.LayerStorage)
	}

	// and then the bundle file name needs to be added to the database file
	db.BundleFiles = append(db.BundleFiles, fmt.Sprintf("%s_%s.bnd", db.Name, bundle.Name))

	// Write the updated database file
	err = databaseService.Store.UpdateDatabaseDataFile(db)
	if err != nil {
		return nil, errors.WrapWithMessage(err, errors.ERR_INTERNAL_STORAGE, fmt.Sprintf("Error updating database file after creating bundle '%s'", bundle.Name), errors.LayerStorage)
	}

	createHashIndexInternal(s, bundle, "DocumentID") // Create a hash index on DocumentID

	// Create unique indexes for all IsUnique fields automatically
	uniqueValidator := NewUniqueConstraintValidator(s, s.logger)
	err = uniqueValidator.CreateUniqueIndexesForBundle(bundle)
	if err != nil {
		return nil, errors.WrapWithMessage(err, errors.ERR_INTERNAL_INDEX, fmt.Sprintf("Failed to create unique indexes for bundle '%s'", bundle.Name), errors.LayerIndex)
	}

	s.bundleMetadata[bundleCommand.BundleName] = bundle

	// Register the new bundle in the Primary database's "Bundles" catalog
	// This will be handled by the catalog service at a higher level to avoid circular imports
	err = s.registerBundleInPrimary(bundle)
	if err != nil {
		s.logger.Warnf("Warning: Failed to register bundle '%s' in Primary catalog: %v", bundle.Name, err)
		// Don't fail the bundle creation if catalog registration fails
	}

	// GRAPHQL INTEGRATION: Generate and store GraphQL schema for new bundle
	// This creates the initial schema version (v1) and caches it for query execution.
	// Schema generation only occurs if GraphQL support is enabled globally.
	//
	// Steps:
	// 1. Get or create schema manager for this database (lazy initialization)
	// 2. Generate GraphQL schema definition from bundle structure
	// 3. Create schema ID and store in versioned file format
	// 4. Cache schema for fast GraphQL query processing
	//
	// Note: Schema generation failures are logged but don't fail bundle creation.
	// This ensures bundles can be created even if GraphQL has issues.
	if s.graphQLEnabled && s.schemaGenerator != nil {
		s.logger.Debugf("[GraphQL] Generating schema for new bundle '%s' in database '%s'", bundle.Name, db.Name)

		// Get or create the schema manager for this database (thread-safe lazy init)
		schemaManager, err := s.getOrCreateSchemaManager(db)
		if err != nil {
			s.logger.Warnf("[GraphQL] Failed to initialize schema manager for database '%s': %v. Skipping schema generation.", db.Name, err)
		} else if schemaManager != nil {
			// Generate GraphQL schema from bundle structure
			// Converts SyndrDB types → GraphQL types (string→String, int→Int, etc.)
			// Applies PascalCase naming convention (users → User, blog_posts → BlogPost)
			schemaDef, err := s.schemaGenerator.GenerateSchema(bundle)
			if err != nil {
				s.logger.Warnf("[GraphQL] Failed to generate schema for bundle '%s': %v", bundle.Name, err)
			} else {
				// Create unique schema ID for this schema version
				// Convert string UUIDs to [16]byte arrays for storage
				var schemaIDBytes, bundleIDBytes [16]byte
				copy(schemaIDBytes[:], []byte(helpers.GenerateFastUUID()))
				copy(bundleIDBytes[:], []byte(bundle.BundleID))

				// Store schema in versioned file format (creates version 1)
				// This writes to {database_dir}/{database_name}_graphql.gql
				err = schemaManager.AddNewSchema(schemaIDBytes, bundleIDBytes, bundle.Name, schemaDef)
				if err != nil {
					s.logger.Errorf("[GraphQL] Failed to store schema for bundle '%s': %v", bundle.Name, err)
				} else {
					s.logger.Debugf("[GraphQL] Schema created for bundle '%s' (version 1, %d fields)", bundle.Name, len(schemaDef.Fields))
				}
			}
		}
	}

	return bundle, nil
}

func (s *BundleService) AddBundleByStruct(databaseService *database.DatabaseService, db *models.Database, bundle *models.Bundle) error {
	// Set the database reference in the bundle
	bundle.Database = db

	// Initialize bundle properties if not set
	if bundle.PageSize == 0 {
		bundle.PageSize = s.defaultPageSize // Use service default (4096)

	}

	// Initialize TotalDocuments and PageCount - documents are now managed via page cache
	if bundle.TotalDocuments == 0 {
		bundle.TotalDocuments = 0
	}

	// Calculate initial PageCount
	// PageCount = ceil(TotalDocuments / PageSize)
	bundle.PageCount = (bundle.TotalDocuments + int64(bundle.PageSize) - 1) / int64(bundle.PageSize)
	if bundle.PageCount == 0 {
		bundle.PageCount = 1 // Always have at least 1 page for new bundles
	}

	// Add the bundle to the database
	db.Bundles[bundle.Name] = *bundle

	// Add the bundle to the service cache so it can be retrieved later with relationships intact
	s.bundleMetadata[bundle.Name] = bundle

	//This needs to be added to a bundle file
	err := s.store.CreateBundleFile(db, bundle)
	if err != nil {
		return fmt.Errorf("error creating bundle file from struct: %w", err)
	}

	// and then the bundle file name needs to be added to the database file
	db.BundleFiles = append(db.BundleFiles, fmt.Sprintf("%s_%s.bnd", db.Name, bundle.Name))

	// Write the updated database file
	err = databaseService.Store.UpdateDatabaseDataFile(db)
	if err != nil {
		return fmt.Errorf("error updating database file: %w", err)
	}

	createHashIndexInternal(s, bundle, "DocumentID") // Create a hash index on DocumentID

	// Register the new bundle in the Primary database's "Bundles" catalog
	// This will be handled by the catalog service at a higher level to avoid circular imports
	err = s.registerBundleInPrimary(bundle)
	if err != nil {
		s.logger.Warnf("Warning: Failed to register bundle '%s' in Primary catalog: %v", bundle.Name, err)
		// Don't fail the bundle creation if catalog registration fails
	}

	return nil
}

// RegisterExistingBundle registers an existing bundle from disk into the BundleService in-memory state.
// Unlike AddBundle/AddBundleByStruct, this does NOT create files on disk — it loads metadata
// from existing .bnd files. Used by ATTACH DATABASE to make discovered bundles queryable.
func (s *BundleService) RegisterExistingBundle(db *models.Database, bundleName string) error {
	// Try to load bundle metadata from disk
	databasePath := helpers.GetDatabaseFolderPath(db.Name)
	bndFileName := fmt.Sprintf("%s_%s.bnd", db.Name, bundleName)

	bundle, err := s.store.LoadBundleMetadata(db, databasePath, bndFileName)
	if err != nil {
		// Fall back: register a shell bundle so it's at least discoverable
		shellBundle := &models.Bundle{
			Name:     bundleName,
			Database: db,
		}
		s.bundleMetadata[bundleName] = shellBundle
		s.logger.Warnf("Bundle '%s' registered with minimal metadata (load error: %v)", bundleName, err)
		return nil
	}

	bundle.Database = db

	// Load the SortedIndex for this bundle
	if err := InitializeBundleSortedIndex(bundle); err != nil {
		s.logger.Warnf("Failed to load sorted index for attached bundle '%s': %v", bundleName, err)
		if bundle.SortedIndex == nil {
			bundle.SortedIndex = models.NewShardedSortedIndex()
		}
	}

	// Discover existing indexes
	if len(bundle.Indexes) == 0 {
		if err := s.discoverBundleIndexes(bundle); err != nil {
			s.logger.Warnf("Failed to discover indexes for attached bundle '%s': %v", bundleName, err)
		}
	}

	s.bundleMetadata[bundleName] = bundle
	s.logger.Debugf("Registered existing bundle '%s' from disk into BundleService", bundleName)
	return nil
}

// DetachBundle removes a bundle from the BundleService in-memory state without deleting files on disk.
// Used by DETACH DATABASE to cleanly remove bundles from the query engine.
func (s *BundleService) DetachBundle(db *models.Database, name string) error {
	// Remove from bundleMetadata
	delete(s.bundleMetadata, name)

	// Remove any loaded document pages for this bundle from all shards
	prefix := name + ":"
	for i := 0; i < PageCacheShardCount; i++ {
		shard := s.pageShards[i]
		shard.mu.Lock()
		keysToDelete := make([]string, 0, 8)
		for pageKey := range shard.pages {
			if strings.HasPrefix(pageKey, prefix) {
				keysToDelete = append(keysToDelete, pageKey)
			}
		}
		for _, key := range keysToDelete {
			if elem, exists := shard.lruElements[key]; exists {
				shard.lruOrder.Remove(elem)
				delete(shard.lruElements, key)
			}
			shard.deleteLocked(key)
		}
		shard.mu.Unlock()
	}

	// Remove from database's Bundles map
	delete(db.Bundles, name)

	s.logger.Debugf("Detached bundle '%s' from in-memory state (files preserved on disk)", name)
	return nil
}

// GetBundleMetadata retrieves only the bundle structure/metadata without documents
func (s *BundleService) GetBundleMetadata(database *models.Database, name string) (*models.Bundle, error) {
	//args := settings.GetSettings()
	fileExists := s.store.BundleFileExists(name, database.Name)

	// Check if the bundle file exists in the store
	if !fileExists {
		return nil, errors.New(errors.ERR_NOT_FOUND_BUNDLE, fmt.Sprintf("Bundle file '%s' does not exist on disk", name), errors.LayerStorage).WithContext("bundle_name", name)
	}

	bundle, exists := s.bundleMetadata[name]
	if !exists {
		if fileExists {
			// If the bundle exists in the store but not in memory, load metadata only
			// if args.Debug {
			// 	s.logger.Debugf("Bundle metadata '%s' not found in memory, loading from store", name)
			// }

			databasePath := helpers.GetDatabaseFolderPath(database.Name)

			bundle, err := s.store.LoadBundleMetadata(database, databasePath, fmt.Sprintf("%s_%s.bnd", database.Name, name))
			if err != nil {
				return nil, errors.WrapWithMessage(err, errors.ERR_INTERNAL_STORAGE, fmt.Sprintf("Failed to load bundle metadata '%s'", name), errors.LayerStorage).WithContext("bundle_name", name)
			}

			// Load the SortedIndex for this bundle (for pageID alignment)
			if err := InitializeBundleSortedIndex(bundle); err != nil {
				s.logger.Warnf("Failed to load sorted index for bundle '%s': %v (will use fallback pageID calculation)", name, err)
				// Ensure SortedIndex is never nil - create empty fallback
				if bundle.SortedIndex == nil {
					bundle.SortedIndex = models.NewShardedSortedIndex()
				}
			}

			// Discover and populate existing index files
			if len(bundle.Indexes) == 0 {
				err = s.discoverBundleIndexes(bundle)
				if err != nil {
					s.logger.Warnf("Failed to discover indexes for bundle '%s': %v", name, err)
					// Continue loading the bundle even if index discovery fails
				}
			}

			// if args.Debug {
			// 	s.logger.Debugf("Loaded bundle metadata '%s' from store", name)
			// }

			s.bundleMetadata[name] = bundle

			// Invalidate any page/document caches that were built before the
			// schema was available (e.g. by background cache warming). Those
			// cached documents only have Values=[DocumentID] because the
			// schemaProvider returned nil before bundleMetadata was populated.
			if bse, ok := s.store.(*bundlestore.BundleStorageEngine); ok {
				bse.InvalidateBundleCaches(name, database.Name)
			}
			s.invalidatePageCachesForBundle(name)

			return bundle, nil
		} else {
			return nil, errors.New(errors.ERR_INTERNAL_STORAGE, fmt.Sprintf("Bundle file exists in memory but not on disk. '%s_%s.bnd' not found", database.Name, name), errors.LayerStorage).WithContext("bundle_name", name)
		}
	}

	// Bundle exists in memory, but check if SortedIndex needs initialization
	if bundle.SortedIndex == nil {
		s.logger.Debugf("Bundle '%s' is in memory but SortedIndex is nil, initializing", name)
		if err := InitializeBundleSortedIndex(bundle); err != nil {
			s.logger.Warnf("Failed to load sorted index for cached bundle '%s': %v (will use fallback)", name, err)
			// Ensure SortedIndex is never nil - create empty fallback
			if bundle.SortedIndex == nil {
				bundle.SortedIndex = models.NewShardedSortedIndex()
			}
		}
	}

	// Bundle exists in memory, but check if indexes need to be discovered
	if len(bundle.Indexes) == 0 {
		s.logger.Debugf("Bundle '%s' is in memory but has no indexes, attempting discovery", name)
		err := s.discoverBundleIndexes(bundle)
		if err != nil {
			s.logger.Warnf("Failed to discover indexes for in-memory bundle '%s': %v", name, err)
		}
	}

	return bundle, nil
}

// DEPRECATED: GetBundleByName - replaced with GetBundleMetadata
// This method is kept temporarily for backward compatibility but should not load all documents
func (s *BundleService) GetBundleByName(database *models.Database, name string) (*models.Bundle, error) {
	// First, get the bundle metadata
	bundle, err := s.GetBundleMetadata(database, name)
	if err != nil {
		return nil, err
	}

	// Return metadata-only bundle - documents should be loaded on-demand via GetDocumentPage
	// WRITE-THROUGH CACHE: All document access goes through page cache now
	// The DocumentsComplete flag is no longer needed since we don't have a memtable
	bundle.DocumentsComplete = true // Always "complete" - all data in page cache

	return bundle, nil
}

func (s *BundleService) GetAllBundles() map[string]*models.Bundle {
	return s.bundleMetadata
}

func (s *BundleService) RemoveBundle(db *models.Database, name string) error {
	// Check if the bundle exists in metadata
	bundle, exists := s.bundleMetadata[name]
	if !exists {
		return errors.New(errors.ERR_NOT_FOUND_BUNDLE, fmt.Sprintf("Bundle '%s' not found", name), errors.LayerDomain)
	}

	// Remove the bundle from the store
	err := s.store.RemoveBundleFile(db, bundle.Name)
	if err != nil {
		return errors.WrapWithMessage(err, errors.ERR_INTERNAL_STORAGE, fmt.Sprintf("Failed to remove bundle '%s' from store", name), errors.LayerStorage).WithContext("bundle_name", name)
	}

	// Remove from metadata
	delete(s.bundleMetadata, name)

	// Remove any loaded document pages for this bundle from all shards
	prefix := name + ":"
	for i := 0; i < PageCacheShardCount; i++ {
		shard := s.pageShards[i]
		shard.mu.Lock()
		keysToDelete := make([]string, 0, 8)
		for pageKey := range shard.pages {
			if strings.HasPrefix(pageKey, prefix) {
				keysToDelete = append(keysToDelete, pageKey)
			}
		}
		for _, key := range keysToDelete {
			// Remove from LRU tracking
			if elem, exists := shard.lruElements[key]; exists {
				shard.lruOrder.Remove(elem)
				delete(shard.lruElements, key)
			}
			shard.deleteLocked(key)
		}
		shard.mu.Unlock()
	}

	return nil
}

func (s *BundleService) UpdateBundle(db *models.Database, bundleCommand models.BundleCommand) error {
	// Check if the bundle exists
	bundle, err := s.GetBundleByName(db, bundleCommand.BundleName)
	if err != nil {
		return fmt.Errorf("bundle '%s' not found", bundleCommand.BundleName)
	}

	// Update the bundle in the store
	err = s.store.UpdateBundleFile(db, bundle)
	if err != nil {
		return fmt.Errorf("failed to update bundle in store: %w", err)
	}

	return nil
}

// RenameBundle renames a bundle and updates all related files and database entries.
// It validates the new name, updates the bundle metadata file, renames the directory,
// and updates the entry in the primary database's Bundles bundle.

// validateBundleName validates that a bundle name follows the naming rules
func (s *BundleService) validateBundleName(name string) error {
	if name == "" {
		return fmt.Errorf("bundle name cannot be empty")
	}

	// Check for reserved _mv_ prefix (reserved for materialized views)
	if strings.HasPrefix(name, "_mv_") {
		return fmt.Errorf("bundle names cannot contain '_mv_' prefix (reserved for materialized views). Please choose a different bundle name")
	}

	// Bundle names should start with a letter and contain only alphanumeric characters and underscores
	if len(name) == 0 {
		return fmt.Errorf("bundle name cannot be empty")
	}

	// Check first character is a letter
	firstChar := rune(name[0])
	if !((firstChar >= 'a' && firstChar <= 'z') || (firstChar >= 'A' && firstChar <= 'Z')) {
		return fmt.Errorf("bundle name must start with a letter")
	}

	// Check remaining characters are alphanumeric or underscore
	for _, char := range name {
		if !((char >= 'a' && char <= 'z') ||
			(char >= 'A' && char <= 'Z') ||
			(char >= '0' && char <= '9') ||
			char == '_') {
			return fmt.Errorf("bundle name can only contain letters, numbers, and underscores")
		}
	}

	return nil
}

// updateBundleInSystemCatalog updates the bundle entry in the primary database's Bundles bundle
func (s *BundleService) updateBundleInSystemCatalog(database *models.Database, oldName, newName string) error {
	// Check if catalog service is available (injected via SetCatalogService)
	if s.catalogService == nil {
		s.logger.Warnf("Catalog service not available, skipping system catalog update for bundle rename")
		return fmt.Errorf("catalog service not initialized")
	}

	// Get the bundle to find its BundleID
	bundle, err := s.GetBundleByName(database, newName)
	if err != nil {
		return fmt.Errorf("failed to get bundle after rename: %w", err)
	}

	// Update the catalog with the new bundle name
	if err := s.catalogService.UpdateBundleNameInCatalog(
		bundle.BundleID,
		database.Name,
		oldName,
		newName,
	); err != nil {
		return fmt.Errorf("failed to update bundle in system catalog: %w", err)
	}

	s.logger.Debugf("Updated system catalog for bundle rename: '%s' -> '%s'", oldName, newName)

	// GRAPHQL INTEGRATION: Regenerate GraphQL schema after bundle rename
	// Bundle rename changes the GraphQL TypeName (e.g., "users" -> "User", "blog_posts" -> "BlogPost")
	// This creates a new schema version with the updated type name.
	//
	// This reuses the regenerateGraphQLSchema method for consistency.
	// Schema update failures are logged but don't fail the rename operation.
	if err := s.regenerateGraphQLSchema(bundle); err != nil {
		s.logger.Warnf("[GraphQL] Failed to regenerate schema after rename to '%s': %v. Rename was successful.",
			newName, err)
	}

	return nil
}

// registerBundleInPrimary adds the bundle information to the "Bundles" bundle in the Primary database
func (s *BundleService) registerBundleInPrimary(bundle *models.Bundle) error {
	// Since we can't directly import the server package due to circular dependency,
	// this method is meant to be overridden or called through the service manager
	// The actual registration logic is implemented in CatalogService.AddBundleToCatalog

	// TODO There are better ways to do this, gotta clean up the architecture in places
	s.logger.Debugf("Bundle '%s' needs to be registered in primary catalog (handled by CatalogService)", bundle.Name)
	return nil
}

// discoverBundleIndexes scans for existing index files and populates the bundle's Indexes field
// UPDATED By Dan: Now supports both legacy (.idx) and new header-based (.hidx) index files
// New naming convention: FieldName-fk.N.hidx (FK) or FieldName.N.hidx (regular)

func (s *BundleService) DeleteBundle(database *models.Database, bundleCommand *models.BundleCommand) error {

	bundle, err := s.GetBundleByName(database, bundleCommand.BundleName)
	if err != nil {
		return fmt.Errorf("failed to find bundle '%s' for deletion: %w", bundleCommand.BundleName, err)
	}

	// === VALIDATE REFERENTIAL INTEGRITY ===
	// Only perform validation if FORCE flag was not specified
	if !bundleCommand.HasForceSwitch {
		// Check if any other bundles have relationships pointing to this bundle
		validator := NewReferentialIntegrityValidator(s, s.logger)

		// Create operation-scoped cache
		bundleCache := make(map[string]*models.Bundle)

		// STEP 1: Validate relationship metadata (schema-level validation)
		violations := validator.ValidateIncomingRelationships(database, bundle.Name, bundleCache)
		if len(violations) > 0 {
			// Log first violation and return error
			firstViolation := violations[0]
			s.logger.Warnf("[REFINT] Found %d incoming relationship(s) that would be orphaned by deleting bundle '%s'",
				len(violations), bundle.Name)
			return fmt.Errorf("%s", firstViolation.Error())
		}

		// STEP 2: Validate document-level foreign key references (data-level validation)
		// This checks if any documents in other bundles actually reference documents in this bundle
		thorough := settings.GetSettings().RestrictValidationThorough
		sampleSize := settings.GetSettings().RestrictValidationSampleSize
		logProgress := settings.GetSettings().RestrictValidationLogProgress

		s.logger.Debugf("[DROP-RESTRICT] Starting document-level validation for bundle '%s' (thorough=%v, sampleSize=%d, logProgress=%v)",
			bundle.Name, thorough, sampleSize, logProgress)

		if err := validator.ValidateDropBundleDocumentReferences(database, bundle, bundleCache, thorough, sampleSize, logProgress); err != nil {
			s.logger.Warnf("[DROP-RESTRICT] Document-level validation failed for bundle '%s': %v", bundle.Name, err)
			return err
		}

		s.logger.Debugf("[DROP-RESTRICT] All validations passed for bundle '%s' - no violations found", bundle.Name)
	} else {
		s.logger.Warnf("[DROP-RESTRICT] FORCE flag specified - skipping referential integrity validation for bundle '%s'", bundle.Name)
	}

	// Close all indexes for this bundle
	if bundle.Indexes != nil {
		for indexName, indexRef := range bundle.Indexes {
			if indexRef.IndexInstance != nil {
				switch idx := indexRef.IndexInstance.(type) {
				case *hashindex.HashIndexV3:
					if err := idx.Close(); err != nil {
						s.logger.Warnf("Failed to close hash index '%s': %v", indexName, err)
					}
				case *btreeindexV2.BTreeIndex:
					if err := idx.Close(); err != nil {
						s.logger.Warnf("Failed to close btree index '%s': %v", indexName, err)
					}
				case *brinindex.BRINIndex:
					if err := idx.Close(); err != nil {
						s.logger.Warnf("Failed to close BRIN index '%s': %v", indexName, err)
					}
				}
			}
		}
	}

	// Remove the bundle from the file system
	if err := s.store.RemoveBundleFile(database, bundle.Name); err != nil {
		return fmt.Errorf("failed to delete bundle '%s': %w", bundle.Name, err)
	}

	// Remove the bundle from in-memory metadata
	delete(database.Bundles, bundle.Name)

	// CRITICAL FIX: Remove from bundleMetadata cache to prevent stale index references
	// When a bundle is dropped and recreated with the same name, the old bundle object
	// with closed index instances must be fully removed from memory. Without this,
	// the stale bundle entry remains in bundleMetadata with closed indexes, causing
	// "document not found in bundle" errors when the recreated bundle tries to use
	// the old closed index instances instead of creating fresh ones.
	delete(s.bundleMetadata, bundle.Name)

	// PHASE 5: Clear the document-page location cache using sharded cache
	s.documentPageCache.InvalidateBundle(bundle.Name)

	s.logger.Debugf("Cleared document-page cache for deleted bundle: %s", bundle.Name)

	// Remove bundle from plan-cache metadata to avoid unbounded growth
	s.removeBundleFromPlanCacheMetadata(bundle.Name)

	// GRAPHQL INTEGRATION: Tombstone all GraphQL schemas when bundle is deleted
	// This marks all schema versions as deleted in the schema file, preventing their use in queries.
	// The schema manager handles tombstoning all versions atomically.
	//
	// Important: This doesn't physically delete schemas from the file (they remain for audit/rollback),
	// but marks them as deleted so GraphQL queries will not use them.
	//
	// Note: Tombstoning failures are logged but don't fail the bundle deletion.
	// The bundle is already deleted from disk, so the operation is considered successful.
	if s.graphQLEnabled && database != nil {
		s.logger.Debugf("[GraphQL] Tombstoning schemas for deleted bundle '%s' in database '%s'", bundle.Name, database.Name)

		// PHASE 5: Get the schema manager using sharded map (no global lock)
		schemaManager, exists := s.schemaManagers.Get(database.Name)

		if exists && schemaManager != nil {
			// Tombstone all schema versions for this bundle
			// This is an atomic operation that marks all versions as deleted
			err := schemaManager.TombstoneAllSchemasForBundle(bundle.Name)
			if err != nil {
				s.logger.Warnf("[GraphQL] Failed to tombstone schemas for deleted bundle '%s': %v. Schemas may remain in cache.", bundle.Name, err)
			} else {
				s.logger.Debugf("[GraphQL] All schema versions tombstoned for deleted bundle '%s'", bundle.Name)
			}
		} else {
			s.logger.Debugf("[GraphQL] No schema manager found for database '%s' - skipping schema tombstoning", database.Name)
		}
	}

	return nil
}

func (s *BundleService) RenameBundle(database *models.Database, bundle *models.Bundle, newBundleName string) error {
	if bundle == nil {
		return fmt.Errorf("bundle is nil")
	}
	if newBundleName == "" {
		return fmt.Errorf("new bundle name cannot be empty")
	}
	if bundle.Name == newBundleName {
		return fmt.Errorf("new bundle name is the same as current name")
	}

	oldName := bundle.Name

	// Validate new bundle name follows naming rules
	if err := s.validateBundleName(newBundleName); err != nil {
		return fmt.Errorf("invalid bundle name '%s': %w", newBundleName, err)
	}

	// Check that new name doesn't already exist
	existingBundle, _ := s.GetBundleByName(database, newBundleName)
	if existingBundle != nil {
		return fmt.Errorf("bundle with name '%s' already exists in database '%s'", newBundleName, database.Name)
	}

	// OPERATION SAFETY: Wait for all active operations to complete before renaming
	// This prevents data corruption or inconsistencies during the rename process.
	// The timeout prevents indefinite waits in case of stuck operations.
	lock := s.getBundleLock(oldName)

	// TODO: Make timeout configurable via settings (currently 30 seconds)
	timeout := 30 * time.Second

	if err := lock.WaitForActiveOperations(timeout); err != nil {
		return fmt.Errorf("cannot rename bundle while operations are active: %w", err)
	}

	// Ensure we clear the rename flag even if the operation fails
	defer lock.CompleteAdministrativeOperation()

	// Get the bundle's current directory path
	databasePath := helpers.GetDatabaseFolderPath(database.Name)
	oldBundlePath := filepath.Join(databasePath, oldName)
	newBundlePath := filepath.Join(databasePath, newBundleName)

	// Verify old directory exists
	if _, err := os.Stat(oldBundlePath); os.IsNotExist(err) {
		return fmt.Errorf("bundle directory does not exist: %s", oldBundlePath)
	}

	// Update bundle metadata
	bundle.Name = newBundleName
	bundle.UpdatedAt = time.Now()

	// Rename the bundle directory (this includes indexes subfolder)
	if err := os.Rename(oldBundlePath, newBundlePath); err != nil {
		return fmt.Errorf("failed to rename bundle directory: %w", err)
	}

	// Update the bundle metadata file with new name
	if err := s.store.UpdateBundleFilename(database, bundle, oldName); err != nil {
		// Try to rollback directory rename
		if rollbackErr := os.Rename(newBundlePath, oldBundlePath); rollbackErr != nil {
			s.logger.Errorf("Failed to rollback directory rename: %v", rollbackErr)
		}
		bundle.Name = oldName // Restore old name in memory
		return fmt.Errorf("failed to update bundle metadata file: %w", err)
	}

	// Update the cache
	delete(s.bundleMetadata, oldName)
	s.bundleMetadata[newBundleName] = bundle

	// Invalidate page cache for old name
	s.invalidateBundlePageCache(oldName)

	// Update entry in primary database's "Bundles" bundle
	// This updates the system catalog that tracks all bundles
	if err := s.updateBundleInSystemCatalog(database, oldName, newBundleName); err != nil {
		s.logger.Warnf("Failed to update system catalog for bundle rename: %v", err)
		// Don't fail the operation - the bundle is already renamed on disk
	}

	s.logger.Debugf("Successfully renamed bundle '%s' to '%s'", oldName, newBundleName)
	return nil
}

func (s *BundleService) discoverBundleIndexes(bundle *models.Bundle) error {
	// Initialize indexes map if nil
	if bundle.Indexes == nil {
		bundle.Indexes = make(map[string]models.IndexReference)
	}

	databasePath := helpers.GetDatabaseFolderPath(bundle.Database.Name)
	indexesPath := filepath.Join(databasePath, bundle.Name, "indexes")

	// 1: Look for NEW header-based index files (.hidx)
	// Pattern: *.hidx (includes both FieldName-fk.N.hidx and FieldName.N.hidx)
	newHashPattern := filepath.Join(indexesPath, "*.hidx")
	newHashFiles, err := filepath.Glob(newHashPattern)
	if err != nil {
		s.logger.Warnf("Failed to scan for new hash index files: %v", err)
		newHashFiles = []string{} // Continue with legacy discovery
	}

	// Process new format files (.hidx with headers)
	for _, hashFile := range newHashFiles {
		baseName := filepath.Base(hashFile)

		// Parse new naming convention: FieldName-fk.N.hidx or FieldName.N.hidx
		// Remove extension
		nameWithoutExt := strings.TrimSuffix(baseName, ".hidx")

		// Split by last dot to separate file number
		parts := strings.Split(nameWithoutExt, ".")
		if len(parts) != 2 {
			s.logger.Warnf("Invalid new index file name format: %s", baseName)
			continue
		}

		fieldPart := parts[0]
		// fileNum := parts[1] // Not needed for discovery

		// Check if it's a foreign key index
		isForeignKey := strings.HasSuffix(fieldPart, "-fk")
		var fieldName string
		var indexName string

		if isForeignKey {
			// Foreign key: FieldName-fk
			fieldName = strings.TrimSuffix(fieldPart, "-fk")
			indexName = fieldName + "_fk" // Restore _fk for index name
		} else {
			// Regular index: FieldName
			fieldName = fieldPart
			indexName = fieldName
		}

		// Check if this field exists in the bundle's field definitions
		if bundle.DocumentStructure.FieldDefinitions != nil {
			if _, exists := bundle.DocumentStructure.FieldDefinitions[fieldName]; !exists {
				s.logger.Warnf("Found hash index file for field '%s' but field not defined in bundle '%s'", fieldName, bundle.Name)
				continue
			}
		}

		// Resolve field type from DocumentStructure when available
		fdType := "string"
		if bundle.DocumentStructure.FieldDefinitions != nil {
			if fd, ok := bundle.DocumentStructure.FieldDefinitions[fieldName]; ok {
				fdType = fd.Type
			}
		}
		// Create index reference (preserving _fk suffix in index name).
		// Fields must be set so HasIndexOnField/GetHashIndexForField can match by document field (e.g. product_id).
		indexRef := models.IndexReference{
			IndexName: indexName,
			IndexType: "hash",
			Fields:    []models.FieldDefinition{{Name: fieldName, Type: fdType}},
			HashIndexField: models.IndexField{
				FieldName: indexName, // Includes _fk if foreign key
			},
		}

		bundle.Indexes[indexName] = indexRef
		s.logger.Debugf("Discovered NEW hash index '%s' for field '%s' in bundle '%s' (FK=%v)",
			indexName, fieldName, bundle.Name, isForeignKey)
	}

	// 2: Look for LEGACY index files (.idx) - OLD FORMAT
	// Pattern: BundleName_*_*.idx
	legacyHashPattern := fmt.Sprintf("%s/%s_*_*.idx", indexesPath, bundle.Name)
	legacyHashFiles, err := filepath.Glob(legacyHashPattern)
	if err != nil {
		return fmt.Errorf("failed to scan for legacy hash index files: %w", err)
	}

	// Process legacy format files (.idx without headers)
	for _, hashFile := range legacyHashFiles {
		var fieldName string

		// Extract field name from filename: BundleName_FieldName_N.idx
		baseName := filepath.Base(hashFile)
		// Remove .idx extension
		baseName = strings.TrimSuffix(baseName, ".idx")
		// remove the bundle name prefix
		baseName = strings.TrimPrefix(baseName, bundle.Name+"_")

		// Strip the trailing index number by working backwards from the end of the string
		underscoreIndex := strings.LastIndex(baseName, "_")
		if underscoreIndex != -1 {
			baseName = baseName[:underscoreIndex]
		}

		// What is left SHOULD be the field name (with _fk if foreign key)
		indexName := baseName

		// For field validation, strip _fk suffix
		fieldName = strings.TrimSuffix(baseName, "_fk")

		// Check if this field exists in the bundle's field definitions
		if bundle.DocumentStructure.FieldDefinitions != nil {
			if _, exists := bundle.DocumentStructure.FieldDefinitions[fieldName]; !exists {
				s.logger.Warnf("Found legacy hash index file for field '%s' but field not defined in bundle '%s'", fieldName, bundle.Name)
				continue
			}
		}

		// Only add if not already discovered as new format
		if _, exists := bundle.Indexes[indexName]; !exists {
			fdType := "string"
			if bundle.DocumentStructure.FieldDefinitions != nil {
				if fd, ok := bundle.DocumentStructure.FieldDefinitions[fieldName]; ok {
					fdType = fd.Type
				}
			}
			// Create index reference (preserving _fk suffix). Fields required for join index lookup.
			indexRef := models.IndexReference{
				IndexName: indexName,
				IndexType: "hash",
				Fields:    []models.FieldDefinition{{Name: fieldName, Type: fdType}},
				HashIndexField: models.IndexField{
					FieldName: indexName, // Preserve _fk suffix
				},
			}

			bundle.Indexes[indexName] = indexRef
			s.logger.Debugf("Discovered LEGACY hash index '%s' for field '%s' in bundle '%s'", indexName, fieldName, bundle.Name)
		}
	}

	// TODO: Add discovery for BTree indexes when they have a consistent file pattern
	// Look for btree index files if there's a predictable naming pattern

	s.logger.Debugf("Discovered %d total indexes for bundle '%s' (%d new format, %d legacy format)",
		len(bundle.Indexes), bundle.Name, len(newHashFiles), len(legacyHashFiles))
	return nil
}
