package bundle

import (
	"fmt"
	"os"
	"path/filepath"
	"syndrdb/src/internal/domain/index/btreeindexV2"
	"syndrdb/src/internal/domain/models"
	"syndrdb/src/internal/registry"

	"syndrdb/src/pkg/settings"
	"time"

	"syndrdb/src/pkg/common/helpers"

	"go.uber.org/zap"
)

/*
Unique Constraint Enforcement Module

This module handles validation and enforcement of IsUnique field constraints.

Design Philosophy:
- Single Responsibility: Only handles unique constraint validation
- DRY: Reuses existing hash index infrastructure for O(1) lookups
- Open/Closed: Easy to extend with additional constraint types
- Performance: O(1) uniqueness checks via hash indexes

Architecture:
Leverages SyndrDB's existing HashIndexV3 infrastructure to enforce uniqueness
without requiring full table scans. Unique fields automatically get hash indexes
created at bundle creation time.
*/

// UniqueConstraintValidator handles validation of unique field constraints
type UniqueConstraintValidator struct {
	bundleService *BundleService
	logger        *zap.SugaredLogger
}

// NewUniqueConstraintValidator creates a new unique constraint validator
func NewUniqueConstraintValidator(bundleService *BundleService, logger *zap.SugaredLogger) *UniqueConstraintValidator {
	return &UniqueConstraintValidator{
		bundleService: bundleService,
		logger:        logger,
	}
}

// ValidateUniqueConstraints checks if any unique field constraints would be violated
// by inserting the provided document.
//
// This method examines all fields marked with IsUnique=true and verifies that
// no existing document in the bundle has the same value for those fields.
//
// Performance Characteristics:
// - O(k) where k = number of unique fields (typically 1-3)
// - Each uniqueness check is O(1) via hash index lookup
// - No full table scans required
//
// Parameters:
//   - bundle: The bundle containing the field definitions
//   - docCommand: The document command with field values to validate
//
// Returns:
//   - error: Non-nil if any unique constraint would be violated
//
// Example:
//
//	validator := NewUniqueConstraintValidator(bundleService, logger)
//	err := validator.ValidateUniqueConstraints(bundle, docCommand)
//	if err != nil {
//	    // Handle unique constraint violation
//	    return fmt.Errorf("unique constraint violation: %w", err)
//	}
//
// Error Response Examples:
//   - "Unique constraint violation: Field 'Email' must be unique, but value 'john@example.com' already exists in document 'doc_123'"
//   - "Multiple unique constraint violations: [Email, Username]"
func (v *UniqueConstraintValidator) ValidateUniqueConstraints(bundle *models.Bundle, docCommand *models.DocumentCommand) error {
	if bundle == nil {
		return fmt.Errorf("bundle cannot be nil")
	}
	if bundle.DocumentStructure.FieldDefinitions == nil {
		return fmt.Errorf("bundle '%s' has no field definitions", bundle.Name)
	}

	// Collect all unique constraint violations
	violations := make([]string, 0, 5)

	// Check each field in the document command
	loopStart := time.Now()
	for _, field := range docCommand.Fields {
		fieldName := field.Key
		fieldValue := field.Value

		// Get field definition
		fieldDef, exists := bundle.DocumentStructure.FieldDefinitions[fieldName]
		if !exists {
			// Field validation should have caught this already
			continue
		}

		// Skip if field is not marked as unique
		if !fieldDef.IsUnique {
			continue
		}

		v.logger.Infof("[UNIQUE] Validating uniqueness for field '%s' with value '%v'", fieldName, fieldValue)

		// Validate uniqueness for this field
		fieldStart := time.Now()
		violation, err := v.validateFieldUniqueness(bundle, fieldName, fieldValue)
		fieldDuration := time.Since(fieldStart)
		if fieldDuration > 1*time.Millisecond {
			v.logger.Warnf("    ⚠️  validateFieldUniqueness('%s') took %v", fieldName, fieldDuration)
		} else {
			v.logger.Debugf("    ✓ validateFieldUniqueness('%s') took %v", fieldName, fieldDuration)
		}
		if err != nil {
			// Technical error (index not found, etc.) - log warning but continue
			v.logger.Warnf("Failed to validate uniqueness for field '%s': %v", fieldName, err)
			violations = append(violations, fmt.Sprintf("field '%s': validation error: %v", fieldName, err))
			continue
		}

		if violation != "" {
			violations = append(violations, violation)
		}
	}
	loopDuration := time.Since(loopStart)
	if loopDuration > 1*time.Millisecond {
		v.logger.Warnf("    ⚠️  Field loop took %v", loopDuration)
	} else {
		v.logger.Debugf("    ✓ Field loop took %v", loopDuration)
	}

	// If any violations found, return detailed error
	if len(violations) > 0 {
		return v.formatViolationError(bundle.Name, violations)
	}

	v.logger.Debugf("All unique constraints validated successfully for bundle '%s'", bundle.Name)
	return nil
}

// validateFieldUniqueness checks if a specific field value violates uniqueness constraint
//
// This method performs an O(1) hash index lookup to check if the value already exists.
//
// Returns:
//   - violation string: Non-empty if violation detected
//   - error: Non-nil if technical error occurred (index not found, etc.)
func (v *UniqueConstraintValidator) validateFieldUniqueness(
	bundle *models.Bundle,
	fieldName string,
	fieldValue interface{},
) (string, error) {
	// Convert field value to string for index lookup
	valueStr := fmt.Sprintf("%v", fieldValue)

	// Look for a unique index on this field
	// Index naming convention: "{fieldName}_unique"
	indexName := fmt.Sprintf("%s_unique", fieldName)
	indexRef, found := bundle.Indexes[indexName]

	if !found {
		// TODO (PHASE 2): Implement fallback to scan if index doesn't exist
		// For now, require index to exist for performance reasons
		return "", fmt.Errorf("no unique index found on field '%s' (expected '%s') - index required for uniqueness enforcement", fieldName, indexName)
	}

	v.logger.Debugf("Using unique index '%s' (type: %s) for field '%s' uniqueness check", indexName, indexRef.IndexType, fieldName)

	// POSTGRESQL-STYLE: Check B-tree index first (in-memory), fall back to hash (disk) if needed
	if indexRef.IndexType == "btree" {
		checkStart := time.Now()
		result, err := v.checkBTreeIndexForDuplicates(bundle, indexName, indexRef, fieldName, valueStr)
		checkDuration := time.Since(checkStart)
		if checkDuration > 100*time.Microsecond {
			v.logger.Warnf("      ⚠️  checkBTreeIndexForDuplicates took %v (expected <100μs for in-memory)", checkDuration)
		} else {
			v.logger.Debugf("      ⚡ checkBTreeIndexForDuplicates took %v (in-memory)", checkDuration)
		}
		return result, err
	} else if indexRef.IndexType == "hash" {
		// FALLBACK: Hash index for disk-based lookups (slower, 5-7ms)
		checkStart := time.Now()
		result, err := v.checkHashIndexForDuplicates(bundle, indexName, indexRef, fieldName, valueStr)
		checkDuration := time.Since(checkStart)
		if checkDuration > 1*time.Millisecond {
			v.logger.Warnf("      ⚠️  checkHashIndexForDuplicates took %v (disk-based fallback)", checkDuration)
		} else {
			v.logger.Debugf("      ✓ checkHashIndexForDuplicates took %v", checkDuration)
		}
		return result, err
	}

	return "", fmt.Errorf("unsupported index type '%s' for unique constraint", indexRef.IndexType)
}

// checkHashIndexForDuplicates performs the actual hash index lookup for duplicates
//
// This is the performance-critical path: O(1) hash lookup to check for existing value
func (v *UniqueConstraintValidator) checkHashIndexForDuplicates(
	bundle *models.Bundle,
	indexName string,
	indexRef models.IndexReference,
	fieldName string,
	valueStr string,
) (string, error) {
	// Load hash index using existing infrastructure (DRY principle)
	loadStart := time.Now()
	hashIndex, err := v.bundleService.GetOrLoadHashIndex(bundle, indexName, indexRef)
	loadDuration := time.Since(loadStart)
	if loadDuration > 1*time.Millisecond {
		v.logger.Warnf("        ⚠️  GetOrLoadHashIndex took %v", loadDuration)
	} else {
		v.logger.Debugf("        ✓ GetOrLoadHashIndex took %v", loadDuration)
	}
	if err != nil {
		return "", fmt.Errorf("failed to load unique index '%s': %w", indexName, err)
	}

	// Perform O(1) hash lookup to check if value already exists
	v.logger.Infof("[UNIQUE] Searching unique index '%s' for value '%s'", indexName, valueStr)
	searchStart := time.Now()
	results, err := hashIndex.Search(valueStr)
	searchDuration := time.Since(searchStart)
	if searchDuration > 1*time.Millisecond {
		v.logger.Warnf("        ⚠️  hashIndex.Search took %v", searchDuration)
	} else {
		v.logger.Debugf("        ✓ hashIndex.Search took %v", searchDuration)
	}
	if err != nil {
		return "", fmt.Errorf("failed to search unique index '%s': %w", indexName, err)
	}

	// PERFORMANCE FIX: Trust the hash index without GetDocument verification
	// GetDocument causes full page scans (10ms per validation). The hash index
	// is the source of truth for uniqueness. Deleted documents will be cleaned
	// during compaction, but for active writes we assume index is current.
	//
	// Trade-off: Extremely rare case where deleted doc not yet compacted could
	// cause false positive uniqueness violation, but this is acceptable vs 10ms
	// penalty on every single document insertion.
	if len(results) > 0 {
		existingDocID := results[0] // Take first match (any match is a violation)
		violation := fmt.Sprintf("Unique constraint violation: Field '%s' must be unique, but value '%s' already exists in document '%s'",
			fieldName, valueStr, existingDocID)
		v.logger.Warnf("[UNIQUE] %s", violation)
		return violation, nil
	}

	v.logger.Infof("[UNIQUE] No duplicates found for field '%s' value '%s'", fieldName, valueStr)
	return "", nil
}

// checkBTreeIndexForDuplicates performs B-tree index lookup for unique constraint validation
// This is the fast path for in-memory indexes: O(log n) lookup, typically <100μs
// B-tree indexes are loaded into memory by LoadDatabaseIndexes() on database context switch
func (v *UniqueConstraintValidator) checkBTreeIndexForDuplicates(
	bundle *models.Bundle,
	indexName string,
	indexRef models.IndexReference,
	fieldName string,
	valueStr string,
) (string, error) {
	v.logger.Debugf("[UNIQUE] Searching unique B-tree index '%s' for value '%s'", indexName, valueStr)

	// Try to load the B-tree index from cache or disk
	btreeIndex, err := v.bundleService.getOrLoadBTreeIndex(bundle, indexName, indexRef)
	if err != nil {
		return "", fmt.Errorf("failed to load B-tree index '%s': %w", indexName, err)
	}

	// Search for existing value in the B-tree (Exists is lean: no stats/logging)
	keyBytes := []byte(valueStr)
	exists, err := btreeIndex.Exists(keyBytes)
	if err != nil {
		return "", fmt.Errorf("B-tree index search failed for field '%s': %w", fieldName, err)
	}

	if exists {
		v.logger.Debugf("[UNIQUE] Duplicate value found in B-tree index '%s': value '%s' already exists",
			indexName, valueStr)
		return fmt.Sprintf("field '%s' with value '%s' already exists (unique constraint violation)",
			fieldName, valueStr), nil
	}

	v.logger.Debugf("[UNIQUE] No duplicates found for field '%s' value '%s' in B-tree index", fieldName, valueStr)
	return "", nil
}

// formatViolationError creates a user-friendly error message for unique constraint violations
func (v *UniqueConstraintValidator) formatViolationError(bundleName string, violations []string) error {
	if len(violations) == 1 {
		return fmt.Errorf("%s", violations[0])
	}

	// Multiple violations - format as list
	errorMsg := fmt.Sprintf("Cannot insert document into bundle '%s' - %d unique constraint violation(s):\n", bundleName, len(violations))
	for i, violation := range violations {
		errorMsg += fmt.Sprintf("  %d. %s\n", i+1, violation)
	}

	return fmt.Errorf("%s", errorMsg)
}

// CreateUniqueIndexesForBundle automatically creates hash indexes for all fields marked IsUnique
//
// This method should be called when a bundle is created or loaded to ensure
// all unique fields have the necessary indexes for enforcement.
//
// Performance Characteristics:
// - O(n) where n = number of unique fields
// - Each index creation is a one-time operation
// - Indexes are persisted and loaded on subsequent runs
//
// Parameters:
//   - bundle: The bundle to create unique indexes for
//
// Returns:
//   - error: Non-nil if any index creation fails
//
// Example:
//
//	validator := NewUniqueConstraintValidator(bundleService, logger)
//	err := validator.CreateUniqueIndexesForBundle(bundle)
//	if err != nil {
//	    return fmt.Errorf("failed to create unique indexes: %w", err)
//	}
func (v *UniqueConstraintValidator) CreateUniqueIndexesForBundle(bundle *models.Bundle) error {
	if bundle == nil {
		return fmt.Errorf("bundle cannot be nil")
	}
	if bundle.DocumentStructure.FieldDefinitions == nil {
		v.logger.Debugf("Bundle '%s' has no field definitions, skipping unique index creation", bundle.Name)
		return nil
	}

	// Find all fields marked as unique
	uniqueFields := make([]models.FieldDefinition, 0, 10)
	for _, fieldDef := range bundle.DocumentStructure.FieldDefinitions {
		if fieldDef.IsUnique {
			uniqueFields = append(uniqueFields, fieldDef)
		}
	}

	if len(uniqueFields) == 0 {
		v.logger.Debugf("Bundle '%s' has no unique fields, no indexes to create", bundle.Name)
		return nil
	}

	v.logger.Infof("[UNIQUE] Creating unique indexes for %d field(s) in bundle '%s'", len(uniqueFields), bundle.Name)

	// Create hash index for each unique field
	for _, fieldDef := range uniqueFields {
		// Skip DocumentID - it already has a unique index created automatically
		if fieldDef.Name == "DocumentID" {
			v.logger.Debugf("Skipping DocumentID - already has unique index")
			continue
		}

		indexName := fmt.Sprintf("%s_unique", fieldDef.Name)

		// Check if index already exists
		if _, exists := bundle.Indexes[indexName]; exists {
			v.logger.Debugf("Unique index '%s' already exists for field '%s'", indexName, fieldDef.Name)
			continue
		}

		// POSTGRESQL-STYLE: Create B-tree index for unique field (stored in memory)
		// Hash indexes are disk-based with lazy loading, B-trees can be loaded into memory
		v.logger.Infof("[UNIQUE] Creating unique B-tree index '%s' for field '%s'", indexName, fieldDef.Name)
		err := createBTreeIndexForUniqueField(v.bundleService, bundle, indexName, fieldDef)
		if err != nil {
			return fmt.Errorf("failed to create unique B-tree index '%s' for field '%s': %w", indexName, fieldDef.Name, err)
		}

		// OLD HASH INDEX APPROACH (kept commented for potential disk-based fallback)
		// err := createHashIndexInternal(v.bundleService, bundle, indexName)

		v.logger.Infof("[UNIQUE] Successfully created unique B-tree index '%s' for field '%s'", indexName, fieldDef.Name)
	}

	return nil
}

// TODO (FUTURE EXTENSION): Support for composite unique constraints
// Example: UNIQUE(firstName, lastName) - combination must be unique
//
// type CompositeUniqueConstraint struct {
//     Name       string
//     FieldNames []string
// }
//
// func (v *UniqueConstraintValidator) ValidateCompositeUniqueness(bundle *Bundle, constraint CompositeUniqueConstraint, values map[string]interface{}) error

// TODO (FUTURE EXTENSION): Support for conditional uniqueness
// Example: UNIQUE WHERE isActive = true
//
// type ConditionalUniqueConstraint struct {
//     FieldName string
//     Condition string // WHERE clause
// }
//
// func (v *UniqueConstraintValidator) ValidateConditionalUniqueness(bundle *Bundle, constraint ConditionalUniqueConstraint, value interface{}) error

// TODO (FUTURE EXTENSION): Support for case-insensitive uniqueness
// Example: Email addresses should be unique regardless of case
//
// func (v *UniqueConstraintValidator) ValidateCaseInsensitiveUniqueness(bundle *Bundle, fieldName string, value string) error

// TODO (FUTURE EXTENSION): Support for nullable unique fields
// Currently, multiple NULL values would violate uniqueness, but SQL allows it
// Decision: Should NULL == NULL for uniqueness purposes?
//
// func (v *UniqueConstraintValidator) ValidateNullableUniqueness(bundle *Bundle, fieldName string, value interface{}) error

// createBTreeIndexForUniqueField creates a B-tree index for a unique field constraint
// This follows PostgreSQL's approach: UNIQUE constraints automatically create B-tree indexes
// The index will be loaded into memory by LoadDatabaseIndexes() for fast lookups
//
// Parameters:
//   - s: BundleService instance
//   - bundle: Bundle to create index for
//   - indexName: Name of the unique index (format: "{fieldName}_unique")
//   - fieldDef: Field definition containing IsUnique=true
//
// Returns:
//   - error: Any error during index creation or population
func createBTreeIndexForUniqueField(s *BundleService, bundle *models.Bundle, indexName string, fieldDef models.FieldDefinition) error {
	args := settings.GetSettings()

	// Validate that field is marked unique
	if !fieldDef.IsUnique {
		return fmt.Errorf("field '%s' is not marked as unique, cannot create unique B-tree index", fieldDef.Name)
	}

	s.logger.Infof("Creating unique B-tree index '%s' on field '%s' for bundle '%s'",
		indexName, fieldDef.Name, bundle.Name)

	// Get proper database path structure (same as hash index)
	databasePath := helpers.GetDatabaseFolderPath(bundle.Database.Name)

	// CRITICAL: Construct full B-tree indexes path to match bundle structure
	// B-tree indexes must be stored in: database/bundle/indexes/btree/
	// Format: /data_dir/<database>/<bundle>/indexes/btree/<btree-index-file-name>.btidx
	btreeIndexesPath := filepath.Join(databasePath, bundle.Name, "indexes", "btree")

	// Calculate optimal split ratio for unique indexes (higher fill factor)
	splitRatio := calculateOptimalSplitRatio(fieldDef, true)

	// Create configuration for the B-tree index
	config := btreeindexV2.IndexConfig{
		IndexName:    indexName, // CRITICAL: Must set IndexName for proper file path construction
		DatabaseName: bundle.Database.Name,
		BundleName:   bundle.Name,
		FieldName:    fieldDef.Name,
		IsUnique:     true,             // Enforce uniqueness at index level
		IndexDir:     btreeIndexesPath, // Use full path: database/bundle/indexes/btree
		DebugMode:    args.Debug,
		PageSize:     8192,       // 8KB pages (PostgreSQL-style)
		CacheSize:    100,        // Cache 100 pages
		FillFactor:   0.8,        // 80% fill factor for unique indexes (higher than default)
		MaxKeyLength: 2048,       // 2KB max key length
		SplitRatio:   splitRatio, // Optimized split ratio
	}

	// Configure WAL for durability (reuse service registry pattern)
	serviceRegistry := registry.GetRegistry()
	if serviceRegistry.IsWALAvailable() {
		config.WALManager = serviceRegistry.GetWALManager()
		s.logger.Debugf("WAL enabled for unique B-tree index '%s'", indexName)
	}

	// Ensure the btree indexes directory exists before creating the index
	if err := os.MkdirAll(btreeIndexesPath, 0755); err != nil {
		s.logger.Errorf("Failed to create btree indexes directory: %v", err)
		return fmt.Errorf("failed to create btree indexes directory: %w", err)
	}

	// Create the B-tree index
	btreeIndex, err := btreeindexV2.CreateBTreeIndex(&config, s.logger)
	if err != nil {
		s.logger.Errorf("Failed to create B-tree index: %v", err)
		return fmt.Errorf("failed to create B-tree index: %w", err)
	}

	// Populate index with existing documents from bundle
	s.logger.Debugf("Populating unique B-tree index with documents from bundle '%s'", bundle.Name)
	allDocuments, err := s.getAllDocumentsForIndexing(bundle.Name, 0, 0, nil)
	if err != nil {
		s.logger.Warnf("Failed to load documents for indexing: %v", err)
		return err
	}

	if len(allDocuments) > 0 {
		s.logger.Debugf("Populating unique B-tree index with %d existing documents", len(allDocuments))

		for documentID, document := range allDocuments {
			// Extract field value for indexing
			fieldValue, err := extractFieldValueForIndex(*document, fieldDef.Name)
			if err != nil {
				s.logger.Warnf("Failed to extract field value for document '%s': %v", documentID, err)
				continue
			}

			// Convert to bytes for B-tree storage
			keyBytes, err := convertValueToBytes(fieldValue)
			if err != nil {
				s.logger.Warnf("Failed to convert field value to bytes for document '%s': %v", documentID, err)
				continue
			}

			// Insert into B-tree - will fail if duplicate (unique constraint)
			err = btreeIndex.Insert(keyBytes, document.DocumentID)
			if err != nil {
				s.logger.Errorf("Failed to insert document '%s' into unique B-tree index: %v", documentID, err)
				btreeIndex.Close()
				return fmt.Errorf("failed to populate unique B-tree index - duplicate value found during initial load: %w", err)
			}
		}

		if err := btreeIndex.PersistMetadata(); err != nil {
			s.logger.Warnf("Failed to persist unique B-tree index metadata after population: %v", err)
		}
		s.logger.Infof("Successfully populated unique B-tree index with %d documents", len(allDocuments))
	} else {
		s.logger.Debugf("No existing documents to populate unique B-tree index")
	}

	// Create index field structure
	indexField := models.IndexField{
		FieldName: fieldDef.Name,
		IsUnique:  true,
		Collation: "",
	}

	// Create index reference
	indexRef := models.IndexReference{
		IndexName:       indexName,
		Fields:          []models.FieldDefinition{fieldDef},
		IndexType:       "btree", // POSTGRESQL-STYLE: B-tree for unique constraints
		CreateTime:      time.Now(),
		IndexInstance:   btreeIndex, // Will be cleared after close, loaded on-demand
		BTreeIndexField: indexField,
	}

	// Initialize indexes map if nil
	if bundle.Indexes == nil {
		bundle.Indexes = make(map[string]models.IndexReference)
	}

	// Add index to bundle
	bundle.Indexes[indexName] = indexRef
	bundle.IndexNames = append(bundle.IndexNames, indexName)

	// Update bundle file
	err = s.store.UpdateBundleFile(bundle.Database, bundle)
	if err != nil {
		s.logger.Errorf("Failed to update bundle file: %v", err)
		return fmt.Errorf("failed to update bundle file: %w", err)
	}

	// Update metadata size estimate
	btreeIndex.Metadata.UpdateMemorySize()

	s.logger.Infof("Successfully created unique B-tree index '%s' for field '%s' (%d MB estimated memory)",
		indexName, fieldDef.Name, btreeIndex.Metadata.EstimatedMemorySizeBytes/(1024*1024))

	return nil
}
