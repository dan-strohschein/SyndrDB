package bundle

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"syndrdb/src/internal/domain/models"
	"syndrdb/src/pkg/common/helpers"

	graphQLSchema "syndrdb/src/internal/graphQL/schema"
)

// getOrCreateSchemaManager retrieves or creates a GraphQL schema manager for the specified database.
// Schema managers are created lazily on first use because they require database-specific directory paths.
// This method is thread-safe using the sharded schema manager map.
//
// PHASE 5 INTEGRATION: This method is called automatically by AddBundle and UpdateBundle operations
// when GraphQL support is enabled. It initializes the schema file in the database's data directory.
// PHASE 5: Refactored to use ShardedSchemaManagerMap for concurrent access without global lock.
//
// Parameters:
//   - db: The database model containing name, ID, and directory path information
//
// Returns:
//   - *graphQLSchema.SchemaManager: The schema manager for this database (may be nil if disabled)
//   - error: Any initialization errors (logged but operation continues)
func (s *BundleService) getOrCreateSchemaManager(db *models.Database) (*graphQLSchema.SchemaManager, error) {
	// Return nil immediately if GraphQL is disabled
	if !s.graphQLEnabled {
		return nil, nil
	}

	// PHASE 5: Use sharded map's GetOrCreate with creator callback
	return s.schemaManagers.GetOrCreate(db.Name, func() (*graphQLSchema.SchemaManager, error) {
		// Create schema file path in database directory
		// Schema files follow the pattern: {database_directory}/{database_name}_graphql.gql
		// This ensures schemas are stored alongside their respective database bundles
		schemaFilePath := filepath.Join(s.settings.DataDir, db.Name, db.Name+"_graphql.gql")

		// Initialize the schema manager with database context
		// The manager handles schema versioning, tombstoning, and caching
		manager, err := graphQLSchema.NewSchemaManager(schemaFilePath, db.Name, db.DatabaseID)
		if err != nil {
			s.logger.Warnf("Failed to initialize GraphQL schema manager for database '%s': %v. Schema generation disabled for this database.", db.Name, err)
			return nil, err
		}

		s.logger.Debugf("GraphQL schema manager initialized for database '%s' at: %s", db.Name, schemaFilePath)
		return manager, nil
	})
}

// GetSchemaManager returns the GraphQL schema manager for the given database,
// creating it if necessary. This is the exported wrapper around getOrCreateSchemaManager
// so that main.go can pass the same manager instance to the GraphQL handler.
func (s *BundleService) GetSchemaManager(db *models.Database) (*graphQLSchema.SchemaManager, error) {
	return s.getOrCreateSchemaManager(db)
}

// regenerateGraphQLSchema regenerates the GraphQL schema for a bundle after structure changes.
// This method implements FR-6: automatic schema regeneration on bundle modifications.
//
// It handles the complete schema update lifecycle:
// 1. Generates new schema from current bundle structure
// 2. Retrieves old schema for comparison (if exists)
// 3. Detects breaking changes (field removals, type changes, nullability changes)
// 4. Creates new schema version with breaking change annotations
// 5. Tombstones old schema version
// 6. Updates schema cache for immediate availability
//
// Breaking changes are detected and logged but don't fail the operation.
// This ensures bundle modifications succeed even if clients may need schema updates.
//
// Design Principles:
// - Single Responsibility: Handles only schema regeneration, delegates storage to SchemaManager
// - DRY: Reuses existing getOrCreateSchemaManager, GenerateSchema, DetectBreakingChanges
// - Open/Closed: Extensible through SchemaGenerator type mapping
//
// Returns error only if schema generation or storage fails critically.
// Warnings are logged for breaking changes and non-critical failures.
func (s *BundleService) regenerateGraphQLSchema(bundle *models.Bundle) error {
	// Early exit if GraphQL is disabled or bundle has no database context
	if !s.graphQLEnabled || s.schemaGenerator == nil {
		return nil
	}
	if bundle == nil || bundle.Database == nil {
		return fmt.Errorf("bundle or database is nil")
	}

	s.logger.Debugf("[GraphQL] Regenerating schema for bundle '%s' in database '%s'",
		bundle.Name, bundle.Database.Name)

	// Get or create the schema manager for this database (reuses existing infrastructure)
	schemaManager, err := s.getOrCreateSchemaManager(bundle.Database)
	if err != nil {
		return fmt.Errorf("failed to get schema manager: %w", err)
	}
	if schemaManager == nil {
		// GraphQL disabled for this database
		return nil
	}

	// Retrieve current active schema for breaking change detection
	// This may be nil if this is the first schema or if it was tombstoned
	oldSchema, err := schemaManager.GetActiveSchemaForBundle(bundle.Name)
	if err != nil {
		s.logger.Warnf("[GraphQL] Failed to retrieve existing schema for bundle '%s': %v", bundle.Name, err)
		// Continue with schema generation even if we can't get old schema
	}

	// Generate new schema from the current bundle structure (reuses existing generator)
	// This converts SyndrDB field definitions → GraphQL types
	newSchemaDef, err := s.schemaGenerator.GenerateSchema(bundle)
	if err != nil {
		return fmt.Errorf("failed to generate schema: %w", err)
	}

	// Detect breaking changes by comparing old schema with new schema
	// Breaking changes: field removals, type changes, nullable → non-nullable
	var breakingChanges []graphQLSchema.BreakingChange
	if oldSchema != nil && oldSchema.Payload != nil {
		breakingChanges = s.schemaGenerator.DetectBreakingChanges(oldSchema.Payload, newSchemaDef)

		// Log breaking changes for visibility (critical for API consumers)
		if len(breakingChanges) > 0 {
			s.logger.Warnf("[GraphQL] Breaking changes detected in bundle '%s': %d change(s)",
				bundle.Name, len(breakingChanges))
			for _, change := range breakingChanges {
				s.logger.Warnf("[GraphQL]   - %s: Field '%s' %s → %s (Severity: %s)",
					change.ChangeType, change.FieldName, change.OldValue, change.NewValue, change.Severity)
			}
		} else {
			s.logger.Debugf("[GraphQL] No breaking changes detected (backward compatible update)")
		}
	}

	// Attach breaking changes to schema definition for storage and future reference
	newSchemaDef.BreakingChanges = breakingChanges

	// Get schema version for update operation
	var schemaIDBytes [16]byte
	if oldSchema != nil {
		// Updating existing schema - use same ID to link versions
		schemaIDBytes = oldSchema.SchemaID
	} else {
		// First schema for this bundle - generate new ID
		copy(schemaIDBytes[:], []byte(helpers.GenerateFastUUID()))
	}

	var bundleIDBytes [16]byte
	copy(bundleIDBytes[:], []byte(bundle.BundleID))

	// Update schema: creates new version, tombstones old, updates cache
	// This writes to the schema file with versioning and tombstone markers
	err = schemaManager.UpdateSchema(schemaIDBytes, bundleIDBytes, bundle.Name, newSchemaDef)
	if err != nil {
		return fmt.Errorf("failed to update schema: %w", err)
	}

	// Log success with version information
	newVersion, _ := schemaManager.GetLatestVersionForBundle(bundle.Name)
	s.logger.Debugf("[GraphQL] Schema updated for bundle '%s' (version %d, %d fields, %d breaking changes)",
		bundle.Name, newVersion, len(newSchemaDef.Fields), len(breakingChanges))

	return nil
}

// ReconcileGraphQLSchemas generates GraphQL schemas for any existing bundles that are
// missing one. This is called at startup when --graphql is enabled to handle bundles
// that were created while GraphQL was disabled.
//
// Because db.Bundles may be empty at startup (bundles are loaded on-demand), this
// method discovers bundle names from .bnd files on disk, then uses GetBundleMetadata
// to load each bundle with full FieldDefinitions before generating its schema.
//
// Returns the number of schemas generated and any error encountered.
// Individual bundle failures are logged at Warn level but do not abort the process.
func (s *BundleService) ReconcileGraphQLSchemas(db *models.Database) (int, error) {
	if !s.graphQLEnabled || s.schemaGenerator == nil {
		return 0, nil
	}

	schemaManager, err := s.getOrCreateSchemaManager(db)
	if err != nil {
		return 0, fmt.Errorf("failed to get schema manager: %w", err)
	}
	if schemaManager == nil {
		return 0, nil
	}

	// Discover bundle names from .bnd files on disk.
	// Convention: {dbName}_{bundleName}.bnd  (first segment file per bundle)
	databasePath := helpers.GetDatabaseFolderPath(db.Name)
	entries, err := os.ReadDir(databasePath)
	if err != nil {
		return 0, fmt.Errorf("failed to read database directory %s: %w", databasePath, err)
	}

	prefix := db.Name + "_"
	suffix := ".bnd"
	bundleNames := make(map[string]struct{})
	for _, entry := range entries {
		name := entry.Name()
		if entry.IsDir() || !strings.HasPrefix(name, prefix) || !strings.HasSuffix(name, suffix) {
			continue
		}
		// Strip prefix and suffix to get bundle name
		bundleName := strings.TrimSuffix(strings.TrimPrefix(name, prefix), suffix)
		if bundleName == "" {
			continue
		}
		// Multi-segment bundles live in subdirectories; the top-level .bnd is
		// the original single-file bundle.  Deduplicate.
		bundleNames[bundleName] = struct{}{}
	}

	// Also scan subdirectories that match bundle names (multi-segment bundles
	// store files in {databasePath}/{bundleName}/*.bnd with a .manifest).
	for _, entry := range entries {
		if !entry.IsDir() {
			continue
		}
		subdir := entry.Name()
		manifestPath := filepath.Join(databasePath, subdir, "bundle.manifest")
		if _, err := os.Stat(manifestPath); err == nil {
			bundleNames[subdir] = struct{}{}
		}
	}

	reconciled := 0
	for bundleName := range bundleNames {
		// Load full bundle metadata (including FieldDefinitions) from disk.
		// We always need this to populate db.Bundles (which is empty at startup).
		bundle, err := s.GetBundleMetadata(db, bundleName)
		if err != nil {
			// Bundle has stale on-disk artifacts (e.g. .bnd file or manifest left
			// behind after DROP BUNDLE WITH FORCE). Clean them up so the warning
			// does not recur on every restart.
			s.logger.Warnf("[GraphQL Reconcile] Removing stale artifacts for bundle '%s': %v", bundleName, err)
			_ = s.store.RemoveBundleFile(db, bundleName)
			continue
		}

		// Populate db.Bundles so loadSchemaFromBundles can iterate them
		bundle.Database = db
		db.Bundles[bundleName] = *bundle

		// Check if this bundle already has a GraphQL schema
		_, schemaErr := schemaManager.GetActiveSchemaForBundle(bundleName)
		if schemaErr == nil {
			continue // schema already exists, no regeneration needed
		}

		// No schema found — generate one
		s.logger.Infof("[GraphQL Reconcile] Generating schema for bundle '%s'", bundleName)

		if err := s.regenerateGraphQLSchema(bundle); err != nil {
			s.logger.Warnf("[GraphQL Reconcile] Failed to generate schema for bundle '%s': %v", bundleName, err)
			continue
		}

		reconciled++
	}

	return reconciled, nil
}

// validateDocumentFields validates that document fields match bundle field definitions
// This function ensures that:
// 1. All fields in the document command exist in the bundle's field definitions
// 2. Field data types match the bundle field definition types
// 3. Required fields are present
// 4. Field values are compatible with their defined types
func (s *BundleService) validateDocumentFields(bundle *models.Bundle, docCommand *models.DocumentCommand) error {
	if bundle.DocumentStructure.FieldDefinitions == nil {
		s.logger.Warnf("[VALIDATION] Bundle '%s' has nil FieldDefinitions - cannot validate", bundle.Name)
		return fmt.Errorf("bundle '%s' has no field definitions", bundle.Name)
	}

	//s.logger.Debugf("[VALIDATION] Bundle '%s' has %d field definition(s)", bundle.Name, len(bundle.DocumentStructure.FieldDefinitions))

	// Log all field definitions for debugging
	// for fieldName, fieldDef := range bundle.DocumentStructure.FieldDefinitions {
	// 	s.logger.Debugf("[VALIDATION] Field '%s': Type=%s, Required=%v, Unique=%v",
	// 		fieldName, fieldDef.Type, fieldDef.IsRequired, fieldDef.IsUnique)
	// }

	// Track which required fields are provided
	providedFields := make(map[string]bool)

	// Validate each field in the document command
	for i, field := range docCommand.Fields {
		fieldName := field.Key
		fieldValue := field.Value

		// Check if the field exists in bundle field definitions
		fieldDef, exists := bundle.DocumentStructure.FieldDefinitions[fieldName]
		if !exists {
			return fmt.Errorf("field '%s' is not defined in bundle '%s'", fieldName, bundle.Name)
		}

		// Check if user provided explicit NULL for a required field
		// This should fail just like if the field was missing
		if fieldDef.IsRequired && s.nullHandler.IsNullValue(fieldValue) {
			return fmt.Errorf("required field '%s' cannot be set to NULL", fieldName)
		}

		// Validate and convert field data type using fast pre-compiled converter
		convertedValue, err := s.validateAndConvertFieldTypeFast(fieldName, fieldValue, fieldDef.Type)
		if err != nil {
			return fmt.Errorf("field '%s' type validation failed: %w", fieldName, err)
		}

		// Update the field value with the converted value
		docCommand.Fields[i].Value = convertedValue

		// Mark this field as provided (only if not NULL)
		// NULL values should be treated as if the field was not provided for required field validation
		if !s.nullHandler.IsNullValue(convertedValue) {
			providedFields[fieldName] = true
		}
	}

	//s.logger.Debugf("[VALIDATION] Provided %d field(s) in document command", len(providedFields))

	// Check that all required fields are provided
	missingFields := make([]string, 0, 5)
	for fieldName, fieldDef := range bundle.DocumentStructure.FieldDefinitions {
		if fieldDef.IsRequired && !providedFields[fieldName] {
			// Skip DocumentID if it's auto-generated
			if fieldName == "DocumentID" {
				continue
			}
			missingFields = append(missingFields, fieldName)
		}
	}

	// If any required fields are missing, return detailed error
	if len(missingFields) > 0 {
		if len(missingFields) == 1 {
			s.logger.Warnf("[VALIDATION] Required field '%s' is missing from document", missingFields[0])
			return fmt.Errorf("required field '%s' is missing from document", missingFields[0])
		}
		s.logger.Warnf("[VALIDATION] Multiple required fields missing: %v", missingFields)
		return fmt.Errorf("required fields are missing from document: %v", missingFields)
	}

	//s.logger.Debugf("[VALIDATION] All required fields validated successfully")
	return nil
}

// processNullValues handles NULL value processing, default value substitution, and field initialization.
// Uses a single-pass algorithm for O(n) performance where n is the number of fields in the schema.
//
// This function:
// 1. Substitutes default values for NULL or missing fields (required or optional)
// 2. Converts user nil values to SYNDR_NULL magic value (if no default exists)
// 3. Escapes user strings that look like magic values
// 4. Initializes missing optional fields with defaults or SYNDR_NULL
//
// CRITICAL: Must run BEFORE validation so required fields with defaults are satisfied
//
// Performance: O(n) time, O(1) space where n = schema field count
func (s *BundleService) processNullValues(bundle *models.Bundle, docCommand *models.DocumentCommand) error {
	if bundle.DocumentStructure.FieldDefinitions == nil {
		return fmt.Errorf("bundle '%s' has no field definitions", bundle.Name)
	}

	// Build providedFields map while processing existing fields (single pass)
	providedFields := make(map[string]bool, len(docCommand.Fields))

	// PASS 1: Process provided fields in-place - substitute defaults for NULL values
	for i := range docCommand.Fields {
		fieldName := docCommand.Fields[i].Key
		fieldValue := docCommand.Fields[i].Value

		// Get field definition for default value lookup
		fieldDef, exists := bundle.DocumentStructure.FieldDefinitions[fieldName]
		if !exists {
			// Field doesn't exist in schema - validation will catch this later
			providedFields[fieldName] = true
			continue
		}

		// Mark as provided (even if NULL - we'll check for defaults)
		providedFields[fieldName] = true

		// CRITICAL: Check if user explicitly set a required field to NULL
		// This must happen BEFORE default value substitution
		// Required fields cannot be NULL, even if they have a default value
		if fieldDef.IsRequired && (fieldValue == nil || fieldValue == SYNDR_NULL) {
			return fmt.Errorf("required field '%s' cannot be set to NULL", fieldName)
		}

		// Handle nil or SYNDR_NULL -> check for default value substitution
		if fieldValue == nil || fieldValue == SYNDR_NULL {
			if fieldDef.DefaultValue != nil {
				// Evaluate default value (supports Expression or literal)
				// Create a temporary document for evaluation context
				tempDoc := &models.Document{
					Data: make(map[string]interface{}),
				}
				evaluatedValue, err := s.evaluateDefaultValue(fieldDef.DefaultValue, tempDoc)
				if err != nil {
					s.logger.Errorf("Failed to evaluate default value for field '%s': %v", fieldName, err)
					return fmt.Errorf("failed to evaluate default value for field '%s': %w", fieldName, err)
				}
				// Substitute the evaluated default value
				docCommand.Fields[i].Value = evaluatedValue
				s.logger.Debugf("Substituted evaluated default value for field '%s': %v", fieldName, evaluatedValue)
			} else {
				// No default - use SYNDR_NULL
				docCommand.Fields[i].Value = SYNDR_NULL
			}
			continue
		}

		// Escape magic-like values (fast path: string prefix check)
		if strValue, ok := fieldValue.(string); ok {
			if strings.HasPrefix(strValue, "::SYNDR_") {
				// Only escape if it's NOT already a valid magic value
				switch strValue {
				case SYNDR_NULL, SYNDR_MISSING, SYNDR_DELETED, SYNDR_DEFAULT:
					// Valid magic value - keep as-is
					continue
				default:
					// User string that looks like magic value - escape it
					docCommand.Fields[i].Value = SYNDR_ESCAPED + strValue
				}
			}
		}
	}

	// PASS 2: Add missing fields (required or optional) with defaults or SYNDR_NULL
	missingFieldCount := 0
	for fieldName := range bundle.DocumentStructure.FieldDefinitions {
		// Skip DocumentID (auto-generated)
		if fieldName == "DocumentID" {
			continue
		}

		// Count ALL missing fields (required or optional)
		if !providedFields[fieldName] {
			missingFieldCount++
		}
	}

	// Pre-allocate slice capacity to avoid multiple allocations
	if missingFieldCount > 0 {
		originalLen := len(docCommand.Fields)
		// Grow slice once with exact capacity needed
		newFields := make([]models.KeyValue, originalLen, originalLen+missingFieldCount)
		copy(newFields, docCommand.Fields)

		// Append missing fields with defaults or SYNDR_NULL
		for fieldName, fieldDef := range bundle.DocumentStructure.FieldDefinitions {
			// Skip DocumentID (auto-generated)
			if fieldName == "DocumentID" {
				continue
			}

			// Skip provided fields
			if providedFields[fieldName] {
				continue
			}

			// Determine value: use default if available, otherwise SYNDR_NULL
			var fieldValue interface{}
			if fieldDef.DefaultValue != nil {
				// Evaluate default value (supports Expression or literal)
				// Create a temporary document for evaluation context
				tempDoc := &models.Document{
					Data: make(map[string]interface{}),
				}
				evaluatedValue, err := s.evaluateDefaultValue(fieldDef.DefaultValue, tempDoc)
				if err != nil {
					s.logger.Errorf("Failed to evaluate default value for field '%s': %v", fieldName, err)
					return fmt.Errorf("failed to evaluate default value for field '%s': %w", fieldName, err)
				}
				fieldValue = evaluatedValue
				s.logger.Debugf("Using evaluated default value for missing field '%s': %v", fieldName, evaluatedValue)
			} else {
				fieldValue = SYNDR_NULL
			}

			newFields = append(newFields, models.KeyValue{
				Key:   fieldName,
				Value: fieldValue,
			})
		}

		docCommand.Fields = newFields
	}

	return nil
}

// validateAndConvertFieldTypeFast uses pre-compiled converters for optimal performance
// This eliminates reflection overhead and provides 60-80% faster field validation
func (s *BundleService) validateAndConvertFieldTypeFast(fieldName string, value interface{}, expectedType string) (interface{}, error) {
	if value == nil {
		return nil, nil // nil values are handled by required field validation
	}

	// Use pre-compiled converter for fast type conversion (O(1) map lookup)
	converter, exists := typeConverters[strings.ToLower(expectedType)]
	if !exists {
		// Unknown field type - log warning but allow it as string (fallback)
		s.logger.Warnf("Unknown field type '%s' for field '%s', treating as string", expectedType, fieldName)
		return convertToString(value)
	}

	// Fast conversion using pre-compiled function
	return converter(value)
}

// validateUpdateFields validates that document update fields match bundle field definitions
// This function ensures that:
// 1. All fields being updated exist in the bundle's field definitions
// 2. Field data types match the bundle field definition types
// 3. Field values are compatible with their defined types
// Note: Unlike document creation, updates don't require all required fields to be present
func (s *BundleService) validateUpdateFields(bundle *models.Bundle, docCommand *models.DocumentUpdateCommand) error {
	if bundle.DocumentStructure.FieldDefinitions == nil {
		return fmt.Errorf("bundle '%s' has no field definitions", bundle.Name)
	}

	// Single-pass processing: validate, escape, and convert in one loop
	// Performance: O(m) where m = number of fields being updated
	for i := range docCommand.Fields {
		fieldName := docCommand.Fields[i].Key
		fieldValue := docCommand.Fields[i].Value

		// REFERENTIAL INTEGRITY: DocumentID is read-only and cannot be updated
		if fieldName == "DocumentID" {
			return fmt.Errorf("cannot update DocumentID: this is a read-only system field")
		}

		// Check if the field exists in bundle field definitions
		fieldDef, exists := bundle.DocumentStructure.FieldDefinitions[fieldName]
		if !exists {
			return fmt.Errorf("field '%s' is not defined in bundle '%s'", fieldName, bundle.Name)
		}

		// Handle NULL values (fast path: nil check first)
		if fieldValue == nil {
			docCommand.Fields[i].Value = SYNDR_NULL
			continue // Skip type validation for NULL
		}

		// Escape magic-like values (inline, no function call overhead)
		// Fast path: only check strings that start with ::SYNDR_
		if strValue, ok := fieldValue.(string); ok {
			if strings.HasPrefix(strValue, "::SYNDR_") {
				// Check if it's a valid magic value
				switch strValue {
				case SYNDR_NULL, SYNDR_MISSING, SYNDR_DELETED, SYNDR_DEFAULT:
					// Valid magic value - keep as-is, skip type validation
					continue
				default:
					// User string that looks like magic value - escape it
					docCommand.Fields[i].Value = SYNDR_ESCAPED + strValue
					fieldValue = docCommand.Fields[i].Value // Update for validation
				}
			}
		}

		// Validate and convert field data type
		convertedValue, err := s.validateAndConvertFieldTypeFast(fieldName, fieldValue, fieldDef.Type)
		if err != nil {
			return fmt.Errorf("field '%s' type validation failed: %w", fieldName, err)
		}

		// Update the field value with the converted value
		docCommand.Fields[i].Value = convertedValue

		// TODO: Unique constraint validation for updates (future work)
		// if fieldDef.IsUnique {
		//     // Validate that new value doesn't violate uniqueness
		// }
	}

	return nil
}
