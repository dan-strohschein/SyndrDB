package bundle

import (
	"fmt"
	"path/filepath"
	"strconv"

	"syndrdb/src/internal/domain/models"
	syndrQL "syndrdb/src/internal/syndrQL"
	"syndrdb/src/pkg/common/conversion"
)

// ApplyFieldChanges applies ADD/REMOVE/MODIFY field operations to a bundle.
// It validates constraints, performs type conversions, and rebuilds indexes as needed.
// This method handles the actual schema modification for UPDATE BUNDLE commands.
func (s *BundleService) ApplyFieldChanges(database *models.Database, bundle *models.Bundle, changes []models.FieldChange) error {
	if bundle == nil {
		return fmt.Errorf("bundle is nil")
	}
	if len(changes) == 0 {
		return fmt.Errorf("no field changes specified")
	}

	// Track which indexes need rebuilding
	indexesToRebuild := make(map[string]bool)

	// Apply each field change
	for _, change := range changes {
		switch change.ChangeType {
		case "ADD":
			if err := s.applyAddField(bundle, &change); err != nil {
				return fmt.Errorf("failed to add field '%s': %w", change.NewField.Name, err)
			}

		case "REMOVE":
			fieldName := change.OldFieldName
			if fieldName == "" {
				fieldName = change.NewField.Name
			}
			if err := s.applyRemoveField(database, bundle, fieldName); err != nil {
				return fmt.Errorf("failed to remove field '%s': %w", fieldName, err)
			}
			// Indexes on this field are removed and their files deleted in applyRemoveField; no rebuild

		case "MODIFY":
			if err := s.applyModifyField(bundle, &change); err != nil {
				return fmt.Errorf("failed to modify field '%s': %w", change.OldFieldName, err)
			}
			// Track if old or new field is indexed (for renames)
			if s.isFieldIndexed(bundle, change.OldFieldName) {
				indexesToRebuild[change.OldFieldName] = true
			}
			if change.OldFieldName != change.NewField.Name && s.isFieldIndexed(bundle, change.NewField.Name) {
				indexesToRebuild[change.NewField.Name] = true
			}

			// Log appropriate message based on whether it's a rename or just a modification
			if change.OldFieldName != change.NewField.Name {
				s.logger.Debugf("Renamed and modified field '%s' to '%s' in bundle '%s'",
					change.OldFieldName, change.NewField.Name, bundle.Name)
			}

		default:
			return fmt.Errorf("unsupported change type: %s", change.ChangeType)
		}
	}

	// Invalidate plan cache for schema changes (field additions/removals/modifications)
	// This ensures queries use fresh plans reflecting the new schema
	s.invalidatePlanCacheForBundle(bundle.Name)

	// Rebuild affected indexes
	if len(indexesToRebuild) > 0 {
		//s.logger.Debugf("Rebuilding %d indexes for bundle '%s'", len(indexesToRebuild), bundle.Name)
		for fieldName := range indexesToRebuild {
			if err := s.rebuildFieldIndex(bundle, fieldName); err != nil {
				s.logger.Warnf("Failed to rebuild index for field '%s': %v", fieldName, err)
				// Don't fail the entire operation if index rebuild fails
			}
		}
	}

	// Persist bundle metadata changes
	err := s.store.UpdateBundleFile(bundle.Database, bundle)
	if err != nil {
		return fmt.Errorf("failed to persist bundle changes: %w", err)
	}

	// Update the cache with the modified bundle
	s.bundleMetadata[bundle.Name] = bundle

	// FR-6 GRAPHQL INTEGRATION: Regenerate GraphQL schema after bundle structure changes
	// This reuses the regenerateGraphQLSchema method which handles:
	// - Breaking change detection (field removals, type changes, nullability changes)
	// - Schema versioning (new version creation + old version tombstoning)
	// - Cache updates for immediate availability
	//
	// Schema update failures are logged but don't fail the field change operation.
	// This ensures bundle modifications succeed even if GraphQL clients may need updates.
	if err := s.regenerateGraphQLSchema(bundle); err != nil {
		s.logger.Warnf("[GraphQL] Failed to regenerate schema for bundle '%s': %v. Field changes were applied successfully.",
			bundle.Name, err)
	}

	s.logger.Debugf("Successfully applied all field changes to bundle '%s'", bundle.Name)
	return nil
}

// applyAddField adds a new field to the bundle schema and existing documents
func (s *BundleService) applyAddField(bundle *models.Bundle, change *models.FieldChange) error {
	fieldName := change.NewField.Name

	// Validate field doesn't already exist
	if _, exists := bundle.DocumentStructure.FieldDefinitions[fieldName]; exists {
		return fmt.Errorf("field '%s' already exists in bundle '%s'", fieldName, bundle.Name)
	}

	// If required, must have default value
	if change.NewField.IsRequired && change.NewField.DefaultValue == nil {
		return fmt.Errorf("cannot add required field '%s' without default value", fieldName)
	}

	// Add field to schema
	if bundle.DocumentStructure.FieldDefinitions == nil {
		bundle.DocumentStructure.FieldDefinitions = make(map[string]models.FieldDefinition)
	}
	bundle.DocumentStructure.FieldDefinitions[fieldName] = change.NewField

	// Apply default value to all existing documents if field is required
	if change.NewField.IsRequired && change.NewField.DefaultValue != nil {
		//s.logger.Debugf("Applying default value to all existing documents in bundle '%s'", bundle.Name)
		if err := s.applyDefaultToExistingDocuments(bundle, fieldName, change.NewField.DefaultValue); err != nil {
			return fmt.Errorf("failed to apply default value to existing documents: %w", err)
		}
	}

	return nil
}

// applyRemoveField removes a field from the bundle schema and all documents.
// Indexes on this field are removed from bundle.Indexes and their files deleted from disk.
func (s *BundleService) applyRemoveField(database *models.Database, bundle *models.Bundle, fieldName string) error {
	// Validate field exists
	if _, exists := bundle.DocumentStructure.FieldDefinitions[fieldName]; !exists {
		return fmt.Errorf("field '%s' does not exist in bundle '%s'", fieldName, bundle.Name)
	}

	// Cannot remove DocumentID field
	if fieldName == "DocumentID" {
		return fmt.Errorf("cannot remove system field 'DocumentID'")
	}

	// === VALIDATE REFERENTIAL INTEGRITY ===
	// Check if this field is used in any relationships (both directions)
	bundleCache := make(map[string]*models.Bundle)
	validator := NewReferentialIntegrityValidator(s, s.logger)
	violation := validator.ValidateFieldRemoval(database, bundle, fieldName, bundleCache)
	if violation != nil {
		s.logger.Warnf("[REFINT] %s | Suggested: %s", violation.Error(), violation.SuggestedAction)
		return fmt.Errorf("%s", violation.Error())
	}

	// Remove indexes on this field from bundle.Indexes and delete their files from disk
	if bundle.Indexes != nil && database != nil {
		var toRemove []string
		for indexName, indexRef := range bundle.Indexes {
			match := indexRef.BTreeIndexField.FieldName == fieldName ||
				indexRef.HashIndexField.FieldName == fieldName
			for _, f := range indexRef.Fields {
				if f.Name == fieldName {
					match = true
					break
				}
			}
			if match {
				toRemove = append(toRemove, indexName)
			}
		}
		indexesPath := filepath.Join(database.DataDirectory, database.Name, bundle.Name, "indexes")
		for _, indexName := range toRemove {
			ir := bundle.Indexes[indexName]
			_ = DeleteIndexFiles(indexesPath, indexName, ir.IndexType, s.logger)
			delete(bundle.Indexes, indexName)
			// Remove from IndexNames
			for i, n := range bundle.IndexNames {
				if n == indexName {
					bundle.IndexNames = append(bundle.IndexNames[:i], bundle.IndexNames[i+1:]...)
					break
				}
			}
			// Remove from sharded index cache (thread-safe)
			s.loadedIndexes.Delete(bundle.Name, indexName)
		}
	}

	// Remove from schema
	delete(bundle.DocumentStructure.FieldDefinitions, fieldName)

	// Remove field from all existing documents
	//s.logger.Debugf("Removing field '%s' from all documents in bundle '%s'", fieldName, bundle.Name)
	if err := s.removeFieldFromExistingDocuments(bundle, fieldName); err != nil {
		return fmt.Errorf("failed to remove field from existing documents: %w", err)
	}

	return nil
}

// applyModifyField modifies an existing field's properties
func (s *BundleService) applyModifyField(bundle *models.Bundle, change *models.FieldChange) error {
	oldFieldName := change.OldFieldName
	newFieldName := change.NewField.Name
	isRenaming := oldFieldName != newFieldName

	// Validate old field exists
	oldField, exists := bundle.DocumentStructure.FieldDefinitions[oldFieldName]
	if !exists {
		return fmt.Errorf("field '%s' does not exist in bundle '%s'", oldFieldName, bundle.Name)
	}

	// Cannot rename system fields
	if isRenaming && oldFieldName == "DocumentID" {
		return fmt.Errorf("cannot rename system field 'DocumentID'")
	}

	// If renaming, validate new field name doesn't already exist
	if isRenaming {
		if _, exists := bundle.DocumentStructure.FieldDefinitions[newFieldName]; exists {
			return fmt.Errorf("cannot rename field '%s' to '%s': target field name already exists", oldFieldName, newFieldName)
		}

		// === VALIDATE REFERENTIAL INTEGRITY ===
		// Check if this field rename would break any relationships
		bundleCache := make(map[string]*models.Bundle)
		validator := NewReferentialIntegrityValidator(s, s.logger)
		violation := validator.ValidateFieldRename(nil, bundle, oldFieldName, newFieldName, bundleCache)
		if violation != nil {
			s.logger.Warnf("[REFINT] %s | Suggested: %s", violation.Error(), violation.SuggestedAction)
			return fmt.Errorf("%s", violation.Error())
		}

		s.logger.Debugf("Renaming field '%s' to '%s' in bundle '%s'", oldFieldName, newFieldName, bundle.Name)
	}

	// If type is changing, validate conversion is possible
	if oldField.Type != change.NewField.Type {
		s.logger.Debugf("Attempting type conversion for field '%s' from %s to %s",
			oldFieldName, oldField.Type, change.NewField.Type)
		if err := s.convertFieldType(bundle, oldFieldName, oldField.Type, change.NewField.Type); err != nil {
			return fmt.Errorf("cannot convert field '%s' from %s to %s - manual migration required: %w",
				oldFieldName, oldField.Type, change.NewField.Type, err)
		}
	}

	// If adding IsUnique constraint, validate no duplicates exist
	if !oldField.IsUnique && change.NewField.IsUnique {
		s.logger.Debugf("Validating uniqueness for field '%s'", oldFieldName)
		if err := s.validateFieldUniqueness(bundle, oldFieldName); err != nil {
			return err
		}
	}

	// If making field required, ensure it has a default or all documents have values
	if !oldField.IsRequired && change.NewField.IsRequired {
		if change.NewField.DefaultValue == nil {
			// Check if all documents already have this field
			if err := s.validateAllDocumentsHaveField(bundle, oldFieldName); err != nil {
				return fmt.Errorf("cannot make field '%s' required: %w. Provide a default value or ensure all documents have this field", oldFieldName, err)
			}
		} else {
			// Apply default to documents missing this field
			if err := s.applyDefaultToMissingField(bundle, oldFieldName, change.NewField.DefaultValue); err != nil {
				return fmt.Errorf("failed to apply default value: %w", err)
			}
		}
	}

	// If renaming, rename field in all documents
	if isRenaming {
		if err := s.renameFieldInDocuments(bundle, oldFieldName, newFieldName); err != nil {
			return fmt.Errorf("failed to rename field in documents: %w", err)
		}
	}

	// Update schema: remove old field and add new field definition
	if isRenaming {
		delete(bundle.DocumentStructure.FieldDefinitions, oldFieldName)
	}
	bundle.DocumentStructure.FieldDefinitions[newFieldName] = change.NewField

	return nil
}

// applyDefaultToExistingDocuments adds a field with default value to all documents
func (s *BundleService) applyDefaultToExistingDocuments(bundle *models.Bundle, fieldName string, defaultValue interface{}) error {
	// UNIVERSAL CACHE: Use GetDocumentPage to populate and benefit from shared documentPages cache
	// Iterate through all document pages
	for pageID := uint32(0); pageID < uint32(bundle.PageCount); pageID++ {
		docs, err := s.SnapshotPageDocuments(bundle.Name, bundle.Database.Name, pageID)
		if err != nil {
			continue // Skip pages that don't exist yet
		}

		schema := bundle.DocumentStructure.FieldSchema()
		for _, doc := range docs {
			var hasField bool
			if schema != nil && len(doc.Values) > 0 {
				if idx, ok := schema.NameToIndex[fieldName]; ok && idx < len(doc.Values) {
					hasField = !doc.Values[idx].IsNil()
				}
			} else {
				if doc.Data == nil {
					doc.Data = make(map[string]interface{})
				}
				_, hasField = doc.Data[fieldName]
			}
			if !hasField {
				evaluatedValue, err := s.evaluateDefaultValue(defaultValue, &doc)
				if err != nil {
					return fmt.Errorf("failed to evaluate default value for field '%s': %w", fieldName, err)
				}
				fv := models.NewInterfaceValue(evaluatedValue)
				if schema != nil && len(doc.Values) > 0 {
					if idx, ok := schema.NameToIndex[fieldName]; ok && idx < len(doc.Values) {
						doc.Values[idx] = fv
					}
				} else {
					doc.Data[fieldName] = evaluatedValue
				}
				doc.CachedJSON = nil
				err = s.store.UpdateDocumentInBundleFile(bundle, &doc)
				if err != nil {
					return fmt.Errorf("failed to update document %s: %w", doc.DocumentID, err)
				}
			}
		}
	}

	s.invalidateBundlePageCache(bundle.Name)
	return nil
}

func (s *BundleService) removeFieldFromExistingDocuments(bundle *models.Bundle, fieldName string) error {
	schema := bundle.DocumentStructure.FieldSchema()
	for pageID := uint32(0); pageID < uint32(bundle.PageCount); pageID++ {
		docs, err := s.SnapshotPageDocuments(bundle.Name, bundle.Database.Name, pageID)
		if err != nil {
			continue
		}
		for _, doc := range docs {
			var hasField bool
			if schema != nil && len(doc.Values) > 0 {
				if idx, ok := schema.NameToIndex[fieldName]; ok && idx < len(doc.Values) {
					hasField = true
				}
			} else if doc.Data != nil {
				_, hasField = doc.Data[fieldName]
			}
			if hasField {
				if schema != nil && len(doc.Values) > 0 {
					if idx, ok := schema.NameToIndex[fieldName]; ok && idx < len(doc.Values) {
						doc.Values[idx] = models.FieldValue{Type: models.FieldTypeNil}
					}
				} else if doc.Data != nil {
					delete(doc.Data, fieldName)
				}
				doc.CachedJSON = nil
				err := s.store.UpdateDocumentInBundleFile(bundle, &doc)
				if err != nil {
					return fmt.Errorf("failed to update document %s: %w", doc.DocumentID, err)
				}
			}
		}
	}
	s.invalidateBundlePageCache(bundle.Name)
	return nil
}

func (s *BundleService) renameFieldInDocuments(bundle *models.Bundle, oldFieldName, newFieldName string) error {
	s.logger.Debugf("Renaming field '%s' to '%s' in all documents of bundle '%s'", oldFieldName, newFieldName, bundle.Name)
	schema := bundle.DocumentStructure.FieldSchema()
	for pageID := uint32(0); pageID < uint32(bundle.PageCount); pageID++ {
		docs, err := s.SnapshotPageDocuments(bundle.Name, bundle.Database.Name, pageID)
		if err != nil {
			continue
		}
		for _, doc := range docs {
			var oldFV models.FieldValue
			var hasOld bool
			if schema != nil && len(doc.Values) > 0 {
				if idx, ok := schema.NameToIndex[oldFieldName]; ok && idx < len(doc.Values) {
					oldFV = doc.Values[idx]
					hasOld = true
				}
			} else if doc.Data != nil {
				if v, ok := doc.Data[oldFieldName]; ok {
					oldFV = models.NewInterfaceValue(v)
					hasOld = true
				}
			}
			if hasOld {
				if schema != nil && len(doc.Values) > 0 {
					if newIdx, ok := schema.NameToIndex[newFieldName]; ok && newIdx < len(doc.Values) {
						doc.Values[newIdx] = oldFV
					}
					if oldIdx, ok := schema.NameToIndex[oldFieldName]; ok && oldIdx < len(doc.Values) && oldFieldName != newFieldName {
						doc.Values[oldIdx] = models.FieldValue{Type: models.FieldTypeNil}
					}
				} else if doc.Data != nil {
					doc.Data[newFieldName] = oldFV.AsInterface()
					delete(doc.Data, oldFieldName)
				}
				doc.CachedJSON = nil
				err := s.store.UpdateDocumentInBundleFile(bundle, &doc)
				if err != nil {
					return fmt.Errorf("failed to update document %s: %w", doc.DocumentID, err)
				}
			}
		}
	}
	s.invalidateBundlePageCache(bundle.Name)
	s.logger.Debugf("Successfully renamed field '%s' to '%s' in bundle '%s'", oldFieldName, newFieldName, bundle.Name)
	return nil
}

func (s *BundleService) convertFieldType(bundle *models.Bundle, fieldName, fromType, toType string) error {
	conversionErrors := []string{}
	schema := bundle.DocumentStructure.FieldSchema()
	for pageID := uint32(0); pageID < uint32(bundle.PageCount); pageID++ {
		docs, err := s.SnapshotPageDocuments(bundle.Name, bundle.Database.Name, pageID)
		if err != nil {
			continue
		}
		for _, doc := range docs {
			var fv models.FieldValue
			var hasField bool
			if schema != nil && len(doc.Values) > 0 {
				fv, hasField = doc.GetFieldValue(schema, fieldName)
			} else if doc.Data != nil {
				if v, ok := doc.Data[fieldName]; ok {
					fv = models.NewInterfaceValue(v)
					hasField = true
				}
			}
			if !hasField {
				continue
			}
			convertedValue, err := s.convertValue(fv.AsInterface(), fromType, toType)
			if err != nil {
				conversionErrors = append(conversionErrors, fmt.Sprintf("doc %s: %v", doc.DocumentID, err))
				continue
			}
			newFV := models.NewInterfaceValue(convertedValue)
			if schema != nil && len(doc.Values) > 0 {
				if idx, ok := schema.NameToIndex[fieldName]; ok && idx < len(doc.Values) {
					doc.Values[idx] = newFV
				}
			} else {
				if doc.Data == nil {
					doc.Data = make(map[string]interface{})
				}
				doc.Data[fieldName] = convertedValue
			}
			doc.CachedJSON = nil
			err = s.store.UpdateDocumentInBundleFile(bundle, &doc)
			if err != nil {
				return fmt.Errorf("failed to update document %s: %w", doc.DocumentID, err)
			}
		}
	}

	if len(conversionErrors) > 0 {
		return fmt.Errorf("conversion failed for %d documents: %v", len(conversionErrors), conversionErrors[:min(5, len(conversionErrors))])
	}

	// Invalidate cached pages to force reload
	s.invalidateBundlePageCache(bundle.Name)

	return nil
}

// convertValue attempts to convert a single value from one type to another
func (s *BundleService) convertValue(value interface{}, fromType, toType string) (interface{}, error) {
	// Handle nil values
	if value == nil {
		return nil, nil
	}

	switch toType {
	case "string":
		return conversion.ValueToString(value), nil

	case "int":
		switch v := value.(type) {
		case int, int32, int64:
			return v, nil
		case float32, float64:
			return int(v.(float64)), nil
		case string:
			return strconv.Atoi(v)
		default:
			return nil, fmt.Errorf("cannot convert %T to int", value)
		}

	case "float":
		switch v := value.(type) {
		case float32, float64:
			return v, nil
		case int, int32, int64:
			return float64(v.(int)), nil
		case string:
			return strconv.ParseFloat(v, 64)
		default:
			return nil, fmt.Errorf("cannot convert %T to float", value)
		}

	case "bool":
		switch v := value.(type) {
		case bool:
			return v, nil
		case string:
			return strconv.ParseBool(v)
		case int, int32, int64:
			return v.(int) != 0, nil
		default:
			return nil, fmt.Errorf("cannot convert %T to bool", value)
		}

	default:
		return nil, fmt.Errorf("unsupported target type: %s", toType)
	}
}

func (s *BundleService) validateFieldUniqueness(bundle *models.Bundle, fieldName string) error {
	valuesSeen := make(map[string][]string)
	schema := bundle.DocumentStructure.FieldSchema()
	for pageID := uint32(0); pageID < uint32(bundle.PageCount); pageID++ {
		docs, err := s.SnapshotPageDocuments(bundle.Name, bundle.Database.Name, pageID)
		if err != nil {
			continue
		}
		for _, doc := range docs {
			var fv models.FieldValue
			var hasField bool
			if schema != nil && len(doc.Values) > 0 {
				fv, hasField = doc.GetFieldValue(schema, fieldName)
			} else if doc.Data != nil {
				if v, ok := doc.Data[fieldName]; ok {
					fv = models.NewInterfaceValue(v)
					hasField = true
				}
			}
			if !hasField {
				continue
			}
			valueKey := conversion.ValueToString(fv.AsInterface())
			valuesSeen[valueKey] = append(valuesSeen[valueKey], doc.DocumentID)
		}
	}

	// Check for duplicates
	duplicates := []string{}
	for value, docIDs := range valuesSeen {
		if len(docIDs) > 1 {
			duplicates = append(duplicates, fmt.Sprintf("%v (in docs: %v)", value, docIDs[:min(3, len(docIDs))]))
		}
	}

	if len(duplicates) > 0 {
		return fmt.Errorf("cannot add IsUnique to field '%s' - duplicate values exist: %v",
			fieldName, duplicates[:min(5, len(duplicates))])
	}

	return nil
}

func (s *BundleService) validateAllDocumentsHaveField(bundle *models.Bundle, fieldName string) error {
	missingCount := 0
	schema := bundle.DocumentStructure.FieldSchema()
	for pageID := uint32(0); pageID < uint32(bundle.PageCount); pageID++ {
		docs, err := s.SnapshotPageDocuments(bundle.Name, bundle.Database.Name, pageID)
		if err != nil {
			continue
		}
		for _, doc := range docs {
			var fv models.FieldValue
			var hasField bool
			if schema != nil && len(doc.Values) > 0 {
				fv, hasField = doc.GetFieldValue(schema, fieldName)
			} else if doc.Data != nil {
				if v, ok := doc.Data[fieldName]; ok {
					fv = models.NewInterfaceValue(v)
					hasField = true
				}
			}
			if !hasField || fv.IsNil() {
				missingCount++
			}
		}
	}

	if missingCount > 0 {
		return fmt.Errorf("%d documents are missing field '%s'", missingCount, fieldName)
	}

	return nil
}

func (s *BundleService) applyDefaultToMissingField(bundle *models.Bundle, fieldName string, defaultValue interface{}) error {
	schema := bundle.DocumentStructure.FieldSchema()
	for pageID := uint32(0); pageID < uint32(bundle.PageCount); pageID++ {
		docs, err := s.SnapshotPageDocuments(bundle.Name, bundle.Database.Name, pageID)
		if err != nil {
			continue
		}
		for _, doc := range docs {
			var fv models.FieldValue
			var hasField bool
			if schema != nil && len(doc.Values) > 0 {
				fv, hasField = doc.GetFieldValue(schema, fieldName)
			} else if doc.Data != nil {
				if v, ok := doc.Data[fieldName]; ok {
					fv = models.NewInterfaceValue(v)
					hasField = true
				}
			}
			if !hasField || fv.IsNil() {
				evaluatedValue, err := s.evaluateDefaultValue(defaultValue, &doc)
				if err != nil {
					return fmt.Errorf("failed to evaluate default value for field '%s': %w", fieldName, err)
				}
				newFV := models.NewInterfaceValue(evaluatedValue)
				if schema != nil && len(doc.Values) > 0 {
					if idx, ok := schema.NameToIndex[fieldName]; ok && idx < len(doc.Values) {
						doc.Values[idx] = newFV
					}
				} else {
					if doc.Data == nil {
						doc.Data = make(map[string]interface{})
					}
					doc.Data[fieldName] = evaluatedValue
				}
				doc.CachedJSON = nil
				err = s.store.UpdateDocumentInBundleFile(bundle, &doc)
				if err != nil {
					return fmt.Errorf("failed to update document %s: %w", doc.DocumentID, err)
				}
			}
		}
	}

	// Invalidate cached pages to force reload
	s.invalidateBundlePageCache(bundle.Name)

	return nil
}

// evaluateDefaultValue evaluates a default value (supports Expression or literal)
func (s *BundleService) evaluateDefaultValue(defaultValue interface{}, doc *models.Document) (interface{}, error) {
	// Check if default value is an Expression (function call)
	if expr, isExpr := defaultValue.(syndrQL.Expression); isExpr {
		// Create evaluator for expression evaluation
		evaluator := syndrQL.NewExpressionEvaluator(s.logger)

		// Use the provided document for evaluation context
		// Field references will work if the field already exists in doc
		// Function calls like F:NOW() don't need document fields
		result, err := evaluator.Evaluate(expr, doc, nil, nil, nil)
		if err != nil {
			return nil, fmt.Errorf("expression evaluation failed: %w", err)
		}

		// Result is already interface{}, return as-is
		return result, nil
	}

	// Literal value - return as-is
	return defaultValue, nil
}

// isFieldIndexed checks if a field has an index
func (s *BundleService) isFieldIndexed(bundle *models.Bundle, fieldName string) bool {
	if bundle.Indexes == nil {
		return false
	}

	// Check if any index references this field
	for _, index := range bundle.Indexes {
		// Check BTreeIndexField
		if index.BTreeIndexField.FieldName == fieldName {
			return true
		}
		// Check HashIndexField
		if index.HashIndexField.FieldName == fieldName {
			return true
		}
		// Check Fields array
		for _, field := range index.Fields {
			if field.Name == fieldName {
				return true
			}
		}
	}

	// DocumentID always has hash index
	return fieldName == "DocumentID"
}

// rebuildFieldIndex rebuilds the index for a specific field
// NOTE: For now, this is a placeholder. Full index rebuilding requires:
// 1. Access to index manager to close/reinitialize indexes
// 2. Knowledge of index storage paths
// 3. Proper handling of different index types (BTree, Hash)
// This is a complex operation that should be implemented when index
// management is refactored to be more modular.
func (s *BundleService) rebuildFieldIndex(bundle *models.Bundle, fieldName string) error {
	s.logger.Warnf("Index rebuilding for field '%s' in bundle '%s' not yet fully implemented", fieldName, bundle.Name)
	s.logger.Debugf("Indexes will be rebuilt on next server restart or when accessed")

	// TODO: Implement full index rebuilding
	// For now, we log a warning. Indexes will be rebuilt when:
	// 1. Server restarts and reloads bundles
	// 2. Index is accessed and found to be stale/corrupted
	// 3. Manual reindex command is run

	return nil
}

func (s *BundleService) AddRelationshipToBundle(bundle *models.Bundle, relationshipCommand *models.RelationshipCommand) error {
	if bundle == nil {
		return fmt.Errorf("bundle is nil")
	}
	if relationshipCommand == nil {
		return fmt.Errorf("relationship command is nil")
	}

	// Generate relationship name with proper counter
	relationshipName := s.generateRelationshipName(bundle, relationshipCommand.SourceBundle, relationshipCommand.DestinationBundle)

	// Check if the relationship already exists
	for _, rel := range bundle.Relationships {
		if rel.Name == relationshipName {
			return fmt.Errorf("relationship '%s' already exists in bundle '%s'", relationshipName, bundle.Name)
		}
	}

	// Create the relationship with new structure
	relationship := models.Relationship{
		Name:              relationshipName,
		SourceField:       relationshipCommand.SourceField,
		DestinationBundle: relationshipCommand.DestinationBundle,
		DestinationField:  relationshipCommand.DestinationField,
		SourceBundle:      relationshipCommand.SourceBundle,
		RelationshipType:  relationshipCommand.RelationshipType,

		// Set legacy fields for backward compatibility
		SourceBundleName: relationshipCommand.SourceBundle,
		TargetBundleName: relationshipCommand.DestinationBundle,
	}

	// Validate source field exists before creating relationship
	sourceFieldDef, exists := bundle.DocumentStructure.FieldDefinitions[relationshipCommand.SourceField]
	if !exists {
		return fmt.Errorf("relationship validation failed: source field '%s.%s' does not exist", bundle.Name, relationshipCommand.SourceField)
	}

	// Add the relationship to the bundle
	if bundle.Relationships == nil {
		bundle.Relationships = make(map[string]models.Relationship)
	}
	bundle.Relationships[relationship.Name] = relationship

	s.logger.Debugf("Validating relationship %s: %s.%s (%s) -> %s.%s",
		relationship.Name,
		relationship.SourceBundle,
		relationship.SourceField,
		sourceFieldDef.Type,
		relationship.DestinationBundle,
		relationship.DestinationField)

	// Handle different relationship types and add appropriate fields
	switch relationship.RelationshipType {
	case "1toMany":
		// For 1toMany relationships, add a field to the destination bundle
		err := s.addFieldToDestinationBundle(bundle, &relationship, true, false) // required=true, unique=false
		if err != nil {
			return fmt.Errorf("failed to add field to destination bundle for 1toMany relationship: %w", err)
		}
	// TODO Add a 1To1 relationship type later
	case "0toMany":
		// For 0toMany relationships, add a field to the destination bundle (not required)
		err := s.addFieldToDestinationBundle(bundle, &relationship, false, false) // required=false, unique=false
		if err != nil {
			return fmt.Errorf("failed to add field to destination bundle for 0toMany relationship: %w", err)
		}

	case "ManyToMany":
		// For ManyToMany relationships, add fields to both bundles
		err := s.addFieldToDestinationBundle(bundle, &relationship, false, false) // required=false, unique=false
		if err != nil {
			return fmt.Errorf("failed to add field to destination bundle for ManyToMany relationship: %w", err)
		}

		// Get destination bundle to lookup its source field type for reverse FK
		destBundle, err := s.GetBundleByName(bundle.Database, relationship.DestinationBundle)
		if err != nil {
			return fmt.Errorf("failed to get destination bundle for ManyToMany reverse field: %w", err)
		}

		// Lookup destination bundle's source field for type preservation
		destSourceFieldDef, exists := destBundle.DocumentStructure.FieldDefinitions[relationship.DestinationField]
		if !exists {
			return fmt.Errorf("relationship validation failed: destination field '%s.%s' does not exist for ManyToMany reverse", destBundle.Name, relationship.DestinationField)
		}

		// Also add the reverse field to the source bundle with preserved destination type
		reverseFieldName := relationship.DestinationBundle + "ID"
		bundle.DocumentStructure.FieldDefinitions[reverseFieldName] = models.FieldDefinition{
			Name:         reverseFieldName,
			Type:         destSourceFieldDef.Type, // Preserve destination field type
			IsRequired:   false,
			IsUnique:     false,
			DefaultValue: nil,
		}

		s.logger.Debugf("Added reverse field '%s' (type %s) to source bundle '%s' for ManyToMany relationship",
			reverseFieldName, destSourceFieldDef.Type, bundle.Name)

	default:
		return fmt.Errorf("unsupported relationship type: %s", relationship.RelationshipType)
	}

	// REFERENTIAL INTEGRITY: Automatically create hash index on source field for FK validation
	// Foreign key validation needs O(1) lookup to verify parent documents exist.
	// This ensures ValidateUpdateForeignKey() and ValidateInsertForeignKey() can efficiently
	// check if a referenced document exists in the source bundle.
	sourceIndexName := fmt.Sprintf("%s_fk", relationshipCommand.SourceField)
	if _, exists := bundle.Indexes[sourceIndexName]; !exists {
		s.logger.Debugf("Automatically creating hash index '%s' on source field '%s.%s' for FK validation",
			sourceIndexName, bundle.Name, relationshipCommand.SourceField)

		sourceIndexCommand := &models.CreateIndexCommand{
			IndexName:  sourceIndexName,
			BundleName: bundle.Name,
			IndexType:  "hash",
			Fields: []models.FieldDefinition{
				{
					Name:     relationshipCommand.SourceField,
					Type:     sourceFieldDef.Type,
					IsUnique: false,
				},
			},
		}

		err := s.AddIndexToBundle(bundle.Database, bundle, sourceIndexCommand)
		if err != nil {
			// Log warning but don't fail the relationship creation
			s.logger.Warnf("Failed to automatically create source field index '%s': %v. "+
				"FK validation may be slower without this index.", sourceIndexName, err)
		} else {
			s.logger.Debugf("Successfully created hash index '%s' on source field for FK validation", sourceIndexName)
		}
	} else {
		s.logger.Debugf("Hash index '%s' already exists on source field '%s', skipping automatic creation",
			sourceIndexName, relationshipCommand.SourceField)
	}

	// Update the source bundle in the store
	err := s.store.UpdateBundleFile(bundle.Database, bundle)
	if err != nil {
		return fmt.Errorf("failed to update source bundle in store: %w", err)
	}

	// Update the cache with the modified bundle
	s.bundleMetadata[bundle.Name] = bundle

	// GRAPHQL INTEGRATION: Regenerate GraphQL schema after relationship changes
	// Relationships add new fields to bundles (e.g., user.posts: [Post], post.author: User)
	// Both source and destination bundles need schema regeneration.
	//
	// This reuses the regenerateGraphQLSchema method for consistency.
	// Schema update failures are logged but don't fail the relationship creation.
	if err := s.regenerateGraphQLSchema(bundle); err != nil {
		s.logger.Warnf("[GraphQL] Failed to regenerate schema for source bundle '%s': %v. Relationship was created successfully.",
			bundle.Name, err)
	}

	// Regenerate destination bundle schema if it's different from source
	// (some relationships may be self-referential, e.g., user.manager -> user)
	if relationshipCommand.DestinationBundle != bundle.Name {
		destBundle, err := s.GetBundleByName(bundle.Database, relationshipCommand.DestinationBundle)
		if err == nil {
			if err := s.regenerateGraphQLSchema(destBundle); err != nil {
				s.logger.Warnf("[GraphQL] Failed to regenerate schema for destination bundle '%s': %v. Relationship was created successfully.",
					destBundle.Name, err)
			}
		} else {
			s.logger.Warnf("[GraphQL] Could not get destination bundle '%s' for schema regeneration: %v",
				relationshipCommand.DestinationBundle, err)
		}
	}

	s.logger.Debugf("Successfully added relationship '%s' to bundle '%s'", relationshipName, bundle.Name)
	return nil
}

// RemoveRelationshipFromBundle removes a relationship from a bundle by name.
// This is a metadata-only operation that removes the relationship definition while preserving
// all fields, indexes, and document data. The foreign key fields remain in place on both
// the source and destination bundles, and any auto-created indexes are also preserved.
// Parameters:
//   - bundle: The source bundle containing the relationship to remove
//   - relationshipName: The name of the relationship to remove (e.g., "Authors_Books_1")
//
// Returns: error if bundle is nil, relationship name is empty, relationship not found, or persistence fails
func (s *BundleService) RemoveRelationshipFromBundle(bundle *models.Bundle, relationshipName string) error {
	// Validate inputs following SyndrDB defensive programming practices
	if bundle == nil {
		return fmt.Errorf("bundle is nil")
	}
	if relationshipName == "" {
		return fmt.Errorf("relationship name cannot be empty")
	}

	// Check if relationship exists in bundle
	relationship, exists := bundle.Relationships[relationshipName]
	if !exists {
		return fmt.Errorf("relationship '%s' not found on bundle '%s'", relationshipName, bundle.Name)
	}

	s.logger.Debugf("Removing relationship '%s' (type: %s) from bundle '%s' to bundle '%s'",
		relationshipName, relationship.RelationshipType, bundle.Name, relationship.DestinationBundle)

	// Remove relationship from bundle metadata (metadata-only operation)
	delete(bundle.Relationships, relationshipName)

	// TODO: Add CASCADE option to remove auto-created foreign key fields and hash indexes when dropping relationship
	// TODO: Add RESTRICT validation to block drop if documents contain non-null foreign key values

	// Persist bundle metadata changes to disk
	err := s.store.UpdateBundleFile(bundle.Database, bundle)
	if err != nil {
		return fmt.Errorf("failed to update bundle file: %w", err)
	}

	// Regenerate GraphQL schema for source bundle only (follows AddRelationshipToBundle pattern)
	// This reuses the regenerateGraphQLSchema method for consistency.
	if err := s.regenerateGraphQLSchema(bundle); err != nil {
		s.logger.Warnf("[GraphQL] Failed to regenerate schema for bundle '%s' after relationship drop: %v", bundle.Name, err)
		// Continue despite GraphQL error - schema regeneration is non-critical
	}

	s.logger.Debugf("Successfully removed relationship '%s' from bundle '%s'", relationshipName, bundle.Name)
	return nil
}

// generateRelationshipName generates a unique relationship name with counter
// TODO This should go into a seperate bundle utilities file
func (s *BundleService) generateRelationshipName(bundle *models.Bundle, sourceBundle, destinationBundle string) string {
	baseName := fmt.Sprintf("%s_%s", sourceBundle, destinationBundle)
	counter := 1

	// Check for existing relationships with similar names and increment counter
	for {
		relationshipName := fmt.Sprintf("%s_%d", baseName, counter)
		if _, exists := bundle.Relationships[relationshipName]; !exists {
			return relationshipName
		}
		counter++
	}
}

// addFieldToDestinationBundle adds a relationship field to the destination bundle
// and automatically creates a hash index on the foreign key field for referential integrity
func (s *BundleService) addFieldToDestinationBundle(sourceBundle *models.Bundle, relationship *models.Relationship, isRequired, isUnique bool) error {
	// Find the destination bundle
	destinationBundle, err := s.GetBundleByName(sourceBundle.Database, relationship.DestinationBundle)
	if err != nil {
		return fmt.Errorf("destination bundle '%s' not found: %w", relationship.DestinationBundle, err)
	}

	// Lookup source field to preserve its type
	sourceFieldDef, exists := sourceBundle.DocumentStructure.FieldDefinitions[relationship.SourceField]
	if !exists {
		return fmt.Errorf("source field '%s' not found in bundle '%s'", relationship.SourceField, sourceBundle.Name)
	}

	// Check if field definitions map is initialized
	if destinationBundle.DocumentStructure.FieldDefinitions == nil {
		destinationBundle.DocumentStructure.FieldDefinitions = make(map[string]models.FieldDefinition)
	}

	// Add the relationship field to the destination bundle with preserved source type
	fieldName := relationship.DestinationField
	destinationBundle.DocumentStructure.FieldDefinitions[fieldName] = models.FieldDefinition{
		Name:         fieldName,
		Type:         sourceFieldDef.Type, // Preserve source field type instead of hardcoding "relationship"
		IsRequired:   isRequired,
		IsUnique:     isUnique,
		DefaultValue: nil,
	}

	s.logger.Debugf("Creating FK field '%s' with preserved type '%s' (from %s.%s)",
		fieldName, sourceFieldDef.Type, sourceBundle.Name, relationship.SourceField)

	// Update the destination bundle in the store
	err = s.store.UpdateBundleFile(destinationBundle.Database, destinationBundle)
	if err != nil {
		return fmt.Errorf("failed to update destination bundle '%s' in store: %w", destinationBundle.Name, err)
	}

	// s.logger.Debugf("Added relationship field '%s' to destination bundle '%s' (required=%t, unique=%t)",
	// 	fieldName, destinationBundle.Name, isRequired, isUnique)

	// Automatically create hash index on the foreign key field for referential integrity
	// This ensures that ValidateDelete() can perform O(1) lookups
	// Note: Index name should NOT include bundle name as infrastructure adds it automatically
	indexName := fmt.Sprintf("%s_fk", fieldName)

	// Check if index already exists
	if _, exists := destinationBundle.Indexes[indexName]; !exists {
		s.logger.Debugf("Automatically creating hash index '%s' on foreign key field '%s' in bundle '%s'",
			indexName, fieldName, destinationBundle.Name)

		// Create index command using FieldDefinition type
		indexCommand := &models.CreateIndexCommand{
			IndexName:  indexName,
			BundleName: destinationBundle.Name,
			IndexType:  "hash",
			Fields: []models.FieldDefinition{
				{
					Name:     fieldName,
					Type:     sourceFieldDef.Type, // Use preserved source field type
					IsUnique: false,               // Foreign keys are typically not unique (1-to-many)
				},
			},
		}

		// Reuse existing AddIndexToBundle infrastructure
		err = s.AddIndexToBundle(destinationBundle.Database, destinationBundle, indexCommand)
		if err != nil {
			// Log the error but don't fail the relationship creation
			// The relationship will still work, just without automatic referential integrity validation
			s.logger.Warnf("Failed to automatically create index on foreign key field '%s': %v. "+
				"Referential integrity validation will require manual index creation.", fieldName, err)
		} else {
			s.logger.Debugf("Successfully created hash index '%s' for referential integrity validation", indexName)
		}
	} else {
		s.logger.Debugf("Hash index '%s' already exists on foreign key field '%s', skipping automatic creation",
			indexName, fieldName)
	}

	return nil
}

// IsFieldForeignKey checks if a field is a foreign key based on relationships from other bundles
// Returns true if the field is used as a foreign key in any relationship
// This function checks both:
// 1. If this bundle has relationships where this field is a source field (outgoing FK)
// 2. If other bundles have relationships where this field is a destination field (incoming FK)
func IsFieldForeignKey(bundle *models.Bundle, fieldName string) (bool, string, string) {
	// Check if this field is used as a source field in any relationship within this bundle
	// This means it references another bundle (making it an outgoing foreign key)
	if bundle.Relationships != nil {
		for _, relationship := range bundle.Relationships {
			if relationship.SourceField == fieldName {
				return true, relationship.DestinationBundle, relationship.DestinationField
			}
		}
	}

	// Check if this field is referenced as a destination field by other bundles
	// This means other bundles reference this field (making it an incoming foreign key)
	if bundle.Database != nil {
		for _, otherBundle := range bundle.Database.Bundles {
			if otherBundle.Relationships != nil {
				for _, relationship := range otherBundle.Relationships {
					// Check if this relationship points to our bundle and field
					if relationship.DestinationBundle == bundle.Name && relationship.DestinationField == fieldName {
						return true, relationship.SourceBundle, relationship.SourceField
					}
				}
			}
		}
	}

	return false, "", ""
}
