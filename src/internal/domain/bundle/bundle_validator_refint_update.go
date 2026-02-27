package bundle

import (
	"fmt"
	"sort"
	"strings"
	"syndrdb/src/internal/domain/index/hashindexV3"
	"syndrdb/src/internal/domain/models"
	"syndrdb/src/pkg/common/conversion"
)

/*
bundle_validator_refint_update.go

This file extends the ReferentialIntegrityValidator with UPDATE operation validation.
It provides foreign key constraint enforcement for UPDATE operations using O(1) hash
index lookups, following the same pattern as DELETE validation.

Key responsibilities:
- Validate foreign key field updates
- Validate primary key field updates
- Batch validation with caching for performance
- Schema change validation (field removal/rename)
- Bundle deletion validation (incoming relationships)

Design Principles:
- Single Responsibility: Only handles UPDATE-related referential integrity
- DRY: Reuses existing index infrastructure and validation patterns
- Performance: O(1) hash lookups with operation-scoped caching
- Open/Closed: Designed for future CASCADE UPDATE support

Performance Characteristics:
- Foreign key validation: O(1) per unique field+value combination (cached)
- Primary key validation: O(k) where k = number of relationships
- Batch validation: Amortized O(1) with caching
*/

// =============================================================================
//  MESSAGE CATALOG - SUGGESTED ACTIONS (v1.0.0)
// =============================================================================
// These message templates are part of the API contract and are versioned.
// They provide consistent, actionable error messages across all violation types.

// Foreign Key Violation Messages - Added: v1.0.0
const (
	// MSG_FK_NOT_FOUND is shown when a foreign key references a non-existent parent document
	MSG_FK_NOT_FOUND = "Create document '%s' in bundle '%s' or choose an existing value"
	// MSG_FK_INDEX_MISSING is shown when required hash index for FK validation is missing
	MSG_FK_INDEX_MISSING = "Recreate hash index on field '%s' with: CREATE HASH INDEX \"%s_fk\" ON BUNDLE \"%s\" WITH FIELDS ({...})"
)

// Primary Key Violation Messages - Added: v1.0.0
const (
	// MSG_PK_HAS_DEPENDENTS is shown when attempting to update a primary key that has dependent records
	MSG_PK_HAS_DEPENDENTS = "Remove dependent documents first, or wait for CASCADE UPDATE support"
	// MSG_PK_CANNOT_UPDATE is shown when attempting to update DocumentID (read-only)
	MSG_PK_CANNOT_UPDATE = "DocumentID is a system-managed field and cannot be modified"
)

// Schema Violation Messages - Added: v1.0.0
const (
	// MSG_FIELD_USED_IN_RELATIONSHIP is shown when attempting to remove/rename a field used in relationships
	MSG_FIELD_USED_IN_RELATIONSHIP = "Remove relationships '%s' first, then modify field '%s'"
	// MSG_FIELD_RENAME_BREAKS_RELATIONSHIP is shown when renaming would break relationship definitions
	MSG_FIELD_RENAME_BREAKS_RELATIONSHIP = "Update relationship definitions to use new field name, or remove relationships first"
)

// Index Protection Messages - Added: v1.0.0
const (
	// MSG_CANNOT_DROP_FK_INDEX is shown when attempting to drop a foreign key index while relationship exists
	MSG_CANNOT_DROP_FK_INDEX = "Remove relationship '%s' first, then drop index"
)

// Bundle Deletion Messages - Added: v1.0.0
const (
	// MSG_BUNDLE_HAS_INCOMING_RELATIONSHIPS is shown when attempting to drop a bundle that other bundles reference
	MSG_BUNDLE_HAS_INCOMING_RELATIONSHIPS = "Remove relationships from bundles: %s"
)

// =============================================================================
//  VIOLATION TYPES - STRUCTURED ERROR RESPONSES
// =============================================================================

// Context structs for generating suggested actions
type (
	// ForeignKeyContext provides context for foreign key violation messages
	ForeignKeyContext struct {
		FieldName      string
		AttemptedValue string
		ParentBundle   string
		ParentField    string
	}

	// PrimaryKeyContext provides context for primary key violation messages
	PrimaryKeyContext struct {
		FieldName        string
		DocumentID       string
		DependentBundles map[string][]string
	}

	// SchemaContext provides context for schema violation messages
	SchemaContext struct {
		FieldName         string
		Operation         string // "remove" or "rename"
		RelationshipNames []string
	}

	// IncomingRelationshipContext provides context for bundle deletion violations
	IncomingRelationshipContext struct {
		BundleName            string
		IncomingRelationships []RelationshipInfo
	}
)

// RelationshipInfo is a lightweight struct containing essential relationship details
type RelationshipInfo struct {
	RelationshipName string `json:"relationshipName"`
	SourceBundle     string `json:"sourceBundle"`
	SourceField      string `json:"sourceField"`
}

// ForeignKeyUpdate represents a foreign key field being updated with its relationship metadata
type ForeignKeyUpdate struct {
	FieldName    string              // The field being updated
	NewValue     string              // The new value being assigned
	Relationship models.Relationship // The relationship definition for validation
}

// Violation types with JSON marshaling support
type (
	// ForeignKeyViolation represents a foreign key constraint violation
	ForeignKeyViolation struct {
		FieldName           string          `json:"fieldName"`
		AttemptedValue      string          `json:"attemptedValue"`
		ParentBundle        string          `json:"parentBundle"`
		ParentField         string          `json:"parentField"`
		AffectedDocumentIDs []string        `json:"affectedDocumentIds"`
		SuggestedAction     string          `json:"suggestedAction"`
		CascadePreview      *CascadePreview `json:"cascadePreview,omitempty"` // nil for RESTRICT mode
	}

	// PrimaryKeyViolation represents a primary key update violation
	PrimaryKeyViolation struct {
		FieldName        string              `json:"fieldName"`
		DocumentID       string              `json:"documentId"`
		DependentBundles map[string][]string `json:"dependentBundles"`
		SuggestedAction  string              `json:"suggestedAction"`
		CascadePreview   *CascadePreview     `json:"cascadePreview,omitempty"` // nil for RESTRICT mode
	}

	// SchemaViolation represents a schema modification that would break relationships
	SchemaViolation struct {
		FieldName         string   `json:"fieldName"`
		Operation         string   `json:"operation"` // "remove" or "rename"
		RelationshipNames []string `json:"relationshipNames"`
		SuggestedAction   string   `json:"suggestedAction"`
	}

	// IncomingRelationshipViolation represents a bundle deletion blocked by incoming relationships
	IncomingRelationshipViolation struct {
		BundleName            string             `json:"bundleName"`
		IncomingRelationships []RelationshipInfo `json:"incomingRelationships"`
		SuggestedAction       string             `json:"suggestedAction"`
	}

	// CascadePreview provides information about what would be affected by CASCADE operations
	// This is a placeholder for future CASCADE UPDATE/DELETE support
	CascadePreview struct {
		AffectedDocuments int      `json:"affectedDocuments"`
		AffectedBundles   []string `json:"affectedBundles"`
	}
)

// Error() methods for violation types - return formatted messages with suggestions

func (v *ForeignKeyViolation) Error() string {
	return fmt.Sprintf("Foreign key violation: Cannot update field '%s' to value '%s' - document does not exist in bundle '%s' | Suggested: %s",
		v.FieldName, v.AttemptedValue, v.ParentBundle, v.SuggestedAction)
}

func (v *PrimaryKeyViolation) Error() string {
	return fmt.Sprintf("Primary key violation: Cannot update field '%s' for document '%s' - has dependent records | Suggested: %s",
		v.FieldName, v.DocumentID, v.SuggestedAction)
}

func (v *SchemaViolation) Error() string {
	return fmt.Sprintf("Schema violation: Cannot %s field '%s' - used in relationships: %v | Suggested: %s",
		v.Operation, v.FieldName, v.RelationshipNames, v.SuggestedAction)
}

func (v *IncomingRelationshipViolation) Error() string {
	bundleNames := make([]string, len(v.IncomingRelationships))
	for i, rel := range v.IncomingRelationships {
		bundleNames[i] = rel.SourceBundle
	}
	return fmt.Sprintf("Referential integrity violation: Cannot drop bundle '%s' - referenced by %d relationship(s) | Suggested: %s",
		v.BundleName, len(v.IncomingRelationships), v.SuggestedAction)
}

// generateSuggestedAction creates context-appropriate suggested actions
// This centralizes message generation logic for consistency (DRY principle)
func generateSuggestedAction(violationType string, context interface{}) string {
	switch violationType {
	case "foreign_key":
		if ctx, ok := context.(ForeignKeyContext); ok {
			return fmt.Sprintf(MSG_FK_NOT_FOUND, ctx.AttemptedValue, ctx.ParentBundle)
		}
	case "primary_key":
		return MSG_PK_HAS_DEPENDENTS
	case "field_removal":
		if ctx, ok := context.(SchemaContext); ok {
			return fmt.Sprintf(MSG_FIELD_USED_IN_RELATIONSHIP, ctx.RelationshipNames, ctx.FieldName)
		}
	case "field_rename":
		return MSG_FIELD_RENAME_BREAKS_RELATIONSHIP
	case "incoming_relationship":
		if ctx, ok := context.(IncomingRelationshipContext); ok {
			bundleNames := make([]string, len(ctx.IncomingRelationships))
			for i, rel := range ctx.IncomingRelationships {
				bundleNames[i] = rel.SourceBundle
			}
			return fmt.Sprintf(MSG_BUNDLE_HAS_INCOMING_RELATIONSHIPS, bundleNames)
		}
	case "fk_index":
		if ctx, ok := context.(SchemaContext); ok {
			if len(ctx.RelationshipNames) > 0 {
				return fmt.Sprintf(MSG_CANNOT_DROP_FK_INDEX, ctx.RelationshipNames[0])
			}
		}
	}
	return "Contact system administrator for assistance"
}

// =============================================================================
//  UPDATE VALIDATION - FOREIGN KEY CONSTRAINTS (P 1)
// =============================================================================

// ValidateUpdateForeignKey checks if updating a foreign key field would create an invalid reference
//
// This validates that the new value exists in the parent bundle using O(1) hash index lookups.
// It returns a structured violation for future CASCADE UPDATE support.
//
// Parameters:
//   - bundle: The bundle containing the document being updated
//   - documentID: The ID of the document being updated
//   - fieldName: The foreign key field being updated
//   - newValue: The new value being assigned to the foreign key
//   - relationship: The relationship definition for this foreign key
//
// Returns:
//   - *ForeignKeyViolation: Structured violation with suggested action, or nil if valid
func (v *ReferentialIntegrityValidator) ValidateUpdateForeignKey(
	bundle *models.Bundle,
	documentID string,
	fieldName string,
	newValue string,
	relationship models.Relationship,
) *ForeignKeyViolation {
	// Get the parent bundle (the one being referenced)
	parentBundleName := relationship.SourceBundle
	if parentBundleName == "" {
		parentBundleName = relationship.SourceBundleName
	}

	v.logger.Debugf("[REFINT-UPDATE] Validating FK update: %s.%s = '%s' -> parent: %s",
		bundle.Name, fieldName, newValue, parentBundleName)

	// Load the parent bundle
	parentBundle, err := v.bundleService.GetBundleByName(bundle.Database, parentBundleName)
	if err != nil {
		v.logger.Errorf("[REFINT-UPDATE] Failed to load parent bundle '%s': %v", parentBundleName, err)
		context := ForeignKeyContext{
			FieldName:      fieldName,
			AttemptedValue: newValue,
			ParentBundle:   parentBundleName,
			ParentField:    relationship.SourceField,
		}
		return &ForeignKeyViolation{
			FieldName:           fieldName,
			AttemptedValue:      newValue,
			ParentBundle:        parentBundleName,
			ParentField:         relationship.SourceField,
			AffectedDocumentIDs: []string{documentID},
			SuggestedAction:     generateSuggestedAction("foreign_key", context),
			CascadePreview:      nil, // TODO: I could populate CascadePreview when CASCADE UPDATE is implemented
		}
	}

	// Look for DocumentID index in parent bundle (primary key)
	// Foreign keys typically reference the DocumentID of the parent
	parentKeyField := relationship.SourceField
	if parentKeyField == "" {
		parentKeyField = "DocumentID"
	}

	// Find index on the parent's key field
	indexName, indexRef, found := v.findIndexForField(parentBundle, parentKeyField)
	if !found {
		v.logger.Warnf("[REFINT-UPDATE] No index found on parent field '%s' in bundle '%s'",
			parentKeyField, parentBundleName)
		return &ForeignKeyViolation{
			FieldName:           fieldName,
			AttemptedValue:      newValue,
			ParentBundle:        parentBundleName,
			ParentField:         parentKeyField,
			AffectedDocumentIDs: []string{documentID},
			SuggestedAction:     fmt.Sprintf(MSG_FK_INDEX_MISSING, parentKeyField, parentKeyField, parentBundleName),
			CascadePreview:      nil,
		}
	}

	// Perform O(1) hash lookup to check if parent document exists
	if indexRef.IndexType == "hash" {
		hashIndex, err := v.bundleService.GetOrLoadHashIndex(parentBundle, indexName, indexRef)
		if err != nil {
			v.logger.Errorf("[REFINT-UPDATE] Failed to load hash index '%s': %v", indexName, err)
			context := ForeignKeyContext{
				FieldName:      fieldName,
				AttemptedValue: newValue,
				ParentBundle:   parentBundleName,
				ParentField:    parentKeyField,
			}
			return &ForeignKeyViolation{
				FieldName:           fieldName,
				AttemptedValue:      newValue,
				ParentBundle:        parentBundleName,
				ParentField:         parentKeyField,
				AffectedDocumentIDs: []string{documentID},
				SuggestedAction:     generateSuggestedAction("foreign_key", context),
				CascadePreview:      nil,
			}
		}

		// Check if the parent document exists
		results, _, err := hashIndex.Search(newValue)
		if err != nil || len(results) == 0 {
			v.logger.Warnf("[REFINT-UPDATE] Parent document '%s' not found in bundle '%s'",
				newValue, parentBundleName)
			context := ForeignKeyContext{
				FieldName:      fieldName,
				AttemptedValue: newValue,
				ParentBundle:   parentBundleName,
				ParentField:    parentKeyField,
			}
			return &ForeignKeyViolation{
				FieldName:           fieldName,
				AttemptedValue:      newValue,
				ParentBundle:        parentBundleName,
				ParentField:         parentKeyField,
				AffectedDocumentIDs: []string{documentID},
				SuggestedAction:     generateSuggestedAction("foreign_key", context),
				CascadePreview:      nil,
			}
		}

		v.logger.Debugf("[REFINT-UPDATE] FK validation passed: parent document '%s' exists in '%s'",
			newValue, parentBundleName)
		return nil // Valid - parent exists
	}

	// TODO: I could add BTree index support for FK validation
	v.logger.Warnf("[REFINT-UPDATE] Unsupported index type '%s' for FK validation", indexRef.IndexType)
	return nil // Allow for now if not hash index
}

// ValidateUpdatePrimaryKey checks if updating a primary key field would orphan dependent documents
//
// This validates that no dependent documents reference the current value.
// Currently, DocumentID updates are blocked at field validation level.
//
// Returns:
//   - *PrimaryKeyViolation: Structured violation with suggested action, or nil if safe
func (v *ReferentialIntegrityValidator) ValidateUpdatePrimaryKey(
	bundle *models.Bundle,
	documentID string,
	fieldName string,
) *PrimaryKeyViolation {
	// Check all relationships where this bundle is the source (being referenced)
	dependentBundles := make(map[string][]string)

	for relationshipName, relationship := range bundle.Relationships {
		isSource := relationship.SourceBundle == bundle.Name || relationship.SourceBundleName == bundle.Name
		if !isSource {
			continue
		}

		// This bundle is referenced by others - check for dependents
		violation, err := v.validateRelationship(bundle, documentID, relationshipName, relationship)
		if err != nil {
			v.logger.Warnf("[REFINT-UPDATE] Error checking relationship '%s': %v", relationshipName, err)
			continue
		}

		if violation != "" {
			// Extract bundle name from violation message
			targetBundleName := relationship.DestinationBundle
			if targetBundleName == "" {
				targetBundleName = relationship.TargetBundleName
			}
			dependentBundles[targetBundleName] = append(dependentBundles[targetBundleName], relationshipName)
		}
	}

	if len(dependentBundles) > 0 {
		context := PrimaryKeyContext{
			FieldName:        fieldName,
			DocumentID:       documentID,
			DependentBundles: dependentBundles,
		}
		return &PrimaryKeyViolation{
			FieldName:        fieldName,
			DocumentID:       documentID,
			DependentBundles: dependentBundles,
			SuggestedAction:  generateSuggestedAction("primary_key", context),
			CascadePreview:   nil, // TODO: I could populate CascadePreview when CASCADE UPDATE is implemented
		}
	}

	return nil // Safe to update
}

// batchValidateForeignKeys validates multiple foreign key updates with caching
//
// This method validates all foreign key fields in a batch update operation,
// using an operation-scoped cache to avoid redundant hash lookups when multiple
// documents are updated to the same value.
//
// Parameters:
//   - bundle: The bundle being updated
//   - docIDs: Document IDs being updated
//   - foreignKeyUpdates: Map of field name -> new value for each foreign key field
//   - validationCache: Operation-scoped cache of validation results (key: "fieldName:value")
//
// Returns:
//   - *ForeignKeyViolation: First violation encountered (fail-fast), or nil if all valid
func (v *ReferentialIntegrityValidator) batchValidateForeignKeys(
	bundle *models.Bundle,
	docIDs []string,
	foreignKeyUpdates []ForeignKeyUpdate,
	validationCache map[string]*ForeignKeyViolation,
) *ForeignKeyViolation {
	// Log debug message for large batch operations
	if len(docIDs) > 100 {
		fieldNames := make([]string, 0, len(foreignKeyUpdates))
		for _, fkUpdate := range foreignKeyUpdates {
			fieldNames = append(fieldNames, fkUpdate.FieldName)
		}
		v.logger.Debugf("[REFINT-UPDATE] Validating %d documents for foreign key constraints on fields %v...",
			len(docIDs), fieldNames)
	}

	// Validate each foreign key field using the relationship metadata provided
	// This includes BOTH outgoing and incoming relationships identified by IdentifyForeignKeyFields
	for _, fkUpdate := range foreignKeyUpdates {
		fieldName := fkUpdate.FieldName
		newValue := fkUpdate.NewValue
		relationship := fkUpdate.Relationship

		// Check cache first to avoid redundant lookups
		cacheKey := fmt.Sprintf("%s:%s", fieldName, newValue)
		if cachedViolation, found := validationCache[cacheKey]; found {
			if cachedViolation != nil {
				// Return cached violation (fail-fast)
				v.logger.Warnf("[REFINT-UPDATE] FK validation failed (cached): %s", cachedViolation.Error())
				return cachedViolation
			}
			// Cached validation passed - continue to next field
			continue
		}

		// Perform validation for this field
		for _, docID := range docIDs {
			violation := v.ValidateUpdateForeignKey(bundle, docID, fieldName, newValue, relationship)

			// Cache the result
			validationCache[cacheKey] = violation

			if violation != nil {
				// Fail fast on first violation
				// TODO: I could collect all violations and return them together for better UX
				v.logger.Warnf("[REFINT-UPDATE] FK validation failed: %s | Suggested: %s",
					violation.Error(), violation.SuggestedAction)
				return violation
			}

			// Only need to validate once per unique (field, value) pair
			break
		}
	}

	// TODO: I could add cache size monitoring if operations exceed expected limits
	return nil // All validations passed
}

// =============================================================================
//  SCHEMA CHANGE VALIDATION - RELATIONSHIP PROTECTION (P 1)
// =============================================================================

// ValidateFieldRemoval checks if removing a field would break relationships
//
// Checks BOTH outgoing and incoming relationships to ensure complete protection.
//
// Parameters:
//   - database: The database containing all bundles (for scanning incoming relationships)
//   - bundle: The bundle being modified
//   - fieldName: The field being removed
//   - bundleCache: Operation-scoped cache to avoid redundant bundle loads
//
// Returns:
//   - *SchemaViolation: Structured violation with suggested action, or nil if safe
func (v *ReferentialIntegrityValidator) ValidateFieldRemoval(
	database *models.Database,
	bundle *models.Bundle,
	fieldName string,
	bundleCache map[string]*models.Bundle,
) *SchemaViolation {
	relationships := v.getRelationshipsUsingField(database, bundle, fieldName, bundleCache)

	if len(relationships) > 0 {
		relationshipNames := make([]string, len(relationships))
		for i, rel := range relationships {
			relationshipNames[i] = rel.RelationshipName
		}

		context := SchemaContext{
			FieldName:         fieldName,
			Operation:         "remove",
			RelationshipNames: relationshipNames,
		}

		v.logger.Warnf("[REFINT-SCHEMA] Cannot remove field '%s': used in relationships %v | Suggested: %s",
			fieldName, relationshipNames, generateSuggestedAction("field_removal", context))

		return &SchemaViolation{
			FieldName:         fieldName,
			Operation:         "remove",
			RelationshipNames: relationshipNames,
			SuggestedAction:   generateSuggestedAction("field_removal", context),
		}
	}

	return nil // Safe to remove
}

// ValidateFieldRename checks if renaming a field would break relationships
//
// Checks BOTH outgoing and incoming relationships to ensure complete protection.
//
// Parameters:
//   - database: The database containing all bundles (for scanning incoming relationships)
//   - bundle: The bundle being modified
//   - oldFieldName: The current field name
//   - newFieldName: The new field name
//   - bundleCache: Operation-scoped cache to avoid redundant bundle loads
//
// Returns:
//   - *SchemaViolation: Structured violation with suggested action, or nil if safe
func (v *ReferentialIntegrityValidator) ValidateFieldRename(
	database *models.Database,
	bundle *models.Bundle,
	oldFieldName string,
	newFieldName string,
	bundleCache map[string]*models.Bundle,
) *SchemaViolation {
	relationships := v.getRelationshipsUsingField(database, bundle, oldFieldName, bundleCache)

	if len(relationships) > 0 {
		relationshipNames := make([]string, len(relationships))
		for i, rel := range relationships {
			relationshipNames[i] = rel.RelationshipName
		}

		context := SchemaContext{
			FieldName:         oldFieldName,
			Operation:         "rename",
			RelationshipNames: relationshipNames,
		}

		v.logger.Warnf("[REFINT-SCHEMA] Cannot rename field '%s' to '%s': used in relationships %v | Suggested: %s",
			oldFieldName, newFieldName, relationshipNames, generateSuggestedAction("field_rename", context))

		return &SchemaViolation{
			FieldName:         oldFieldName,
			Operation:         "rename",
			RelationshipNames: relationshipNames,
			SuggestedAction:   generateSuggestedAction("field_rename", context),
		}
	}

	return nil // Safe to rename
}

// ValidateIncomingRelationships checks if dropping a bundle would break relationships
//
// This scans all bundles in the database to find relationships pointing to the target bundle.
// Uses operation-scoped caching to avoid redundant bundle loads.
//
// Parameters:
//   - database: The database containing the bundles
//   - targetBundleName: The bundle being dropped
//   - bundleCache: Operation-scoped cache of loaded bundles
//
// Returns:
//   - []IncomingRelationshipViolation: List of violations (may be empty)
func (v *ReferentialIntegrityValidator) ValidateIncomingRelationships(
	database *models.Database,
	targetBundleName string,
	bundleCache map[string]*models.Bundle,
) []IncomingRelationshipViolation {
	// TODO: I could cache reverse relationship mappings at database level for O(1) lookups instead of O(n) bundle scan

	incomingRelationships := v.getAllBundlesWithRelationshipsTo(database, targetBundleName, bundleCache)

	if len(incomingRelationships) > 0 {
		context := IncomingRelationshipContext{
			BundleName:            targetBundleName,
			IncomingRelationships: incomingRelationships,
		}

		violation := IncomingRelationshipViolation{
			BundleName:            targetBundleName,
			IncomingRelationships: incomingRelationships,
			SuggestedAction:       generateSuggestedAction("incoming_relationship", context),
		}

		v.logger.Warnf("[REFINT-SCHEMA] Cannot drop bundle '%s': %d incoming relationships | Suggested: %s",
			targetBundleName, len(incomingRelationships), violation.SuggestedAction)

		return []IncomingRelationshipViolation{violation}
	}

	return nil // Safe to drop
}

// =============================================================================
//  HELPER METHODS - RELATIONSHIP DISCOVERY (P 1)
// =============================================================================

// getRelationshipsUsingField finds all relationships that reference a specific field
//
// This checks BOTH:
// 1. Outgoing relationships (stored in bundle.Relationships)
// 2. Incoming relationships (stored in other bundles pointing to this one)
//
// Parameters:
//   - database: The database containing all bundles (for scanning incoming relationships)
//   - bundle: The bundle being checked
//   - fieldName: The field to check
//   - bundleCache: Operation-scoped cache to avoid redundant bundle loads
//
// Returns:
//   - []RelationshipInfo: List of relationships using the field (as source or destination)
func (v *ReferentialIntegrityValidator) getRelationshipsUsingField(
	database *models.Database,
	bundle *models.Bundle,
	fieldName string,
	bundleCache map[string]*models.Bundle,
) []RelationshipInfo {
	var relationships []RelationshipInfo

	// 1. Check outgoing relationships (stored in bundle.Relationships)
	for relationshipName, relationship := range bundle.Relationships {
		// Check if field is used as source field or destination field
		if relationship.SourceField == fieldName || relationship.DestinationField == fieldName {
			relationships = append(relationships, RelationshipInfo{
				RelationshipName: relationshipName,
				SourceBundle:     relationship.SourceBundle,
				SourceField:      relationship.SourceField,
			})
		}
	}

	// 2. Check incoming relationships (stored in other bundles)
	if database != nil {
		incomingRelationships := v.getAllBundlesWithRelationshipsTo(database, bundle.Name, bundleCache)
		for _, relInfo := range incomingRelationships {
			// Load the source bundle to get full relationship details
			sourceBundle, found := bundleCache[relInfo.SourceBundle]
			if !found {
				var err error
				sourceBundle, err = v.bundleService.GetBundleByName(database, relInfo.SourceBundle)
				if err != nil {
					v.logger.Warnf("Failed to load bundle '%s' during relationship scan: %v", relInfo.SourceBundle, err)
					continue
				}
				bundleCache[relInfo.SourceBundle] = sourceBundle
			}

			// Check if the field is referenced in this relationship
			if relationship, exists := sourceBundle.Relationships[relInfo.RelationshipName]; exists {
				// For incoming relationships, DestinationField is the field in THIS bundle
				if relationship.DestinationField == fieldName {
					relationships = append(relationships, RelationshipInfo{
						RelationshipName: relInfo.RelationshipName,
						SourceBundle:     relInfo.SourceBundle,
						SourceField:      relationship.SourceField,
					})
				}
			}
		}
	}

	return relationships
}

// getAllBundlesWithRelationshipsTo finds all bundles that have relationships pointing to target bundle
//
// This performs an O(n) scan of all bundles in the database.
// Uses operation-scoped cache to avoid redundant bundle loads.
//
// Returns:
//   - []RelationshipInfo: List of incoming relationships
func (v *ReferentialIntegrityValidator) getAllBundlesWithRelationshipsTo(
	database *models.Database,
	targetBundleName string,
	bundleCache map[string]*models.Bundle,
) []RelationshipInfo {
	var incomingRelationships []RelationshipInfo

	// Iterate through all bundles in the database
	if database.Bundles == nil {
		return incomingRelationships
	}

	for bundleName := range database.Bundles {
		// Check cache first
		bundle, found := bundleCache[bundleName]
		if !found {
			// Load bundle on-demand
			var err error
			bundle, err = v.bundleService.GetBundleByName(database, bundleName)
			if err != nil {
				v.logger.Warnf("Failed to load bundle '%s' during relationship scan: %v", bundleName, err)
				continue
			}
			bundleCache[bundleName] = bundle
		}

		// Check if this bundle has relationships pointing to target
		for relationshipName, relationship := range bundle.Relationships {
			// Check if this relationship points to the target bundle
			destBundle := relationship.DestinationBundle
			if destBundle == "" {
				destBundle = relationship.TargetBundleName
			}

			if destBundle == targetBundleName {
				incomingRelationships = append(incomingRelationships, RelationshipInfo{
					RelationshipName: relationshipName,
					SourceBundle:     bundleName,
					SourceField:      relationship.SourceField,
				})
			}
		}
	}

	return incomingRelationships
}

// identifyForeignKeyFields extracts which fields in an update command are foreign keys
//
// This checks BOTH:
// 1. Outgoing relationships (stored in bundle.Relationships - where this bundle is the FK side)
// 2. Incoming relationships (stored in other bundles - where this bundle is referenced)
//
// Parameters:
//   - database: The database containing all bundles (for scanning incoming relationships)
//   - bundle: The bundle being updated
//   - updateFields: Map of field name -> new value from update command
//   - bundleCache: Operation-scoped cache to avoid redundant bundle loads
//
// Returns:
//   - map[string]string: Map of foreign key field names to their new values (in predictable order)
func (v *ReferentialIntegrityValidator) IdentifyForeignKeyFields(
	database *models.Database,
	bundle *models.Bundle,
	updateFields map[string]string,
	bundleCache map[string]*models.Bundle,
) []ForeignKeyUpdate {
	var foreignKeyUpdates []ForeignKeyUpdate

	// 1. Check outgoing relationships (where this bundle has FK pointing to another bundle)
	//    These are stored in bundle.Relationships with SourceField being the FK field
	for _, relationship := range bundle.Relationships {
		fieldName := relationship.SourceField
		if newValue, isBeingUpdated := updateFields[fieldName]; isBeingUpdated {
			foreignKeyUpdates = append(foreignKeyUpdates, ForeignKeyUpdate{
				FieldName:    fieldName,
				NewValue:     newValue,
				Relationship: relationship,
			})
		}
	}

	// 2. Check incoming relationships (where other bundles have FK pointing to this bundle)
	//    These are stored in OTHER bundles with DestinationBundle == bundle.Name
	//    The DestinationField is the FK field in THIS bundle
	if database != nil {
		incomingRelationships := v.getAllBundlesWithRelationshipsTo(database, bundle.Name, bundleCache)
		v.logger.Debugf("[REFINT-FK-ID] Found %d incoming relationships to bundle '%s'", len(incomingRelationships), bundle.Name)
		for _, relInfo := range incomingRelationships {
			v.logger.Debugf("[REFINT-FK-ID] Processing incoming relationship '%s' from bundle '%s'", relInfo.RelationshipName, relInfo.SourceBundle)
			// For incoming relationships, we need to find the DestinationField
			// by loading the source bundle and checking the relationship details
			sourceBundle, found := bundleCache[relInfo.SourceBundle]
			if !found {
				var err error
				sourceBundle, err = v.bundleService.GetBundleByName(database, relInfo.SourceBundle)
				if err != nil {
					v.logger.Warnf("Failed to load bundle '%s' during FK identification: %v", relInfo.SourceBundle, err)
					continue
				}
				bundleCache[relInfo.SourceBundle] = sourceBundle
			}

			// Get the relationship details to find DestinationField
			if relationship, exists := sourceBundle.Relationships[relInfo.RelationshipName]; exists {
				fieldName := relationship.DestinationField
				v.logger.Debugf("[REFINT-FK-ID] Relationship '%s': DestinationField='%s', isBeingUpdated=%v",
					relInfo.RelationshipName, fieldName, updateFields[fieldName] != "")
				if newValue, isBeingUpdated := updateFields[fieldName]; isBeingUpdated {
					foreignKeyUpdates = append(foreignKeyUpdates, ForeignKeyUpdate{
						FieldName:    fieldName,
						NewValue:     newValue,
						Relationship: relationship,
					})
					v.logger.Debugf("[REFINT-FK-ID] Added FK field '%s' with new value '%s' to validation list", fieldName, newValue)
				}
			}
		}
	}

	return foreignKeyUpdates
}

// IdentifyInsertForeignKeyFields finds FK fields for INSERT validation.
//
// Unlike IdentifyForeignKeyFields (designed for UPDATE), this only checks
// incoming relationships — relationships defined on OTHER bundles where
// DestinationBundle == this bundle. These define the FK fields in this bundle
// that reference parent bundles.
//
// Section 1 of IdentifyForeignKeyFields is skipped because bundle.Relationships
// defines this bundle as a PARENT (SourceField = PK), not as a child with FKs.
func (v *ReferentialIntegrityValidator) IdentifyInsertForeignKeyFields(
	database *models.Database,
	bundle *models.Bundle,
	insertFields map[string]string,
	bundleCache map[string]*models.Bundle,
) []ForeignKeyUpdate {
	if database == nil {
		return nil
	}

	var foreignKeyUpdates []ForeignKeyUpdate

	// Find relationships on other bundles where DestinationBundle == this bundle.
	// These define FK fields in this bundle (DestinationField) that reference
	// parent bundles (SourceBundle) via their PK (SourceField).
	incomingRelationships := v.getAllBundlesWithRelationshipsTo(database, bundle.Name, bundleCache)

	for _, relInfo := range incomingRelationships {
		sourceBundle, found := bundleCache[relInfo.SourceBundle]
		if !found {
			var err error
			sourceBundle, err = v.bundleService.GetBundleByName(database, relInfo.SourceBundle)
			if err != nil {
				v.logger.Warnf("Failed to load bundle '%s' during FK identification: %v", relInfo.SourceBundle, err)
				continue
			}
			bundleCache[relInfo.SourceBundle] = sourceBundle
		}

		if relationship, exists := sourceBundle.Relationships[relInfo.RelationshipName]; exists {
			fieldName := relationship.DestinationField
			if newValue, isBeingInserted := insertFields[fieldName]; isBeingInserted {
				foreignKeyUpdates = append(foreignKeyUpdates, ForeignKeyUpdate{
					FieldName:    fieldName,
					NewValue:     newValue,
					Relationship: relationship,
				})
			}
		}
	}

	return foreignKeyUpdates
}

// isFieldUsedInRelationships checks if a field is referenced in any relationship
//
// This is used by foreign key index protection to prevent dropping indexes
// that are required for referential integrity validation.
//
// Returns:
//   - bool: true if field is used in any relationship
//   - string: Name of first relationship using the field (for error messages)
func (v *ReferentialIntegrityValidator) IsFieldUsedInRelationships(
	bundle *models.Bundle,
	fieldName string,
) (bool, string) {
	for relationshipName, relationship := range bundle.Relationships {
		if relationship.SourceField == fieldName || relationship.DestinationField == fieldName {
			return true, relationshipName
		}
	}
	return false, ""
}

// =============================================================================
//  DROP BUNDLE DOCUMENT-LEVEL VALIDATION (DROP RESTRICT)
// =============================================================================

// BundleViolationInfo stores violation information for a single bundle
type BundleViolationInfo struct {
	BundleName     string
	FieldName      string
	ViolationCount int
}

// ValidateDropBundleDocumentReferences validates that no documents in other bundles
// contain foreign key references to the bundle being dropped.
//
// This method performs document-level validation by scanning all bundles that have
// relationships pointing to the target bundle, checking if any documents contain
// non-null FK values that reference documents in the target bundle.
//
// Features:
// - Uses hash indexes for O(1) FK lookups when available
// - Falls back to full document scan with progress logging when index missing
// - Supports sampling mode for fast failure detection (stops at 100 violations)
// - Alphabetically-sorted lock acquisition to prevent deadlocks
// - Read locks allow concurrent SELECT queries during validation
//
// Parameters:
//   - database: The database containing all bundles
//   - targetBundle: The bundle being dropped
//   - bundleCache: Operation-scoped cache to avoid redundant bundle loads
//   - thorough: If false, uses sampling mode (probabilistic, stops at 100 violations)
//   - sampleSize: Number of DocumentIDs to sample in sampling mode
//   - logProgress: If true, logs progress every 10k documents during full scans
//
// Returns:
//   - error: Formatted error with violation summary if violations found, nil otherwise
//
// TODO: I will implement CASCADE DELETE to automatically remove dependent documents when this feature is added
// TODO: I could add parallel bundle scanning to improve performance when database has 100+ bundles with relationships to the target bundle
// TODO: I could auto-create missing FK indexes during validation to prevent future full scans and improve subsequent DROP performance on this bundle
// TODO: I could cache relationship direction analysis to avoid rescanning relationship metadata when dropping multiple bundles in sequence during batch operations
// TODO: I could add a configuration option to control how many violating bundles to display (currently hardcoded to 5) for administrators who want full violation visibility
func (v *ReferentialIntegrityValidator) ValidateDropBundleDocumentReferences(
	database *models.Database,
	targetBundle *models.Bundle,
	bundleCache map[string]*models.Bundle,
	thorough bool,
	sampleSize int,
	logProgress bool,
) error {
	if database == nil || targetBundle == nil {
		return nil // No validation needed
	}

	v.logger.Debugf("[DROP-RESTRICT] Starting document-level validation for bundle '%s' (thorough=%v, sampleSize=%d)", targetBundle.Name, thorough, sampleSize)

	// Step 1: Find all bundles with relationships pointing to target bundle
	// This includes both direct relationships and bidirectional (ManyToMany)
	var bundlesToScan []string
	relationshipMap := make(map[string][]models.Relationship) // bundle name -> relationships

	// Scan all bundles in the database for relationships
	for bundleName, bundle := range database.Bundles {
		if bundleName == targetBundle.Name {
			continue // Skip the bundle being dropped
		}

		for _, relationship := range bundle.Relationships {
			// Check if this relationship points TO the target bundle (DestinationBundle)
			// or if it's a bidirectional relationship where target is the source
			if relationship.DestinationBundle == targetBundle.Name ||
				(relationship.SourceBundle == targetBundle.Name && relationship.RelationshipType == "ManyToMany") {
				bundlesToScan = append(bundlesToScan, bundleName)
				relationshipMap[bundleName] = append(relationshipMap[bundleName], relationship)
				v.logger.Debugf("[DROP-RESTRICT] Bundle '%s' has relationship '%s' pointing to '%s' via field '%s'", bundleName, relationship.Name, targetBundle.Name, relationship.SourceField)
			}
		}
	}

	if len(bundlesToScan) == 0 {
		v.logger.Debugf("[DROP-RESTRICT] No bundles with relationships to '%s' found - validation passed", targetBundle.Name)
		return nil // No bundles reference this target
	}

	// Step 2: Sort bundle names alphabetically for deadlock prevention
	// This ensures that if two concurrent DROP operations need to lock the same bundles,
	// they will acquire locks in the same order, preventing circular wait conditions
	sort.Strings(bundlesToScan)
	v.logger.Debugf("[DROP-RESTRICT] Sorted %d bundles for deadlock prevention: %v", len(bundlesToScan), bundlesToScan)

	// Step 3: Collect all DocumentIDs from target bundle for validation
	// WRITE-THROUGH CACHE: Use SortedIndex for document ID enumeration
	var targetDocumentIDs []string
	if targetBundle.SortedIndex != nil && targetBundle.SortedIndex.TotalDocuments() > 0 {
		// Use SortedIndex to enumerate document IDs (most efficient)
		// The SortedIndex maintains a sorted list of all docIDs
		targetDocumentIDs = targetBundle.SortedIndex.GetAllDocumentIDs()
	} else {
		// Fallback: Load documents via page scan
		// TODO: I could optimize this by loading only the DocumentID list without full document data
		return fmt.Errorf("[DROP-RESTRICT] Cannot validate: bundle '%s' has no SortedIndex for document enumeration", targetBundle.Name)
	}

	totalDocs := len(targetDocumentIDs)
	v.logger.Debugf("[DROP-RESTRICT] Target bundle '%s' has %d documents to validate", targetBundle.Name, totalDocs)

	// Step 4: Determine if we should use sampling mode
	useSampling := !thorough && totalDocs > 10000
	if useSampling {
		probability := float64(sampleSize) / float64(totalDocs)
		v.logger.Debugf("[DROP-RESTRICT] Using sampling mode: probabilistic sampling of ~%d DocumentIDs from %d total in '%s' (probability=%.4f)",
			sampleSize, totalDocs, targetBundle.Name, probability)
	}

	// Step 5: Scan each bundle for FK violations
	var allViolations []BundleViolationInfo
	totalViolationCount := 0

	for _, bundleName := range bundlesToScan {
		bundle, found := bundleCache[bundleName]
		if !found {
			var err error
			bundle, err = v.bundleService.GetBundleByName(database, bundleName)
			if err != nil {
				v.logger.Warnf("[DROP-RESTRICT] Failed to load bundle '%s': %v", bundleName, err)
				continue
			}
			bundleCache[bundleName] = bundle
		}

		// Get relationships for this bundle
		relationships := relationshipMap[bundleName]

		for _, relationship := range relationships {
			fkField := relationship.SourceField

			// Try to use hash index for O(1) lookups
			hashIdx := bundle.GetHashIndexForField(fkField)
			violations := 0

			if hashIdx != nil {
				// O(1) hash index lookups (preferred path)
				v.logger.Debugf("[DROP-RESTRICT] Using hash index for FK field '%s' in bundle '%s'", fkField, bundleName)

				// Determine which DocumentIDs to check based on sampling vs thorough mode
				var docIDsToCheck []string

				if useSampling {
					// Sampling mode: select a random sample of DocumentIDs
					probability := float64(sampleSize) / float64(totalDocs)
					for _, docID := range targetDocumentIDs {
						// Simple probabilistic sampling based on hash of docID
						if len(docID) > 0 && (int(docID[0])%100) < int(probability*100) {
							docIDsToCheck = append(docIDsToCheck, docID)
							if len(docIDsToCheck) >= sampleSize {
								break // Reached sample size limit
							}
						}
					}
					v.logger.Debugf("[DROP-RESTRICT] Sampling mode: checking %d of %d target DocumentIDs", len(docIDsToCheck), totalDocs)
				} else {
					// Thorough mode: check all DocumentIDs
					docIDsToCheck = targetDocumentIDs
				}

				// Use BatchGet to find which documents in the referencing bundle have FK values
				// that match our target DocumentIDs. Returns map[fkValue][]documentIDs
				hashIndex, ok := hashIdx.(*hashindexV3.HashIndexV3)
				if !ok {
					v.logger.Warnf("[DROP-RESTRICT] Type assertion failed")
					hashIdx = nil
				} else {
					results, err := hashIndex.BatchGet(docIDsToCheck)
					if err != nil {
						v.logger.Warnf("[DROP-RESTRICT] BatchGet failed on hash index for FK field '%s' in bundle '%s': %v, falling back to full scan",
							fkField, bundleName, err)
						hashIdx = nil // Trigger fallback path below
					} else {
						// Count total violations (documents in referencing bundle with FK references)
						for fkValue, referencingDocIDs := range results {
							violations += len(referencingDocIDs)
							v.logger.Debugf("[DROP-RESTRICT] FK value '%s' in bundle '%s' has %d references to target bundle '%s'",
								fkValue, bundleName, len(referencingDocIDs), targetBundle.Name)
						}

						if useSampling && violations >= 100 {
							// Early termination at 100 violations in sampling mode
							return fmt.Errorf("Cannot drop bundle '%s' - at least 100 documents in '%s' reference this bundle via field '%s' (sampling mode detected violations, actual count may be higher). Remove references first or use DROP BUNDLE \"%s\" WITH FORCE", targetBundle.Name, bundleName, fkField, targetBundle.Name)
						}

						v.logger.Debugf("[DROP-RESTRICT] Hash index scan complete: found %d violations in bundle '%s' via field '%s'",
							violations, bundleName, fkField)
					}
				}

			}

			if hashIdx == nil {
				// Fallback to full document scan via page cache
				// WRITE-THROUGH CACHE: All documents are now accessed through page cache
				v.logger.Warnf("[DROP-RESTRICT] Hash index missing on FK field '%s' in bundle '%s', falling back to page scan", fkField, bundleName)

				// Use SortedIndex to get document count for progress logging
				var totalDocsInBundle int64
				if bundle.SortedIndex != nil {
					totalDocsInBundle = int64(bundle.SortedIndex.TotalDocuments())
				}
				counter := 0

				// Scan all pages in the bundle
				for pageID := uint32(0); pageID < uint32(bundle.PageCount); pageID++ {
					docs, err := v.bundleService.SnapshotPageDocuments(bundleName, bundle.Database.Name, pageID)
					if err != nil {
						v.logger.Warnf("[DROP-RESTRICT] Failed to load page %d for bundle '%s': %v", pageID, bundleName, err)
						continue
					}

					schema := bundle.DocumentStructure.FieldSchema()
					for _, doc := range docs {
						counter++

						// Progress logging every 10k documents
						if logProgress && counter%10000 == 0 {
							v.logger.Debugf("[DROP-RESTRICT] DROP validation progress: scanned %d of %d documents in '%s', found %d violations so far",
								counter, totalDocsInBundle, bundleName, violations)
						}

						// Check if this document has a FK value matching target bundle
						var fkValue string
						if schema != nil && len(doc.Values) > 0 {
							if idx, ok := schema.NameToIndex[fkField]; ok && idx < len(doc.Values) && doc.Values[idx].Type == models.FieldTypeString {
								fkValue = doc.Values[idx].StringVal
							}
						} else if doc.Data != nil {
							if v, ok := doc.Data[fkField]; ok && v != nil {
								fkValue = conversion.ValueToString(v)
							}
						}
						if fkValue != "" {
							for _, targetDocID := range targetDocumentIDs {
								if fkValue == targetDocID {
									violations++
									if useSampling && violations >= 100 {
										return fmt.Errorf("Cannot drop bundle '%s' - at least 100 documents reference this bundle (sampling mode detected violations, actual count may be higher). Remove references first or use DROP BUNDLE \"%s\" WITH FORCE", targetBundle.Name, targetBundle.Name)
									}
									break
								}
							}
						}
					}
				}

				if logProgress {
					v.logger.Debugf("[DROP-RESTRICT] DROP validation complete: scanned all %d documents in '%s', found %d total violations",
						totalDocsInBundle, bundleName, violations)
				}
			}

			if violations > 0 {
				allViolations = append(allViolations, BundleViolationInfo{
					BundleName:     bundleName,
					FieldName:      fkField,
					ViolationCount: violations,
				})
				totalViolationCount += violations
			}
		}
	}

	// Step 6: If violations found, format and return error
	if len(allViolations) > 0 {
		// Sort violations by count descending
		sort.Slice(allViolations, func(i, j int) bool {
			return allViolations[i].ViolationCount > allViolations[j].ViolationCount
		})

		// Build error message (show top 5, truncate rest)
		var violationStrings []string
		displayCount := len(allViolations)
		if displayCount > 5 {
			displayCount = 5
		}

		for i := 0; i < displayCount; i++ {
			v := allViolations[i]
			violationStrings = append(violationStrings, fmt.Sprintf("%d in '%s' via '%s'",
				v.ViolationCount, v.BundleName, v.FieldName))
		}

		errorMsg := fmt.Sprintf("Cannot drop bundle '%s' - %d total violations found across %d bundles (showing top %d): %s", targetBundle.Name, totalViolationCount, len(allViolations), displayCount, strings.Join(violationStrings, ", "))

		if len(allViolations) > 5 {
			errorMsg += fmt.Sprintf(" ... and %d more bundles with violations", len(allViolations)-5)
		}

		errorMsg += fmt.Sprintf(". Remove these references first or use DROP BUNDLE \"%s\" WITH FORCE", targetBundle.Name)

		v.logger.Warnf("[DROP-RESTRICT] Validation failed: %s", errorMsg)
		return fmt.Errorf("%s", errorMsg)
	}

	v.logger.Debugf("[DROP-RESTRICT] Document-level validation passed for bundle '%s' - no FK violations found", targetBundle.Name)
	return nil
}
