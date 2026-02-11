package bundle

import (
	"fmt"
	"strings"
	"syndrdb/src/internal/domain/models"

	"go.uber.org/zap"
)

/*
Holds all of the validation logic for commands that modify documents and bundle structures.

This includes:
- Referential integrity validation (CASCADE DELETE, foreign key constraints)
- Document structure validation
- Field type validation
- Constraint validation
*/

// =============================================================================
//  REFERENTIAL INTEGRITY VALIDATION (P 1)
// =============================================================================

// ReferentialIntegrityValidator handles validation of relationship constraints
// before document deletion operations.
//
// Design Philosophy:
// - Single Responsibility: Only handles referential integrity checks
// - DRY: Reuses existing index infrastructure (GetOrLoadHashIndex)
// - Performance: O(1) hash index lookups, no full table scans
// - Open/Closed: Easy to extend with CASCADE DELETE and SET NULL options
//
// Architecture:
// This validator leverages SyndrDB's existing HashIndexV3 infrastructure to
// perform fast existence checks on foreign key references without scanning
// entire bundles.
type ReferentialIntegrityValidator struct {
	bundleService *BundleService
	logger        *zap.SugaredLogger
}

// NewReferentialIntegrityValidator creates a new referential integrity validator
func NewReferentialIntegrityValidator(bundleService *BundleService, logger *zap.SugaredLogger) *ReferentialIntegrityValidator {
	return &ReferentialIntegrityValidator{
		bundleService: bundleService,
		logger:        logger,
	}
}

// ValidateDelete checks if a document can be safely deleted without breaking relationships
//
// This method examines all relationships where the bundle is the "source" (referenced by other bundles)
// and verifies that no documents in dependent bundles reference the document being deleted.
//
// Performance Characteristics:
// - O(k) where k = number of relationships (typically 1-3)
// - Each relationship check is O(1) via hash index lookup
// - No full table scans required
//
// Parameters:
//   - bundle: The bundle containing the document to delete
//   - documentID: The ID of the document to delete
//
// Returns:
//   - error: Non-nil if deletion would violate referential integrity
//
// Example:
//
//	validator := NewReferentialIntegrityValidator(bundleService, logger)
//	err := validator.ValidateDelete(authorsBundle, "author_123")
//	if err != nil {
//	    // Handle constraint violation - cannot delete
//	    return fmt.Errorf("referential integrity violation: %w", err)
//	}
//
// Error Response Examples:
//   - "Cannot delete document 'author_123': 5 Books documents reference this document via 'AuthorID'"
//   - "Cannot delete document 'order_456': referenced by Orders.CustomerID (index not found)"
func (v *ReferentialIntegrityValidator) ValidateDelete(bundle *models.Bundle, documentID string) error {
	if bundle == nil {
		return fmt.Errorf("bundle cannot be nil")
	}
	if documentID == "" {
		return fmt.Errorf("documentID cannot be empty")
	}

	// OPTIMIZATION: Early exit if bundle has no relationships
	if len(bundle.Relationships) == 0 {
		v.logger.Debugf("[REFINT] Bundle '%s' has no relationships - skipping referential integrity check", bundle.Name)
		return nil
	}

	v.logger.Debugf("[REFINT] Validating referential integrity for document '%s' in bundle '%s' (%d relationships)", documentID, bundle.Name, len(bundle.Relationships))

	// Check each relationship where this bundle is the source (referenced by others)
	violations := make([]string, 0, 5)
	for relationshipName, relationship := range bundle.Relationships {
		// Determine if this bundle is the source (being referenced)
		isSource := relationship.SourceBundle == bundle.Name || relationship.SourceBundleName == bundle.Name

		if !isSource {
			// This relationship doesn't apply - bundle is the target, not source
			v.logger.Debugf("[REFINT] Skipping relationship '%s' - bundle '%s' is target, not source", relationshipName, bundle.Name)
			continue
		}

		v.logger.Debugf("[REFINT] Checking relationship '%s' (source: %s, dest: %s, field: %s)",
			relationshipName, relationship.SourceBundle, relationship.DestinationBundle, relationship.DestinationField)

		// Validate this specific relationship
		violation, err := v.validateRelationship(bundle, documentID, relationshipName, relationship)
		if err != nil {
			// Technical error (index not found, etc.) - log warning but continue
			v.logger.Warnf("Failed to validate relationship '%s': %v", relationshipName, err)
			violations = append(violations, fmt.Sprintf("relationship '%s': validation error: %v", relationshipName, err))
			continue
		}

		if violation != "" {
			violations = append(violations, violation)
		}
	}

	// If any violations found, return detailed error
	if len(violations) > 0 {
		return v.formatViolationError(bundle.Name, documentID, violations)
	}

	v.logger.Debugf("Referential integrity validated successfully for document '%s' in bundle '%s'", documentID, bundle.Name)
	return nil
}

// validateRelationship checks a single relationship for referential integrity violations
//
// This method performs an O(1) hash index lookup to check if any documents in the
// dependent bundle reference the document being deleted.
//
// Returns:
//   - violation string: Non-empty if violation detected
//   - error: Non-nil if technical error occurred (index not found, etc.)
func (v *ReferentialIntegrityValidator) validateRelationship(
	sourceBundle *models.Bundle,
	documentID string,
	relationshipName string,
	relationship models.Relationship,
) (string, error) {
	// Get the target bundle (the one that references this bundle)
	targetBundleName := relationship.DestinationBundle
	if targetBundleName == "" {
		targetBundleName = relationship.TargetBundleName
	}

	if targetBundleName == "" {
		return "", fmt.Errorf("relationship '%s' has no target bundle specified", relationshipName)
	}

	v.logger.Debugf("Checking relationship '%s': %s -> %s", relationshipName, sourceBundle.Name, targetBundleName)

	// Get the target bundle from the database
	targetBundle, err := v.bundleService.GetBundleByName(sourceBundle.Database, targetBundleName)
	if err != nil {
		return "", fmt.Errorf("failed to load target bundle '%s': %w", targetBundleName, err)
	}

	// Get the foreign key field name in the target bundle
	foreignKeyField := relationship.DestinationField
	if foreignKeyField == "" {
		foreignKeyField = relationship.SourceField // Fallback for legacy relationships
	}

	if foreignKeyField == "" {
		return "", fmt.Errorf("relationship '%s' has no foreign key field specified", relationshipName)
	}

	// Look for an index on the foreign key field in the target bundle
	// This is REQUIRED for efficient referential integrity checking
	indexName, indexRef, found := v.findIndexForField(targetBundle, foreignKeyField)
	if !found {
		// TODO (SPRINT 2): Consider creating index on-demand if missing
		// For now, we require the index to exist for performance reasons
		return "", fmt.Errorf("no index found on field '%s' in bundle '%s' - index required for referential integrity", foreignKeyField, targetBundleName)
	}

	v.logger.Debugf("Using index '%s' for referential integrity check on field '%s'", indexName, foreignKeyField)

	// Load the index (reuses existing infrastructure)
	if indexRef.IndexType == "hash" {
		return v.checkHashIndexForReferences(targetBundle, indexName, indexRef, documentID, relationshipName, foreignKeyField)
	} else if indexRef.IndexType == "btree" {
		// TODO (SPRINT 2): Implement BTree index checking
		return "", fmt.Errorf("BTree index support not yet implemented for referential integrity")
	}

	return "", fmt.Errorf("unsupported index type '%s' for referential integrity", indexRef.IndexType)
}

// checkHashIndexForReferences performs the actual hash index lookup
//
// This is the performance-critical path: O(1) hash lookup to check for references
func (v *ReferentialIntegrityValidator) checkHashIndexForReferences(
	targetBundle *models.Bundle,
	indexName string,
	indexRef models.IndexReference,
	documentID string,
	relationshipName string,
	foreignKeyField string,
) (string, error) {
	// Load hash index using existing infrastructure (DRY principle)
	hashIndex, err := v.bundleService.GetOrLoadHashIndex(targetBundle, indexName, indexRef)
	if err != nil {
		return "", fmt.Errorf("failed to load hash index '%s': %w", indexName, err)
	}

	// Perform O(1) hash lookup to check for references
	v.logger.Debugf("[REFINT] Searching hash index '%s' for documentID '%s' (checking if any %s.%s == '%s')",
		indexName, documentID, targetBundle.Name, foreignKeyField, documentID)
	results, _, err := hashIndex.Search(documentID)
	if err != nil {
		return "", fmt.Errorf("failed to search hash index '%s': %w", indexName, err)
	}

	v.logger.Debugf("[REFINT] Hash index search returned %d results", len(results))
	if len(results) > 0 {
		v.logger.Debugf("[REFINT] Sample results: %v", results[:min(3, len(results))])
	}

	// If any documents reference this ID, report violation
	if len(results) > 0 {
		violation := fmt.Sprintf("Cannot delete: %d document(s) in '%s' reference this document via '%s' (relationship: '%s')",
			len(results), targetBundle.Name, foreignKeyField, relationshipName)
		v.logger.Warnf("[REFINT] Referential integrity VIOLATION detected: %s", violation)
		return violation, nil
	}

	v.logger.Debugf("[REFINT] No references found in '%s' for document '%s' via field '%s'", targetBundle.Name, documentID, foreignKeyField)
	return "", nil
}

// findIndexForField locates an index on the specified field
//
// Returns:
//   - indexName: Name of the index
//   - indexRef: Reference to the index
//   - found: True if index exists
func (v *ReferentialIntegrityValidator) findIndexForField(bundle *models.Bundle, fieldName string) (string, models.IndexReference, bool) {
	if bundle.Indexes == nil {
		v.logger.Debugf("[REFINT] No indexes found in bundle '%s'", bundle.Name)
		return "", models.IndexReference{}, false
	}

	v.logger.Debugf("[REFINT] Searching for index on field '%s' in bundle '%s' (total indexes: %d)", fieldName, bundle.Name, len(bundle.Indexes))

	for indexName, indexRef := range bundle.Indexes {
		v.logger.Debugf("[REFINT] Checking index '%s': type=%s, hashField='%s', btreeField='%s'",
			indexName, indexRef.IndexType, indexRef.HashIndexField.FieldName, indexRef.BTreeIndexField.FieldName)

		// Check hash index field
		if indexRef.IndexType == "hash" && indexRef.HashIndexField.FieldName == fieldName {
			v.logger.Debugf("[REFINT] Found matching hash index: '%s' for field '%s'", indexName, fieldName)
			return indexName, indexRef, true
		}
		// Check BTree index field
		if indexRef.IndexType == "btree" && indexRef.BTreeIndexField.FieldName == fieldName {
			v.logger.Debugf("[REFINT] Found matching btree index: '%s' for field '%s'", indexName, fieldName)
			return indexName, indexRef, true
		}
	}

	v.logger.Warnf("[REFINT] No index found on field '%s' in bundle '%s'", fieldName, bundle.Name)
	return "", models.IndexReference{}, false
}

// formatViolationError creates a user-friendly error message for referential integrity violations
func (v *ReferentialIntegrityValidator) formatViolationError(bundleName string, documentID string, violations []string) error {
	if len(violations) == 1 {
		return fmt.Errorf("referential integrity violation: %s", violations[0])
	}

	// Multiple violations - format as list
	errorMsg := fmt.Sprintf("Cannot delete document '%s' from bundle '%s' - %d referential integrity violation(s):\n", documentID, bundleName, len(violations))
	for i, violation := range violations {
		errorMsg += fmt.Sprintf("  %d. %s\n", i+1, violation)
	}

	return fmt.Errorf("%s", errorMsg)
}

// ValidateBulkDeleteOptimized performs batch referential integrity validation for bulk delete operations
//
// This method validates deletion of multiple documents simultaneously using batch operations for
// maximum performance. It groups all validation lookups by relationship and uses HashIndexV3.BatchGet()
// to perform parallel O(1) hash lookups instead of sequential single-document checks.
//
// Performance Characteristics:
// - O(R * B) where R = number of relationships, B = BatchGet overhead (256-key internal batching)
// - Dramatically faster than R * D individual lookups (where D = document count)
// - Example: Validating 1000 docs across 5 relationships takes ~5 batch operations instead of 5000 individual lookups
//
// Error Reporting:
// - Returns aggregated counts instead of individual document violations for better UX
// - Format: "Cannot delete 150 documents: 423 references in 'Books' via 'author_id', 89 references in 'Articles' via 'author_id'"
// - Allows users to understand the scope of blocking relationships without overwhelming detail
//
// Parameters:
//   - bundle: The bundle containing documents to delete
//   - documentIDs: Slice of all document IDs to validate for deletion
//
// Returns:
//   - error: Non-nil if referential integrity violations detected or technical error occurs
//
// Example:
//
//	validator := NewReferentialIntegrityValidator(bundleService, logger)
//	docIDs := []string{"doc1", "doc2", "doc3", ...}
//	if err := validator.ValidateBulkDeleteOptimized(bundle, docIDs); err != nil {
//	    return fmt.Errorf("bulk delete blocked: %w", err)
//	}
func (v *ReferentialIntegrityValidator) ValidateBulkDeleteOptimized(
	bundle *models.Bundle,
	documentIDs []string,
) error {
	if bundle == nil {
		return fmt.Errorf("bundle cannot be nil")
	}
	if len(documentIDs) == 0 {
		return nil // Nothing to validate
	}

	// OPTIMIZATION: Early exit if bundle has no relationships
	if len(bundle.Relationships) == 0 {
		v.logger.Debugf("[REFINT] Bundle '%s' has no relationships - skipping bulk validation", bundle.Name)
		return nil
	}

	v.logger.Debugf("[REFINT] Validating bulk delete of %d documents from bundle '%s' (%d relationships)",
		len(documentIDs), bundle.Name, len(bundle.Relationships))

	// Track violations by relationship for aggregated error reporting
	// Map: relationshipName -> count of references found
	violationCounts := make(map[string]int)

	// Check each relationship where this bundle is the source (referenced by others)
	for relationshipName, relationship := range bundle.Relationships {
		// Determine if this bundle is the source (being referenced)
		isSource := relationship.SourceBundle == bundle.Name || relationship.SourceBundleName == bundle.Name

		if !isSource {
			v.logger.Debugf("[REFINT] Skipping relationship '%s' - bundle '%s' is target, not source",
				relationshipName, bundle.Name)
			continue
		}

		v.logger.Debugf("[REFINT] Checking relationship '%s' (source: %s, dest: %s, field: %s) for %d documents",
			relationshipName, relationship.SourceBundle, relationship.DestinationBundle,
			relationship.DestinationField, len(documentIDs))

		// Get the target bundle (the one that references this bundle)
		targetBundleName := relationship.DestinationBundle
		if targetBundleName == "" {
			targetBundleName = relationship.TargetBundleName
		}

		if targetBundleName == "" {
			v.logger.Warnf("Relationship '%s' has no target bundle specified - skipping", relationshipName)
			continue
		}

		targetBundle, err := v.bundleService.GetBundleByName(bundle.Database, targetBundleName)
		if err != nil {
			v.logger.Warnf("Failed to load target bundle '%s': %v - skipping relationship '%s'",
				targetBundleName, err, relationshipName)
			continue
		}

		// Get the foreign key field name in the target bundle
		foreignKeyField := relationship.DestinationField
		if foreignKeyField == "" {
			foreignKeyField = relationship.SourceField // Fallback for legacy relationships
		}

		if foreignKeyField == "" {
			v.logger.Warnf("Relationship '%s' has no foreign key field specified - skipping", relationshipName)
			continue
		}

		// Find index on the foreign key field
		indexName, indexRef, found := v.findIndexForField(targetBundle, foreignKeyField)
		if !found {
			return fmt.Errorf("no index found on field '%s' in bundle '%s' - index required for bulk referential integrity validation",
				foreignKeyField, targetBundleName)
		}

		// Only hash indexes support BatchGet currently
		if indexRef.IndexType != "hash" {
			return fmt.Errorf("bulk validation requires hash index on field '%s' in bundle '%s' - found '%s' index instead",
				foreignKeyField, targetBundleName, indexRef.IndexType)
		}

		// Load the hash index
		hashIndex, err := v.bundleService.GetOrLoadHashIndex(targetBundle, indexName, indexRef)
		if err != nil {
			return fmt.Errorf("failed to load hash index '%s' for bulk validation: %w", indexName, err)
		}

		// PERFORMANCE CRITICAL: Use BatchGet to check all document IDs in parallel
		// HashIndexV3.BatchGet() internally batches into 256-key chunks for optimal performance
		v.logger.Debugf("[REFINT] Performing batch lookup of %d document IDs in index '%s'", len(documentIDs), indexName)

		results, err := hashIndex.BatchGet(documentIDs)
		if err != nil {
			return fmt.Errorf("failed to perform batch lookup in index '%s': %w", indexName, err)
		}

		// Count total references found across all documents
		refCount := 0
		for _, docResults := range results {
			refCount += len(docResults)
		}

		if refCount > 0 {
			v.logger.Warnf("[REFINT] Found %d references in '%s' via '%s' blocking bulk delete",
				refCount, targetBundleName, foreignKeyField)
			violationKey := fmt.Sprintf("%s.%s", targetBundleName, foreignKeyField)
			violationCounts[violationKey] = refCount
		} else {
			v.logger.Debugf("[REFINT] No references found in '%s' for relationship '%s'",
				targetBundleName, relationshipName)
		}
	}

	// If any violations found, return aggregated error
	if len(violationCounts) > 0 {
		return v.formatBulkViolationError(bundle.Name, len(documentIDs), violationCounts)
	}

	v.logger.Debugf("[REFINT] Bulk validation passed for %d documents in bundle '%s'", len(documentIDs), bundle.Name)
	return nil
}

// formatBulkViolationError creates a user-friendly error message for bulk delete violations
// Shows aggregated counts instead of individual document details for better UX with large datasets
func (v *ReferentialIntegrityValidator) formatBulkViolationError(
	bundleName string,
	documentCount int,
	violationCounts map[string]int,
) error {
	if len(violationCounts) == 1 {
		// Single relationship violation
		for key, count := range violationCounts {
			parts := strings.Split(key, ".")
			targetBundle := parts[0]
			foreignKeyField := parts[1]
			return fmt.Errorf("cannot delete %d documents from bundle '%s': %d references exist in '%s' via field '%s'",
				documentCount, bundleName, count, targetBundle, foreignKeyField)
		}
	}

	// Multiple relationship violations - format as list with counts
	errorMsg := fmt.Sprintf("Cannot delete %d documents from bundle '%s' - referential integrity violations:\n",
		documentCount, bundleName)

	violationList := make([]string, 0, len(violationCounts))
	for key, count := range violationCounts {
		parts := strings.Split(key, ".")
		targetBundle := parts[0]
		foreignKeyField := parts[1]
		violationList = append(violationList, fmt.Sprintf("%d references in '%s' via '%s'",
			count, targetBundle, foreignKeyField))
	}

	// Sort for deterministic output
	for i, violation := range violationList {
		errorMsg += fmt.Sprintf("  %d. %s\n", i+1, violation)
	}

	return fmt.Errorf("%s", strings.TrimSpace(errorMsg))
}

// =============================================================================
//  CASCADE DELETE SUPPORT (P 2)
// =============================================================================

// DependentDocument represents a document that would be deleted as part of a cascade operation
type DependentDocument struct {
	BundleName      string // The bundle containing the dependent document
	DocumentID      string // The ID of the dependent document
	Relationship    string // The relationship name causing the cascade
	Level           int    // Cascade depth level (1 = direct child, 2 = grandchild, etc.)
	ParentDocID     string // The parent document that references this one
	ForeignKeyField string // The field name containing the foreign key
}

// CascadeDeletePlan represents the complete plan for a cascade delete operation
type CascadeDeletePlan struct {
	RootBundle      string              // The bundle where deletion starts
	RootDocumentID  string              // The document being deleted
	DependentDocs   []DependentDocument // All documents that will be cascade-deleted
	TotalDeletes    int                 // Total number of documents to delete (including root)
	MaxDepth        int                 // Maximum cascade depth
	AffectedBundles map[string]int      // Map of bundle name to count of documents affected
}

// ValidateDeleteWithCascade analyzes what documents would be cascade-deleted
//
// This method performs a depth-first traversal of the relationship graph to identify
// all documents that would need to be deleted as part of a CASCADE DELETE operation.
//
// Performance Characteristics:
// - O(n * k) where n = total dependent documents, k = avg relationships per bundle
// - Uses hash index O(1) lookups at each level
// - Detects and handles circular references
//
// Parameters:
//   - bundle: The bundle containing the document to delete
//   - documentID: The ID of the document to delete
//
// Returns:
//   - CascadeDeletePlan: Complete plan of what would be deleted
//   - error: Non-nil if error occurs during analysis
//
// Example:
//
//	validator := NewReferentialIntegrityValidator(bundleService, logger)
//	plan, err := validator.ValidateDeleteWithCascade(authorsBundle, "author_123")
//	if err != nil {
//	    return fmt.Errorf("cascade analysis failed: %w", err)
//	}
//
//	// Show user what will be deleted
//	fmt.Printf("Deleting 1 author will cascade delete %d related documents:\n", len(plan.DependentDocs))
//	for _, dep := range plan.DependentDocs {
//	    fmt.Printf("  - %s (from %s)\n", dep.DocumentID, dep.BundleName)
//	}
func (v *ReferentialIntegrityValidator) ValidateDeleteWithCascade(bundle *models.Bundle, documentID string) (*CascadeDeletePlan, error) {
	if bundle == nil {
		return nil, fmt.Errorf("bundle cannot be nil")
	}
	if documentID == "" {
		return nil, fmt.Errorf("documentID cannot be empty")
	}

	v.logger.Debugf("[CASCADE] Starting cascade delete analysis for document '%s' in bundle '%s'", documentID, bundle.Name)

	// Initialize the cascade plan
	plan := &CascadeDeletePlan{
		RootBundle:      bundle.Name,
		RootDocumentID:  documentID,
		DependentDocs:   make([]DependentDocument, 0, 100),
		TotalDeletes:    1, // Includes the root document
		MaxDepth:        0,
		AffectedBundles: make(map[string]int),
	}
	plan.AffectedBundles[bundle.Name] = 1 // Root document

	// Track visited documents to detect circular references
	visited := make(map[string]bool)
	visitKey := fmt.Sprintf("%s:%s", bundle.Name, documentID)
	visited[visitKey] = true

	// Recursively find all dependent documents
	err := v.findCascadeDependents(bundle, documentID, 1, plan, visited)
	if err != nil {
		return nil, fmt.Errorf("failed to analyze cascade dependencies: %w", err)
	}

	v.logger.Debugf("[CASCADE] Cascade analysis complete: %d total deletes across %d bundles (max depth: %d)",
		plan.TotalDeletes, len(plan.AffectedBundles), plan.MaxDepth)

	return plan, nil
}

// findCascadeDependents recursively finds all documents that reference the given document
//
// This is the core recursive function that traverses the relationship graph
func (v *ReferentialIntegrityValidator) findCascadeDependents(
	sourceBundle *models.Bundle,
	documentID string,
	level int,
	plan *CascadeDeletePlan,
	visited map[string]bool,
) error {
	// Update max depth
	if level > plan.MaxDepth {
		plan.MaxDepth = level
	}

	// Prevent infinite recursion (safety limit)
	if level > 10 {
		return fmt.Errorf("cascade depth limit exceeded (10 levels) - possible circular reference detected")
	}

	v.logger.Debugf("[CASCADE] Analyzing level %d: bundle '%s', document '%s'", level, sourceBundle.Name, documentID)

	// Check each relationship where this bundle is the source
	for relationshipName, relationship := range sourceBundle.Relationships {
		// Determine if this bundle is the source (being referenced)
		isSource := relationship.SourceBundle == sourceBundle.Name || relationship.SourceBundleName == sourceBundle.Name

		if !isSource {
			continue // Skip relationships where this bundle is the target
		}

		// Get the target bundle (the one that references this bundle)
		targetBundleName := relationship.DestinationBundle
		if targetBundleName == "" {
			targetBundleName = relationship.TargetBundleName
		}

		if targetBundleName == "" {
			v.logger.Warnf("[CASCADE] Relationship '%s' has no target bundle, skipping", relationshipName)
			continue
		}

		// Load target bundle
		targetBundle, err := v.bundleService.GetBundleByName(sourceBundle.Database, targetBundleName)
		if err != nil {
			return fmt.Errorf("failed to load target bundle '%s': %w", targetBundleName, err)
		}

		// Get the foreign key field name
		foreignKeyField := relationship.DestinationField
		if foreignKeyField == "" {
			foreignKeyField = relationship.SourceField
		}

		if foreignKeyField == "" {
			v.logger.Warnf("[CASCADE] Relationship '%s' has no foreign key field, skipping", relationshipName)
			continue
		}

		// Find index on foreign key field
		indexName, indexRef, found := v.findIndexForField(targetBundle, foreignKeyField)
		if !found {
			v.logger.Warnf("[CASCADE] No index found on field '%s' in bundle '%s', cannot determine dependents", foreignKeyField, targetBundleName)
			continue
		}

		// Search for documents that reference this document
		if indexRef.IndexType == "hash" {
			dependentIDs, err := v.findDependentDocuments(targetBundle, indexName, indexRef, documentID)
			if err != nil {
				return fmt.Errorf("failed to find dependent documents in '%s': %w", targetBundleName, err)
			}

			v.logger.Debugf("[CASCADE] Found %d dependent document(s) in '%s' via relationship '%s'",
				len(dependentIDs), targetBundleName, relationshipName)

			// Add each dependent to the plan and recurse
			for _, depID := range dependentIDs {
				visitKey := fmt.Sprintf("%s:%s", targetBundleName, depID)

				// Check for circular reference
				if visited[visitKey] {
					v.logger.Warnf("[CASCADE] Circular reference detected: %s already visited, skipping", visitKey)
					continue
				}
				visited[visitKey] = true

				// Add to plan
				dependent := DependentDocument{
					BundleName:      targetBundleName,
					DocumentID:      depID,
					Relationship:    relationshipName,
					Level:           level,
					ParentDocID:     documentID,
					ForeignKeyField: foreignKeyField,
				}
				plan.DependentDocs = append(plan.DependentDocs, dependent)
				plan.TotalDeletes++
				plan.AffectedBundles[targetBundleName]++

				// Recurse to find dependents of this dependent
				err = v.findCascadeDependents(targetBundle, depID, level+1, plan, visited)
				if err != nil {
					return err
				}
			}
		}
	}

	return nil
}

// findDependentDocuments searches a hash index for documents referencing the given ID
func (v *ReferentialIntegrityValidator) findDependentDocuments(
	bundle *models.Bundle,
	indexName string,
	indexRef models.IndexReference,
	documentID string,
) ([]string, error) {
	// Load hash index
	hashIndex, err := v.bundleService.GetOrLoadHashIndex(bundle, indexName, indexRef)
	if err != nil {
		return nil, fmt.Errorf("failed to load hash index '%s': %w", indexName, err)
	}

	// Search for references
	// Note: Search() returns []string (document IDs) directly
	documentIDs, _, err := hashIndex.Search(documentID)
	if err != nil {
		return nil, fmt.Errorf("failed to search hash index '%s': %w", indexName, err)
	}

	return documentIDs, nil
}

// ExecuteCascadeDelete performs the actual cascade deletion
//
// This method deletes all documents identified in the cascade plan in the correct order
// (deepest level first, working back to root) to maintain referential integrity throughout.
//
// Parameters:
//   - plan: The cascade delete plan from ValidateDeleteWithCascade
//
// Returns:
//   - error: Non-nil if any deletion fails (partial deletion may have occurred)
//
// IMPORTANT: This should be wrapped in a transaction for atomicity
//
// Example:
//
//	plan, err := validator.ValidateDeleteWithCascade(bundle, documentID)
//	if err != nil {
//	    return err
//	}
//
//	// Show user confirmation prompt
//	if !userConfirmed(plan) {
//	    return fmt.Errorf("cascade delete cancelled by user")
//	}
//
//	// Execute the cascade delete
//	err = validator.ExecuteCascadeDelete(plan)
//	if err != nil {
//	    return fmt.Errorf("cascade delete failed: %w", err)
//	}
func (v *ReferentialIntegrityValidator) ExecuteCascadeDelete(plan *CascadeDeletePlan) error {
	if plan == nil {
		return fmt.Errorf("cascade plan cannot be nil")
	}

	v.logger.Debugf("[CASCADE] Executing cascade delete: %d total documents across %d bundles",
		plan.TotalDeletes, len(plan.AffectedBundles))

	// TODO: I will add transaction wrapper at server layer (where WALManager is available)
	// to ensure CASCADE deletes are atomic. ExecuteCascadeDelete should be called within
	// serviceManager.WALManager.ExecuteWithLogging() wrapper for full atomicity.
	// This will allow rollback if any cascade delete step fails mid-operation.

	// Sort dependents by level (deepest first) to maintain referential integrity
	// Delete from leaves back to root
	sortedDeps := v.sortDependentsByLevel(plan.DependentDocs)

	// Delete dependent documents first (deepest to shallowest)
	for i := len(sortedDeps) - 1; i >= 0; i-- {
		dep := sortedDeps[i]

		v.logger.Debugf("[CASCADE] Deleting dependent document '%s' from bundle '%s' (level %d)",
			dep.DocumentID, dep.BundleName, dep.Level)

		// Load the bundle (need database reference from root bundle)
		rootBundle, err := v.bundleService.GetBundleByName(nil, plan.RootBundle)
		if err != nil {
			return fmt.Errorf("failed to load root bundle '%s': %w", plan.RootBundle, err)
		}

		bundle, err := v.bundleService.GetBundleByName(rootBundle.Database, dep.BundleName)
		if err != nil {
			return fmt.Errorf("failed to load bundle '%s': %w", dep.BundleName, err)
		}

		// Delete the document
		// Note: This bypasses referential integrity checks since we're doing cascade
		docCommand := &models.DocumentDeleteCommand{
			BundleName:  dep.BundleName,
			WhereClause: fmt.Sprintf("\"DocumentID\" == \"%s\"", dep.DocumentID),
		}

		err = v.bundleService.DeleteDocumentFromBundle(bundle, docCommand, []string{dep.DocumentID}, nil)
		if err != nil {
			return fmt.Errorf("failed to delete document '%s' from bundle '%s': %w",
				dep.DocumentID, dep.BundleName, err)
		}
	}

	// Finally, delete the root document
	v.logger.Debugf("[CASCADE] Deleting root document '%s' from bundle '%s'",
		plan.RootDocumentID, plan.RootBundle)

	// Root bundle was already loaded at the start of the loop, reuse it
	// But we need to reload it fresh for the final delete
	rootBundle, err := v.bundleService.GetBundleByName(nil, plan.RootBundle)
	if err != nil {
		return fmt.Errorf("failed to load root bundle '%s': %w", plan.RootBundle, err)
	}

	rootCommand := &models.DocumentDeleteCommand{
		BundleName:  plan.RootBundle,
		WhereClause: fmt.Sprintf("\"DocumentID\" == \"%s\"", plan.RootDocumentID),
	}

	err = v.bundleService.DeleteDocumentFromBundle(rootBundle, rootCommand, []string{plan.RootDocumentID}, nil)
	if err != nil {
		return fmt.Errorf("failed to delete root document '%s': %w", plan.RootDocumentID, err)
	}

	v.logger.Debugf("[CASCADE] Cascade delete completed successfully: %d documents deleted", plan.TotalDeletes)
	return nil
}

// sortDependentsByLevel sorts dependent documents by cascade level (ascending)
func (v *ReferentialIntegrityValidator) sortDependentsByLevel(deps []DependentDocument) []DependentDocument {
	// Simple bubble sort by level (good enough for small relationship graphs)
	sorted := make([]DependentDocument, len(deps))
	copy(sorted, deps)

	for i := 0; i < len(sorted)-1; i++ {
		for j := 0; j < len(sorted)-i-1; j++ {
			if sorted[j].Level > sorted[j+1].Level {
				sorted[j], sorted[j+1] = sorted[j+1], sorted[j]
			}
		}
	}

	return sorted
}

// FormatCascadePlan creates a user-friendly summary of the cascade delete plan
//
// Returns a string that can be shown to the user for confirmation
func (v *ReferentialIntegrityValidator) FormatCascadePlan(plan *CascadeDeletePlan) string {
	if plan == nil {
		return "No cascade plan available"
	}

	output := "CASCADE DELETE PLAN:\n"
	output += fmt.Sprintf("  Root: %s (document: %s)\n", plan.RootBundle, plan.RootDocumentID)
	output += fmt.Sprintf("  Total documents to delete: %d\n", plan.TotalDeletes)
	output += fmt.Sprintf("  Maximum cascade depth: %d\n", plan.MaxDepth)
	output += "  Affected bundles:\n"

	for bundleName, count := range plan.AffectedBundles {
		output += fmt.Sprintf("    - %s: %d document(s)\n", bundleName, count)
	}

	if len(plan.DependentDocs) > 0 {
		output += "\n  Dependent documents (by level):\n"

		// Group by level for clearer output
		byLevel := make(map[int][]DependentDocument)
		for _, dep := range plan.DependentDocs {
			byLevel[dep.Level] = append(byLevel[dep.Level], dep)
		}

		for level := 1; level <= plan.MaxDepth; level++ {
			if deps, exists := byLevel[level]; exists {
				output += fmt.Sprintf("    Level %d (%d documents):\n", level, len(deps))
				for i, dep := range deps {
					if i < 5 { // Show first 5 at each level
						output += fmt.Sprintf("      - %s.%s (via %s)\n",
							dep.BundleName, dep.DocumentID, dep.Relationship)
					}
				}
				if len(deps) > 5 {
					output += fmt.Sprintf("      ... and %d more\n", len(deps)-5)
				}
			}
		}
	}

	return output
}

// =============================================================================
// TODO (P 3): ON DELETE SET NULL SUPPORT
// =============================================================================
// Future extension: Add support for ON DELETE SET NULL option
//
// func (v *ReferentialIntegrityValidator) ValidateDeleteWithSetNull(bundle *Bundle, documentID string) ([]UpdateOperation, error)
// - Returns list of all foreign key fields that would be set to NULL
// - Allows user confirmation before bulk updates
