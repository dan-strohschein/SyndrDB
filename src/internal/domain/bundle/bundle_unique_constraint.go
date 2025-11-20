package bundle

import (
	"fmt"
	"syndrdb/src/internal/domain/models"

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
		violation, err := v.validateFieldUniqueness(bundle, fieldName, fieldValue)
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

	v.logger.Debugf("Using unique index '%s' for field '%s' uniqueness check", indexName, fieldName)

	// Load the unique index (reuses existing infrastructure - DRY principle)
	if indexRef.IndexType == "hash" {
		return v.checkHashIndexForDuplicates(bundle, indexName, indexRef, fieldName, valueStr)
	} else if indexRef.IndexType == "btree" {
		// TODO (FUTURE): Implement BTree index checking for unique constraints
		return "", fmt.Errorf("BTree unique index support not yet implemented")
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
	hashIndex, err := v.bundleService.GetOrLoadHashIndex(bundle, indexName, indexRef)
	if err != nil {
		return "", fmt.Errorf("failed to load unique index '%s': %w", indexName, err)
	}

	// Perform O(1) hash lookup to check if value already exists
	v.logger.Infof("[UNIQUE] Searching unique index '%s' for value '%s'", indexName, valueStr)
	results, err := hashIndex.Search(valueStr)
	if err != nil {
		return "", fmt.Errorf("failed to search unique index '%s': %w", indexName, err)
	}

	// If any documents have this value, it's a uniqueness violation
	if len(results) > 0 {
		existingDocID := results[0] // First document with this value
		violation := fmt.Sprintf("Unique constraint violation: Field '%s' must be unique, but value '%s' already exists in document '%s'",
			fieldName, valueStr, existingDocID)
		v.logger.Warnf("[UNIQUE] %s", violation)
		return violation, nil
	}

	v.logger.Infof("[UNIQUE] No duplicates found for field '%s' value '%s'", fieldName, valueStr)
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

		// Create hash index for this unique field
		v.logger.Infof("[UNIQUE] Creating unique hash index '%s' for field '%s'", indexName, fieldDef.Name)
		err := createHashIndexInternal(v.bundleService, bundle, indexName)
		if err != nil {
			return fmt.Errorf("failed to create unique index '%s' for field '%s': %w", indexName, fieldDef.Name, err)
		}

		v.logger.Infof("[UNIQUE] Successfully created unique index '%s' for field '%s'", indexName, fieldDef.Name)
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
