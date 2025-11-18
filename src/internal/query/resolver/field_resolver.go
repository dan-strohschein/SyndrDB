package resolver

import (
	"fmt"
	"sort"
	"strings"
	"syndrdb/src/internal/domain/models"
)

/*
field_resolver.go

This file implements field resolution logic for qualified and unqualified field names
in multi-bundle queries. It provides validation, ambiguity detection, and helpful error
messages to guide users when field references are unclear or invalid.

Key responsibilities:
- Resolving unqualified field names to specific bundles
- Detecting ambiguous field references across multiple bundles
- Validating qualified field names against bundle schemas
- Providing helpful error messages with suggestions
- Caching field maps for performance

The resolver is designed to be used by the evaluator, query planner, and adapter
to ensure consistent field resolution logic throughout the query execution pipeline.
*/

// FieldResolver provides centralized field resolution for qualified and unqualified field names
type FieldResolver struct {
	bundles map[string]*models.Bundle // Map of bundle name to bundle instance
}

// NewFieldResolver creates a new field resolver with the given bundles
func NewFieldResolver(bundles map[string]*models.Bundle) *FieldResolver {
	return &FieldResolver{
		bundles: bundles,
	}
}

// ResolveField attempts to resolve an unqualified field name to a specific bundle
// Returns the bundle name and nil error if resolution is successful
// Returns error if field is ambiguous or not found in any bundle
func (r *FieldResolver) ResolveField(fieldName string) (string, error) {
	if len(r.bundles) == 0 {
		return "", fmt.Errorf("no bundles available for field resolution")
	}

	// If only one bundle, resolve to that bundle (no ambiguity possible)
	if len(r.bundles) == 1 {
		for bundleName := range r.bundles {
			return bundleName, nil
		}
	}

	// Check which bundles contain this field
	bundlesWithField := []string{}
	for bundleName, bundle := range r.bundles {
		if r.bundleHasField(bundle, fieldName) {
			bundlesWithField = append(bundlesWithField, bundleName)
		}
	}

	// No bundles have this field
	if len(bundlesWithField) == 0 {
		availableFields := r.getAllFieldNames()
		sort.Strings(availableFields)
		return "", fmt.Errorf("field '%s' not found in any bundle. Available fields: %s",
			fieldName, strings.Join(availableFields, ", "))
		// TODO: I should add Levenshtein distance suggestions for typos (e.g., "Did you mean 'AuthorName'?")
	}

	// Multiple bundles have this field - ambiguous
	if len(bundlesWithField) > 1 {
		sort.Strings(bundlesWithField)
		qualifiedOptions := make([]string, len(bundlesWithField))
		for i, bundleName := range bundlesWithField {
			qualifiedOptions[i] = fmt.Sprintf("'%s'.'%s'", bundleName, fieldName)
		}
		return "", fmt.Errorf("field '%s' is ambiguous (found in bundles: %s). Use qualified name: %s",
			fieldName, strings.Join(bundlesWithField, ", "), strings.Join(qualifiedOptions, " or "))
	}

	// Exactly one bundle has this field - resolved
	return bundlesWithField[0], nil
}

// IsAmbiguous checks if an unqualified field name is ambiguous across bundles
// Returns true if the field exists in multiple bundles
func (r *FieldResolver) IsAmbiguous(fieldName string) bool {
	if len(r.bundles) <= 1 {
		return false // Single bundle queries cannot have ambiguous fields
	}

	count := 0
	for _, bundle := range r.bundles {
		if r.bundleHasField(bundle, fieldName) {
			count++
			if count > 1 {
				return true
			}
		}
	}

	return false
}

// ValidateQualifiedField validates that a qualified field name (Bundle.Field) is valid
// Returns error if bundle doesn't exist or field not found in bundle
func (r *FieldResolver) ValidateQualifiedField(bundleName, fieldName string) error {
	bundle, exists := r.bundles[bundleName]
	if !exists {
		availableBundles := make([]string, 0, len(r.bundles))
		for name := range r.bundles {
			availableBundles = append(availableBundles, name)
		}
		sort.Strings(availableBundles)
		return fmt.Errorf("bundle '%s' not found in query. Available bundles: %s",
			bundleName, strings.Join(availableBundles, ", "))
		// TODO: I should track schema versions to provide "Bundle 'X' exists but not in this query context" errors
	}

	if !r.bundleHasField(bundle, fieldName) {
		availableFields := r.getFieldsFromBundle(bundle)
		sort.Strings(availableFields)
		return fmt.Errorf("field '%s' not found in bundle '%s'. Available fields: %s",
			fieldName, bundleName, strings.Join(availableFields, ", "))
		// TODO: I should add Levenshtein distance suggestions here too
	}

	return nil
}

// GetFieldsFromBundles returns all field names from all bundles (for error messages)
func (r *FieldResolver) GetFieldsFromBundles() map[string][]string {
	result := make(map[string][]string)
	for bundleName, bundle := range r.bundles {
		result[bundleName] = r.getFieldsFromBundle(bundle)
	}
	return result
}

// bundleHasField checks if a bundle contains a specific field
func (r *FieldResolver) bundleHasField(bundle *models.Bundle, fieldName string) bool {
	if bundle == nil {
		return false
	}

	// Check bundle schema/definition for field existence
	if bundle.DocumentStructure.FieldDefinitions != nil {
		_, exists := bundle.DocumentStructure.FieldDefinitions[fieldName]
		return exists
	}

	return false
}

// getFieldsFromBundle extracts all field names from a bundle
func (r *FieldResolver) getFieldsFromBundle(bundle *models.Bundle) []string {
	if bundle == nil || bundle.DocumentStructure.FieldDefinitions == nil {
		return []string{}
	}

	fields := make([]string, 0, len(bundle.DocumentStructure.FieldDefinitions))
	for fieldName := range bundle.DocumentStructure.FieldDefinitions {
		fields = append(fields, fieldName)
	}

	return fields
}

// getAllFieldNames returns all unique field names across all bundles
func (r *FieldResolver) getAllFieldNames() []string {
	fieldSet := make(map[string]bool)
	for _, bundle := range r.bundles {
		for _, fieldName := range r.getFieldsFromBundle(bundle) {
			fieldSet[fieldName] = true
		}
	}

	fields := make([]string, 0, len(fieldSet))
	for field := range fieldSet {
		fields = append(fields, field)
	}

	return fields
}

// TODO: I should support federated queries across multiple databases when that feature is implemented
// TODO: I should add field map caching in BundleContext to avoid repeated schema lookups during evaluation
