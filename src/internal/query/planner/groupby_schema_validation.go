/*
GROUP BY SCHEMA VALIDATION

Validates that GROUP BY fields exist in the schema of the FROM (and joined) bundle(s)
before plan creation. This provides fail-fast behavior: invalid fields like "category"
on a bundle that has no such column produce a clear error at validation time instead
of per-document "Skipping document" warnings in the aggregation node.

Called from CreatePlan at the very start, before the plan cache.
*/

package planner

import (
	"fmt"
	"strings"

	"syndrdb/src/internal/domain/models"
	"syndrdb/src/internal/query/queryparser"

	"go.uber.org/zap"
)

// ValidateGroupByFieldsAgainstSchema checks that each GROUP BY field exists in the
// schema of the appropriate bundle(s). Uses DocumentStructure.FieldDefinitions and
// DocumentID as a well-known system field. Schemaless bundles (nil/empty FieldDefinitions)
// are skipped; validation does not fail for them.
//
// getBundle resolves a bundle by name; typically database.Bundles or BundleService.GetBundleByName.
func ValidateGroupByFieldsAgainstSchema(
	query *queryparser.UnifiedSelectQuery,
	database *models.Database,
	getBundle func(*models.Database, string) (*models.Bundle, error),
	logger *zap.SugaredLogger,
) error {
	if query.GroupBy == nil || len(query.GroupBy.Fields) == 0 {
		return nil
	}

	// 1. Build in-scope bundles: FromBundle + each JoinClauses[].RightBundle
	inScopeNames := make([]string, 0, 1+len(query.JoinClauses))
	inScopeNames = append(inScopeNames, query.FromBundle)
	for _, jc := range query.JoinClauses {
		inScopeNames = append(inScopeNames, jc.RightBundle)
	}

	bundles := make(map[string]*models.Bundle)
	for _, name := range inScopeNames {
		b, err := getBundle(database, name)
		if err != nil {
			return fmt.Errorf("bundle '%s' does not exist: %w", name, err)
		}
		if b == nil {
			return fmt.Errorf("bundle '%s' does not exist", name)
		}
		bundles[name] = b
	}

	// 2. Validate each GROUP BY field
	for _, qualified := range query.GroupBy.Fields {
		bundleName, fieldName := extractBundleAndFieldFromQualified(qualified)

		if bundleName != "" {
			// Qualified: "orders"."category" -> must be in that bundle and in scope
			inScope := false
			for _, n := range inScopeNames {
				if n == bundleName {
					inScope = true
					break
				}
			}
			if !inScope {
				return fmt.Errorf("GROUP BY field '%s' references bundle '%s' which is not in scope (FROM or JOIN)", qualified, bundleName)
			}
			b := bundles[bundleName]
			if b == nil {
				return fmt.Errorf("bundle '%s' does not exist", bundleName)
			}
			if isSchemaless(b) {
				logger.Debugf("Skipping GROUP BY validation for field '%s': bundle '%s' is schemaless", fieldName, bundleName)
				continue
			}
			if !fieldExistsInBundle(b, fieldName) {
				return fmt.Errorf("GROUP BY field '%s' not found in bundle '%s'", fieldName, bundleName)
			}
			continue
		}

		// Unqualified: "category"
		if len(inScopeNames) == 1 {
			b := bundles[inScopeNames[0]]
			if b == nil {
				continue
			}
			if isSchemaless(b) {
				logger.Debugf("Skipping GROUP BY validation for field '%s': bundle '%s' is schemaless", fieldName, inScopeNames[0])
				continue
			}
			if !fieldExistsInBundle(b, fieldName) {
				return fmt.Errorf("GROUP BY field '%s' not found in bundle '%s'", fieldName, inScopeNames[0])
			}
			continue
		}

		// Multi-bundle: field must exist in exactly one non-schemaless bundle
		var foundIn []string
		hasAnySchema := false
		for _, n := range inScopeNames {
			b := bundles[n]
			if b == nil {
				continue
			}
			if isSchemaless(b) {
				continue
			}
			hasAnySchema = true
			if fieldExistsInBundle(b, fieldName) {
				foundIn = append(foundIn, n)
			}
		}
		if !hasAnySchema {
			logger.Debugf("Skipping GROUP BY validation for field '%s': all in-scope bundles are schemaless", fieldName)
			continue
		}
		switch len(foundIn) {
		case 0:
			return fmt.Errorf("GROUP BY field '%s' not found in any bundle in scope", fieldName)
		case 1:
			// ok
		default:
			return fmt.Errorf("GROUP BY field '%s' is ambiguous (exists in multiple bundles: %v)", fieldName, foundIn)
		}
	}

	logger.Debugf("GROUP BY schema validation passed for %d field(s)", len(query.GroupBy.Fields))
	return nil
}

// extractBundleAndFieldFromQualified parses "a"."b" -> ("a","b"), "b" -> ("","b").
// For "a.b.c", returns ("a", "c"). Uses same trimming/splitting as unified_parser.
func extractBundleAndFieldFromQualified(qualified string) (bundle, field string) {
	s := strings.Trim(qualified, "\"'")
	parts := strings.Split(s, ".")
	if len(parts) == 1 {
		return "", strings.Trim(parts[0], "\"'")
	}
	if len(parts) == 2 {
		return strings.Trim(parts[0], "\"'"), strings.Trim(parts[1], "\"'")
	}
	// 3+ parts: bundle=first, field=last
	return strings.Trim(parts[0], "\"'"), strings.Trim(parts[len(parts)-1], "\"'")
}

// isSchemaless returns true if the bundle has no FieldDefinitions (schemaless).
// For schemaless bundles we skip validation and do not fail.
func isSchemaless(bundle *models.Bundle) bool {
	if bundle == nil {
		return true
	}
	return len(bundle.DocumentStructure.FieldDefinitions) == 0
}

// fieldExistsInBundle returns true if the field is in the bundle's schema.
// If FieldDefinitions is nil or empty (schemaless), returns false; the caller
// must use isSchemaless to skip validation without failing. DocumentID is always valid.
func fieldExistsInBundle(bundle *models.Bundle, fieldName string) bool {
	if bundle == nil || isSchemaless(bundle) {
		return false
	}
	defs := bundle.DocumentStructure.FieldDefinitions
	if strings.EqualFold(fieldName, "DocumentID") {
		return true
	}
	for k := range defs {
		if strings.EqualFold(k, fieldName) {
			return true
		}
	}
	return false
}
