// Package results implements relationship analysis for SyndrDB hierarchical JOIN transformations.
// This file contains the RelationshipAnalyzer component which examines Bundle field metadata
// to determine relationship cardinality and provides metadata for hierarchical transformations.
//
// The analyzer follows SyndrDB's comprehensive error handling practices and uses Bundle.Field
// metadata to automatically detect relationship types without requiring explicit syntax.
package results

import (
	"fmt"
	"strings"

	"syndrdb/src/internal/domain/models"

	"go.uber.org/zap"
)

// RelationshipAnalyzer analyzes relationships between bundles for hierarchical transformations
type RelationshipAnalyzer struct {
	logger *zap.SugaredLogger
}

// NewRelationshipAnalyzer creates a new RelationshipAnalyzer instance
func NewRelationshipAnalyzer(logger *zap.SugaredLogger) *RelationshipAnalyzer {
	return &RelationshipAnalyzer{
		logger: logger,
	}
}

// AnalyzeRelationship analyzes the relationship between two bundles based on JOIN conditions
// and Bundle field metadata to determine cardinality and relationship structure.
//
// This function follows SyndrDB's Single Responsibility Principle by focusing solely on
// relationship analysis and metadata extraction.
//
// Parameters:
//   - request: Contains all information needed for relationship analysis
//
// Returns:
//   - RelationshipAnalysisResult: Contains the analyzed relationship metadata
func (ra *RelationshipAnalyzer) AnalyzeRelationship(request RelationshipAnalysisRequest) RelationshipAnalysisResult {
	ra.logger.Infof("Analyzing relationship between bundles '%s' and '%s'",
		request.LeftBundle.Name, request.RightBundle.Name)

	// Validate input parameters
	if request.LeftBundle == nil || request.RightBundle == nil {
		return RelationshipAnalysisResult{
			IsSupported:  false,
			ErrorMessage: "left and right bundles cannot be nil",
		}
	}

	if len(request.JoinConditions) == 0 {
		return RelationshipAnalysisResult{
			IsSupported:  false,
			ErrorMessage: "at least one join condition is required",
		}
	}

	if request.RelationshipName == "" {
		return RelationshipAnalysisResult{
			IsSupported:  false,
			ErrorMessage: "relationship name is required",
		}
	}

	// Extract join keys from the first join condition
	// TODO: Support multiple join conditions for complex relationships
	joinCondition := request.JoinConditions[0]

	// Determine which bundle is parent and which is child based on relationship name
	// The relationship name should match the child bundle name (e.g., "Books" for Authors->Books)
	var parentBundle, childBundle *models.Bundle
	var parentKey, childKey string

	if strings.EqualFold(request.RelationshipName, request.RightBundle.Name) {
		// Right bundle is the child (relationship name matches right bundle)
		parentBundle = request.LeftBundle
		childBundle = request.RightBundle
		parentKey = ra.extractFieldName(joinCondition.LeftField)
		childKey = ra.extractFieldName(joinCondition.RightField)
	} else if strings.EqualFold(request.RelationshipName, request.LeftBundle.Name) {
		// Left bundle is the child (relationship name matches left bundle)
		parentBundle = request.RightBundle
		childBundle = request.LeftBundle
		parentKey = ra.extractFieldName(joinCondition.RightField)
		childKey = ra.extractFieldName(joinCondition.LeftField)
	} else {
		return RelationshipAnalysisResult{
			IsSupported:  false,
			ErrorMessage: fmt.Sprintf("relationship name '%s' does not match either bundle name", request.RelationshipName),
		}
	}

	ra.logger.Infof("Determined parent bundle: '%s', child bundle: '%s'",
		parentBundle.Name, childBundle.Name)

	// Analyze cardinality based on Bundle field metadata and join keys
	cardinality := ra.determineCardinality(parentBundle, childBundle, parentKey, childKey)

	// Create relationship metadata
	metadata := RelationshipMetadata{
		RelationshipName: request.RelationshipName,
		ParentBundle:     parentBundle.Name,
		ChildBundle:      childBundle.Name,
		Cardinality:      cardinality,
		JoinKey:          fmt.Sprintf("%s=%s", parentKey, childKey),
		ParentKey:        parentKey,
		ChildKey:         childKey,
	}

	ra.logger.Infof("Analyzed relationship: %s (%s) -> %s (%s) with cardinality %s",
		metadata.ParentBundle, metadata.ParentKey,
		metadata.ChildBundle, metadata.ChildKey,
		metadata.Cardinality.String())

	return RelationshipAnalysisResult{
		Metadata:    metadata,
		IsSupported: true,
	}
}

// determineCardinality analyzes Bundle field metadata to determine relationship cardinality
// This function examines the join keys and bundle structures to infer the relationship type.
//
// Current implementation uses heuristics based on field names and bundle structure.
// Future enhancement: Examine actual Bundle.Field metadata when relationship definitions
// are stored in bundle schemas.
//
// Parameters:
//   - parentBundle: The parent bundle in the relationship
//   - childBundle: The child bundle in the relationship
//   - parentKey: The join key in the parent bundle
//   - childKey: The join key in the child bundle
//
// Returns:
//   - CardinalityType: The determined relationship cardinality
func (ra *RelationshipAnalyzer) determineCardinality(parentBundle, childBundle *models.Bundle, parentKey, childKey string) CardinalityType {
	ra.logger.Infof("Determining cardinality for relationship %s.%s -> %s.%s",
		parentBundle.Name, parentKey, childBundle.Name, childKey)

	// Heuristic 1: If child key contains "ID" and references parent's primary key, likely 1:Many
	if strings.Contains(strings.ToLower(childKey), "id") &&
		(parentKey == "DocumentID" || strings.Contains(strings.ToLower(parentKey), "id")) {
		ra.logger.Infof("Detected 1:Many relationship based on ID field pattern")
		return OneToMany
	}

	// Heuristic 2: If parent key contains child bundle name + "ID", likely 1:1
	expectedForeignKey := fmt.Sprintf("%sID", childBundle.Name)
	if strings.EqualFold(parentKey, expectedForeignKey) {
		ra.logger.Infof("Detected 1:1 relationship based on foreign key pattern")
		return OneToOne
	}

	// Heuristic 3: If both keys are "DocumentID", could be Many:Many through junction
	if parentKey == "DocumentID" && childKey == "DocumentID" {
		ra.logger.Infof("Detected potential Many:Many relationship based on DocumentID join")
		return ManyToMany
	}

	// Default assumption: 1:Many relationship (most common case)
	ra.logger.Infof("Defaulting to 1:Many relationship")
	return OneToMany
}

// extractFieldName extracts the field name from a qualified field reference
// Handles both qualified names (e.g., "Authors.DocumentID") and simple names (e.g., "DocumentID")
//
// Parameters:
//   - fieldRef: The field reference to extract from
//
// Returns:
//   - string: The extracted field name
func (ra *RelationshipAnalyzer) extractFieldName(fieldRef string) string {
	// Remove quotes if present
	fieldRef = strings.Trim(fieldRef, "\"'")

	// Handle qualified field names (e.g., "Authors.DocumentID" -> "DocumentID")
	if dotIndex := strings.LastIndex(fieldRef, "."); dotIndex >= 0 {
		return fieldRef[dotIndex+1:]
	}

	return fieldRef
}

// ValidateRelationshipName validates that a relationship name is valid for hierarchical transformation
// This function follows SyndrDB's comprehensive error handling practices.
//
// Parameters:
//   - relationshipName: The relationship name to validate
//   - leftBundle: The left bundle in the JOIN
//   - rightBundle: The right bundle in the JOIN
//
// Returns:
//   - bool: True if the relationship name is valid
//   - string: Error message if validation fails
func (ra *RelationshipAnalyzer) ValidateRelationshipName(relationshipName string, leftBundle, rightBundle *models.Bundle) (bool, string) {
	if relationshipName == "" {
		return false, "relationship name cannot be empty"
	}

	// Relationship name should match one of the bundle names
	if !strings.EqualFold(relationshipName, leftBundle.Name) &&
		!strings.EqualFold(relationshipName, rightBundle.Name) {
		return false, fmt.Sprintf("relationship name '%s' must match either left bundle '%s' or right bundle '%s'",
			relationshipName, leftBundle.Name, rightBundle.Name)
	}

	return true, ""
}
