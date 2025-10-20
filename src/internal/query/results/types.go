// Package results provides hierarchical result transformation for SyndrDB JOIN queries.
// This package implements the ORM-like result formatting that groups related documents
// into nested structures based on their relationships. It analyzes Bundle field metadata
// to automatically determine relationship cardinality and transforms flat JOIN results
// into hierarchical object structures.
//
// Key components:
// - RelationshipAnalyzer: Examines Bundle.Field metadata to determine relationship types
// - HierarchicalTransformer: Groups and nests documents based on relationships
// - CardinalityType: Defines relationship cardinality (1:1, 1:Many, Many:Many)
//
// The package follows SyndrDB's Single Responsibility Principle with each component
// handling a specific aspect of the transformation process.
package results

import (
	"time"

	"syndrdb/src/internal/domain/models"
	joinexecutor "syndrdb/src/internal/query/join_executor"

	"go.uber.org/zap"
)

// CardinalityType represents the type of relationship between bundles
type CardinalityType int

const (
	// OneToOne represents a 1:1 relationship - child will be a single object
	OneToOne CardinalityType = iota
	// OneToMany represents a 1:Many relationship - child will be an array
	OneToMany
	// ManyToMany represents a Many:Many relationship - child will be an array
	ManyToMany
)

// String returns the string representation of the cardinality type
func (c CardinalityType) String() string {
	switch c {
	case OneToOne:
		return "OneToOne"
	case OneToMany:
		return "OneToMany"
	case ManyToMany:
		return "ManyToMany"
	default:
		return "Unknown"
	}
}

// RelationshipMetadata contains information about the relationship between two bundles
type RelationshipMetadata struct {
	// RelationshipName is the name of the relationship field (e.g., "Books")
	RelationshipName string
	// ParentBundle is the name of the parent bundle (e.g., "Authors")
	ParentBundle string
	// ChildBundle is the name of the child bundle (e.g., "Books")
	ChildBundle string
	// Cardinality defines the relationship type
	Cardinality CardinalityType
	// JoinKey is the field used to join the bundles
	JoinKey string
	// ParentKey is the field in the parent bundle used for joining
	ParentKey string
	// ChildKey is the field in the child bundle used for joining
	ChildKey string
}

// JoinCondition represents a JOIN condition between two fields
type JoinCondition struct {
	LeftField  string
	RightField string
	Operator   string
}

// HierarchicalTransformRequest contains all information needed for hierarchical transformation
type HierarchicalTransformRequest struct {
	// JoinResults are the flat JOIN results to be transformed
	JoinResults []*joinexecutor.JoinedDocument
	// Relationship contains the metadata about the relationship
	Relationship RelationshipMetadata
	// SelectedFields are the fields to include in the result (empty = all fields)
	SelectedFields []string
	// Logger for debugging and error reporting
	Logger *zap.SugaredLogger
}

// HierarchicalTransformResult contains the result of hierarchical transformation
type HierarchicalTransformResult struct {
	// Documents contains the hierarchically structured documents
	Documents map[string]*models.Document
	// ParentCount is the number of unique parent documents
	ParentCount int
	// TotalChildDocuments is the total number of child documents nested
	TotalChildDocuments int
	// TransformationTime is the time taken for the transformation
	TransformationTime time.Duration
}

// RelationshipAnalysisRequest contains information needed to analyze relationships
type RelationshipAnalysisRequest struct {
	// LeftBundle is the left bundle in the JOIN
	LeftBundle *models.Bundle
	// RightBundle is the right bundle in the JOIN
	RightBundle *models.Bundle
	// JoinConditions are the conditions used in the JOIN
	JoinConditions []JoinCondition
	// RelationshipName is the specified relationship name from the query
	RelationshipName string
	// Logger for debugging and error reporting
	Logger *zap.SugaredLogger
}

// RelationshipAnalysisResult contains the result of relationship analysis
type RelationshipAnalysisResult struct {
	// Metadata contains the analyzed relationship information
	Metadata RelationshipMetadata
	// IsSupported indicates if the relationship is supported for hierarchical transformation
	IsSupported bool
	// ErrorMessage contains any error message if analysis failed
	ErrorMessage string
}
