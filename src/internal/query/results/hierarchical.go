// Package results implements hierarchical transformation for SyndrDB JOIN results.
// This file contains the HierarchicalTransformer component which groups flat JOIN results
// into nested structures based on relationship metadata, creating ORM-like object hierarchies.
//
// The transformer follows SyndrDB's Single Responsibility Principle by focusing solely on
// result transformation and structure building, while leveraging RelationshipAnalyzer for
// relationship metadata.
package results

import (
	"fmt"
	"strings"
	"time"

	"syndrdb/src/internal/domain/models"
	joinexecutor "syndrdb/src/internal/query/join_executor"

	"go.uber.org/zap"
)

// HierarchicalTransformer transforms flat JOIN results into hierarchical structures
type HierarchicalTransformer struct {
	logger *zap.SugaredLogger
}

// NewHierarchicalTransformer creates a new HierarchicalTransformer instance
func NewHierarchicalTransformer(logger *zap.SugaredLogger) *HierarchicalTransformer {
	return &HierarchicalTransformer{
		logger: logger,
	}
}

// Transform converts flat JOIN results into hierarchical document structures
// based on relationship metadata. This creates ORM-like nested objects where
// related documents are grouped under their parent documents.
//
// The transformation process:
// 1. Groups JOIN results by parent document ID
// 2. Creates parent documents with all parent fields
// 3. Nests child documents as arrays (1:Many, Many:Many) or objects (1:1)
// 4. Applies field selection if specified
//
// Parameters:
//   - request: Contains JOIN results, relationship metadata, and transformation options
//
// Returns:
//   - HierarchicalTransformResult: Contains the transformed hierarchical documents
func (ht *HierarchicalTransformer) Transform(request HierarchicalTransformRequest) (*HierarchicalTransformResult, error) {
	startTime := time.Now()

	ht.logger.Infof("Starting hierarchical transformation for %d JOIN results with relationship '%s'",
		len(request.JoinResults), request.Relationship.RelationshipName)

	// Validate input
	if len(request.JoinResults) == 0 {
		return &HierarchicalTransformResult{
			Documents:           make(map[string]*models.Document),
			ParentCount:         0,
			TotalChildDocuments: 0,
			TransformationTime:  time.Since(startTime),
		}, nil
	}

	// Group JOIN results by parent document ID
	parentGroups := ht.groupByParentDocument(request.JoinResults, request.Relationship)

	ht.logger.Infof("Grouped %d JOIN results into %d parent groups",
		len(request.JoinResults), len(parentGroups))

	// Transform groups into hierarchical documents
	hierarchicalDocs := make(map[string]*models.Document)
	totalChildDocuments := 0

	for parentID, group := range parentGroups {
		hierarchicalDoc, childCount, err := ht.createHierarchicalDocument(group, request.Relationship)
		if err != nil {
			return nil, fmt.Errorf("error creating hierarchical document for parent '%s': %w", parentID, err)
		}

		// Apply field selection if specified
		if len(request.SelectedFields) > 0 {
			hierarchicalDoc = ht.applyFieldSelection(hierarchicalDoc, request.SelectedFields, request.Relationship)
		}

		hierarchicalDocs[parentID] = hierarchicalDoc
		totalChildDocuments += childCount
	}

	transformationTime := time.Since(startTime)

	ht.logger.Infof("Hierarchical transformation completed in %v: %d parent documents with %d total child documents",
		transformationTime, len(hierarchicalDocs), totalChildDocuments)

	return &HierarchicalTransformResult{
		Documents:           hierarchicalDocs,
		ParentCount:         len(hierarchicalDocs),
		TotalChildDocuments: totalChildDocuments,
		TransformationTime:  transformationTime,
	}, nil
}

// groupByParentDocument groups JOIN results by their parent document ID
// This creates a map where each key is a parent document ID and the value
// is a slice of all JOIN results that belong to that parent.
//
// Parameters:
//   - joinResults: The flat JOIN results to group
//   - relationship: Metadata about the relationship
//
// Returns:
//   - map[string][]*joinexecutor.JoinedDocument: Grouped results by parent ID
func (ht *HierarchicalTransformer) groupByParentDocument(joinResults []*joinexecutor.JoinedDocument, relationship RelationshipMetadata) map[string][]*joinexecutor.JoinedDocument {
	groups := make(map[string][]*joinexecutor.JoinedDocument)

	for _, joinResult := range joinResults {
		// Determine which document is the parent based on relationship metadata
		var parentDoc *models.Document
		if relationship.ParentBundle == "left" || ht.isLeftBundleParent(joinResult, relationship) {
			parentDoc = joinResult.LeftDocument
		} else {
			parentDoc = joinResult.RightDocument
		}

		if parentDoc == nil {
			ht.logger.Warnf("Skipping JOIN result with nil parent document")
			continue
		}

		parentID := parentDoc.DocumentID
		groups[parentID] = append(groups[parentID], joinResult)
	}

	return groups
}

// isLeftBundleParent determines if the left document is the parent in the relationship
// This uses the relationship metadata to determine the correct parent-child orientation.
//
// Parameters:
//   - joinResult: The JOIN result to analyze
//   - relationship: Metadata about the relationship
//
// Returns:
//   - bool: True if left document is the parent
func (ht *HierarchicalTransformer) isLeftBundleParent(joinResult *joinexecutor.JoinedDocument, relationship RelationshipMetadata) bool {
	// Simple heuristic: if the relationship name matches the right document's bundle context,
	// then the left document is likely the parent
	// This can be enhanced with actual bundle name comparison when available
	return true // Default to left as parent for now
}

// createHierarchicalDocument creates a single hierarchical document from a group of JOIN results
// This combines the parent document with nested child documents based on relationship cardinality.
//
// Parameters:
//   - group: All JOIN results for a single parent document
//   - relationship: Metadata about the relationship
//
// Returns:
//   - *models.Document: The hierarchical document
//   - int: Number of child documents nested
//   - error: Any error that occurred
func (ht *HierarchicalTransformer) createHierarchicalDocument(group []*joinexecutor.JoinedDocument, relationship RelationshipMetadata) (*models.Document, int, error) {
	if len(group) == 0 {
		return nil, 0, fmt.Errorf("cannot create hierarchical document from empty group")
	}

	// Get the parent document from the first JOIN result
	firstResult := group[0]
	var parentDoc *models.Document
	if ht.isLeftBundleParent(firstResult, relationship) {
		parentDoc = firstResult.LeftDocument
	} else {
		parentDoc = firstResult.RightDocument
	}

	if parentDoc == nil {
		return nil, 0, fmt.Errorf("parent document is nil")
	}

	// Create a copy of the parent document for the hierarchical result
	hierarchicalDoc := &models.Document{
		DocumentID: parentDoc.DocumentID,
		Fields:     make(map[string]models.Field),
		CreatedAt:  parentDoc.CreatedAt,
		UpdatedAt:  parentDoc.UpdatedAt,
	}

	// Copy parent fields
	for fieldName, field := range parentDoc.Fields {
		hierarchicalDoc.Fields[fieldName] = field
	}

	// Collect child documents
	var childDocuments []*models.Document
	for _, joinResult := range group {
		var childDoc *models.Document
		if ht.isLeftBundleParent(joinResult, relationship) {
			childDoc = joinResult.RightDocument
		} else {
			childDoc = joinResult.LeftDocument
		}

		if childDoc != nil {
			// Create a copy of the child document
			childCopy := &models.Document{
				DocumentID: childDoc.DocumentID,
				Fields:     make(map[string]models.Field),
				CreatedAt:  childDoc.CreatedAt,
				UpdatedAt:  childDoc.UpdatedAt,
			}

			// Copy child fields
			for fieldName, field := range childDoc.Fields {
				childCopy.Fields[fieldName] = field
			}

			childDocuments = append(childDocuments, childCopy)
		}
	}

	// Add child documents to parent based on relationship cardinality
	err := ht.addChildDocumentsToParent(hierarchicalDoc, childDocuments, relationship)
	if err != nil {
		return nil, 0, fmt.Errorf("error adding child documents: %w", err)
	}

	return hierarchicalDoc, len(childDocuments), nil
}

// addChildDocumentsToParent adds child documents to the parent document
// based on the relationship cardinality (array for 1:Many/Many:Many, object for 1:1)
//
// Parameters:
//   - parentDoc: The parent document to add children to
//   - childDocuments: The child documents to add
//   - relationship: Metadata about the relationship
//
// Returns:
//   - error: Any error that occurred
func (ht *HierarchicalTransformer) addChildDocumentsToParent(parentDoc *models.Document, childDocuments []*models.Document, relationship RelationshipMetadata) error {
	relationshipFieldName := relationship.RelationshipName

	switch relationship.Cardinality {
	case OneToOne:
		// For 1:1 relationships, add the child as a single object
		if len(childDocuments) > 0 {
			// Convert first child document to a field value (simplified approach)
			childFields := ht.documentToFieldValue(childDocuments[0])
			parentDoc.Fields[relationshipFieldName] = models.Field{
				Name:  relationshipFieldName,
				Value: childFields,
			}
		}

	case OneToMany, ManyToMany:
		// For 1:Many and Many:Many relationships, add children as an array
		var childArray []interface{}
		for _, childDoc := range childDocuments {
			childFields := ht.documentToFieldValue(childDoc)
			childArray = append(childArray, childFields)
		}

		parentDoc.Fields[relationshipFieldName] = models.Field{
			Name:  relationshipFieldName,
			Value: childArray,
		}

	default:
		return fmt.Errorf("unsupported relationship cardinality: %v", relationship.Cardinality)
	}

	return nil
}

// documentToFieldValue converts a Document to a field value representation
// This creates a map structure suitable for nesting within parent documents.
//
// Parameters:
//   - doc: The document to convert
//
// Returns:
//   - map[string]interface{}: The field value representation
func (ht *HierarchicalTransformer) documentToFieldValue(doc *models.Document) map[string]interface{} {
	fieldMap := make(map[string]interface{})

	// Add DocumentID
	fieldMap["DocumentID"] = doc.DocumentID

	// Add all fields
	for fieldName, field := range doc.Fields {
		fieldMap[fieldName] = field.Value
	}

	// Add timestamps
	fieldMap["CreatedAt"] = doc.CreatedAt.Format(time.RFC3339)
	fieldMap["UpdatedAt"] = doc.UpdatedAt.Format(time.RFC3339)

	return fieldMap
}

// applyFieldSelection filters the hierarchical document to include only selected fields
// This maintains the hierarchical structure while limiting the fields included in the result.
//
// Parameters:
//   - doc: The hierarchical document to filter
//   - selectedFields: The fields to include
//   - relationship: Metadata about the relationship
//
// Returns:
//   - *models.Document: The filtered document
func (ht *HierarchicalTransformer) applyFieldSelection(doc *models.Document, selectedFields []string, relationship RelationshipMetadata) *models.Document {
	if len(selectedFields) == 0 {
		return doc
	}

	filteredDoc := &models.Document{
		DocumentID: doc.DocumentID,
		Fields:     make(map[string]models.Field),
		CreatedAt:  doc.CreatedAt,
		UpdatedAt:  doc.UpdatedAt,
	}

	// Always include the relationship field if it exists
	relationshipFieldName := relationship.RelationshipName
	if relationshipField, exists := doc.Fields[relationshipFieldName]; exists {
		filteredDoc.Fields[relationshipFieldName] = relationshipField
	}

	// Include selected fields if they exist
	for _, fieldName := range selectedFields {
		// Skip the relationship field as it's already included
		if strings.EqualFold(fieldName, relationshipFieldName) {
			continue
		}

		if field, exists := doc.Fields[fieldName]; exists {
			filteredDoc.Fields[fieldName] = field
		}
	}

	return filteredDoc
}
