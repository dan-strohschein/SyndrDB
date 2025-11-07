package mutations

import (
	"fmt"
	"syndrdb/src/internal/domain/models"
	"syndrdb/src/internal/server"

	"github.com/vektah/gqlparser/v2/ast"
	"go.uber.org/zap"
)

// MutationResolver resolves mutation response fields by fetching and formatting data.
// After a mutation executes, this resolver handles the response formatting:
// - For CREATE/UPDATE: Fetch the created/updated document and resolve requested fields
// - For DELETE: Format deletion metadata (success, deletedId, message)
//
// This follows the Single Responsibility Principle - only handles response resolution.
// Execution logic is in MutationExecutor, validation is in InputValidator.
type MutationResolver struct {
	serviceManager server.ServiceManager
	database       *models.Database
	logger         *zap.SugaredLogger
}

// NewMutationResolver creates a new mutation resolver instance.
func NewMutationResolver(serviceManager server.ServiceManager, database *models.Database, logger *zap.SugaredLogger) *MutationResolver {
	return &MutationResolver{
		serviceManager: serviceManager,
		database:       database,
		logger:         logger,
	}
}

// ResolveCreateResponse resolves the response for a create mutation.
// After creating a document, this fetches it and resolves the requested fields.
//
// Flow:
//  1. Fetch the newly created document by ID
//  2. Resolve selection set fields from the document
//  3. Return formatted response
//
// Example:
//   createUser(input: {...}) { id name email }
//   -> { "id": "123", "name": "Alice", "email": "alice@..." }
func (r *MutationResolver) ResolveCreateResponse(documentID string, bundleName string, selectionSet ast.SelectionSet, variables map[string]interface{}) (map[string]interface{}, error) {
	r.logger.Debugf("Resolving create mutation response for document '%s' in bundle '%s'", documentID, bundleName)

	// Get the bundle
	bundle, err := r.serviceManager.BundleService.GetBundleByName(r.database, bundleName)
	if err != nil {
		return nil, fmt.Errorf("failed to get bundle '%s': %w", bundleName, err)
	}

	// Fetch the created document
	whereClause := fmt.Sprintf("DocumentID = '%s'", documentID)
	documents, err := r.serviceManager.BundleService.GetDocumentsByFilter(bundle, whereClause)
	if err != nil {
		return nil, fmt.Errorf("failed to fetch created document: %w", err)
	}

	if len(documents) == 0 {
		return nil, fmt.Errorf("created document '%s' not found", documentID)
	}

	document := documents[0]

	// Resolve selection set fields
	result, err := r.resolveDocumentFields(document, bundle, selectionSet, variables)
	if err != nil {
		return nil, fmt.Errorf("failed to resolve response fields: %w", err)
	}

	return result, nil
}

// ResolveUpdateResponse resolves the response for an update mutation.
// After updating a document, this fetches it and resolves the requested fields.
//
// Flow:
//  1. Extract document ID from WHERE clause
//  2. Fetch the updated document by ID
//  3. Resolve selection set fields from the document
//  4. Return formatted response
//
// Example:
//   updateUser(id: "123", input: {...}) { id name email }
//   -> { "id": "123", "name": "Bob", "email": "bob@..." }
func (r *MutationResolver) ResolveUpdateResponse(updateCommand *models.DocumentUpdateCommand, bundleName string, selectionSet ast.SelectionSet, variables map[string]interface{}) (map[string]interface{}, error) {
	r.logger.Debugf("Resolving update mutation response for bundle '%s'", bundleName)

	// Get the bundle
	bundle, err := r.serviceManager.BundleService.GetBundleByName(r.database, bundleName)
	if err != nil {
		return nil, fmt.Errorf("failed to get bundle '%s': %w", bundleName, err)
	}

	// Fetch the updated document using the WHERE clause
	documents, err := r.serviceManager.BundleService.GetDocumentsByFilter(bundle, updateCommand.WhereClause)
	if err != nil {
		return nil, fmt.Errorf("failed to fetch updated document: %w", err)
	}

	if len(documents) == 0 {
		return nil, fmt.Errorf("updated document not found")
	}

	// Return the first document (update by ID should only return one)
	document := documents[0]

	// Resolve selection set fields
	result, err := r.resolveDocumentFields(document, bundle, selectionSet, variables)
	if err != nil {
		return nil, fmt.Errorf("failed to resolve response fields: %w", err)
	}

	return result, nil
}

// ResolveDeleteResponse resolves the response for a delete mutation.
// Returns metadata about the deletion (not the deleted document).
//
// Response format:
//
//	{
//	  "success": true,
//	  "deletedId": "123",
//	  "message": "User deleted successfully"
//	}
//
// Example:
//
//	deleteUser(id: "123") { success deletedId message }
//	-> { "success": true, "deletedId": "123", "message": "..." }
func (r *MutationResolver) ResolveDeleteResponse(deletedIDs []string, bundleName string, selectionSet ast.SelectionSet) (map[string]interface{}, error) {
	r.logger.Debugf("Resolving delete mutation response for %d document(s) in bundle '%s'", len(deletedIDs), bundleName)

	if len(deletedIDs) == 0 {
		return nil, fmt.Errorf("no documents were deleted")
	}

	// For single delete, return the deleted ID
	deletedID := deletedIDs[0]

	// Build response based on selection set
	result := make(map[string]interface{})

	for _, selection := range selectionSet {
		field, ok := selection.(*ast.Field)
		if !ok {
			continue
		}

		switch field.Name {
		case "success":
			result["success"] = true
		case "deletedId":
			result["deletedId"] = deletedID
		case "message":
			result["message"] = fmt.Sprintf("%s deleted successfully", bundleName)
		default:
			r.logger.Warnf("Unknown field '%s' in delete response selection set", field.Name)
		}
	}

	return result, nil
}

// resolveDocumentFields resolves the requested fields from a document.
// This handles field mapping, type conversion, and relationship resolution.
//
// TODO: I will add relationship field resolution here when integrating with the relationship resolver.
// For now, this resolves direct fields only. Relationship fields would require:
// - Detecting relationship fields from bundle schema
// - Calling RelationshipResolver to fetch related documents
// - Recursively resolving nested selection sets
func (r *MutationResolver) resolveDocumentFields(document *models.Document, bundle *models.Bundle, selectionSet ast.SelectionSet, variables map[string]interface{}) (map[string]interface{}, error) {
	result := make(map[string]interface{})

	for _, selection := range selectionSet {
		field, ok := selection.(*ast.Field)
		if !ok {
			continue
		}

		fieldName := field.Name

		// Special handling for 'id' field (maps to DocumentID)
		if fieldName == "id" {
			result["id"] = document.DocumentID
			continue
		}

		// Resolve direct field from document
		if docField, exists := document.Fields[fieldName]; exists {
			result[fieldName] = docField.Value
		} else if fieldName == "DocumentID" {
			// Allow explicit DocumentID access
			result["DocumentID"] = document.DocumentID
		} else {
			// Field not found - could be a relationship field
			// TODO: I will add relationship field detection and resolution here.
			// For now, log a warning and return null
			r.logger.Warnf("Field '%s' not found in document '%s'", fieldName, document.DocumentID)
			result[fieldName] = nil
		}
	}

	return result, nil
}

// ResolveBatchCreateResponse resolves responses for batch create mutations.
// TODO: I will implement this when SyndrDB adds batch operation support.
// Response would include:
// - Array of created documents (with resolved fields)
// - Success/failure counts
// - Error details for failed items
//
// Example response:
//
//	{
//	  "success": true,
//	  "count": 10,
//	  "items": [ { "id": "1", "name": "..." }, ... ],
//	  "errors": []
//	}
func (r *MutationResolver) ResolveBatchCreateResponse(documentIDs []string, bundleName string, selectionSet ast.SelectionSet, variables map[string]interface{}) (map[string]interface{}, error) {
	return nil, fmt.Errorf("batch create response resolution not yet supported")
}

// ResolveNestedCreateResponse resolves responses for nested create mutations.
// TODO: I will implement this when adding advanced relationship features.
// Response would include:
// - Parent document with resolved fields
// - Nested relationship documents with resolved fields
// - Proper nesting structure matching GraphQL schema
//
// Example response:
//
//	{
//	  "id": "123",
//	  "name": "Alice",
//	  "posts": [
//	    { "id": "456", "title": "First Post" },
//	    { "id": "789", "title": "Second Post" }
//	  ]
//	}
func (r *MutationResolver) ResolveNestedCreateResponse(parentID string, bundleName string, nestedResults map[string][]string, selectionSet ast.SelectionSet, variables map[string]interface{}) (map[string]interface{}, error) {
	return nil, fmt.Errorf("nested create response resolution not yet supported")
}

// ResolveFieldValue resolves a single field value with proper type handling.
// This is a helper for more complex field resolution scenarios.
//
// TODO: I will enhance this when implementing advanced type handling:
// - Custom scalar serialization (DateTime, JSON, UUID)
// - Enum value validation
// - Union type resolution
// - Interface type resolution
func (r *MutationResolver) ResolveFieldValue(fieldValue interface{}, fieldType string) (interface{}, error) {
	// For now, return value as-is
	// TODO: Add type-specific formatting and validation
	return fieldValue, nil
}

// FormatErrorResponse formats an error into a GraphQL-compliant error response.
// GraphQL errors include path, message, and optional extensions.
//
// Example:
//
//	{
//	  "errors": [{
//	    "message": "Validation failed",
//	    "path": ["createUser"],
//	    "extensions": {
//	      "code": "VALIDATION_ERROR",
//	      "field": "email"
//	    }
//	  }]
//	}
func (r *MutationResolver) FormatErrorResponse(err error, mutationName string, path []string) map[string]interface{} {
	return map[string]interface{}{
		"errors": []map[string]interface{}{
			{
				"message": err.Error(),
				"path":    append(path, mutationName),
				"extensions": map[string]interface{}{
					"code": "MUTATION_ERROR",
				},
			},
		},
	}
}
