package graphQL

/*
GraphQL Resolvers for SyndrDB

This file implements the GraphQL resolvers that translate GraphQL operations
into SyndrDB native commands using the existing command director infrastructure.

The resolvers provide:
- Query resolvers for fetching data (databases, bundles, documents)
- Mutation resolvers for modifying data (create, update, delete)
- Integration with SyndrDB's existing CRUD operations
- Type-safe data transformation between GraphQL and SyndrDB formats
*/

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"syndrdb/src/internal/server"
	"time"

	"github.com/vektah/gqlparser/v2/ast"
)

// Database resolver response
type DatabaseResponse struct {
	Name      string    `json:"name"`
	Bundles   []string  `json:"bundles"`
	CreatedAt time.Time `json:"createdAt"`
}

// Bundle resolver response
type BundleResponse struct {
	Name          string                   `json:"name"`
	Database      string                   `json:"database"`
	Documents     []map[string]interface{} `json:"documents"`
	DocumentCount int                      `json:"documentCount"`
	CreatedAt     time.Time                `json:"createdAt"`
}

// Document resolver response
type DocumentResponse struct {
	ID        string                 `json:"id"`
	Bundle    string                 `json:"bundle"`
	Fields    map[string]interface{} `json:"fields"`
	CreatedAt time.Time              `json:"createdAt"`
	UpdatedAt time.Time              `json:"updatedAt"`
}

// Previously:resolveDatabases(field *ast.Field, variables map[string]interface{})
// resolveDatabases resolves the "databases" query field
func (h *GraphQLHandler) resolveDatabases() (interface{}, error) {
	h.logger.Info("Resolving databases query")

	// Use the service manager to get all databases
	// For now, we'll return the current database - this can be expanded to support multiple databases
	databases := []DatabaseResponse{
		{
			Name:      h.database.Name,
			Bundles:   []string{}, // Will be populated below
			CreatedAt: time.Now(), // In a real implementation, this would come from database metadata
		},
	}

	// Get bundles for the database
	for bundleName := range h.database.Bundles {
		databases[0].Bundles = append(databases[0].Bundles, bundleName)
	}

	h.logger.Infof("Resolved %d databases", len(databases))
	return databases, nil
}

// resolveDatabase resolves the "database" query field
func (h *GraphQLHandler) resolveDatabase(field *ast.Field, variables map[string]interface{}) (interface{}, error) {
	// Get the database name argument
	nameArg, ok := h.getArgument(field, "name", variables)
	if !ok {
		return nil, fmt.Errorf("database name argument is required")
	}

	databaseName, ok := nameArg.(string)
	if !ok {
		return nil, fmt.Errorf("database name must be a string")
	}

	h.logger.Infof("Resolving database query for: %s", databaseName)

	// Check if the requested database matches our current database
	if h.database.Name != databaseName {
		return nil, nil // Database not found
	}

	var bundles []string
	for bundleName := range h.database.Bundles {
		bundles = append(bundles, bundleName)
	}

	database := DatabaseResponse{
		Name:      h.database.Name,
		Bundles:   bundles,
		CreatedAt: time.Now(),
	}

	h.logger.Infof("Resolved database: %s", databaseName)
	return database, nil
}

// resolveBundles resolves the "bundles" query field
func (h *GraphQLHandler) resolveBundles(field *ast.Field, variables map[string]interface{}) (interface{}, error) {
	// Get the database name argument
	databaseArg, ok := h.getArgument(field, "database", variables)
	if !ok {
		return nil, fmt.Errorf("database argument is required")
	}

	databaseName, ok := databaseArg.(string)
	if !ok {
		return nil, fmt.Errorf("database name must be a string")
	}

	h.logger.Infof("Resolving bundles query for database: %s", databaseName)

	// Check if the requested database matches our current database
	if h.database.Name != databaseName {
		return []BundleResponse{}, nil
	}

	var bundles []BundleResponse

	for bundleName, bundle := range h.database.Bundles {
		bundleResponse := BundleResponse{
			Name:          bundleName,
			Database:      databaseName,
			Documents:     []map[string]interface{}{}, // Will be populated if requested
			DocumentCount: len(*bundle.Documents),
			CreatedAt:     time.Now(),
		}

		bundles = append(bundles, bundleResponse)
	}

	h.logger.Infof("Resolved %d bundles for database: %s", len(bundles), databaseName)
	return bundles, nil
}

// resolveBundle resolves the "bundle" query field
func (h *GraphQLHandler) resolveBundle(field *ast.Field, variables map[string]interface{}) (interface{}, error) {
	// Get the bundle name argument
	nameArg, ok := h.getArgument(field, "name", variables)
	if !ok {
		return nil, fmt.Errorf("bundle name argument is required")
	}

	bundleName, ok := nameArg.(string)
	if !ok {
		return nil, fmt.Errorf("bundle name must be a string")
	}

	// Get the database name argument
	databaseArg, ok := h.getArgument(field, "database", variables)
	if !ok {
		return nil, fmt.Errorf("database argument is required")
	}

	databaseName, ok := databaseArg.(string)
	if !ok {
		return nil, fmt.Errorf("database name must be a string")
	}

	h.logger.Infof("Resolving bundle query for: %s.%s", databaseName, bundleName)

	// Check if the requested database matches our current database
	if h.database.Name != databaseName {
		return nil, nil
	}

	bundle, exists := h.database.Bundles[bundleName]
	if !exists {
		return nil, nil
	}

	bundleResponse := BundleResponse{
		Name:          bundleName,
		Database:      databaseName,
		Documents:     []map[string]interface{}{},
		DocumentCount: len(*bundle.Documents),
		CreatedAt:     time.Now(),
	}

	h.logger.Infof("Resolved bundle: %s.%s", databaseName, bundleName)
	return bundleResponse, nil
}

// resolveDocuments resolves the "documents" query field
func (h *GraphQLHandler) resolveDocuments(field *ast.Field, variables map[string]interface{}) (interface{}, error) {
	// Get the bundle name argument
	bundleArg, ok := h.getArgument(field, "bundle", variables)
	if !ok {
		return nil, fmt.Errorf("bundle argument is required")
	}

	bundleName, ok := bundleArg.(string)
	if !ok {
		return nil, fmt.Errorf("bundle name must be a string")
	}

	h.logger.Infof("Resolving documents query for bundle: %s", bundleName)

	// Get optional arguments
	var whereClause string
	if whereArg, exists := h.getArgument(field, "where", variables); exists {
		if whereStr, ok := whereArg.(string); ok {
			whereClause = whereStr
		}
	}

	var orderBy string
	if orderByArg, exists := h.getArgument(field, "orderBy", variables); exists {
		if orderByStr, ok := orderByArg.(string); ok {
			orderBy = orderByStr
		}
	}

	var limit int
	if limitArg, exists := h.getArgument(field, "limit", variables); exists {
		if limitStr, ok := limitArg.(string); ok {
			if parsedLimit, err := strconv.Atoi(limitStr); err == nil {
				limit = parsedLimit
			}
		}
	}

	// Build the SyndrDB query
	query := fmt.Sprintf("SELECT * FROM %s", bundleName)

	if whereClause != "" {
		query += fmt.Sprintf(" WHERE %s", whereClause)
	}

	if orderBy != "" {
		query += fmt.Sprintf(" ORDER BY %s", orderBy)
	}

	if limit > 0 {
		query += fmt.Sprintf(" LIMIT %d", limit)
	}

	// Execute the query using the command director
	startTime := time.Now()
	response, err := server.CommandDirector(context.Background(), h.database, h.serviceManager, query, h.logger, startTime, h.currentSession, h.currentClientIP)
	if err != nil {
		return nil, fmt.Errorf("failed to execute documents query: %w", err)
	}

	// Parse the response
	var documents []DocumentResponse
	if response != nil {
		// Convert the response data to document responses
		if docs, ok := response.([]interface{}); ok {
			for _, doc := range docs {
				if docMap, ok := doc.(map[string]interface{}); ok {
					documentResponse := DocumentResponse{
						ID:        fmt.Sprintf("%v", docMap["id"]),
						Bundle:    bundleName,
						Fields:    docMap,
						CreatedAt: time.Now(),
						UpdatedAt: time.Now(),
					}
					documents = append(documents, documentResponse)
				}
			}
		}
	}

	h.logger.Infof("Resolved %d documents for bundle: %s", len(documents), bundleName)
	return documents, nil
}

// resolveDocument resolves the "document" query field
func (h *GraphQLHandler) resolveDocument(field *ast.Field, variables map[string]interface{}) (interface{}, error) {
	// Get the document ID argument
	idArg, ok := h.getArgument(field, "id", variables)
	if !ok {
		return nil, fmt.Errorf("document id argument is required")
	}

	documentID, ok := idArg.(string)
	if !ok {
		return nil, fmt.Errorf("document id must be a string")
	}

	// Get the bundle name argument
	bundleArg, ok := h.getArgument(field, "bundle", variables)
	if !ok {
		return nil, fmt.Errorf("bundle argument is required")
	}

	bundleName, ok := bundleArg.(string)
	if !ok {
		return nil, fmt.Errorf("bundle name must be a string")
	}

	h.logger.Infof("Resolving document query for: %s.%s", bundleName, documentID)

	// Build the SyndrDB query
	query := fmt.Sprintf("SELECT * FROM %s WHERE id = '%s'", bundleName, documentID)

	// Execute the query using the command director
	startTime := time.Now()
	response, err := server.CommandDirector(context.Background(), h.database, h.serviceManager, query, h.logger, startTime, h.currentSession, h.currentClientIP)
	if err != nil {
		return nil, fmt.Errorf("failed to execute document query: %w", err)
	}

	// Parse the response
	if response != nil {
		if docs, ok := response.([]interface{}); ok && len(docs) > 0 {
			if docMap, ok := docs[0].(map[string]interface{}); ok {
				documentResponse := DocumentResponse{
					ID:        documentID,
					Bundle:    bundleName,
					Fields:    docMap,
					CreatedAt: time.Now(),
					UpdatedAt: time.Now(),
				}

				h.logger.Infof("Resolved document: %s.%s", bundleName, documentID)
				return documentResponse, nil
			}
		}
	}

	// Document not found
	return nil, nil
}

// mutateCreateDatabase resolves the "createDatabase" mutation field
func (h *GraphQLHandler) mutateCreateDatabase(field *ast.Field, variables map[string]interface{}) (interface{}, error) {
	// Get the database name argument
	nameArg, ok := h.getArgument(field, "name", variables)
	if !ok {
		return nil, fmt.Errorf("database name argument is required")
	}

	databaseName, ok := nameArg.(string)
	if !ok {
		return nil, fmt.Errorf("database name must be a string")
	}

	h.logger.Infof("Creating database: %s", databaseName)

	// Build the SyndrDB command
	command := fmt.Sprintf("CREATE DATABASE %s", databaseName)

	// Execute the command using the command director
	startTime := time.Now()
	response, err := server.CommandDirector(context.Background(), h.database, h.serviceManager, command, h.logger, startTime, h.currentSession, h.currentClientIP)
	if err != nil {
		return nil, fmt.Errorf("failed to create database: %w", err)
	}

	if response == nil {
		return nil, fmt.Errorf("database creation failed: no response")
	}

	database := DatabaseResponse{
		Name:      databaseName,
		Bundles:   []string{},
		CreatedAt: time.Now(),
	}

	h.logger.Infof("Created database: %s", databaseName)
	return database, nil
}

// mutateCreateBundle resolves the "createBundle" mutation field
func (h *GraphQLHandler) mutateCreateBundle(field *ast.Field, variables map[string]interface{}) (interface{}, error) {
	// Get the bundle name argument
	nameArg, ok := h.getArgument(field, "name", variables)
	if !ok {
		return nil, fmt.Errorf("bundle name argument is required")
	}

	bundleName, ok := nameArg.(string)
	if !ok {
		return nil, fmt.Errorf("bundle name must be a string")
	}

	// Get the database name argument
	databaseArg, ok := h.getArgument(field, "database", variables)
	if !ok {
		return nil, fmt.Errorf("database argument is required")
	}

	databaseName, ok := databaseArg.(string)
	if !ok {
		return nil, fmt.Errorf("database name must be a string")
	}

	// Get the fields argument
	fieldsArg, ok := h.getArgument(field, "fields", variables)
	if !ok {
		return nil, fmt.Errorf("fields argument is required")
	}

	fieldsStr, ok := fieldsArg.(string)
	if !ok {
		return nil, fmt.Errorf("fields must be a string")
	}

	h.logger.Infof("Creating bundle: %s.%s", databaseName, bundleName)

	// Build the SyndrDB command
	command := fmt.Sprintf("CREATE BUNDLE %s %s", bundleName, fieldsStr)

	// Execute the command using the command director
	startTime := time.Now()
	response, err := server.CommandDirector(context.Background(), h.database, h.serviceManager, command, h.logger, startTime, h.currentSession, h.currentClientIP)
	if err != nil {
		return nil, fmt.Errorf("failed to create bundle: %w", err)
	}

	if response == nil {
		return nil, fmt.Errorf("bundle creation failed: no response")
	}

	bundle := BundleResponse{
		Name:          bundleName,
		Database:      databaseName,
		Documents:     []map[string]interface{}{},
		DocumentCount: 0,
		CreatedAt:     time.Now(),
	}

	h.logger.Infof("Created bundle: %s.%s", databaseName, bundleName)
	return bundle, nil
}

// mutateCreateDocument resolves the "createDocument" mutation field
// mutateCreateDocument creates a new document in a bundle
//
// TODO: I will reimplement this mutation using the native mutation pattern when
// implementing Phase 6 mutation support. The native pattern will:
// - Parse GraphQL mutations into unified mutation commands
// - Use Phase 5 schemas for type validation and field mapping
// - Execute through the same service layer as native SyndrDB mutations
// - Ensure identical performance to SyndrDB's native CREATE DOCUMENT command
//
// Current implementation works but is part of the pre-Phase 6 GraphQL system.
func (h *GraphQLHandler) mutateCreateDocument(field *ast.Field, variables map[string]interface{}) (interface{}, error) {
	// Get the bundle name argument
	bundleArg, ok := h.getArgument(field, "bundle", variables)
	if !ok {
		return nil, fmt.Errorf("bundle argument is required")
	}

	bundleName, ok := bundleArg.(string)
	if !ok {
		return nil, fmt.Errorf("bundle name must be a string")
	}

	// Get the fields argument
	fieldsArg, ok := h.getArgument(field, "fields", variables)
	if !ok {
		return nil, fmt.Errorf("fields argument is required")
	}

	fieldsStr, ok := fieldsArg.(string)
	if !ok {
		return nil, fmt.Errorf("fields must be a string")
	}

	h.logger.Infof("Creating document in bundle: %s", bundleName)

	// Parse the fields JSON
	var fields map[string]interface{}
	if err := json.Unmarshal([]byte(fieldsStr), &fields); err != nil {
		return nil, fmt.Errorf("invalid fields JSON: %w", err)
	}

	// Build the SyndrDB command
	command := fmt.Sprintf("INSERT INTO %s %s", bundleName, fieldsStr)

	// Execute the command using the command director
	startTime := time.Now()
	response, err := server.CommandDirector(context.Background(), h.database, h.serviceManager, command, h.logger, startTime, h.currentSession, h.currentClientIP)
	if err != nil {
		return nil, fmt.Errorf("failed to create document: %w", err)
	}

	if response == nil {
		return nil, fmt.Errorf("document creation failed: no response")
	}

	// Generate a document ID (in a real implementation, this would come from the response)
	documentID := fmt.Sprintf("doc_%d", time.Now().Unix())

	document := DocumentResponse{
		ID:        documentID,
		Bundle:    bundleName,
		Fields:    fields,
		CreatedAt: time.Now(),
		UpdatedAt: time.Now(),
	}

	h.logger.Infof("Created document in bundle: %s", bundleName)
	return document, nil
}

// mutateUpdateDocument resolves the "updateDocument" mutation field
// mutateUpdateDocument updates an existing document in a bundle
//
// TODO: I will reimplement this mutation using the native mutation pattern when
// implementing Phase 6 mutation support. The native pattern will:
// - Parse GraphQL mutations into unified UPDATE commands
// - Use Phase 5 schemas for field validation and type checking
// - Support partial updates with proper merge semantics
// - Execute through the same service layer as native SyndrDB UPDATE DOCUMENT
// - Ensure identical performance to SyndrDB's native update operations
//
// Current implementation works but is part of the pre-Phase 6 GraphQL system.
func (h *GraphQLHandler) mutateUpdateDocument(field *ast.Field, variables map[string]interface{}) (interface{}, error) {
	// Get the document ID argument
	idArg, ok := h.getArgument(field, "id", variables)
	if !ok {
		return nil, fmt.Errorf("document id argument is required")
	}

	documentID, ok := idArg.(string)
	if !ok {
		return nil, fmt.Errorf("document id must be a string")
	}

	// Get the bundle name argument
	bundleArg, ok := h.getArgument(field, "bundle", variables)
	if !ok {
		return nil, fmt.Errorf("bundle argument is required")
	}

	bundleName, ok := bundleArg.(string)
	if !ok {
		return nil, fmt.Errorf("bundle name must be a string")
	}

	// Get the fields argument
	fieldsArg, ok := h.getArgument(field, "fields", variables)
	if !ok {
		return nil, fmt.Errorf("fields argument is required")
	}

	fieldsStr, ok := fieldsArg.(string)
	if !ok {
		return nil, fmt.Errorf("fields must be a string")
	}

	h.logger.Infof("Updating document: %s.%s", bundleName, documentID)

	// Parse the fields JSON
	var fields map[string]interface{}
	if err := json.Unmarshal([]byte(fieldsStr), &fields); err != nil {
		return nil, fmt.Errorf("invalid fields JSON: %w", err)
	}

	// Build the SyndrDB command
	command := fmt.Sprintf("UPDATE %s SET %s WHERE id = '%s'", bundleName, fieldsStr, documentID)

	// Execute the command using the command director
	startTime := time.Now()
	response, err := server.CommandDirector(context.Background(), h.database, h.serviceManager, command, h.logger, startTime, h.currentSession, h.currentClientIP)
	if err != nil {
		return nil, fmt.Errorf("failed to update document: %w", err)
	}

	if response == nil {
		return nil, fmt.Errorf("document update failed: no response")
	}

	document := DocumentResponse{
		ID:        documentID,
		Bundle:    bundleName,
		Fields:    fields,
		CreatedAt: time.Now(), // In a real implementation, this would be preserved
		UpdatedAt: time.Now(),
	}

	h.logger.Infof("Updated document: %s.%s", bundleName, documentID)
	return document, nil
}

// mutateDeleteDocument resolves the "deleteDocument" mutation field
// mutateDeleteDocument deletes a document from a bundle
//
// TODO: I will reimplement this mutation using the native mutation pattern when
// implementing Phase 6 mutation support. The native pattern will:
// - Parse GraphQL mutations into unified DELETE commands
// - Support bulk delete operations with WHERE clause filtering
// - Handle cascading deletes when implementing relationship support
// - Execute through the same service layer as native SyndrDB DELETE DOCUMENT
// - Ensure identical performance to SyndrDB's native delete operations
//
// Current implementation works but is part of the pre-Phase 6 GraphQL system.
func (h *GraphQLHandler) mutateDeleteDocument(field *ast.Field, variables map[string]interface{}) (interface{}, error) {
	// Get the bundle name argument
	bundleArg, ok := h.getArgument(field, "bundle", variables)
	if !ok {
		return nil, fmt.Errorf("bundle argument is required")
	}

	bundleName, ok := bundleArg.(string)
	if !ok {
		return nil, fmt.Errorf("bundle name must be a string")
	}

	// Get the where clause argument
	whereArg, ok := h.getArgument(field, "where", variables)
	if !ok {
		return nil, fmt.Errorf("where argument is required")
	}

	whereClause, ok := whereArg.(string)
	if !ok {
		return nil, fmt.Errorf("where clause must be a string")
	}

	h.logger.Infof("Deleting documents from bundle: %s where %s", bundleName, whereClause)

	// Build the SyndrDB command
	command := fmt.Sprintf("DELETE FROM %s WHERE %s", bundleName, whereClause)

	// Execute the command using the command director
	startTime := time.Now()
	response, err := server.CommandDirector(context.Background(), h.database, h.serviceManager, command, h.logger, startTime, h.currentSession, h.currentClientIP)
	if err != nil {
		return nil, fmt.Errorf("failed to delete document: %w", err)
	}

	if response == nil {
		return nil, fmt.Errorf("document deletion failed: no response")
	}

	// Return the number of deleted documents
	deletedCount := 1 // In a real implementation, this would come from the response

	h.logger.Infof("Deleted %d documents from bundle: %s", deletedCount, bundleName)
	return deletedCount, nil
}
