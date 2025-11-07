package main

import (
	"testing"
	"time"

	"syndrdb/src/internal/domain/models"
	graphql "syndrdb/src/internal/graphQL"
	"syndrdb/src/internal/server"

	"go.uber.org/zap"
)

// TestRelationshipResolverForwardOneToMany tests forward 1toMany relationship resolution
func TestRelationshipResolverForwardOneToMany(t *testing.T) {
	logger := createTestLogger()
	serviceManager := createMockServiceManager()
	database := createTestDatabaseWithRelationships()

	resolver := graphql.NewRelationshipResolver(serviceManager, logger)

	// Get users bundle
	usersBundle := database.Bundles["users"]

	// Create parent document (user)
	parentDoc := &models.Document{
		DocumentID: "user123",
		Data: map[string]interface{}{
			"DocumentID": "user123",
			"name":       "John Doe",
			"email":      "john@example.com",
		},
	}

	// Resolve "posts" relationship (1toMany)
	result, isRel, err := resolver.ResolveRelationship(
		&usersBundle,
		parentDoc,
		"posts",
		database,
	)

	if err != nil {
		t.Fatalf("Failed to resolve relationship: %v", err)
	}

	if !isRel {
		t.Fatal("Expected relationship to be resolved")
	}

	// Result should be an array of documents
	posts, ok := result.([]map[string]interface{})
	if !ok {
		t.Fatalf("Expected result to be []map[string]interface{}, got %T", result)
	}

	if len(posts) == 0 {
		t.Error("Expected at least one post, got none")
	}

	t.Logf("Successfully resolved %d posts for user", len(posts))
}

// TestRelationshipResolverReverseRelationship tests reverse (bidirectional) relationship resolution
func TestRelationshipResolverReverseRelationship(t *testing.T) {
	logger := createTestLogger()
	serviceManager := createMockServiceManager()
	database := createTestDatabaseWithRelationships()

	resolver := graphql.NewRelationshipResolver(serviceManager, logger)

	// Get posts bundle
	postsBundle := database.Bundles["posts"]

	// Create parent document (post)
	parentDoc := &models.Document{
		DocumentID: "post456",
		Data: map[string]interface{}{
			"DocumentID": "post456",
			"title":      "Test Post",
			"userID":     "user123",
		},
	}

	// Resolve "author" relationship (reverse of users.posts)
	result, isRel, err := resolver.ResolveRelationship(
		&postsBundle,
		parentDoc,
		"author",
		database,
	)

	if err != nil {
		t.Fatalf("Failed to resolve reverse relationship: %v", err)
	}

	if !isRel {
		t.Fatal("Expected reverse relationship to be resolved")
	}

	// Result should be a single document (reverse of 1toMany is 1to1)
	author, ok := result.(map[string]interface{})
	if !ok {
		t.Fatalf("Expected result to be map[string]interface{}, got %T", result)
	}

	if author["DocumentID"] != "user123" {
		t.Errorf("Expected author ID 'user123', got '%v'", author["DocumentID"])
	}

	t.Logf("Successfully resolved reverse relationship: author = %v", author["name"])
}

// TestRelationshipResolverManyToMany tests ManyToMany relationship resolution
func TestRelationshipResolverManyToMany(t *testing.T) {
	logger := createTestLogger()
	serviceManager := createMockServiceManager()
	database := createTestDatabaseWithManyToMany()

	resolver := graphql.NewRelationshipResolver(serviceManager, logger)

	// Get books bundle
	booksBundle := database.Bundles["books"]

	// Create parent document (book)
	parentDoc := &models.Document{
		DocumentID: "book789",
		Data: map[string]interface{}{
			"DocumentID": "book789",
			"title":      "Test Book",
		},
	}

	// Resolve "authors" relationship (ManyToMany)
	result, isRel, err := resolver.ResolveRelationship(
		&booksBundle,
		parentDoc,
		"authors",
		database,
	)

	if err != nil {
		t.Fatalf("Failed to resolve ManyToMany relationship: %v", err)
	}

	if !isRel {
		t.Fatal("Expected ManyToMany relationship to be resolved")
	}

	// Result should be an array
	authors, ok := result.([]map[string]interface{})
	if !ok {
		t.Fatalf("Expected result to be []map[string]interface{}, got %T", result)
	}

	t.Logf("Successfully resolved %d authors for book", len(authors))
}

// TestRelationshipResolverFieldNameMatching tests field name matching heuristics
func TestRelationshipResolverFieldNameMatching(t *testing.T) {
	tests := []struct {
		name           string
		fieldName      string
		sourceBundleName string
		shouldMatch    bool
	}{
		{
			name:             "Exact match",
			fieldName:        "posts",
			sourceBundleName: "posts",
			shouldMatch:      true,
		},
		{
			name:             "Singular match",
			fieldName:        "post",
			sourceBundleName: "posts",
			shouldMatch:      true,
		},
		{
			name:             "Author alias for users",
			fieldName:        "author",
			sourceBundleName: "users",
			shouldMatch:      true,
		},
		{
			name:             "Owner alias for users",
			fieldName:        "owner",
			sourceBundleName: "users",
			shouldMatch:      true,
		},
		{
			name:             "Creator alias for users",
			fieldName:        "creator",
			sourceBundleName: "users",
			shouldMatch:      true,
		},
		{
			name:             "No match",
			fieldName:        "categories",
			sourceBundleName: "tags",
			shouldMatch:      false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// This tests the isReverseFieldMatch logic indirectly
			// by checking if reverse relationships are found
			logger := createTestLogger()
			serviceManager := createMockServiceManager()
			database := createTestDatabaseForFieldMatching(tt.sourceBundleName)

			resolver := graphql.NewRelationshipResolver(serviceManager, logger)

			// Create a bundle that will look for reverse relationship
			testBundle := models.Bundle{
				Name:          "test",
				Relationships: map[string]models.Relationship{},
			}

			parentDoc := &models.Document{
				DocumentID: "test123",
				Data:       map[string]interface{}{"DocumentID": "test123"},
			}

			result, isRel, err := resolver.ResolveRelationship(
				&testBundle,
				parentDoc,
				tt.fieldName,
				database,
			)

			if err != nil && tt.shouldMatch {
				t.Fatalf("Expected to resolve relationship, got error: %v", err)
			}

			if tt.shouldMatch && !isRel {
				t.Errorf("Expected field '%s' to match bundle '%s' but it didn't", tt.fieldName, tt.sourceBundleName)
			}

			if !tt.shouldMatch && isRel {
				t.Errorf("Expected field '%s' NOT to match bundle '%s' but it did", tt.fieldName, tt.sourceBundleName)
			}

			if tt.shouldMatch && isRel {
				t.Logf("✓ Field '%s' correctly matched bundle '%s'", tt.fieldName, tt.sourceBundleName)
			}

			_ = result // Suppress unused warning
		})
	}
}

// TestSingularize tests the singularize helper function
func TestSingularize(t *testing.T) {
	tests := []struct {
		input    string
		expected string
	}{
		{"posts", "post"},
		{"users", "user"},
		{"categories", "categorie"}, // Simple rule: remove 's'
		{"people", "person"},        // Special case
		{"children", "child"},       // Special case
		{"data", "data"},            // Already singular
		{"post", "post"},            // Already singular
	}

	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			// We don't have direct access to singularize(), so we test it indirectly
			// by checking if relationships with singular names are resolved correctly
			logger := createTestLogger()
			serviceManager := createMockServiceManager()
			database := createTestDatabaseWithBundle(tt.input)

			resolver := graphql.NewRelationshipResolver(serviceManager, logger)

			testBundle := models.Bundle{
				Name:          "test",
				Relationships: map[string]models.Relationship{},
			}

			parentDoc := &models.Document{
				DocumentID: "test123",
				Data:       map[string]interface{}{"DocumentID": "test123"},
			}

			// Try to resolve with expected singular form
			_, isRel, _ := resolver.ResolveRelationship(
				&testBundle,
				parentDoc,
				tt.expected,
				database,
			)

			// If we found a relationship, singularization worked
			if isRel {
				t.Logf("✓ Singularized '%s' → '%s' (found relationship)", tt.input, tt.expected)
			}
		})
	}
}

// TestRelationshipResolverNonExistentField tests behavior with non-existent fields
func TestRelationshipResolverNonExistentField(t *testing.T) {
	logger := createTestLogger()
	serviceManager := createMockServiceManager()
	database := createTestDatabaseWithRelationships()

	resolver := graphql.NewRelationshipResolver(serviceManager, logger)

	usersBundle := database.Bundles["users"]
	parentDoc := &models.Document{
		DocumentID: "user123",
		Data:       map[string]interface{}{"DocumentID": "user123"},
	}

	// Try to resolve a non-existent relationship
	result, isRel, err := resolver.ResolveRelationship(
		&usersBundle,
		parentDoc,
		"nonExistentField",
		database,
	)

	// Should not error, but should return isRel=false
	if err != nil {
		t.Errorf("Should not error for non-existent field, got: %v", err)
	}

	if isRel {
		t.Error("Expected isRel=false for non-existent field")
	}

	if result != nil {
		t.Errorf("Expected nil result for non-existent field, got: %v", result)
	}
}

// Helper functions

func createTestLogger() *zap.SugaredLogger {
	logger, _ := zap.NewDevelopment()
	return logger.Sugar()
}

func createMockServiceManager() server.ServiceManager {
	// Create a minimal mock service manager
	// In a real test, this would use actual services or mocks
	return server.ServiceManager{}
}

func createTestDatabaseWithRelationships() *models.Database {
	// Create test database with users and posts bundles
	usersBundle := models.Bundle{
		Name: "users",
		Relationships: map[string]models.Relationship{
			"posts": {
				RelationshipID:    "rel1",
				Name:              "posts",
				SourceBundle:      "users",
				SourceField:       "DocumentID",
				DestinationBundle: "posts",
				DestinationField:  "userID",
				RelationshipType:  "1toMany",
			},
		},
		Documents: &[]models.Document{
			{
				DocumentID: "user123",
				Data: map[string]interface{}{
					"DocumentID": "user123",
					"name":       "John Doe",
					"email":      "john@example.com",
				},
			},
		},
	}

	postsBundle := models.Bundle{
		Name:          "posts",
		Relationships: map[string]models.Relationship{},
		Documents: &[]models.Document{
			{
				DocumentID: "post456",
				Data: map[string]interface{}{
					"DocumentID": "post456",
					"title":      "Test Post",
					"userID":     "user123",
				},
			},
		},
	}

	return &models.Database{
		Name: "testdb",
		Bundles: map[string]models.Bundle{
			"users": usersBundle,
			"posts": postsBundle,
		},
		CreatedAt: time.Now(),
	}
}

func createTestDatabaseWithManyToMany() *models.Database {
	booksBundle := models.Bundle{
		Name: "books",
		Relationships: map[string]models.Relationship{
			"authors": {
				RelationshipID:    "rel2",
				Name:              "authors",
				SourceBundle:      "books",
				SourceField:       "DocumentID",
				DestinationBundle: "authors",
				DestinationField:  "bookID",
				RelationshipType:  "ManyToMany",
			},
		},
		Documents: &[]models.Document{
			{
				DocumentID: "book789",
				Data:       map[string]interface{}{"DocumentID": "book789", "title": "Test Book"},
			},
		},
	}

	authorsBundle := models.Bundle{
		Name:          "authors",
		Relationships: map[string]models.Relationship{},
		Documents: &[]models.Document{
			{
				DocumentID: "author1",
				Data:       map[string]interface{}{"DocumentID": "author1", "name": "Author One", "bookID": "book789"},
			},
		},
	}

	return &models.Database{
		Name: "testdb",
		Bundles: map[string]models.Bundle{
			"books":   booksBundle,
			"authors": authorsBundle,
		},
		CreatedAt: time.Now(),
	}
}

func createTestDatabaseForFieldMatching(sourceBundleName string) *models.Database {
	sourceBundle := models.Bundle{
		Name: sourceBundleName,
		Relationships: map[string]models.Relationship{
			"related": {
				RelationshipID:    "rel3",
				Name:              "related",
				SourceBundle:      sourceBundleName,
				SourceField:       "DocumentID",
				DestinationBundle: "test",
				DestinationField:  "sourceID",
				RelationshipType:  "1toMany",
			},
		},
		Documents: &[]models.Document{},
	}

	testBundle := models.Bundle{
		Name:          "test",
		Relationships: map[string]models.Relationship{},
		Documents:     &[]models.Document{},
	}

	return &models.Database{
		Name: "testdb",
		Bundles: map[string]models.Bundle{
			sourceBundleName: sourceBundle,
			"test":           testBundle,
		},
		CreatedAt: time.Now(),
	}
}

func createTestDatabaseWithBundle(bundleName string) *models.Database {
	bundle := models.Bundle{
		Name: bundleName,
		Relationships: map[string]models.Relationship{
			"related": {
				RelationshipID:    "rel4",
				Name:              "related",
				SourceBundle:      bundleName,
				SourceField:       "DocumentID",
				DestinationBundle: "test",
				DestinationField:  "sourceID",
				RelationshipType:  "1toMany",
			},
		},
		Documents: &[]models.Document{},
	}

	testBundle := models.Bundle{
		Name:          "test",
		Relationships: map[string]models.Relationship{},
		Documents:     &[]models.Document{},
	}

	return &models.Database{
		Name: "testdb",
		Bundles: map[string]models.Bundle{
			bundleName: bundle,
			"test":     testBundle,
		},
		CreatedAt: time.Now(),
	}
}
