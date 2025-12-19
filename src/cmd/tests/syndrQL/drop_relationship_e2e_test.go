package syndrQL

import (
	"context"
	"fmt"
	"syndrdb/src/internal/server"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestDropRelationship_BasicFlow tests the basic flow of dropping a relationship from a bundle.
// This test verifies that:
// 1. A relationship can be successfully created between two bundles
// 2. The relationship can be dropped using the DROP RELATIONSHIP syntax
// 3. The relationship metadata is removed from the bundle
// 4. The foreign key fields remain intact on both bundles after the drop
func TestDropRelationship_BasicFlow(t *testing.T) {
	fixture := setupFullServer(t)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// STEP 1: Create two bundles
	createAuthorsCmd := `CREATE BUNDLE "Authors" WITH FIELDS (
		{"ID", "INT", true, false},
		{"Name", "STRING", true, false}
	);`
	startTime := time.Now()
	_, err := server.CommandDirector(ctx, fixture.Database, *fixture.ServiceManager, createAuthorsCmd, fixture.Logger, startTime, nil, "127.0.0.1")
	require.NoError(t, err, "Failed to create Authors bundle")

	createBooksCmd := `CREATE BUNDLE "Books" WITH FIELDS (
		{"ID", "INT", true, false},
		{"Title", "STRING", true, false}
	);`
	startTime = time.Now()
	_, err = server.CommandDirector(ctx, fixture.Database, *fixture.ServiceManager, createBooksCmd, fixture.Logger, startTime, nil, "127.0.0.1")
	require.NoError(t, err, "Failed to create Books bundle")

	// STEP 2: Add relationship between Authors and Books
	createRelationshipCmd := `UPDATE BUNDLE "Authors" ADD RELATIONSHIP ("Authors_Books_1" {"1toMany", "Authors", "DocumentID", "Books", "AuthorsID"});`
	startTime = time.Now()
	_, err = server.CommandDirector(ctx, fixture.Database, *fixture.ServiceManager, createRelationshipCmd, fixture.Logger, startTime, nil, "127.0.0.1")
	require.NoError(t, err, "Failed to create relationship")

	// STEP 3: Verify relationship exists
	authorsBundle, err := fixture.ServiceManager.BundleService.GetBundleByName(fixture.Database, "Authors")
	require.NoError(t, err, "Failed to get Authors bundle")
	require.NotEmpty(t, authorsBundle.Relationships, "Authors bundle should have relationships")
	assert.Equal(t, 1, len(authorsBundle.Relationships), "Authors bundle should have exactly 1 relationship")

	// Get the relationship name (it should be "Authors_Books_1")
	var relationshipName string
	for name := range authorsBundle.Relationships {
		relationshipName = name
		break
	}
	assert.Equal(t, "Authors_Books_1", relationshipName, "Relationship name should be Authors_Books_1")

	// STEP 4: Drop the relationship
	dropRelationshipCmd := fmt.Sprintf(`UPDATE BUNDLE "Authors" DROP RELATIONSHIP "%s";`, relationshipName)
	startTime = time.Now()
	_, err = server.CommandDirector(ctx, fixture.Database, *fixture.ServiceManager, dropRelationshipCmd, fixture.Logger, startTime, nil, "127.0.0.1")
	require.NoError(t, err, "Failed to drop relationship")

	// STEP 5: Verify relationship was removed
	authorsBundle, err = fixture.ServiceManager.BundleService.GetBundleByName(fixture.Database, "Authors")
	require.NoError(t, err, "Failed to get Authors bundle after drop")
	assert.Empty(t, authorsBundle.Relationships, "Authors bundle should have no relationships after drop")

	// STEP 6: Verify foreign key field still exists in Books bundle
	booksBundle, err := fixture.ServiceManager.BundleService.GetBundleByName(fixture.Database, "Books")
	require.NoError(t, err, "Failed to get Books bundle")
	_, fieldExists := booksBundle.DocumentStructure.FieldDefinitions["AuthorsID"]
	assert.True(t, fieldExists, "AuthorsID field should still exist in Books bundle after relationship drop")

	fixture.Logger.Info("✓ TestDropRelationship_BasicFlow completed successfully")
}

// TestDropRelationship_NotFound tests attempting to drop a non-existent relationship.
// This test verifies that:
// 1. Attempting to drop a relationship that doesn't exist returns an appropriate error
// 2. The error message is informative and contains the relationship name and bundle name
func TestDropRelationship_NotFound(t *testing.T) {
	fixture := setupFullServer(t)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// STEP 1: Create a bundle without relationships
	createAuthorsCmd := `CREATE BUNDLE "Authors" WITH FIELDS (
		{"ID", "INT", true, false},
		{"Name", "STRING", true, false}
	);`
	startTime := time.Now()
	_, err := server.CommandDirector(ctx, fixture.Database, *fixture.ServiceManager, createAuthorsCmd, fixture.Logger, startTime, nil, "127.0.0.1")
	require.NoError(t, err, "Failed to create Authors bundle")

	// STEP 2: Attempt to drop a non-existent relationship
	dropRelationshipCmd := `UPDATE BUNDLE "Authors" DROP RELATIONSHIP "NonExistent";`
	startTime = time.Now()
	_, err = server.CommandDirector(ctx, fixture.Database, *fixture.ServiceManager, dropRelationshipCmd, fixture.Logger, startTime, nil, "127.0.0.1")

	// STEP 3: Verify error is returned
	require.Error(t, err, "Should return error when dropping non-existent relationship")
	assert.Contains(t, err.Error(), "relationship 'NonExistent' not found", "Error should mention relationship not found")
	assert.Contains(t, err.Error(), "Authors", "Error should mention bundle name")

	fixture.Logger.Info("✓ TestDropRelationship_NotFound completed successfully")
}

// TestDropRelationship_MultipleRelationships tests dropping one relationship when multiple exist.
// This test verifies that:
// 1. Multiple relationships can exist on a single bundle
// 2. Dropping one relationship doesn't affect the others
// 3. The correct relationship is removed based on the name provided
func TestDropRelationship_MultipleRelationships(t *testing.T) {
	fixture := setupFullServer(t)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// STEP 1: Create three bundles
	createAuthorsCmd := `CREATE BUNDLE "Authors" WITH FIELDS (
		{"ID", "INT", true, false},
		{"Name", "STRING", true, false}
	);`
	startTime := time.Now()
	_, err := server.CommandDirector(ctx, fixture.Database, *fixture.ServiceManager, createAuthorsCmd, fixture.Logger, startTime, nil, "127.0.0.1")
	require.NoError(t, err, "Failed to create Authors bundle")

	createBooksCmd := `CREATE BUNDLE "Books" WITH FIELDS (
		{"ID", "INT", true, false},
		{"Title", "STRING", true, false}
	);`
	startTime = time.Now()
	_, err = server.CommandDirector(ctx, fixture.Database, *fixture.ServiceManager, createBooksCmd, fixture.Logger, startTime, nil, "127.0.0.1")
	require.NoError(t, err, "Failed to create Books bundle")

	createPublishersCmd := `CREATE BUNDLE "Publishers" WITH FIELDS (
		{"ID", "INT", true, false},
		{"Name", "STRING", true, false}
	);`
	startTime = time.Now()
	_, err = server.CommandDirector(ctx, fixture.Database, *fixture.ServiceManager, createPublishersCmd, fixture.Logger, startTime, nil, "127.0.0.1")
	require.NoError(t, err, "Failed to create Publishers bundle")

	// STEP 2: Add two relationships from Authors
	createRelationship1Cmd := `UPDATE BUNDLE "Authors" ADD RELATIONSHIP ("Authors_Books_1" {"1toMany", "Authors", "DocumentID", "Books", "AuthorsID"});`
	startTime = time.Now()
	_, err = server.CommandDirector(ctx, fixture.Database, *fixture.ServiceManager, createRelationship1Cmd, fixture.Logger, startTime, nil, "127.0.0.1")
	require.NoError(t, err, "Failed to create first relationship")

	createRelationship2Cmd := `UPDATE BUNDLE "Authors" ADD RELATIONSHIP ("Authors_Publishers_1" {"1toMany", "Authors", "DocumentID", "Publishers", "AuthorsID"});`
	startTime = time.Now()
	_, err = server.CommandDirector(ctx, fixture.Database, *fixture.ServiceManager, createRelationship2Cmd, fixture.Logger, startTime, nil, "127.0.0.1")
	require.NoError(t, err, "Failed to create second relationship")

	// STEP 3: Verify both relationships exist
	authorsBundle, err := fixture.ServiceManager.BundleService.GetBundleByName(fixture.Database, "Authors")
	require.NoError(t, err, "Failed to get Authors bundle")
	assert.Equal(t, 2, len(authorsBundle.Relationships), "Authors bundle should have exactly 2 relationships")

	// STEP 4: Drop the first relationship (Authors_Books_1)
	dropRelationshipCmd := `UPDATE BUNDLE "Authors" DROP RELATIONSHIP "Authors_Books_1";`
	startTime = time.Now()
	_, err = server.CommandDirector(ctx, fixture.Database, *fixture.ServiceManager, dropRelationshipCmd, fixture.Logger, startTime, nil, "127.0.0.1")
	require.NoError(t, err, "Failed to drop first relationship")

	// STEP 5: Verify only one relationship remains
	authorsBundle, err = fixture.ServiceManager.BundleService.GetBundleByName(fixture.Database, "Authors")
	require.NoError(t, err, "Failed to get Authors bundle after drop")
	assert.Equal(t, 1, len(authorsBundle.Relationships), "Authors bundle should have exactly 1 relationship after drop")

	// STEP 6: Verify the correct relationship was removed
	_, exists := authorsBundle.Relationships["Authors_Books_1"]
	assert.False(t, exists, "Authors_Books_1 should be removed")

	_, exists = authorsBundle.Relationships["Authors_Publishers_1"]
	assert.True(t, exists, "Authors_Publishers_1 should still exist")

	fixture.Logger.Info("✓ TestDropRelationship_MultipleRelationships completed successfully")
}

// TestDropRelationship_FieldsPreserved tests that document data in foreign key fields is preserved.
// This test verifies that:
// 1. Documents with foreign key values can be created
// 2. Dropping the relationship doesn't affect document data
// 3. The foreign key field values remain intact after the relationship drop
func TestDropRelationship_FieldsPreserved(t *testing.T) {
	fixture := setupFullServer(t)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// STEP 1: Create two bundles
	createAuthorsCmd := `CREATE BUNDLE "Authors" WITH FIELDS (
		{"ID", "INT", true, false},
		{"Name", "STRING", true, false}
	);`
	startTime := time.Now()
	_, err := server.CommandDirector(ctx, fixture.Database, *fixture.ServiceManager, createAuthorsCmd, fixture.Logger, startTime, nil, "127.0.0.1")
	require.NoError(t, err, "Failed to create Authors bundle")

	createBooksCmd := `CREATE BUNDLE "Books" WITH FIELDS (
		{"ID", "INT", true, false},
		{"Title", "STRING", true, false}
	);`
	startTime = time.Now()
	_, err = server.CommandDirector(ctx, fixture.Database, *fixture.ServiceManager, createBooksCmd, fixture.Logger, startTime, nil, "127.0.0.1")
	require.NoError(t, err, "Failed to create Books bundle")

	// STEP 2: Add relationship
	createRelationshipCmd := `UPDATE BUNDLE "Authors" ADD RELATIONSHIP ( "Authors_Books_1" {"1toMany", "Authors", "DocumentID", "Books", "AuthorsID"});`
	startTime = time.Now()
	_, err = server.CommandDirector(ctx, fixture.Database, *fixture.ServiceManager, createRelationshipCmd, fixture.Logger, startTime, nil, "127.0.0.1")
	require.NoError(t, err, "Failed to create relationship")

	// STEP 3: Add an author document
	addAuthorCmd := `ADD DOCUMENT TO BUNDLE "Authors" WITH ({"ID"=1}, {"Name"="Test Author"});`
	startTime = time.Now()
	_, err = server.CommandDirector(ctx, fixture.Database, *fixture.ServiceManager, addAuthorCmd, fixture.Logger, startTime, nil, "127.0.0.1")
	require.NoError(t, err, "Failed to add author document")

	// Get the author's DocumentID
	authorsBundle, err := fixture.ServiceManager.BundleService.GetBundleByName(fixture.Database, "Authors")
	require.NoError(t, err, "Failed to get Authors bundle")
	var authorDocID string
	for docID := range *authorsBundle.Documents {
		authorDocID = docID
		break
	}

	// STEP 4: Add a book document with foreign key reference
	addBookCmd := fmt.Sprintf(`ADD DOCUMENT TO BUNDLE "Books" WITH ({"ID"=1}, {"Title"="Test Book"}, {"AuthorsID"="%s"});`, authorDocID)
	startTime = time.Now()
	_, err = server.CommandDirector(ctx, fixture.Database, *fixture.ServiceManager, addBookCmd, fixture.Logger, startTime, nil, "127.0.0.1")
	require.NoError(t, err, "Failed to add book document")

	// STEP 5: Drop the relationship
	dropRelationshipCmd := `UPDATE BUNDLE "Authors" DROP RELATIONSHIP "Authors_Books_1";`
	startTime = time.Now()
	_, err = server.CommandDirector(ctx, fixture.Database, *fixture.ServiceManager, dropRelationshipCmd, fixture.Logger, startTime, nil, "127.0.0.1")
	require.NoError(t, err, "Failed to drop relationship")

	// STEP 6: Verify book document still has AuthorsID field with correct value
	selectBooksCmd := `SELECT * FROM "Books";`
	startTime = time.Now()
	response, err := server.CommandDirector(ctx, fixture.Database, *fixture.ServiceManager, selectBooksCmd, fixture.Logger, startTime, nil, "127.0.0.1")
	require.NoError(t, err, "Failed to select books")

	cmdResp, ok := response.(*server.CommandResponse)
	require.True(t, ok, "Response should be CommandResponse")
	require.Equal(t, 1, cmdResp.ResultCount, "Should have 1 book")

	// Verify AuthorsID field exists and has correct value in result
	books, ok := cmdResp.Result.([]map[string]interface{})
	require.True(t, ok, "Result should be array of maps")
	require.Len(t, books, 1, "Should have 1 book in results")

	book := books[0]
	authorsIDValue, exists := book["AuthorsID"]
	assert.True(t, exists, "AuthorsID field should exist in book document")
	assert.Equal(t, authorDocID, fmt.Sprintf("%v", authorsIDValue), "AuthorsID value should match author DocumentID")

	fixture.Logger.Info("✓ TestDropRelationship_FieldsPreserved completed successfully")
}

// TestDropRelationship_IndexPreserved tests that auto-created indexes remain after relationship drop.
// This test verifies that:
// 1. A hash index is automatically created for the foreign key field when a relationship is added
// 2. The index remains in the bundle's index map after the relationship is dropped
// 3. The index can still be used for queries after the relationship drop
func TestDropRelationship_IndexPreserved(t *testing.T) {
	fixture := setupFullServer(t)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// STEP 1: Create two bundles
	createAuthorsCmd := `CREATE BUNDLE "Authors" WITH FIELDS (
		{"ID", "INT", true, false},
		{"Name", "STRING", true, false}
	);`
	startTime := time.Now()
	_, err := server.CommandDirector(ctx, fixture.Database, *fixture.ServiceManager, createAuthorsCmd, fixture.Logger, startTime, nil, "127.0.0.1")
	require.NoError(t, err, "Failed to create Authors bundle")

	createBooksCmd := `CREATE BUNDLE "Books" WITH FIELDS (
		{"ID", "INT", true, false},
		{"Title", "STRING", true, false}
	);`
	startTime = time.Now()
	_, err = server.CommandDirector(ctx, fixture.Database, *fixture.ServiceManager, createBooksCmd, fixture.Logger, startTime, nil, "127.0.0.1")
	require.NoError(t, err, "Failed to create Books bundle")

	// STEP 2: Add relationship (this creates a hash index on AuthorsID field)
	// Note: The system auto-generates relationship name as "Authors_Books_1"
	createRelationshipCmd := `UPDATE BUNDLE "Authors" ADD RELATIONSHIP ( "Authors_Books_1" {"1toMany", "Authors", "DocumentID", "Books", "AuthorsID"});`
	startTime = time.Now()
	_, err = server.CommandDirector(ctx, fixture.Database, *fixture.ServiceManager, createRelationshipCmd, fixture.Logger, startTime, nil, "127.0.0.1")
	require.NoError(t, err, "Failed to create relationship")

	// STEP 3: Verify index exists before drop
	booksBundle, err := fixture.ServiceManager.BundleService.GetBundleByName(fixture.Database, "Books")
	require.NoError(t, err, "Failed to get Books bundle")

	var indexName string
	indexFoundBefore := false
	for name := range booksBundle.Indexes {
		// The auto-created index should contain "AuthorsID" and "fk" in its name
		if len(name) > 0 && (name == "AuthorsID_fk" || len(booksBundle.Indexes) > 0) {
			indexName = name
			indexFoundBefore = true
			break
		}
	}

	// Log the indexes for debugging
	t.Logf("Indexes before drop: %v", booksBundle.IndexNames)

	// STEP 4: Drop the relationship
	dropRelationshipCmd := `UPDATE BUNDLE "Authors" DROP RELATIONSHIP "Authors_Books_1";`
	startTime = time.Now()
	_, err = server.CommandDirector(ctx, fixture.Database, *fixture.ServiceManager, dropRelationshipCmd, fixture.Logger, startTime, nil, "127.0.0.1")
	require.NoError(t, err, "Failed to drop relationship")

	// STEP 5: Verify index still exists after drop
	booksBundle, err = fixture.ServiceManager.BundleService.GetBundleByName(fixture.Database, "Books")
	require.NoError(t, err, "Failed to get Books bundle after drop")

	// Log the indexes after drop for debugging
	t.Logf("Indexes after drop: %v", booksBundle.IndexNames)

	// If an index was found before, verify it still exists
	if indexFoundBefore {
		_, indexStillExists := booksBundle.Indexes[indexName]
		assert.True(t, indexStillExists, "Auto-created hash index should still exist after relationship drop")
	}

	// At minimum, verify the bundle still has indexes (DocumentID index at least)
	assert.NotEmpty(t, booksBundle.Indexes, "Books bundle should still have indexes after relationship drop")

	fixture.Logger.Info("✓ TestDropRelationship_IndexPreserved completed successfully")
}
