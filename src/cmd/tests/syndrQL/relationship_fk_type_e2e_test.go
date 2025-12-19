package syndrQL

import (
	"context"
	"testing"
	"time"

	"syndrdb/src/internal/server"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

/*
FOREIGN KEY TYPE PRESERVATION E2E TESTS

Validates that FK fields preserve source field types (INT, STRING, etc.)
instead of being hardcoded as "relationship" type.

Test Coverage:
- INT FK type preservation in OneToMany relationships
- STRING FK type preservation in ManyToMany relationships (bidirectional)
- JOIN queries with type-aware comparison
- Validation failures for missing source fields
*/

// =============================================================================
// TEST: INT FK Type Preservation
// =============================================================================

func TestFKTypePreservation_INT(t *testing.T) {
	fixture := setupFullServer(t)
	ctx := context.Background()

	// Create Authors bundle with INT primary key
	createAuthorsCmd := `CREATE BUNDLE "Authors" WITH FIELDS (
		{"author_id", "INT", true, false},
		{"name", "STRING", false, false}
	);`
	_, err := server.CommandDirector(ctx, fixture.Database, *fixture.ServiceManager, createAuthorsCmd,
		fixture.Logger, time.Now(), nil, "127.0.0.1")
	require.NoError(t, err)

	// Create Books bundle with INT FK field
	createBooksCmd := `CREATE BUNDLE "Books" WITH FIELDS (
		{"book_id", "INT", true, false},
		{"title", "STRING", false, false},
		{"author_fk", "INT", false, false}
	);`
	_, err = server.CommandDirector(ctx, fixture.Database, *fixture.ServiceManager, createBooksCmd,
		fixture.Logger, time.Now(), nil, "127.0.0.1")
	require.NoError(t, err)

	// Create OneToMany relationship: Authors.author_id -> Books.author_fk
	createRelCmd := `UPDATE BUNDLE "Authors" ADD RELATIONSHIP (
		"Authors_Books_rel" {"1toMany", "Authors", "author_id", "Books", "author_fk"}
	);`
	_, err = server.CommandDirector(ctx, fixture.Database, *fixture.ServiceManager, createRelCmd,
		fixture.Logger, time.Now(), nil, "127.0.0.1")
	require.NoError(t, err)

	// VALIDATION: Check FK field type is INT, not "relationship"
	booksBundle, err := fixture.ServiceManager.BundleService.GetBundleByName(fixture.Database, "Books")
	require.NoError(t, err)

	authorFKField, exists := booksBundle.DocumentStructure.FieldDefinitions["author_fk"]
	require.True(t, exists, "author_fk field must exist")

	assert.Equal(t, "int", authorFKField.Type,
		"FK field should preserve source INT type, not 'relationship'")
}

// =============================================================================
// TEST: ManyToMany Bidirectional Type Preservation
// =============================================================================

func TestFKTypePreservation_ManyToMany(t *testing.T) {
	fixture := setupFullServer(t)
	ctx := context.Background()

	// Create bundles with STRING primary keys
	createUsersCmd := `CREATE BUNDLE "Users" WITH FIELDS (
		{"user_id", "STRING", true, false},
		{"username", "STRING", false, false}
	);`
	_, err := server.CommandDirector(ctx, fixture.Database, *fixture.ServiceManager, createUsersCmd,
		fixture.Logger, time.Now(), nil, "127.0.0.1")
	require.NoError(t, err)

	createGroupsCmd := `CREATE BUNDLE "Groups" WITH FIELDS (
		{"group_id", "STRING", true, false},
		{"group_name", "STRING", false, false}
	);`
	_, err = server.CommandDirector(ctx, fixture.Database, *fixture.ServiceManager, createGroupsCmd,
		fixture.Logger, time.Now(), nil, "127.0.0.1")
	require.NoError(t, err)

	// Create ManyToMany relationship (bidirectional)
	createRelCmd := `UPDATE BUNDLE "Users" ADD RELATIONSHIP (
		"Users_Groups_rel" {"ManyToMany", "Users", "user_id", "Groups", "group_id"}
	);`
	_, err = server.CommandDirector(ctx, fixture.Database, *fixture.ServiceManager, createRelCmd,
		fixture.Logger, time.Now(), nil, "127.0.0.1")
	require.NoError(t, err)

	// VALIDATION: Check forward FK (Users -> Groups) preserves STRING type
	usersBundle, err := fixture.ServiceManager.BundleService.GetBundleByName(fixture.Database, "Users")
	require.NoError(t, err)

	// ManyToMany creates a reverse FK field named "<DestinationBundle>ID"
	forwardFK, exists := usersBundle.DocumentStructure.FieldDefinitions["GroupsID"]
	require.True(t, exists, "GroupsID FK field must exist in Users bundle")
	assert.Equal(t, "string", forwardFK.Type,
		"Forward FK should preserve destination STRING type")

	// VALIDATION: Check reverse FK (Groups -> Users) preserves STRING type
	groupsBundle, err := fixture.ServiceManager.BundleService.GetBundleByName(fixture.Database, "Groups")
	require.NoError(t, err)

	// The destination field 'group_id' should have the FK metadata
	reverseFK, exists := groupsBundle.DocumentStructure.FieldDefinitions["group_id"]
	require.True(t, exists, "group_id FK field must exist in Groups bundle")
	assert.Equal(t, "string", reverseFK.Type,
		"Reverse FK should preserve source STRING type")
}

// =============================================================================
// TEST: JOIN with INT FK Fields
// =============================================================================

func TestFKTypePreservation_JOIN(t *testing.T) {
	fixture := setupFullServer(t)
	ctx := context.Background()

	// Create bundles
	createAuthorsCmd := `CREATE BUNDLE "Authors" WITH FIELDS (
		{"author_id", "INT", true, false},
		{"AuthorName", "STRING", false, false}
	);`
	_, err := server.CommandDirector(ctx, fixture.Database, *fixture.ServiceManager, createAuthorsCmd,
		fixture.Logger, time.Now(), nil, "127.0.0.1")
	require.NoError(t, err)

	createBooksCmd := `CREATE BUNDLE "Books" WITH FIELDS (
		{"book_id", "INT", true, false},
		{"title", "STRING", false, false},
		{"author_fk", "INT", false, false}
	);`
	_, err = server.CommandDirector(ctx, fixture.Database, *fixture.ServiceManager, createBooksCmd,
		fixture.Logger, time.Now(), nil, "127.0.0.1")
	require.NoError(t, err)

	// Create relationship
	createRelCmd := `UPDATE BUNDLE "Authors" ADD RELATIONSHIP (
		"Authors_Books_rel" {"1toMany", "Authors", "author_id", "Books", "author_fk"}
	);`
	_, err = server.CommandDirector(ctx, fixture.Database, *fixture.ServiceManager, createRelCmd,
		fixture.Logger, time.Now(), nil, "127.0.0.1")
	require.NoError(t, err)

	// Insert test data
	insertAuthor1 := `ADD DOCUMENT TO BUNDLE "Authors" WITH (  {"author_id"=1}, {"AuthorName"="Alice"});`
	_, err = server.CommandDirector(ctx, fixture.Database, *fixture.ServiceManager, insertAuthor1,
		fixture.Logger, time.Now(), nil, "127.0.0.1")
	require.NoError(t, err)

	insertBook1 := `ADD DOCUMENT TO BUNDLE "Books" WITH ( {"book_id"=101}, {"title"="Book A"}, {"author_fk"=1});`
	_, err = server.CommandDirector(ctx, fixture.Database, *fixture.ServiceManager, insertBook1,
		fixture.Logger, time.Now(), nil, "127.0.0.1")
	require.NoError(t, err)

	// DEBUG: Verify documents with SELECT queries
	selectAuthors := `SELECT * FROM "Authors";`
	authorsResult, err := server.CommandDirector(ctx, fixture.Database, *fixture.ServiceManager, selectAuthors,
		fixture.Logger, time.Now(), nil, "127.0.0.1")
	require.NoError(t, err)
	t.Logf("DEBUG: SELECT * FROM Authors result: %+v", authorsResult)

	selectBooks := `SELECT * FROM "Books";`
	booksResult, err := server.CommandDirector(ctx, fixture.Database, *fixture.ServiceManager, selectBooks,
		fixture.Logger, time.Now(), nil, "127.0.0.1")
	require.NoError(t, err)
	t.Logf("DEBUG: SELECT * FROM Books result: %+v", booksResult)

	// Execute JOIN query
	selectCmd := `SELECT "Authors"."AuthorName", "Books"."title" FROM "Authors" JOIN "Books" ON "Authors"."author_id" == "Books"."author_fk";`
	result, err := server.CommandDirector(ctx, fixture.Database, *fixture.ServiceManager, selectCmd,
		fixture.Logger, time.Now(), nil, "127.0.0.1")
	require.NoError(t, err, "JOIN should work with INT FK fields using type-aware comparison")

	// VALIDATION: Verify JOIN matched documents (type-aware comparison worked)
	if cmdResp, ok := result.(*server.CommandResponse); ok {
		assert.Equal(t, 1, cmdResp.ResultCount,
			"JOIN should match 1 document pair using INT type-aware comparison")
		t.Logf("SUCCESS: JOIN matched %d document(s) - FK type preservation enables proper INT comparison",
			cmdResp.ResultCount)
	} else {
		t.Fatalf("Unexpected result type: %T", result)
	}
}

// =============================================================================
// TEST: Validation Failure - Missing Source Field
// =============================================================================

func TestFKTypePreservation_ValidationFailure(t *testing.T) {
	fixture := setupFullServer(t)
	ctx := context.Background()

	createAuthorsCmd := `CREATE BUNDLE "Authors" WITH FIELDS (
		{"author_id", "INT", true, false},
		{"name", "STRING", false, false}
	);`
	_, err := server.CommandDirector(ctx, fixture.Database, *fixture.ServiceManager, createAuthorsCmd,
		fixture.Logger, time.Now(), nil, "127.0.0.1")
	require.NoError(t, err)

	createBooksCmd := `CREATE BUNDLE "Books" WITH FIELDS (
		{"book_id", "INT", true, false},
		{"title", "STRING", false, false},
		{"author_fk", "INT", false, false}
	);`
	_, err = server.CommandDirector(ctx, fixture.Database, *fixture.ServiceManager, createBooksCmd,
		fixture.Logger, time.Now(), nil, "127.0.0.1")
	require.NoError(t, err)

	// Attempt to create relationship with non-existent source field
	createRelCmd := `UPDATE BUNDLE "Authors" ADD RELATIONSHIP (
		"Authors_Books_invalid" {"1toMany", "Authors", "nonexistent_field", "Books", "author_fk"}
	);`
	_, err = server.CommandDirector(ctx, fixture.Database, *fixture.ServiceManager, createRelCmd,
		fixture.Logger, time.Now(), nil, "127.0.0.1")

	// VALIDATION: Should fail with field error
	assert.Error(t, err, "Should reject relationship when source field doesn't exist")
	assert.Contains(t, err.Error(), "field", "Error message should mention field issue")
}
