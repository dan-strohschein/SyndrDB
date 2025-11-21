package homegrown

import (
	"os"
	"path/filepath"
	"testing"

	"syndrdb/src/internal/domain/models"
	graphqlparser "syndrdb/src/internal/graphQL/parser"
	"syndrdb/src/internal/graphQL/schema"
	"syndrdb/src/internal/query/queryparser"

	"github.com/vektah/gqlparser/v2/ast"
	gqlparser "github.com/vektah/gqlparser/v2/parser"
	"go.uber.org/zap"
)

// TestPhase6ParserBasicQuery tests basic GraphQL to UnifiedSelectQuery parsing
func TestPhase6ParserBasicQuery(t *testing.T) {
	// Setup
	tmpDir, _ := os.MkdirTemp("", "phase6_test_")
	defer os.RemoveAll(tmpDir)

	logger, _ := zap.NewDevelopment()
	sugar := logger.Sugar()
	defer logger.Sync()

	database := &models.Database{
		DatabaseID:    "test-db",
		Name:          "testdb",
		DataDirectory: tmpDir,
		Bundles:       make(map[string]models.Bundle),
	}

	database.Bundles["users"] = models.Bundle{
		BundleID: "bundle-users",
		Name:     "users",
	}

	schemaPath := filepath.Join(tmpDir, "test_schemas.gqls")
	schemaManager, _ := schema.NewSchemaManager(schemaPath, "testdb", "test-db")
	defer schemaManager.Close()

	usersSchema := &schema.GraphQLSchemaDefinition{
		TypeName: "User",
		Fields: []schema.GraphQLField{
			{Name: "id", Type: "ID!", BundleField: "DocumentID"},
			{Name: "name", Type: "String!", BundleField: "name"},
		},
	}

	var schemaID, bundleID [16]byte
	copy(schemaID[:], []byte("schema-1"))
	copy(bundleID[:], []byte("bundle-1"))
	schemaManager.AddNewSchema(schemaID, bundleID, "users", usersSchema)

	// Test: Parse GraphQL query
	parser := graphqlparser.NewGraphQLParser(schemaManager, database, sugar)

	queryStr := `{ users { id name } }`
	doc, _ := gqlparser.ParseQuery(&ast.Source{Input: queryStr})
	field := doc.Operations[0].SelectionSet[0].(*ast.Field)

	unifiedQuery, err := parser.ParseGraphQLQuery(field, nil)
	if err != nil {
		t.Fatalf("Parse failed: %v", err)
	}

	// Verify
	if unifiedQuery.FromBundle != "users" {
		t.Errorf("Expected bundle='users', got '%s'", unifiedQuery.FromBundle)
	}

	if len(unifiedQuery.SelectFields) != 2 {
		t.Errorf("Expected 2 fields, got %d", len(unifiedQuery.SelectFields))
	}

	if unifiedQuery.SelectFields[0] != "DocumentID" {
		t.Errorf("Expected 'DocumentID', got '%s'", unifiedQuery.SelectFields[0])
	}

	t.Logf("✓ GraphQL parsed to UnifiedSelectQuery: %s [%v]",
		unifiedQuery.FromBundle, unifiedQuery.SelectFields)
}

// TestPhase6ParserWithLimit tests LIMIT argument parsing
func TestPhase6ParserWithLimit(t *testing.T) {
	tmpDir, _ := os.MkdirTemp("", "phase6_test_")
	defer os.RemoveAll(tmpDir)

	logger, _ := zap.NewDevelopment()
	sugar := logger.Sugar()
	defer logger.Sync()

	database := &models.Database{
		DatabaseID:    "test-db",
		Name:          "testdb",
		DataDirectory: tmpDir,
		Bundles:       make(map[string]models.Bundle),
	}

	database.Bundles["users"] = models.Bundle{BundleID: "bundle-users", Name: "users"}

	schemaPath := filepath.Join(tmpDir, "test_schemas.gqls")
	schemaManager, _ := schema.NewSchemaManager(schemaPath, "testdb", "test-db")
	defer schemaManager.Close()

	usersSchema := &schema.GraphQLSchemaDefinition{
		TypeName: "User",
		Fields: []schema.GraphQLField{
			{Name: "id", Type: "ID!", BundleField: "DocumentID"},
		},
	}

	var schemaID, bundleID [16]byte
	copy(schemaID[:], []byte("schema-1"))
	copy(bundleID[:], []byte("bundle-1"))
	schemaManager.AddNewSchema(schemaID, bundleID, "users", usersSchema)

	parser := graphqlparser.NewGraphQLParser(schemaManager, database, sugar)

	queryStr := `{ users(limit: 10) { id } }`
	doc, _ := gqlparser.ParseQuery(&ast.Source{Input: queryStr})
	field := doc.Operations[0].SelectionSet[0].(*ast.Field)

	unifiedQuery, _ := parser.ParseGraphQLQuery(field, nil)

	if unifiedQuery.Limit != 10 {
		t.Errorf("Expected Limit=10, got %d", unifiedQuery.Limit)
	}

	t.Logf("✓ LIMIT parsed: %d", unifiedQuery.Limit)
}

// TestPhase6ParserWithOrderBy tests ORDER BY argument parsing
func TestPhase6ParserWithOrderBy(t *testing.T) {
	tmpDir, _ := os.MkdirTemp("", "phase6_test_")
	defer os.RemoveAll(tmpDir)

	logger, _ := zap.NewDevelopment()
	sugar := logger.Sugar()
	defer logger.Sync()

	database := &models.Database{
		DatabaseID:    "test-db",
		Name:          "testdb",
		DataDirectory: tmpDir,
		Bundles:       make(map[string]models.Bundle),
	}

	database.Bundles["users"] = models.Bundle{BundleID: "bundle-users", Name: "users"}

	schemaPath := filepath.Join(tmpDir, "test_schemas.gqls")
	schemaManager, _ := schema.NewSchemaManager(schemaPath, "testdb", "test-db")
	defer schemaManager.Close()

	usersSchema := &schema.GraphQLSchemaDefinition{
		TypeName: "User",
		Fields: []schema.GraphQLField{
			{Name: "name", Type: "String!", BundleField: "name"},
		},
	}

	var schemaID, bundleID [16]byte
	copy(schemaID[:], []byte("schema-1"))
	copy(bundleID[:], []byte("bundle-1"))
	schemaManager.AddNewSchema(schemaID, bundleID, "users", usersSchema)

	parser := graphqlparser.NewGraphQLParser(schemaManager, database, sugar)

	// Test DESC
	queryStr := `{ users(orderBy: "name DESC") { name } }`
	doc, _ := gqlparser.ParseQuery(&ast.Source{Input: queryStr})
	field := doc.Operations[0].SelectionSet[0].(*ast.Field)

	unifiedQuery, _ := parser.ParseGraphQLQuery(field, nil)

	if unifiedQuery.OrderBy == nil {
		t.Fatal("Expected OrderBy to be set")
	}

	if unifiedQuery.OrderBy.Fields[0].Direction != queryparser.SortDesc {
		t.Errorf("Expected DESC, got %v", unifiedQuery.OrderBy.Fields[0].Direction)
	}

	t.Logf("✓ ORDER BY parsed: %s %v",
		unifiedQuery.OrderBy.Fields[0].FieldName,
		unifiedQuery.OrderBy.Fields[0].Direction)
}

// TestPhase6ParserWithWhere tests WHERE clause parsing
func TestPhase6ParserWithWhere(t *testing.T) {
	tmpDir, _ := os.MkdirTemp("", "phase6_test_")
	defer os.RemoveAll(tmpDir)

	logger, _ := zap.NewDevelopment()
	sugar := logger.Sugar()
	defer logger.Sync()

	database := &models.Database{
		DatabaseID:    "test-db",
		Name:          "testdb",
		DataDirectory: tmpDir,
		Bundles:       make(map[string]models.Bundle),
	}

	database.Bundles["users"] = models.Bundle{BundleID: "bundle-users", Name: "users"}

	schemaPath := filepath.Join(tmpDir, "test_schemas.gqls")
	schemaManager, _ := schema.NewSchemaManager(schemaPath, "testdb", "test-db")
	defer schemaManager.Close()

	usersSchema := &schema.GraphQLSchemaDefinition{
		TypeName: "User",
		Fields: []schema.GraphQLField{
			{Name: "status", Type: "String", BundleField: "status"},
		},
	}

	var schemaID, bundleID [16]byte
	copy(schemaID[:], []byte("schema-1"))
	copy(bundleID[:], []byte("bundle-1"))
	schemaManager.AddNewSchema(schemaID, bundleID, "users", usersSchema)

	parser := graphqlparser.NewGraphQLParser(schemaManager, database, sugar)

	// Use SyndrQL syntax for WHERE (== not =)
	queryStr := `{ users(where: "status == 'active'") { status } }`
	doc, _ := gqlparser.ParseQuery(&ast.Source{Input: queryStr})
	field := doc.Operations[0].SelectionSet[0].(*ast.Field)

	unifiedQuery, err := parser.ParseGraphQLQuery(field, nil)
	if err != nil {
		t.Fatalf("Parse failed: %v", err)
	}

	// Verify WHERE was parsed
	if unifiedQuery.WhereExpression == nil {
		t.Error("Expected WhereExpression to be set")
	}

	t.Logf("✓ WHERE clause parsed successfully")
}

// TestPhase6ParserFieldMapping tests GraphQL field name mapping
func TestPhase6ParserFieldMapping(t *testing.T) {
	tmpDir, _ := os.MkdirTemp("", "phase6_test_")
	defer os.RemoveAll(tmpDir)

	logger, _ := zap.NewDevelopment()
	sugar := logger.Sugar()
	defer logger.Sync()

	database := &models.Database{
		DatabaseID:    "test-db",
		Name:          "testdb",
		DataDirectory: tmpDir,
		Bundles:       make(map[string]models.Bundle),
	}

	database.Bundles["users"] = models.Bundle{BundleID: "bundle-users", Name: "users"}

	schemaPath := filepath.Join(tmpDir, "test_schemas.gqls")
	schemaManager, _ := schema.NewSchemaManager(schemaPath, "testdb", "test-db")
	defer schemaManager.Close()

	usersSchema := &schema.GraphQLSchemaDefinition{
		TypeName: "User",
		Fields: []schema.GraphQLField{
			{Name: "id", Type: "ID!", BundleField: "DocumentID"},
		},
	}

	var schemaID, bundleID [16]byte
	copy(schemaID[:], []byte("schema-1"))
	copy(bundleID[:], []byte("bundle-1"))
	schemaManager.AddNewSchema(schemaID, bundleID, "users", usersSchema)

	parser := graphqlparser.NewGraphQLParser(schemaManager, database, sugar)

	queryStr := `{ users { id } }`
	doc, _ := gqlparser.ParseQuery(&ast.Source{Input: queryStr})
	field := doc.Operations[0].SelectionSet[0].(*ast.Field)

	unifiedQuery, _ := parser.ParseGraphQLQuery(field, nil)

	if unifiedQuery.SelectFields[0] != "DocumentID" {
		t.Errorf("Expected 'DocumentID', got '%s'", unifiedQuery.SelectFields[0])
	}

	t.Logf("✓ Field mapping: 'id' → 'DocumentID'")
}

// TestPhase6ParserErrorHandling tests parser error handling
func TestPhase6ParserErrorHandling(t *testing.T) {
	tmpDir, _ := os.MkdirTemp("", "phase6_test_")
	defer os.RemoveAll(tmpDir)

	logger, _ := zap.NewDevelopment()
	sugar := logger.Sugar()
	defer logger.Sync()

	database := &models.Database{
		DatabaseID:    "test-db",
		Name:          "testdb",
		DataDirectory: tmpDir,
		Bundles:       make(map[string]models.Bundle),
	}

	database.Bundles["users"] = models.Bundle{BundleID: "bundle-users", Name: "users"}

	schemaPath := filepath.Join(tmpDir, "test_schemas.gqls")
	schemaManager, _ := schema.NewSchemaManager(schemaPath, "testdb", "test-db")
	defer schemaManager.Close()

	usersSchema := &schema.GraphQLSchemaDefinition{
		TypeName: "User",
		Fields: []schema.GraphQLField{
			{Name: "name", Type: "String!", BundleField: "name"},
		},
	}

	var schemaID, bundleID [16]byte
	copy(schemaID[:], []byte("schema-1"))
	copy(bundleID[:], []byte("bundle-1"))
	schemaManager.AddNewSchema(schemaID, bundleID, "users", usersSchema)

	parser := graphqlparser.NewGraphQLParser(schemaManager, database, sugar)

	// Test invalid field
	queryStr := `{ users { invalid_field } }`
	doc, _ := gqlparser.ParseQuery(&ast.Source{Input: queryStr})
	field := doc.Operations[0].SelectionSet[0].(*ast.Field)

	_, err := parser.ParseGraphQLQuery(field, nil)

	if err == nil {
		t.Error("Expected error for invalid field")
	} else {
		t.Logf("✓ Correctly rejected invalid field: %v", err)
	}
}
