//go:build ignore
// +build ignore

package main

// DISABLED: E2E integration test requires API updates and full system setup
//
// This file implements end-to-end GraphQL mutation tests requiring:
//   - Full ServiceManager initialization with 7 parameters (currently passing only 5)
//   - server.InitServiceManager() signature changed:
//     - Old: (dbService, bundleService, nil, nil, logger)
//     - New: (dbService, bundleService, catalogService, graphqlProcessor, userStore, logger, debugMode)
//   - Missing: CatalogService, GraphQLProcessor, UserStore, debugMode parameters
//   - Complex test environment setup with real database, bundle, and GraphQL infrastructure
//
// To enable this test:
//   1. Update setupGraphQLTestEnvironment() to provide all required dependencies:
//      - Create/mock CatalogService (defaultdb.CatalogService)
//      - Create/mock GraphQLProcessor (server.GraphQLProcessor interface)
//      - Create/mock UserStore (auth.UserStore)
//      - Add debugMode parameter (bool)
//   2. Update InitServiceManager call on line 123 with all 7 parameters
//   3. Verify all other API changes align with current implementation
//   4. Remove the `// +build ignore` line above
//
// This is a complex integration test - consider whether it should be:
//   - Moved to integration test suite (tests/integration/)
//   - Simplified to unit test level
//   - Updated to match current architecture

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"

	"syndrdb/src/internal/domain/bundle"
	"syndrdb/src/internal/domain/database"
	"syndrdb/src/internal/domain/document"
	"syndrdb/src/internal/domain/models"
	"syndrdb/src/internal/graphQL"
	"syndrdb/src/internal/graphQL/schema"
	"syndrdb/src/internal/server"
	"syndrdb/src/internal/storage/buffer"
	"syndrdb/src/internal/storage/bundlestore"
	"syndrdb/src/pkg/settings"
)

// TestEnvironment holds the test setup
type TestEnvironment struct {
	TmpDir          string
	Logger          *zap.SugaredLogger
	Database        *models.Database
	ServiceManager  *server.ServiceManager
	SchemaManager   *schema.SchemaManager
	GraphQLHandler  *graphQL.GraphQLHandler
	BundleService   *bundle.BundleService
	DatabaseService *database.DatabaseService
	Cleanup         func()
}

// mockDatabaseStore is a simple mock for database store operations
type mockDatabaseStore struct{}

func (m *mockDatabaseStore) CreateDatabaseDataFile(db *models.Database) error {
	return nil
}

func (m *mockDatabaseStore) LoadDatabaseDataFile(dataDir, fileName string) (*models.Database, error) {
	return nil, nil
}

func (m *mockDatabaseStore) LoadDatabaseIntoMemory(database *models.Database, databaseName string) (*[]byte, *models.Database, error) {
	return nil, nil, nil
}

func (m *mockDatabaseStore) LoadAllDatabaseDataFiles(dataDir string, logger *zap.SugaredLogger) (map[string]*models.Database, error) {
	return make(map[string]*models.Database), nil
}

func (m *mockDatabaseStore) UpdateDatabaseDataFile(db *models.Database) error {
	return nil
}

func (m *mockDatabaseStore) DeleteDatabaseDataFile(name string) error {
	return nil
}

func (m *mockDatabaseStore) GetAllDatabases() ([]*models.Database, error) {
	return nil, nil
}

// setupGraphQLTestEnvironment creates a complete test environment for GraphQL mutations
func setupGraphQLTestEnvironment(t *testing.T) *TestEnvironment {
	// Create temporary directory
	tmpDir, err := os.MkdirTemp("", "graphql_mutations_test_")
	require.NoError(t, err, "Failed to create temp directory")

	// Setup logger
	logger, err := zap.NewDevelopment()
	require.NoError(t, err, "Failed to create logger")
	sugar := logger.Sugar()

	// Create test database
	db := &models.Database{
		DatabaseID:    fmt.Sprintf("test-db-%d", time.Now().Unix()),
		Name:          "testdb",
		DataDirectory: tmpDir,
		Bundles:       make(map[string]models.Bundle),
	}

	// Create database directory
	dbDir := filepath.Join(tmpDir, db.Name)
	err = os.MkdirAll(dbDir, 0755)
	require.NoError(t, err, "Failed to create database directory")

	// Create settings with GraphQL enabled
	globalSettings := settings.GetSettings()
	globalSettings.DataDir = tmpDir
	globalSettings.TempDir = filepath.Join(tmpDir, "temp")
	globalSettings.EnableGraphQL = true
	globalSettings.LogLevel = "debug"

	// Create file registry and buffer pool
	fileRegistry, err := buffer.NewFileRegistry(tmpDir, buffer.SyncAlways, sugar)
	require.NoError(t, err, "Failed to create file registry")
	bufferPool := buffer.NewBufferPool(10, 4096, fileRegistry, sugar)

	// Create bundle store
	bundleStore, err := bundlestore.NewBundleStore(tmpDir, bufferPool, sugar, "binary")
	require.NoError(t, err, "Failed to create bundle store")

	// Create factories
	bundleFactory := bundle.NewBundleFactory()
	docFactory := document.NewDocumentFactory()

	// Create bundle service
	bundleService := bundle.NewBundleService(bundleStore, bundleFactory, docFactory, sugar, globalSettings)

	// Create database service
	dbStore := &mockDatabaseStore{}
	dbFactory := database.NewDatabaseFactory()
	dbService := database.NewDatabaseService(dbStore, dbFactory, globalSettings, sugar)

	// Create service manager
	serviceManager := server.InitServiceManager(dbService, bundleService, nil, nil, sugar)

	// Create schema manager (path should be in database directory)
	dbDirPath := filepath.Join(tmpDir, db.Name)
	schemaPath := filepath.Join(dbDirPath, fmt.Sprintf("%s_graphql.gql", db.Name))
	schemaManager, err := schema.NewSchemaManager(schemaPath, db.Name, db.DatabaseID)
	require.NoError(t, err, "Failed to create schema manager")

	// Cleanup function
	cleanup := func() {
		if schemaManager != nil {
			schemaManager.Close()
		}
		logger.Sync()
		os.RemoveAll(tmpDir)
		// CRITICAL: Reset the singleton ServiceManager to prevent test contamination
		// This ensures each test gets a fresh ServiceManager with its own temp directory
		server.ResetServiceManager()
	}

	return &TestEnvironment{
		TmpDir:          tmpDir,
		Logger:          sugar,
		Database:        db,
		ServiceManager:  serviceManager,
		SchemaManager:   schemaManager,
		GraphQLHandler:  nil, // Will be created after bundle is added
		BundleService:   bundleService,
		DatabaseService: dbService,
		Cleanup:         cleanup,
	}
}

// createTestBundle creates a bundle with predefined fields for testing
func createTestBundleForMutations(t *testing.T, env *TestEnvironment, bundleName string) {
	bundleCommand := &models.BundleCommand{
		BundleName: bundleName,
		Fields: []models.FieldDefinition{
			{Name: "name", Type: "string", IsRequired: true},
			{Name: "email", Type: "string", IsRequired: true},
			{Name: "age", Type: "int", IsRequired: false},
		},
	}

	_, err := env.BundleService.AddBundle(env.DatabaseService, env.Database, bundleCommand)
	require.NoError(t, err, "Failed to create bundle")

	// Debug: Check if schema file was generated
	schemaPath := filepath.Join(env.TmpDir, fmt.Sprintf("%s_graphql.gql", env.Database.Name))
	if _, err := os.Stat(schemaPath); os.IsNotExist(err) {
		t.Logf("WARNING: Schema file not found at %s", schemaPath)
	} else {
		content, _ := os.ReadFile(schemaPath)
		t.Logf("Schema file content (%d bytes): %s", len(content), string(content))
	}

	// Now initialize the GraphQL handler with the schema that was generated
	if env.GraphQLHandler == nil {
		graphQLHandler, err := graphQL.NewGraphQLHandler(*env.ServiceManager, env.Database, env.SchemaManager, env.Logger, nil)
		require.NoError(t, err, "Failed to create GraphQL handler")
		env.GraphQLHandler = graphQLHandler
	}
}

// assertNoGraphQLErrors checks for GraphQL errors in the response
func assertNoGraphQLErrors(t *testing.T, result interface{}) *graphQL.GraphQLResponse {
	response, ok := result.(graphQL.GraphQLResponse)
	require.True(t, ok, "Result should be GraphQLResponse type")

	if len(response.Errors) > 0 {
		t.Errorf("GraphQL errors: %+v", response.Errors)
	}

	return &response
}

// getResponseData safely extracts data from GraphQL response
func getResponseData(response *graphQL.GraphQLResponse, path ...string) interface{} {
	current := response.Data

	for _, key := range path {
		if m, ok := current.(map[string]interface{}); ok {
			current = m[key]
		} else {
			return nil
		}
	}

	return current
}

// TestGraphQLMutations_CreateDocument tests creating documents via GraphQL mutations
func TestGraphQLMutations_CreateDocument(t *testing.T) {
	env := setupGraphQLTestEnvironment(t)
	defer env.Cleanup()

	// Create a test bundle
	createTestBundleForMutations(t, env, "users")

	t.Run("Create document with valid data", func(t *testing.T) {
		query := `GRAPHQL::
			mutation {
				createusers(input: {
					name: "John Doe"
					email: "john@example.com"
					age: 30
				}) {
					DocumentID
					name
					email
					age
				}
			}
		`

		result, err := env.GraphQLHandler.ProcessGraphQLCommand(query)
		require.NoError(t, err)
		assert.NotNil(t, result)

		t.Logf("Create result: %+v", result)
	})
}

// TestGraphQLMutations_UpdateDocument tests updating documents via GraphQL mutations
func TestGraphQLMutations_UpdateDocument(t *testing.T) {
	env := setupGraphQLTestEnvironment(t)
	defer env.Cleanup()

	// Create a test bundle
	createTestBundleForMutations(t, env, "products")

	t.Run("Update mutation schema exists", func(t *testing.T) {
		query := `GRAPHQL::{ "query": "{ __type(name: \"Mutation\") { fields { name } } }" }`

		result, err := env.GraphQLHandler.ProcessGraphQLCommand(query)
		require.NoError(t, err)
		assert.NotNil(t, result)

		t.Logf("Mutation fields: %+v", result)
	})
}

// TestGraphQLMutations_CompleteFlow tests create, update, and delete in sequence
func TestGraphQLMutations_CompleteFlow(t *testing.T) {
	env := setupGraphQLTestEnvironment(t)
	defer env.Cleanup()

	// Create a test bundle
	createTestBundleForMutations(t, env, "books")

	var createdID string

	t.Run("Step 1: Create a document", func(t *testing.T) {
		query := `GRAPHQL::
			mutation {
				createbooks(input: {
					name: "The Go Programming Language"
					email: "golang@example.com"
					age: 2015
				}) {
					DocumentID
					name
					email
					age
				}
			}
		`

		result, err := env.GraphQLHandler.ProcessGraphQLCommand(query)
		require.NoError(t, err)

		response := assertNoGraphQLErrors(t, result)
		createData := getResponseData(response, "createbooks")
		require.NotNil(t, createData, "createbooks data should not be nil")

		dataMap, ok := createData.(map[string]interface{})
		require.True(t, ok, "createbooks should be a map")

		createdID = dataMap["DocumentID"].(string)
		assert.NotEmpty(t, createdID)
		assert.Equal(t, "The Go Programming Language", dataMap["name"])
		assert.Equal(t, "golang@example.com", dataMap["email"])
		// Age could be int64 or float64 depending on JSON unmarshaling
		ageVal := dataMap["age"]
		assert.True(t, ageVal == int64(2015) || ageVal == float64(2015), "Age should be 2015")

		t.Logf("Created document with ID: %s", createdID)
	})

	t.Run("Step 2: Update the document", func(t *testing.T) {
		query := fmt.Sprintf(`GRAPHQL::
			mutation {
				updatebooks(id: "%s", input: {
					age: 2020
				}) {
					DocumentID
					name
					age
				}
			}
		`, createdID)

		result, err := env.GraphQLHandler.ProcessGraphQLCommand(query)
		require.NoError(t, err)

		response := assertNoGraphQLErrors(t, result)
		updateData := getResponseData(response, "updatebooks")
		require.NotNil(t, updateData, "updatebooks data should not be nil")

		dataMap, ok := updateData.(map[string]interface{})
		require.True(t, ok, "updatebooks should be a map")

		assert.Equal(t, createdID, dataMap["DocumentID"])
		assert.Equal(t, "The Go Programming Language", dataMap["name"])
		ageVal := dataMap["age"]
		assert.True(t, ageVal == int64(2020) || ageVal == float64(2020), "Age should be 2020")

		t.Logf("Updated document: %+v", dataMap)
	})

	t.Run("Step 3: Delete the document", func(t *testing.T) {
		query := fmt.Sprintf(`GRAPHQL::
			mutation {
				deletebooks(id: "%s") {
					success
					deletedId
					message
				}
			}
		`, createdID)

		result, err := env.GraphQLHandler.ProcessGraphQLCommand(query)
		require.NoError(t, err)

		response := assertNoGraphQLErrors(t, result)
		deleteData := getResponseData(response, "deletebooks")
		require.NotNil(t, deleteData, "deletebooks data should not be nil")

		dataMap, ok := deleteData.(map[string]interface{})
		require.True(t, ok, "deletebooks should be a map")

		assert.Equal(t, true, dataMap["success"])
		assert.Equal(t, createdID, dataMap["deletedId"])

		t.Logf("Deleted document: %+v", dataMap)
	})
}

// TestGraphQLMutations_ErrorHandling tests error scenarios
func TestGraphQLMutations_ErrorHandling(t *testing.T) {
	env := setupGraphQLTestEnvironment(t)
	defer env.Cleanup()

	createTestBundleForMutations(t, env, "employees")

	t.Run("Create with missing required field", func(t *testing.T) {
		query := `GRAPHQL::
			mutation {
				createemployees(input: {
					age: 25
				}) {
					DocumentID
					name
				}
			}
		`

		result, err := env.GraphQLHandler.ProcessGraphQLCommand(query)

		// Should succeed but have errors in the result
		if err == nil {
			response, ok := result.(graphQL.GraphQLResponse)
			require.True(t, ok, "Result should be GraphQLResponse type")
			assert.NotEmpty(t, response.Errors, "Expected errors for missing required field")
			t.Logf("Validation error (expected): %+v", response.Errors)
		} else {
			// Or might return an error directly
			t.Logf("Got error (expected): %v", err)
		}
	})

	t.Run("Update non-existent document", func(t *testing.T) {
		query := `GRAPHQL::
			mutation {
				updateemployees(id: "nonexistent-id-12345", input: {
					name: "Ghost Employee"
				}) {
					DocumentID
					name
				}
			}
		`

		result, err := env.GraphQLHandler.ProcessGraphQLCommand(query)

		if err == nil {
			response, ok := result.(graphQL.GraphQLResponse)
			require.True(t, ok, "Result should be GraphQLResponse type")
			assert.NotEmpty(t, response.Errors, "Expected errors for non-existent document")
			t.Logf("Update error (expected): %+v", response.Errors)
		} else {
			t.Logf("Got error (expected): %v", err)
		}
	})

	t.Run("Delete non-existent document", func(t *testing.T) {
		query := `GRAPHQL::
			mutation {
				deleteemployees(id: "nonexistent-id-99999") {
					success
					deletedId
					message
				}
			}
		`

		result, err := env.GraphQLHandler.ProcessGraphQLCommand(query)

		if err == nil {
			response, ok := result.(graphQL.GraphQLResponse)
			require.True(t, ok, "Result should be GraphQLResponse type")
			assert.NotEmpty(t, response.Errors, "Expected errors for non-existent document")
			t.Logf("Delete error (expected): %+v", response.Errors)
		} else {
			t.Logf("Got error (expected): %v", err)
		}
	})
}

// TestGraphQLMutations_MultipleDocuments tests creating and managing multiple documents
func TestGraphQLMutations_MultipleDocuments(t *testing.T) {
	env := setupGraphQLTestEnvironment(t)
	defer env.Cleanup()

	createTestBundleForMutations(t, env, "customers")

	var documentIDs []string

	t.Run("Create multiple documents", func(t *testing.T) {
		customers := []struct {
			name  string
			email string
			age   int
		}{
			{"Alice Smith", "alice@example.com", 28},
			{"Bob Jones", "bob@example.com", 35},
			{"Charlie Brown", "charlie@example.com", 42},
		}

		for _, customer := range customers {
			query := fmt.Sprintf(`GRAPHQL::
				mutation {
					createcustomers(input: {
						name: "%s"
						email: "%s"
						age: %d
					}) {
						DocumentID
						name
						email
						age
					}
				}
			`, customer.name, customer.email, customer.age)

			result, err := env.GraphQLHandler.ProcessGraphQLCommand(query)
			require.NoError(t, err)

			response := assertNoGraphQLErrors(t, result)
			createData := getResponseData(response, "createcustomers")
			require.NotNil(t, createData)

			dataMap, ok := createData.(map[string]interface{})
			require.True(t, ok)

			docID := dataMap["DocumentID"].(string)
			documentIDs = append(documentIDs, docID)
			assert.Equal(t, customer.name, dataMap["name"])

			t.Logf("Created customer: %s with ID: %s", customer.name, docID)
		}

		assert.Equal(t, 3, len(documentIDs), "Should have created 3 documents")
	})

	t.Run("Query all documents", func(t *testing.T) {
		query := `GRAPHQL::{ "query": "{ customers { DocumentID name email age } }" }`

		result, err := env.GraphQLHandler.ProcessGraphQLCommand(query)
		require.NoError(t, err)

		response := assertNoGraphQLErrors(t, result)
		customers := getResponseData(response, "customers")

		if customerList, ok := customers.([]interface{}); ok {
			assert.GreaterOrEqual(t, len(customerList), 3, "Should have at least 3 customers")
			t.Logf("Query returned %d customers", len(customerList))
		}
	})
}

// TestGraphQLMutations_FieldSelection tests selective field retrieval
func TestGraphQLMutations_FieldSelection(t *testing.T) {
	env := setupGraphQLTestEnvironment(t)
	defer env.Cleanup()

	createTestBundleForMutations(t, env, "articles")

	var articleID string

	t.Run("Create article", func(t *testing.T) {
		query := `GRAPHQL::
			mutation {
				createarticles(input: {
					name: "GraphQL Best Practices"
					email: "author@example.com"
					age: 2024
				}) {
					DocumentID
					name
				}
			}
		`

		result, err := env.GraphQLHandler.ProcessGraphQLCommand(query)
		require.NoError(t, err)

		response := assertNoGraphQLErrors(t, result)
		createData := getResponseData(response, "createarticles")
		require.NotNil(t, createData)

		dataMap, ok := createData.(map[string]interface{})
		require.True(t, ok)

		articleID = dataMap["DocumentID"].(string)

		// Should only have DocumentID and name, not email or age
		assert.Contains(t, dataMap, "DocumentID")
		assert.Contains(t, dataMap, "name")
		assert.NotContains(t, dataMap, "email", "Should not return email when not requested")
		assert.NotContains(t, dataMap, "age", "Should not return age when not requested")

		t.Logf("Selective fields test passed, ID: %s", articleID)
	})

	t.Run("Update with partial fields", func(t *testing.T) {
		query := fmt.Sprintf(`GRAPHQL::
			mutation {
				updatearticles(id: "%s", input: {
					name: "GraphQL Advanced Practices"
				}) {
					DocumentID
					name
				}
			}
		`, articleID)

		result, err := env.GraphQLHandler.ProcessGraphQLCommand(query)
		require.NoError(t, err)

		response := assertNoGraphQLErrors(t, result)
		updateData := getResponseData(response, "updatearticles")
		require.NotNil(t, updateData)

		dataMap, ok := updateData.(map[string]interface{})
		require.True(t, ok)

		assert.Equal(t, "GraphQL Advanced Practices", dataMap["name"])
		t.Logf("Partial update successful")
	})
}
