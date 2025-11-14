/*
BUNDLE CREATION AND MANAGEMENT USE CASES

This file defines comprehensive use cases for creating and managing bundles using SyndrDB.
Bundles are the equivalent of tables in traditional relational databases, but designed
to store and work with JSON documents. Each bundle contains documents with automatically
managed DocumentIDs and maintains hash indexes for efficient document retrieval.

ALGORITHM OVERVIEW:
The use cases are organized into categories that test different aspects of bundle
management, ensuring robust coverage of all functionality. Each use case is designed
to validate specific requirements while maintaining the modular development approach
required by the SyndrDB project.

BUNDLE FUNCTIONALITY:
Bundles serve as containers for JSON documents, similar to tables in Postgres but
designed for document storage like MongoDB. Each bundle automatically creates and
maintains a hash index on the DocumentID field, ensuring efficient document lookup
and retrieval operations.

USE CASE CATEGORIES:
1. Basic Bundle Operations - Core CRUD functionality
2. Index Management - Hash index creation and maintenance
3. Document Operations - Adding, updating, removing documents
4. Bundle Relationships - Inter-bundle relationships and constraints
5. Performance Testing - Load testing and optimization validation
6. Error Handling - Invalid operations and edge cases
7. Integration Testing - Full workflow validation

This implementation follows the Single Responsibility Principle where each test
handles a specific aspect of bundle management while maintaining the robust
error handling and data integrity standards required by the SyndrDB project.
*/

package homegrown

import (
	"time"
)

// BundleManagementUseCase represents a single test case for bundle operations
// Following SyndrDB comprehensive error handling, encapsulates test metadata
type BundleManagementUseCase struct {
	Name          string
	Description   string
	Category      string
	SetupFunc     func() error
	ExecuteFunc   func() error
	ValidateFunc  func() error
	CleanupFunc   func() error
	ExpectSuccess bool
	Tags          []string
	Timeout       time.Duration
}

// BundleManagementUseCase implements the UseCase interface with method receivers
func (b BundleManagementUseCase) GetName() string        { return b.Name }
func (b BundleManagementUseCase) GetDescription() string { return b.Description }
func (b BundleManagementUseCase) GetCategory() string    { return b.Category }
func (b BundleManagementUseCase) GetExpectSuccess() bool { return b.ExpectSuccess }
func (b BundleManagementUseCase) Setup() error {
	if b.SetupFunc != nil {
		return b.SetupFunc()
	}
	return nil
}
func (b BundleManagementUseCase) Execute() error {
	if b.ExecuteFunc != nil {
		return b.ExecuteFunc()
	}
	return nil
}
func (b BundleManagementUseCase) Validate() error {
	if b.ValidateFunc != nil {
		return b.ValidateFunc()
	}
	return nil
}
func (b BundleManagementUseCase) Cleanup() error {
	if b.CleanupFunc != nil {
		return b.CleanupFunc()
	}
	return nil
}

// type BundleManagementUseCase struct {
// 	Name          string
// 	Description   string
// 	Category      string
// 	ExpectSuccess bool
// 	SetupFunc     func() error
// 	ExecuteFunc   func() error
// 	ValidateFunc  func() error
// 	CleanupFunc   func() error
// }

// GetBundleManagementUseCases returns comprehensive test cases for bundle operations
// This function follows the Single Responsibility Principle by handling only use case definition
// Following SyndrDB comprehensive error handling, it provides complete test coverage
func GetBundleManagementUseCases() []BundleManagementUseCase {
	return []BundleManagementUseCase{
		// CATEGORY: Basic Bundle Operations
		{
			Name:          "CreateEmptyBundle",
			Description:   "Create a new empty bundle with default configuration",
			Category:      "BasicOperations",
			SetupFunc:     setupBundleTestEnvironment,
			ExecuteFunc:   createEmptyBundle,
			ValidateFunc:  validateEmptyBundleCreation,
			CleanupFunc:   cleanupBundleTest,
			ExpectSuccess: true,
			Tags:          []string{"basic", "create", "empty"},
			Timeout:       30 * time.Second,
		},
		{
			Name:          "CreateBundleWithCustomName",
			Description:   "Create a bundle with a custom name following naming conventions",
			Category:      "BasicOperations",
			SetupFunc:     setupBundleTestEnvironment,
			ExecuteFunc:   createBundleWithCustomName,
			ValidateFunc:  validateBundleCustomName,
			CleanupFunc:   cleanupBundleTest,
			ExpectSuccess: true,
			Tags:          []string{"basic", "create", "naming"},
			Timeout:       30 * time.Second,
		},
		{
			Name:          "CreateBundleWithSchema",
			Description:   "Create a bundle with predefined schema constraints",
			Category:      "BasicOperations",
			SetupFunc:     setupBundleTestEnvironment,
			ExecuteFunc:   createBundleWithSchema,
			ValidateFunc:  validateBundleSchema,
			CleanupFunc:   cleanupBundleTest,
			ExpectSuccess: true,
			Tags:          []string{"basic", "create", "schema"},
			Timeout:       30 * time.Second,
		},
		{
			Name:          "DeleteEmptyBundle",
			Description:   "Delete an empty bundle and verify complete removal",
			Category:      "BasicOperations",
			SetupFunc:     setupBundleWithData,
			ExecuteFunc:   deleteEmptyBundle,
			ValidateFunc:  validateBundleDeletion,
			CleanupFunc:   cleanupBundleTest,
			ExpectSuccess: true,
			Tags:          []string{"basic", "delete", "empty"},
			Timeout:       30 * time.Second,
		},
		{
			Name:          "RenameBundle",
			Description:   "Rename an existing bundle and validate name change",
			Category:      "BasicOperations",
			SetupFunc:     setupBundleWithData,
			ExecuteFunc:   renameBundle,
			ValidateFunc:  validateBundleRename,
			CleanupFunc:   cleanupBundleTest,
			ExpectSuccess: true,
			Tags:          []string{"basic", "rename", "metadata"},
			Timeout:       30 * time.Second,
		},

		// CATEGORY: Index Management
		{
			Name:          "ValidateDefaultHashIndex",
			Description:   "Verify that hash index on DocumentID is automatically created",
			Category:      "IndexManagement",
			SetupFunc:     setupBundleTestEnvironment,
			ExecuteFunc:   createBundleAndVerifyIndex,
			ValidateFunc:  validateDefaultHashIndex,
			CleanupFunc:   cleanupBundleTest,
			ExpectSuccess: true,
			Tags:          []string{"index", "hash", "documentid", "automatic"},
			Timeout:       30 * time.Second,
		},
		// B-Tree index test - re-enabled with infinite loop protection
		{
			Name:          "CreateCustomBTreeIndex",
			Description:   "Create a BTree index on a custom field for filtering",
			Category:      "IndexManagement",
			SetupFunc:     setupBundleForIndexTests,
			ExecuteFunc:   createCustomBTreeIndex,
			ValidateFunc:  validateCustomBTreeIndex,
			CleanupFunc:   cleanupBundleTest,
			ExpectSuccess: true,
			Tags:          []string{"index", "btree", "custom", "filtering"},
			Timeout:       10 * time.Second,
		},
		{
			Name:          "UpdateHashIndexAfterDocumentOperation",
			Description:   "Verify hash index updates correctly after document add/remove",
			Category:      "IndexManagement",
			SetupFunc:     setupBundleForIndexTests,
			ExecuteFunc:   performDocumentOperationsAndCheckIndex,
			ValidateFunc:  validateHashIndexConsistency,
			CleanupFunc:   cleanupBundleTest,
			ExpectSuccess: true,
			Tags:          []string{"index", "hash", "consistency", "documents"},
			Timeout:       60 * time.Second,
		},
		// This functionality has not been implemented yet
		// {
		// 	Name:          "DropCustomBTreeIndex",
		// 	Description:   "Drop a custom BTree index and verify it no longer exists",
		// 	Category:      "IndexManagement",
		// 	SetupFunc:     setupBundleWithIndexes,
		// 	ExecuteFunc:   dropCustomIndex,
		// 	ValidateFunc:  validateIndexDeletion,
		// 	CleanupFunc:   cleanupBundleTest,
		// 	ExpectSuccess: true,
		// 	Tags:          []string{"index", "drop", "cleanup"},
		// 	Timeout:       30 * time.Second,
		// },

		// CATEGORY: Document Operations
		{
			Name:          "AddSingleDocument",
			Description:   "Add a single JSON document to a bundle",
			Category:      "DocumentOperations",
			SetupFunc:     setupBundleWithData,
			ExecuteFunc:   addSingleDocument,
			ValidateFunc:  validateSingleDocumentAddition,
			CleanupFunc:   cleanupBundleTest,
			ExpectSuccess: true,
			Tags:          []string{"document", "add", "single", "json"},
			Timeout:       30 * time.Second,
		},
		{
			Name:          "AddMultipleDocuments",
			Description:   "Add multiple JSON documents to a bundle in batch",
			Category:      "DocumentOperations",
			SetupFunc:     setupBundleWithData,
			ExecuteFunc:   addMultipleDocuments,
			ValidateFunc:  validateMultipleDocumentAddition,
			CleanupFunc:   cleanupBundleTest,
			ExpectSuccess: true,
			Tags:          []string{"document", "add", "multiple", "batch"},
			Timeout:       60 * time.Second,
		},
		{
			Name:          "UpdateExistingDocument",
			Description:   "Update an existing document and verify changes",
			Category:      "DocumentOperations",
			SetupFunc:     setupBundleForUpdateTests,
			ExecuteFunc:   updateExistingDocument,
			ValidateFunc:  validateDocumentUpdate,
			CleanupFunc:   cleanupBundleTest,
			ExpectSuccess: true,
			Tags:          []string{"document", "update", "modify"},
			Timeout:       30 * time.Second,
		},
		{
			Name:          "RemoveDocument",
			Description:   "Remove a document and verify it's no longer accessible",
			Category:      "DocumentOperations",
			SetupFunc:     setupBundleForUpdateTests,
			ExecuteFunc:   removeDocument,
			ValidateFunc:  validateDocumentRemoval,
			CleanupFunc:   cleanupBundleTest,
			ExpectSuccess: true,
			Tags:          []string{"document", "remove", "delete"},
			Timeout:       30 * time.Second,
		},
		{
			Name:          "QueryDocumentByID",
			Description:   "Query a document using its DocumentID",
			Category:      "DocumentOperations",
			SetupFunc:     setupBundleForQueryTests,
			ExecuteFunc:   queryDocumentByID,
			ValidateFunc:  validateDocumentQuery,
			CleanupFunc:   cleanupBundleTest,
			ExpectSuccess: true,
			Tags:          []string{"document", "query", "documentid"},
			Timeout:       30 * time.Second,
		},
		{
			Name:          "QueryDocumentsByField",
			Description:   "Query documents using custom field filters",
			Category:      "DocumentOperations",
			SetupFunc:     setupBundleForQueryTests,
			ExecuteFunc:   queryDocumentsByField,
			ValidateFunc:  validateFieldQuery,
			CleanupFunc:   cleanupBundleTest,
			ExpectSuccess: true,
			Tags:          []string{"document", "query", "field", "filter"},
			Timeout:       45 * time.Second,
		},

		// CATEGORY: Bundle Relationships
		{
			Name:          "CreateBundleRelationship",
			Description:   "Create a relationship between two bundles",
			Category:      "BundleRelationships",
			SetupFunc:     setupMultipleBundles,
			ExecuteFunc:   createBundleRelationship,
			ValidateFunc:  validateBundleRelationship,
			CleanupFunc:   cleanupMultipleBundles,
			ExpectSuccess: true,
			Tags:          []string{"relationship", "bundles", "create"},
			Timeout:       60 * time.Second,
		},
		{
			Name:          "AddRelationship1toMany",
			Description:   "Add a 1toMany relationship using new syntax",
			Category:      "BundleRelationships",
			SetupFunc:     setupRelationshipBundles,
			ExecuteFunc:   addRelationship1toMany,
			ValidateFunc:  validateAddedRelationship,
			CleanupFunc:   cleanupRelationshipBundles,
			ExpectSuccess: true,
			Tags:          []string{"relationship", "1toMany", "add"},
			Timeout:       60 * time.Second,
		},
		{
			Name:          "QueryRelatedDocuments",
			Description:   "Query documents across related bundles",
			Category:      "BundleRelationships",
			SetupFunc:     setupBundlesWithRelationships,
			ExecuteFunc:   queryRelatedDocuments,
			ValidateFunc:  validateRelatedDocumentQuery,
			CleanupFunc:   cleanupMultipleBundles,
			ExpectSuccess: true,
			Tags:          []string{"relationship", "query", "cross-bundle"},
			Timeout:       60 * time.Second,
		},
		{
			Name:          "UpdateRelationshipConstraints",
			Description:   "Update constraints on bundle relationships",
			Category:      "BundleRelationships",
			SetupFunc:     setupBundlesWithRelationships,
			ExecuteFunc:   updateRelationshipConstraints,
			ValidateFunc:  validateRelationshipConstraints,
			CleanupFunc:   cleanupMultipleBundles,
			ExpectSuccess: true,
			Tags:          []string{"relationship", "constraints", "update"},
			Timeout:       45 * time.Second,
		},
		{
			Name:          "RemoveBundleRelationship",
			Description:   "Remove a relationship between bundles",
			Category:      "BundleRelationships",
			SetupFunc:     setupBundlesWithRelationships,
			ExecuteFunc:   removeBundleRelationship,
			ValidateFunc:  validateRelationshipRemoval,
			CleanupFunc:   cleanupMultipleBundles,
			ExpectSuccess: true,
			Tags:          []string{"relationship", "remove", "cleanup"},
			Timeout:       45 * time.Second,
		},

		// CATEGORY: Performance Testing
		{
			Name:          "BulkDocumentInsertion",
			Description:   "Insert large number of documents and measure performance",
			Category:      "Performance",
			SetupFunc:     setupPerformanceBundleEnvironment,
			ExecuteFunc:   performBulkDocumentInsertion,
			ValidateFunc:  validateBulkInsertionPerformance,
			CleanupFunc:   cleanupPerformanceTest,
			ExpectSuccess: true,
			Tags:          []string{"performance", "bulk", "insert"},
			Timeout:       5 * time.Minute,
		},
		{
			Name:          "ConcurrentDocumentOperations",
			Description:   "Perform concurrent document operations on bundle",
			Category:      "Performance",
			SetupFunc:     setupConcurrentOperationEnvironment,
			ExecuteFunc:   performConcurrentDocumentOperations,
			ValidateFunc:  validateConcurrentOperationsPerformance,
			CleanupFunc:   cleanupPerformanceTest,
			ExpectSuccess: true,
			Tags:          []string{"performance", "concurrent", "operations"},
			Timeout:       3 * time.Minute,
		},
		// {
		// 	Name:          "LargeDocumentHandling",
		// 	Description:   "Handle documents with large JSON payloads",
		// 	Category:      "Performance",
		// 	SetupFunc:     setupLargeDocumentEnvironment,
		// 	ExecuteFunc:   handleLargeDocuments,
		// 	ValidateFunc:  validateLargeDocumentHandling,
		// 	CleanupFunc:   cleanupPerformanceTest,
		// 	ExpectSuccess: true,
		// 	Tags:          []string{"performance", "large", "documents"},
		// 	Timeout:       2 * time.Minute,
		// },

		// CATEGORY: Error Handling
		{
			Name:          "CreateBundleWithInvalidName",
			Description:   "Attempt to create bundle with invalid characters in name",
			Category:      "ErrorHandling",
			SetupFunc:     setupBundleTestEnvironment,
			ExecuteFunc:   createBundleWithInvalidName,
			ValidateFunc:  validateBundleCreationFailed,
			CleanupFunc:   cleanupBundleTest,
			ExpectSuccess: false,
			Tags:          []string{"error", "validation", "naming"},
			Timeout:       30 * time.Second,
		},
		{
			Name:          "CreateDuplicateBundle",
			Description:   "Attempt to create a bundle that already exists",
			Category:      "ErrorHandling",
			SetupFunc:     setupBundleWithData,
			ExecuteFunc:   createDuplicateBundle,
			ValidateFunc:  validateDuplicateBundleCreationFailed,
			CleanupFunc:   cleanupBundleTest,
			ExpectSuccess: false,
			Tags:          []string{"error", "duplicate", "conflict"},
			Timeout:       30 * time.Second,
		},
		{
			Name:          "AddDocumentWithInvalidJSON",
			Description:   "Attempt to add document with malformed JSON",
			Category:      "ErrorHandling",
			SetupFunc:     setupBundleWithData,
			ExecuteFunc:   addDocumentWithInvalidJSON,
			ValidateFunc:  validateInvalidJSONRejection,
			CleanupFunc:   cleanupBundleTest,
			ExpectSuccess: false,
			Tags:          []string{"error", "json", "validation"},
			Timeout:       30 * time.Second,
		},
		{
			Name:          "UpdateNonExistentDocument",
			Description:   "Attempt to update a document that doesn't exist",
			Category:      "ErrorHandling",
			SetupFunc:     setupBundleWithData,
			ExecuteFunc:   updateNonExistentDocument,
			ValidateFunc:  validateNonExistentDocumentUpdateFailed,
			CleanupFunc:   cleanupBundleTest,
			ExpectSuccess: false,
			Tags:          []string{"error", "nonexistent", "update"},
			Timeout:       30 * time.Second,
		},
		{
			Name:          "DeleteBundleWithDocuments",
			Description:   "Attempt to delete bundle containing documents",
			Category:      "ErrorHandling",
			SetupFunc:     setupBundleForUpdateTests, // Use update test bundle to avoid conflicts
			ExecuteFunc:   deleteBundleWithDocuments,
			ValidateFunc:  validateBundleDeletionWithDocumentsFailed,
			CleanupFunc:   cleanupBundleTest,
			ExpectSuccess: false,
			Tags:          []string{"error", "delete", "non-empty"},
			Timeout:       30 * time.Second,
		},

		// CATEGORY: Integration Testing
		{
			Name:          "CompleteDocumentLifecycle",
			Description:   "Full lifecycle: create bundle, add document, update, query, delete",
			Category:      "Integration",
			SetupFunc:     setupIntegrationBundleEnvironment,
			ExecuteFunc:   executeCompleteDocumentLifecycle,
			ValidateFunc:  validateCompleteDocumentLifecycle,
			CleanupFunc:   cleanupIntegrationTest,
			ExpectSuccess: true,
			Tags:          []string{"integration", "lifecycle", "complete"},
			Timeout:       2 * time.Minute,
		},
		{
			Name:          "MultiBundleWorkflow",
			Description:   "Complex workflow with multiple bundles and relationships",
			Category:      "Integration",
			SetupFunc:     setupMultiBundleWorkflowEnvironment,
			ExecuteFunc:   executeMultiBundleWorkflow,
			ValidateFunc:  validateMultiBundleWorkflow,
			CleanupFunc:   cleanupIntegrationTest,
			ExpectSuccess: true,
			Tags:          []string{"integration", "multi-bundle", "workflow"},
			Timeout:       3 * time.Minute,
		},
		{
			Name:          "BundleBackupAndRestore",
			Description:   "Backup bundle data and restore to verify integrity",
			Category:      "Integration",
			SetupFunc:     setupBackupRestoreEnvironment,
			ExecuteFunc:   executeBackupAndRestore,
			ValidateFunc:  validateBackupRestore,
			CleanupFunc:   cleanupIntegrationTest,
			ExpectSuccess: true,
			Tags:          []string{"integration", "backup", "restore"},
			Timeout:       3 * time.Minute,
		},
	}
}

// Setup Functions
