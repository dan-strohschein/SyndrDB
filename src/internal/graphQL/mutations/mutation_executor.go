package mutations

import (
	"context"
	"fmt"
	"syndrdb/src/internal/domain/models"
	"syndrdb/src/internal/server"

	"go.uber.org/zap"
)

// MutationExecutor handles execution of GraphQL mutations by invoking BundleService methods.
// This executor follows the Single Responsibility Principle - it ONLY executes mutations,
// delegating validation to the service layer and parsing to the MutationParser.
//
// Design Philosophy:
// - DRY: Reuses existing BundleService methods (no duplication)
// - Direct Service Calls: No translation layer, calls BundleService directly
// - Same Path as SyndrQL: Mutations follow identical execution path as SyndrQL commands
//
// The executor intentionally does NOT:
// - Validate inputs (handled by Bundle Service validateDocumentFields)
// - Parse mutations (handled by MutationParser)
// - Format responses (handled by MutationResolver)
type MutationExecutor struct {
	serviceManager server.ServiceManager
	database       *models.Database
	logger         *zap.SugaredLogger
}

// NewMutationExecutor creates a new mutation executor instance.
func NewMutationExecutor(serviceManager server.ServiceManager, database *models.Database, logger *zap.SugaredLogger) *MutationExecutor {
	return &MutationExecutor{
		serviceManager: serviceManager,
		database:       database,
		logger:         logger,
	}
}

// ExecuteCreate executes a create mutation by calling BundleService.AddDocumentToBundle.
// This follows the EXACT same path as SyndrQL's ADD DOCUMENT command.
//
// Flow:
//  1. Get bundle by name from database
//  2. Call BundleService.AddDocumentToBundle (includes validation, NULL handling, unique constraints)
//  3. Return document ID for response formatting
//
// Validation happens in the service layer:
// - validateDocumentFields: Type checking, required fields
// - processNullValues: Default value substitution
// - ValidateUniqueConstraints: Uniqueness checks
//
// TODO: I will add permission checking here when SyndrDB implements the permission system.
// The permission check would verify that the current user has CREATE permission on the bundle.
// For example: if !permissionService.CanCreate(user, bundleName) { return error }
//
// TODO: I will add transaction support here when SyndrDB implements full ACID transactions.
// The transaction would wrap the mutation execution:
// - tx := transactionManager.Begin()
// - Execute mutation within transaction context
// - tx.Commit() on success, tx.Rollback() on failure
// This would enable atomic multi-mutation operations and proper isolation.
func (e *MutationExecutor) ExecuteCreate(docCommand *models.DocumentCommand) (string, error) {
	e.logger.Debugf("Executing create mutation for bundle '%s'", docCommand.BundleName)

	// Get the bundle by name
	bundle, err := e.serviceManager.BundleService.GetBundleByName(e.database, docCommand.BundleName)
	if err != nil {
		return "", fmt.Errorf("bundle '%s' not found: %w", docCommand.BundleName, err)
	}

	// TODO: I will add relationship validation here to ensure referenced documents exist.
	// For example, if creating a Post with authorId, verify the User exists:
	// if err := e.validateRelationships(bundle, docCommand); err != nil {
	//     return "", fmt.Errorf("relationship validation failed: %w", err)
	// }
	// This validation would:
	// - Identify foreign key fields from bundle relationships
	// - Check that referenced documents exist in target bundles
	// - Optionally auto-create missing parent records (with command-line flag)

	var documentID string

	// Execute with WAL logging if available
	// WAL provides crash recovery but NOT full ACID transactions (yet)
	if e.serviceManager.WALManager != nil {
		err = e.serviceManager.WALManager.ExecuteWithLogging(func(txID string) error {
			// Log the document insertion before execution
			err := e.serviceManager.WALManager.LogDocumentInsert(txID, docCommand.BundleName, "pending", docCommand.Fields)
			if err != nil {
				return fmt.Errorf("failed to log document insert: %w", err)
			}

			// Add the document to the bundle
			// This method handles ALL validation:
			// - processNullValues (default values)
			// - validateDocumentFields (types, required fields)
			// - ValidateUniqueConstraints (uniqueness)
			documentID, err = e.serviceManager.BundleService.AddDocumentToBundle(e.database, bundle, docCommand)
			return err
		})
	} else {
		// Fallback to direct execution if WAL is not available
		e.logger.Warn("WAL Manager not available, executing without transaction logging")
		documentID, err = e.serviceManager.BundleService.AddDocumentToBundle(e.database, bundle, docCommand)
	}

	if err != nil {
		return "", fmt.Errorf("failed to create document in bundle '%s': %w", docCommand.BundleName, err)
	}

	e.logger.Debugf("Successfully created document '%s' in bundle '%s'", documentID, docCommand.BundleName)
	return documentID, nil
}

// ExecuteUpdate executes an update mutation by calling BundleService.UpdateDocumentInBundle.
// This follows the EXACT same path as SyndrQL's UPDATE DOCUMENTS command.
//
// Flow:
//  1. Get bundle by name from database
//  2. Call BundleService.UpdateDocumentInBundle (includes validation, filtering, index updates)
//  3. Return success (document updated in-place)
//
// The service layer handles:
// - validateUpdateFields: Type checking for update fields
// - GetDocumentsByFilter: Find documents matching WHERE clause
// - Index maintenance: Update BTree/Hash indexes for changed fields
//
// TODO: I will add permission checking here when SyndrDB implements the permission system.
// The permission check would verify that the current user has UPDATE permission on the bundle.
//
// TODO: I will add transaction support here when SyndrDB implements full ACID transactions.
// Multiple updates in a single GraphQL mutation would be wrapped in a transaction.
func (e *MutationExecutor) ExecuteUpdate(updateCommand *models.DocumentUpdateCommand) error {
	e.logger.Debugf("Executing update mutation for bundle '%s'", updateCommand.BundleName)

	// Get the bundle by name
	bundle, err := e.serviceManager.BundleService.GetBundleByName(e.database, updateCommand.BundleName)
	if err != nil {
		return fmt.Errorf("bundle '%s' not found: %w", updateCommand.BundleName, err)
	}

	// TODO: I will add relationship validation here for updates that modify foreign keys.
	// If updating a foreign key field (e.g., authorId), verify the new referenced document exists.
	// This ensures referential integrity is maintained across mutations.

	// Execute with WAL logging if available
	if e.serviceManager.WALManager != nil {
		err = e.serviceManager.WALManager.ExecuteWithLogging(func(txID string) error {
			// Log the document update before execution
			err := e.serviceManager.WALManager.LogDocumentUpdate(txID, updateCommand.BundleName, "multiple", nil, updateCommand.Fields)
			if err != nil {
				return fmt.Errorf("failed to log document update: %w", err)
			}

			return e.serviceManager.BundleService.UpdateDocumentInBundle(context.Background(), e.database, bundle, updateCommand)
		})
	} else {
		e.logger.Warn("WAL Manager not available, executing without transaction logging")
		err = e.serviceManager.BundleService.UpdateDocumentInBundle(context.Background(), e.database, bundle, updateCommand)
	}

	if err != nil {
		return fmt.Errorf("failed to update document in bundle '%s': %w", updateCommand.BundleName, err)
	}

	e.logger.Debugf("Successfully updated document(s) in bundle '%s'", updateCommand.BundleName)
	return nil
}

// ExecuteDelete executes a delete mutation by calling BundleService.DeleteDocumentFromBundle.
// This follows the EXACT same path as SyndrQL's DELETE DOCUMENTS command.
//
// Flow:
//  1. Get bundle by name from database
//  2. Filter documents by WHERE clause to get document IDs
//  3. Call BundleService.DeleteDocumentFromBundle (includes referential integrity validation)
//  4. Return deleted document IDs for response
//
// The service layer handles:
// - GetDocumentsByFilter: Find documents to delete
// - ValidateDelete: Check referential integrity (can't delete if referenced)
// - DeleteDocumentFromBundleFile: Remove from storage
// - Index cleanup: Remove entries from BTree/Hash indexes
//
// TODO: I will add permission checking here when SyndrDB implements the permission system.
// The permission check would verify that the current user has DELETE permission on the bundle.
//
// TODO: I will add transaction support here when SyndrDB implements full ACID transactions.
// Delete operations would be transactional, with automatic rollback on referential integrity violations.
//
// TODO: I will add cascade delete support when implementing advanced relationship features.
// For example, deleting a User could optionally cascade delete their Posts:
// - Configurable per relationship (CASCADE, RESTRICT, SET NULL)
// - Would require relationship metadata in bundle schema
// - DELETE_CASCADE flag would trigger recursive deletion
func (e *MutationExecutor) ExecuteDelete(deleteCommand *models.DocumentDeleteCommand) ([]string, error) {
	e.logger.Debugf("Executing delete mutation for bundle '%s'", deleteCommand.BundleName)

	// Get the bundle by name
	bundle, err := e.serviceManager.BundleService.GetBundleByName(e.database, deleteCommand.BundleName)
	if err != nil {
		return nil, fmt.Errorf("bundle '%s' not found: %w", deleteCommand.BundleName, err)
	}

	// Get documents to delete by filtering
	// This is the same path as SyndrQL: parse WHERE clause, filter documents
	filteredDocs, err := e.serviceManager.BundleService.GetDocumentsByFilter(bundle, deleteCommand.WhereClause, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to filter documents: %w", err)
	}

	if len(filteredDocs) == 0 {
		return []string{}, nil // No documents matched the filter
	}

	// Extract document IDs from filtered documents
	docIDs := make([]string, len(filteredDocs))
	for i, doc := range filteredDocs {
		docIDs[i] = doc.DocumentID
	}

	// Execute with WAL logging if available
	if e.serviceManager.WALManager != nil {
		err = e.serviceManager.WALManager.ExecuteWithLogging(func(txID string) error {
			// Log each document deletion
			for _, docID := range docIDs {
				err := e.serviceManager.WALManager.LogDocumentDelete(txID, deleteCommand.BundleName, docID, nil)
				if err != nil {
					return fmt.Errorf("failed to log document delete: %w", err)
				}
			}

			// Delete the documents from the bundle
			// This method handles:
			// - Referential integrity validation (ValidateDelete)
			// - Deleting from bundle file storage
			// - Cleaning up indexes
			// - Updating metadata
			return e.serviceManager.BundleService.DeleteDocumentFromBundle(bundle, deleteCommand, docIDs, nil)
		})
	} else {
		// Fallback to direct execution if WAL is not available
		e.logger.Warn("WAL Manager not available, executing without transaction logging")
		err = e.serviceManager.BundleService.DeleteDocumentFromBundle(bundle, deleteCommand, docIDs, nil)
	}

	if err != nil {
		return nil, fmt.Errorf("failed to delete document(s) from bundle '%s': %w", deleteCommand.BundleName, err)
	}

	// Store deleted IDs for response formatting
	deleteCommand.DeletedDocumentIDs = docIDs

	e.logger.Debugf("Successfully deleted %d document(s) from bundle '%s'", len(docIDs), deleteCommand.BundleName)
	return docIDs, nil
}

// ExecuteBatchCreate executes a batch create mutation.
// TODO: I will implement this when SyndrDB adds batch operation support.
// Batch creates would:
// - Accept an array of DocumentCommand structs
// - Execute them in a single transaction for atomicity
// - Return array of document IDs or partial failure details
// - Optimize by batching index updates
// - Provide better performance than N individual creates
//
// Example usage:
//
//	createUsers(inputs: [
//	  { name: "Alice", email: "alice@..." }
//	  { name: "Bob", email: "bob@..." }
//	])
//
// Implementation considerations:
// - Atomic: All succeed or all fail (with transaction support)
// - Partial success mode: Continue on errors, return which succeeded/failed
// - Validation: Validate all inputs before executing any
// - Index optimization: Batch index updates for better performance
func (e *MutationExecutor) ExecuteBatchCreate(docCommands []*models.DocumentCommand) ([]string, error) {
	return nil, fmt.Errorf("batch create not yet supported - requires SyndrDB batch operation implementation")
}

// ExecuteNestedCreate executes a create mutation with nested relationship creates.
// TODO: I will implement this when adding advanced relationship features.
// Nested creates would allow creating a parent and children in one mutation:
//
// Example:
//
//	createUser(input: {
//	  name: "Alice"
//	  posts: [
//	    { title: "First Post", content: "..." }
//	    { title: "Second Post", content: "..." }
//	  ]
//	})
//
// Implementation would:
// - Parse nested input objects
// - Create parent document first
// - Create child documents with parent ID
// - Link relationships automatically
// - Execute in transaction for atomicity
// - Rollback all on failure
func (e *MutationExecutor) ExecuteNestedCreate(docCommand *models.DocumentCommand, nestedRelationships map[string][]map[string]interface{}) (string, error) {
	return "", fmt.Errorf("nested create not yet supported - requires relationship metadata and transaction support")
}
