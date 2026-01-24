package bundle

import (
	"fmt"
	"syndrdb/src/internal/domain/models"

	"go.uber.org/zap"
)

// WhereFilterService provides efficient WHERE clause filtering operations
// This service is optimized for extracting specific data from WHERE clause results
// without the overhead of full document loading when only DocumentIDs are needed.
//
// Design Philosophy:
// - Single Responsibility: Handles only WHERE clause filtering
// - DRY: Reuses existing GetDocumentsByFilter infrastructure
// - Performance: Extracts only IDs when full documents aren't needed
// - Reusability: Used by DELETE, UPDATE, SELECT, and COUNT operations
type WhereFilterService struct {
	bundleService *BundleService
	logger        *zap.SugaredLogger
}

// NewWhereFilterService creates a new WHERE clause filtering service
func NewWhereFilterService(bundleService *BundleService, logger *zap.SugaredLogger) *WhereFilterService {
	return &WhereFilterService{
		bundleService: bundleService,
		logger:        logger,
	}
}

// GetDocumentIDsByFilter returns only the DocumentIDs of documents matching the WHERE clause
// This is optimized for DELETE and UPDATE operations that only need IDs, not full documents.
//
// Performance Benefits:
// - Reuses existing GetDocumentsByFilter infrastructure
// - Extracts only DocumentID field, avoiding unnecessary data transfer
// - Cleaner code separation between filtering and ID extraction
//
// Parameters:
//   - bundle: The bundle to filter documents from
//   - whereClause: The WHERE clause condition (e.g., "Age > 30 AND Status == 'active'")
//
// Returns:
//   - []string: List of DocumentIDs matching the WHERE clause
//   - error: Any error encountered during filtering
//
// Example Usage:
//
//	whereService := NewWhereFilterService(bundleService, logger)
//	docIDs, err := whereService.GetDocumentIDsByFilter(bundle, "Age > 30")
//	if err != nil {
//	    return fmt.Errorf("failed to filter documents: %w", err)
//	}
//	// docIDs = ["doc1", "doc2", "doc3"]
func (w *WhereFilterService) GetDocumentIDsByFilter(bundle *models.Bundle, whereClause string) ([]string, error) {
	docIDs, _, err := w.GetDocumentsAndIDsByFilter(bundle, whereClause)
	return docIDs, err
}

// GetDocumentsAndIDsByFilter returns both document IDs and full documents for the WHERE clause.
// Used by UPDATE with document-level locks: one WHERE scan yields docs+IDs; we pass docs to
// UpdateDocumentInBundle so it never re-fetches by GetDocument(docID), avoiding index→page
// lookup failures (e.g. after compaction) and ensuring accurate updates.
func (w *WhereFilterService) GetDocumentsAndIDsByFilter(bundle *models.Bundle, whereClause string) ([]string, []*models.Document, error) {
	if bundle == nil {
		return nil, nil, fmt.Errorf("bundle cannot be nil")
	}

	w.logger.Debugf("Filtering documents by WHERE clause for docs+IDs: %s", whereClause)

	documents, err := w.bundleService.GetDocumentsByFilter(bundle, whereClause, nil)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to filter documents: %w", err)
	}

	docIDs := make([]string, 0, len(documents))
	docs := make([]*models.Document, 0, len(documents))
	for _, doc := range documents {
		docIDs = append(docIDs, doc.DocumentID)
		docs = append(docs, doc)
	}

	w.logger.Debugf("WHERE filter matched %d documents in bundle '%s'", len(docIDs), bundle.Name)

	return docIDs, docs, nil
}

// GetDocumentCountByFilter returns only the count of documents matching the WHERE clause
// This is optimized for COUNT queries that don't need the actual documents.
//
// Parameters:
//   - bundle: The bundle to count documents in
//   - whereClause: The WHERE clause condition
//
// Returns:
//   - int: Count of documents matching the WHERE clause
//   - error: Any error encountered during filtering
func (w *WhereFilterService) GetDocumentCountByFilter(bundle *models.Bundle, whereClause string) (int, error) {
	if bundle == nil {
		return 0, fmt.Errorf("bundle cannot be nil")
	}

	// Get the IDs and count them
	// Future optimization: Add a count-only execution mode to planner
	docIDs, err := w.GetDocumentIDsByFilter(bundle, whereClause)
	if err != nil {
		return 0, fmt.Errorf("failed to count documents: %w", err)
	}

	return len(docIDs), nil
}
