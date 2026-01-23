/*
PREDICATE PUSHDOWN - QUERY OPTIMIZATION

This file implements PostgreSQL-style predicate pushdown for JOIN queries.
Predicate pushdown is a critical optimization that filters data as early as possible
in the query execution pipeline, dramatically reducing the amount of data that needs
to be joined.

OPTIMIZATION STRATEGY:
1. Analyze WHERE clause to identify bundle-specific conditions
2. Push conditions down to individual bundles before JOIN
3. Only evaluate cross-bundle conditions after JOIN

PERFORMANCE IMPACT:
- Reduces JOIN input size by filtering early
- Leverages indexes on filtered bundles
- Can provide 100-1000x speedup for selective queries

Example:
  SELECT * FROM "Authors" JOIN "Books" WHERE "Authors"."DocumentID" == "X"

  Without pushdown: Load 1000 authors × 7000 books = 7M comparisons
  With pushdown:    Load 1 author × 7000 books = 7K comparisons
*/

package planner

import (
	"context"
	"fmt"
	"strings"
	"syndrdb/src/internal/domain/models"
	"syndrdb/src/internal/query/documentscanner"
	"syndrdb/src/internal/query/queryparser"

	"go.uber.org/zap"
)

// WhereAnalysis represents the analysis of WHERE conditions for JOIN optimization
type WhereAnalysis struct {
	LeftBundleConditions  []queryparser.WhereClause // Conditions that can be pushed down to left bundle
	RightBundleConditions []queryparser.WhereClause // Conditions that can be pushed down to right bundle
	CrossBundleConditions []queryparser.WhereClause // Conditions that must be evaluated after JOIN
	RemainingConditions   []queryparser.WhereClause // Complex conditions that need post-JOIN evaluation
}

// AnalyzeWhereClauseForJoin analyzes WHERE conditions to determine optimization opportunities
// This implements PostgreSQL-style predicate pushdown by separating bundle-specific conditions
// from cross-bundle conditions that require joined data to evaluate
func AnalyzeWhereClauseForJoin(whereGroup *queryparser.WhereGroup, leftBundle, rightBundle string, logger *zap.SugaredLogger) *WhereAnalysis {
	if whereGroup == nil {
		return &WhereAnalysis{}
	}

	analysis := &WhereAnalysis{
		LeftBundleConditions:  make([]queryparser.WhereClause, 0),
		RightBundleConditions: make([]queryparser.WhereClause, 0),
		CrossBundleConditions: make([]queryparser.WhereClause, 0),
		RemainingConditions:   make([]queryparser.WhereClause, 0),
	}

	// Analyze each clause in the WHERE group
	analyzeWhereConditions(whereGroup.Clauses, leftBundle, rightBundle, analysis, logger)

	// Recursively analyze subgroups
	for _, subGroup := range whereGroup.SubGroups {
		subAnalysis := AnalyzeWhereClauseForJoin(&subGroup, leftBundle, rightBundle, logger)

		// Merge subgroup analysis into main analysis
		analysis.LeftBundleConditions = append(analysis.LeftBundleConditions, subAnalysis.LeftBundleConditions...)
		analysis.RightBundleConditions = append(analysis.RightBundleConditions, subAnalysis.RightBundleConditions...)
		analysis.CrossBundleConditions = append(analysis.CrossBundleConditions, subAnalysis.CrossBundleConditions...)
		analysis.RemainingConditions = append(analysis.RemainingConditions, subAnalysis.RemainingConditions...)
	}

	logger.Infof("WHERE clause analysis: Left=%d, Right=%d, Cross=%d, Remaining=%d conditions",
		len(analysis.LeftBundleConditions), len(analysis.RightBundleConditions),
		len(analysis.CrossBundleConditions), len(analysis.RemainingConditions))

	return analysis
}

// analyzeWhereConditions analyzes individual WHERE conditions for pushdown opportunities
func analyzeWhereConditions(clauses []queryparser.WhereClause, leftBundle, rightBundle string, analysis *WhereAnalysis, logger *zap.SugaredLogger) {
	for _, clause := range clauses {
		// Determine which category this condition belongs to
		category := categorizeWhereCondition(clause, leftBundle, rightBundle, logger)

		switch category {
		case "left":
			analysis.LeftBundleConditions = append(analysis.LeftBundleConditions, clause)
		case "right":
			analysis.RightBundleConditions = append(analysis.RightBundleConditions, clause)
		case "cross":
			analysis.CrossBundleConditions = append(analysis.CrossBundleConditions, clause)
		default:
			analysis.RemainingConditions = append(analysis.RemainingConditions, clause)
		}
	}
}

// categorizeWhereCondition determines if a condition can be pushed down to a specific bundle
func categorizeWhereCondition(clause queryparser.WhereClause, leftBundle, rightBundle string, logger *zap.SugaredLogger) string {
	field := clause.Field

	// Check for bundle-qualified field names (e.g., "Authors"."Age")
	if strings.Contains(field, ".") {
		parts := strings.Split(field, ".")
		if len(parts) == 2 {
			bundleName := strings.Trim(parts[0], "\"'")

			// Check if this condition belongs to left bundle
			if strings.EqualFold(bundleName, leftBundle) {
				logger.Debugf("Condition '%s %s %v' can be pushed down to left bundle '%s'",
					field, clause.Operator, clause.Value, leftBundle)
				return "left"
			}

			// Check if this condition belongs to right bundle
			if strings.EqualFold(bundleName, rightBundle) {
				logger.Debugf("Condition '%s %s %v' can be pushed down to right bundle '%s'",
					field, clause.Operator, clause.Value, rightBundle)
				return "right"
			}
		}
	}

	// If field name doesn't specify a bundle, this might be a cross-bundle condition
	// or require post-JOIN evaluation
	logger.Debugf("Condition '%s %s %v' requires post-JOIN evaluation",
		field, clause.Operator, clause.Value)
	return "remaining"
}

// FilteredBundleAdapter wraps a bundle with predicate filtering
// This adapter applies WHERE conditions during document loading, implementing predicate pushdown
type FilteredBundleAdapter struct {
	bundle         *models.Bundle
	conditions     []queryparser.WhereClause
	bundleName     string
	logger         *zap.SugaredLogger
	scanner        documentscanner.DocumentScannerInterface
	bundleService  BundleServiceInterface
	totalDocuments int64
}

// NewFilteredBundleAdapter creates a bundle adapter that filters documents based on WHERE conditions
func NewFilteredBundleAdapter(
	bundle *models.Bundle,
	conditions []queryparser.WhereClause,
	bundleService BundleServiceInterface,
	logger *zap.SugaredLogger,
) (*FilteredBundleAdapter, error) {

	// Get or create document scanner for this bundle
	scanner, err := bundleService.GetOrCreateDocumentScanner(bundle)
	if err != nil {
		return nil, fmt.Errorf("failed to create document scanner: %w", err)
	}

	adapter := &FilteredBundleAdapter{
		bundle:         bundle,
		conditions:     conditions,
		bundleName:     bundle.Name,
		logger:         logger,
		scanner:        scanner,
		bundleService:  bundleService,
		totalDocuments: bundle.TotalDocuments,
	}

	// Remove bundle prefix from condition field names for filtering
	for i := range adapter.conditions {
		adapter.conditions[i].Field = removeBundlePrefix(adapter.conditions[i].Field)
	}

	logger.Infof("Created filtered bundle adapter for '%s' with %d conditions", bundle.Name, len(conditions))

	return adapter, nil
}

// GetName implements BundleInterface
func (fba *FilteredBundleAdapter) GetName() string {
	return fba.bundleName
}

// GetTotalDocuments implements BundleInterface - returns original total, not filtered count
func (fba *FilteredBundleAdapter) GetTotalDocuments() int {
	return int(fba.totalDocuments)
}

// GetDocument implements BundleInterface - retrieves and filters a single document
func (fba *FilteredBundleAdapter) GetDocument(docID string) *models.Document {
	// Load all filtered documents (they're cached in the scanner)
	allDocs := fba.GetAllDocuments()
	return allDocs[docID]
}

// GetDocumentIDs implements BundleInterface - returns IDs of filtered documents
func (fba *FilteredBundleAdapter) GetDocumentIDs() []string {
	// Load all filtered documents
	allDocs := fba.GetAllDocuments()
	ids := make([]string, 0, len(allDocs))
	for id := range allDocs {
		ids = append(ids, id)
	}
	return ids
}

// GetAllDocuments implements BundleInterface with predicate pushdown
// This is where the actual filtering happens during document loading
func (fba *FilteredBundleAdapter) GetAllDocuments() map[string]*models.Document {
	fba.logger.Infof("Loading documents from bundle '%s' with %d filter conditions (predicate pushdown)",
		fba.bundleName, len(fba.conditions))

	// Create predicate function that evaluates all conditions
	predicate := func(doc *models.Document) bool {
		return evaluateConditions(doc, fba.conditions, fba.logger)
	}

	// Use document scanner with predicate filtering
	scanResult, err := fba.scanner.ScanWithPredicate(predicate)
	if err != nil {
		fba.logger.Errorf("Failed to scan with predicate: %v", err)
		return make(map[string]*models.Document)
	}

	// Convert scan result to map
	documents := make(map[string]*models.Document)
	for i, doc := range scanResult.Documents {
		docID := scanResult.DocumentIDs[i]
		documents[docID] = doc
	}

	fba.logger.Infof("Predicate pushdown filtered bundle '%s' from %d to %d documents (%.1f%% reduction)",
		fba.bundleName, fba.totalDocuments, len(documents),
		100.0*(1.0-float64(len(documents))/float64(fba.totalDocuments)))

	return documents
}

// LoadPage implements BundleInterface - loads a page and filters documents by conditions
func (fba *FilteredBundleAdapter) LoadPage(pageID uint32) (*models.DocumentPage, error) {
	// Load page from bundleService if available
	if fba.bundleService != nil {
		// Get database name
		databaseName := ""
		if fba.bundle != nil && fba.bundle.Database != nil {
			databaseName = fba.bundle.Database.Name
		}
		
		page, err := fba.bundleService.GetDocumentPage(fba.bundleName, databaseName, pageID)
		if err != nil {
			return nil, err
		}
		
		// Filter documents by conditions
		filteredPage := &models.DocumentPage{
			PageID:         page.PageID,
			BundleID:       page.BundleID,
			Documents:      make(map[string]models.Document),
			NextPageID:     page.NextPageID,
			PreviousPageID: page.PreviousPageID,
			IsDirty:        page.IsDirty,
			LoadedAt:       page.LoadedAt,
			DocumentCount:  0,
		}
		
		for docID, doc := range page.Documents {
			if evaluateConditions(&doc, fba.conditions, fba.logger) {
				filteredPage.Documents[docID] = doc
				filteredPage.DocumentCount++
			}
		}
		
		return filteredPage, nil
	}
	
	// Fallback: can't load page without bundleService
	return nil, fmt.Errorf("FilteredBundleAdapter: bundleService not available for LoadPage")
}

// ScanDocumentChunks streams documents in chunks via bundleService and filters each chunk by conditions.
func (fba *FilteredBundleAdapter) ScanDocumentChunks(ctx context.Context, chunkSize int, fn func(chunk []*models.Document) (stop bool)) error {
	if fba.bundleService == nil {
		// No bundleService: fall back to loading all and chunking (higher memory)
		all := fba.GetAllDocuments()
		slice := make([]*models.Document, 0, len(all))
		for _, d := range all {
			slice = append(slice, d)
		}
		if chunkSize <= 0 {
			chunkSize = 4096
		}
		for i := 0; i < len(slice); i += chunkSize {
			end := i + chunkSize
			if end > len(slice) {
				end = len(slice)
			}
			if !fn(slice[i:end]) {
				return nil
			}
		}
		return nil
	}
	bundleName := fba.bundleName
	if bundleName == "" && fba.bundle != nil {
		bundleName = fba.bundle.Name
	}
	if bundleName == "" {
		return fmt.Errorf("FilteredBundleAdapter: bundle name unknown")
	}
	return fba.bundleService.GetDocumentChunksForIndexing(ctx, bundleName, chunkSize, func(chunk []*models.Document) bool {
		filtered := make([]*models.Document, 0, len(chunk))
		for _, doc := range chunk {
			if evaluateConditions(doc, fba.conditions, fba.logger) {
				filtered = append(filtered, doc)
			}
		}
		if len(filtered) == 0 {
			return true
		}
		return fn(filtered)
	})
}

// evaluateConditions checks if a document matches all conditions
func evaluateConditions(doc *models.Document, conditions []queryparser.WhereClause, logger *zap.SugaredLogger) bool {
	for _, condition := range conditions {
		if !condition.Matches(doc, logger) {
			return false
		}
	}
	return true
}

// removeBundlePrefix removes bundle name prefix from field name
// e.g., "Authors"."DocumentID" -> "DocumentID"
func removeBundlePrefix(field string) string {
	if strings.Contains(field, ".") {
		parts := strings.Split(field, ".")
		if len(parts) == 2 {
			return strings.Trim(parts[1], "\"'")
		}
	}
	return field
}
