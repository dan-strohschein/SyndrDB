package server

import (
	"fmt"
	"strconv"
	"strings"
	"time"

	"syndrdb/src/internal/domain/document"
	"syndrdb/src/internal/domain/models"
	"syndrdb/src/internal/query/documentscanner"
	joinexecutor "syndrdb/src/internal/query/join_executor"
	"syndrdb/src/internal/query/queryparser"
	"syndrdb/src/internal/query/results"

	"go.uber.org/zap"
)

// JOIN WHERE CLAUSE OPTIMIZATION SYSTEM
// These types and functions implement PostgreSQL-style predicate pushdown for JOIN queries

// WhereAnalysis represents the analysis of WHERE conditions for JOIN optimization
type WhereAnalysis struct {
	LeftBundleConditions  []queryparser.WhereClause // Conditions that can be pushed down to left bundle
	RightBundleConditions []queryparser.WhereClause // Conditions that can be pushed down to right bundle
	CrossBundleConditions []queryparser.WhereClause // Conditions that must be evaluated after JOIN
	RemainingConditions   []queryparser.WhereClause // Complex conditions that need post-JOIN evaluation
}

// convertToJoinRequestWithWhereOptimization converts a parsed JOIN query to the format expected by the JOIN executor
// NEW: Now includes PostgreSQL-style predicate pushdown for optimal performance
func convertToJoinRequestWithWhereOptimization(joinQuery *queryparser.SelectJoinQuery, database *models.Database, serviceManager ServiceManager, logger *zap.SugaredLogger) (*joinexecutor.JoinRequest, *WhereAnalysis, error) {
	// For now, handle only the first JOIN clause (Phase 1 supports single JOIN)
	if len(joinQuery.JoinClauses) == 0 {
		return nil, nil, fmt.Errorf("no JOIN clauses found in query")
	}

	firstJoin := joinQuery.JoinClauses[0]
	leftBundleName := joinQuery.FromBundle
	rightBundleName := firstJoin.RightBundle

	// Analyze WHERE clause for optimization opportunities
	var whereAnalysis *WhereAnalysis
	if joinQuery.WhereClause != nil {
		whereAnalysis = analyzeWhereClauseForJoin(joinQuery.WhereClause, leftBundleName, rightBundleName, logger)
		logger.Infof("WHERE clause optimization: Found %d left-bundle, %d right-bundle, %d cross-bundle conditions",
			len(whereAnalysis.LeftBundleConditions),
			len(whereAnalysis.RightBundleConditions),
			len(whereAnalysis.CrossBundleConditions)+len(whereAnalysis.RemainingConditions))
	} else {
		whereAnalysis = &WhereAnalysis{}
	}

	// Get and pre-filter left bundle
	leftBundle, err := serviceManager.BundleService.GetBundleByName(database, leftBundleName)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to get left bundle '%s': %w", leftBundleName, err)
	}

	leftBundleAdapter, err := createFilteredBundleAdapter(leftBundle, whereAnalysis.LeftBundleConditions, serviceManager, logger, "left")
	if err != nil {
		return nil, nil, fmt.Errorf("failed to create filtered left bundle adapter: %w", err)
	}

	// Get and pre-filter right bundle
	rightBundle, err := serviceManager.BundleService.GetBundleByName(database, rightBundleName)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to get right bundle '%s': %w", rightBundleName, err)
	}

	rightBundleAdapter, err := createFilteredBundleAdapter(rightBundle, whereAnalysis.RightBundleConditions, serviceManager, logger, "right")
	if err != nil {
		return nil, nil, fmt.Errorf("failed to create filtered right bundle adapter: %w", err)
	}

	// Convert JOIN conditions to new format
	var conditions []joinexecutor.JoinCondition
	for _, condition := range firstJoin.JoinConditions {
		conditions = append(conditions, joinexecutor.JoinCondition{
			LeftKey:  condition.LeftField,
			RightKey: condition.RightField,
			Operator: condition.Operator,
		})
	}

	// Convert JOIN type
	var joinType joinexecutor.JoinType
	switch firstJoin.JoinType {
	case queryparser.InnerJoin:
		joinType = joinexecutor.InnerJoin
	case queryparser.LeftJoin:
		joinType = joinexecutor.LeftJoin
	case queryparser.RightJoin:
		joinType = joinexecutor.RightJoin
	case queryparser.FullOuterJoin:
		joinType = joinexecutor.FullOuterJoin
	default:
		joinType = joinexecutor.InnerJoin // Default to inner join
	}

	joinRequest := &joinexecutor.JoinRequest{
		LeftBundle:         leftBundleAdapter,
		RightBundle:        rightBundleAdapter,
		JoinType:           joinType,
		Conditions:         conditions,
		ExpectedResultSize: 10000,            // Reasonable default
		MemoryLimit:        64 * 1024 * 1024, // 64MB default
		AllowDiskSpillover: true,             // Enable disk spillover for large datasets
	}

	return joinRequest, whereAnalysis, nil
}

// createFilteredBundleAdapter creates a bundle adapter with pre-applied WHERE filtering
// This implements the pushdown optimization by filtering documents before they reach the JOIN executor
func createFilteredBundleAdapter(bundle *models.Bundle, conditions []queryparser.WhereClause, serviceManager ServiceManager, logger *zap.SugaredLogger, side string) (documentscanner.BundleInterface, error) {
	if len(conditions) == 0 {
		// No conditions to push down - return regular adapter
		logger.Infof("No conditions to push down to %s bundle '%s'", side, bundle.Name)
		return documentscanner.NewBundleAdapter(bundle, serviceManager.BundleService, logger), nil
	}

	// Build WHERE clause for this bundle (remove bundle prefixes)
	whereClause := buildWhereClauseFromConditions(conditions, true)
	logger.Infof("Pushing down WHERE clause to %s bundle '%s': %s", side, bundle.Name, whereClause)

	// Use modern page-based document filtering with architectural fix
	// The BundleService now properly handles page-based filtering without relying on legacy bundle.Documents
	filteredDocs, err := serviceManager.BundleService.GetDocumentsByFilter(bundle, whereClause, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to apply WHERE filter to %s bundle: %w", side, err)
	}

	// Get original document count for performance metrics
	originalCount := int(bundle.TotalDocuments)
	if originalCount == 0 {
		// Fallback: load all documents to get count (less efficient but accurate)
		allDocs, countErr := serviceManager.BundleService.GetDocumentsByFilter(bundle, "", nil)
		if countErr == nil {
			originalCount = len(allDocs)
		}
	}

	logger.Infof("Pre-filter optimization: %s bundle '%s' reduced from %d to %d documents (%.1f%% reduction)",
		side, bundle.Name, originalCount, len(filteredDocs),
		float64(originalCount-len(filteredDocs))/float64(originalCount)*100)

	// Create a filtered bundle with only the matching documents
	filteredBundle := &models.Bundle{
		BundleID:    bundle.BundleID,
		Name:        bundle.Name,
		Description: bundle.Description,
		CreatedAt:   bundle.CreatedAt,
		UpdatedAt:   bundle.UpdatedAt,
		Indexes:     bundle.Indexes, // Keep indexes for potential JOIN optimization
	}

	// Convert filtered documents to map format
	filteredDocMap := make(map[string]models.Document)
	for _, doc := range filteredDocs {
		filteredDocMap[doc.DocumentID] = *doc
	}
	filteredBundle.Documents = &filteredDocMap

	// Return adapter for the filtered bundle
	return documentscanner.NewBundleAdapter(filteredBundle, serviceManager.BundleService, logger), nil
}

// convertToJoinRequest converts a parsed JOIN query to the format expected by the JOIN executor
// Legacy function - kept for backward compatibility
func convertToJoinRequest(joinQuery *queryparser.SelectJoinQuery, database *models.Database, serviceManager ServiceManager, logger *zap.SugaredLogger) (*joinexecutor.JoinRequest, error) {
	// Get left bundle (FROM bundle)
	leftBundle, err := serviceManager.BundleService.GetBundleByName(database, joinQuery.FromBundle)
	if err != nil {
		return nil, fmt.Errorf("failed to get left bundle '%s': %w", joinQuery.FromBundle, err)
	}

	// NEW: Create bundle interface adapters for JOIN executor compatibility using page-based loading
	leftBundleAdapter := documentscanner.NewBundleAdapter(leftBundle, serviceManager.BundleService, logger)

	// For now, handle only the first JOIN clause (Phase 1 supports single JOIN)
	if len(joinQuery.JoinClauses) == 0 {
		return nil, fmt.Errorf("no JOIN clauses found in query")
	}

	firstJoin := joinQuery.JoinClauses[0]

	// Get right bundle
	rightBundle, err := serviceManager.BundleService.GetBundleByName(database, firstJoin.RightBundle)
	if err != nil {
		return nil, fmt.Errorf("failed to get right bundle '%s': %w", firstJoin.RightBundle, err)
	}

	// NEW: Create bundle interface adapter for right bundle using page-based loading
	rightBundleAdapter := documentscanner.NewBundleAdapter(rightBundle, serviceManager.BundleService, logger)

	// Convert JOIN conditions to new format
	var conditions []joinexecutor.JoinCondition
	for _, condition := range firstJoin.JoinConditions {
		conditions = append(conditions, joinexecutor.JoinCondition{
			LeftKey:  condition.LeftField,
			RightKey: condition.RightField,
			Operator: condition.Operator,
		})
	}

	// Convert JOIN type
	var joinType joinexecutor.JoinType
	switch firstJoin.JoinType {
	case queryparser.InnerJoin:
		joinType = joinexecutor.InnerJoin
	case queryparser.LeftJoin:
		joinType = joinexecutor.LeftJoin
	case queryparser.RightJoin:
		joinType = joinexecutor.RightJoin
	case queryparser.FullOuterJoin:
		joinType = joinexecutor.FullOuterJoin
	default:
		joinType = joinexecutor.InnerJoin // Default to inner join
	}

	return &joinexecutor.JoinRequest{
		LeftBundle:         leftBundleAdapter,
		RightBundle:        rightBundleAdapter,
		JoinType:           joinType,
		Conditions:         conditions,
		ExpectedResultSize: 10000,            // Reasonable default
		MemoryLimit:        64 * 1024 * 1024, // 64MB default
		AllowDiskSpillover: true,             // Enable disk spillover for large datasets
	}, nil
}

// mergeJoinedDocument merges left and right documents from a JOIN result into a single document
// LIFECYCLE: After this function copies fields to the final result document, the input JoinedDocument
// is automatically returned to the pool via deferred cleanup in the calling function.
// This follows the same pattern as document_pool.go's FreeDocuments() for bulk cleanup.
func mergeJoinedDocument(joinedDoc *joinexecutor.JoinedDocument, logger *zap.SugaredLogger) *models.Document {
	// Create new document with combined fields
	mergedFields := make(map[string]models.Field)

	// Add fields from left document with prefix to avoid conflicts
	if joinedDoc.LeftDocument != nil {
		for fieldName, field := range joinedDoc.LeftDocument.Fields {
			prefixedName := fmt.Sprintf("left_%s", fieldName)
			mergedFields[prefixedName] = field
		}
	}

	// Add fields from right document with prefix
	if joinedDoc.RightDocument != nil {
		for fieldName, field := range joinedDoc.RightDocument.Fields {
			prefixedName := fmt.Sprintf("right_%s", fieldName)
			mergedFields[prefixedName] = field
		}
	}

	// Add join metadata
	mergedFields["join_key"] = models.Field{
		Name:  "join_key",
		Value: models.NewStringValue(joinedDoc.JoinKey),
	}

	// STEP 1: Use document pool to reduce allocations
	// TODO: Option C - Implement reference counting for automatic pool return
	// Create merged document
	mergedDoc := document.GetPooledDocument()
	mergedDoc.DocumentID = fmt.Sprintf("join_%s", joinedDoc.JoinKey)
	mergedDoc.Fields = mergedFields
	mergedDoc.CreatedAt = time.Now()
	mergedDoc.UpdatedAt = time.Now()

	return mergedDoc
}

// BundleAdapter adapts a Bundle to implement the BundleInterface required by JOIN executor
// This adapter bridges the existing Bundle model with the JOIN executor's interface requirements
type BundleAdapter struct {
	bundle *models.Bundle
	logger *zap.SugaredLogger
}

// GetDocumentIDs returns all document IDs in the bundle
func (ba *BundleAdapter) GetDocumentIDs() []string {
	if ba.bundle == nil || ba.bundle.Documents == nil {
		return []string{}
	}

	ids := make([]string, 0, len(*ba.bundle.Documents))
	for docID := range *ba.bundle.Documents {
		ids = append(ids, docID)
	}

	return ids
}

// GetDocument retrieves a document by its ID
func (ba *BundleAdapter) GetDocument(docID string) *models.Document {
	if ba.bundle == nil || ba.bundle.Documents == nil {
		return nil
	}

	documents := *ba.bundle.Documents
	doc, exists := documents[docID]
	if !exists {
		return nil
	}

	return &doc
}

// GetAllDocuments returns all documents in the bundle as a map
func (ba *BundleAdapter) GetAllDocuments() map[string]*models.Document {
	if ba.bundle == nil || ba.bundle.Documents == nil {
		return make(map[string]*models.Document)
	}

	// Convert from map[string]models.Document to map[string]*models.Document
	result := make(map[string]*models.Document)
	documents := *ba.bundle.Documents
	for docID, doc := range documents {
		docCopy := doc // Create copy to avoid address issues
		result[docID] = &docCopy
	}

	return result
}

// GetName returns the bundle name for logging and metrics
func (ba *BundleAdapter) GetName() string {
	if ba.bundle == nil {
		return "unknown_bundle"
	}
	return ba.bundle.Name
}

// GetTotalDocuments returns the total number of documents in the bundle
func (ba *BundleAdapter) GetTotalDocuments() int {
	if ba.bundle == nil || ba.bundle.Documents == nil {
		return 0
	}
	return len(*ba.bundle.Documents)
}

// HIERARCHICAL TRANSFORMATION FUNCTIONS

// transformToHierarchicalResults converts flat JOIN results to hierarchical documents
// using the new results package transformation logic
func transformToHierarchicalResults(joinResults []*joinexecutor.JoinedDocument, joinQuery *queryparser.SelectJoinQuery, database *models.Database, serviceManager ServiceManager, logger *zap.SugaredLogger) (map[string]*models.Document, error) {
	if len(joinResults) == 0 {
		return make(map[string]*models.Document), nil
	}

	// Import the results package for hierarchical transformation
	// Note: We need to add this import at the top of the file
	logger.Infof("Starting hierarchical transformation for %d JOIN results", len(joinResults))

	// Get bundles for relationship analysis
	leftBundle, err := serviceManager.BundleService.GetBundleByName(database, joinQuery.FromBundle)
	if err != nil {
		return nil, fmt.Errorf("error retrieving left bundle '%s': %w", joinQuery.FromBundle, err)
	}

	var rightBundle *models.Bundle
	if len(joinQuery.JoinClauses) > 0 {
		rightBundle, err = serviceManager.BundleService.GetBundleByName(database, joinQuery.JoinClauses[0].RightBundle)
		if err != nil {
			return nil, fmt.Errorf("error retrieving right bundle '%s': %w", joinQuery.JoinClauses[0].RightBundle, err)
		}
	} else {
		return nil, fmt.Errorf("no JOIN clauses found in query")
	}

	// Extract join conditions for relationship analysis
	var joinConditions []results.JoinCondition
	for _, clause := range joinQuery.JoinClauses {
		for _, condition := range clause.JoinConditions {
			joinConditions = append(joinConditions, results.JoinCondition{
				LeftField:  condition.LeftField,
				RightField: condition.RightField,
				Operator:   condition.Operator,
			})
		}
	}

	// Analyze the relationship
	analyzer := results.NewRelationshipAnalyzer(logger)
	analysisRequest := results.RelationshipAnalysisRequest{
		LeftBundle:       leftBundle,
		RightBundle:      rightBundle,
		JoinConditions:   joinConditions,
		RelationshipName: joinQuery.RelationshipName,
		Logger:           logger,
	}

	analysisResult := analyzer.AnalyzeRelationship(analysisRequest)
	if !analysisResult.IsSupported {
		return nil, fmt.Errorf("relationship analysis failed: %s", analysisResult.ErrorMessage)
	}

	// Transform using hierarchical transformer
	transformer := results.NewHierarchicalTransformer(logger)
	transformRequest := results.HierarchicalTransformRequest{
		JoinResults:    joinResults,
		Relationship:   analysisResult.Metadata,
		SelectedFields: joinQuery.SelectFields,
		Logger:         logger,
	}

	transformResult, err := transformer.Transform(transformRequest)
	if err != nil {
		return nil, fmt.Errorf("hierarchical transformation failed: %w", err)
	}

	logger.Infof("Hierarchical transformation completed: %d parent documents with %d total child documents in %v",
		transformResult.ParentCount, transformResult.TotalChildDocuments, transformResult.TransformationTime)

	return transformResult.Documents, nil
}

// transformHierarchicalToResponse converts hierarchical documents to response format
// This function extracts the nested field values for JSON serialization
func transformHierarchicalToResponse(documents map[string]*models.Document) []map[string]interface{} {
	var response []map[string]interface{}

	for _, doc := range documents {
		docMap := make(map[string]interface{})

		// Add standard document fields
		docMap["DocumentID"] = doc.DocumentID
		docMap["CreatedAt"] = doc.CreatedAt.Format(time.RFC3339)
		docMap["UpdatedAt"] = doc.UpdatedAt.Format(time.RFC3339)

		// Add all document fields
		for fieldName, field := range doc.Fields {
			docMap[fieldName] = field.Value
		}

		response = append(response, docMap)
	}

	return response
}

// WHERE CLAUSE OPTIMIZATION FUNCTIONS

// analyzeWhereClauseForJoin analyzes WHERE conditions to determine optimization opportunities
// This implements PostgreSQL-style predicate pushdown by separating bundle-specific conditions
// from cross-bundle conditions that require joined data to evaluate
func analyzeWhereClauseForJoin(whereGroup *queryparser.WhereGroup, leftBundle, rightBundle string, logger *zap.SugaredLogger) *WhereAnalysis {
	if whereGroup == nil {
		return &WhereAnalysis{}
	}

	analysis := &WhereAnalysis{
		LeftBundleConditions:  make([]queryparser.WhereClause, 0, 10),
		RightBundleConditions: make([]queryparser.WhereClause, 0, 10),
		CrossBundleConditions: make([]queryparser.WhereClause, 0, 5),
		RemainingConditions:   make([]queryparser.WhereClause, 0, 5),
	}

	// Analyze each clause in the WHERE group
	analyzeWhereConditions(whereGroup.Clauses, leftBundle, rightBundle, analysis, logger)

	// Recursively analyze subgroups
	for _, subGroup := range whereGroup.SubGroups {
		subAnalysis := analyzeWhereClauseForJoin(&subGroup, leftBundle, rightBundle, logger)

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

// buildWhereClauseFromConditions reconstructs a WHERE clause string from a list of conditions
func buildWhereClauseFromConditions(conditions []queryparser.WhereClause, removeBundlePrefix bool) string {
	if len(conditions) == 0 {
		return ""
	}

	var parts []string
	for i, condition := range conditions {
		if i > 0 {
			parts = append(parts, "AND")
		}

		// Optionally remove bundle prefix for single-bundle filtering
		fieldName := condition.Field
		if removeBundlePrefix && strings.Contains(fieldName, ".") {
			fieldParts := strings.Split(fieldName, ".")
			if len(fieldParts) == 2 {
				fieldName = strings.Trim(fieldParts[1], "\"'")
			}
		}

		// Format the condition based on value type
		if condition.Value != nil {
			switch v := condition.Value.(type) {
			case string:
				parts = append(parts, fmt.Sprintf("\"%s\" %s \"%s\"", fieldName, condition.Operator, v))
			default:
				parts = append(parts, fmt.Sprintf("\"%s\" %s %v", fieldName, condition.Operator, v))
			}
		}
	}

	return strings.Join(parts, " ")
}

// applyPostJoinFiltering applies cross-bundle and remaining WHERE conditions to joined results
// This handles conditions that couldn't be pushed down to individual bundles
func applyPostJoinFiltering(joinedDocs []*joinexecutor.JoinedDocument, whereAnalysis *WhereAnalysis, logger *zap.SugaredLogger) ([]*joinexecutor.JoinedDocument, error) {
	if whereAnalysis == nil {
		return joinedDocs, nil
	}

	// Combine cross-bundle and remaining conditions
	postJoinConditions := append(whereAnalysis.CrossBundleConditions, whereAnalysis.RemainingConditions...)

	if len(postJoinConditions) == 0 {
		logger.Infof("No post-JOIN filtering needed")
		return joinedDocs, nil
	}

	logger.Infof("Applying post-JOIN filtering with %d conditions", len(postJoinConditions))

	var filteredDocs []*joinexecutor.JoinedDocument
	for _, joinedDoc := range joinedDocs {
		// Convert joined document to a format that can be evaluated by WHERE clause logic
		if shouldIncludeJoinedDocument(joinedDoc, postJoinConditions, logger) {
			filteredDocs = append(filteredDocs, joinedDoc)
		}
	}

	logger.Infof("Post-JOIN filtering: %d documents reduced to %d documents (%.1f%% reduction)",
		len(joinedDocs), len(filteredDocs),
		float64(len(joinedDocs)-len(filteredDocs))/float64(len(joinedDocs))*100)

	return filteredDocs, nil
}

// shouldIncludeJoinedDocument evaluates if a joined document meets the post-JOIN WHERE conditions
func shouldIncludeJoinedDocument(joinedDoc *joinexecutor.JoinedDocument, conditions []queryparser.WhereClause, logger *zap.SugaredLogger) bool {
	// Create a virtual document that combines fields from both sides for evaluation
	virtualDoc := createVirtualDocumentForEvaluation(joinedDoc)

	// Evaluate each condition against the virtual document
	for _, condition := range conditions {
		if !evaluateConditionOnVirtualDocument(virtualDoc, condition, logger) {
			// If any condition fails, exclude this document
			return false
		}
	}

	return true
}

// createVirtualDocumentForEvaluation creates a combined document from joined results for WHERE evaluation
func createVirtualDocumentForEvaluation(joinedDoc *joinexecutor.JoinedDocument) map[string]interface{} {
	virtualDoc := make(map[string]interface{})

	// Add fields from left document with bundle prefix
	if joinedDoc.LeftDocument != nil {
		for fieldName, field := range joinedDoc.LeftDocument.Fields {
			// Add both prefixed and unprefixed versions for flexibility
			virtualDoc[fieldName] = field.Value
			if joinedDoc.LeftDocument.DocumentID != "" {
				// Use the bundle name if available, otherwise use "left"
				virtualDoc["left."+fieldName] = field.Value
			}
		}
	}

	// Add fields from right document with bundle prefix
	if joinedDoc.RightDocument != nil {
		for fieldName, field := range joinedDoc.RightDocument.Fields {
			// Add both prefixed and unprefixed versions for flexibility
			virtualDoc[fieldName] = field.Value
			if joinedDoc.RightDocument.DocumentID != "" {
				// Use the bundle name if available, otherwise use "right"
				virtualDoc["right."+fieldName] = field.Value
			}
		}
	}

	return virtualDoc
}

// evaluateConditionOnVirtualDocument evaluates a single WHERE condition on the virtual joined document
func evaluateConditionOnVirtualDocument(virtualDoc map[string]interface{}, condition queryparser.WhereClause, logger *zap.SugaredLogger) bool {
	fieldName := condition.Field

	// Handle bundle-qualified field names (e.g., "Authors"."Age")
	if strings.Contains(fieldName, ".") {
		parts := strings.Split(fieldName, ".")
		if len(parts) == 2 {
			bundleName := strings.Trim(parts[0], "\"'")
			actualFieldName := strings.Trim(parts[1], "\"'")

			// Try to find the field with bundle prefix
			qualifiedFieldName := strings.ToLower(bundleName) + "." + actualFieldName
			if value, exists := virtualDoc[qualifiedFieldName]; exists {
				return evaluateFieldCondition(value, condition, logger)
			}

			// Fallback to unqualified field name
			if value, exists := virtualDoc[actualFieldName]; exists {
				return evaluateFieldCondition(value, condition, logger)
			}
		}
	} else {
		// Direct field name lookup
		if value, exists := virtualDoc[fieldName]; exists {
			return evaluateFieldCondition(value, condition, logger)
		}
	}

	// Field not found - condition fails
	logger.Debugf("Field '%s' not found in joined document for condition evaluation", fieldName)
	return false
}

// evaluateFieldCondition evaluates a condition against a specific field value
func evaluateFieldCondition(fieldValue interface{}, condition queryparser.WhereClause, logger *zap.SugaredLogger) bool {
	switch condition.Operator {
	case "==", "=":
		return compareValues(fieldValue, condition.Value, logger, func(a, b float64) bool { return a == b })
	case "!=", "<>":
		return !compareValues(fieldValue, condition.Value, logger, func(a, b float64) bool { return a == b })
	case ">":
		return compareValues(fieldValue, condition.Value, logger, func(a, b float64) bool { return a > b })
	case ">=":
		return compareValues(fieldValue, condition.Value, logger, func(a, b float64) bool { return a >= b })
	case "<":
		return compareValues(fieldValue, condition.Value, logger, func(a, b float64) bool { return a < b })
	case "<=":
		return compareValues(fieldValue, condition.Value, logger, func(a, b float64) bool { return a <= b })
	default:
		logger.Warnf("Unsupported operator '%s' in post-JOIN filtering", condition.Operator)
		return false
	}
}

// compareValues compares two values with type conversion support
func compareValues(a, b interface{}, logger *zap.SugaredLogger, numericComparison func(float64, float64) bool) bool {
	// Handle nil values
	if a == nil && b == nil {
		return true
	}
	if a == nil || b == nil {
		return false
	}

	// Try string comparison first
	aStr, aIsString := a.(string)
	bStr, bIsString := b.(string)

	if aIsString && bIsString {
		return aStr == bStr
	}

	// Try numeric comparison
	aFloat, aErr := convertToFloat64(a)
	bFloat, bErr := convertToFloat64(b)

	if aErr == nil && bErr == nil {
		return numericComparison(aFloat, bFloat)
	}

	// Fallback to string representation comparison
	return fmt.Sprintf("%v", a) == fmt.Sprintf("%v", b)
}

// convertToFloat64 attempts to convert an interface{} to float64
func convertToFloat64(value interface{}) (float64, error) {
	switch v := value.(type) {
	case float64:
		return v, nil
	case float32:
		return float64(v), nil
	case int:
		return float64(v), nil
	case int32:
		return float64(v), nil
	case int64:
		return float64(v), nil
	case string:
		return strconv.ParseFloat(v, 64)
	default:
		return 0, fmt.Errorf("cannot convert %T to float64", value)
	}
}
