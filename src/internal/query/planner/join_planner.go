/*
JOIN QUERY PLANNER SYSTEM

This file extends the existing query planner to handle JOIN operations in SyndrDB.
It implements PostgreSQL-style join planning logic that selects the most efficient
join algorithm based on data characteristics and available indexes.

JOIN PLANNING STRATEGY:
1. Analyzes join conditions and available indexes
2. Estimates costs for different join algorithms:
   - Nested Loop Join: Good for small datasets or indexed lookups
   - Hash Join: Efficient for equality joins with large datasets
   - Merge Join: Optimal when both sides are sorted
3. Selects the lowest-cost join algorithm
4. Creates an execution plan tree with appropriate join nodes

COST-BASED OPTIMIZATION:
Following PostgreSQL's approach, the planner estimates:
- I/O costs for reading data
- CPU costs for processing and comparison
- Memory usage for hash tables
- Sort costs when required

RELATIONSHIP INTEGRATION:
The planner can leverage existing relationships between bundles to optimize
join operations by using relationship metadata and indexes.

This implementation follows the Single Responsibility Principle by focusing
on join planning while delegating execution to specialized join nodes.
*/

package planner

import (
	"fmt"
	"strings"
	"syndrdb/src/internal/domain/models"
	"syndrdb/src/internal/query/documentscanner"
	joinexecutor "syndrdb/src/internal/query/join_executor" // NEW: For JOIN executor integration
	"syndrdb/src/internal/query/queryparser"
	"time" // NEW: For document timestamps

	"go.uber.org/zap"
)

// JoinPlannerInterface extends the existing planner interface for JOIN operations
type JoinPlannerInterface interface {
	BundleServiceInterface
	CreateJoinExecutionPlan(query *queryparser.SelectJoinQuery, database *models.Database) (*ExecutionPlan, error)
}

// JoinQueryPlanner extends the existing QueryPlanner with JOIN capabilities
type JoinQueryPlanner struct {
	*QueryPlanner // Embed existing planner
}

// NewJoinQueryPlanner creates a new join-capable query planner
func NewJoinQueryPlanner(logger *zap.SugaredLogger, bundleServiceInt BundleServiceInterface, bundleService interface {
	GetOrCreateDocumentScanner(bundle *models.Bundle) (documentscanner.DocumentScannerInterface, error)
}) *JoinQueryPlanner {
	return &JoinQueryPlanner{
		QueryPlanner: NewQueryPlannerWithService(logger, bundleServiceInt, bundleService),
	}
}

// CreateJoinExecutionPlan creates an execution plan for a JOIN query
// NEW: Now uses the Phase 1 JOIN executor for improved performance and extensibility
func (jp *JoinQueryPlanner) CreateJoinExecutionPlan(query *queryparser.SelectJoinQuery, database *models.Database) (*ExecutionPlan, error) {
	jp.Logger.Infof("Creating execution plan using new JOIN executor: FROM %s with %d joins",
		query.FromBundle, len(query.JoinClauses))

	// Validate that all referenced bundles exist
	bundles := make(map[string]*models.Bundle)

	// Get the primary bundle
	// fromBundle, exists := database.Bundles[query.FromBundle]
	// if !exists {
	// 	return nil, fmt.Errorf("bundle '%s' does not exist", query.FromBundle)
	// }
	fromBundle, err := jp.BundleServiceInt.GetBundleByName(database, query.FromBundle)
	if err != nil {
		return nil, fmt.Errorf("bundle '%s' does not exist: %w", query.FromBundle, err)
	}
	bundles[query.FromBundle] = fromBundle

	// Get all joined bundles
	for _, joinClause := range query.JoinClauses {
		// rightBundle, exists := database.Bundles[joinClause.RightBundle]
		// if !exists {
		// 	return nil, fmt.Errorf("joined bundle '%s' does not exist", joinClause.RightBundle)
		// }
		rightBundle, err := jp.BundleServiceInt.GetBundleByName(database, joinClause.RightBundle)
		if err != nil {
			return nil, fmt.Errorf("joined bundle '%s' had an err %v", joinClause.RightBundle, err)
		}
		bundles[joinClause.RightBundle] = rightBundle
	}

	// Validate the query
	if err := queryparser.ValidateJoinQuery(query, bundles, jp.Logger); err != nil {
		return nil, fmt.Errorf("query validation failed: %w", err)
	}

	// NEW: Create JOIN executor with pattern tracking
	joinExecutor := joinexecutor.NewDefaultJoinExecutor(jp.Logger, 64*1024*1024) // 64MB memory limit

	// Estimate execution cost based on bundle sizes
	// NOTE: Use TotalDocuments metadata instead of Documents field (which is nil for paginated bundles)
	leftSize := int(bundles[query.FromBundle].TotalDocuments)
	rightSize := 0
	if len(query.JoinClauses) > 0 {
		rightSize = int(bundles[query.JoinClauses[0].RightBundle].TotalDocuments)
	}

	// Cost estimation: hash join cost is roughly O(M + N) where M and N are bundle sizes
	estimatedCost := float64(leftSize + rightSize)
	estimatedRows := leftSize / 10 // Assume 10% selectivity as default

	// NEW: Predicate pushdown optimization for WHERE clause
	// NOTE: For now, we apply WHERE filtering after JOIN using WhereExpression
	// TODO: Implement Expression-based predicate pushdown analysis in future
	var leftBundleInterface, rightBundleInterface documentscanner.BundleInterface
	hasWhereExpression := query.WhereExpression != nil

	if hasWhereExpression {
		jp.Logger.Info("WHERE expression detected - will apply post-JOIN filtering")
		// Predicate pushdown for Expressions is not yet implemented
		// For now, we'll apply the entire WHERE clause after JOIN execution
	}

	// NEW: Create service manager adapter for the execution node
	serviceManager := &PlannerServiceManager{
		bundles:       bundles,
		logger:        jp.Logger,
		bundleService: jp.BundleServiceInt,
	}

	// Create the new JOIN execution node with optional filtered bundle interfaces
	joinNode := &JoinExecutionNode{
		Query:                query,
		Database:             database,
		ServiceManager:       serviceManager,
		Logger:               jp.Logger,
		Cost:                 estimatedCost,
		EstimatedRows:        estimatedRows,
		JoinExecutor:         joinExecutor,
		LeftBundleInterface:  leftBundleInterface,  // May be nil or FilteredBundleAdapter
		RightBundleInterface: rightBundleInterface, // May be nil or FilteredBundleAdapter
	}

	jp.Logger.Infof("Created JOIN execution plan: cost=%.2f, estimated_rows=%d, algorithm=hash_join",
		estimatedCost, estimatedRows)

	// Wrap with FilterNode only if there are remaining WHERE conditions
	var rootNode ExecutionNode = joinNode
	finalCost := estimatedCost
	finalEstimatedRows := estimatedRows

	// Check for Expression-based WHERE filtering (new unified parser)
	if hasWhereExpression {
		// Create FilterNode to apply WHERE expression after JOIN
		filterNode := &FilterNode{
			Child:           joinNode,
			WhereExpression: query.WhereExpression,
			Cost:            estimatedCost + float64(estimatedRows)*0.1, // Add filter cost
			EstimatedRows:   estimatedRows / 10,                         // Assume 10% selectivity
			Logger:          jp.Logger,
		}
		rootNode = filterNode
		finalCost = filterNode.Cost
		finalEstimatedRows = filterNode.EstimatedRows
		jp.Logger.Info("Wrapped JOIN with FilterNode using WhereExpression (unified parser)")
	} else if leftBundleInterface != nil || rightBundleInterface != nil {
		jp.Logger.Info("All WHERE conditions pushed down - no post-JOIN filtering needed")
	}

	// Create the final execution plan using the wrapped node
	plan := &ExecutionPlan{
		RootNode:      rootNode,
		Cost:          finalCost,
		EstimatedRows: finalEstimatedRows,
		IndexesUsed:   []string{}, // Phase 1: Basic implementation, Phase 2 will add index usage tracking
		Logger:        jp.Logger,
	}

	jp.Logger.Infof("Created new JOIN execution plan with cost %.2f, estimated rows: %d",
		plan.Cost, plan.EstimatedRows)

	return plan, nil
}

// chooseBestJoinAlgorithm selects the most efficient join algorithm
func (jp *JoinQueryPlanner) chooseBestJoinAlgorithm(leftNode, rightNode ExecutionNode, joinClause queryparser.JoinClause) (ExecutionNode, error) {
	jp.Logger.Debugf("Choosing join algorithm for %d conditions", len(joinClause.JoinConditions))

	// Create candidate join nodes
	candidates := make([]ExecutionNode, 0, 3)

	// Nested Loop Join - always available
	nestedLoopNode := NewNestedLoopJoinNode(leftNode, rightNode, joinClause.JoinConditions, joinClause.JoinType, jp.Logger)
	candidates = append(candidates, nestedLoopNode)

	// Hash Join - only for equality joins
	if jp.hasEqualityJoinConditions(joinClause.JoinConditions) {
		hashJoinNode := NewHashJoinNode(leftNode, rightNode, joinClause.JoinConditions, joinClause.JoinType, jp.Logger)
		candidates = append(candidates, hashJoinNode)
	}

	// Merge Join - only if both sides can be sorted efficiently
	// For now, we'll skip merge join as it requires more complex sort detection
	// TODO: Implement merge join when we have better sort detection

	// Choose the candidate with the lowest cost
	var bestNode ExecutionNode
	bestCost := float64(^uint(0) >> 1) // Max float64

	for _, candidate := range candidates {
		cost := candidate.GetCost()
		jp.Logger.Debugf("Candidate %T: cost=%.2f, rows=%d", candidate, cost, candidate.GetEstimatedRows())

		if cost < bestCost {
			bestCost = cost
			bestNode = candidate
		}
	}

	if bestNode == nil {
		return nil, fmt.Errorf("no suitable join algorithm found")
	}

	return bestNode, nil
}

// hasEqualityJoinConditions checks if all join conditions use equality
func (jp *JoinQueryPlanner) hasEqualityJoinConditions(conditions []queryparser.JoinCondition) bool {
	for _, condition := range conditions {
		if condition.Operator != "==" {
			return false
		}
	}
	return true
}

// createRightSideNode creates an execution node for the right side of a join
func (jp *JoinQueryPlanner) createRightSideNode(bundle *models.Bundle, whereClause *queryparser.WhereGroup, bundleName string) ExecutionNode {
	// Check if there are WHERE conditions that apply to this bundle
	if whereClause != nil {
		bundleConditions := jp.extractBundleConditions(whereClause, bundleName)
		if len(bundleConditions) > 0 {
			// Try to create an optimized plan for this bundle
			whereClauseStr := jp.reconstructWhereClause(bundleConditions)
			plan, err := jp.QueryPlanner.CreateExecutionPlan(bundle, whereClauseStr)
			if err == nil {
				jp.Logger.Debugf("Created optimized right-side plan for bundle '%s'", bundleName)
				return plan.RootNode
			}
			jp.Logger.Warnf("Failed to create optimized plan for bundle '%s', using full scan: %v", bundleName, err)
		}
	}

	// Default to full scan
	return &FullScanNode{
		Bundle:           bundle,
		Cost:             float64(len(*bundle.Documents)),
		EstimatedRows:    len(*bundle.Documents),
		Logger:           jp.Logger,
		BundleServiceInt: jp.BundleServiceInt,
	}
}

// extractBundleConditions extracts WHERE conditions that apply to a specific bundle
func (jp *JoinQueryPlanner) extractBundleConditions(whereGroup *queryparser.WhereGroup, bundleName string) []queryparser.WhereClause {
	var conditions []queryparser.WhereClause

	// Extract conditions from direct clauses
	for _, clause := range whereGroup.Clauses {
		// Check if this condition references the bundle (either directly or with bundle prefix)
		if jp.conditionAppliesToBundle(clause, bundleName) {
			conditions = append(conditions, clause)
		}
	}

	// Recursively extract from subgroups
	for _, subGroup := range whereGroup.SubGroups {
		subConditions := jp.extractBundleConditions(&subGroup, bundleName)
		conditions = append(conditions, subConditions...)
	}

	return conditions
}

// conditionAppliesToBundle checks if a WHERE condition applies to a specific bundle
func (jp *JoinQueryPlanner) conditionAppliesToBundle(clause queryparser.WhereClause, bundleName string) bool {
	// Check for bundle-prefixed field names like "Bundle_Name.Field_Name"
	if strings.Contains(clause.Field, ".") {
		parts := strings.Split(clause.Field, ".")
		if len(parts) == 2 && parts[0] == bundleName {
			return true
		}
	}

	// For now, assume unprefixed fields apply to the current bundle
	// TODO: Improve this logic when we have better field resolution
	return !strings.Contains(clause.Field, ".")
}

// extractRemainingConditions extracts WHERE conditions that haven't been pushed down
func (jp *JoinQueryPlanner) extractRemainingConditions(whereGroup *queryparser.WhereGroup, bundles map[string]*models.Bundle) []queryparser.WhereClause {
	var conditions []queryparser.WhereClause

	// For now, include all conditions that reference multiple bundles
	for _, clause := range whereGroup.Clauses {
		// This is a simplified implementation - in practice, we'd need more sophisticated analysis
		if strings.Contains(clause.Field, ".") {
			// This might be a cross-bundle condition that needs to be evaluated after the join
			conditions = append(conditions, clause)
		}
	}

	// Recursively extract from subgroups
	for _, subGroup := range whereGroup.SubGroups {
		subConditions := jp.extractRemainingConditions(&subGroup, bundles)
		conditions = append(conditions, subConditions...)
	}

	return conditions
}

// reconstructWhereClause reconstructs a WHERE clause string from conditions
func (jp *JoinQueryPlanner) reconstructWhereClause(conditions []queryparser.WhereClause) string {
	if len(conditions) == 0 {
		return ""
	}

	var parts []string
	for i, condition := range conditions {
		if i > 0 {
			parts = append(parts, "AND")
		}

		// Remove bundle prefix if present
		fieldName := condition.Field
		if strings.Contains(fieldName, ".") {
			parts := strings.Split(fieldName, ".")
			if len(parts) == 2 {
				fieldName = parts[1]
			}
		}

		// Format the condition
		if condition.Value != nil {
			switch v := condition.Value.(type) {
			case string:
				parts = append(parts, fmt.Sprintf("%s %s \"%s\"", fieldName, condition.Operator, v))
			default:
				parts = append(parts, fmt.Sprintf("%s %s %v", fieldName, condition.Operator, v))
			}
		}
	}

	return strings.Join(parts, " ")
}

// extractAllWhereClauses recursively extracts all WHERE clauses from a WhereGroup
// This is used to apply filtering after JOIN execution
func (jp *JoinQueryPlanner) extractAllWhereClauses(whereGroup *queryparser.WhereGroup) []queryparser.WhereClause {
	if whereGroup == nil {
		return []queryparser.WhereClause{}
	}

	var allClauses []queryparser.WhereClause

	// Add direct clauses
	allClauses = append(allClauses, whereGroup.Clauses...)

	// Recursively add clauses from subgroups
	for _, subGroup := range whereGroup.SubGroups {
		subClauses := jp.extractAllWhereClauses(&subGroup)
		allClauses = append(allClauses, subClauses...)
	}

	return allClauses
}

// NEW JOIN INTEGRATION: JoinExecutionNode adapts the new JOIN executor to work with execution plans
// This bridges the new Phase 1 JOIN executor with the existing planner infrastructure
type JoinExecutionNode struct {
	Query          *queryparser.SelectJoinQuery // Original JOIN query
	Database       *models.Database             // Database context
	ServiceManager interface {                  // Service manager for bundle operations
		GetBundleByName(database *models.Database, name string) (*models.Bundle, error)
	}
	Logger               *zap.SugaredLogger              // Logger for debugging
	Cost                 float64                         // Estimated execution cost
	EstimatedRows        int                             // Estimated result rows
	JoinExecutor         joinexecutor.JoinExecutor       // NEW: The Phase 1 JOIN executor
	joinedResults        []*joinexecutor.JoinedDocument  // PHASE 3: Store JoinedDocument results for hierarchical transformation
	LeftBundleInterface  documentscanner.BundleInterface // NEW: Optional filtered bundle for LEFT side (predicate pushdown)
	RightBundleInterface documentscanner.BundleInterface // NEW: Optional filtered bundle for RIGHT side (predicate pushdown)
}

// Execute implements ExecutionNode interface using the new JOIN executor
func (jen *JoinExecutionNode) Execute() (map[string]*models.Document, error) {
	jen.Logger.Infof("Executing JOIN using Phase 1 JOIN executor")

	// Convert query to JOIN request format
	joinRequest, err := jen.convertQueryToJoinRequest()
	if err != nil {
		return nil, fmt.Errorf("failed to convert query to JOIN request: %w", err)
	}

	// Execute JOIN using the new executor
	result, err := jen.JoinExecutor.Execute(joinRequest)
	if err != nil {
		return nil, fmt.Errorf("JOIN execution failed: %w", err)
	}

	// PHASE 3: Store JoinedDocument results for hierarchical transformation
	jen.joinedResults = result.Documents

	// Convert JOIN results back to document map
	documents := make(map[string]*models.Document)
	for i, joinedDoc := range result.Documents {
		// Create merged document from JOIN result
		mergedDoc := jen.mergeJoinedDocument(joinedDoc, i)
		documents[mergedDoc.DocumentID] = mergedDoc
	}

	jen.Logger.Infof("JOIN execution completed: %d results, algorithm=%s, memory=%d bytes",
		len(documents), result.Algorithm, result.MemoryUsed)

	return documents, nil
}

// GetCost implements ExecutionNode interface
func (jen *JoinExecutionNode) GetCost() float64 {
	return jen.Cost
}

// GetEstimatedRows implements ExecutionNode interface
func (jen *JoinExecutionNode) GetEstimatedRows() int {
	return jen.EstimatedRows
}

// GetJoinedResults returns the JoinedDocument results for hierarchical transformation
// PHASE 3: Expose JOIN results to hierarchical transformer
func (jen *JoinExecutionNode) GetJoinedResults() []*joinexecutor.JoinedDocument {
	return jen.joinedResults
}

// convertQueryToJoinRequest converts the parsed query to JOIN executor format
func (jen *JoinExecutionNode) convertQueryToJoinRequest() (*joinexecutor.JoinRequest, error) {
	// Get left bundle
	leftBundle, err := jen.ServiceManager.GetBundleByName(jen.Database, jen.Query.FromBundle)
	if err != nil {
		return nil, fmt.Errorf("failed to get left bundle '%s': %w", jen.Query.FromBundle, err)
	}

	// Handle first JOIN clause (Phase 1 supports single JOIN)
	if len(jen.Query.JoinClauses) == 0 {
		return nil, fmt.Errorf("no JOIN clauses found")
	}

	firstJoin := jen.Query.JoinClauses[0]

	// Get right bundle
	rightBundle, err := jen.ServiceManager.GetBundleByName(jen.Database, firstJoin.RightBundle)
	if err != nil {
		return nil, fmt.Errorf("failed to get right bundle '%s': %w", firstJoin.RightBundle, err)
	}

	// Create bundle adapters with bundleService for document loading
	// Extract bundleService from ServiceManager if it implements the full interface
	var bundleService BundleServiceInterface
	if psm, ok := jen.ServiceManager.(*PlannerServiceManager); ok {
		bundleService = psm.bundleService
	}

	// Use filtered bundle interfaces if predicate pushdown was applied
	// Otherwise create standard adapters from bundles
	var leftAdapter, rightAdapter documentscanner.BundleInterface

	if jen.LeftBundleInterface != nil {
		// Use the filtered adapter created during query planning (predicate pushdown)
		leftAdapter = jen.LeftBundleInterface
		jen.Logger.Infof("Using predicate-filtered LEFT bundle adapter")
	} else {
		// Create standard adapter
		leftAdapter = &PlannerBundleAdapter{
			bundle:        leftBundle,
			logger:        jen.Logger,
			bundleService: bundleService,
		}
	}

	if jen.RightBundleInterface != nil {
		// Use the filtered adapter created during query planning (predicate pushdown)
		rightAdapter = jen.RightBundleInterface
		jen.Logger.Infof("Using predicate-filtered RIGHT bundle adapter")
	} else {
		// Create standard adapter
		rightAdapter = &PlannerBundleAdapter{
			bundle:        rightBundle,
			logger:        jen.Logger,
			bundleService: bundleService,
		}
	}

	// Convert JOIN conditions
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
		joinType = joinexecutor.InnerJoin
	}

	return &joinexecutor.JoinRequest{
		LeftBundle:         leftAdapter,
		RightBundle:        rightAdapter,
		JoinType:           joinType,
		Conditions:         conditions,
		ExpectedResultSize: int64(jen.EstimatedRows),
		MemoryLimit:        64 * 1024 * 1024, // 64MB default
		AllowDiskSpillover: true,
	}, nil
}

// mergeJoinedDocument creates a single document from JOIN results
func (jen *JoinExecutionNode) mergeJoinedDocument(joinedDoc *joinexecutor.JoinedDocument, index int) *models.Document {
	// Create merged document with fields from both sides
	mergedFields := make(map[string]models.Field)

	// Add left document fields WITHOUT prefix (commented out prefix code to fix WHERE clause filtering)
	if joinedDoc.LeftDocument != nil {
		for fieldName, field := range joinedDoc.LeftDocument.Fields {
			// ORIGINAL CODE (added prefixes that broke WHERE filtering):
			// prefixedName := fmt.Sprintf("left_%s", fieldName)
			// mergedFields[prefixedName] = field

			// NEW CODE: Use original field names without prefix
			mergedFields[fieldName] = field
		}
	}

	// Add right document fields WITHOUT prefix (commented out prefix code to fix WHERE clause filtering)
	if joinedDoc.RightDocument != nil {
		for fieldName, field := range joinedDoc.RightDocument.Fields {
			// ORIGINAL CODE (added prefixes that broke WHERE filtering):
			// prefixedName := fmt.Sprintf("right_%s", fieldName)
			// mergedFields[prefixedName] = field

			// NEW CODE: Use original field names without prefix
			// NOTE: This may cause field name collisions if left and right have same field names
			// In that case, right side will overwrite left side
			mergedFields[fieldName] = field
		}
	}

	// Add JOIN metadata
	mergedFields["join_key"] = models.Field{
		Name:  "join_key",
		Value: joinedDoc.JoinKey,
	}

	return &models.Document{
		//DocumentID: fmt.Sprintf("join_%d_%s", index, joinedDoc.JoinKey),
		DocumentID: joinedDoc.JoinKey,
		Fields:     mergedFields,
		CreatedAt:  time.Now(),
		UpdatedAt:  time.Now(),
	}
}

// PlannerBundleAdapter adapts a Bundle to implement BundleInterface for the JOIN executor
// This avoids circular import issues between planner and server packages
type PlannerBundleAdapter struct {
	bundle        *models.Bundle
	logger        *zap.SugaredLogger
	bundleService BundleServiceInterface
}

// GetDocumentIDs returns all document IDs in the bundle
func (pba *PlannerBundleAdapter) GetDocumentIDs() []string {
	if pba.bundle == nil {
		return []string{}
	}

	// Load documents via bundleService if available
	if pba.bundleService != nil {
		docs, err := pba.bundleService.GetAllDocumentsForIndexing(pba.bundle.Name)
		if err != nil {
			pba.logger.Warnf("Failed to load documents for bundle '%s': %v", pba.bundle.Name, err)
			return []string{}
		}
		ids := make([]string, 0, len(docs))
		for _, doc := range docs {
			ids = append(ids, doc.DocumentID)
		}
		return ids
	}

	// Fallback to deprecated Documents field (legacy bundles)
	if pba.bundle.Documents == nil {
		return []string{}
	}

	ids := make([]string, 0, len(*pba.bundle.Documents))
	for docID := range *pba.bundle.Documents {
		ids = append(ids, docID)
	}

	return ids
}

// GetDocument retrieves a document by its ID
func (pba *PlannerBundleAdapter) GetDocument(docID string) *models.Document {
	if pba.bundle == nil {
		return nil
	}

	// Load documents via bundleService if available
	if pba.bundleService != nil {
		docs, err := pba.bundleService.GetAllDocumentsForIndexing(pba.bundle.Name)
		if err != nil {
			pba.logger.Warnf("Failed to load documents for bundle '%s': %v", pba.bundle.Name, err)
			return nil
		}
		for _, doc := range docs {
			if doc.DocumentID == docID {
				return doc
			}
		}
		return nil
	}

	// Fallback to deprecated Documents field (legacy bundles)
	if pba.bundle.Documents == nil {
		return nil
	}

	documents := *pba.bundle.Documents
	doc, exists := documents[docID]
	if !exists {
		return nil
	}

	return &doc
}

// GetAllDocuments returns all documents in the bundle as a map
func (pba *PlannerBundleAdapter) GetAllDocuments() map[string]*models.Document {
	if pba.bundle == nil {
		return make(map[string]*models.Document)
	}

	// Load documents via bundleService if available
	if pba.bundleService != nil {
		docs, err := pba.bundleService.GetAllDocumentsForIndexing(pba.bundle.Name)
		if err != nil {
			pba.logger.Warnf("Failed to load documents for bundle '%s': %v", pba.bundle.Name, err)
			return make(map[string]*models.Document)
		}
		// Convert []*models.Document to map[string]*models.Document
		result := make(map[string]*models.Document, len(docs))
		for _, doc := range docs {
			result[doc.DocumentID] = doc
		}
		pba.logger.Debugf("Loaded %d documents for bundle '%s' via bundleService", len(result), pba.bundle.Name)
		return result
	}

	// Fallback to deprecated Documents field (legacy bundles)
	if pba.bundle.Documents == nil {
		return make(map[string]*models.Document)
	}

	// Convert from map[string]models.Document to map[string]*models.Document
	result := make(map[string]*models.Document)
	documents := *pba.bundle.Documents
	for docID, doc := range documents {
		docCopy := doc // Create copy to avoid address issues
		result[docID] = &docCopy
	}

	return result
}

// GetName returns the bundle name for logging and metrics
func (pba *PlannerBundleAdapter) GetName() string {
	if pba.bundle == nil {
		return "unknown_bundle"
	}
	return pba.bundle.Name
}

// GetTotalDocuments returns the total number of documents in the bundle
func (pba *PlannerBundleAdapter) GetTotalDocuments() int {
	if pba.bundle == nil {
		return 0
	}
	// Use metadata field instead of loading all documents
	return int(pba.bundle.TotalDocuments)
}

// PlannerServiceManager adapts bundle operations for the JOIN execution node
// This provides the service interface needed by the JOIN executor without circular imports
type PlannerServiceManager struct {
	bundles       map[string]*models.Bundle
	logger        *zap.SugaredLogger
	bundleService BundleServiceInterface
}

// GetBundleByName retrieves a bundle by name from the planner's bundle map
func (psm *PlannerServiceManager) GetBundleByName(database *models.Database, name string) (*models.Bundle, error) {
	bundle, exists := psm.bundles[name]
	if !exists {
		return nil, fmt.Errorf("bundle '%s' not found", name)
	}
	return bundle, nil
}
