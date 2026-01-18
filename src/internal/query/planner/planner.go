package planner

import (
	"context"
	"syndrdb/src/internal/domain/bundle"
	"syndrdb/src/internal/domain/models"
	"syndrdb/src/internal/query/documentscanner"
	"syndrdb/src/internal/query/queryparser"

	"go.uber.org/zap"
)

// BundleServiceInterface defines the interface for bundle service operations needed by the query planner
type BundleServiceInterface interface {
	GetOrLoadHashIndexInterface(bundle *models.Bundle, indexName string, indexRef models.IndexReference) (interface{}, error)
	GetOrLoadBTreeIndex(bundle *models.Bundle, indexName string, indexRef models.IndexReference) (interface{}, error)
	GetOrCreateDocumentScanner(bundle *models.Bundle) (documentscanner.DocumentScannerInterface, error)
	GetBundleByName(database *models.Database, name string) (*models.Bundle, error)
	GetAllDocumentsForIndexing(bundleName string) ([]*models.Document, error)
}

// ExecutionNode represents a node in the execution tree
type ExecutionNode interface {
	Execute(ctx context.Context) (map[string]*models.Document, error)
	GetCost() float64
	GetEstimatedRows() int
	EstimateMemoryUsage() int64 // Memory estimation for cache sizing
}

// ExecutionPlan represents the complete execution plan
type ExecutionPlan struct {
	RootNode      ExecutionNode
	Cost          float64
	EstimatedRows int
	IndexesUsed   []string
	Logger        *zap.SugaredLogger

	// Cached memory estimation (computed once at plan creation)
	// NOTE: ExecutionPlan is immutable after creation - if plan mutation support added, must recompute
	estimatedMemoryBytes int64
}

// ScanType represents different types of scans
type ScanType int

const (
	FullBundleScan ScanType = iota
	HashIndexScan
	BTreeIndexScan
	BTreeRangeScan
)

// IndexScanNode represents an index-based scan
type IndexScanNode struct {
	Bundle           *models.Bundle
	IndexName        string
	ScanType         ScanType
	SearchKey        interface{}
	RangeStart       interface{}
	RangeEnd         interface{}
	Operator         string // "=", ">", "<", ">=", "<=", "BETWEEN"
	Cost             float64
	EstimatedRows    int
	Logger           *zap.SugaredLogger
	BundleServiceInt BundleServiceInterface
	BundleService    bundle.BundleService
	// DOCUMENT SCANNER INTEGRATION: Add document scanner for paginated operations
	DocumentScanner documentscanner.DocumentScannerInterface
}

// FullScanNode represents a full bundle scan
type FullScanNode struct {
	Bundle           *models.Bundle
	Cost             float64
	EstimatedRows    int
	Logger           *zap.SugaredLogger
	BundleServiceInt BundleServiceInterface
	DocumentScanner  documentscanner.DocumentScannerInterface
	MaxDocuments     int // OPTIMIZATION: Early termination limit (0 = no limit, set when LIMIT-only query)
	ProjectionFields []string // PROJECTION PUSHDOWN: Field names to deserialize (e.g., ["name"] for ORDER BY name queries)
} // FilterNode represents post-scan filtering
type FilterNode struct {
	Child      ExecutionNode
	Conditions []FilterCondition
	Clauses    []queryparser.WhereClause // DEPRECATED: Will be removed
	// NEW: Expression-based filtering
	WhereExpression interface{} // syndrQL.Expression - use type assertion
	BundleContext   interface{} // syndrQL.BundleContext - use type assertion
	Cost            float64
	EstimatedRows   int
	Logger          *zap.SugaredLogger
	// DOCUMENT SCANNER INTEGRATION: Add document scanner for paginated operations
	DocumentScanner documentscanner.DocumentScannerInterface
	// PRIORITY 4: Query cache for expression caching and predicate reordering
	QueryCache *QueryCache
	// TIER 1 SUBQUERY SUPPORT: Subquery executor for detecting and executing subqueries in WHERE expressions
	SubqueryExecutor interface{} // *subquery.SubqueryExecutor - use type assertion to avoid circular dependencies
	// TIER 1 SUBQUERY SUPPORT: Database reference needed for executing inner queries
	Database *models.Database // Database containing bundles for subquery execution
}

// FilterCondition represents a single filter condition
type FilterCondition struct {
	Field    string
	Operator string
	Value    interface{}
}

// UnionNode represents a UNION operation for OR conditions
type UnionNode struct {
	Children      []ExecutionNode
	Cost          float64
	EstimatedRows int
	Logger        *zap.SugaredLogger
	// DOCUMENT SCANNER INTEGRATION: Add document scanner for paginated operations
	DocumentScanner documentscanner.DocumentScannerInterface
}

// Execute executes the execution plan by running the root node
// This method implements the ExecutionPlanInterface for subquery execution
func (ep *ExecutionPlan) Execute(ctx context.Context) (interface{}, error) {
	return ep.RootNode.Execute(ctx)
}
