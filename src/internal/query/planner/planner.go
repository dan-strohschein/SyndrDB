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
	// GetAllDocumentsForIndexingWithOptions supports streaming filter and parallel page loading for predicate pushdown.
	// opts may be nil (delegates to GetAllDocumentsForIndexing). Uses bundle.IndexingOptions.
	GetAllDocumentsForIndexingWithOptions(bundleName string, opts *bundle.IndexingOptions) ([]*models.Document, error)
	// GetDocumentChunksForIndexing streams documents in chunks (e.g. page-by-page) to avoid loading the full bundle.
	// fn is called with each chunk; return false to stop. Used by ScanDocumentChunks for streaming probe.
	GetDocumentChunksForIndexing(ctx context.Context, bundleName string, chunkSize int, fn func(chunk []*models.Document) (stop bool)) error
	// SetProjectionFieldsForBundle sets projection for a bundle (opt #1). Pass nil or empty to clear.
	SetProjectionFieldsForBundle(bundleName string, fields []string)
	// CountDocuments counts all documents using optimized count-only parser (does NOT cache pages)
	// Used for COUNT(*) optimization to avoid massive memory spikes from caching full pages
	CountDocuments(bundleName, databaseName string) (int, error)
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

// OrderedChild is an optional interface for execution nodes that can produce documents
// in a defined order (e.g. B-tree index key order). AggregationNode uses this to skip
// the in-memory sort in SortGroupAggregate when the child is already ordered by the first GROUP BY field.
type OrderedChild interface {
	ExecuteOrdered(ctx context.Context) ([]*models.Document, error)
	OrderedByField() string
}

// BTreeOrderedScanNode performs a full B-tree index scan and returns documents in index key order.
// Used for single-field GROUP BY when that field has a B-tree index: avoids in-memory sort.
// Implements ExecutionNode and OrderedChild. When Bundle.Documents is not available, ExecuteOrdered
// returns an error so the consumer can fall back to Execute (map) and a regular sort.
type BTreeOrderedScanNode struct {
	Bundle              *models.Bundle
	IndexName           string
	OrderedByFieldName  string
	Logger              *zap.SugaredLogger
	BundleServiceInt    BundleServiceInterface
	Cost                float64
	EstimatedRows       int
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
