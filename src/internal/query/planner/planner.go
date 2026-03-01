package planner

import (
	"context"
	"fmt"
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
	// GetDocumentPage loads a specific page by page ID
	// Used for PostgreSQL-style index-based joins to load pages directly
	GetDocumentPage(bundleName, databaseName string, pageID uint32) (*models.DocumentPage, error)
	// GetDocumentPageReadOnly returns an immutable readerView snapshot without copying.
	// Zero-allocation fast path — caller MUST NOT mutate the returned page or Documents map.
	GetDocumentPageReadOnly(bundleName, databaseName string, pageID uint32) (*models.DocumentPage, error)
	// SnapshotPageDocuments safely snapshots documents from a page to avoid concurrent map iteration
	SnapshotPageDocuments(bundleName, databaseName string, pageID uint32) ([]models.Document, error)
	// CopyProjectedFromCache copies projected documents from documentPages cache under one RLock
	// OPTIMIZATION: One-time lock acquisition, iterates cached pages, copies only projected fields
	// Returns: (projected docs map, docs copied, pages that were cached, total pages, error)
	// effectiveLimit: 0 = no limit, >0 = stop after that many docs
	CopyProjectedFromCache(bundleName, databaseName string, pageCount uint32, projectFields []string, effectiveLimit int, schema *models.BundleFieldSchema) (map[string]*documentscanner.ProjectedDocument, int, int, int, error)
	// GetDocument retrieves a single document by ID using direct lookup (O(1) via index)
	// Much more efficient than GetAllDocumentsForIndexing for single document retrieval
	// Used by JOINs to avoid N+1 query problem
	GetDocument(bundleName, databaseName, documentID string, snapshotParams ...interface{}) (*models.Document, error)
	// GetDocumentsByIDs retrieves multiple documents by ID in a single batch operation
	// Groups documents by page and loads each page once, much more efficient than N individual GetDocument calls
	// Used by JOINs for batch retrieval to avoid N+1 query problem
	GetDocumentsByIDs(bundle *models.Bundle, docIDs []string) ([]*models.Document, error)
	// GetDocumentsByIDsFromCacheDirect retrieves documents by IDs directly from cache.
	// Bypasses pageID lookups entirely - used when index pageIDs may be stale.
	// Returns documents found as a slice preserving input order, skipping missing docs.
	GetDocumentsByIDsFromCacheDirect(bundle *models.Bundle, docIDs []string) []*models.Document
}

// ExecutionNode represents a node in the execution tree
type ExecutionNode interface {
	Execute(ctx context.Context) (map[string]*models.Document, error)
	GetCost() float64
	GetEstimatedRows() int
	EstimateMemoryUsage() int64 // Memory estimation for cache sizing
}

// SliceExecutionNode is an optional interface that execution nodes can implement
// to return results as a slice instead of a map. This avoids the O(n) map insertion
// overhead for scan-heavy queries where random access by docID is not needed.
type SliceExecutionNode interface {
	ExecuteSlice(ctx context.Context) ([]*models.Document, []string, error)
}

// IteratorNode is a Volcano-style pull-based execution interface.
// Each call to Next() returns exactly one document. EOF is signaled by (nil, nil).
//
// Lifecycle: Init(ctx) → Next()* → Close()
//
// Ownership: Documents returned by Next() are independent copies from the page cache
// (via SnapshotPageDocuments). Callers may read fields freely. Nodes that mutate
// Fields (e.g. projection) must copy the document first.
//
// Concurrency: A single IteratorNode is NOT safe for concurrent use.
// Each query execution must create its own iterator chain.
type IteratorNode interface {
	// Init prepares the iterator for iteration. Must be called before Next().
	Init(ctx context.Context) error
	// Next returns the next document, or (nil, nil) on EOF.
	// After returning (nil, nil), all subsequent calls must also return (nil, nil).
	Next() (*models.Document, error)
	// Close releases resources. Must be called even if iteration is incomplete.
	// Safe to call multiple times.
	Close() error
}

// IterableNode is an optional interface for ExecutionNodes that can produce
// a native IteratorNode without materialization overhead.
type IterableNode interface {
	AsIterator() IteratorNode
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

	// CacheKey is set when the plan was returned from the plan cache (hit or stale serve).
	// 0 means the plan was built on miss. Used by RecordPlanStats for adaptive planning.
	CacheKey uint64
	// IsGeneric is true when the plan is a generic (parameter-independent) plan.
	// Set when serving from cache; used by RecordPlanStats.
	IsGeneric bool

	// ResultSchema maps Values[i] to field names for result documents.
	// Set from the bundle's FieldSchema at plan build time; aggregation nodes
	// override it with their synthetic result schema after Execute().
	ResultSchema *models.BundleFieldSchema

	// Iterator support: when UseIterator is true, the plan should be executed via
	// the pull-based iterator path instead of the materialized Execute() path.
	// IteratorFactory creates a fresh iterator chain per execution (iterators are
	// not reusable after Close()).
	UseIterator     bool
	IteratorFactory func() IteratorNode

	// TemporalClause carries the parsed FOR SYSTEM_TIME clause (interface{} to avoid import cycle).
	// When set, the execution layer applies temporal filtering via TemporalExtension.
	TemporalClause interface{}
}

// GetEffectiveResultSchema returns the schema that should be used for encoding result documents.
// If an AggregationNode has a result schema (set after Execute), that takes precedence over
// the bundle schema since aggregation produces synthetic documents with different fields.
func (ep *ExecutionPlan) GetEffectiveResultSchema() *models.BundleFieldSchema {
	if schema := findAggregationResultSchema(ep.RootNode); schema != nil {
		return schema
	}
	return ep.ResultSchema
}

// findAggregationResultSchema walks the plan tree to find an AggregationNode with a result schema.
func findAggregationResultSchema(node ExecutionNode) *models.BundleFieldSchema {
	if node == nil {
		return nil
	}
	if agg, ok := node.(*AggregationNode); ok {
		if s := agg.GetResultSchema(); s != nil {
			return s
		}
	}
	// Check common wrapper nodes
	switch n := node.(type) {
	case *SortNode:
		return findAggregationResultSchema(n.Child)
	case *LimitNode:
		return findAggregationResultSchema(n.Child)
	case *FilterNode:
		return findAggregationResultSchema(n.Child)
	case *DistinctNode:
		return findAggregationResultSchema(n.Child)
	}
	return nil
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

// BRINScanNode uses a BRIN index to skip page ranges that cannot match the predicate.
// For each range whose [min, max] overlaps the query range, loads and scans those pages.
type BRINScanNode struct {
	Bundle           *models.Bundle
	IndexName        string
	FieldName        string
	Operator         string      // ">", ">=", "<", "<=", "=", "BETWEEN"
	SearchValue      interface{} // Query value (single-sided) or min for BETWEEN
	SearchValueEnd   interface{} // End value for BETWEEN
	Cost             float64
	EstimatedRows    int
	Logger           *zap.SugaredLogger
	BundleServiceInt BundleServiceInterface
	DocumentScanner  documentscanner.DocumentScannerInterface
}

func (node *BRINScanNode) GetCost() float64           { return node.Cost }
func (node *BRINScanNode) GetEstimatedRows() int      { return node.EstimatedRows }
func (node *BRINScanNode) EstimateMemoryUsage() int64 { return 0 }

// IndexOnlyScanNode reads data from the index without fetching full documents.
// Used when the index covers all columns needed by the query (SELECT + WHERE fields).
// Delegates to the underlying index scan but signals to the planner that this is
// a cheaper path (no heap fetch needed when all data is in the index).
type IndexOnlyScanNode struct {
	Child           ExecutionNode       // The underlying index scan node
	Bundle          *models.Bundle
	IndexName       string
	IndexRef        models.IndexReference
	ProjectedFields []string            // Fields to return from the index
	Cost            float64
	EstimatedRows   int
	Logger          *zap.SugaredLogger
}

func (node *IndexOnlyScanNode) GetCost() float64           { return node.Cost }
func (node *IndexOnlyScanNode) GetEstimatedRows() int      { return node.EstimatedRows }
func (node *IndexOnlyScanNode) EstimateMemoryUsage() int64 { return 0 }

// OrderedChild is an optional interface for execution nodes that can produce documents
// in a defined order (e.g. B-tree index key order). AggregationNode uses this to skip
// the in-memory sort in SortGroupAggregate when the child is already ordered by the first GROUP BY field.
type OrderedChild interface {
	ExecuteOrdered(ctx context.Context) ([]*models.Document, error)
	OrderedByField() string
}

// BTreeOrderedScanNode performs a full B-tree index scan and returns documents in index key order.
// Used for single-field GROUP BY when that field has a B-tree index: avoids in-memory sort.
// Also used for ORDER BY optimization when a B-tree index exists on the sort field.
// Implements ExecutionNode and OrderedChild. When Bundle.Documents is not available, ExecuteOrdered
// returns an error so the consumer can fall back to Execute (map) and a regular sort.
type BTreeOrderedScanNode struct {
	Bundle             *models.Bundle
	IndexName          string
	OrderedByFieldName string
	Logger             *zap.SugaredLogger
	BundleServiceInt   BundleServiceInterface
	Cost               float64
	EstimatedRows      int
	// ORDER BY optimization fields
	Descending      bool // If true, iterate in descending order (for ORDER BY ... DESC)
	Limit           int  // If > 0, stop after this many documents (for LIMIT optimization)
	sortedDocuments []*models.Document // Cached sorted result from Execute()
}

// FullScanNode represents a full bundle scan
type FullScanNode struct {
	Bundle           *models.Bundle
	Cost             float64
	EstimatedRows    int
	Logger           *zap.SugaredLogger
	BundleServiceInt BundleServiceInterface
	DocumentScanner  documentscanner.DocumentScannerInterface
	MaxDocuments     int      // OPTIMIZATION: Early termination limit (0 = no limit, set when LIMIT-only query)
	ProjectionFields []string // PROJECTION PUSHDOWN: Field names to deserialize (e.g., ["name"] for ORDER BY name queries)
	// PREDICATE PUSHDOWN: When set, uses ScanWithPredicate to filter during page iteration
	// instead of loading all documents and filtering afterward. Reduces copies for high-selectivity queries.
	Predicate func(*models.Document) bool
	// PAGE BLOOM FILTER: Equality predicates extracted from WHERE for page-skip optimization.
	// Set by the planner when creating FullScanNode for filtered queries.
	BloomHints []documentscanner.BloomHint
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
	// PERFORMANCE OPTIMIZATION: Early termination limit for LIMIT pushdown (0 = no limit)
	// When set and query has no ORDER BY, stop filtering after MaxDocuments matches
	MaxDocuments int
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

// ExecuteIterator creates and initializes an iterator chain for pull-based execution.
// The caller must call Close() on the returned iterator when done.
func (ep *ExecutionPlan) ExecuteIterator(ctx context.Context) (IteratorNode, error) {
	if ep.IteratorFactory == nil {
		return nil, fmt.Errorf("plan does not support iterator execution")
	}
	iter := ep.IteratorFactory()
	if err := iter.Init(ctx); err != nil {
		iter.Close()
		return nil, err
	}
	return iter, nil
}
