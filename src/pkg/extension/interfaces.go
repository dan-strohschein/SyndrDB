package extension

import (
	"context"
	"net"
	"time"
)

// ExtensionContext provides enterprise extensions with safe access to core services.
// Core creates the concrete implementation wrapping ServiceManager.
// Uses interface{} to avoid importing internal/ types.
type ExtensionContext interface {
	// ExecuteQuery runs a SyndrQL query and returns the result.
	ExecuteQuery(ctx context.Context, sql string) (interface{}, error)
	// Logger returns the system logger (concrete type: *zap.SugaredLogger).
	Logger() interface{}
	// Settings returns the settings singleton (concrete type: *settings.Arguments).
	Settings() interface{}
	// SessionInfo returns the current session's user context, or nil if unavailable.
	SessionInfo() *SessionInfo
	// BundleService returns the core bundle service (concrete type: *bundle.BundleService).
	BundleService() interface{}
	// DatabaseService returns the core database service (concrete type: *database.DatabaseService).
	DatabaseService() interface{}
}

// CommandExtension allows enterprise features to register new SyndrQL commands.
type CommandExtension interface {
	// CommandPrefixes returns lowercase command prefixes this extension handles.
	// e.g., []string{"repl", "fulltext", "encrypt"}
	CommandPrefixes() []string
	// HandleCommand processes a matched command. Return ErrNotHandled to
	// fall through to core routing.
	HandleCommand(ctx context.Context, command string, extCtx ExtensionContext) (interface{}, error)
}

// LifecycleHook allows enterprise features to initialize and clean up.
type LifecycleHook interface {
	// OnServerStart is called after InitServiceManager completes,
	// before the server accepts connections.
	OnServerStart(ctx context.Context, extCtx ExtensionContext) error
	// OnServerStop is called during Server.Stop(), before session cleanup.
	OnServerStop(ctx context.Context) error
}

// SessionInfo carries user context for extensions (masking, audit).
type SessionInfo struct {
	Username     string
	SessionID    string
	DatabaseName string
	IsAdmin      bool
}

// ResultTransformExtension modifies query results before returning to client (masking).
type ResultTransformExtension interface {
	TransformResult(ctx context.Context, bundleName string, row map[string]interface{}, session *SessionInfo) map[string]interface{}
	ShouldTransform(bundleName string) bool
}

// AuditEventExtension receives DML/DDL event notifications.
type AuditEventExtension interface {
	OnCommandExecuted(ctx context.Context, eventType string, detail map[string]interface{})
}

// StorageEncryptionExtension provides block-level encryption for storage, WAL, and backups.
// The scope string drives DEK selection: "bundle:<name>", "wal", "backup:<id>".
type StorageEncryptionExtension interface {
	EncryptBlock(plaintext []byte, scope string) ([]byte, error)
	DecryptBlock(ciphertext []byte, scope string) ([]byte, error)
	// EncryptionEnabled returns true if encryption is active for the given scope.
	EncryptionEnabled(scope string) bool
}

// IndexExtension manages custom index types (e.g., FTS inverted index).
type IndexExtension interface {
	// IndexType returns the name, e.g. "fulltext".
	IndexType() string
	// BuildIndex creates the index from existing bundle data.
	BuildIndex(ctx context.Context, bundleName string, fieldNames []string, options map[string]interface{}, extCtx ExtensionContext) error
	// DropIndex removes the index.
	DropIndex(ctx context.Context, bundleName string, indexName string, extCtx ExtensionContext) error
	// OnDocumentChange notifies the index of a DML event for async/sync update.
	OnDocumentChange(ctx context.Context, bundleName string, docID string, changeType string, doc map[string]interface{}) error
}

// PlannerExtension allows enterprise features to inject custom execution nodes.
type PlannerExtension interface {
	// PlanQuery is called during query planning. Return nil, false to decline.
	PlanQuery(ctx context.Context, bundleName string, query interface{}) (interface{}, bool)
}

// ReplicationExtension hooks into the WAL pipeline to stream entries to followers.
type ReplicationExtension interface {
	// OnWALEntry is called after a WAL entry is durably written.
	OnWALEntry(entry WALEntryInfo) error
	// IsLeader returns true if this node is the current leader.
	IsLeader() bool
	// IsFollower returns true if this node is a follower.
	IsFollower() bool
	// ReplicationMode returns "async" or "semisync".
	ReplicationMode() string
}

// WALEntryInfo is a lightweight struct passed to replication hooks.
type WALEntryInfo struct {
	LSN        uint64
	Operation  int    // maps to journal.OperationType
	TxID       string
	BundleName string
	DocumentID string
	RawBytes   []byte // serialized binary WAL entry (pre-encryption)
}

// ReadRouterExtension intercepts read queries and can route them to followers.
type ReadRouterExtension interface {
	// RouteRead is called before SELECT execution. Returns:
	//   result, true  — extension handled the query (return result to client)
	//   nil, false    — execute locally as normal
	RouteRead(ctx context.Context, command string, extCtx ExtensionContext) (interface{}, bool)
}

// --- Milestone 5: Performance + Operations ---

// ExecutionExtension intercepts query execution for parallelization.
type ExecutionExtension interface {
	// ParallelExecute is called with the execution plan. Returns (result, true)
	// if handled in parallel, or (nil, false) to use default execution.
	ParallelExecute(ctx context.Context, plan interface{}, extCtx ExtensionContext) (interface{}, bool)
	// ShouldParallelize returns true if the plan is eligible for parallel execution.
	ShouldParallelize(plan interface{}) bool
}

// QueryGovernorExtension enforces resource limits on queries.
type QueryGovernorExtension interface {
	// OnQueryStart is called before execution. Returns queryID or error to reject.
	OnQueryStart(ctx context.Context, command string, session *SessionInfo) (string, error)
	// OnQueryEnd is called after execution completes (success or error).
	OnQueryEnd(queryID string, elapsed time.Duration, rowsScanned int64, err error)
	// GetActiveQueries returns info about running queries.
	GetActiveQueries() []ActiveQueryInfo
	// KillQuery terminates a running query by ID.
	KillQuery(queryID string) error
}

// ActiveQueryInfo describes a running query for monitoring.
type ActiveQueryInfo struct {
	QueryID   string
	Command   string
	Username  string
	StartTime time.Time
	Elapsed   time.Duration
}

// MetricsExporterExtension provides metrics in various formats.
type MetricsExporterExtension interface {
	// RecordMetric records a metric data point.
	RecordMetric(name string, value float64, labels map[string]string)
	// IncrementCounter increments a counter metric.
	IncrementCounter(name string, labels map[string]string)
	// RecordHistogram records a value in a histogram.
	RecordHistogram(name string, value float64, labels map[string]string)
	// ServeMetrics returns formatted metrics (Prometheus text format).
	ServeMetrics() ([]byte, error)
}

// DocumentSecurityExtension applies row-level security filters.
type DocumentSecurityExtension interface {
	// FilterDocuments applies DLS policies, returning only docs the user can see.
	FilterDocuments(ctx context.Context, bundleName string, docs []map[string]interface{}, session *SessionInfo) []map[string]interface{}
	// ShouldFilter returns true if this bundle has DLS policies.
	ShouldFilter(bundleName string) bool
}

// CDCExtension captures data changes for external consumption.
type CDCExtension interface {
	// OnDataChange is called after a mutation is committed.
	OnDataChange(ctx context.Context, change DataChangeEvent)
}

// DataChangeEvent describes a data mutation for CDC.
type DataChangeEvent struct {
	Operation  string                 // "INSERT", "UPDATE", "DELETE"
	BundleName string
	DocumentID string
	Before     map[string]interface{} // nil for INSERT
	After      map[string]interface{} // nil for DELETE
	Timestamp  time.Time
	TxID       string
	LSN        uint64
}

// ConnectionPoolExtension manages server-side connection pooling.
type ConnectionPoolExtension interface {
	// OnConnectionRequest is called when a new connection arrives.
	OnConnectionRequest(conn net.Conn) (net.Conn, bool)
	// OnConnectionRelease returns a connection to the pool.
	OnConnectionRelease(connID string)
	// GetPoolStats returns pool utilization info.
	GetPoolStats() interface{}
}

// AdaptiveOptimizerExtension provides runtime feedback to the query planner.
type AdaptiveOptimizerExtension interface {
	// RecordExecution records actual execution metrics for a plan.
	RecordExecution(planHash uint64, stats ExecutionStats)
	// SuggestPlanChange returns true if the plan should be re-optimized.
	SuggestPlanChange(planHash uint64) (bool, string)
	// GetExecutionHistory returns historical stats for a plan.
	GetExecutionHistory(planHash uint64) []ExecutionStats
}

// ExecutionStats captures actual runtime behavior of a query.
type ExecutionStats struct {
	RowsScanned  int64
	RowsReturned int64
	ElapsedMs    float64
	MemoryBytes  int64
	PlanCost     float64
	ActualCost   float64
	Timestamp    time.Time
}

// MatViewRefreshExtension supports incremental materialized view refresh.
type MatViewRefreshExtension interface {
	// OnSourceChange is called when a document in a source bundle changes.
	OnSourceChange(ctx context.Context, bundleName string, docID string, changeType string, doc map[string]interface{}) error
	// IsSourceBundle returns true if this bundle is a source for any materialized view.
	IsSourceBundle(bundleName string) bool
	// RefreshView triggers an incremental refresh of a materialized view.
	RefreshView(ctx context.Context, viewName string) error
}

// --- Milestone 6.2: Distributed Transactions (2PC) ---

// DistributedTransactionExtension tracks cross-shard transaction state.
type DistributedTransactionExtension interface {
	TrackBegin(sessionID string, participants []string) (dtxID string)
	TrackPrepared(dtxID string)
	TrackCommit(dtxID string)
	TrackAbort(dtxID string)
	GetState(dtxID string) (DistributedTxState, error)
	ListActive() []DistributedTxInfo
}

// DistributedTxState represents the state of a distributed transaction.
type DistributedTxState int

const (
	DTxStateActive    DistributedTxState = iota
	DTxStatePrepared
	DTxStateCommitted
	DTxStateAborted
)

// DistributedTxInfo describes an active or recently resolved distributed transaction.
type DistributedTxInfo struct {
	DTXID        string
	State        DistributedTxState
	Participants []string
	SessionID    string
	StartedAt    time.Time
}

// --- Milestone 6.7: Enterprise Query Result Caching ---

// CachedResultInfo describes a cached query result set.
type CachedResultInfo struct {
	CacheKey    string
	BundleName  string
	Username    string
	IsAdmin     bool
	ResultCount int
	MemoryBytes int64
	CachedAt    time.Time
	TTL         time.Duration
	HitCount    uint64
	IsAggregate bool
}

// ResultCacheStats is a point-in-time snapshot of result cache statistics.
type ResultCacheStats struct {
	Hits            uint64
	Misses          uint64
	Stores          uint64
	Evictions       uint64
	Invalidations   uint64
	MemoryUsed      int64
	MemoryLimit     int64
	EntryCount      int
	HitRate         float64
}

// QueryResultCacheExtension caches materialized query result sets (single-provider model).
type QueryResultCacheExtension interface {
	// LookupResult checks the cache for a result set.
	// Returns (result, resultCount, true) on hit, or (nil, 0, false) on miss.
	LookupResult(ctx context.Context, queryHash uint64, bundleName string, session *SessionInfo) ([]map[string]interface{}, int, bool)
	// StoreResult caches a result set after execution and DLS/masking.
	StoreResult(ctx context.Context, queryHash uint64, bundleName string, session *SessionInfo, result []map[string]interface{}, isAggregate bool)
	// InvalidateBundle evicts all cached results for a bundle.
	InvalidateBundle(bundleName string)
	// ShouldCache returns true if caching is enabled for this bundle.
	ShouldCache(bundleName string) bool
	// GetStats returns a point-in-time snapshot of cache statistics.
	GetStats() ResultCacheStats
	// FlushAll evicts all cached results.
	FlushAll()
	// FlushBundle evicts cached results for a specific bundle.
	FlushBundle(bundleName string)
}

// --- Milestone 6.1: Range-Based Sharding ---

// ShardQueryExecutorFn is the callback type that core passes to enterprise
// for executing a per-shard query through the full SelectDocuments pipeline.
type ShardQueryExecutorFn func(shardBundleName string) ([]map[string]interface{}, int, error)

// ShardingExtension provides range-based sharding for bundles.
type ShardingExtension interface {
	// IsShardedBundle returns true if the bundle has an active shard policy.
	IsShardedBundle(bundleName string) bool
	// ResolveWriteShard determines which shard sub-bundle a document should be written to.
	ResolveWriteShard(bundleName string, doc map[string]interface{}) (shardBundleName string, err error)
	// ResolveReadShards returns the shard sub-bundle names relevant to the given predicates.
	// Returns all shards if predicates don't reference the shard key.
	ResolveReadShards(bundleName string, predicates interface{}) []string
	// GetShardBundles returns all shard sub-bundle names for a sharded bundle.
	GetShardBundles(bundleName string) []string
	// GetShardPolicy returns the shard policy info for a bundle, or nil if not sharded.
	GetShardPolicy(bundleName string) *ShardPolicyInfo
	// ExecuteShardedQuery handles SELECT scatter-gather for sharded bundles.
	// Called by core's SelectDocuments() when the target bundle is sharded.
	// The executor callback calls SelectDocuments recursively per shard.
	ExecuteShardedQuery(ctx context.Context, bundleName string, query interface{},
		rawCommand string, session *SessionInfo,
		executor ShardQueryExecutorFn) ([]map[string]interface{}, int, bool)
}

// ShardPolicyInfo describes a shard policy on a bundle.
type ShardPolicyInfo struct {
	BundleName string
	ShardKey   string
	KeyType    string // "int", "float", "string"
	Ranges     []ShardRange
	ShardCount int
	CreatedAt  time.Time
}

// ShardRange describes one shard's key range.
type ShardRange struct {
	ShardID    int
	ShardName  string
	LowerBound interface{} // inclusive; nil = MIN
	UpperBound interface{} // exclusive; nil = MAX
	DocCount   int64
}

// --- Milestone 6.6: In-Memory Columnar Processing ---

// ColumnarSegmentStats — per-segment column statistics for pruning.
type ColumnarSegmentStats struct {
	SegmentID   int
	RowCount    int
	MinValues   map[string]interface{}
	MaxValues   map[string]interface{}
	NullCounts  map[string]int
	DistinctEst map[string]int
}

// ColumnarBundleInfo — columnar processing state for a bundle.
type ColumnarBundleInfo struct {
	BundleName    string
	SegmentCount  int
	TotalRows     int
	MemoryBytes   int64
	ColumnCount   int
	Compression   string
	LastRefreshed time.Time
	IsStale       bool
	StaleSegments int
}

// ColumnarProcessingExtension — single-provider model.
type ColumnarProcessingExtension interface {
	IsColumnarBundle(bundleName string) bool
	ExecuteColumnar(ctx context.Context, bundleName string, query interface{},
		session *SessionInfo) ([]map[string]interface{}, int, bool)
	InvalidateSegments(bundleName string)
	GetBundleInfo(bundleName string) *ColumnarBundleInfo
	GetSegmentStats(bundleName string) []ColumnarSegmentStats
	GetMemoryUsage() int64
}

// --- Milestone 7.3: Field-Level Encryption ---

// FieldEncryptionExtension provides application-level encryption for individual document fields.
// Single-provider model (like StorageEncryptionExtension).
type FieldEncryptionExtension interface {
	// EncryptFieldValues modifies the map in-place, replacing plaintext values with FLE markers
	// for fields that have an active FLE policy on the given bundle.
	EncryptFieldValues(ctx context.Context, bundleName string, fields map[string]interface{}) error
	// HasFLEPolicy returns true if the bundle has any active FLE policies (fast-path skip).
	HasFLEPolicy(bundleName string) bool
}

// --- Milestone 6.4: Online Schema Changes ---

// OnlineSchemaChangeExtension provides non-blocking schema migration status.
type OnlineSchemaChangeExtension interface {
	IsMigrating(bundleName string) bool
	GetMigrationState(bundleName string) *SchemaChangeInfo
	ListMigrations() []SchemaChangeInfo
}

// SchemaChangeInfo describes the state of an online schema migration.
type SchemaChangeInfo struct {
	MigrationID    string
	BundleName     string
	State          string // "PENDING", "CREATING_SHADOW", "COPYING", "CATCHING_UP", "SWAPPING", "COMPLETED", "FAILED", "CANCELLED"
	ShadowBundle   string
	DocsCopied     int64
	DocsTotal      int64
	EventsReplayed int64
	EventsPending  int64
	StartedAt      time.Time
	CompletedAt    time.Time
	ErrorMessage   string
}

// --- Milestone 7.1: Multi-Primary CRDT Replication ---

// CRDTReplicationExtension enables multi-primary replication using CRDTs.
// Single-provider model (like StorageEncryptionExtension).
type CRDTReplicationExtension interface {
	// IsCRDTBundle returns true if the bundle has an active CRDT policy.
	IsCRDTBundle(bundleName string) bool
	// OnDocumentWrite injects CRDT metadata into document fields before storage.
	OnDocumentWrite(ctx context.Context, bundleName, docID string,
		fields map[string]interface{}, operation string) (map[string]interface{}, error)
	// MergeRemoteState merges a remote document's CRDT state with local state.
	MergeRemoteState(ctx context.Context, bundleName, docID string,
		remoteFields map[string]interface{}) (map[string]interface{}, error)
	// IsPrimary returns true if this node accepts writes (always true in CRDT mode).
	IsPrimary() bool
	// GetPeerInfo returns information about all known peers.
	GetPeerInfo() []CRDTPeerInfo
}

// CRDTPeerInfo describes a peer node in the multi-primary cluster.
type CRDTPeerInfo struct {
	NodeID, Host, Status string
	Port, GossipPort     int
	LastSyncTime         time.Time
	PendingDeltas        int64
}

// SpatialFunctionExtension evaluates ST_* spatial functions at query time.
// The core stubs delegate to this extension when it is registered.
// geomA/geomB are interface{} — either a map[string]interface{} (GeoJSON) or string (WKT).
type SpatialFunctionExtension interface {
	// EvalDistance returns distance between two geometries. geomA from doc field, geomB from arg.
	EvalDistance(bundleName string, geomA interface{}, geomB interface{}) (float64, error)
	// EvalWithin returns true if geomA is within geomB.
	EvalWithin(bundleName string, geomA interface{}, geomB interface{}) (bool, error)
	// EvalContains returns true if geomA contains geomB.
	EvalContains(bundleName string, geomA interface{}, geomB interface{}) (bool, error)
	// EvalIntersects returns true if geomA intersects geomB.
	EvalIntersects(bundleName string, geomA interface{}, geomB interface{}) (bool, error)
	// EvalDWithin returns true if geomA is within distance of geomB.
	EvalDWithin(bundleName string, geomA interface{}, geomB interface{}, distance float64) (bool, error)
}

// TemporalExtension manages system-versioned bundle lifecycle.
type TemporalExtension interface {
	// OnDocumentWrite is called before a document write to capture history.
	OnDocumentWrite(ctx context.Context, bundleName string, docID string, oldDoc map[string]interface{}, newDoc map[string]interface{}) error
	// OnDocumentDelete is called before a document delete to capture final history.
	OnDocumentDelete(ctx context.Context, bundleName string, docID string, oldDoc map[string]interface{}) error
	// IsTemporalBundle returns true if the bundle has system versioning enabled.
	IsTemporalBundle(bundleName string) bool
	// FilterTemporalDocs filters documents based on temporal clause (AS OF, BETWEEN, ALL).
	FilterTemporalDocs(ctx context.Context, bundleName string, docs []map[string]interface{}, clause interface{}) ([]map[string]interface{}, error)
}
