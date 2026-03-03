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

// --- Milestone 6.1: Range-Based Sharding ---

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
