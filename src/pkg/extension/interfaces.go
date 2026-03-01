package extension

import (
	"context"
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
