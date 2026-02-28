package extension

import "context"

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
