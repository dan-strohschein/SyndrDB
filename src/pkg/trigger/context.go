package trigger

import (
	"context"
	"syndrdb/src/internal/domain/models"
)

// ExecutionContext provides OLD/NEW document references and metadata
// to trigger bodies during execution. Placed in pkg/trigger to avoid
// import cycles between syndrQL and server packages.
type ExecutionContext struct {
	OldDocument      *models.Document    // nil for INSERT
	NewDocument      *models.Document    // nil for DELETE
	SourceBundleName string              // Bundle that fired the trigger
	EventType        models.TriggerEvent // INSERT, UPDATE, or DELETE
	TriggerDepth     int                 // Current cascade depth
}

// triggerContextKey is the context key for ExecutionContext
type triggerContextKey struct{}

// WithExecutionContext attaches a trigger ExecutionContext to a context.Context
func WithExecutionContext(ctx context.Context, tc *ExecutionContext) context.Context {
	return context.WithValue(ctx, triggerContextKey{}, tc)
}

// GetExecutionContext retrieves the trigger ExecutionContext from a context.Context.
// Returns nil if not in a trigger execution context.
func GetExecutionContext(ctx context.Context) *ExecutionContext {
	if tc, ok := ctx.Value(triggerContextKey{}).(*ExecutionContext); ok {
		return tc
	}
	return nil
}
