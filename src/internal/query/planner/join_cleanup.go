/*
JOIN CLEANUP CONTEXT FOR EXECUTION NODES

This file provides a context-based mechanism for deferring cleanup of pooled
JOIN result slices until after the query response has been fully consumed.
The server attaches a *[]func() to the context before executing the plan;
JoinExecutionNode appends a cleanup function when it uses pooled JoinedDocuments.
The server runs all registered cleanup functions after Execute returns, avoiding
deep copies while keeping pooled memory lifecycle correct.

CRITICAL: The server package must set this context key (JoinCleanupContextKey)
to a *[]func() before calling plan.RootNode.Execute(ctx), and run each function
after the result has been consumed (e.g., after building the CommandResponse).
*/

package planner

import "context"

// JoinCleanupContextKey is the context key for the join cleanup slice.
// The value must be *[]func(). The server sets it; execution nodes append cleanup funcs.
const JoinCleanupContextKey = "join_cleanup"

// GetJoinCleanup returns the join cleanup slice pointer from the context, or nil.
// When non-nil, JoinExecutionNode registers a cleanup instead of copying JoinedDocuments.
func GetJoinCleanup(ctx context.Context) *[]func() {
	if v := ctx.Value(JoinCleanupContextKey); v != nil {
		if ptr, ok := v.(*[]func()); ok {
			return ptr
		}
	}
	return nil
}
