package planner

/*
SNAPSHOT CONTEXT - MVCC SNAPSHOT ISOLATION SUPPORT

This file provides context-based snapshot information for MVCC query execution.
Snapshots are passed through the query execution context to enable visibility filtering.
*/

import (
	"context"
)

// SnapshotInfo contains MVCC snapshot information for query execution
type SnapshotInfo struct {
	SnapshotSequence uint64        // Commit sequence boundary
	TransactionID    uint64        // Transaction ID for read-your-own-writes
	ActiveTxIDs      map[uint64]bool // Active transactions at snapshot time
}

type snapshotContextKey struct{}

// WithSnapshotInfo adds snapshot information to the context
func WithSnapshotInfo(ctx context.Context, snapshot *SnapshotInfo) context.Context {
	return context.WithValue(ctx, snapshotContextKey{}, snapshot)
}

// GetSnapshotInfoFromContext retrieves snapshot information from context
// Returns nil if no snapshot is set (non-transactional query)
func GetSnapshotInfoFromContext(ctx context.Context) *SnapshotInfo {
	if snapshot, ok := ctx.Value(snapshotContextKey{}).(*SnapshotInfo); ok {
		return snapshot
	}
	return nil
}
