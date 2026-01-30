package planner

/*
SNAPSHOT CONTEXT - MVCC SNAPSHOT ISOLATION SUPPORT

This file re-exports snapshot context functions from models package for
backward compatibility. The actual implementation is in models to avoid
import cycles (since both bundle and planner need access to SnapshotInfo).

DEPRECATED: New code should use models.SnapshotInfo, models.WithSnapshotInfo,
and models.GetSnapshotInfoFromContext directly.
*/

import (
	"context"

	"syndrdb/src/internal/domain/models"
)

// SnapshotInfo is an alias to models.SnapshotInfo for backward compatibility
// DEPRECATED: Use models.SnapshotInfo directly
type SnapshotInfo = models.SnapshotInfo

// WithSnapshotInfo re-exports models.WithSnapshotInfo for backward compatibility
// DEPRECATED: Use models.WithSnapshotInfo directly
func WithSnapshotInfo(ctx context.Context, snapshot *SnapshotInfo) context.Context {
	return models.WithSnapshotInfo(ctx, snapshot)
}

// GetSnapshotInfoFromContext re-exports models.GetSnapshotInfoFromContext for backward compatibility
// DEPRECATED: Use models.GetSnapshotInfoFromContext directly
func GetSnapshotInfoFromContext(ctx context.Context) *SnapshotInfo {
	return models.GetSnapshotInfoFromContext(ctx)
}
