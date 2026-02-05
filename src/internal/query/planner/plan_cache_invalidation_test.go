package planner

import (
	"testing"

	"syndrdb/src/pkg/settings"

	"go.uber.org/zap"
)

// TestInvalidationManager_BurstWithinWindowTriggersInvalidation ensures that
// a burst of writes within one batch window (e.g. 1500 in 50ms) triggers
// InvalidateBundle when total (count + batchWindowWrites) exceeds threshold (Issue 3).
func TestInvalidationManager_BurstWithinWindowTriggersInvalidation(t *testing.T) {
	args := &settings.Arguments{}
	args.PlanCacheEnabled = true
	args.PlanCacheCapacity = 1000
	spc := NewShardedPlanCache(args, zap.NewNop().Sugar())
	if spc == nil {
		t.Fatal("NewShardedPlanCache returned nil")
	}

	im := NewInvalidationManager(spc, 1000, zap.NewNop().Sugar())
	bundleName := "test_burst_bundle"

	// Simulate 1500 writes within the same 100ms window (no delay between calls)
	for i := 0; i < 1500; i++ {
		im.OnWrite(bundleName, 1)
	}

	// Invalidation should have been triggered (total = 0 + 1500 >= 1000)
	if count := spc.GetBundleInvalidationCount(bundleName); count < 1 {
		t.Errorf("expected at least 1 invalidation after burst of 1500 writes within one window, got %d", count)
	}
}

// TestInvalidationManager_RemoveBundlePrunesTrackers ensures that when a bundle is
// dropped, its write tracker is removed so writeTrackers does not grow unbounded (Issue 10).
func TestInvalidationManager_RemoveBundlePrunesTrackers(t *testing.T) {
	spc := NewShardedPlanCache(&settings.Arguments{PlanCacheEnabled: true, PlanCacheCapacity: 1000}, zap.NewNop().Sugar())
	if spc == nil {
		t.Fatal("NewShardedPlanCache returned nil")
	}
	im := NewInvalidationManager(spc, 1000, zap.NewNop().Sugar())

	// Create trackers for several bundles
	for i := 0; i < 10; i++ {
		im.OnWrite("bundle_"+string(rune('a'+i)), 1)
	}
	if n := im.WriteTrackerCount(); n != 10 {
		t.Errorf("after 10 OnWrites for 10 bundles, WriteTrackerCount = %d, want 10", n)
	}

	// Remove half of them
	for i := 0; i < 5; i++ {
		im.RemoveBundle("bundle_" + string(rune('a'+i)))
	}
	if n := im.WriteTrackerCount(); n != 5 {
		t.Errorf("after RemoveBundle for 5 bundles, WriteTrackerCount = %d, want 5", n)
	}

	// Remove the rest
	for i := 5; i < 10; i++ {
		im.RemoveBundle("bundle_" + string(rune('a'+i)))
	}
	if n := im.WriteTrackerCount(); n != 0 {
		t.Errorf("after RemoveBundle for all, WriteTrackerCount = %d, want 0", n)
	}
}
