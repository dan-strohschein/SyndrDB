package statistics

import (
	"sync"
	"testing"
	"time"

	"go.uber.org/zap"
)

func trackerLogger() *zap.SugaredLogger {
	return zap.NewNop().Sugar()
}

func newTestTracker(t *testing.T, enabled bool) (*AutoAnalyzeTracker, *StatisticsStorage) {
	t.Helper()
	ss, err := NewStatisticsStorage(StorageConfig{
		BaseDir: t.TempDir(),
	}, trackerLogger())
	if err != nil {
		t.Fatalf("failed to create storage: %v", err)
	}
	t.Cleanup(func() { ss.Close() })

	tracker := NewAutoAnalyzeTracker(AutoAnalyzeConfig{
		Enabled: enabled,
	}, ss, trackerLogger())

	if enabled {
		t.Cleanup(func() { tracker.Stop() })
	}

	return tracker, ss
}

// --- Constructor ---

func TestNewAutoAnalyzeTracker_Defaults(t *testing.T) {
	tracker, _ := newTestTracker(t, false)
	if tracker.absoluteThreshold != DEFAULT_AUTO_ANALYZE_THRESHOLD {
		t.Errorf("expected default threshold %d, got %d", DEFAULT_AUTO_ANALYZE_THRESHOLD, tracker.absoluteThreshold)
	}
	if tracker.ratioThreshold != DEFAULT_AUTO_ANALYZE_RATIO {
		t.Errorf("expected default ratio %f, got %f", DEFAULT_AUTO_ANALYZE_RATIO, tracker.ratioThreshold)
	}
}

func TestNewAutoAnalyzeTracker_CustomConfig(t *testing.T) {
	ss, err := NewStatisticsStorage(StorageConfig{BaseDir: t.TempDir()}, trackerLogger())
	if err != nil {
		t.Fatalf("storage creation failed: %v", err)
	}
	defer ss.Close()

	tracker := NewAutoAnalyzeTracker(AutoAnalyzeConfig{
		Enabled:           false,
		AbsoluteThreshold: 500,
		RatioThreshold:    0.05,
	}, ss, trackerLogger())

	if tracker.absoluteThreshold != 500 {
		t.Errorf("expected threshold 500, got %d", tracker.absoluteThreshold)
	}
	if tracker.ratioThreshold != 0.05 {
		t.Errorf("expected ratio 0.05, got %f", tracker.ratioThreshold)
	}
}

// --- RegisterBundle ---

func TestRegisterBundle(t *testing.T) {
	tracker, _ := newTestTracker(t, false)
	tracker.RegisterBundle("test_bundle", 5000)

	bundles := tracker.ListTrackedBundles()
	if len(bundles) != 1 {
		t.Fatalf("expected 1 tracked bundle, got %d", len(bundles))
	}
	if bundles[0] != "test_bundle" {
		t.Errorf("expected 'test_bundle', got '%s'", bundles[0])
	}
}

func TestRegisterBundle_UpdateExisting(t *testing.T) {
	tracker, _ := newTestTracker(t, false)
	tracker.RegisterBundle("test_bundle", 5000)
	tracker.RegisterBundle("test_bundle", 10000)

	totalDocs, _, _, _ := tracker.GetTrackerStats("test_bundle")
	if totalDocs != 10000 {
		t.Errorf("expected updated total docs 10000, got %d", totalDocs)
	}
}

// --- calculateThreshold ---

func TestCalculateThreshold_SmallBundle(t *testing.T) {
	tracker, _ := newTestTracker(t, false)
	threshold := tracker.calculateThreshold(5000)
	// 10% of 5000 = 500, absoluteThreshold = 1000, max = 1000
	if threshold != 1000 {
		t.Errorf("expected threshold 1000 for small bundle, got %d", threshold)
	}
}

func TestCalculateThreshold_LargeBundle(t *testing.T) {
	tracker, _ := newTestTracker(t, false)
	threshold := tracker.calculateThreshold(50000)
	// 10% of 50000 = 5000, absoluteThreshold = 1000, max = 5000
	if threshold != 5000 {
		t.Errorf("expected threshold 5000 for large bundle, got %d", threshold)
	}
}

func TestCalculateThreshold_BoundaryExact(t *testing.T) {
	tracker, _ := newTestTracker(t, false)
	// Exactly at crossover: 10% of 10000 = 1000 = absoluteThreshold
	threshold := tracker.calculateThreshold(10000)
	if threshold != 1000 {
		t.Errorf("expected threshold 1000 at exact crossover, got %d", threshold)
	}
}

// --- shouldAnalyze ---

func TestShouldAnalyze_BelowThreshold(t *testing.T) {
	tracker, _ := newTestTracker(t, false)
	if tracker.shouldAnalyze(5000, 500) {
		t.Error("should not analyze when below threshold")
	}
}

func TestShouldAnalyze_AtThreshold(t *testing.T) {
	tracker, _ := newTestTracker(t, false)
	if !tracker.shouldAnalyze(5000, 1000) {
		t.Error("should analyze when at threshold")
	}
}

func TestShouldAnalyze_AboveThreshold(t *testing.T) {
	tracker, _ := newTestTracker(t, false)
	if !tracker.shouldAnalyze(5000, 2000) {
		t.Error("should analyze when above threshold")
	}
}

// --- Track changes ---

func TestTrackInserts(t *testing.T) {
	tracker, _ := newTestTracker(t, false)
	tracker.RegisterBundle("test_bundle", 1000)

	// Drain the analysis queue from registration (needsReAnalyze because no stats on disk)
	drainQueue(tracker)

	tracker.TrackInserts("test_bundle", 50)

	totalDocs, changes, _, _ := tracker.GetTrackerStats("test_bundle")
	if totalDocs != 1050 {
		t.Errorf("expected total docs 1050, got %d", totalDocs)
	}
	if changes != 50 {
		t.Errorf("expected 50 changes, got %d", changes)
	}
}

func TestTrackUpdates(t *testing.T) {
	tracker, _ := newTestTracker(t, false)
	tracker.RegisterBundle("test_bundle", 1000)
	drainQueue(tracker)

	tracker.TrackUpdates("test_bundle", 30)

	totalDocs, changes, _, _ := tracker.GetTrackerStats("test_bundle")
	if totalDocs != 1000 {
		t.Errorf("expected total docs 1000 (unchanged), got %d", totalDocs)
	}
	if changes != 30 {
		t.Errorf("expected 30 changes, got %d", changes)
	}
}

func TestTrackDeletes(t *testing.T) {
	tracker, _ := newTestTracker(t, false)
	tracker.RegisterBundle("test_bundle", 1000)
	drainQueue(tracker)

	tracker.TrackDeletes("test_bundle", 20)

	totalDocs, changes, _, _ := tracker.GetTrackerStats("test_bundle")
	if totalDocs != 980 {
		t.Errorf("expected total docs 980, got %d", totalDocs)
	}
	if changes != 20 {
		t.Errorf("expected 20 changes, got %d", changes)
	}
}

func TestTrackChanges_UnregisteredBundle(t *testing.T) {
	tracker, _ := newTestTracker(t, false)
	// Should not panic
	tracker.TrackInserts("nonexistent", 10)
	tracker.TrackUpdates("nonexistent", 10)
	tracker.TrackDeletes("nonexistent", 10)
}

func TestTrackChanges_TriggersAnalysis(t *testing.T) {
	tracker, _ := newTestTracker(t, false)
	tracker.RegisterBundle("test_bundle", 5000)
	drainQueue(tracker)

	// MarkAnalyzed to clear NeedsReAnalyze
	tracker.MarkAnalyzed("test_bundle")

	// Insert enough to reach threshold (1000 for 5000-doc bundle)
	tracker.TrackInserts("test_bundle", 1000)

	// Check if queued
	count := tracker.GetPendingAnalysisCount()
	if count == 0 {
		// May have been consumed by checking needsAnalysis flag
		_, _, _, needsAnalysis := tracker.GetTrackerStats("test_bundle")
		if !needsAnalysis {
			t.Error("expected analysis to be triggered after reaching threshold")
		}
	}
}

// --- MarkAnalyzed ---

func TestMarkAnalyzed(t *testing.T) {
	tracker, _ := newTestTracker(t, false)
	tracker.RegisterBundle("test_bundle", 5000)
	drainQueue(tracker)

	// Track some changes
	tracker.TrackInserts("test_bundle", 100)

	// Mark analyzed
	before := time.Now()
	tracker.MarkAnalyzed("test_bundle")
	after := time.Now()

	_, changes, lastAnalyzed, needsAnalysis := tracker.GetTrackerStats("test_bundle")
	if changes != 0 {
		t.Errorf("expected 0 changes after MarkAnalyzed, got %d", changes)
	}
	if lastAnalyzed.Before(before) || lastAnalyzed.After(after) {
		t.Errorf("lastAnalyzed %v not in expected range [%v, %v]", lastAnalyzed, before, after)
	}
	if needsAnalysis {
		t.Error("expected needsAnalysis to be false after MarkAnalyzed")
	}
}

// --- GetTrackerStats ---

func TestGetTrackerStats(t *testing.T) {
	tracker, _ := newTestTracker(t, false)
	tracker.RegisterBundle("test_bundle", 5000)
	drainQueue(tracker)
	tracker.MarkAnalyzed("test_bundle")

	tracker.TrackInserts("test_bundle", 200)

	totalDocs, changes, lastAnalyzed, needsAnalysis := tracker.GetTrackerStats("test_bundle")
	if totalDocs != 5200 {
		t.Errorf("expected 5200 total docs, got %d", totalDocs)
	}
	if changes != 200 {
		t.Errorf("expected 200 changes, got %d", changes)
	}
	if lastAnalyzed.IsZero() {
		t.Error("expected lastAnalyzed to be set")
	}
	if needsAnalysis {
		t.Error("expected needsAnalysis to be false (200 < 1000 threshold)")
	}
}

func TestGetTrackerStats_Unregistered(t *testing.T) {
	tracker, _ := newTestTracker(t, false)
	totalDocs, changes, lastAnalyzed, needsAnalysis := tracker.GetTrackerStats("nonexistent")
	if totalDocs != 0 || changes != 0 || !lastAnalyzed.IsZero() || needsAnalysis {
		t.Error("expected zero values for unregistered bundle")
	}
}

// --- ForceAnalysis ---

func TestForceAnalysis(t *testing.T) {
	tracker, _ := newTestTracker(t, false)
	tracker.RegisterBundle("test_bundle", 5000)
	drainQueue(tracker)
	tracker.MarkAnalyzed("test_bundle")

	err := tracker.ForceAnalysis("test_bundle")
	if err != nil {
		t.Fatalf("ForceAnalysis failed: %v", err)
	}

	_, _, _, needsAnalysis := tracker.GetTrackerStats("test_bundle")
	if !needsAnalysis {
		t.Error("expected needsAnalysis to be true after ForceAnalysis")
	}
}

func TestForceAnalysis_Unregistered(t *testing.T) {
	tracker, _ := newTestTracker(t, false)
	err := tracker.ForceAnalysis("nonexistent")
	if err == nil {
		t.Fatal("expected error for unregistered bundle")
	}
}

// --- Stop ---

func TestAutoAnalyzeTracker_Stop(t *testing.T) {
	// Don't use newTestTracker with enabled=true since we want to Stop() manually
	ss, err := NewStatisticsStorage(StorageConfig{BaseDir: t.TempDir()}, trackerLogger())
	if err != nil {
		t.Fatalf("storage creation failed: %v", err)
	}
	defer ss.Close()

	tracker := NewAutoAnalyzeTracker(AutoAnalyzeConfig{
		Enabled: true,
	}, ss, trackerLogger())

	// Stop should complete without hanging
	done := make(chan struct{})
	go func() {
		tracker.Stop()
		close(done)
	}()
	select {
	case <-done:
		// success
	case <-time.After(5 * time.Second):
		t.Fatal("Stop() timed out")
	}
}

// --- GetPendingAnalysisCount ---

func TestGetPendingAnalysisCount(t *testing.T) {
	tracker, _ := newTestTracker(t, false)
	initial := tracker.GetPendingAnalysisCount()
	if initial != 0 {
		t.Errorf("expected 0 pending initially, got %d", initial)
	}
}

// --- ListTrackedBundles ---

func TestListTrackedBundles(t *testing.T) {
	tracker, _ := newTestTracker(t, false)
	tracker.RegisterBundle("bundle_a", 1000)
	tracker.RegisterBundle("bundle_b", 2000)
	tracker.RegisterBundle("bundle_c", 3000)

	bundles := tracker.ListTrackedBundles()
	if len(bundles) != 3 {
		t.Errorf("expected 3 tracked bundles, got %d", len(bundles))
	}

	bundleSet := make(map[string]bool)
	for _, b := range bundles {
		bundleSet[b] = true
	}
	for _, expected := range []string{"bundle_a", "bundle_b", "bundle_c"} {
		if !bundleSet[expected] {
			t.Errorf("missing expected bundle: %s", expected)
		}
	}
}

// --- Concurrent tracking ---

func TestConcurrentTracking(t *testing.T) {
	tracker, _ := newTestTracker(t, false)
	tracker.RegisterBundle("test_bundle", 100000)
	drainQueue(tracker)
	tracker.MarkAnalyzed("test_bundle")

	const goroutines = 20
	const opsPerGoroutine = 100
	var wg sync.WaitGroup

	for i := 0; i < goroutines; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for j := 0; j < opsPerGoroutine; j++ {
				if id%2 == 0 {
					tracker.TrackInserts("test_bundle", 1)
				} else {
					tracker.TrackUpdates("test_bundle", 1)
				}
			}
		}(i)
	}

	wg.Wait()

	totalDocs, changes, _, _ := tracker.GetTrackerStats("test_bundle")
	expectedInserts := int64(goroutines / 2 * opsPerGoroutine)
	expectedChanges := int64(goroutines * opsPerGoroutine)

	if totalDocs != 100000+expectedInserts {
		t.Errorf("expected total docs %d, got %d", 100000+expectedInserts, totalDocs)
	}
	if changes != expectedChanges {
		t.Errorf("expected changes %d, got %d", expectedChanges, changes)
	}
}

// helper: drain the analysis queue to clear initial registration triggers
func drainQueue(tracker *AutoAnalyzeTracker) {
	for {
		select {
		case <-tracker.analysisQueue:
		default:
			return
		}
	}
}
