package statistics

import (
	"testing"
	"time"

	"go.uber.org/zap"
)

func schedulerLogger() *zap.SugaredLogger {
	return zap.NewNop().Sugar()
}

func newTestScheduler(t *testing.T, enabled bool, schedTime string) (*ScheduledAnalyzer, *AutoAnalyzeTracker) {
	t.Helper()
	ss, err := NewStatisticsStorage(StorageConfig{BaseDir: t.TempDir()}, schedulerLogger())
	if err != nil {
		t.Fatalf("storage creation failed: %v", err)
	}
	t.Cleanup(func() { ss.Close() })

	tracker := NewAutoAnalyzeTracker(AutoAnalyzeConfig{Enabled: false}, ss, schedulerLogger())

	if schedTime == "" {
		schedTime = DEFAULT_SCHEDULED_TIME
	}

	sa, err := NewScheduledAnalyzer(ScheduledAnalyzerConfig{
		Enabled:       enabled,
		ScheduledTime: schedTime,
	}, tracker, ss, schedulerLogger())
	if err != nil {
		t.Fatalf("scheduler creation failed: %v", err)
	}
	if enabled {
		t.Cleanup(func() { sa.Stop() })
	}

	return sa, tracker
}

// --- parseScheduledTime ---

func TestParseScheduledTime_Valid(t *testing.T) {
	tests := []struct {
		input    string
		wantHour int
		wantMin  int
	}{
		{"02:00", 2, 0},
		{"14:30", 14, 30},
		{"00:00", 0, 0},
		{"23:59", 23, 59},
	}

	for _, tt := range tests {
		parsed, err := parseScheduledTime(tt.input)
		if err != nil {
			t.Errorf("parseScheduledTime(%q) failed: %v", tt.input, err)
			continue
		}
		if parsed.Hour() != tt.wantHour {
			t.Errorf("parseScheduledTime(%q) hour = %d, want %d", tt.input, parsed.Hour(), tt.wantHour)
		}
		if parsed.Minute() != tt.wantMin {
			t.Errorf("parseScheduledTime(%q) minute = %d, want %d", tt.input, parsed.Minute(), tt.wantMin)
		}
	}
}

func TestParseScheduledTime_Invalid(t *testing.T) {
	invalid := []string{
		"25:00",
		"abc",
		"",
		"12:60",
		"12",
		"-1:00",
	}

	for _, input := range invalid {
		_, err := parseScheduledTime(input)
		if err == nil {
			t.Errorf("parseScheduledTime(%q) expected error, got nil", input)
		}
	}
}

// --- NewScheduledAnalyzer ---

func TestNewScheduledAnalyzer_Defaults(t *testing.T) {
	sa, _ := newTestScheduler(t, false, "")
	if sa.minDocuments != DEFAULT_MIN_DOCS {
		t.Errorf("expected default min docs %d, got %d", DEFAULT_MIN_DOCS, sa.minDocuments)
	}
	timeStr := sa.GetScheduledTime()
	if timeStr != DEFAULT_SCHEDULED_TIME {
		t.Errorf("expected default time %s, got %s", DEFAULT_SCHEDULED_TIME, timeStr)
	}
}

func TestNewScheduledAnalyzer_Disabled(t *testing.T) {
	sa, _ := newTestScheduler(t, false, "03:00")
	// Verify it's created but no goroutine leak
	if sa == nil {
		t.Fatal("expected non-nil scheduler")
	}
	timeStr := sa.GetScheduledTime()
	if timeStr != "03:00" {
		t.Errorf("expected time 03:00, got %s", timeStr)
	}
}

func TestNewScheduledAnalyzer_InvalidTime(t *testing.T) {
	ss, err := NewStatisticsStorage(StorageConfig{BaseDir: t.TempDir()}, schedulerLogger())
	if err != nil {
		t.Fatalf("storage creation failed: %v", err)
	}
	defer ss.Close()

	tracker := NewAutoAnalyzeTracker(AutoAnalyzeConfig{Enabled: false}, ss, schedulerLogger())

	_, err = NewScheduledAnalyzer(ScheduledAnalyzerConfig{
		Enabled:       false,
		ScheduledTime: "invalid",
	}, tracker, ss, schedulerLogger())
	if err == nil {
		t.Fatal("expected error for invalid time")
	}
}

// --- shouldRunNow ---

func TestShouldRunNow_CorrectTime(t *testing.T) {
	sa, _ := newTestScheduler(t, false, "14:30")

	now := time.Date(2026, 2, 25, 14, 30, 0, 0, time.UTC)
	var lastRun time.Time // zero = never run

	if !sa.shouldRunNow(now, lastRun) {
		t.Error("expected shouldRunNow to return true at scheduled time")
	}
}

func TestShouldRunNow_WrongTime(t *testing.T) {
	sa, _ := newTestScheduler(t, false, "14:30")

	now := time.Date(2026, 2, 25, 10, 00, 0, 0, time.UTC)
	var lastRun time.Time

	if sa.shouldRunNow(now, lastRun) {
		t.Error("expected shouldRunNow to return false at wrong time")
	}
}

func TestShouldRunNow_TooRecent(t *testing.T) {
	sa, _ := newTestScheduler(t, false, "14:30")

	now := time.Date(2026, 2, 25, 14, 30, 0, 0, time.UTC)
	lastRun := now.Add(-10 * time.Hour) // only 10 hours ago, need 20

	if sa.shouldRunNow(now, lastRun) {
		t.Error("expected shouldRunNow to return false when last run too recent")
	}
}

func TestShouldRunNow_FirstRun(t *testing.T) {
	sa, _ := newTestScheduler(t, false, "14:30")

	now := time.Date(2026, 2, 25, 14, 30, 0, 0, time.UTC)
	var lastRun time.Time // zero value = first run

	if !sa.shouldRunNow(now, lastRun) {
		t.Error("expected shouldRunNow to return true on first run")
	}
}

// --- UpdateScheduledTime ---

func TestUpdateScheduledTime(t *testing.T) {
	sa, _ := newTestScheduler(t, false, "02:00")

	err := sa.UpdateScheduledTime("15:45")
	if err != nil {
		t.Fatalf("UpdateScheduledTime failed: %v", err)
	}

	if sa.GetScheduledTime() != "15:45" {
		t.Errorf("expected 15:45, got %s", sa.GetScheduledTime())
	}
}

func TestUpdateScheduledTime_Invalid(t *testing.T) {
	sa, _ := newTestScheduler(t, false, "02:00")

	err := sa.UpdateScheduledTime("invalid")
	if err == nil {
		t.Fatal("expected error for invalid time format")
	}

	// Verify original time unchanged
	if sa.GetScheduledTime() != "02:00" {
		t.Errorf("expected original time 02:00, got %s", sa.GetScheduledTime())
	}
}

// --- GetScheduledTime ---

func TestGetScheduledTime(t *testing.T) {
	sa, _ := newTestScheduler(t, false, "08:15")
	result := sa.GetScheduledTime()
	if result != "08:15" {
		t.Errorf("expected 08:15, got %s", result)
	}
}

// --- GetNextScheduledRun ---

func TestGetNextScheduledRun_FutureToday(t *testing.T) {
	// Set scheduled time to 23:59 which is likely in the future
	sa, _ := newTestScheduler(t, false, "23:59")
	now := time.Now()
	next := sa.GetNextScheduledRun()

	// If current time is before 23:59, next should be today
	todayScheduled := time.Date(now.Year(), now.Month(), now.Day(), 23, 59, 0, 0, now.Location())
	if now.Before(todayScheduled) {
		if next.Day() != now.Day() {
			t.Errorf("expected today, got %v", next)
		}
	}
	// next should always be in the future or just now
	if next.Before(now.Add(-1 * time.Minute)) {
		t.Errorf("next scheduled run %v should not be in the past (now: %v)", next, now)
	}
}

func TestGetNextScheduledRun_PastToday(t *testing.T) {
	// Set scheduled time to 00:00 which has almost certainly passed
	sa, _ := newTestScheduler(t, false, "00:00")
	now := time.Now()
	next := sa.GetNextScheduledRun()

	// If current time is after 00:00 (which it almost always is), next should be tomorrow
	todayScheduled := time.Date(now.Year(), now.Month(), now.Day(), 0, 0, 0, 0, now.Location())
	if now.After(todayScheduled) {
		tomorrow := todayScheduled.Add(24 * time.Hour)
		if next.Day() != tomorrow.Day() || next.Month() != tomorrow.Month() {
			t.Errorf("expected tomorrow, got %v (now: %v)", next, now)
		}
	}
}

// --- runScheduledAnalysis ---

func TestRunScheduledAnalysis_SkipSmallBundles(t *testing.T) {
	sa, tracker := newTestScheduler(t, false, "02:00")
	tracker.RegisterBundle("small_bundle", 1000) // below 100k minimum
	drainTrackerQueue(tracker)
	tracker.MarkAnalyzed("small_bundle")

	err := sa.runScheduledAnalysis()
	if err != nil {
		t.Fatalf("runScheduledAnalysis failed: %v", err)
	}

	// small_bundle should not be queued
	_, _, _, needsAnalysis := tracker.GetTrackerStats("small_bundle")
	if needsAnalysis {
		t.Error("small bundle should not need analysis")
	}
}

func TestRunScheduledAnalysis_SkipRecentlyAnalyzed(t *testing.T) {
	sa, tracker := newTestScheduler(t, false, "02:00")
	tracker.RegisterBundle("big_bundle", 200000)
	drainTrackerQueue(tracker)
	tracker.MarkAnalyzed("big_bundle") // just analyzed

	err := sa.runScheduledAnalysis()
	if err != nil {
		t.Fatalf("runScheduledAnalysis failed: %v", err)
	}

	// Should not be queued since it was just analyzed
	count := tracker.GetPendingAnalysisCount()
	if count > 0 {
		t.Error("recently analyzed bundle should not be re-queued")
	}
}

func TestRunScheduledAnalysis_SkipAlreadyQueued(t *testing.T) {
	sa, tracker := newTestScheduler(t, false, "02:00")
	tracker.RegisterBundle("big_bundle", 200000)
	drainTrackerQueue(tracker)

	// Force analysis to set needsAnalysis flag
	tracker.ForceAnalysis("big_bundle")
	drainTrackerQueue(tracker) // drain the queue but flag stays

	err := sa.runScheduledAnalysis()
	if err != nil {
		t.Fatalf("runScheduledAnalysis failed: %v", err)
	}
	// The needsAnalysis flag is true, so scheduled analysis should skip it
}

func TestRunScheduledAnalysis_QueuesEligible(t *testing.T) {
	sa, tracker := newTestScheduler(t, false, "02:00")
	tracker.RegisterBundle("big_bundle", 200000)
	drainTrackerQueue(tracker)

	// Set lastAnalyzed to 25 hours ago so it's eligible
	tracker.mu.RLock()
	bt := tracker.trackers["big_bundle"]
	tracker.mu.RUnlock()
	bt.mu.Lock()
	bt.LastAnalyzed = time.Now().Add(-25 * time.Hour)
	bt.NeedsReAnalyze = false
	bt.ChangesSinceAnalyze = 0
	bt.mu.Unlock()

	err := sa.runScheduledAnalysis()
	if err != nil {
		t.Fatalf("runScheduledAnalysis failed: %v", err)
	}

	// Should be queued via ForceAnalysis
	_, _, _, needsAnalysis := tracker.GetTrackerStats("big_bundle")
	if !needsAnalysis {
		t.Error("eligible bundle should be queued for analysis")
	}
}

// --- Stop ---

func TestScheduledAnalyzer_Stop(t *testing.T) {
	// Create manually to avoid cleanup double-Stop
	ss, err := NewStatisticsStorage(StorageConfig{BaseDir: t.TempDir()}, schedulerLogger())
	if err != nil {
		t.Fatalf("storage creation failed: %v", err)
	}
	defer ss.Close()

	tracker := NewAutoAnalyzeTracker(AutoAnalyzeConfig{Enabled: false}, ss, schedulerLogger())

	sa, err := NewScheduledAnalyzer(ScheduledAnalyzerConfig{
		Enabled:       true,
		ScheduledTime: "02:00",
	}, tracker, ss, schedulerLogger())
	if err != nil {
		t.Fatalf("scheduler creation failed: %v", err)
	}

	done := make(chan struct{})
	go func() {
		sa.Stop()
		close(done)
	}()
	select {
	case <-done:
		// success
	case <-time.After(5 * time.Second):
		t.Fatal("Stop() timed out")
	}
}

func drainTrackerQueue(tracker *AutoAnalyzeTracker) {
	for {
		select {
		case <-tracker.analysisQueue:
		default:
			return
		}
	}
}
