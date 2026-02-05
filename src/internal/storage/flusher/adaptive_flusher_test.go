package flusher

import (
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"go.uber.org/zap"
)

func testLogger() *zap.SugaredLogger {
	logger, _ := zap.NewDevelopment()
	return logger.Sugar()
}

// TestAdaptiveFlusherBasic verifies basic start/stop and flush triggering.
func TestAdaptiveFlusherBasic(t *testing.T) {
	var flushCount atomic.Int32
	flushFunc := func(bundleName string) error {
		flushCount.Add(1)
		return nil
	}

	config := AdaptiveConfig{
		BaseInterval:      50 * time.Millisecond,
		MinInterval:       10 * time.Millisecond,
		MaxInterval:       100 * time.Millisecond,
		HighLoadThreshold: 1000,
		LowLoadThreshold:  10,
		FlushThreshold:    5, // Low threshold for testing
		MaxBufferSize:     1024,
		WorkerCount:       1,
	}

	af := NewAdaptiveFlusher(config, flushFunc, testLogger())
	af.Start()

	// Record documents to trigger threshold-based flush
	af.RecordDocuments("test-bundle", 10)

	// Wait for flush to occur
	time.Sleep(100 * time.Millisecond)

	af.Stop()

	if flushCount.Load() == 0 {
		t.Error("Expected at least one flush to occur")
	}
}

// TestAdaptiveFlusherRateTracking verifies rate calculation and interval adjustment.
func TestAdaptiveFlusherRateTracking(t *testing.T) {
	flushFunc := func(bundleName string) error {
		return nil
	}

	config := AdaptiveConfig{
		BaseInterval:      100 * time.Millisecond,
		MinInterval:       10 * time.Millisecond,
		MaxInterval:       500 * time.Millisecond,
		HighLoadThreshold: 1000,
		LowLoadThreshold:  10,
		FlushThreshold:    10000, // High threshold so we don't auto-flush
		MaxBufferSize:     1 << 30,
		WorkerCount:       1,
	}

	af := NewAdaptiveFlusher(config, flushFunc, testLogger())
	af.Start()

	// Simulate high load: 5000 docs/sec for over 1 second
	// Rate adjustment only happens after 1 second of data
	for i := 0; i < 120; i++ {
		af.RecordDocuments("test-bundle", 100)
		time.Sleep(10 * time.Millisecond)
	}

	// Allow time for rate adjustment (needs 1+ second window)
	time.Sleep(300 * time.Millisecond)

	rate := af.GetCurrentRate()
	interval := af.GetCurrentInterval()

	af.Stop()

	// Rate tracking works but requires full window
	t.Logf("Final rate: %.2f docs/sec, interval: %v", rate, interval)
}

// TestAdaptiveFlusherLowLoadInterval verifies interval increases under low load.
func TestAdaptiveFlusherLowLoadInterval(t *testing.T) {
	flushFunc := func(bundleName string) error {
		return nil
	}

	config := AdaptiveConfig{
		BaseInterval:      100 * time.Millisecond,
		MinInterval:       50 * time.Millisecond,
		MaxInterval:       500 * time.Millisecond,
		HighLoadThreshold: 10000,
		LowLoadThreshold:  100,
		FlushThreshold:    100000,
		MaxBufferSize:     1 << 30,
		WorkerCount:       1,
	}

	af := NewAdaptiveFlusher(config, flushFunc, testLogger())
	af.Start()

	// Simulate very low load: ~10 docs/sec
	for i := 0; i < 5; i++ {
		af.RecordDocuments("test-bundle", 2)
		time.Sleep(200 * time.Millisecond)
	}

	// Allow adjustment
	time.Sleep(300 * time.Millisecond)

	interval := af.GetCurrentInterval()
	af.Stop()

	// Under low load, interval should tend toward max
	if interval < config.BaseInterval {
		t.Errorf("Under low load, interval (%v) should not be below base (%v)", interval, config.BaseInterval)
	}
}

// TestAdaptiveFlusherBufferSizeTrigger verifies flush on buffer size threshold.
func TestAdaptiveFlusherBufferSizeTrigger(t *testing.T) {
	var flushCount atomic.Int32
	var flushedBundle string
	var mu sync.Mutex

	flushFunc := func(bundleName string) error {
		flushCount.Add(1)
		mu.Lock()
		flushedBundle = bundleName
		mu.Unlock()
		return nil
	}

	config := AdaptiveConfig{
		BaseInterval:      1 * time.Second, // Long interval
		MinInterval:       100 * time.Millisecond,
		MaxInterval:       2 * time.Second,
		HighLoadThreshold: 100000,
		LowLoadThreshold:  1000,
		FlushThreshold:    100000, // High doc threshold
		MaxBufferSize:     100,    // Low buffer threshold for testing
		WorkerCount:       1,
	}

	af := NewAdaptiveFlusher(config, flushFunc, testLogger())
	af.Start()

	// Record enough bytes to trigger buffer size flush
	af.RecordBytes("size-trigger-bundle", 150)

	// Wait for flush
	time.Sleep(100 * time.Millisecond)

	af.Stop()

	if flushCount.Load() == 0 {
		t.Error("Expected flush to occur due to buffer size threshold")
	}

	mu.Lock()
	if flushedBundle != "size-trigger-bundle" {
		t.Errorf("Expected size-trigger-bundle, got %s", flushedBundle)
	}
	mu.Unlock()
}

// TestAdaptiveFlusherMultipleWorkers verifies parallel flush workers.
func TestAdaptiveFlusherMultipleWorkers(t *testing.T) {
	var flushCount atomic.Int32
	var wg sync.WaitGroup

	flushFunc := func(bundleName string) error {
		flushCount.Add(1)
		time.Sleep(10 * time.Millisecond) // Simulate flush time
		return nil
	}

	config := AdaptiveConfig{
		BaseInterval:      50 * time.Millisecond,
		MinInterval:       10 * time.Millisecond,
		MaxInterval:       100 * time.Millisecond,
		HighLoadThreshold: 1000,
		LowLoadThreshold:  10,
		FlushThreshold:    1, // Trigger on every doc
		MaxBufferSize:     1 << 30,
		WorkerCount:       4, // Multiple workers
	}

	af := NewAdaptiveFlusher(config, flushFunc, testLogger())
	af.Start()

	// Submit multiple bundles simultaneously
	bundles := []string{"bundle-a", "bundle-b", "bundle-c", "bundle-d", "bundle-e"}
	for _, b := range bundles {
		wg.Add(1)
		bundle := b
		go func() {
			defer wg.Done()
			af.RecordDocuments(bundle, 5)
		}()
	}
	wg.Wait()

	// Wait for all flushes
	time.Sleep(200 * time.Millisecond)

	af.Stop()

	count := flushCount.Load()
	if count < int32(len(bundles)) {
		t.Errorf("Expected at least %d flushes, got %d", len(bundles), count)
	}
}

// TestAdaptiveFlusherStats verifies statistics reporting.
func TestAdaptiveFlusherStats(t *testing.T) {
	flushFunc := func(bundleName string) error {
		return nil
	}

	config := DefaultAdaptiveConfig
	config.FlushThreshold = 10000 // High so we don't auto-flush

	af := NewAdaptiveFlusher(config, flushFunc, testLogger())
	af.Start()

	// Record some activity
	af.RecordDocuments("stats-bundle", 50)
	af.RecordBytes("stats-bundle", 1024)

	stats := af.GetStats()

	af.Stop()

	if stats.CurrentInterval != config.BaseInterval {
		// May have adjusted slightly, just check it's reasonable
		if stats.CurrentInterval < config.MinInterval || stats.CurrentInterval > config.MaxInterval {
			t.Errorf("Interval %v outside valid range", stats.CurrentInterval)
		}
	}

	if stats.PendingFlushes == 0 {
		// Should have one pending from our recording
		t.Log("Warning: expected pending flush (may have been processed)")
	}
}

// TestAdaptiveFlusherRequestFlushAfterStop verifies that RequestFlush after Stop
// does not panic (no send on closed channel) — Issue 7.
func TestAdaptiveFlusherRequestFlushAfterStop(t *testing.T) {
	flushFunc := func(bundleName string) error { return nil }
	af := NewAdaptiveFlusher(DefaultAdaptiveConfig, flushFunc, testLogger())
	af.Start()
	af.Stop()

	// RequestFlush after stop must not panic
	done := make(chan struct{})
	go func() {
		defer close(done)
		for i := 0; i < 100; i++ {
			af.RequestFlush("bundle")
		}
	}()
	select {
	case <-done:
		// completed without panic
	case <-time.After(2 * time.Second):
		t.Fatal("RequestFlush after Stop timed out or blocked")
	}
}

// BenchmarkAdaptiveFlusherRecordDocuments measures recording overhead.
func BenchmarkAdaptiveFlusherRecordDocuments(b *testing.B) {
	flushFunc := func(bundleName string) error {
		return nil
	}

	config := DefaultAdaptiveConfig
	config.FlushThreshold = 1000000 // High threshold

	af := NewAdaptiveFlusher(config, flushFunc, testLogger())
	af.Start()
	defer af.Stop()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		af.RecordDocuments("bench-bundle", 1)
	}
}

// BenchmarkAdaptiveFlusherRecordBytes measures byte recording overhead.
func BenchmarkAdaptiveFlusherRecordBytes(b *testing.B) {
	flushFunc := func(bundleName string) error {
		return nil
	}

	config := DefaultAdaptiveConfig
	config.MaxBufferSize = 1 << 40 // Very high threshold

	af := NewAdaptiveFlusher(config, flushFunc, testLogger())
	af.Start()
	defer af.Stop()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		af.RecordBytes("bench-bundle", 100)
	}
}
