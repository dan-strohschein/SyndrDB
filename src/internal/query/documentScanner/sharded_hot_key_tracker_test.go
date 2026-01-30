package documentscanner_test

import (
	"fmt"
	"sync"
	"testing"
	"time"

	"syndrdb/src/internal/query/documentscanner"

	"go.uber.org/zap"
)

// TestShardedHotKeyTracker_HighConcurrency tests the sharded hot key tracker under high concurrency
func TestShardedHotKeyTracker_HighConcurrency(t *testing.T) {
	logger := zap.NewNop().Sugar()
	tracker := documentscanner.NewShardedHotKeyTracker(logger, 100, 500)

	const numGoroutines = 150
	const opsPerGoroutine = 1000

	var wg sync.WaitGroup
	wg.Add(numGoroutines)

	start := time.Now()

	// Spawn 150 goroutines simulating concurrent queries
	for i := 0; i < numGoroutines; i++ {
		go func(id int) {
			defer wg.Done()
			for j := 0; j < opsPerGoroutine; j++ {
				keyName := fmt.Sprintf("key_%d", (id*opsPerGoroutine+j)%500)
				tracker.RecordQuery(keyName, "value", time.Millisecond)

				if j%10 == 0 {
					tracker.IsHotKey(keyName)
				}

				if j%20 == 0 {
					tracker.RecordCacheHit(keyName)
				}
			}
		}(i)
	}

	wg.Wait()
	elapsed := time.Since(start)

	totalOps := numGoroutines * opsPerGoroutine
	t.Logf("Completed %d operations in %v (%.2f ops/sec)",
		totalOps, elapsed, float64(totalOps)/elapsed.Seconds())

	// Verify stats
	stats := tracker.GetOverallStats()
	totalQueries := stats["total_queries"].(int64)

	if totalQueries != int64(totalOps) {
		t.Errorf("Expected %d total queries, got %d", totalOps, totalQueries)
	}

	t.Logf("Overall stats: %+v", stats)
}

// BenchmarkShardedHotKeyTracker_RecordQuery benchmarks the RecordQuery operation
func BenchmarkShardedHotKeyTracker_RecordQuery(b *testing.B) {
	logger := zap.NewNop().Sugar()
	tracker := documentscanner.NewShardedHotKeyTracker(logger, 100, 500)

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			keyName := fmt.Sprintf("key_%d", i%1000)
			tracker.RecordQuery(keyName, "value", time.Millisecond)
			i++
		}
	})
}
