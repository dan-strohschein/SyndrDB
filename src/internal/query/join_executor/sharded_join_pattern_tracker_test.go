package joinexecutor_test

import (
	"fmt"
	"sync"
	"testing"
	"time"

	joinexecutor "syndrdb/src/internal/query/join_executor"

	"go.uber.org/zap"
)

// TestShardedJoinPatternTracker_HighConcurrency tests the sharded join pattern tracker under high concurrency
func TestShardedJoinPatternTracker_HighConcurrency(t *testing.T) {
	logger := zap.NewNop().Sugar()
	tracker := joinexecutor.NewShardedJoinPatternTracker(logger, 5, 10)

	const numGoroutines = 150
	const opsPerGoroutine = 100

	var wg sync.WaitGroup
	wg.Add(numGoroutines)

	start := time.Now()

	// Spawn 150 goroutines simulating concurrent joins
	for i := 0; i < numGoroutines; i++ {
		go func(id int) {
			defer wg.Done()
			for j := 0; j < opsPerGoroutine; j++ {
				leftBundle := fmt.Sprintf("bundle_%d", (id+j)%20)
				rightBundle := fmt.Sprintf("bundle_%d", ((id+j)/2)%20)

				result := &joinexecutor.JoinResult{
					Algorithm:     "hash_join",
					ExecutionTime: time.Millisecond * time.Duration(10+j%100),
					MemoryUsed:    int64(1024 * (100 + j%900)),
					LeftScanned:   int64(100 + j%400),
					RightScanned:  int64(50 + j%200),
					Documents:     make([]*joinexecutor.JoinedDocument, 10+j%90),
					DiskSpilled:   j%10 == 0,
				}

				tracker.RecordJoin(leftBundle, rightBundle, result)

				if j%5 == 0 {
					tracker.GetJoinStats(leftBundle, rightBundle)
					tracker.IsJoinPatternHot(leftBundle, rightBundle)
				}
			}
		}(i)
	}

	wg.Wait()
	elapsed := time.Since(start)

	totalOps := numGoroutines * opsPerGoroutine
	t.Logf("Completed %d join operations in %v (%.2f ops/sec)",
		totalOps, elapsed, float64(totalOps)/elapsed.Seconds())

	hotPatterns := tracker.GetHotJoinPatterns()
	t.Logf("Identified %d hot join patterns", len(hotPatterns))
}

// BenchmarkShardedJoinPatternTracker_RecordJoin benchmarks the RecordJoin operation
func BenchmarkShardedJoinPatternTracker_RecordJoin(b *testing.B) {
	logger := zap.NewNop().Sugar()
	tracker := joinexecutor.NewShardedJoinPatternTracker(logger, 5, 10)

	result := &joinexecutor.JoinResult{
		Algorithm:     "hash_join",
		ExecutionTime: time.Millisecond * 10,
		MemoryUsed:    1024,
		LeftScanned:   100,
		RightScanned:  100,
		Documents:     make([]*joinexecutor.JoinedDocument, 50),
		DiskSpilled:   false,
	}

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			leftBundle := fmt.Sprintf("bundle_%d", i%100)
			rightBundle := fmt.Sprintf("bundle_%d", (i+1)%100)
			tracker.RecordJoin(leftBundle, rightBundle, result)
			i++
		}
	})
}
