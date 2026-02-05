package hashindexV3

import (
	"sync"
	"testing"
)

// TestMemTableGetConcurrentHitsMisses runs many concurrent Get() calls and verifies
// that hits + misses equals the number of Get invocations (no data race on stats).
// Run with: go test -race -run TestMemTableGetConcurrentHitsMisses
func TestMemTableGetConcurrentHitsMisses(t *testing.T) {
	mt := NewHashMemTable(1000)

	// Pre-populate with some keys
	for i := 0; i < 100; i++ {
		key := string(rune('a' + (i % 26)))
		entry := NewHashIndexEntry(key, "doc", uint32(i), uint64(i), 0, 0)
		if err := mt.Put(entry); err != nil {
			t.Fatalf("Put failed: %v", err)
		}
	}

	const numGoroutines = 20
	const getsPerGoroutine = 500
	totalGets := numGoroutines * getsPerGoroutine

	var wg sync.WaitGroup
	wg.Add(numGoroutines)
	for g := 0; g < numGoroutines; g++ {
		go func(offset int) {
			defer wg.Done()
			for i := 0; i < getsPerGoroutine; i++ {
				key := string(rune('a' + (i+offset)%26))
				mt.Get(key)
			}
		}(g)
	}
	wg.Wait()

	stats := mt.GetStats()
	hitsPlusMisses := stats.Hits + stats.Misses
	if hitsPlusMisses != uint64(totalGets) {
		t.Errorf("hits + misses = %d, expected %d (data race or wrong count)", hitsPlusMisses, totalGets)
	}
}
