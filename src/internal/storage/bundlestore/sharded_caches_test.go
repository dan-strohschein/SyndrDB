package bundlestore

import (
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"syndrdb/src/internal/domain/models"
)

// TestShardedParsedDocsCacheConcurrentGetAndTouchAndEviction runs GetAndTouch
// and SetWithLRU/eviction from multiple goroutines to ensure lastAccess atomic
// access does not cause data races. Run with -race.
func TestShardedParsedDocsCacheConcurrentGetAndTouchAndEviction(t *testing.T) {
	cache := NewShardedParsedDocsCache()
	const maxEntriesPerShard = 4
	const keys = 20
	const goroutines = 10
	const opsPerGoroutine = 50

	// Pre-populate so eviction can occur
	for i := 0; i < keys; i++ {
		key := string(rune('a' + (i % 26))) + string(rune('0'+(i/26)))
		e := &parsedDocsCacheEntry{
			documents: make(map[string]models.Document),
		}
		e.lastAccess.Store(time.Now().UnixNano())
		cache.SetWithLRU(key, e, maxEntriesPerShard)
	}

	var wg sync.WaitGroup
	var touchCount atomic.Int64
	wg.Add(goroutines)
	for g := 0; g < goroutines; g++ {
		go func(seed int) {
			defer wg.Done()
			for i := 0; i < opsPerGoroutine; i++ {
				key := string(rune('a'+(seed+i)%26)) + string(rune('0'+((seed+i)/26)%10))
				if entry := cache.GetAndTouch(key); entry != nil {
					touchCount.Add(1)
				}
				// Occasionally add new entries to trigger eviction
				if (seed+i)%5 == 0 {
					k := string(rune('z'-i%10)) + string(rune('0'+i%10))
					e := &parsedDocsCacheEntry{documents: make(map[string]models.Document)}
					e.lastAccess.Store(time.Now().UnixNano())
					cache.SetWithLRU(k, e, maxEntriesPerShard)
				}
			}
		}(g)
	}
	wg.Wait()

	if cache.Len() == 0 {
		t.Log("cache empty after concurrent ops (eviction dominated)")
	}
	t.Logf("GetAndTouch touched %d entries", touchCount.Load())
}
