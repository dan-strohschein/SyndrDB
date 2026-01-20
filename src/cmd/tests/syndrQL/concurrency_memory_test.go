package syndrQL

import (
	"context"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"syndrdb/src/internal/server"
)

// TestConcurrency_MemoryGrowth runs concurrent SELECTs and asserts that heap
// allocation does not grow without bound. This complements the memory-leak fixes
// (BundleAdapter.cachedPages cap, plan-cache clone, documentPageMap cap, etc.).
//
// - With delay: N concurrent connections, each sending SELECTs with a short delay.
// - Samples MemStats at start, after warmup, and after a run; growth should be bounded.
func TestConcurrency_MemoryGrowth(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping concurrency memory test in short mode")
	}

	fixture := setupRealServerTB(t)
	seedSimpleAuthorsBundleTB(t, fixture, 300)

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	query := `SELECT * FROM "Authors"`

	// Warmup: a few queries to fill caches
	for i := 0; i < 5; i++ {
		_, _ = server.CommandDirector(ctx, fixture.Database, *fixture.ServiceManager, query, fixture.Logger, time.Now(), nil, "127.0.0.1")
	}

	runtime.GC()
	time.Sleep(100 * time.Millisecond)
	var m0 runtime.MemStats
	runtime.ReadMemStats(&m0)

	// Run concurrent queries for a period
	const concurrency = 20
	const queriesPerGoroutine = 50
	const delayMs = 30 // small delay between queries

	var done int64
	var wg sync.WaitGroup
	for g := 0; g < concurrency; g++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < queriesPerGoroutine; i++ {
				_, err := server.CommandDirector(ctx, fixture.Database, *fixture.ServiceManager, query, fixture.Logger, time.Now(), nil, "127.0.0.1")
				if err != nil {
					t.Logf("CommandDirector error: %v", err)
				}
				atomic.AddInt64(&done, 1)
				if delayMs > 0 {
					time.Sleep(time.Duration(delayMs) * time.Millisecond)
				}
			}
		}()
	}

	// Sample memory midway
	time.Sleep(2 * time.Second)
	runtime.GC()
	time.Sleep(50 * time.Millisecond)
	var m1 runtime.MemStats
	runtime.ReadMemStats(&m1)

	wg.Wait()

	// Final sample
	runtime.GC()
	time.Sleep(100 * time.Millisecond)
	var m2 runtime.MemStats
	runtime.ReadMemStats(&m2)

	// Bounded growth: Alloc at end should not exceed ~4x the midway sample.
	// Without the leak fixes, Alloc would grow to GB. We allow some growth for
	// GC pacing and cached plans but reject unbounded growth.
	allocMid := m1.Alloc
	allocEnd := m2.Alloc
	if allocMid > 0 && allocEnd > allocMid*4 {
		t.Errorf("memory grew without bound: Alloc mid=%.1fMB end=%.1fMB (%.1fx); leaks may remain",
			float64(allocMid)/(1<<20), float64(allocEnd)/(1<<20), float64(allocEnd)/float64(allocMid))
	}

	t.Logf("Concurrency memory: %d queries, Alloc %.1fMB -> %.1fMB", atomic.LoadInt64(&done), float64(m0.Alloc)/(1<<20), float64(allocEnd)/(1<<20))
}
