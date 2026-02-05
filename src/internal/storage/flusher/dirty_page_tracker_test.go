package flusher

import (
	"sync"
	"testing"

	"syndrdb/src/internal/domain/models"
)

// TestDirtyPageTrackerBasic verifies basic dirty page operations.
func TestDirtyPageTrackerBasic(t *testing.T) {
	config := DefaultDirtyPageTrackerConfig
	tracker := NewDirtyPageTracker(config)
	defer tracker.Close()

	doc := &models.Document{
		DocumentID: "test-doc-1",
		Fields:     make(map[string]models.Field),
	}

	// Mark a page dirty
	shouldFlush := tracker.MarkDirty("test-bundle", 1, doc, 100)

	if shouldFlush {
		t.Error("Should not flush after single document")
	}

	// Verify page was tracked
	var pageDocCount int
	var pageBundle string
	tracker.GetDirtyPageAndDo("test-bundle", 1, func(page *DirtyPage) {
		if page == nil {
			return
		}
		pageDocCount = len(page.Documents)
		pageBundle = page.BundleName
	})
	if pageDocCount == 0 && pageBundle == "" {
		t.Fatal("Expected dirty page to be tracked")
	}
	if pageDocCount != 1 {
		t.Errorf("Expected 1 document, got %d", pageDocCount)
	}
	if pageBundle != "test-bundle" {
		t.Errorf("Expected bundle name 'test-bundle', got '%s'", pageBundle)
	}

	// Verify bundle tracking
	pages := tracker.GetDirtyPagesForBundle("test-bundle")
	if len(pages) != 1 || pages[0] != 1 {
		t.Errorf("Expected [1], got %v", pages)
	}
}

// TestDirtyPageTrackerThreshold verifies flush threshold triggers.
func TestDirtyPageTrackerThreshold(t *testing.T) {
	config := DirtyPageTrackerConfig{
		WorkerCount:     2,
		MaxDocsPerPage:  5, // Low threshold for testing
		MaxBytesPerPage: 1024 * 1024,
		QueueSize:       100,
	}
	tracker := NewDirtyPageTracker(config)
	defer tracker.Close()

	// Add documents until threshold
	for i := 0; i < 4; i++ {
		doc := &models.Document{DocumentID: "doc-" + string(rune('a'+i))}
		shouldFlush := tracker.MarkDirty("bundle", 1, doc, 100)
		if shouldFlush {
			t.Errorf("Should not flush at %d documents", i+1)
		}
	}

	// 5th document should trigger flush
	doc := &models.Document{DocumentID: "doc-e"}
	shouldFlush := tracker.MarkDirty("bundle", 1, doc, 100)
	if !shouldFlush {
		t.Error("Should flush after reaching threshold")
	}
}

// TestDirtyPageTrackerBytesThreshold verifies byte size threshold.
func TestDirtyPageTrackerBytesThreshold(t *testing.T) {
	config := DirtyPageTrackerConfig{
		WorkerCount:     2,
		MaxDocsPerPage:  1000, // High doc threshold
		MaxBytesPerPage: 500,  // Low byte threshold
		QueueSize:       100,
	}
	tracker := NewDirtyPageTracker(config)
	defer tracker.Close()

	doc := &models.Document{DocumentID: "doc-1"}

	// First write under threshold
	shouldFlush := tracker.MarkDirty("bundle", 1, doc, 200)
	if shouldFlush {
		t.Error("Should not flush under byte threshold")
	}

	// Second write pushes over threshold
	shouldFlush = tracker.MarkDirty("bundle", 1, doc, 400)
	if !shouldFlush {
		t.Error("Should flush after exceeding byte threshold")
	}
}

// TestDirtyPageTrackerPageKeyNoCollision verifies that different (bundle, pageID) pairs
// are stored and retrieved as distinct pages (no hash collision from struct key).
func TestDirtyPageTrackerPageKeyNoCollision(t *testing.T) {
	tracker := NewDirtyPageTracker(DefaultDirtyPageTrackerConfig)
	defer tracker.Close()

	const samePageID = 42
	docA := &models.Document{DocumentID: "doc-bundle-a"}
	docB := &models.Document{DocumentID: "doc-bundle-b"}

	tracker.MarkDirty("bundle-a", samePageID, docA, 100)
	tracker.MarkDirty("bundle-b", samePageID, docB, 100)

	var nameA, nameB string
	var docIDA, docIDB string
	var foundA, foundB bool
	tracker.GetDirtyPageAndDo("bundle-a", samePageID, func(p *DirtyPage) {
		if p != nil {
			foundA = true
			nameA = p.BundleName
			if len(p.Documents) > 0 {
				docIDA = p.Documents[0].DocumentID
			}
		}
	})
	tracker.GetDirtyPageAndDo("bundle-b", samePageID, func(p *DirtyPage) {
		if p != nil {
			foundB = true
			nameB = p.BundleName
			if len(p.Documents) > 0 {
				docIDB = p.Documents[0].DocumentID
			}
		}
	})
	if !foundA || !foundB {
		t.Fatalf("both pages must exist: foundA=%v foundB=%v", foundA, foundB)
	}
	if nameA != "bundle-a" || docIDA != "doc-bundle-a" {
		t.Errorf("bundle-a page: BundleName=%s docID=%s", nameA, docIDA)
	}
	if nameB != "bundle-b" || docIDB != "doc-bundle-b" {
		t.Errorf("bundle-b page: BundleName=%s docID=%s", nameB, docIDB)
	}
}

// TestDirtyPageTrackerMultipleBundles verifies tracking across bundles.
func TestDirtyPageTrackerMultipleBundles(t *testing.T) {
	tracker := NewDirtyPageTracker(DefaultDirtyPageTrackerConfig)
	defer tracker.Close()

	bundles := []string{"bundle-a", "bundle-b", "bundle-c"}
	pagesPerBundle := 3

	for _, bundle := range bundles {
		for page := uint32(0); page < uint32(pagesPerBundle); page++ {
			doc := &models.Document{DocumentID: "doc"}
			tracker.MarkDirty(bundle, page, doc, 100)
		}
	}

	// Verify each bundle has correct pages
	for _, bundle := range bundles {
		pages := tracker.GetDirtyPagesForBundle(bundle)
		if len(pages) != pagesPerBundle {
			t.Errorf("Bundle %s: expected %d pages, got %d", bundle, pagesPerBundle, len(pages))
		}
	}

	stats := tracker.GetStats()
	expectedPages := len(bundles) * pagesPerBundle
	if stats.DirtyPageCount != uint64(expectedPages) {
		t.Errorf("Expected %d dirty pages, got %d", expectedPages, stats.DirtyPageCount)
	}
}

// TestDirtyPageTrackerEnqueueFlush verifies page enqueuing for flush.
func TestDirtyPageTrackerEnqueueFlush(t *testing.T) {
	config := DirtyPageTrackerConfig{
		WorkerCount:     2,
		MaxDocsPerPage:  100,
		MaxBytesPerPage: 1024 * 1024,
		QueueSize:       10,
	}
	tracker := NewDirtyPageTracker(config)
	defer tracker.Close()

	doc := &models.Document{DocumentID: "doc-1"}
	tracker.MarkDirty("test-bundle", 5, doc, 100)

	// Enqueue for flush
	enqueued := tracker.EnqueueForFlush("test-bundle", 5)
	if !enqueued {
		t.Error("Expected page to be enqueued")
	}

	// Page should be removed from tracker
	var found bool
	tracker.GetDirtyPageAndDo("test-bundle", 5, func(p *DirtyPage) { found = true })
	if found {
		t.Error("Page should be removed from tracker after enqueue")
	}

	// Page should be in worker queue
	workerID := 5 % config.WorkerCount
	select {
	case p := <-tracker.GetPagesForWorker(workerID):
		if p.PageID != 5 {
			t.Errorf("Expected page 5, got %d", p.PageID)
		}
	default:
		t.Error("Expected page in worker queue")
	}
}

// TestDirtyPageTrackerConcurrent verifies thread safety.
func TestDirtyPageTrackerConcurrent(t *testing.T) {
	tracker := NewDirtyPageTracker(DefaultDirtyPageTrackerConfig)
	defer tracker.Close()

	const goroutines = 10
	const docsPerGoroutine = 100

	var wg sync.WaitGroup
	wg.Add(goroutines)

	for g := 0; g < goroutines; g++ {
		go func(gid int) {
			defer wg.Done()
			bundle := "bundle-" + string(rune('a'+gid%3))
			pageID := uint32(gid % 5)

			for i := 0; i < docsPerGoroutine; i++ {
				doc := &models.Document{DocumentID: "doc"}
				tracker.MarkDirty(bundle, pageID, doc, 50)
			}
		}(g)
	}

	wg.Wait()

	stats := tracker.GetStats()
	expectedDocs := uint64(goroutines * docsPerGoroutine)
	if stats.PendingDocs != expectedDocs {
		t.Errorf("Expected %d pending docs, got %d", expectedDocs, stats.PendingDocs)
	}
}

// TestDirtyPageTrackerIndexUpdates verifies index update tracking.
func TestDirtyPageTrackerIndexUpdates(t *testing.T) {
	tracker := NewDirtyPageTracker(DefaultDirtyPageTrackerConfig)
	defer tracker.Close()

	update := IndexUpdate{
		IndexName: "hash_idx_name",
		Key:       "john",
		DocID:     "doc-123",
		Operation: "put",
		Sequence:  1,
	}

	tracker.MarkIndexDirty("test-bundle", 10, update)

	var updateCount int
	var firstKey string
	tracker.GetDirtyPageAndDo("test-bundle", 10, func(page *DirtyPage) {
		if page != nil {
			updateCount = len(page.IndexUpdates)
			if len(page.IndexUpdates) > 0 {
				firstKey = page.IndexUpdates[0].Key
			}
		}
	})
	if updateCount == 0 {
		t.Fatal("Expected dirty page with index updates")
	}
	if updateCount != 1 {
		t.Errorf("Expected 1 index update, got %d", updateCount)
	}
	if firstKey != "john" {
		t.Errorf("Expected key 'john', got '%s'", firstKey)
	}
}

// TestDirtyPageTrackerEnqueueDecrementsCount verifies totalDirtyPages is decremented
// when pages are successfully enqueued for flush (Issue 1).
func TestDirtyPageTrackerEnqueueDecrementsCount(t *testing.T) {
	config := DirtyPageTrackerConfig{
		WorkerCount:     1,
		MaxDocsPerPage:   100,
		MaxBytesPerPage:  1024 * 1024,
		QueueSize:        10,
	}
	tracker := NewDirtyPageTracker(config)
	defer tracker.Close()

	doc := &models.Document{DocumentID: "doc"}
	for i := uint32(0); i < 5; i++ {
		tracker.MarkDirty("bundle", i, doc, 100)
	}
	if n := tracker.GetStats().DirtyPageCount; n != 5 {
		t.Errorf("expected 5 dirty pages, got %d", n)
	}
	for i := uint32(0); i < 5; i++ {
		tracker.EnqueueForFlush("bundle", i)
	}
	if n := tracker.GetStats().DirtyPageCount; n != 0 {
		t.Errorf("after enqueue all, expected 0 dirty pages, got %d", n)
	}
}

// TestDirtyPageTrackerConcurrentMarkAndEnqueue verifies that concurrent MarkDirty
// and EnqueueForFlush for the same page do not lose updates or race (Issue 4).
// Run with -race to detect data races.
func TestDirtyPageTrackerConcurrentMarkAndEnqueue(t *testing.T) {
	config := DirtyPageTrackerConfig{
		WorkerCount:     1,
		MaxDocsPerPage:  100,
		MaxBytesPerPage: 1024 * 1024,
		QueueSize:       100,
	}
	tracker := NewDirtyPageTracker(config)
	defer tracker.Close()

	const numRounds = 20
	var wg sync.WaitGroup
	for r := 0; r < numRounds; r++ {
		wg.Add(2)
		go func() {
			defer wg.Done()
			for i := 0; i < 10; i++ {
				doc := &models.Document{DocumentID: "doc"}
				tracker.MarkDirty("bundle", 1, doc, 100)
			}
		}()
		go func() {
			defer wg.Done()
			for i := 0; i < 10; i++ {
				tracker.EnqueueForFlush("bundle", 1)
			}
		}()
	}
	wg.Wait()
}

// TestDirtyPageTrackerClearPageTotalDocsPending verifies totalDocsPending after
// MarkDirty and ClearPage matches expected (Issue 10 helper).
func TestDirtyPageTrackerClearPageTotalDocsPending(t *testing.T) {
	config := DirtyPageTrackerConfig{
		WorkerCount: 1, MaxDocsPerPage: 100, MaxBytesPerPage: 1024 * 1024, QueueSize: 10,
	}
	tracker := NewDirtyPageTracker(config)
	defer tracker.Close()

	for i := 0; i < 3; i++ {
		tracker.MarkDirty("bundle", 1, &models.Document{DocumentID: "doc"}, 100)
	}
	if n := tracker.GetStats().PendingDocs; n != 3 {
		t.Errorf("expected 3 pending docs, got %d", n)
	}
	tracker.EnqueueForFlush("bundle", 1)
	page := <-tracker.GetPagesForWorker(0) // single worker, page 1 % 1 = 0
	if page == nil {
		t.Fatal("expected page from queue")
	}
	tracker.ClearPage(page)
	if n := tracker.GetStats().PendingDocs; n != 0 {
		t.Errorf("after ClearPage(3 docs), expected 0 pending docs, got %d", n)
	}
}

// BenchmarkDirtyPageTrackerMarkDirty measures marking overhead.
func BenchmarkDirtyPageTrackerMarkDirty(b *testing.B) {
	tracker := NewDirtyPageTracker(DefaultDirtyPageTrackerConfig)
	defer tracker.Close()

	doc := &models.Document{
		DocumentID: "bench-doc",
		Fields:     make(map[string]models.Field),
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		pageID := uint32(i % 100)
		tracker.MarkDirty("bench-bundle", pageID, doc, 100)
	}
}

// BenchmarkDirtyPageTrackerConcurrent measures concurrent marking.
func BenchmarkDirtyPageTrackerConcurrent(b *testing.B) {
	tracker := NewDirtyPageTracker(DefaultDirtyPageTrackerConfig)
	defer tracker.Close()

	doc := &models.Document{
		DocumentID: "bench-doc",
		Fields:     make(map[string]models.Field),
	}

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			pageID := uint32(i % 100)
			tracker.MarkDirty("bench-bundle", pageID, doc, 100)
			i++
		}
	})
}
