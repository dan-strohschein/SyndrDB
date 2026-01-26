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
	page := tracker.GetDirtyPage("test-bundle", 1)
	if page == nil {
		t.Fatal("Expected dirty page to be tracked")
	}

	if len(page.Documents) != 1 {
		t.Errorf("Expected 1 document, got %d", len(page.Documents))
	}

	if page.BundleName != "test-bundle" {
		t.Errorf("Expected bundle name 'test-bundle', got '%s'", page.BundleName)
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
	page := tracker.GetDirtyPage("test-bundle", 5)
	if page != nil {
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

	page := tracker.GetDirtyPage("test-bundle", 10)
	if page == nil {
		t.Fatal("Expected dirty page")
	}

	if len(page.IndexUpdates) != 1 {
		t.Errorf("Expected 1 index update, got %d", len(page.IndexUpdates))
	}

	if page.IndexUpdates[0].Key != "john" {
		t.Errorf("Expected key 'john', got '%s'", page.IndexUpdates[0].Key)
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
