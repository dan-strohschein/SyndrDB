package flusher

import (
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"syndrdb/src/internal/domain/models"

	"go.uber.org/zap"
)

// mockPageWriter implements PageWriter for testing.
type mockPageWriter struct {
	writes      []mockWrite
	mu          sync.Mutex
	writeDelay  time.Duration
	shouldError bool
}

type mockWrite struct {
	BundleName string
	PageID     uint32
	DataLen    int
}

func (m *mockPageWriter) WritePageBatch(bundleName string, pageID uint32, data []byte) error {
	if m.writeDelay > 0 {
		time.Sleep(m.writeDelay)
	}

	if m.shouldError {
		return &mockError{}
	}

	m.mu.Lock()
	m.writes = append(m.writes, mockWrite{
		BundleName: bundleName,
		PageID:     pageID,
		DataLen:    len(data),
	})
	m.mu.Unlock()

	return nil
}

func (m *mockPageWriter) WriteIndexUpdates(bundleName string, pageID uint32, updates []IndexUpdate) error {
	// No-op for tests; index updates can be recorded here if needed.
	return nil
}

func (m *mockPageWriter) getWrites() []mockWrite {
	m.mu.Lock()
	defer m.mu.Unlock()
	result := make([]mockWrite, len(m.writes))
	copy(result, m.writes)
	return result
}

type mockError struct{}

func (e *mockError) Error() string { return "mock error" }

func testWorkerLogger() *zap.SugaredLogger {
	logger, _ := zap.NewDevelopment()
	return logger.Sugar()
}

// TestFlushWorkerBasic verifies basic flush worker operation.
func TestFlushWorkerBasic(t *testing.T) {
	config := DirtyPageTrackerConfig{
		WorkerCount:     1,
		MaxDocsPerPage:  100,
		MaxBytesPerPage: 1024 * 1024,
		QueueSize:       10,
	}
	tracker := NewDirtyPageTracker(config)
	writer := &mockPageWriter{}

	worker := NewFlushWorker(0, tracker, writer, testWorkerLogger())
	worker.Start()

	// Add dirty page (schema required for encoding Values)
	schema := models.NewProjectionSchema([]string{"name"})
	doc := &models.Document{
		DocumentID: "test-doc",
		Values:     []models.FieldValue{models.NewStringValue("test-doc"), models.NewStringValue("Test")},
	}
	if len(doc.Values) != len(schema.Names) {
		doc.Values = make([]models.FieldValue, len(schema.Names))
		doc.Values[0] = models.NewStringValue(doc.DocumentID)
		if len(schema.Names) > 1 {
			doc.Values[1] = models.NewStringValue("Test")
		}
	}
	tracker.MarkDirty("test-bundle", 1, doc, 100, schema)

	// Enqueue for flush
	tracker.EnqueueForFlush("test-bundle", 1)

	// Wait for flush
	time.Sleep(100 * time.Millisecond)

	worker.Stop()

	writes := writer.getWrites()
	if len(writes) != 1 {
		t.Errorf("Expected 1 write, got %d", len(writes))
	}

	if len(writes) > 0 && writes[0].BundleName != "test-bundle" {
		t.Errorf("Expected bundle 'test-bundle', got '%s'", writes[0].BundleName)
	}

	stats := worker.GetStats()
	if stats.PagesWritten != 1 {
		t.Errorf("Expected 1 page written, got %d", stats.PagesWritten)
	}
	if stats.DocsWritten != 1 {
		t.Errorf("Expected 1 doc written, got %d", stats.DocsWritten)
	}
}

// TestFlushWorkerMultiplePages verifies handling multiple pages.
func TestFlushWorkerMultiplePages(t *testing.T) {
	config := DirtyPageTrackerConfig{
		WorkerCount:     1,
		MaxDocsPerPage:  100,
		MaxBytesPerPage: 1024 * 1024,
		QueueSize:       100,
	}
	tracker := NewDirtyPageTracker(config)
	writer := &mockPageWriter{}

	worker := NewFlushWorker(0, tracker, writer, testWorkerLogger())
	worker.Start()

	schema := models.NewProjectionSchema(nil)
	docValues := []models.FieldValue{models.NewStringValue("doc")}
	for page := uint32(0); page < 5; page++ {
		for i := 0; i < 3; i++ {
			doc := &models.Document{DocumentID: "doc", Values: docValues}
			tracker.MarkDirty("bundle", page, doc, 50, schema)
		}
	}

	// Enqueue all for flush
	tracker.FlushAllForBundle("bundle")

	// Wait for flushes
	time.Sleep(200 * time.Millisecond)

	worker.Stop()

	writes := writer.getWrites()
	if len(writes) != 5 {
		t.Errorf("Expected 5 writes, got %d", len(writes))
	}

	stats := worker.GetStats()
	if stats.PagesWritten != 5 {
		t.Errorf("Expected 5 pages written, got %d", stats.PagesWritten)
	}
	if stats.DocsWritten != 15 {
		t.Errorf("Expected 15 docs written, got %d", stats.DocsWritten)
	}
}

// TestFlushWorkerPoolBasic verifies worker pool operation.
func TestFlushWorkerPoolBasic(t *testing.T) {
	config := DirtyPageTrackerConfig{
		WorkerCount:     4,
		MaxDocsPerPage:  100,
		MaxBytesPerPage: 1024 * 1024,
		QueueSize:       100,
	}
	tracker := NewDirtyPageTracker(config)
	writer := &mockPageWriter{}

	pool := NewFlushWorkerPool(4, tracker, writer, testWorkerLogger())
	pool.Start()

	schema := models.NewProjectionSchema(nil)
	docValues := []models.FieldValue{models.NewStringValue("doc")}
	for page := uint32(0); page < 8; page++ {
		doc := &models.Document{DocumentID: "doc", Values: docValues}
		tracker.MarkDirty("bundle", page, doc, 100, schema)
		tracker.EnqueueForFlush("bundle", page)
	}

	// Wait for all flushes
	time.Sleep(200 * time.Millisecond)

	pool.Stop()

	writes := writer.getWrites()
	if len(writes) != 8 {
		t.Errorf("Expected 8 writes, got %d", len(writes))
	}

	aggStats := pool.GetAggregateStats()
	if aggStats.TotalPages != 8 {
		t.Errorf("Expected 8 total pages, got %d", aggStats.TotalPages)
	}
	if aggStats.WorkerCount != 4 {
		t.Errorf("Expected 4 workers, got %d", aggStats.WorkerCount)
	}
}

// TestFlushWorkerPoolConcurrent verifies concurrent flush handling.
func TestFlushWorkerPoolConcurrent(t *testing.T) {
	config := DirtyPageTrackerConfig{
		WorkerCount:     4,
		MaxDocsPerPage:  100,
		MaxBytesPerPage: 1024 * 1024,
		QueueSize:       1000,
	}
	tracker := NewDirtyPageTracker(config)
	writer := &mockPageWriter{writeDelay: 1 * time.Millisecond}

	pool := NewFlushWorkerPool(4, tracker, writer, testWorkerLogger())
	pool.Start()

	const numPages = 100
	var wg sync.WaitGroup
	wg.Add(numPages)

	schema := models.NewProjectionSchema(nil)
	docValues := []models.FieldValue{models.NewStringValue("doc")}
	for i := 0; i < numPages; i++ {
		go func(pageID int) {
			defer wg.Done()
			doc := &models.Document{DocumentID: "doc", Values: docValues}
			tracker.MarkDirty("bundle", uint32(pageID), doc, 100, schema)
			tracker.EnqueueForFlush("bundle", uint32(pageID))
		}(i)
	}

	wg.Wait()

	// Wait for all flushes to complete
	time.Sleep(500 * time.Millisecond)

	pool.Stop()

	writes := writer.getWrites()
	if len(writes) != numPages {
		t.Errorf("Expected %d writes, got %d", numPages, len(writes))
	}
}

// TestFlushWorkerErrorHandling verifies error handling.
func TestFlushWorkerErrorHandling(t *testing.T) {
	config := DirtyPageTrackerConfig{
		WorkerCount:     1,
		MaxDocsPerPage:  100,
		MaxBytesPerPage: 1024 * 1024,
		QueueSize:       10,
	}
	tracker := NewDirtyPageTracker(config)
	writer := &mockPageWriter{shouldError: true}

	worker := NewFlushWorker(0, tracker, writer, testWorkerLogger())
	worker.Start()

	schema := models.NewProjectionSchema(nil)
	doc := &models.Document{
		DocumentID: "test-doc",
		Values:     []models.FieldValue{models.NewStringValue("test-doc")},
	}
	tracker.MarkDirty("test-bundle", 1, doc, 100, schema)
	tracker.EnqueueForFlush("test-bundle", 1)

	time.Sleep(100 * time.Millisecond)

	worker.Stop()

	stats := worker.GetStats()
	if stats.WriteErrors == 0 {
		t.Error("Expected write errors to be tracked")
	}
}

// TestFlushWorkerGracefulShutdown verifies pending work is completed.
func TestFlushWorkerGracefulShutdown(t *testing.T) {
	config := DirtyPageTrackerConfig{
		WorkerCount:     1,
		MaxDocsPerPage:  100,
		MaxBytesPerPage: 1024 * 1024,
		QueueSize:       100,
	}
	tracker := NewDirtyPageTracker(config)

	var writeCount atomic.Int32
	writer := &mockPageWriter{}

	worker := NewFlushWorker(0, tracker, writer, testWorkerLogger())
	worker.Start()

	schema := models.NewProjectionSchema(nil)
	docValues := []models.FieldValue{models.NewStringValue("doc")}
	for i := 0; i < 10; i++ {
		doc := &models.Document{DocumentID: "doc", Values: docValues}
		tracker.MarkDirty("bundle", uint32(i), doc, 100, schema)
		tracker.EnqueueForFlush("bundle", uint32(i))
	}

	// Stop immediately - should still complete pending
	worker.Stop()

	writes := writer.getWrites()
	if len(writes) != 10 {
		t.Errorf("Expected 10 writes after graceful shutdown, got %d (atomic: %d)", len(writes), writeCount.Load())
	}
}

// BenchmarkFlushWorkerPool measures pool throughput.
func BenchmarkFlushWorkerPool(b *testing.B) {
	config := DirtyPageTrackerConfig{
		WorkerCount:     4,
		MaxDocsPerPage:  100,
		MaxBytesPerPage: 1024 * 1024,
		QueueSize:       10000,
	}
	tracker := NewDirtyPageTracker(config)
	writer := &mockPageWriter{} // No delay for max throughput

	pool := NewFlushWorkerPool(4, tracker, writer, testWorkerLogger())
	pool.Start()

	schema := models.NewProjectionSchema([]string{"name"})
	doc := &models.Document{
		DocumentID: "bench-doc",
		Values:     []models.FieldValue{models.NewStringValue("bench-doc"), models.NewStringValue("Test")},
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		pageID := uint32(i % 1000)
		tracker.MarkDirty("bench-bundle", pageID, doc, 100, schema)
		if i%10 == 0 { // Flush every 10 docs
			tracker.EnqueueForFlush("bench-bundle", pageID)
		}
	}

	pool.Stop()
}
