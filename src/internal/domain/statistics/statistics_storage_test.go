package statistics

import (
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"go.uber.org/zap"
)

func testStorageLogger() *zap.SugaredLogger {
	return zap.NewNop().Sugar()
}

func newTestStorage(t *testing.T) *StatisticsStorage {
	t.Helper()
	ss, err := NewStatisticsStorage(StorageConfig{
		BaseDir: t.TempDir(),
	}, testStorageLogger())
	if err != nil {
		t.Fatalf("failed to create storage: %v", err)
	}
	t.Cleanup(func() { ss.Close() })
	return ss
}

func makeSampleStats(bundleName string) *BundleStatistics {
	return &BundleStatistics{
		BundleName:     bundleName,
		TotalDocuments: 1000,
		LastAnalyzed:   time.Now(),
		StatsVersion:   STATS_VERSION,
		FieldStats: map[string]*FieldStatistics{
			"name": {
				FieldName:     "name",
				DistinctCount: 50,
				NullCount:     10,
				MostCommonValues: []ValueFrequency{
					{Value: "alice", Frequency: 0.1},
				},
			},
		},
	}
}

// --- NewStatisticsStorage ---

func TestNewStatisticsStorage(t *testing.T) {
	ss, err := NewStatisticsStorage(StorageConfig{
		BaseDir: t.TempDir(),
	}, testStorageLogger())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	defer ss.Close()
	if ss == nil {
		t.Fatal("expected non-nil storage")
	}
	if ss.zstdEncoder == nil {
		t.Error("expected non-nil zstd encoder")
	}
	if ss.zstdDecoder == nil {
		t.Error("expected non-nil zstd decoder")
	}
}

func TestNewStatisticsStorage_InvalidDir(t *testing.T) {
	_, err := NewStatisticsStorage(StorageConfig{
		BaseDir: "/dev/null/impossible/path",
	}, testStorageLogger())
	if err == nil {
		t.Fatal("expected error for invalid directory")
	}
}

func TestNewStatisticsStorage_DefaultThreshold(t *testing.T) {
	ss, err := NewStatisticsStorage(StorageConfig{
		BaseDir:                t.TempDir(),
		CompressionThresholdMB: 0, // should default to 1
	}, testStorageLogger())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	defer ss.Close()
	if ss.compressionThresholdMB != 1 {
		t.Errorf("expected default threshold 1, got %d", ss.compressionThresholdMB)
	}
}

// --- SaveStatistics / LoadStatistics ---

func TestSaveAndLoadStatistics_Uncompressed(t *testing.T) {
	ss := newTestStorage(t)
	stats := makeSampleStats("test_bundle")

	if err := ss.SaveStatistics(stats); err != nil {
		t.Fatalf("save failed: %v", err)
	}

	loaded, err := ss.LoadStatistics("test_bundle")
	if err != nil {
		t.Fatalf("load failed: %v", err)
	}

	if loaded.BundleName != "test_bundle" {
		t.Errorf("expected bundle name 'test_bundle', got '%s'", loaded.BundleName)
	}
	if loaded.TotalDocuments != 1000 {
		t.Errorf("expected 1000 total documents, got %d", loaded.TotalDocuments)
	}
	if loaded.StatsVersion != STATS_VERSION {
		t.Errorf("expected stats version %d, got %d", STATS_VERSION, loaded.StatsVersion)
	}
	if len(loaded.FieldStats) != 1 {
		t.Errorf("expected 1 field stat, got %d", len(loaded.FieldStats))
	}
	if fs, ok := loaded.FieldStats["name"]; ok {
		if fs.DistinctCount != 50 {
			t.Errorf("expected 50 distinct count, got %d", fs.DistinctCount)
		}
	} else {
		t.Error("missing 'name' field stats")
	}
}

func TestSaveStatistics_NilStats(t *testing.T) {
	ss := newTestStorage(t)
	err := ss.SaveStatistics(nil)
	if err == nil {
		t.Fatal("expected error for nil stats")
	}
}

func TestLoadStatistics_NotFound(t *testing.T) {
	ss := newTestStorage(t)
	_, err := ss.LoadStatistics("nonexistent_bundle")
	if err == nil {
		t.Fatal("expected error for missing bundle")
	}
}

func TestLoadStatistics_VersionMismatch(t *testing.T) {
	ss := newTestStorage(t)
	stats := makeSampleStats("test_bundle")
	stats.StatsVersion = 999 // wrong version

	// Manually save with wrong version (bypass SaveStatistics which would set the right version)
	// We need to save it, then the LoadStatistics should reject it
	if err := ss.SaveStatistics(stats); err != nil {
		t.Fatalf("save failed: %v", err)
	}

	_, err := ss.LoadStatistics("test_bundle")
	if err == nil {
		t.Fatal("expected error for version mismatch")
	}
}

// --- Compression tiers ---

func TestSaveAndLoadStatistics_Gzip(t *testing.T) {
	// Use threshold of 0 so everything gets gzip compressed
	tmpDir := t.TempDir()
	ss, err := NewStatisticsStorage(StorageConfig{
		BaseDir:                tmpDir,
		CompressionThresholdMB: 0, // will default to 1MB
	}, testStorageLogger())
	if err != nil {
		t.Fatalf("failed to create storage: %v", err)
	}
	defer ss.Close()

	// Manually set threshold bytes to 0 so any data triggers gzip
	ss.compressionThresholdMB = 0 // 0 * 1MB = 0 bytes threshold → GZIP for < 5MB

	stats := makeSampleStats("gzip_bundle")
	if err := ss.SaveStatistics(stats); err != nil {
		t.Fatalf("save failed: %v", err)
	}

	loaded, err := ss.LoadStatistics("gzip_bundle")
	if err != nil {
		t.Fatalf("load failed: %v", err)
	}
	if loaded.BundleName != "gzip_bundle" {
		t.Errorf("expected 'gzip_bundle', got '%s'", loaded.BundleName)
	}
}

func TestSaveAndLoadStatistics_Zstd(t *testing.T) {
	// Create storage where even small data triggers zstd (by making thresholds tiny)
	tmpDir := t.TempDir()
	ss, err := NewStatisticsStorage(StorageConfig{
		BaseDir: tmpDir,
	}, testStorageLogger())
	if err != nil {
		t.Fatalf("failed to create storage: %v", err)
	}
	defer ss.Close()

	// Generate large stats to trigger zstd (>= 5MB)
	stats := &BundleStatistics{
		BundleName:     "zstd_bundle",
		TotalDocuments: 1000000,
		LastAnalyzed:   time.Now(),
		StatsVersion:   STATS_VERSION,
		FieldStats:     make(map[string]*FieldStatistics),
	}
	// Add many fields to push over 5MB
	for i := 0; i < 5000; i++ {
		fieldName := fmt.Sprintf("field_%04d", i)
		mcvs := make([]ValueFrequency, 10)
		for j := 0; j < 10; j++ {
			mcvs[j] = ValueFrequency{Value: fmt.Sprintf("value_%s_mcv_%d", fieldName, j), Frequency: 0.1}
		}
		histBuckets := make([]HistogramBucket, 20)
		for j := 0; j < 20; j++ {
			histBuckets[j] = HistogramBucket{
				LowerBound:    fmt.Sprintf("lower_%s_%d", fieldName, j),
				UpperBound:    fmt.Sprintf("upper_%s_%d", fieldName, j),
				DocumentCount: int64(j * 100),
				DistinctCount: int64(j * 10),
			}
		}
		stats.FieldStats[fieldName] = &FieldStatistics{
			FieldName:        fieldName,
			DistinctCount:    int64(i * 10),
			NullCount:        int64(i),
			AvgDocumentSize:  128,
			MostCommonValues: mcvs,
			Histogram:        histBuckets,
		}
	}

	if err := ss.SaveStatistics(stats); err != nil {
		t.Fatalf("save failed: %v", err)
	}

	loaded, err := ss.LoadStatistics("zstd_bundle")
	if err != nil {
		t.Fatalf("load failed: %v", err)
	}
	if loaded.BundleName != "zstd_bundle" {
		t.Errorf("expected 'zstd_bundle', got '%s'", loaded.BundleName)
	}
	if len(loaded.FieldStats) != 5000 {
		t.Errorf("expected 5000 field stats, got %d", len(loaded.FieldStats))
	}
}

// --- DeleteStatistics ---

func TestDeleteStatistics(t *testing.T) {
	ss := newTestStorage(t)
	stats := makeSampleStats("delete_me")

	if err := ss.SaveStatistics(stats); err != nil {
		t.Fatalf("save failed: %v", err)
	}
	if !ss.ExistsStatistics("delete_me") {
		t.Fatal("expected stats to exist after save")
	}

	if err := ss.DeleteStatistics("delete_me"); err != nil {
		t.Fatalf("delete failed: %v", err)
	}
	if ss.ExistsStatistics("delete_me") {
		t.Error("expected stats to not exist after delete")
	}
}

func TestDeleteStatistics_NotFound(t *testing.T) {
	ss := newTestStorage(t)
	err := ss.DeleteStatistics("nonexistent")
	if err == nil {
		t.Fatal("expected error deleting non-existent stats")
	}
}

// --- ExistsStatistics ---

func TestExistsStatistics(t *testing.T) {
	ss := newTestStorage(t)

	if ss.ExistsStatistics("test_bundle") {
		t.Error("expected false before save")
	}

	stats := makeSampleStats("test_bundle")
	if err := ss.SaveStatistics(stats); err != nil {
		t.Fatalf("save failed: %v", err)
	}

	if !ss.ExistsStatistics("test_bundle") {
		t.Error("expected true after save")
	}
}

// --- ListBundlesWithStatistics ---

func TestListBundlesWithStatistics(t *testing.T) {
	ss := newTestStorage(t)

	for _, name := range []string{"bundle_a", "bundle_b", "bundle_c"} {
		stats := makeSampleStats(name)
		if err := ss.SaveStatistics(stats); err != nil {
			t.Fatalf("save %s failed: %v", name, err)
		}
	}

	bundles, err := ss.ListBundlesWithStatistics()
	if err != nil {
		t.Fatalf("list failed: %v", err)
	}

	if len(bundles) != 3 {
		t.Errorf("expected 3 bundles, got %d", len(bundles))
	}

	bundleSet := make(map[string]bool)
	for _, b := range bundles {
		bundleSet[b] = true
	}
	for _, expected := range []string{"bundle_a", "bundle_b", "bundle_c"} {
		if !bundleSet[expected] {
			t.Errorf("missing expected bundle: %s", expected)
		}
	}
}

func TestListBundlesWithStatistics_EmptyDir(t *testing.T) {
	ss := newTestStorage(t)
	bundles, err := ss.ListBundlesWithStatistics()
	if err != nil {
		t.Fatalf("list failed: %v", err)
	}
	if len(bundles) != 0 {
		t.Errorf("expected 0 bundles, got %d", len(bundles))
	}
}

// --- determineCompressionStrategy ---

func TestDetermineCompressionStrategy(t *testing.T) {
	ss := newTestStorage(t)

	// Below threshold (1MB default) → NONE
	s := ss.determineCompressionStrategy(500 * 1024)
	if s != COMPRESSION_NONE {
		t.Errorf("expected NONE for 500KB, got %d", s)
	}

	// Between 1MB and 5MB → GZIP
	s = ss.determineCompressionStrategy(2 * 1024 * 1024)
	if s != COMPRESSION_GZIP {
		t.Errorf("expected GZIP for 2MB, got %d", s)
	}

	// >= 5MB → ZSTD
	s = ss.determineCompressionStrategy(6 * 1024 * 1024)
	if s != COMPRESSION_ZSTD {
		t.Errorf("expected ZSTD for 6MB, got %d", s)
	}

	// Exact boundary: 5MB → ZSTD
	s = ss.determineCompressionStrategy(ZSTD_THRESHOLD)
	if s != COMPRESSION_ZSTD {
		t.Errorf("expected ZSTD at exact 5MB boundary, got %d", s)
	}

	// Exact boundary: 1MB → GZIP (not less than)
	s = ss.determineCompressionStrategy(COMPRESSION_THRESHOLD)
	if s != COMPRESSION_GZIP {
		t.Errorf("expected GZIP at exact 1MB boundary, got %d", s)
	}
}

// --- getStatisticsPath ---

func TestGetStatisticsPath(t *testing.T) {
	ss := newTestStorage(t)

	pathNone := ss.getStatisticsPath("test", COMPRESSION_NONE)
	if filepath.Ext(pathNone) != ".json" {
		t.Errorf("expected .json extension for NONE, got %s", filepath.Ext(pathNone))
	}

	pathGzip := ss.getStatisticsPath("test", COMPRESSION_GZIP)
	if filepath.Ext(pathGzip) != ".gz" {
		t.Errorf("expected .gz extension for GZIP, got %s", filepath.Ext(pathGzip))
	}

	pathZstd := ss.getStatisticsPath("test", COMPRESSION_ZSTD)
	if filepath.Ext(pathZstd) != ".zst" {
		t.Errorf("expected .zst extension for ZSTD, got %s", filepath.Ext(pathZstd))
	}
}

// --- Close ---

func TestClose(t *testing.T) {
	ss := newTestStorage(t)
	if err := ss.Close(); err != nil {
		t.Fatalf("close failed: %v", err)
	}
	// Double close should be safe
	if err := ss.Close(); err != nil {
		t.Fatalf("double close failed: %v", err)
	}
}

// --- Atomic write ---

func TestSaveStatistics_AtomicWrite(t *testing.T) {
	ss := newTestStorage(t)
	stats := makeSampleStats("atomic_test")

	if err := ss.SaveStatistics(stats); err != nil {
		t.Fatalf("save failed: %v", err)
	}

	// Verify the temp file does not exist
	tempPath := ss.getStatisticsPath("atomic_test", COMPRESSION_NONE) + TEMP_FILE_SUFFIX
	if fileExists(tempPath) {
		t.Error("temp file should be cleaned up after successful save")
	}

	// Verify the final file exists
	finalPath := ss.getStatisticsPath("atomic_test", COMPRESSION_NONE)
	if !fileExists(finalPath) {
		t.Error("final file should exist after save")
	}
}

func fileExists(path string) bool {
	_, err := os.Stat(path)
	return err == nil
}

// --- Concurrent access ---

func TestConcurrentSaveLoad(t *testing.T) {
	ss := newTestStorage(t)
	const goroutines = 10

	// First save initial stats
	stats := makeSampleStats("concurrent_bundle")
	if err := ss.SaveStatistics(stats); err != nil {
		t.Fatalf("initial save failed: %v", err)
	}

	var wg sync.WaitGroup
	errCh := make(chan error, goroutines*2)

	for i := 0; i < goroutines; i++ {
		wg.Add(2)

		// Writer
		go func(id int) {
			defer wg.Done()
			s := makeSampleStats("concurrent_bundle")
			s.TotalDocuments = int64(id * 100)
			if err := ss.SaveStatistics(s); err != nil {
				errCh <- err
			}
		}(i)

		// Reader
		go func() {
			defer wg.Done()
			if _, err := ss.LoadStatistics("concurrent_bundle"); err != nil {
				errCh <- err
			}
		}()
	}

	wg.Wait()
	close(errCh)

	for err := range errCh {
		t.Errorf("concurrent error: %v", err)
	}
}
