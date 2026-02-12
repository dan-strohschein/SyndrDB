package planner

import (
	"context"
	"fmt"
	"math"
	"sync"
	"testing"

	"syndrdb/src/internal/domain/models"

	"go.uber.org/zap"
)

// ---- HyperLogLog Tests ----

func TestHyperLogLog_EmptyCount(t *testing.T) {
	hll := NewHyperLogLog()
	count := hll.Count()
	if count != 0 {
		t.Errorf("empty HLL count = %f, want 0", count)
	}
}

func TestHyperLogLog_SingleValue(t *testing.T) {
	hll := NewHyperLogLog()
	hll.AddString("hello")
	count := hll.Count()
	if count < 0.5 || count > 1.5 {
		t.Errorf("single value HLL count = %f, want ~1", count)
	}
}

func TestHyperLogLog_DuplicateValues(t *testing.T) {
	hll := NewHyperLogLog()
	for i := 0; i < 10000; i++ {
		hll.AddString("same-value")
	}
	count := hll.Count()
	if count < 0.5 || count > 2.0 {
		t.Errorf("duplicate values HLL count = %f, want ~1", count)
	}
}

func TestHyperLogLog_DistinctValues(t *testing.T) {
	hll := NewHyperLogLog()
	n := 10000
	for i := 0; i < n; i++ {
		hll.AddString(fmt.Sprintf("value-%d", i))
	}
	count := hll.Count()
	// HLL with 12-bit precision has ~1.6% standard error
	// Allow 5% tolerance for test reliability
	low := float64(n) * 0.95
	high := float64(n) * 1.05
	if count < low || count > high {
		t.Errorf("HLL count = %f, want between %f and %f (actual distinct: %d)", count, low, high, n)
	}
}

func TestHyperLogLog_LargeCardinality(t *testing.T) {
	hll := NewHyperLogLog()
	n := 100000
	for i := 0; i < n; i++ {
		hll.AddString(fmt.Sprintf("item-%d", i))
	}
	count := hll.Count()
	// Allow 5% error
	low := float64(n) * 0.95
	high := float64(n) * 1.05
	if count < low || count > high {
		t.Errorf("HLL count = %f for %d distinct values, want within 5%%", count, n)
	}
}

func TestHyperLogLog_Merge(t *testing.T) {
	hll1 := NewHyperLogLog()
	hll2 := NewHyperLogLog()

	// Add 5000 values to each, with 2500 overlap
	for i := 0; i < 5000; i++ {
		hll1.AddString(fmt.Sprintf("value-%d", i))
	}
	for i := 2500; i < 7500; i++ {
		hll2.AddString(fmt.Sprintf("value-%d", i))
	}

	hll1.Merge(hll2)
	count := hll1.Count()
	// Merged should have ~7500 distinct values
	expected := 7500.0
	low := expected * 0.90
	high := expected * 1.10
	if count < low || count > high {
		t.Errorf("merged HLL count = %f, want ~%f", count, expected)
	}
}

func TestHyperLogLog_MergeNil(t *testing.T) {
	hll := NewHyperLogLog()
	hll.AddString("test")
	hll.Merge(nil) // should not panic
	count := hll.Count()
	if count < 0.5 || count > 1.5 {
		t.Errorf("after nil merge, count = %f, want ~1", count)
	}
}

// ---- ColumnStats Tests ----

func TestColumnStats_NullFrac(t *testing.T) {
	tests := []struct {
		name      string
		rowCount  int64
		nullCount int64
		want      float64
	}{
		{"no rows", 0, 0, 0.0},
		{"no nulls", 100, 0, 0.0},
		{"half nulls", 100, 50, 0.5},
		{"all nulls", 100, 100, 1.0},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			cs := &ColumnStats{RowCount: tc.rowCount, NullCount: tc.nullCount}
			got := cs.NullFrac()
			if math.Abs(got-tc.want) > 0.001 {
				t.Errorf("NullFrac() = %f, want %f", got, tc.want)
			}
		})
	}
}

func TestColumnStats_NDV(t *testing.T) {
	// With HLL
	cs := &ColumnStats{
		HLL:           NewHyperLogLog(),
		DistinctCount: 0,
	}
	for i := 0; i < 100; i++ {
		cs.HLL.AddString(fmt.Sprintf("v%d", i))
	}
	ndv := cs.NDV()
	if ndv < 90 || ndv > 110 {
		t.Errorf("NDV with HLL = %d, want ~100", ndv)
	}

	// Without HLL, fallback to DistinctCount
	cs2 := &ColumnStats{DistinctCount: 42}
	if cs2.NDV() != 42 {
		t.Errorf("NDV without HLL = %d, want 42", cs2.NDV())
	}
}

func TestColumnStats_UpdateMinMax(t *testing.T) {
	cs := &ColumnStats{}

	// First value sets both min and max
	cs.updateMinMax(10.0)
	if cs.MinValue != 10.0 || cs.MaxValue != 10.0 {
		t.Errorf("after first value: min=%v, max=%v, want 10, 10", cs.MinValue, cs.MaxValue)
	}

	// Lower value updates min
	cs.updateMinMax(5.0)
	if cs.MinValue != 5.0 || cs.MaxValue != 10.0 {
		t.Errorf("after lower value: min=%v, max=%v, want 5, 10", cs.MinValue, cs.MaxValue)
	}

	// Higher value updates max
	cs.updateMinMax(20.0)
	if cs.MinValue != 5.0 || cs.MaxValue != 20.0 {
		t.Errorf("after higher value: min=%v, max=%v, want 5, 20", cs.MinValue, cs.MaxValue)
	}

	// Nil value ignored
	cs.updateMinMax(nil)
	if cs.MinValue != 5.0 || cs.MaxValue != 20.0 {
		t.Errorf("after nil: min=%v, max=%v, want 5, 20", cs.MinValue, cs.MaxValue)
	}

	// Non-numeric value ignored
	cs.updateMinMax("hello")
	if cs.MinValue != 5.0 || cs.MaxValue != 20.0 {
		t.Errorf("after string: min=%v, max=%v, want 5, 20", cs.MinValue, cs.MaxValue)
	}
}

func TestToFloat64ForStats(t *testing.T) {
	tests := []struct {
		input    interface{}
		expected float64
		ok       bool
	}{
		{int(42), 42.0, true},
		{int32(42), 42.0, true},
		{int64(42), 42.0, true},
		{float32(3.14), float64(float32(3.14)), true},
		{float64(3.14), 3.14, true},
		{"not a number", 0, false},
		{true, 0, false},
		{nil, 0, false},
	}

	for _, tc := range tests {
		got, ok := toFloat64ForStats(tc.input)
		if ok != tc.ok {
			t.Errorf("toFloat64ForStats(%v): ok=%v, want %v", tc.input, ok, tc.ok)
		}
		if ok && math.Abs(got-tc.expected) > 0.001 {
			t.Errorf("toFloat64ForStats(%v) = %f, want %f", tc.input, got, tc.expected)
		}
	}
}

// ---- EquiDepthHistogram Tests ----

func TestEquiDepthHistogram_FractionInRange(t *testing.T) {
	h := &EquiDepthHistogram{
		Boundaries:    []float64{10, 20, 30, 40, 50},
		NumBuckets:    5,
		RowsPerBucket: 100,
	}

	// Full range covers all buckets (0 maps to bucket 0, 100 maps to last bucket)
	frac := h.FractionInRange(0, 100)
	if frac < 0.5 {
		t.Errorf("full range fraction = %f, want >= 0.5", frac)
	}

	// Narrow range within one bucket
	narrowFrac := h.FractionInRange(10, 20)
	if narrowFrac < 0 || narrowFrac > 1 {
		t.Errorf("narrow range fraction = %f, want between 0 and 1", narrowFrac)
	}

	// Nil histogram
	var nilH *EquiDepthHistogram
	frac = nilH.FractionInRange(0, 100)
	if frac != 0.5 {
		t.Errorf("nil histogram fraction = %f, want 0.5", frac)
	}
}

// ---- StatsStore Tests ----

func TestStatsStore_GetSetColumnStats(t *testing.T) {
	logger, _ := zap.NewDevelopment()
	ss := NewStatsStore(logger.Sugar())

	// Get non-existent stats
	cs := ss.GetColumnStats("users", "age")
	if cs != nil {
		t.Error("expected nil for non-existent stats")
	}

	// Set stats
	ss.UpdateColumnStats(&ColumnStats{
		BundleName:    "users",
		FieldName:     "age",
		RowCount:      1000,
		DistinctCount: 80,
	})

	// Get existing stats
	cs = ss.GetColumnStats("users", "age")
	if cs == nil {
		t.Fatal("expected non-nil stats after update")
	}
	if cs.RowCount != 1000 {
		t.Errorf("RowCount = %d, want 1000", cs.RowCount)
	}
	if cs.DistinctCount != 80 {
		t.Errorf("DistinctCount = %d, want 80", cs.DistinctCount)
	}
	if cs.StatsVersion == 0 {
		t.Error("StatsVersion should be > 0 after update")
	}
}

func TestStatsStore_GetBundleStats(t *testing.T) {
	logger, _ := zap.NewDevelopment()
	ss := NewStatsStore(logger.Sugar())

	// Non-existent bundle
	bundleStats := ss.GetBundleStats("users")
	if bundleStats != nil {
		t.Error("expected nil for non-existent bundle")
	}

	// Add two fields
	ss.UpdateColumnStats(&ColumnStats{BundleName: "users", FieldName: "age"})
	ss.UpdateColumnStats(&ColumnStats{BundleName: "users", FieldName: "name"})

	bundleStats = ss.GetBundleStats("users")
	if len(bundleStats) != 2 {
		t.Errorf("expected 2 fields, got %d", len(bundleStats))
	}
}

func TestStatsStore_RemoveBundle(t *testing.T) {
	logger, _ := zap.NewDevelopment()
	ss := NewStatsStore(logger.Sugar())

	ss.UpdateColumnStats(&ColumnStats{BundleName: "users", FieldName: "age"})
	ss.RemoveBundle("users")

	cs := ss.GetColumnStats("users", "age")
	if cs != nil {
		t.Error("expected nil after RemoveBundle")
	}
}

func TestStatsStore_IncrementalUpdate(t *testing.T) {
	logger, _ := zap.NewDevelopment()
	ss := NewStatsStore(logger.Sugar())

	// INSERT: oldValue=nil, newValue=42
	ss.IncrementalUpdate("users", "age", nil, 42, 100)

	cs := ss.GetColumnStats("users", "age")
	if cs == nil {
		t.Fatal("expected stats after incremental update")
	}
	if cs.RowCount != 100 {
		t.Errorf("RowCount = %d, want 100", cs.RowCount)
	}
	if cs.DistinctCount < 1 {
		t.Error("DistinctCount should be >= 1 after inserting a value")
	}

	// DELETE: oldValue=42, newValue=nil (becomes null)
	ss.IncrementalUpdate("users", "age", 42, nil, 99)
	cs = ss.GetColumnStats("users", "age")
	if cs.NullCount != 1 {
		t.Errorf("NullCount = %d, want 1 after delete", cs.NullCount)
	}
	if cs.RowCount != 99 {
		t.Errorf("RowCount = %d, want 99", cs.RowCount)
	}
}

func TestStatsStore_IncrementalUpdate_NullTracking(t *testing.T) {
	logger, _ := zap.NewDevelopment()
	ss := NewStatsStore(logger.Sugar())

	// INSERT with null value
	ss.IncrementalUpdate("users", "email", nil, nil, 1)
	cs := ss.GetColumnStats("users", "email")
	// oldValue=nil, newValue=nil -> no null change (both are null)
	if cs.NullCount != 0 {
		t.Errorf("NullCount = %d, want 0 (nil->nil has no change)", cs.NullCount)
	}

	// INSERT with non-null from null
	ss.IncrementalUpdate("users", "email", nil, "test@example.com", 2)
	cs = ss.GetColumnStats("users", "email")
	// oldValue=nil, newValue=non-nil -> null count decreases (but was 0, so stays 0)
	if cs.NullCount != 0 {
		t.Errorf("NullCount = %d, want 0", cs.NullCount)
	}

	// UPDATE from non-null to null
	ss.IncrementalUpdate("users", "email", "test@example.com", nil, 2)
	cs = ss.GetColumnStats("users", "email")
	if cs.NullCount != 1 {
		t.Errorf("NullCount = %d, want 1", cs.NullCount)
	}
}

func TestStatsStore_GlobalVersion(t *testing.T) {
	logger, _ := zap.NewDevelopment()
	ss := NewStatsStore(logger.Sugar())

	v0 := ss.GetStatsVersion()
	if v0 != 0 {
		t.Errorf("initial version = %d, want 0", v0)
	}

	ss.UpdateColumnStats(&ColumnStats{BundleName: "b1", FieldName: "f1"})
	v1 := ss.GetStatsVersion()
	if v1 <= v0 {
		t.Errorf("version after update = %d, should be > %d", v1, v0)
	}

	ss.IncrementalUpdate("b1", "f2", nil, "val", 10)
	v2 := ss.GetStatsVersion()
	if v2 <= v1 {
		t.Errorf("version after incremental = %d, should be > %d", v2, v1)
	}
}

func TestStatsStore_ConcurrentAccess(t *testing.T) {
	logger, _ := zap.NewDevelopment()
	ss := NewStatsStore(logger.Sugar())

	var wg sync.WaitGroup
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func(n int) {
			defer wg.Done()
			ss.IncrementalUpdate("users", fmt.Sprintf("field_%d", n%10), nil, n, int64(n))
			ss.GetColumnStats("users", fmt.Sprintf("field_%d", n%10))
			ss.GetBundleStats("users")
		}(i)
	}
	wg.Wait()

	// Should not panic or race
	bundleStats := ss.GetBundleStats("users")
	if bundleStats == nil {
		t.Fatal("expected non-nil stats after concurrent updates")
	}
}

func TestStatsStore_GetBundleStatsVersion(t *testing.T) {
	logger, _ := zap.NewDevelopment()
	ss := NewStatsStore(logger.Sugar())

	// Non-existent
	v := ss.GetBundleStatsVersion("users")
	if v != 0 {
		t.Errorf("non-existent bundle version = %d, want 0", v)
	}

	ss.UpdateColumnStats(&ColumnStats{BundleName: "users", FieldName: "f1"})
	ss.UpdateColumnStats(&ColumnStats{BundleName: "users", FieldName: "f2"})

	v = ss.GetBundleStatsVersion("users")
	if v == 0 {
		t.Error("bundle stats version should be > 0 after updates")
	}
}

// ---- AnalyzeBundle Tests ----

// mockBundleServiceForAnalyze is a minimal mock for testing AnalyzeBundle
type mockBundleServiceForAnalyze struct {
	BundleServiceInterface
	docs []*models.Document
}

func (m *mockBundleServiceForAnalyze) GetDocumentChunksForIndexing(
	ctx context.Context, bundleName string, chunkSize int,
	fn func(chunk []*models.Document) (stop bool),
) error {
	// Deliver docs in chunks
	for i := 0; i < len(m.docs); i += chunkSize {
		end := i + chunkSize
		if end > len(m.docs) {
			end = len(m.docs)
		}
		if fn(m.docs[i:end]) {
			break
		}
	}
	return nil
}

func makeTestDocument(id string, fields map[string]interface{}) *models.Document {
	doc := &models.Document{
		DocumentID: id,
		Fields:     make(map[string]models.Field),
	}
	for k, v := range fields {
		doc.Fields[k] = models.Field{Value: models.NewInterfaceValue(v)}
	}
	return doc
}

func TestAnalyzeBundle_Basic(t *testing.T) {
	logger, _ := zap.NewDevelopment()
	ss := NewStatsStore(logger.Sugar())

	docs := make([]*models.Document, 100)
	for i := 0; i < 100; i++ {
		docs[i] = makeTestDocument(fmt.Sprintf("doc-%d", i), map[string]interface{}{
			"age":  float64(i % 50),
			"name": fmt.Sprintf("user-%d", i),
		})
	}

	mock := &mockBundleServiceForAnalyze{docs: docs}

	err := AnalyzeBundle(context.Background(), "users", mock, ss, logger.Sugar(), 100)
	if err != nil {
		t.Fatalf("AnalyzeBundle failed: %v", err)
	}

	// Check age stats
	ageStat := ss.GetColumnStats("users", "age")
	if ageStat == nil {
		t.Fatal("expected age stats")
	}
	if ageStat.RowCount != 100 {
		t.Errorf("age RowCount = %d, want 100", ageStat.RowCount)
	}
	if ageStat.DistinctCount < 40 || ageStat.DistinctCount > 60 {
		t.Errorf("age DistinctCount = %d, want ~50", ageStat.DistinctCount)
	}
	if ageStat.IsIncremental {
		t.Error("ANALYZE stats should not be marked incremental")
	}

	// Check name stats
	nameStat := ss.GetColumnStats("users", "name")
	if nameStat == nil {
		t.Fatal("expected name stats")
	}
	if nameStat.DistinctCount < 90 || nameStat.DistinctCount > 110 {
		t.Errorf("name DistinctCount = %d, want ~100", nameStat.DistinctCount)
	}
}

func TestAnalyzeBundle_EmptyBundle(t *testing.T) {
	logger, _ := zap.NewDevelopment()
	ss := NewStatsStore(logger.Sugar())

	mock := &mockBundleServiceForAnalyze{docs: nil}

	err := AnalyzeBundle(context.Background(), "empty", mock, ss, logger.Sugar())
	if err != nil {
		t.Fatalf("AnalyzeBundle failed on empty bundle: %v", err)
	}

	// No stats should be created for an empty bundle
	bundleStats := ss.GetBundleStats("empty")
	if bundleStats != nil {
		t.Errorf("expected nil stats for empty bundle, got %d fields", len(bundleStats))
	}
}

func TestAnalyzeBundle_WithNulls(t *testing.T) {
	logger, _ := zap.NewDevelopment()
	ss := NewStatsStore(logger.Sugar())

	docs := make([]*models.Document, 100)
	for i := 0; i < 100; i++ {
		fields := map[string]interface{}{
			"name": fmt.Sprintf("user-%d", i),
		}
		// Half have email, half don't (nil)
		if i%2 == 0 {
			fields["email"] = fmt.Sprintf("user%d@example.com", i)
		}
		docs[i] = makeTestDocument(fmt.Sprintf("doc-%d", i), fields)
	}

	mock := &mockBundleServiceForAnalyze{docs: docs}

	err := AnalyzeBundle(context.Background(), "users", mock, ss, logger.Sugar(), 100)
	if err != nil {
		t.Fatalf("AnalyzeBundle failed: %v", err)
	}

	// email field: 50 docs have it, 50 don't (missing field = not counted)
	emailStat := ss.GetColumnStats("users", "email")
	if emailStat == nil {
		t.Fatal("expected email stats")
	}
	// The 50 docs without email field won't contribute to email stats at all
	// since the field simply doesn't exist in their Fields map
	if emailStat.NullCount != 0 {
		t.Errorf("email NullCount = %d, want 0 (missing fields are not counted)", emailStat.NullCount)
	}
}

func TestAnalyzeBundle_Cancellation(t *testing.T) {
	logger, _ := zap.NewDevelopment()
	ss := NewStatsStore(logger.Sugar())

	docs := make([]*models.Document, 10000)
	for i := 0; i < 10000; i++ {
		docs[i] = makeTestDocument(fmt.Sprintf("doc-%d", i), map[string]interface{}{
			"value": float64(i),
		})
	}

	mock := &mockBundleServiceForAnalyze{docs: docs}

	ctx, cancel := context.WithCancel(context.Background())
	cancel() // cancel immediately

	err := AnalyzeBundle(ctx, "users", mock, ss, logger.Sugar(), 100)
	// Should not error - cancellation just stops early
	if err != nil {
		t.Fatalf("AnalyzeBundle with cancelled context: %v", err)
	}
}
