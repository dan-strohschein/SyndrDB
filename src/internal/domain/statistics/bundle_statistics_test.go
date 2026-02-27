package statistics

import (
	"math"
	"syndrdb/src/internal/domain/models"
	"testing"
	"time"

	"go.uber.org/zap"
)

func testLogger() *zap.SugaredLogger {
	return zap.NewNop().Sugar()
}

// helper: create a schema + document with given field values
func makeDocWithFields(schema *models.BundleFieldSchema, vals map[string]models.FieldValue) *models.Document {
	doc := &models.Document{
		DocumentID: "doc-1",
		Values:     make([]models.FieldValue, len(schema.Names)),
		CreatedAt:  time.Now(),
		UpdatedAt:  time.Now(),
	}
	for name, fv := range vals {
		if idx, ok := schema.NameToIndex[name]; ok {
			doc.Values[idx] = fv
		}
	}
	return doc
}

func makeSchema(fieldNames ...string) *models.BundleFieldSchema {
	return models.BuildBundleFieldSchemaFromNames(fieldNames)
}

// --- StatisticsCollector constructor ---

func TestNewStatisticsCollector(t *testing.T) {
	sc := NewStatisticsCollector(testLogger())
	if sc == nil {
		t.Fatal("expected non-nil StatisticsCollector")
	}
}

// --- determineSampleStrategy ---

func TestDetermineSampleStrategy_Small(t *testing.T) {
	sc := NewStatisticsCollector(testLogger())
	rate := sc.determineSampleStrategy(500)
	if rate != 1.0 {
		t.Errorf("expected 1.0 for small bundle, got %f", rate)
	}
}

func TestDetermineSampleStrategy_Medium(t *testing.T) {
	sc := NewStatisticsCollector(testLogger())
	rate := sc.determineSampleStrategy(50000)
	if rate != 0.10 {
		t.Errorf("expected 0.10 for medium bundle, got %f", rate)
	}
}

func TestDetermineSampleStrategy_Large(t *testing.T) {
	sc := NewStatisticsCollector(testLogger())
	rate := sc.determineSampleStrategy(500000)
	expected := 1000.0 / 500000.0
	if math.Abs(rate-expected) > 1e-9 {
		t.Errorf("expected %f for large bundle, got %f", expected, rate)
	}
}

func TestDetermineSampleStrategy_Boundary(t *testing.T) {
	sc := NewStatisticsCollector(testLogger())

	// Exactly 1000: should be full scan (< 1000 is full scan, 1000 is medium tier)
	rate1000 := sc.determineSampleStrategy(1000)
	// 1000 is NOT < 1000 so it falls into medium tier
	if rate1000 != 0.10 {
		t.Errorf("expected 0.10 for exactly 1000 docs, got %f", rate1000)
	}

	// Exactly 999: should be full scan
	rate999 := sc.determineSampleStrategy(999)
	if rate999 != 1.0 {
		t.Errorf("expected 1.0 for 999 docs, got %f", rate999)
	}

	// Exactly 100000: should be medium tier (<=100000)
	rate100k := sc.determineSampleStrategy(100000)
	if rate100k != 0.10 {
		t.Errorf("expected 0.10 for exactly 100000 docs, got %f", rate100k)
	}

	// Exactly 100001: should be large tier
	rate100k1 := sc.determineSampleStrategy(100001)
	expected := 1000.0 / 100001.0
	if math.Abs(rate100k1-expected) > 1e-9 {
		t.Errorf("expected %f for 100001 docs, got %f", expected, rate100k1)
	}
}

// --- AnalyzeBundle ---

func TestAnalyzeBundle_NilBundle(t *testing.T) {
	sc := NewStatisticsCollector(testLogger())
	_, err := sc.AnalyzeBundle(nil)
	if err == nil {
		t.Fatal("expected error for nil bundle")
	}
}

func TestAnalyzeBundle_EmptyBundle(t *testing.T) {
	sc := NewStatisticsCollector(testLogger())
	bundle := &models.Bundle{
		Name:           "test_bundle",
		TotalDocuments: 0,
		DocumentStructure: models.DocumentStructure{
			FieldDefinitions: map[string]models.FieldDefinition{},
		},
	}
	stats, err := sc.AnalyzeBundle(bundle)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if stats.BundleName != "test_bundle" {
		t.Errorf("expected bundle name 'test_bundle', got '%s'", stats.BundleName)
	}
	if stats.TotalDocuments != 0 {
		t.Errorf("expected 0 total documents, got %d", stats.TotalDocuments)
	}
	if stats.StatsVersion != STATS_VERSION {
		t.Errorf("expected stats version %d, got %d", STATS_VERSION, stats.StatsVersion)
	}
	if stats.FieldStats == nil {
		t.Error("expected non-nil FieldStats map")
	}
	if stats.LastAnalyzed.IsZero() {
		t.Error("expected LastAnalyzed to be set")
	}
}

func TestAnalyzeBundle_SampleDocumentsError(t *testing.T) {
	sc := NewStatisticsCollector(testLogger())
	bundle := &models.Bundle{
		Name:           "test_bundle",
		TotalDocuments: 100,
		DocumentStructure: models.DocumentStructure{
			FieldDefinitions: map[string]models.FieldDefinition{
				"name": {Name: "name", Type: "string"},
			},
		},
	}
	// sampleDocuments is a TODO stub that returns an error
	_, err := sc.AnalyzeBundle(bundle)
	if err == nil {
		t.Fatal("expected error from sampleDocuments stub")
	}
}

// --- analyzeField ---

func TestAnalyzeField_EmptyDocuments(t *testing.T) {
	sc := NewStatisticsCollector(testLogger())
	schema := makeSchema("name")
	stats, err := sc.analyzeField("name", []*models.Document{}, schema, 0)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if stats.FieldName != "name" {
		t.Errorf("expected field name 'name', got '%s'", stats.FieldName)
	}
	if stats.DistinctCount != 0 {
		t.Errorf("expected 0 distinct count, got %d", stats.DistinctCount)
	}
	if stats.NullCount != 0 {
		t.Errorf("expected 0 null count, got %d", stats.NullCount)
	}
}

func TestAnalyzeField_NilSchema(t *testing.T) {
	sc := NewStatisticsCollector(testLogger())
	docs := []*models.Document{
		{DocumentID: "d1", Values: []models.FieldValue{{Type: models.FieldTypeString, StringVal: "hello"}}},
		{DocumentID: "d2", Values: []models.FieldValue{{Type: models.FieldTypeString, StringVal: "world"}}},
	}
	stats, err := sc.analyzeField("name", docs, nil, 2)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	// nil schema → all counted as null
	if stats.NullCount != 2 {
		t.Errorf("expected 2 null count with nil schema, got %d", stats.NullCount)
	}
	if stats.DistinctCount != 0 {
		t.Errorf("expected 0 distinct count with nil schema, got %d", stats.DistinctCount)
	}
}

func TestAnalyzeField_WithValues(t *testing.T) {
	sc := NewStatisticsCollector(testLogger())
	schema := makeSchema("name")
	docs := []*models.Document{
		makeDocWithFields(schema, map[string]models.FieldValue{
			"name": {Type: models.FieldTypeString, StringVal: "alice"},
		}),
		makeDocWithFields(schema, map[string]models.FieldValue{
			"name": {Type: models.FieldTypeString, StringVal: "bob"},
		}),
		makeDocWithFields(schema, map[string]models.FieldValue{
			"name": {Type: models.FieldTypeString, StringVal: "alice"},
		}),
	}

	stats, err := sc.analyzeField("name", docs, schema, 3)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if stats.DistinctCount != 2 {
		t.Errorf("expected 2 distinct values, got %d", stats.DistinctCount)
	}
	if stats.NullCount != 0 {
		t.Errorf("expected 0 null count, got %d", stats.NullCount)
	}
	if stats.AvgDocumentSize <= 0 {
		t.Errorf("expected positive avg document size, got %d", stats.AvgDocumentSize)
	}
}

func TestAnalyzeField_AllNulls(t *testing.T) {
	sc := NewStatisticsCollector(testLogger())
	schema := makeSchema("name")
	// Documents with nil/missing field values
	docs := []*models.Document{
		{DocumentID: "d1", Values: []models.FieldValue{{Type: models.FieldTypeNil}}},
		{DocumentID: "d2", Values: []models.FieldValue{{Type: models.FieldTypeNil}}},
		{DocumentID: "d3", Values: []models.FieldValue{{Type: models.FieldTypeNil}}},
	}
	stats, err := sc.analyzeField("name", docs, schema, 3)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if stats.NullCount != 3 {
		t.Errorf("expected 3 null count, got %d", stats.NullCount)
	}
	if stats.DistinctCount != 0 {
		t.Errorf("expected 0 distinct count, got %d", stats.DistinctCount)
	}
}

// --- findMostCommonValues ---

func TestFindMostCommonValues_Empty(t *testing.T) {
	sc := NewStatisticsCollector(testLogger())
	result := sc.findMostCommonValues(map[interface{}]int64{}, 0, 0)
	if result != nil {
		t.Errorf("expected nil for empty input, got %v", result)
	}
}

func TestFindMostCommonValues_SingleValue(t *testing.T) {
	sc := NewStatisticsCollector(testLogger())
	valueCount := map[interface{}]int64{"hello": 10}
	result := sc.findMostCommonValues(valueCount, 10, 1)
	if len(result) != 1 {
		t.Fatalf("expected 1 MCV, got %d", len(result))
	}
	if result[0].Frequency != 1.0 {
		t.Errorf("expected frequency 1.0, got %f", result[0].Frequency)
	}
}

func TestFindMostCommonValues_TopN(t *testing.T) {
	sc := NewStatisticsCollector(testLogger())
	// Create 15 values with varying frequencies
	valueCount := make(map[interface{}]int64)
	totalSampled := 0
	for i := 0; i < 15; i++ {
		count := int64(100 - i*5)
		valueCount[int64(i)] = count
		totalSampled += int(count)
	}
	result := sc.findMostCommonValues(valueCount, totalSampled, 15)
	if len(result) > DEFAULT_MCV_COUNT {
		t.Errorf("expected at most %d MCVs, got %d", DEFAULT_MCV_COUNT, len(result))
	}
	// Verify sorted by frequency descending
	for i := 1; i < len(result); i++ {
		if result[i].Frequency > result[i-1].Frequency {
			t.Errorf("MCV not sorted by frequency: %f > %f at index %d", result[i].Frequency, result[i-1].Frequency, i)
		}
	}
}

func TestFindMostCommonValues_AdaptiveThreshold(t *testing.T) {
	sc := NewStatisticsCollector(testLogger())
	// With 2 distinct values, threshold = max(0.01, 1/2) = 0.5
	// Both values at exactly 50% should be included
	valueCount := map[interface{}]int64{"a": 50, "b": 50}
	result := sc.findMostCommonValues(valueCount, 100, 2)
	if len(result) != 2 {
		t.Errorf("expected 2 MCVs with adaptive threshold, got %d", len(result))
	}
}

func TestFindMostCommonValues_BelowThreshold(t *testing.T) {
	sc := NewStatisticsCollector(testLogger())
	// With 1000 distinct values, threshold = max(0.01, 1/1000) = 0.01
	// A value appearing 0.5% of the time (5/1000) should be excluded
	valueCount := make(map[interface{}]int64)
	valueCount["common"] = 100 // 10%
	for i := 0; i < 999; i++ {
		valueCount[int64(i)] = 1 // 0.1% each
	}
	total := 100 + 999
	result := sc.findMostCommonValues(valueCount, total, 1000)
	// Only "common" should be included (10% > 1% threshold)
	// The others at 0.1% are below the 1% threshold
	for _, v := range result {
		if v.Frequency < MIN_MCV_FREQUENCY {
			t.Errorf("MCV below threshold included: frequency %f", v.Frequency)
		}
	}
}

// --- buildHistogram ---

func TestBuildHistogram_Empty(t *testing.T) {
	sc := NewStatisticsCollector(testLogger())
	result := sc.buildHistogram(map[interface{}]int64{}, 100)
	if result != nil {
		t.Errorf("expected nil for empty input, got %v", result)
	}
}

func TestBuildHistogram_SingleValue(t *testing.T) {
	sc := NewStatisticsCollector(testLogger())
	valueCount := map[interface{}]int64{"hello": 10}
	result := sc.buildHistogram(valueCount, 10)
	if len(result) != 1 {
		t.Fatalf("expected 1 bucket, got %d", len(result))
	}
	if result[0].DocumentCount != 10 {
		t.Errorf("expected 10 documents in bucket, got %d", result[0].DocumentCount)
	}
	if result[0].DistinctCount != 1 {
		t.Errorf("expected 1 distinct in bucket, got %d", result[0].DistinctCount)
	}
}

func TestBuildHistogram_EquiDepth(t *testing.T) {
	sc := NewStatisticsCollector(testLogger())
	// Create 100 distinct int values, each appearing once
	valueCount := make(map[interface{}]int64)
	for i := 0; i < 100; i++ {
		valueCount[int64(i)] = 1
	}
	result := sc.buildHistogram(valueCount, 100)
	if len(result) == 0 {
		t.Fatal("expected non-empty histogram")
	}
	// Verify total document count across buckets
	var totalDocs int64
	for _, bucket := range result {
		totalDocs += bucket.DocumentCount
	}
	if totalDocs != 100 {
		t.Errorf("expected 100 total documents across buckets, got %d", totalDocs)
	}
}

func TestBuildHistogram_MaxBuckets(t *testing.T) {
	sc := NewStatisticsCollector(testLogger())
	// Create many values to potentially exceed bucket count
	valueCount := make(map[interface{}]int64)
	for i := 0; i < 1000; i++ {
		valueCount[int64(i)] = 1
	}
	result := sc.buildHistogram(valueCount, 1000)
	if len(result) > DEFAULT_HISTOGRAM_BUCKETS {
		t.Errorf("expected at most %d buckets, got %d", DEFAULT_HISTOGRAM_BUCKETS, len(result))
	}
}

// --- EstimateSelectivity ---

func TestEstimateSelectivity_NilFieldStats(t *testing.T) {
	sc := NewStatisticsCollector(testLogger())
	sel := sc.EstimateSelectivity(nil, "=", "test")
	if sel != 0.1 {
		t.Errorf("expected 0.1 for nil field stats, got %f", sel)
	}
}

func TestEstimateSelectivity_EqualityMCV(t *testing.T) {
	sc := NewStatisticsCollector(testLogger())
	fieldStats := &FieldStatistics{
		DistinctCount: 10,
		MostCommonValues: []ValueFrequency{
			{Value: "alice", Frequency: 0.30},
			{Value: "bob", Frequency: 0.20},
		},
	}
	sel := sc.EstimateSelectivity(fieldStats, "==", "alice")
	if sel != 0.30 {
		t.Errorf("expected 0.30 for MCV match, got %f", sel)
	}

	// Also test with "=" operator
	sel2 := sc.EstimateSelectivity(fieldStats, "=", "bob")
	if sel2 != 0.20 {
		t.Errorf("expected 0.20 for MCV match with =, got %f", sel2)
	}
}

func TestEstimateSelectivity_EqualityNoMCV(t *testing.T) {
	sc := NewStatisticsCollector(testLogger())
	fieldStats := &FieldStatistics{
		DistinctCount: 100,
		MostCommonValues: []ValueFrequency{
			{Value: "alice", Frequency: 0.30},
		},
	}
	sel := sc.EstimateSelectivity(fieldStats, "==", "unknown")
	expected := 1.0 / 100.0
	if math.Abs(sel-expected) > 1e-9 {
		t.Errorf("expected %f for non-MCV equality, got %f", expected, sel)
	}
}

func TestEstimateSelectivity_EqualityZeroDistinct(t *testing.T) {
	sc := NewStatisticsCollector(testLogger())
	fieldStats := &FieldStatistics{
		DistinctCount:    0,
		MostCommonValues: []ValueFrequency{},
	}
	sel := sc.EstimateSelectivity(fieldStats, "==", "test")
	if sel != 0.01 {
		t.Errorf("expected 0.01 for zero distinct, got %f", sel)
	}
}

func TestEstimateSelectivity_Range(t *testing.T) {
	sc := NewStatisticsCollector(testLogger())
	fieldStats := &FieldStatistics{DistinctCount: 100}
	for _, op := range []string{">", "<", ">=", "<="} {
		sel := sc.EstimateSelectivity(fieldStats, op, 50)
		if sel != 0.33 {
			t.Errorf("expected 0.33 for operator %s, got %f", op, sel)
		}
	}
}

func TestEstimateSelectivity_DefaultOperator(t *testing.T) {
	sc := NewStatisticsCollector(testLogger())
	fieldStats := &FieldStatistics{DistinctCount: 100}
	sel := sc.EstimateSelectivity(fieldStats, "LIKE", "test%")
	if sel != 0.1 {
		t.Errorf("expected 0.1 for unknown operator, got %f", sel)
	}
}

// --- estimateValueSize ---

func TestEstimateValueSize(t *testing.T) {
	sc := NewStatisticsCollector(testLogger())

	tests := []struct {
		value    interface{}
		expected int64
	}{
		{"hello", 5},
		{"", 0},
		{42, 8},
		{int32(42), 8},
		{int64(42), 8},
		{float32(3.14), 8},
		{float64(3.14), 8},
		{true, 8},
		{false, 8},
		{[]int{1, 2, 3}, 16}, // unknown type
		{nil, 16},             // unknown type
	}

	for _, tt := range tests {
		size := sc.estimateValueSize(tt.value)
		if size != tt.expected {
			t.Errorf("estimateValueSize(%v) = %d, expected %d", tt.value, size, tt.expected)
		}
	}
}

// --- compareValues ---

func TestCompareValues_Strings(t *testing.T) {
	sc := NewStatisticsCollector(testLogger())
	if sc.compareValues("abc", "def") >= 0 {
		t.Error("expected abc < def")
	}
	if sc.compareValues("def", "abc") <= 0 {
		t.Error("expected def > abc")
	}
	if sc.compareValues("abc", "abc") != 0 {
		t.Error("expected abc == abc")
	}
}

func TestCompareValues_Int(t *testing.T) {
	sc := NewStatisticsCollector(testLogger())
	if sc.compareValues(1, 2) >= 0 {
		t.Error("expected 1 < 2")
	}
	if sc.compareValues(2, 1) <= 0 {
		t.Error("expected 2 > 1")
	}
	if sc.compareValues(5, 5) != 0 {
		t.Error("expected 5 == 5")
	}
}

func TestCompareValues_Int64(t *testing.T) {
	sc := NewStatisticsCollector(testLogger())
	if sc.compareValues(int64(1), int64(2)) >= 0 {
		t.Error("expected int64(1) < int64(2)")
	}
	if sc.compareValues(int64(2), int64(1)) <= 0 {
		t.Error("expected int64(2) > int64(1)")
	}
	if sc.compareValues(int64(5), int64(5)) != 0 {
		t.Error("expected int64(5) == int64(5)")
	}
}

func TestCompareValues_Float64(t *testing.T) {
	sc := NewStatisticsCollector(testLogger())
	if sc.compareValues(1.0, 2.0) >= 0 {
		t.Error("expected 1.0 < 2.0")
	}
	if sc.compareValues(2.0, 1.0) <= 0 {
		t.Error("expected 2.0 > 1.0")
	}
	if sc.compareValues(3.14, 3.14) != 0 {
		t.Error("expected 3.14 == 3.14")
	}
}

func TestCompareValues_MixedTypes(t *testing.T) {
	sc := NewStatisticsCollector(testLogger())
	// Different types should return 0 (no comparison possible)
	if sc.compareValues("abc", 123) != 0 {
		t.Error("expected 0 for mixed types string vs int")
	}
	if sc.compareValues(int64(1), "hello") != 0 {
		t.Error("expected 0 for mixed types int64 vs string")
	}
	if sc.compareValues(1.0, "hello") != 0 {
		t.Error("expected 0 for mixed types float64 vs string")
	}
}
