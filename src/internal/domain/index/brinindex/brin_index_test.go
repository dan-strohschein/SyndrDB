package brinindex

import (
	"os"
	"path/filepath"
	"testing"

	"go.uber.org/zap"
)

func testLogger() *zap.SugaredLogger {
	logger, _ := zap.NewDevelopment()
	return logger.Sugar()
}

func TestNewBRINIndex(t *testing.T) {
	t.Run("valid config", func(t *testing.T) {
		idx, err := NewBRINIndex(BRINConfig{
			IndexName:     "test_brin",
			BundleName:    "users",
			DatabaseName:  "testdb",
			FieldName:     "age",
			DataDir:       t.TempDir(),
			PagesPerRange: 128,
			Logger:        testLogger(),
		})
		if err != nil {
			t.Fatalf("expected no error, got %v", err)
		}
		if idx.EntryCount() != 0 {
			t.Errorf("expected 0 entries, got %d", idx.EntryCount())
		}
	})

	t.Run("empty field name", func(t *testing.T) {
		_, err := NewBRINIndex(BRINConfig{
			FieldName: "",
			Logger:    testLogger(),
		})
		if err == nil {
			t.Fatal("expected error for empty field name")
		}
	})

	t.Run("nil logger", func(t *testing.T) {
		_, err := NewBRINIndex(BRINConfig{
			FieldName: "age",
			Logger:    nil,
		})
		if err == nil {
			t.Fatal("expected error for nil logger")
		}
	})

	t.Run("default PagesPerRange", func(t *testing.T) {
		idx, err := NewBRINIndex(BRINConfig{
			FieldName:     "age",
			PagesPerRange: 0, // should default to 128
			Logger:        testLogger(),
		})
		if err != nil {
			t.Fatalf("expected no error, got %v", err)
		}
		if idx.config.PagesPerRange != 128 {
			t.Errorf("expected PagesPerRange=128, got %d", idx.config.PagesPerRange)
		}
	})
}

func TestUpdateRange_Numeric(t *testing.T) {
	idx, _ := NewBRINIndex(BRINConfig{
		FieldName:     "age",
		PagesPerRange: 4,
		Logger:        testLogger(),
	})

	// Insert values into page 0
	idx.UpdateRange(0, 25)
	idx.UpdateRange(1, 30)
	idx.UpdateRange(2, 20)
	idx.UpdateRange(3, 35)

	if idx.EntryCount() != 1 {
		t.Fatalf("expected 1 range entry, got %d", idx.EntryCount())
	}

	// Insert into a different range (page 4-7)
	idx.UpdateRange(4, 50)
	idx.UpdateRange(5, 60)

	if idx.EntryCount() != 2 {
		t.Fatalf("expected 2 range entries, got %d", idx.EntryCount())
	}

	// Verify min/max for first range
	entry := idx.entries[0]
	if entry.MinFloat != 20 || entry.MaxFloat != 35 {
		t.Errorf("range 0: expected min=20 max=35, got min=%v max=%v", entry.MinFloat, entry.MaxFloat)
	}
	if entry.DocCount != 4 {
		t.Errorf("range 0: expected 4 docs, got %d", entry.DocCount)
	}
	if !entry.IsNumeric {
		t.Error("range 0: expected IsNumeric=true")
	}

	// Verify min/max for second range
	entry = idx.entries[1]
	if entry.MinFloat != 50 || entry.MaxFloat != 60 {
		t.Errorf("range 1: expected min=50 max=60, got min=%v max=%v", entry.MinFloat, entry.MaxFloat)
	}
}

func TestUpdateRange_String(t *testing.T) {
	idx, _ := NewBRINIndex(BRINConfig{
		FieldName:     "name",
		PagesPerRange: 4,
		Logger:        testLogger(),
	})

	idx.UpdateRange(0, "Alice")
	idx.UpdateRange(1, "Charlie")
	idx.UpdateRange(2, "Bob")

	entry := idx.entries[0]
	if !entry.IsString {
		t.Error("expected IsString=true")
	}
	if entry.MinString != "Alice" || entry.MaxString != "Charlie" {
		t.Errorf("expected min=Alice max=Charlie, got min=%s max=%s", entry.MinString, entry.MaxString)
	}
}

func TestUpdateRange_NullValues(t *testing.T) {
	idx, _ := NewBRINIndex(BRINConfig{
		FieldName:     "age",
		PagesPerRange: 4,
		Logger:        testLogger(),
	})

	idx.UpdateRange(0, nil)
	idx.UpdateRange(1, 25)

	entry := idx.entries[0]
	if !entry.HasNulls {
		t.Error("expected HasNulls=true")
	}
	if entry.DocCount != 2 {
		t.Errorf("expected 2 docs, got %d", entry.DocCount)
	}
}

func TestScanRanges_NumericOverlap(t *testing.T) {
	idx, _ := NewBRINIndex(BRINConfig{
		FieldName:     "age",
		PagesPerRange: 4,
		Logger:        testLogger(),
	})

	// Range 0 (pages 0-3): age 20-35
	idx.UpdateRange(0, 20)
	idx.UpdateRange(1, 35)

	// Range 1 (pages 4-7): age 50-60
	idx.UpdateRange(4, 50)
	idx.UpdateRange(5, 60)

	// Range 2 (pages 8-11): age 100-200
	idx.UpdateRange(8, 100)
	idx.UpdateRange(9, 200)

	t.Run("query overlaps first range only", func(t *testing.T) {
		ranges := idx.ScanRanges(25, 40)
		if len(ranges) != 1 {
			t.Fatalf("expected 1 matching range, got %d", len(ranges))
		}
		if ranges[0].StartPageID != 0 {
			t.Errorf("expected start=0, got %d", ranges[0].StartPageID)
		}
	})

	t.Run("query overlaps all ranges", func(t *testing.T) {
		ranges := idx.ScanRanges(0, 300)
		if len(ranges) != 3 {
			t.Fatalf("expected 3 matching ranges, got %d", len(ranges))
		}
	})

	t.Run("query overlaps no ranges", func(t *testing.T) {
		ranges := idx.ScanRanges(70, 90)
		if len(ranges) != 0 {
			t.Fatalf("expected 0 matching ranges, got %d", len(ranges))
		}
	})

	t.Run("query overlaps second range boundary", func(t *testing.T) {
		ranges := idx.ScanRanges(40, 55)
		if len(ranges) != 1 {
			t.Fatalf("expected 1 matching range, got %d", len(ranges))
		}
		if ranges[0].StartPageID != 4 {
			t.Errorf("expected start=4, got %d", ranges[0].StartPageID)
		}
	})

	t.Run("open-ended range min only", func(t *testing.T) {
		// age >= 55 → should match ranges 1 and 2
		ranges := idx.ScanRanges(55, nil)
		if len(ranges) != 2 {
			t.Fatalf("expected 2 matching ranges, got %d", len(ranges))
		}
	})

	t.Run("open-ended range max only", func(t *testing.T) {
		// age <= 40 → should match range 0 only
		ranges := idx.ScanRanges(nil, 40)
		if len(ranges) != 1 {
			t.Fatalf("expected 1 matching range, got %d", len(ranges))
		}
	})
}

func TestScanRangesForOperator(t *testing.T) {
	idx, _ := NewBRINIndex(BRINConfig{
		FieldName:     "age",
		PagesPerRange: 4,
		Logger:        testLogger(),
	})

	// Range 0: 10-30, Range 1: 50-70
	idx.UpdateRange(0, 10)
	idx.UpdateRange(1, 30)
	idx.UpdateRange(4, 50)
	idx.UpdateRange(5, 70)

	t.Run("greater than", func(t *testing.T) {
		ranges := idx.ScanRangesForOperator(">", 40)
		if len(ranges) != 1 {
			t.Fatalf("expected 1 range for > 40, got %d", len(ranges))
		}
	})

	t.Run("less than", func(t *testing.T) {
		ranges := idx.ScanRangesForOperator("<", 40)
		if len(ranges) != 1 {
			t.Fatalf("expected 1 range for < 40, got %d", len(ranges))
		}
	})

	t.Run("equals", func(t *testing.T) {
		ranges := idx.ScanRangesForOperator("=", 25)
		if len(ranges) != 1 {
			t.Fatalf("expected 1 range for = 25, got %d", len(ranges))
		}
	})

	t.Run("unknown operator falls back to all", func(t *testing.T) {
		ranges := idx.ScanRangesForOperator("LIKE", "foo")
		if len(ranges) != 2 {
			t.Fatalf("expected 2 ranges (all) for unknown op, got %d", len(ranges))
		}
	})
}

func TestAllRanges(t *testing.T) {
	idx, _ := NewBRINIndex(BRINConfig{
		FieldName:     "age",
		PagesPerRange: 4,
		Logger:        testLogger(),
	})

	idx.UpdateRange(0, 10)
	idx.UpdateRange(4, 20)
	idx.UpdateRange(8, 30)

	ranges := idx.AllRanges()
	if len(ranges) != 3 {
		t.Fatalf("expected 3 ranges, got %d", len(ranges))
	}
}

func TestFlushAndReopen(t *testing.T) {
	dir := t.TempDir()
	config := BRINConfig{
		IndexName:     "test_brin",
		BundleName:    "users",
		FieldName:     "age",
		DataDir:       dir,
		PagesPerRange: 4,
		Logger:        testLogger(),
	}

	// Create and populate
	idx, _ := NewBRINIndex(config)
	idx.UpdateRange(0, 25)
	idx.UpdateRange(1, 30)
	idx.UpdateRange(4, 50)

	err := idx.Flush()
	if err != nil {
		t.Fatalf("flush failed: %v", err)
	}

	// Verify file exists
	filePath := filepath.Join(dir, "users_age.brin")
	if _, err := os.Stat(filePath); os.IsNotExist(err) {
		t.Fatal("BRIN file was not created")
	}

	// Reopen
	idx2, err := OpenBRINIndex(config)
	if err != nil {
		t.Fatalf("open failed: %v", err)
	}

	if idx2.EntryCount() != 2 {
		t.Fatalf("reopened index: expected 2 entries, got %d", idx2.EntryCount())
	}

	// Verify min/max preserved
	ranges := idx2.ScanRanges(26, 35)
	if len(ranges) != 1 {
		t.Fatalf("expected 1 matching range after reopen, got %d", len(ranges))
	}
}

func TestFlush_NoDirtyNoWrite(t *testing.T) {
	idx, _ := NewBRINIndex(BRINConfig{
		FieldName:     "age",
		DataDir:       t.TempDir(),
		PagesPerRange: 4,
		Logger:        testLogger(),
		BundleName:    "test",
	})

	// Flush without any updates should be a no-op
	err := idx.Flush()
	if err != nil {
		t.Fatalf("flush of clean index failed: %v", err)
	}

	filePath := idx.getFilePath()
	if _, err := os.Stat(filePath); !os.IsNotExist(err) {
		t.Error("expected no file to be written for clean index")
	}
}

func TestFindOrCreateRange_SortedInsertion(t *testing.T) {
	idx, _ := NewBRINIndex(BRINConfig{
		FieldName:     "id",
		PagesPerRange: 4,
		Logger:        testLogger(),
	})

	// Insert ranges out of order
	idx.UpdateRange(8, 100)  // range 8-11
	idx.UpdateRange(0, 10)   // range 0-3
	idx.UpdateRange(4, 50)   // range 4-7
	idx.UpdateRange(12, 200) // range 12-15

	// Verify sorted order
	if len(idx.entries) != 4 {
		t.Fatalf("expected 4 entries, got %d", len(idx.entries))
	}
	for i := 1; i < len(idx.entries); i++ {
		if idx.entries[i].RangeStart <= idx.entries[i-1].RangeStart {
			t.Errorf("entries not sorted: [%d].start=%d <= [%d].start=%d",
				i, idx.entries[i].RangeStart, i-1, idx.entries[i-1].RangeStart)
		}
	}
}

func TestScanRanges_SkipsEmptyAndAllNull(t *testing.T) {
	idx, _ := NewBRINIndex(BRINConfig{
		FieldName:     "age",
		PagesPerRange: 4,
		Logger:        testLogger(),
	})

	// Range with only null values
	idx.UpdateRange(0, nil)
	idx.UpdateRange(1, nil)
	idx.entries[0].AllNulls = true

	// Range with real data
	idx.UpdateRange(4, 50)
	idx.UpdateRange(5, 60)

	ranges := idx.ScanRanges(40, 70)
	if len(ranges) != 1 {
		t.Fatalf("expected 1 range (skipping all-null), got %d", len(ranges))
	}
	if ranges[0].StartPageID != 4 {
		t.Errorf("expected matching range start=4, got %d", ranges[0].StartPageID)
	}
}

func TestGetStats(t *testing.T) {
	idx, _ := NewBRINIndex(BRINConfig{
		FieldName:     "age",
		PagesPerRange: 4,
		Logger:        testLogger(),
	})

	idx.UpdateRange(0, 10)
	idx.UpdateRange(1, 30)
	idx.UpdateRange(4, 50)
	idx.UpdateRange(5, 70)

	// Scan that skips range 1
	idx.ScanRanges(5, 35)

	stats := idx.GetStats()
	if stats.TotalScans != 1 {
		t.Errorf("expected 1 scan, got %d", stats.TotalScans)
	}
	if stats.RangesScanned == 0 {
		t.Error("expected some ranges scanned")
	}
}

func TestGetConfig(t *testing.T) {
	config := BRINConfig{
		IndexName:     "my_idx",
		BundleName:    "users",
		DatabaseName:  "testdb",
		FieldName:     "ts",
		DataDir:       "/tmp/test",
		PagesPerRange: 64,
		Logger:        testLogger(),
	}
	idx, _ := NewBRINIndex(config)
	got := idx.GetConfig()
	if got.IndexName != "my_idx" || got.FieldName != "ts" || got.PagesPerRange != 64 {
		t.Errorf("config mismatch: %+v", got)
	}
}

func TestLargeScale_ManyRanges(t *testing.T) {
	idx, _ := NewBRINIndex(BRINConfig{
		FieldName:     "id",
		PagesPerRange: 4,
		Logger:        testLogger(),
	})

	// Simulate 1000 pages (250 ranges of 4 pages each)
	for pageID := uint32(0); pageID < 1000; pageID++ {
		idx.UpdateRange(pageID, float64(pageID*10))
	}

	if idx.EntryCount() != 250 {
		t.Fatalf("expected 250 ranges, got %d", idx.EntryCount())
	}

	// Query that should match ~10% of ranges
	ranges := idx.ScanRanges(float64(4500), float64(5500))
	if len(ranges) == 0 {
		t.Fatal("expected some matching ranges for mid-range query")
	}
	if len(ranges) >= 250 {
		t.Fatal("expected BRIN to skip some ranges")
	}
}

func TestToFloat64BRIN(t *testing.T) {
	tests := []struct {
		input    interface{}
		isNum    bool
		expected float64
	}{
		{42, true, 42.0},
		{int32(10), true, 10.0},
		{int64(999), true, 999.0},
		{float32(3.14), true, float64(float32(3.14))},
		{float64(2.718), true, 2.718},
		{uint(5), true, 5.0},
		{uint32(100), true, 100.0},
		{uint64(1000), true, 1000.0},
		{"hello", false, 0},
		{nil, false, 0},
		{true, false, 0},
	}

	for _, tc := range tests {
		isNum, val := toFloat64BRIN(tc.input)
		if isNum != tc.isNum {
			t.Errorf("toFloat64BRIN(%v): isNum=%v, want %v", tc.input, isNum, tc.isNum)
		}
		if isNum && val != tc.expected {
			t.Errorf("toFloat64BRIN(%v): val=%v, want %v", tc.input, val, tc.expected)
		}
	}
}
