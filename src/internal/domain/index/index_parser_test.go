package index

import (
	"testing"

	"go.uber.org/zap"
)

func testLogger() *zap.SugaredLogger {
	logger, _ := zap.NewDevelopment()
	return logger.Sugar()
}

func TestParseCreateBRINIndexCommand(t *testing.T) {
	t.Run("basic BRIN index", func(t *testing.T) {
		cmd := `CREATE BRIN INDEX "ts_brin_idx" ON BUNDLE "events" WITH FIELDS ({"timestamp", true, false})`
		result, err := ParseCreateBRINIndexCommand(cmd, testLogger())
		if err != nil {
			t.Fatalf("parse failed: %v", err)
		}
		if result.IndexType != "brin" {
			t.Errorf("expected IndexType=brin, got %s", result.IndexType)
		}
		if result.IndexName != "ts_brin_idx" {
			t.Errorf("expected IndexName=ts_brin_idx, got %s", result.IndexName)
		}
		if result.BundleName != "events" {
			t.Errorf("expected BundleName=events, got %s", result.BundleName)
		}
		if len(result.Fields) != 1 || result.Fields[0].Name != "timestamp" {
			t.Errorf("expected field timestamp, got %v", result.Fields)
		}
		if result.PagesPerRange != 128 {
			t.Errorf("expected default PagesPerRange=128, got %d", result.PagesPerRange)
		}
	})

	t.Run("with PAGES_PER_RANGE", func(t *testing.T) {
		cmd := `CREATE BRIN INDEX "id_brin" ON BUNDLE "users" WITH FIELDS ({"id", false, false}) PAGES_PER_RANGE 64`
		result, err := ParseCreateBRINIndexCommand(cmd, testLogger())
		if err != nil {
			t.Fatalf("parse failed: %v", err)
		}
		if result.PagesPerRange != 64 {
			t.Errorf("expected PagesPerRange=64, got %d", result.PagesPerRange)
		}
	})

	t.Run("case insensitive", func(t *testing.T) {
		cmd := `create brin index "my_idx" on bundle "data" with fields ({"val", true, true})`
		result, err := ParseCreateBRINIndexCommand(cmd, testLogger())
		if err != nil {
			t.Fatalf("parse failed: %v", err)
		}
		if result.IndexName != "my_idx" {
			t.Errorf("expected IndexName=my_idx, got %s", result.IndexName)
		}
	})

	t.Run("invalid syntax", func(t *testing.T) {
		cmd := `CREATE BRIN "bad_syntax"`
		_, err := ParseCreateBRINIndexCommand(cmd, testLogger())
		if err == nil {
			t.Fatal("expected error for invalid syntax")
		}
	})

	t.Run("with semicolon and whitespace", func(t *testing.T) {
		cmd := `  CREATE BRIN INDEX "idx" ON BUNDLE "b" WITH FIELDS ({"f", true, false})  ;  `
		result, err := ParseCreateBRINIndexCommand(cmd, testLogger())
		if err != nil {
			t.Fatalf("parse failed: %v", err)
		}
		if result.IndexName != "idx" {
			t.Errorf("expected IndexName=idx, got %s", result.IndexName)
		}
	})
}
