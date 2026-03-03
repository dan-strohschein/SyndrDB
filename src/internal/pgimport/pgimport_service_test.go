package pgimport

import (
	"os"
	"strings"
	"testing"

	"go.uber.org/zap"
)

func newTestLogger() *zap.SugaredLogger {
	logger, _ := zap.NewDevelopment()
	return logger.Sugar()
}

func TestDetectFormat_PlainSQL(t *testing.T) {
	tmp, err := os.CreateTemp("", "pgimport-test-*.sql")
	if err != nil {
		t.Fatal(err)
	}
	defer os.Remove(tmp.Name())

	tmp.WriteString("CREATE TABLE users (id integer);\n")
	tmp.Close()

	format, err := DetectFormat(tmp.Name())
	if err != nil {
		t.Fatalf("error: %v", err)
	}
	if format != FormatPlain {
		t.Errorf("format = %v, want FormatPlain", format)
	}
}

func TestDetectFormat_CustomFormat(t *testing.T) {
	tmp, err := os.CreateTemp("", "pgimport-test-*.dump")
	if err != nil {
		t.Fatal(err)
	}
	defer os.Remove(tmp.Name())

	tmp.Write([]byte("PGDMP"))
	tmp.Write([]byte{0x01, 0x0e, 0x00, 0x04, 0x00})
	tmp.Close()

	format, err := DetectFormat(tmp.Name())
	if err != nil {
		t.Fatalf("error: %v", err)
	}
	if format != FormatCustom {
		t.Errorf("format = %v, want FormatCustom", format)
	}
}

func TestConvertCustomToPlain_NoPgRestore(t *testing.T) {
	origPath := os.Getenv("PATH")
	os.Setenv("PATH", "")
	defer os.Setenv("PATH", origPath)

	_, _, err := ConvertCustomToPlain("/nonexistent.dump", "", nil)
	if err == nil {
		t.Fatal("expected error when pg_restore not found")
	}
	if !strings.Contains(err.Error(), "pg_restore") {
		t.Errorf("error should mention pg_restore: %v", err)
	}
}

func TestImportReport_Duration(t *testing.T) {
	report := &ImportReport{}
	report.EndTime = report.StartTime.Add(5_000_000_000)
	if report.Duration().Seconds() != 5 {
		t.Errorf("duration = %v, want 5s", report.Duration())
	}
}

func TestFormatName(t *testing.T) {
	if formatName(FormatPlain) != "plain SQL (-Fp)" {
		t.Errorf("plain = %q", formatName(FormatPlain))
	}
	if formatName(FormatCustom) != "custom (-Fc)" {
		t.Errorf("custom = %q", formatName(FormatCustom))
	}
}

func TestValidate_MissingFile(t *testing.T) {
	svc := &PGImportService{logger: newTestLogger()}
	opts := ImportOptions{SourceFile: "/nonexistent/file.sql", TargetDatabase: "test"}
	err := svc.validate(&opts)
	if err == nil {
		t.Fatal("expected error for missing file")
	}
}

func TestValidate_EmptySource(t *testing.T) {
	svc := &PGImportService{logger: newTestLogger()}
	opts := ImportOptions{TargetDatabase: "test"}
	err := svc.validate(&opts)
	if err == nil {
		t.Fatal("expected error for empty source")
	}
}

func TestValidate_AutoDetect(t *testing.T) {
	tmp, err := os.CreateTemp("", "pgimport-test-*.sql")
	if err != nil {
		t.Fatal(err)
	}
	defer os.Remove(tmp.Name())
	tmp.WriteString("SELECT 1;\n")
	tmp.Close()

	svc := &PGImportService{logger: newTestLogger()}
	opts := ImportOptions{SourceFile: tmp.Name(), TargetDatabase: "test", Format: FormatAuto}
	err = svc.validate(&opts)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if opts.Format != FormatPlain {
		t.Errorf("expected format to be auto-detected as plain, got %v", opts.Format)
	}
}

func TestDefaultImportOptions(t *testing.T) {
	opts := DefaultImportOptions()
	if opts.BatchSize != 1000 {
		t.Errorf("BatchSize = %d, want 1000", opts.BatchSize)
	}
	if !opts.CreateIndexesAfter {
		t.Error("CreateIndexesAfter should be true")
	}
	if opts.MaxErrors != 100 {
		t.Errorf("MaxErrors = %d, want 100", opts.MaxErrors)
	}
	if opts.ProgressInterval != 10000 {
		t.Errorf("ProgressInterval = %d, want 10000", opts.ProgressInterval)
	}
}
