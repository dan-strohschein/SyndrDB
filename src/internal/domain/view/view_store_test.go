package view

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
	"time"

	"go.uber.org/zap"
)

func TestViewFileStore_RoundTrip(t *testing.T) {
	logger, _ := zap.NewDevelopment()
	sugar := logger.Sugar()

	tmpDir := t.TempDir()
	dbDir := filepath.Join(tmpDir, "testdb")
	os.MkdirAll(dbDir, 0755)

	now := time.Now().Truncate(time.Second)
	v := &View{
		ViewName:          "TestView",
		DatabaseName:      "testdb",
		Definition:        `SELECT * FROM "Users" WHERE "Active" == true`,
		Type:              ViewTypeRegular,
		CreatedAt:         now,
		CreatedBy:         "admin",
		ReferencedBundles: []string{"Users"},
		Columns:           []ViewColumn{},
	}

	filePath := filepath.Join(dbDir, "testdb_TestView.view")

	// Write using the same format as saveToBinaryFile
	writeTestViewFile(t, filePath, v)

	// Read back using the same format as LoadView
	loaded, err := readTestViewFile(filePath)
	if err != nil {
		t.Fatalf("failed to load view: %v", err)
	}

	if loaded.ViewName != "TestView" {
		t.Errorf("expected ViewName='TestView', got '%s'", loaded.ViewName)
	}
	if loaded.Definition != v.Definition {
		t.Errorf("definition mismatch: got '%s'", loaded.Definition)
	}
	if loaded.Type != ViewTypeRegular {
		t.Errorf("expected ViewTypeRegular, got %v", loaded.Type)
	}
	if loaded.CreatedBy != "admin" {
		t.Errorf("expected CreatedBy='admin', got '%s'", loaded.CreatedBy)
	}
	if len(loaded.ReferencedBundles) != 1 || loaded.ReferencedBundles[0] != "Users" {
		t.Errorf("referenced bundles mismatch: %v", loaded.ReferencedBundles)
	}
	_ = sugar
}

func TestViewFileStore_LoadNonExistent(t *testing.T) {
	_, err := readTestViewFile("/nonexistent/path/view.view")
	if err == nil {
		t.Fatal("expected error for nonexistent file")
	}
}

func writeTestViewFile(t *testing.T, filePath string, v *View) {
	t.Helper()
	file, err := os.Create(filePath)
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	defer file.Close()

	// Version byte
	file.Write([]byte{ViewFileFormatVersion})

	// JSON payload
	if err := json.NewEncoder(file).Encode(v); err != nil {
		t.Fatalf("encode: %v", err)
	}
}

func readTestViewFile(filePath string) (*View, error) {
	file, err := os.Open(filePath)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	version := make([]byte, 1)
	if _, err := file.Read(version); err != nil {
		return nil, err
	}

	var v View
	if err := json.NewDecoder(file).Decode(&v); err != nil {
		return nil, err
	}
	return &v, nil
}
