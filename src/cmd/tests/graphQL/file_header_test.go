package main

import (
	"testing"

	"syndrdb/src/internal/graphQL/schema"
)

func TestNewFileHeader(t *testing.T) {
	header := schema.NewFileHeader("testdb", "test1234")

	if header.Magic != schema.MagicNumber {
		t.Errorf("Expected magic 0x%08X, got 0x%08X", schema.MagicNumber, header.Magic)
	}

	if header.GetDatabaseName() != "testdb" {
		t.Errorf("Expected database name 'testdb', got '%s'", header.GetDatabaseName())
	}
}

func TestFileHeaderSerializeDeserialize(t *testing.T) {
	original := schema.NewFileHeader("mydb", "uuid5678")
	original.TotalRecords = 100
	original.ActiveRecords = 75
	original.TombstoneCount = 25

	data, err := original.Serialize()
	if err != nil {
		t.Fatalf("Serialize failed: %v", err)
	}

	restored := &schema.FileHeader{}
	if err := restored.Deserialize(data[:]); err != nil {
		t.Fatalf("Deserialize failed: %v", err)
	}

	if original.TotalRecords != restored.TotalRecords {
		t.Errorf("TotalRecords mismatch")
	}
}
