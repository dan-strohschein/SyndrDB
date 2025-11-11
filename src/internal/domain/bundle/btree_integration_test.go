// B-tree Integration Unit Tests for Phase 3 (Tasks 3 & 4)

package bundle

import (
	"strings"
	"syndrdb/src/internal/domain/models"
	"testing"

	"go.uber.org/zap"
)

// createTestBundleService creates a minimal BundleService for testing
func createTestBundleService() *BundleService {
	testLogger, _ := zap.NewDevelopment()
	return &BundleService{
		logger: testLogger.Sugar(),
	}
}

// createTestBundle creates a test bundle with sample indexes
func createTestBundle(withIndexes bool) *models.Bundle {
	bundle := &models.Bundle{
		Name: "test-bundle",
		DocumentStructure: models.DocumentStructure{
			FieldDefinitions: map[string]models.FieldDefinition{
				"id":   {Name: "id", Type: "int"},
				"name": {Name: "name", Type: "string"},
				"age":  {Name: "age", Type: "int"},
			},
		},
	}

	if withIndexes {
		bundle.Indexes = map[string]models.IndexReference{
			"idx_name": {
				IndexName: "idx_name",
				IndexType: "btree",
				BTreeIndexField: models.IndexField{
					FieldName: "name",
					IsUnique:  false,
				},
			},
			"idx_age": {
				IndexName: "idx_age",
				IndexType: "btree",
				BTreeIndexField: models.IndexField{
					FieldName: "age",
					IsUnique:  false,
				},
			},
			"idx_hash": {
				IndexName: "idx_hash",
				IndexType: "hash",
				HashIndexField: models.IndexField{
					FieldName: "id",
				},
			},
		}
	}

	return bundle
}

// TASK 3 TESTS: INDEX LIFECYCLE MANAGEMENT

func TestInvalidateBTreeIndexesOnBundleChange_NilBundle(t *testing.T) {
	service := createTestBundleService()
	err := service.InvalidateBTreeIndexesOnBundleChange(nil, []string{"name"})
	if err != nil {
		t.Errorf("Expected no error for nil bundle, got: %v", err)
	}
}

func TestInvalidateBTreeIndexesOnBundleChange_AffectedField(t *testing.T) {
	service := createTestBundleService()
	bundle := createTestBundle(true)
	err := service.InvalidateBTreeIndexesOnBundleChange(bundle, []string{"name"})
	if err != nil {
		t.Errorf("Expected no error, got: %v", err)
	}
}

func TestOnBundleDeleteCleanupIndexes_NilBundle(t *testing.T) {
	service := createTestBundleService()
	err := service.OnBundleDeleteCleanupIndexes(nil)
	if err != nil {
		t.Errorf("Expected no error for nil bundle, got: %v", err)
	}
}

// TASK 4 TESTS: INDEX REBUILD & MAINTENANCE (ERROR CASES)

func TestRebuildBTreeIndexFromBundle_NilBundle(t *testing.T) {
	service := createTestBundleService()
	_, err := service.RebuildBTreeIndexFromBundle(nil, "idx_name")
	if err == nil {
		t.Error("Expected error for nil bundle, got none")
	}
	if err.Error() != "bundle cannot be nil" {
		t.Errorf("Expected error 'bundle cannot be nil', got '%s'", err.Error())
	}
}

func TestRebuildBTreeIndexFromBundle_NonExistentIndex(t *testing.T) {
	service := createTestBundleService()
	bundle := createTestBundle(true)
	_, err := service.RebuildBTreeIndexFromBundle(bundle, "idx_nonexistent")
	if err == nil {
		t.Error("Expected error for nonexistent index, got none")
	}
	if !strings.Contains(err.Error(), "does not exist") {
		t.Errorf("Expected error containing 'does not exist', got '%s'", err.Error())
	}
}

func TestCompactBTreeIndexFromBundle_NilBundle(t *testing.T) {
	service := createTestBundleService()
	_, err := service.CompactBTreeIndexFromBundle(nil, "idx_name")
	if err == nil {
		t.Error("Expected error for nil bundle, got none")
	}
}

func TestValidateBTreeIndexFromBundle_NilBundle(t *testing.T) {
	service := createTestBundleService()
	_, err := service.ValidateBTreeIndexFromBundle(nil, "idx_name")
	if err == nil {
		t.Error("Expected error for nil bundle, got none")
	}
}
