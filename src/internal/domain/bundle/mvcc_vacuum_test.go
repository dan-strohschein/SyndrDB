package bundle

import (
	"testing"
	"time"

	"syndrdb/src/internal/domain/models"
)

// ============================================================================
// Tests for isDeadVersion (dead version reclamation logic)
// ============================================================================

func TestIsDeadVersion_CurrentVersionNeverDead(t *testing.T) {
	doc := &models.Document{
		DocumentID:      "doc1",
		CommitSequence:  100,
		SupersededAt:    time.Time{}, // Zero = current version
		DeletedByTxID:   0,
		VersionSequence: 1,
	}

	if isDeadVersion(doc, 200) {
		t.Error("current version (not superseded, not deleted) should never be dead")
	}
}

func TestIsDeadVersion_SupersededPastGracePeriod(t *testing.T) {
	doc := &models.Document{
		DocumentID:      "doc1",
		CommitSequence:  50,
		SupersededAt:    time.Now().Add(-200 * time.Millisecond), // 200ms ago (past 100ms grace)
		DeletedByTxID:   0,
		VersionSequence: 1,
	}

	// Safe cutoff is 100 (doc's CommitSequence=50 < 100)
	if !isDeadVersion(doc, 100) {
		t.Error("superseded version past grace period and before cutoff should be dead")
	}
}

func TestIsDeadVersion_SupersededWithinGracePeriod(t *testing.T) {
	doc := &models.Document{
		DocumentID:      "doc1",
		CommitSequence:  50,
		SupersededAt:    time.Now().Add(-10 * time.Millisecond), // 10ms ago (within 100ms grace)
		DeletedByTxID:   0,
		VersionSequence: 1,
	}

	if isDeadVersion(doc, 100) {
		t.Error("superseded version within grace period should NOT be dead")
	}
}

func TestIsDeadVersion_DeletedBeforeCutoff(t *testing.T) {
	doc := &models.Document{
		DocumentID:      "doc1",
		CommitSequence:  50,
		SupersededAt:    time.Now().Add(-200 * time.Millisecond),
		DeletedByTxID:   10, // Deleted by transaction 10
		VersionSequence: 1,
	}

	if !isDeadVersion(doc, 100) {
		t.Error("deleted version before cutoff should be dead")
	}
}

func TestIsDeadVersion_DeletedAfterCutoff(t *testing.T) {
	doc := &models.Document{
		DocumentID:      "doc1",
		CommitSequence:  150, // After cutoff
		SupersededAt:    time.Now().Add(-200 * time.Millisecond),
		DeletedByTxID:   10,
		VersionSequence: 1,
	}

	if isDeadVersion(doc, 100) {
		t.Error("deleted version after cutoff should NOT be dead (active snapshot may see it)")
	}
}

func TestIsDeadVersion_UncommittedNeverDead(t *testing.T) {
	doc := &models.Document{
		DocumentID:      "doc1",
		CommitSequence:  0, // Uncommitted
		SupersededAt:    time.Now().Add(-200 * time.Millisecond),
		DeletedByTxID:   0,
		VersionSequence: 1,
	}

	if isDeadVersion(doc, 100) {
		t.Error("uncommitted version should never be dead")
	}
}

func TestIsDeadVersion_NilDocument(t *testing.T) {
	if isDeadVersion(nil, 100) {
		t.Error("nil document should not be dead")
	}
}

func TestIsDeadVersion_ZeroCutoff(t *testing.T) {
	doc := &models.Document{
		DocumentID:      "doc1",
		CommitSequence:  50,
		SupersededAt:    time.Now().Add(-200 * time.Millisecond),
		DeletedByTxID:   0,
		VersionSequence: 1,
	}

	if isDeadVersion(doc, 0) {
		t.Error("zero cutoff means no safe cutoff available - should not reclaim")
	}
}

func TestIsDeadVersion_VisibleToActiveSnapshot(t *testing.T) {
	doc := &models.Document{
		DocumentID:      "doc1",
		CommitSequence:  150, // After the safe cutoff
		SupersededAt:    time.Now().Add(-200 * time.Millisecond),
		DeletedByTxID:   0,
		VersionSequence: 2,
	}

	// Safe cutoff is 100, but doc's CommitSequence (150) >= 100
	if isDeadVersion(doc, 100) {
		t.Error("version visible to active snapshot (CommitSequence >= cutoff) should NOT be dead")
	}
}

// ============================================================================
// Tests for updatesIndexedField (HOT-like update detection)
// ============================================================================

func TestUpdatesIndexedField_NoIndexes(t *testing.T) {
	bundle := &models.Bundle{
		Name:    "test-bundle",
		Indexes: nil,
	}
	updates := []models.KeyValue{
		{Key: "name", Value: "alice"},
	}

	if updatesIndexedField(bundle, updates) {
		t.Error("bundle with no indexes should never report indexed field updates")
	}
}

func TestUpdatesIndexedField_DocumentIDOnly(t *testing.T) {
	bundle := &models.Bundle{
		Name: "test-bundle",
		Indexes: map[string]models.IndexReference{
			"DocumentID_idx": {
				IndexType: "hash",
				HashIndexField: models.IndexField{
					FieldName: "DocumentID",
				},
			},
		},
	}
	updates := []models.KeyValue{
		{Key: "name", Value: "alice"},
		{Key: "age", Value: 30},
	}

	if updatesIndexedField(bundle, updates) {
		t.Error("updating non-indexed fields when only DocumentID index exists should return false")
	}
}

func TestUpdatesIndexedField_BTreeFieldChanged(t *testing.T) {
	bundle := &models.Bundle{
		Name: "test-bundle",
		Indexes: map[string]models.IndexReference{
			"DocumentID_idx": {
				IndexType: "hash",
				HashIndexField: models.IndexField{
					FieldName: "DocumentID",
				},
			},
			"name_idx": {
				IndexType: "btree",
				BTreeIndexField: models.IndexField{
					FieldName: "name",
				},
			},
		},
	}
	updates := []models.KeyValue{
		{Key: "name", Value: "alice"}, // This field has a B-tree index
	}

	if !updatesIndexedField(bundle, updates) {
		t.Error("updating a B-tree indexed field should return true")
	}
}

func TestUpdatesIndexedField_BTreeFieldNotChanged(t *testing.T) {
	bundle := &models.Bundle{
		Name: "test-bundle",
		Indexes: map[string]models.IndexReference{
			"name_idx": {
				IndexType: "btree",
				BTreeIndexField: models.IndexField{
					FieldName: "name",
				},
			},
		},
	}
	updates := []models.KeyValue{
		{Key: "age", Value: 30}, // "age" is NOT indexed
	}

	if updatesIndexedField(bundle, updates) {
		t.Error("updating a non-indexed field should return false even when B-tree index exists")
	}
}

func TestUpdatesIndexedField_BRINFieldChanged(t *testing.T) {
	bundle := &models.Bundle{
		Name: "test-bundle",
		Indexes: map[string]models.IndexReference{
			"created_brin": {
				IndexType: "brin",
				Fields: []models.FieldDefinition{
					{Name: "created_at"},
				},
			},
		},
	}
	updates := []models.KeyValue{
		{Key: "created_at", Value: "2026-01-01"},
	}

	if !updatesIndexedField(bundle, updates) {
		t.Error("updating a BRIN indexed field should return true")
	}
}

func TestUpdatesIndexedField_HashUserField(t *testing.T) {
	bundle := &models.Bundle{
		Name: "test-bundle",
		Indexes: map[string]models.IndexReference{
			"email_idx": {
				IndexType: "hash",
				HashIndexField: models.IndexField{
					FieldName: "email",
				},
			},
		},
	}
	updates := []models.KeyValue{
		{Key: "email", Value: "alice@example.com"},
	}

	if !updatesIndexedField(bundle, updates) {
		t.Error("updating a hash indexed user field should return true")
	}
}

func TestUpdatesIndexedField_MultiColumnBTree(t *testing.T) {
	bundle := &models.Bundle{
		Name: "test-bundle",
		Indexes: map[string]models.IndexReference{
			"composite_idx": {
				IndexType: "btree",
				BTreeIndexField: models.IndexField{
					FieldName: "city",
				},
				Fields: []models.FieldDefinition{
					{Name: "city"},
					{Name: "state"},
				},
			},
		},
	}

	// Update "state" which is part of the composite index
	updates := []models.KeyValue{
		{Key: "state", Value: "CA"},
	}

	if !updatesIndexedField(bundle, updates) {
		t.Error("updating a field that is part of a multi-column B-tree index should return true")
	}

	// Update "age" which is NOT part of the composite index
	updates2 := []models.KeyValue{
		{Key: "age", Value: 25},
	}

	if updatesIndexedField(bundle, updates2) {
		t.Error("updating a field NOT in the composite index should return false")
	}
}
