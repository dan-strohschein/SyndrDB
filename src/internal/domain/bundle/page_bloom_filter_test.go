package bundle

import (
	"testing"

	"syndrdb/src/internal/domain/models"
	"syndrdb/src/pkg/settings"
)

func init() {
	// Ensure settings are initialized for tests
	s := settings.GetSettings()
	s.PageBloomEnabled = true
	s.PageBloomFalsePositiveRate = 0.01
	s.PageBloomMinDocsPerPage = 5 // Lower for tests
	s.PageBloomMaxMemoryMB = 64
}

func makeTestDocs(n int, fieldValues map[string]interface{}) []models.Document {
	docs := make([]models.Document, n)
	for i := 0; i < n; i++ {
		data := make(map[string]interface{})
		for k, v := range fieldValues {
			data[k] = v
		}
		docs[i] = models.Document{
			DocumentID: "doc-" + string(rune('A'+i%26)),
			Data:       data,
		}
	}
	return docs
}

func TestPageBloomMap_BuildAndQuery(t *testing.T) {
	pbm := NewPageBloomMap("test-bundle")

	docs := makeTestDocs(100, map[string]interface{}{
		"country": "Iceland",
		"city":    "Reykjavik",
	})

	delta := pbm.BuildForPage(0, docs, 0.01)
	if delta <= 0 {
		t.Fatalf("expected positive memory delta, got %d", delta)
	}

	// Should find existing values
	mayContain, exists := pbm.MayContain(0, "country", "Iceland")
	if !exists {
		t.Fatal("expected bloom to exist for page 0")
	}
	if !mayContain {
		t.Fatal("expected bloom to say 'may contain' for country=Iceland")
	}

	mayContain, exists = pbm.MayContain(0, "city", "Reykjavik")
	if !exists {
		t.Fatal("expected bloom to exist for page 0")
	}
	if !mayContain {
		t.Fatal("expected bloom to say 'may contain' for city=Reykjavik")
	}

	// Should NOT find non-existent values (bloom says definitely absent)
	mayContain, exists = pbm.MayContain(0, "country", "Norway")
	if !exists {
		t.Fatal("expected bloom to exist for page 0")
	}
	if mayContain {
		t.Error("expected bloom to say 'definitely absent' for country=Norway")
	}

	// Non-existent page should return exists=false
	_, exists = pbm.MayContain(99, "country", "Iceland")
	if exists {
		t.Error("expected no bloom for page 99")
	}
}

func TestPageBloomMap_CompositeKeyIsolation(t *testing.T) {
	pbm := NewPageBloomMap("test-bundle")

	// Page 0 has name=Alice
	docs0 := makeTestDocs(100, map[string]interface{}{
		"name": "Alice",
	})
	pbm.BuildForPage(0, docs0, 0.01)

	// Page 1 has city=Alice
	docs1 := makeTestDocs(100, map[string]interface{}{
		"city": "Alice",
	})
	pbm.BuildForPage(1, docs1, 0.01)

	// name=Alice should be on page 0, NOT on page 1
	mayContain, exists := pbm.MayContain(0, "name", "Alice")
	if !exists || !mayContain {
		t.Error("expected name=Alice to be found on page 0")
	}

	mayContain, exists = pbm.MayContain(1, "name", "Alice")
	if !exists {
		t.Fatal("expected bloom to exist for page 1")
	}
	if mayContain {
		t.Error("expected name=Alice to be absent from page 1 (only has city=Alice)")
	}

	// city=Alice should be on page 1, NOT on page 0
	mayContain, exists = pbm.MayContain(1, "city", "Alice")
	if !exists || !mayContain {
		t.Error("expected city=Alice to be found on page 1")
	}

	mayContain, exists = pbm.MayContain(0, "city", "Alice")
	if !exists {
		t.Fatal("expected bloom to exist for page 0")
	}
	if mayContain {
		t.Error("expected city=Alice to be absent from page 0 (only has name=Alice)")
	}
}

func TestPageBloomMap_InvalidatePage(t *testing.T) {
	pbm := NewPageBloomMap("test-bundle")

	docs := makeTestDocs(100, map[string]interface{}{
		"status": "active",
	})
	pbm.BuildForPage(0, docs, 0.01)

	// Verify bloom exists
	if !pbm.HasBloom(0) {
		t.Fatal("expected bloom for page 0")
	}

	genBefore := pbm.Generation()

	// Invalidate
	pbm.InvalidatePage(0)

	// Bloom should be gone
	if pbm.HasBloom(0) {
		t.Error("expected bloom for page 0 to be invalidated")
	}

	// Generation should have incremented
	if pbm.Generation() <= genBefore {
		t.Error("expected generation to increment after invalidation")
	}

	// MayContain should return exists=false
	_, exists := pbm.MayContain(0, "status", "active")
	if exists {
		t.Error("expected no bloom after invalidation")
	}
}

func TestPageBloomMap_InvalidateAll(t *testing.T) {
	pbm := NewPageBloomMap("test-bundle")

	docs := makeTestDocs(100, map[string]interface{}{"x": "y"})
	pbm.BuildForPage(0, docs, 0.01)
	pbm.BuildForPage(1, docs, 0.01)
	pbm.BuildForPage(2, docs, 0.01)

	if !pbm.HasBloom(0) || !pbm.HasBloom(1) || !pbm.HasBloom(2) {
		t.Fatal("expected all 3 blooms to exist")
	}

	genBefore := pbm.Generation()
	pbm.InvalidateAll()

	if pbm.HasBloom(0) || pbm.HasBloom(1) || pbm.HasBloom(2) {
		t.Error("expected all blooms to be invalidated")
	}

	if pbm.Generation() <= genBefore {
		t.Error("expected generation to increment after InvalidateAll")
	}

	if pbm.TotalMemoryBytes() != 0 {
		t.Errorf("expected 0 memory after InvalidateAll, got %d", pbm.TotalMemoryBytes())
	}
}

func TestPageBloomMap_GenerationTracking(t *testing.T) {
	pbm := NewPageBloomMap("test-bundle")

	gen0 := pbm.Generation()
	if gen0 != 0 {
		t.Errorf("expected initial generation 0, got %d", gen0)
	}

	docs := makeTestDocs(100, map[string]interface{}{"a": "b"})
	pbm.BuildForPage(0, docs, 0.01)

	// Building doesn't bump generation (only invalidation does)
	if pbm.Generation() != gen0 {
		t.Error("expected generation unchanged after build")
	}

	pbm.InvalidatePage(0)
	gen1 := pbm.Generation()
	if gen1 <= gen0 {
		t.Errorf("expected generation to increase after InvalidatePage: %d <= %d", gen1, gen0)
	}

	pbm.BuildForPage(1, docs, 0.01)
	pbm.InvalidateAll()
	gen2 := pbm.Generation()
	if gen2 <= gen1 {
		t.Errorf("expected generation to increase after InvalidateAll: %d <= %d", gen2, gen1)
	}
}

func TestPageBloomMap_MemoryTracking(t *testing.T) {
	pbm := NewPageBloomMap("test-bundle")

	if pbm.TotalMemoryBytes() != 0 {
		t.Errorf("expected 0 initial memory, got %d", pbm.TotalMemoryBytes())
	}

	docs := makeTestDocs(100, map[string]interface{}{"a": "b"})
	pbm.BuildForPage(0, docs, 0.01)
	mem1 := pbm.TotalMemoryBytes()
	if mem1 <= 0 {
		t.Errorf("expected positive memory after build, got %d", mem1)
	}

	pbm.BuildForPage(1, docs, 0.01)
	mem2 := pbm.TotalMemoryBytes()
	if mem2 <= mem1 {
		t.Errorf("expected memory to increase after second build: %d <= %d", mem2, mem1)
	}

	pbm.InvalidatePage(0)
	mem3 := pbm.TotalMemoryBytes()
	if mem3 >= mem2 {
		t.Errorf("expected memory to decrease after invalidation: %d >= %d", mem3, mem2)
	}

	pbm.InvalidateAll()
	if pbm.TotalMemoryBytes() != 0 {
		t.Errorf("expected 0 memory after InvalidateAll, got %d", pbm.TotalMemoryBytes())
	}
}

func TestPageBloomMap_BelowMinDocsSkipped(t *testing.T) {
	pbm := NewPageBloomMap("test-bundle")

	// Create only 3 docs (below MinDocsPerPage threshold of 5 set in init)
	docs := makeTestDocs(3, map[string]interface{}{"a": "b"})
	delta := pbm.BuildForPage(0, docs, 0.01)

	if delta != 0 {
		t.Errorf("expected 0 delta for below-threshold page, got %d", delta)
	}

	if pbm.HasBloom(0) {
		t.Error("expected no bloom for below-threshold page")
	}
}

func TestPageBloomMap_EmptyDocs(t *testing.T) {
	pbm := NewPageBloomMap("test-bundle")

	delta := pbm.BuildForPage(0, nil, 0.01)
	if delta != 0 {
		t.Errorf("expected 0 delta for nil docs, got %d", delta)
	}

	delta = pbm.BuildForPage(0, []models.Document{}, 0.01)
	if delta != 0 {
		t.Errorf("expected 0 delta for empty docs, got %d", delta)
	}
}

func TestPageBloomMap_InvalidateNonExistentPage(t *testing.T) {
	pbm := NewPageBloomMap("test-bundle")

	genBefore := pbm.Generation()

	// Invalidating a non-existent page should be a no-op
	pbm.InvalidatePage(999)

	if pbm.Generation() != genBefore {
		t.Error("expected generation unchanged when invalidating non-existent page")
	}
}
