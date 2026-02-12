package bundle

import (
	"sync"
	"testing"
	"time"

	"syndrdb/src/internal/domain/models"
)

func TestNewVisibilityMap(t *testing.T) {
	vm := NewVisibilityMap("test-bundle", 100)
	if vm == nil {
		t.Fatal("expected non-nil visibility map")
	}
	if vm.PageCount() != 100 {
		t.Errorf("expected 100 pages, got %d", vm.PageCount())
	}
	visCount, total := vm.Stats()
	if visCount != 0 || total != 100 {
		t.Errorf("expected 0 visible of 100, got %d of %d", visCount, total)
	}
}

func TestSetAndCheckAllVisible(t *testing.T) {
	vm := NewVisibilityMap("test-bundle", 64)

	// Initially not visible
	if vm.IsAllVisible(0) {
		t.Error("page 0 should not be all-visible initially")
	}

	// Set and verify
	vm.SetAllVisible(0)
	if !vm.IsAllVisible(0) {
		t.Error("page 0 should be all-visible after SetAllVisible")
	}

	// Other pages still not visible
	if vm.IsAllVisible(1) {
		t.Error("page 1 should not be all-visible")
	}

	// Set another page
	vm.SetAllVisible(63)
	if !vm.IsAllVisible(63) {
		t.Error("page 63 should be all-visible after SetAllVisible")
	}

	visCount, _ := vm.Stats()
	if visCount != 2 {
		t.Errorf("expected 2 visible pages, got %d", visCount)
	}
}

func TestSetAllVisibleIdempotent(t *testing.T) {
	vm := NewVisibilityMap("test-bundle", 10)
	vm.SetAllVisible(5)
	vm.SetAllVisible(5) // duplicate set
	vm.SetAllVisible(5) // another duplicate

	visCount, _ := vm.Stats()
	if visCount != 1 {
		t.Errorf("expected 1 visible page (idempotent), got %d", visCount)
	}
}

func TestClearPage(t *testing.T) {
	vm := NewVisibilityMap("test-bundle", 10)

	vm.SetAllVisible(3)
	if !vm.IsAllVisible(3) {
		t.Fatal("page 3 should be all-visible")
	}

	vm.ClearPage(3)
	if vm.IsAllVisible(3) {
		t.Error("page 3 should not be all-visible after ClearPage")
	}

	visCount, _ := vm.Stats()
	if visCount != 0 {
		t.Errorf("expected 0 visible pages, got %d", visCount)
	}
}

func TestClearPageIdempotent(t *testing.T) {
	vm := NewVisibilityMap("test-bundle", 10)

	// Clear an already-clear page should not decrement
	vm.ClearPage(3)
	visCount, _ := vm.Stats()
	if visCount != 0 {
		t.Errorf("expected 0 visible pages, got %d", visCount)
	}
}

func TestClearAll(t *testing.T) {
	vm := NewVisibilityMap("test-bundle", 128)

	// Set several pages
	for i := uint32(0); i < 128; i++ {
		vm.SetAllVisible(i)
	}
	visCount, _ := vm.Stats()
	if visCount != 128 {
		t.Errorf("expected 128 visible pages, got %d", visCount)
	}

	vm.ClearAll()
	visCount, _ = vm.Stats()
	if visCount != 0 {
		t.Errorf("expected 0 visible pages after ClearAll, got %d", visCount)
	}

	for i := uint32(0); i < 128; i++ {
		if vm.IsAllVisible(i) {
			t.Errorf("page %d should not be all-visible after ClearAll", i)
		}
	}
}

func TestGrow(t *testing.T) {
	vm := NewVisibilityMap("test-bundle", 10)

	// Set some pages in original range
	vm.SetAllVisible(5)
	vm.SetAllVisible(9)

	// Grow
	vm.Grow(100)

	if vm.PageCount() != 100 {
		t.Errorf("expected 100 pages after grow, got %d", vm.PageCount())
	}

	// Old bits preserved
	if !vm.IsAllVisible(5) {
		t.Error("page 5 should still be all-visible after grow")
	}
	if !vm.IsAllVisible(9) {
		t.Error("page 9 should still be all-visible after grow")
	}

	// New pages not visible
	if vm.IsAllVisible(50) {
		t.Error("page 50 should not be all-visible after grow")
	}

	// Can set new pages
	vm.SetAllVisible(99)
	if !vm.IsAllVisible(99) {
		t.Error("page 99 should be all-visible after set")
	}
}

func TestGrowShrinkNoOp(t *testing.T) {
	vm := NewVisibilityMap("test-bundle", 100)
	vm.SetAllVisible(50)
	vm.Grow(50) // smaller than current - no-op
	if vm.PageCount() != 100 {
		t.Errorf("grow with smaller count should be no-op, got %d", vm.PageCount())
	}
	if !vm.IsAllVisible(50) {
		t.Error("page 50 should still be all-visible")
	}
}

func TestOutOfBoundsAccess(t *testing.T) {
	vm := NewVisibilityMap("test-bundle", 10)

	// Out of bounds reads return false
	if vm.IsAllVisible(10) {
		t.Error("out of bounds page should return false")
	}
	if vm.IsAllVisible(1000) {
		t.Error("far out of bounds page should return false")
	}

	// Out of bounds writes are no-ops (no panic)
	vm.SetAllVisible(10)
	vm.ClearPage(10)
}

func TestGenerationIncrement(t *testing.T) {
	vm := NewVisibilityMap("test-bundle", 10)

	gen0 := vm.Generation()

	// SetAllVisible does NOT increment generation
	vm.SetAllVisible(3)
	gen1 := vm.Generation()
	if gen1 != gen0 {
		t.Error("SetAllVisible should not change generation")
	}

	// ClearPage increments generation
	vm.ClearPage(3)
	gen2 := vm.Generation()
	if gen2 <= gen1 {
		t.Errorf("ClearPage should increment generation: was %d, now %d", gen1, gen2)
	}

	// ClearPage on already-clear page does NOT increment
	vm.ClearPage(3)
	gen3 := vm.Generation()
	if gen3 != gen2 {
		t.Error("ClearPage on already-clear should not change generation")
	}

	// ClearAll increments generation
	vm.SetAllVisible(5)
	vm.ClearAll()
	gen4 := vm.Generation()
	if gen4 <= gen3 {
		t.Errorf("ClearAll should increment generation: was %d, now %d", gen3, gen4)
	}
}

func TestConcurrentSetClear(t *testing.T) {
	vm := NewVisibilityMap("test-bundle", 256)
	var wg sync.WaitGroup

	// Concurrent setters
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func(base uint32) {
			defer wg.Done()
			for j := uint32(0); j < 25; j++ {
				vm.SetAllVisible(base + j)
			}
		}(uint32(i) * 25)
	}

	// Concurrent clearers (overlapping range)
	for i := 0; i < 5; i++ {
		wg.Add(1)
		go func(base uint32) {
			defer wg.Done()
			for j := uint32(0); j < 10; j++ {
				vm.ClearPage(base + j)
			}
		}(uint32(i) * 20)
	}

	// Concurrent readers
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := uint32(0); j < 256; j++ {
				_ = vm.IsAllVisible(j)
			}
			_ = vm.Generation()
			_, _ = vm.Stats()
		}()
	}

	wg.Wait()
	// No panic or race = success (run with -race)
}

// ---- CheckPageAllVisible tests ----

func makeDoc(commitSeq uint64, createdByTx uint64, deletedByTx uint64, superseded bool) models.Document {
	doc := models.Document{
		DocumentID:     "test-doc",
		CommitSequence: commitSeq,
		CreatedByTxID:  createdByTx,
		DeletedByTxID:  deletedByTx,
	}
	if superseded {
		doc.SupersededAt = time.Now().Add(-time.Hour)
	}
	return doc
}

func TestCheckPageAllVisible_AllCommitted(t *testing.T) {
	docs := []models.Document{
		makeDoc(10, 1, 0, false),
		makeDoc(20, 2, 0, false),
		makeDoc(30, 3, 0, false),
	}
	if !CheckPageAllVisible(docs, 0) {
		t.Error("all committed docs with no active snapshots should be all-visible")
	}
	if !CheckPageAllVisible(docs, 50) {
		t.Error("all committed docs before oldest snapshot should be all-visible")
	}
}

func TestCheckPageAllVisible_OneUncommitted(t *testing.T) {
	docs := []models.Document{
		makeDoc(10, 1, 0, false),
		makeDoc(0, 5, 0, false), // uncommitted (non-legacy)
	}
	if CheckPageAllVisible(docs, 0) {
		t.Error("page with uncommitted doc should not be all-visible")
	}
}

func TestCheckPageAllVisible_OneDeleted(t *testing.T) {
	docs := []models.Document{
		makeDoc(10, 1, 0, false),
		makeDoc(20, 2, 3, false), // deleted by tx 3
	}
	if CheckPageAllVisible(docs, 0) {
		t.Error("page with deleted doc should not be all-visible")
	}
}

func TestCheckPageAllVisible_OneSuperseded(t *testing.T) {
	docs := []models.Document{
		makeDoc(10, 1, 0, false),
		makeDoc(20, 2, 0, true), // superseded
	}
	if CheckPageAllVisible(docs, 0) {
		t.Error("page with superseded doc should not be all-visible")
	}
}

func TestCheckPageAllVisible_CommitAfterSnapshot(t *testing.T) {
	docs := []models.Document{
		makeDoc(10, 1, 0, false),
		makeDoc(100, 2, 0, false), // committed after oldest snapshot
	}
	if CheckPageAllVisible(docs, 50) {
		t.Error("page with doc committed after oldest snapshot should not be all-visible")
	}
}

func TestCheckPageAllVisible_EmptyPage(t *testing.T) {
	if CheckPageAllVisible(nil, 0) {
		t.Error("nil page should not be all-visible")
	}
	if CheckPageAllVisible([]models.Document{}, 0) {
		t.Error("empty page should not be all-visible")
	}
}

func TestCheckPageAllVisible_LegacyDocs(t *testing.T) {
	// Legacy docs have CommitSequence=0 AND CreatedByTxID=0
	docs := []models.Document{
		makeDoc(0, 0, 0, false), // legacy doc
		makeDoc(10, 1, 0, false),
	}
	if !CheckPageAllVisible(docs, 0) {
		t.Error("page with legacy docs should be all-visible")
	}
}
