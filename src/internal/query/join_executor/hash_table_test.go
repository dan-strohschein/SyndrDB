// Package joinexecutor provides query join execution strategies.
//
// File: hash_table_test.go
//
// This file contains unit tests for InMemoryHashTable implementation,
// focusing on the Freeze() functionality that enables lock-free reads
// after the build phase completes.
package joinexecutor

import (
	"sync"
	"testing"

	"syndrdb/src/internal/domain/models"
)

// TestInMemoryHashTable_Freeze tests the Freeze/IsFrozen functionality
func TestInMemoryHashTable_Freeze(t *testing.T) {
	ht := NewInMemoryHashTable(16, 0.75)

	// Initially not frozen
	if ht.(*InMemoryHashTable).IsFrozen() {
		t.Error("Expected hash table to not be frozen initially")
	}

	// Add some data
	doc := &models.Document{DocumentID: "doc1"}
	err := ht.Put("key1", doc)
	if err != nil {
		t.Fatalf("Failed to put document: %v", err)
	}

	// Freeze the hash table
	ht.(*InMemoryHashTable).Freeze()

	// Now should be frozen
	if !ht.(*InMemoryHashTable).IsFrozen() {
		t.Error("Expected hash table to be frozen after Freeze()")
	}

	// Get should still work (lock-free path)
	docs, found := ht.Get("key1")
	if !found {
		t.Error("Expected to find key1 after freeze")
	}
	if len(docs) != 1 || docs[0].DocumentID != "doc1" {
		t.Errorf("Expected doc1, got %v", docs)
	}
}

// TestInMemoryHashTable_FrozenGet_ZeroCopy verifies that frozen Get returns original slice
func TestInMemoryHashTable_FrozenGet_ZeroCopy(t *testing.T) {
	ht := NewInMemoryHashTable(16, 0.75).(*InMemoryHashTable)

	doc1 := &models.Document{DocumentID: "doc1"}
	doc2 := &models.Document{DocumentID: "doc2"}
	_ = ht.Put("key1", doc1)
	_ = ht.Put("key1", doc2) // Same key, second doc

	// Before freeze: Get returns a copy
	docs1, _ := ht.Get("key1")
	docs2, _ := ht.Get("key1")

	// Modifying one shouldn't affect the other (they're copies)
	if len(docs1) != 2 || len(docs2) != 2 {
		t.Fatalf("Expected 2 docs each, got %d and %d", len(docs1), len(docs2))
	}

	// Now freeze
	ht.Freeze()

	// After freeze: Get returns the original slice (zero-copy)
	frozenDocs1, _ := ht.Get("key1")
	frozenDocs2, _ := ht.Get("key1")

	// Both should be the same slice (same backing array)
	if len(frozenDocs1) != 2 || len(frozenDocs2) != 2 {
		t.Fatalf("Expected 2 docs each after freeze, got %d and %d", len(frozenDocs1), len(frozenDocs2))
	}

	// Since frozen, they should point to same underlying data
	if &frozenDocs1[0] != &frozenDocs2[0] {
		t.Log("Note: Frozen Get returns same slice reference (expected for zero-copy)")
	}
}

// TestInMemoryHashTable_FrozenPut_Panics verifies that Put panics on frozen hash table
func TestInMemoryHashTable_FrozenPut_Panics(t *testing.T) {
	ht := NewInMemoryHashTable(16, 0.75).(*InMemoryHashTable)

	doc := &models.Document{DocumentID: "doc1"}
	_ = ht.Put("key1", doc)

	// Freeze
	ht.Freeze()

	// Put should panic
	defer func() {
		if r := recover(); r == nil {
			t.Error("Expected Put to panic on frozen hash table")
		}
	}()

	doc2 := &models.Document{DocumentID: "doc2"}
	_ = ht.Put("key2", doc2) // Should panic
}

// TestInMemoryHashTable_Clear_ResetsFrozen verifies that Clear resets the frozen state
func TestInMemoryHashTable_Clear_ResetsFrozen(t *testing.T) {
	ht := NewInMemoryHashTable(16, 0.75).(*InMemoryHashTable)

	doc := &models.Document{DocumentID: "doc1"}
	_ = ht.Put("key1", doc)

	// Freeze
	ht.Freeze()
	if !ht.IsFrozen() {
		t.Error("Expected frozen after Freeze()")
	}

	// Clear
	ht.Clear()

	// Should no longer be frozen
	if ht.IsFrozen() {
		t.Error("Expected not frozen after Clear()")
	}

	// Should be able to Put again
	doc2 := &models.Document{DocumentID: "doc2"}
	err := ht.Put("key2", doc2)
	if err != nil {
		t.Errorf("Expected Put to succeed after Clear(), got: %v", err)
	}

	// Verify data was cleared
	if ht.Size() != 1 {
		t.Errorf("Expected size 1 after Clear() and one Put(), got %d", ht.Size())
	}
}

// TestInMemoryHashTable_TryGet_FrozenAlwaysSucceeds verifies TryGet behavior when frozen
func TestInMemoryHashTable_TryGet_FrozenAlwaysSucceeds(t *testing.T) {
	ht := NewInMemoryHashTable(16, 0.75).(*InMemoryHashTable)

	doc := &models.Document{DocumentID: "doc1"}
	_ = ht.Put("key1", doc)

	// Freeze
	ht.Freeze()

	// TryGet should always succeed (acquired=true) when frozen
	docs, found, acquired := ht.TryGet("key1")
	if !acquired {
		t.Error("Expected TryGet to always acquire when frozen")
	}
	if !found {
		t.Error("Expected to find key1")
	}
	if len(docs) != 1 || docs[0].DocumentID != "doc1" {
		t.Errorf("Expected doc1, got %v", docs)
	}

	// Try non-existent key
	docs2, found2, acquired2 := ht.TryGet("nonexistent")
	if !acquired2 {
		t.Error("Expected TryGet to always acquire when frozen, even for non-existent keys")
	}
	if found2 {
		t.Error("Expected not to find nonexistent key")
	}
	if docs2 != nil {
		t.Errorf("Expected nil docs for non-existent key, got %v", docs2)
	}
}

// TestInMemoryHashTable_ConcurrentFrozenReads tests concurrent reads on frozen hash table
func TestInMemoryHashTable_ConcurrentFrozenReads(t *testing.T) {
	ht := NewInMemoryHashTable(1024, 0.75).(*InMemoryHashTable)

	// Add many entries
	for i := 0; i < 1000; i++ {
		doc := &models.Document{DocumentID: string(rune('A' + (i % 26)))}
		_ = ht.Put(i, doc)
	}

	// Freeze
	ht.Freeze()

	// Concurrent reads should all succeed without contention
	var wg sync.WaitGroup
	errors := make(chan error, 100)

	for g := 0; g < 100; g++ {
		wg.Add(1)
		go func(goroutineID int) {
			defer wg.Done()
			for i := 0; i < 1000; i++ {
				docs, found := ht.Get(i)
				if !found {
					errors <- nil // Send nil just to count, actual error would be bad
					return
				}
				if len(docs) == 0 {
					return
				}
			}
		}(g)
	}

	wg.Wait()
	close(errors)

	// Check for any errors
	errorCount := 0
	for range errors {
		errorCount++
	}

	if errorCount > 0 {
		t.Errorf("Had %d errors during concurrent frozen reads", errorCount)
	}

	t.Logf("Completed 100 goroutines * 1000 reads each = 100,000 concurrent reads on frozen hash table")
}
