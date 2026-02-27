package journal

import (
	"path/filepath"
	"testing"
)

func TestRestorePointManager_CreateAndList(t *testing.T) {
	dir := t.TempDir()
	rpm := NewRestorePointManager(filepath.Join(dir, "restore_points.json"))

	rp, err := rpm.Create("before_migration", 100, 50)
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	if rp.Name != "before_migration" {
		t.Errorf("Expected name 'before_migration', got '%s'", rp.Name)
	}
	if rp.LSN != 100 || rp.CommitSeq != 50 {
		t.Errorf("LSN/CommitSeq mismatch: %d/%d", rp.LSN, rp.CommitSeq)
	}

	points := rpm.List()
	if len(points) != 1 {
		t.Fatalf("Expected 1 point, got %d", len(points))
	}
}

func TestRestorePointManager_DuplicateName(t *testing.T) {
	dir := t.TempDir()
	rpm := NewRestorePointManager(filepath.Join(dir, "restore_points.json"))

	if _, err := rpm.Create("rp1", 100, 50); err != nil {
		t.Fatalf("Create: %v", err)
	}

	_, err := rpm.Create("rp1", 200, 60)
	if err == nil {
		t.Fatal("Expected error for duplicate name, got nil")
	}
}

func TestRestorePointManager_Get(t *testing.T) {
	dir := t.TempDir()
	rpm := NewRestorePointManager(filepath.Join(dir, "restore_points.json"))

	rpm.Create("rp1", 100, 50)
	rpm.Create("rp2", 200, 60)

	rp := rpm.Get("rp2")
	if rp == nil {
		t.Fatal("Expected to find rp2")
	}
	if rp.LSN != 200 {
		t.Errorf("Expected LSN 200, got %d", rp.LSN)
	}

	rp = rpm.Get("nonexistent")
	if rp != nil {
		t.Error("Expected nil for nonexistent restore point")
	}
}

func TestRestorePointManager_Delete(t *testing.T) {
	dir := t.TempDir()
	rpm := NewRestorePointManager(filepath.Join(dir, "restore_points.json"))

	rpm.Create("rp1", 100, 50)
	rpm.Create("rp2", 200, 60)

	if err := rpm.Delete("rp1"); err != nil {
		t.Fatalf("Delete: %v", err)
	}

	points := rpm.List()
	if len(points) != 1 {
		t.Fatalf("Expected 1 point after delete, got %d", len(points))
	}
	if points[0].Name != "rp2" {
		t.Errorf("Expected 'rp2' to remain, got '%s'", points[0].Name)
	}

	err := rpm.Delete("nonexistent")
	if err == nil {
		t.Error("Expected error deleting nonexistent point")
	}
}

func TestRestorePointManager_PersistAndLoad(t *testing.T) {
	dir := t.TempDir()
	filePath := filepath.Join(dir, "restore_points.json")

	rpm1 := NewRestorePointManager(filePath)
	rpm1.Create("rp1", 100, 50)
	rpm1.Create("rp2", 200, 60)

	// Load into a new manager
	rpm2 := NewRestorePointManager(filePath)

	points := rpm2.List()
	if len(points) != 2 {
		t.Fatalf("Expected 2 points after reload, got %d", len(points))
	}
	if points[0].Name != "rp1" || points[1].Name != "rp2" {
		t.Errorf("Unexpected point names: %s, %s", points[0].Name, points[1].Name)
	}
}

func TestRestorePointManager_EmptyFile(t *testing.T) {
	dir := t.TempDir()
	rpm := NewRestorePointManager(filepath.Join(dir, "nonexistent.json"))

	points := rpm.List()
	if len(points) != 0 {
		t.Errorf("Expected 0 points from nonexistent file, got %d", len(points))
	}
}
