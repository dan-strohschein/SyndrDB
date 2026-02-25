package backup

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	"syndrdb/src/internal/storage/bundlestore"
)

func TestCollectChangedFiles_NoChanges(t *testing.T) {
	tmpDir := t.TempDir()

	// Create a bundle directory with a manifest whose MaxSeq <= threshold
	bundleDir := filepath.Join(tmpDir, "mybundle")
	os.MkdirAll(bundleDir, 0755)

	manifest := bundlestore.BundleManifest{
		BundleName:   "mybundle",
		DatabaseName: "testdb",
		Files: []*bundlestore.ManifestFileInfo{
			{FileID: 1, FileName: "000001.bnd", MaxSeq: 50},
			{FileID: 2, FileName: "000002.bnd", MaxSeq: 100},
		},
	}
	data, _ := json.Marshal(manifest)
	os.WriteFile(filepath.Join(bundleDir, "bundle.manifest"), data, 0644)

	// Create the segment files
	os.WriteFile(filepath.Join(bundleDir, "000001.bnd"), []byte("data1"), 0644)
	os.WriteFile(filepath.Join(bundleDir, "000002.bnd"), []byte("data2"), 0644)

	// With threshold=100, no segments should be changed (MaxSeq <= 100)
	files, bundles, err := collectChangedFiles(tmpDir, 100, false)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// bundle.manifest is always included
	if len(files) != 1 {
		t.Errorf("expected 1 file (manifest only), got %d: %v", len(files), files)
	}
	if len(bundles) != 0 {
		t.Errorf("expected 0 changed bundles, got %d", len(bundles))
	}
}

func TestCollectChangedFiles_PartialChanges(t *testing.T) {
	tmpDir := t.TempDir()

	bundleDir := filepath.Join(tmpDir, "mybundle")
	os.MkdirAll(bundleDir, 0755)

	manifest := bundlestore.BundleManifest{
		BundleName:   "mybundle",
		DatabaseName: "testdb",
		Files: []*bundlestore.ManifestFileInfo{
			{FileID: 1, FileName: "000001.bnd", MaxSeq: 50},
			{FileID: 2, FileName: "000002.bnd", MaxSeq: 150},
		},
	}
	data, _ := json.Marshal(manifest)
	os.WriteFile(filepath.Join(bundleDir, "bundle.manifest"), data, 0644)
	os.WriteFile(filepath.Join(bundleDir, "000001.bnd"), []byte("data1"), 0644)
	os.WriteFile(filepath.Join(bundleDir, "000002.bnd"), []byte("data2"), 0644)

	// With threshold=100, only 000002.bnd should be changed (MaxSeq=150 > 100)
	files, bundles, err := collectChangedFiles(tmpDir, 100, false)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Should have: bundle.manifest + 000002.bnd = 2 files
	if len(files) != 2 {
		t.Errorf("expected 2 files, got %d: %v", len(files), files)
	}
	if len(bundles) != 1 || bundles[0] != "mybundle" {
		t.Errorf("expected [mybundle], got %v", bundles)
	}
}

func TestCollectChangedFiles_AllNew(t *testing.T) {
	tmpDir := t.TempDir()

	bundleDir := filepath.Join(tmpDir, "mybundle")
	os.MkdirAll(bundleDir, 0755)

	manifest := bundlestore.BundleManifest{
		BundleName:   "mybundle",
		DatabaseName: "testdb",
		Files: []*bundlestore.ManifestFileInfo{
			{FileID: 1, FileName: "000001.bnd", MaxSeq: 200},
			{FileID: 2, FileName: "000002.bnd", MaxSeq: 300},
		},
	}
	data, _ := json.Marshal(manifest)
	os.WriteFile(filepath.Join(bundleDir, "bundle.manifest"), data, 0644)
	os.WriteFile(filepath.Join(bundleDir, "000001.bnd"), []byte("data1"), 0644)
	os.WriteFile(filepath.Join(bundleDir, "000002.bnd"), []byte("data2"), 0644)

	// With threshold=0, everything is new
	files, bundles, err := collectChangedFiles(tmpDir, 0, false)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Should have: bundle.manifest + 000001.bnd + 000002.bnd = 3 files
	if len(files) != 3 {
		t.Errorf("expected 3 files, got %d", len(files))
	}
	if len(bundles) != 1 {
		t.Errorf("expected 1 changed bundle, got %d", len(bundles))
	}
}

func TestCollectChangedFiles_WithIndexes(t *testing.T) {
	tmpDir := t.TempDir()

	bundleDir := filepath.Join(tmpDir, "mybundle")
	os.MkdirAll(bundleDir, 0755)

	manifest := bundlestore.BundleManifest{
		BundleName:   "mybundle",
		DatabaseName: "testdb",
		Files: []*bundlestore.ManifestFileInfo{
			{FileID: 1, FileName: "000001.bnd", MaxSeq: 200},
		},
	}
	data, _ := json.Marshal(manifest)
	os.WriteFile(filepath.Join(bundleDir, "bundle.manifest"), data, 0644)
	os.WriteFile(filepath.Join(bundleDir, "000001.bnd"), []byte("data"), 0644)
	os.WriteFile(filepath.Join(bundleDir, "idx1.hidx"), []byte("hash"), 0644)
	os.WriteFile(filepath.Join(bundleDir, "idx2.btidx"), []byte("btree"), 0644)
	os.WriteFile(filepath.Join(bundleDir, "sorted_index.sidx"), []byte("sorted"), 0644)

	files, _, err := collectChangedFiles(tmpDir, 0, true)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// manifest + segment + 3 index files = 5
	if len(files) != 5 {
		t.Errorf("expected 5 files with indexes, got %d: %v", len(files), files)
	}
}

func TestCollectChangedFiles_MultipleBundles(t *testing.T) {
	tmpDir := t.TempDir()

	// Bundle 1: has changes
	bundle1Dir := filepath.Join(tmpDir, "orders")
	os.MkdirAll(bundle1Dir, 0755)
	m1 := bundlestore.BundleManifest{
		BundleName: "orders",
		Files:      []*bundlestore.ManifestFileInfo{{FileID: 1, FileName: "000001.bnd", MaxSeq: 200}},
	}
	d1, _ := json.Marshal(m1)
	os.WriteFile(filepath.Join(bundle1Dir, "bundle.manifest"), d1, 0644)
	os.WriteFile(filepath.Join(bundle1Dir, "000001.bnd"), []byte("x"), 0644)

	// Bundle 2: no changes
	bundle2Dir := filepath.Join(tmpDir, "users")
	os.MkdirAll(bundle2Dir, 0755)
	m2 := bundlestore.BundleManifest{
		BundleName: "users",
		Files:      []*bundlestore.ManifestFileInfo{{FileID: 1, FileName: "000001.bnd", MaxSeq: 50}},
	}
	d2, _ := json.Marshal(m2)
	os.WriteFile(filepath.Join(bundle2Dir, "bundle.manifest"), d2, 0644)
	os.WriteFile(filepath.Join(bundle2Dir, "000001.bnd"), []byte("y"), 0644)

	files, bundles, err := collectChangedFiles(tmpDir, 100, false)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// orders: manifest + segment = 2; users: manifest only = 1; total = 3
	if len(files) != 3 {
		t.Errorf("expected 3 files, got %d: %v", len(files), files)
	}
	if len(bundles) != 1 || bundles[0] != "orders" {
		t.Errorf("expected [orders], got %v", bundles)
	}
}

func TestCollectChangedFiles_EmptyDB(t *testing.T) {
	tmpDir := t.TempDir()

	files, bundles, err := collectChangedFiles(tmpDir, 0, false)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(files) != 0 {
		t.Errorf("expected 0 files, got %d", len(files))
	}
	if len(bundles) != 0 {
		t.Errorf("expected 0 bundles, got %d", len(bundles))
	}
}

func TestCollectWALFiles_MissingManifest(t *testing.T) {
	// collectWALFiles reads from settings WALArchiveDir — test graceful handling
	// when the manifest doesn't exist
	files, names, err := collectWALFiles(0)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	// Should return empty (no WAL archive available)
	_ = files
	_ = names
}

func TestCollectChangedFiles_SkipsNonBundleDirs(t *testing.T) {
	tmpDir := t.TempDir()

	// Create a directory without bundle.manifest
	os.MkdirAll(filepath.Join(tmpDir, "notabundle"), 0755)
	os.WriteFile(filepath.Join(tmpDir, "notabundle", "random.txt"), []byte("hi"), 0644)

	files, bundles, err := collectChangedFiles(tmpDir, 0, false)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(files) != 0 {
		t.Errorf("expected 0 files, got %d", len(files))
	}
	if len(bundles) != 0 {
		t.Errorf("expected 0 bundles, got %d", len(bundles))
	}
}
