package bundle

/*
INDEX CLEANUP - STARTUP, SHUTDOWN, AND DROP INDEX FILE CLEANUP

Removes orphaned and temporary index files to limit disk usage:
- Temp/pattern-based: *.tmp, *.idx.tmp, *.compact.tmp under any .../indexes
- Schema-based orphans: .hidx and .btidx that no longer exist in bundle.Indexes

RunIndexCleanup: on startup (temp + schema-based). Requires loaded databases.
DeleteIndexFiles: when an index is dropped (UPDATE BUNDLE / DROP INDEX).
CleanupIndexTempFiles: temp-only, e.g. on shutdown (optional).
*/

import (
	"os"
	"path/filepath"
	"strings"

	"syndrdb/src/internal/domain/models"
	hashindex "syndrdb/src/internal/domain/index/hashindexV3"

	"go.uber.org/zap"
)

// RunIndexCleanup runs temp and schema-based cleanup under dataDir.
// Temp: removes *.tmp, *.idx.tmp, *.compact.tmp under any .../indexes or .../indexes/btree.
// Schema-based: removes .hidx and .btidx that are not in bundle.Indexes for each db/bundle.
// databases: from DatabaseService.Databases (can be nil or empty; then only temp runs).
func RunIndexCleanup(dataDir string, databases map[string]*models.Database, logger *zap.SugaredLogger) error {
	if logger == nil {
		return nil
	}
	// 1) Temp/pattern-based (no schema needed)
	CleanupIndexTempFiles(dataDir, logger)

	// 2) Schema-based orphan cleanup
	if dataDir == "" || len(databases) == 0 {
		return nil
	}

	fnh := hashindex.NewFileNamingHelper("", "")

	for _, db := range databases {
		if db == nil || db.Bundles == nil {
			continue
		}
		for _, b := range db.Bundles { //nolint:copylocks // only reading Indexes and Name
			if b.Indexes == nil {
				continue
			}

			validHash := make(map[string]bool)
			validBtree := make(map[string]bool)
			for _, ir := range b.Indexes {
				switch ir.IndexType {
				case "hash":
					validHash[ir.IndexName] = true
				case "btree":
					validBtree[ir.IndexName] = true
				}
			}

			indexesPath := filepath.Join(dataDir, db.Name, b.Name, "indexes")
			// Hash and top-level temp already handled by CleanupIndexTempFiles; here only orphans
			entries, err := os.ReadDir(indexesPath)
			if err != nil {
				if os.IsNotExist(err) {
					continue
				}
				logger.Warnw("index cleanup: cannot read indexes dir", "path", indexesPath, "error", err)
				continue
			}

			for _, e := range entries {
				if e.IsDir() {
					if e.Name() == "btree" {
						btreePath := filepath.Join(indexesPath, "btree")
						btreeEntries, err := os.ReadDir(btreePath)
						if err != nil {
							continue
						}
						for _, be := range btreeEntries {
							if be.IsDir() {
								continue
							}
							name := be.Name()
							if !strings.HasSuffix(name, ".btidx") {
								continue
							}
							base := strings.TrimSuffix(name, ".btidx")
							if !validBtree[base] {
								fp := filepath.Join(btreePath, name)
								if err := os.Remove(fp); err != nil {
									logger.Warnw("index cleanup: failed to remove orphan btree index", "path", fp, "error", err)
								} else {
									logger.Infow("index cleanup: removed orphan btree index", "path", fp)
								}
							}
						}
					}
					continue
				}

				name := e.Name()
				if !strings.HasSuffix(name, hashindex.IndexFileExtension) {
					continue
				}

				// Try legacy then bucket parsing
				base := ""
				parsed := fnh.ParseIndexFileName(name)
				if parsed.IsValid {
					if parsed.IsForeignKey {
						base = parsed.FieldName + hashindex.ForeignKeySuffix
					} else {
						base = parsed.FieldName
					}
				} else {
					bp := fnh.ParseBucketIndexFileName(name)
					if bp.IsValid {
						if bp.IsForeignKey {
							base = bp.FieldName + hashindex.ForeignKeySuffix
						} else {
							base = bp.FieldName
						}
					}
				}
				if base != "" && !validHash[base] {
					fp := filepath.Join(indexesPath, name)
					if err := os.Remove(fp); err != nil {
						logger.Warnw("index cleanup: failed to remove orphan hash index", "path", fp, "error", err)
					} else {
						logger.Infow("index cleanup: removed orphan hash index", "path", fp)
					}
				}
			}
		}
	}

	return nil
}

// DeleteIndexFiles removes all on-disk files for a single dropped index.
// bundleIndexesPath: data_files/<database>/<bundle>/indexes.
// indexName: IndexReference.IndexName (e.g. "DocumentID", "UserID_fk", or btree index name).
// indexType: "hash" or "btree".
func DeleteIndexFiles(bundleIndexesPath string, indexName string, indexType string, logger *zap.SugaredLogger) error {
	if bundleIndexesPath == "" || indexName == "" || indexType == "" {
		return nil
	}
	if logger == nil {
		logger = zap.NewNop().Sugar()
	}

	switch indexType {
	case "btree":
		fp := filepath.Join(bundleIndexesPath, "btree", indexName+".btidx")
		if err := os.Remove(fp); err != nil && !os.IsNotExist(err) {
			logger.Warnw("DeleteIndexFiles: failed to remove btree index", "path", fp, "error", err)
			return err
		}
		logger.Infow("DeleteIndexFiles: removed btree index file", "path", fp)
		return nil

	case "hash":
		entries, err := os.ReadDir(bundleIndexesPath)
		if err != nil {
			if os.IsNotExist(err) {
				return nil
			}
			return err
		}

		fnh := hashindex.NewFileNamingHelper("", "")
		removed := 0

		for _, e := range entries {
			if e.IsDir() {
				continue
			}
			name := e.Name()
			if !strings.HasSuffix(name, hashindex.IndexFileExtension) {
				continue
			}

			base := ""
			parsed := fnh.ParseIndexFileName(name)
			if parsed.IsValid {
				if parsed.IsForeignKey {
					base = parsed.FieldName + hashindex.ForeignKeySuffix
				} else {
					base = parsed.FieldName
				}
			} else {
				bp := fnh.ParseBucketIndexFileName(name)
				if bp.IsValid {
					if bp.IsForeignKey {
						base = bp.FieldName + hashindex.ForeignKeySuffix
					} else {
						base = bp.FieldName
					}
				}
			}
			if base == indexName {
				fp := filepath.Join(bundleIndexesPath, name)
				if err := os.Remove(fp); err != nil {
					logger.Warnw("DeleteIndexFiles: failed to remove hash index file", "path", fp, "error", err)
				} else {
					removed++
				}
			}
		}
		if removed > 0 {
			logger.Infow("DeleteIndexFiles: removed hash index files", "indexName", indexName, "count", removed)
		}
		return nil
	}

	return nil
}

// CleanupIndexTempFiles removes *.tmp, *.idx.tmp, *.compact.tmp under any
// .../indexes or .../indexes/btree under dataDir. Safe to call at any time.
func CleanupIndexTempFiles(dataDir string, logger *zap.SugaredLogger) {
	if dataDir == "" {
		return
	}

	isTemp := func(name string) bool {
		return strings.HasSuffix(name, ".tmp") ||
			strings.HasSuffix(name, ".idx.tmp") ||
			strings.HasSuffix(name, ".compact.tmp")
	}

	var count int
	_ = filepath.WalkDir(dataDir, func(path string, d os.DirEntry, err error) error {
		if err != nil {
			if os.IsNotExist(err) {
				return nil
			}
			return nil
		}
		if d.IsDir() {
			return nil
		}
		dir, name := filepath.Split(path)
		dir = filepath.Clean(dir)
		base := filepath.Base(dir)
		// Only under .../indexes or .../indexes/btree
		if base == "indexes" {
			// file in .../indexes/<name>
		} else if base == "btree" && filepath.Base(filepath.Dir(dir)) == "indexes" {
			// file in .../indexes/btree/<name>
		} else {
			return nil
		}
		if !isTemp(name) {
			return nil
		}
		if err := os.Remove(path); err != nil {
			if logger != nil {
				logger.Warnw("CleanupIndexTempFiles: failed to remove", "path", path, "error", err)
			}
		} else {
			count++
			if logger != nil {
				logger.Infow("CleanupIndexTempFiles: removed", "path", path)
			}
		}
		return nil
	})

	if count > 0 && logger != nil {
		logger.Infow("CleanupIndexTempFiles: done", "removed", count)
	}
}
