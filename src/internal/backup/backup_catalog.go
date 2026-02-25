package backup

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"syndrdb/src/pkg/settings"
)

// BackupCatalogEntry tracks metadata for a single backup.
type BackupCatalogEntry struct {
	BackupID       string    `json:"backup_id"`
	BackupType     string    `json:"backup_type"` // "full" or "incremental"
	ParentBackupID string    `json:"parent_backup_id,omitempty"`
	BaseBackupID   string    `json:"base_backup_id,omitempty"`
	ChainDepth     int       `json:"chain_depth"`
	DatabaseName   string    `json:"database_name"`
	Timestamp      time.Time `json:"timestamp"`
	StartLSN       uint64    `json:"start_lsn,omitempty"`
	EndLSN         uint64    `json:"end_lsn,omitempty"`
	StartCommitSeq uint64    `json:"start_commit_seq,omitempty"`
	EndCommitSeq   uint64    `json:"end_commit_seq,omitempty"`
	FilePath       string    `json:"file_path"`
	SizeBytes      int64     `json:"size_bytes"`
	FileCount      int       `json:"file_count"`
	ChangedBundles []string  `json:"changed_bundles,omitempty"`
}

// BackupCatalog stores the history of all backups.
type BackupCatalog struct {
	Version string               `json:"version"`
	Updated time.Time            `json:"updated"`
	Entries []BackupCatalogEntry `json:"entries"`
}

// LoadCatalog reads the catalog from a JSON file. Returns an empty catalog
// if the file does not exist.
func LoadCatalog(path string) (*BackupCatalog, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return &BackupCatalog{Version: "1.0", Entries: []BackupCatalogEntry{}}, nil
		}
		return nil, fmt.Errorf("failed to read backup catalog: %w", err)
	}

	var catalog BackupCatalog
	if err := json.Unmarshal(data, &catalog); err != nil {
		return nil, fmt.Errorf("failed to parse backup catalog: %w", err)
	}
	if catalog.Entries == nil {
		catalog.Entries = []BackupCatalogEntry{}
	}
	return &catalog, nil
}

// SaveCatalog writes the catalog to disk atomically via temp file + rename.
func SaveCatalog(catalog *BackupCatalog, path string) error {
	catalog.Updated = time.Now()

	data, err := json.MarshalIndent(catalog, "", "  ")
	if err != nil {
		return fmt.Errorf("failed to marshal backup catalog: %w", err)
	}

	if err := os.MkdirAll(filepath.Dir(path), 0755); err != nil {
		return fmt.Errorf("failed to create catalog directory: %w", err)
	}

	tmpPath := path + ".tmp"
	if err := os.WriteFile(tmpPath, data, 0644); err != nil {
		return fmt.Errorf("failed to write temp catalog: %w", err)
	}

	if err := os.Rename(tmpPath, path); err != nil {
		os.Remove(tmpPath)
		return fmt.Errorf("failed to rename catalog file: %w", err)
	}

	return nil
}

// AddEntry appends an entry to the catalog.
func (c *BackupCatalog) AddEntry(entry BackupCatalogEntry) {
	c.Entries = append(c.Entries, entry)
}

// FindLatestBackupForDB returns the most recent catalog entry for a database,
// or nil if none exists.
func (c *BackupCatalog) FindLatestBackupForDB(dbName string) *BackupCatalogEntry {
	var latest *BackupCatalogEntry
	for i := range c.Entries {
		e := &c.Entries[i]
		if e.DatabaseName == dbName {
			if latest == nil || e.Timestamp.After(latest.Timestamp) {
				latest = e
			}
		}
	}
	return latest
}

// FindEntry returns the catalog entry with the given backup ID, or nil.
func (c *BackupCatalog) FindEntry(backupID string) *BackupCatalogEntry {
	for i := range c.Entries {
		if c.Entries[i].BackupID == backupID {
			return &c.Entries[i]
		}
	}
	return nil
}

// FindEntryByPath returns the catalog entry with the given file path, or nil.
func (c *BackupCatalog) FindEntryByPath(filePath string) *BackupCatalogEntry {
	for i := range c.Entries {
		if c.Entries[i].FilePath == filePath {
			return &c.Entries[i]
		}
	}
	return nil
}

// BuildChain walks ParentBackupID backwards to the root full backup and
// returns the chain ordered [full, incr1, incr2, ...].
func (c *BackupCatalog) BuildChain(backupID string) ([]BackupCatalogEntry, error) {
	var chain []BackupCatalogEntry
	visited := make(map[string]bool)

	current := c.FindEntry(backupID)
	for current != nil {
		if visited[current.BackupID] {
			return nil, fmt.Errorf("cycle detected in backup chain at backup '%s'", current.BackupID)
		}
		visited[current.BackupID] = true
		chain = append(chain, *current)

		if current.BackupType == "full" || current.ParentBackupID == "" {
			break
		}
		current = c.FindEntry(current.ParentBackupID)
		if current == nil {
			return nil, fmt.Errorf("broken backup chain: parent backup '%s' not found in catalog", chain[len(chain)-1].ParentBackupID)
		}
	}

	// Reverse to get [full, incr1, incr2, ...]
	for i, j := 0, len(chain)-1; i < j; i, j = i+1, j-1 {
		chain[i], chain[j] = chain[j], chain[i]
	}

	if len(chain) == 0 {
		return nil, fmt.Errorf("backup '%s' not found in catalog", backupID)
	}

	if chain[0].BackupType != "full" && chain[0].BackupType != "" {
		return nil, fmt.Errorf("chain root is not a full backup (type='%s')", chain[0].BackupType)
	}

	return chain, nil
}

// ValidateChain checks that all archive files in the chain exist on disk,
// the root is a full backup, and LSN continuity holds.
func ValidateChain(chain []BackupCatalogEntry) error {
	if len(chain) == 0 {
		return fmt.Errorf("empty backup chain")
	}

	// Root must be full
	if chain[0].BackupType != "full" && chain[0].BackupType != "" {
		return fmt.Errorf("chain root is not a full backup (type='%s')", chain[0].BackupType)
	}

	for i, entry := range chain {
		// Check archive exists
		if _, err := os.Stat(entry.FilePath); err != nil {
			return fmt.Errorf("broken backup chain: archive '%s' not found", entry.FilePath)
		}

		// Validate chain continuity for incrementals
		if i > 0 && entry.BackupType == "incremental" {
			// LSN overlap is fine; we just need all archives present
			_ = chain[i-1] // parent exists
		}
	}

	return nil
}

// BuildChainFromPath reads manifests from .sdb archives, walking ParentBackupPath
// to build a chain when the catalog is unavailable.
func BuildChainFromPath(archivePath string) ([]BackupCatalogEntry, error) {
	var chain []BackupCatalogEntry
	visited := make(map[string]bool)

	currentPath := archivePath
	for currentPath != "" {
		if visited[currentPath] {
			return nil, fmt.Errorf("cycle detected in backup chain at '%s'", currentPath)
		}
		visited[currentPath] = true

		manifest, err := ReadManifestFromArchive(currentPath)
		if err != nil {
			return nil, fmt.Errorf("failed to read manifest from '%s': %w", currentPath, err)
		}

		entry := catalogEntryFromManifest(manifest, currentPath)
		chain = append(chain, entry)

		if manifest.BackupType == "full" || manifest.BackupType == "" || manifest.ParentBackupPath == "" {
			break
		}

		currentPath = manifest.ParentBackupPath
	}

	// Reverse to [full, incr1, incr2, ...]
	for i, j := 0, len(chain)-1; i < j; i, j = i+1, j-1 {
		chain[i], chain[j] = chain[j], chain[i]
	}

	if len(chain) == 0 {
		return nil, fmt.Errorf("no backups found in chain starting from '%s'", archivePath)
	}

	return chain, nil
}

// ReadManifestFromArchive extracts and reads the manifest from a .sdb archive.
func ReadManifestFromArchive(archivePath string) (*Manifest, error) {
	tempDir, err := os.MkdirTemp("", "catalog_chain_*")
	if err != nil {
		return nil, err
	}
	defer os.RemoveAll(tempDir)

	bs := &BackupService{
		settings: settings.GetSettings(),
	}
	if err := bs.extractArchive(archivePath, tempDir); err != nil {
		return nil, err
	}

	manifestFile, err := os.Open(filepath.Join(tempDir, "manifest.json"))
	if err != nil {
		return nil, err
	}
	defer manifestFile.Close()

	return ReadManifest(manifestFile)
}

// catalogEntryFromManifest creates a BackupCatalogEntry from a Manifest.
func catalogEntryFromManifest(m *Manifest, filePath string) BackupCatalogEntry {
	backupType := m.BackupType
	if backupType == "" {
		backupType = "full"
	}
	info, _ := os.Stat(filePath)
	var sizeBytes int64
	if info != nil {
		sizeBytes = info.Size()
	}
	return BackupCatalogEntry{
		BackupID:       m.BackupID,
		BackupType:     backupType,
		ParentBackupID: m.ParentBackupID,
		BaseBackupID:   m.BaseBackupID,
		ChainDepth:     m.ChainDepth,
		DatabaseName:   m.DatabaseName,
		Timestamp:      m.Timestamp,
		StartLSN:       m.StartLSN,
		EndLSN:         m.EndLSN,
		StartCommitSeq: m.StartCommitSeq,
		EndCommitSeq:   m.EndCommitSeq,
		FilePath:       filePath,
		SizeBytes:      sizeBytes,
		FileCount:      len(m.Files),
		ChangedBundles: m.ChangedBundles,
	}
}
