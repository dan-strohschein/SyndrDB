/*
Package backup provides manifest management for database backups.

The manifest is a JSON file included in every backup archive that contains:
- Backup metadata (version, timestamp, database name, server version)
- List of all files with sizes and CRC32 checksums
- Primary database documents (Databases and Bundles catalog entries)
- Compression format used

The manifest enables:
- Backup validation (verify file integrity via CRC)
- Compatibility checking (server version compatibility)
- Selective restore (future: restore specific bundles)
- Backup inspection without full extraction
*/

package backup

import (
	"encoding/json"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"time"
)

// Manifest contains metadata about a backup
type Manifest struct {
	BackupVersion      string         `json:"backup_version"`       // Backup format version
	Timestamp          time.Time      `json:"timestamp"`            // When backup was created
	DatabaseName       string         `json:"database_name"`        // Name of backed up database
	ServerVersion      string         `json:"server_version"`       // SyndrDB version that created backup
	Compression        string         `json:"compression"`          // "gzip", "zstd", or "none"
	IncludesIndexes    bool           `json:"includes_indexes"`     // Whether indexes are included
	Files              []FileEntry    `json:"files"`                // List of files in backup
	PrimaryDBDocuments []PrimaryDBDoc `json:"primary_db_documents"` // Metadata from Primary database
	TotalSizeBytes     int64          `json:"total_size_bytes"`     // Total size of all files
	CompressedSize     int64          `json:"compressed_size"`      // Size of compressed archive

	// PITR (Point-in-Time Recovery) fields — v1.1+
	StartLSN       uint64   `json:"start_lsn,omitempty"`         // WAL LSN before checkpoint
	EndLSN         uint64   `json:"end_lsn,omitempty"`           // WAL LSN after checkpoint
	StartCommitSeq uint64   `json:"start_commit_seq,omitempty"`  // Commit sequence before checkpoint
	EndCommitSeq   uint64   `json:"end_commit_seq,omitempty"`    // Commit sequence after checkpoint
	PITREnabled    bool     `json:"pitr_enabled,omitempty"`      // Whether this backup supports PITR
	WALSegments    []string `json:"wal_segments_needed,omitempty"` // Informational: WAL segments needed for replay

	// Incremental backup fields — v2.0+
	BackupID       string   `json:"backup_id,omitempty"`
	BackupType     string   `json:"backup_type,omitempty"`        // "full" or "incremental"
	ParentBackupID string   `json:"parent_backup_id,omitempty"`
	ParentBackupPath string `json:"parent_backup_path,omitempty"`
	BaseBackupID   string   `json:"base_backup_id,omitempty"`     // Root full backup UUID
	ChainDepth     int      `json:"chain_depth,omitempty"`        // 0=full, 1=first incr, ...
	ChangedBundles []string `json:"changed_bundles,omitempty"`
	WALFiles       []string `json:"wal_files,omitempty"`

	// Encryption fields (v3.0+ — Enterprise)
	EncryptionEnabled bool   `json:"encryption_enabled,omitempty"`
	EncryptionAlgo    string `json:"encryption_algorithm,omitempty"`
	WrappedDEK        string `json:"wrapped_dek,omitempty"`  // base64-encoded wrapped DEK
	KeyID             string `json:"key_id,omitempty"`       // MK identifier for unwrapping
}

// FileEntry represents a single file in the backup
type FileEntry struct {
	Path           string `json:"path"`            // Relative path within backup
	SizeBytes      int64  `json:"size_bytes"`      // Original file size in bytes
	CRC32          uint32 `json:"crc32"`           // CRC32 checksum for validation
	CompressedSize int64  `json:"compressed_size"` // Size after compression (0 if not compressed)
}

// PrimaryDBDoc represents a document from the Primary database
// This is the metadata catalog that tracks all databases and bundles
type PrimaryDBDoc struct {
	Bundle     string                 `json:"bundle"`      // "Databases" or "Bundles"
	DocumentID string                 `json:"document_id"` // Document UUID
	Data       map[string]interface{} `json:"data"`        // Document fields
}

// WriteManifest serializes and writes the manifest to a writer
func WriteManifest(manifest *Manifest, writer io.Writer) error {
	encoder := json.NewEncoder(writer)
	encoder.SetIndent("", "  ") // Pretty-print for readability
	return encoder.Encode(manifest)
}

// ReadManifest reads and deserializes a manifest from a reader
func ReadManifest(reader io.Reader) (*Manifest, error) {
	var manifest Manifest
	decoder := json.NewDecoder(reader)
	if err := decoder.Decode(&manifest); err != nil {
		return nil, err
	}
	return &manifest, nil
}

// CalculateFileCRC calculates the CRC32 checksum of a file
func CalculateFileCRC(filePath string) (uint32, error) {
	file, err := os.Open(filePath)
	if err != nil {
		return 0, err
	}
	defer file.Close()

	hash := crc32.NewIEEE()
	if _, err := io.Copy(hash, file); err != nil {
		return 0, err
	}

	return hash.Sum32(), nil
}

// VerifyFileCRC verifies that a file matches the expected CRC32 checksum
func VerifyFileCRC(filePath string, expectedCRC uint32) error {
	actualCRC, err := CalculateFileCRC(filePath)
	if err != nil {
		return err
	}

	if actualCRC != expectedCRC {
		return fmt.Errorf("CRC32 checksum mismatch for file %s: expected %d, got %d - file may be corrupted", filePath, expectedCRC, actualCRC)
	}

	return nil
}

// TODO: I will add schema versioning to support backward compatibility
// TODO: I will add custom metadata fields for user-defined backup annotations
