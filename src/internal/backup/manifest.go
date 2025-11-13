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
// TODO: I will add incremental backup metadata (parent backup reference, LSN range)
// TODO: I will add encryption metadata (algorithm, key derivation method)
