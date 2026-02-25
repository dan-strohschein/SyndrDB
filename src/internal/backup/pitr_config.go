package backup

import (
	"encoding/json"
	"os"
	"path/filepath"
	"time"
)

// PITRTargetType specifies the type of PITR recovery target.
type PITRTargetType int

const (
	PITRTargetTimestamp    PITRTargetType = iota + 1
	PITRTargetLSN
	PITRTargetRestorePoint
)

// PITRRecoveryTarget defines the desired recovery endpoint.
type PITRRecoveryTarget struct {
	Type             PITRTargetType
	TargetTime       time.Time // for PITRTargetTimestamp
	TargetLSN        uint64    // for PITRTargetLSN
	RestorePointName string    // for PITRTargetRestorePoint
}

// PITRRecoveryOptions configures a PITR recovery operation.
type PITRRecoveryOptions struct {
	BackupPath    string
	TargetDBName  string
	WALArchiveDir string
	Target        PITRRecoveryTarget
	Force         bool // overwrite existing DB
	DryRun        bool // validate only, don't restore
}

// PITRResult holds statistics from a PITR recovery.
type PITRResult struct {
	RestoredToLSN    uint64
	RestoredToSeq    uint64
	RestoredToTime   time.Time
	RedoneOps        int
	SkippedOps       int
	WALFilesReplayed int
}

// WALArchiveEntry mirrors journal.WALArchiveFileEntry for cross-package use.
type WALArchiveEntry struct {
	FileName       string    `json:"file_name"`
	FirstLSN       uint64    `json:"first_lsn"`
	LastLSN        uint64    `json:"last_lsn"`
	FirstTimestamp time.Time `json:"first_timestamp"`
	LastTimestamp   time.Time `json:"last_timestamp"`
	SizeBytes      int64     `json:"size_bytes"`
	Compressed     bool      `json:"compressed,omitempty"`
	ArchivedAt     time.Time `json:"archived_at"`
}

// WALArchiveBackend provides read access to WAL archive manifests from the backup package.
type WALArchiveBackend struct {
	archiveDir string
}

// NewWALArchiveBackendFromDir creates a backend for reading WAL archive manifests.
func NewWALArchiveBackendFromDir(archiveDir string) (*WALArchiveBackend, error) {
	return &WALArchiveBackend{archiveDir: archiveDir}, nil
}

// List reads the WAL archive manifest and returns all entries.
func (b *WALArchiveBackend) List() ([]WALArchiveEntry, error) {
	manifest, err := readWALArchiveManifest(b.archiveDir)
	if err != nil {
		return nil, err
	}
	return manifest, nil
}

// walArchiveManifestFile is the structure of the WAL archive manifest JSON file.
type walArchiveManifestFile struct {
	Files []WALArchiveEntry `json:"files"`
}

// readWALArchiveManifest reads the wal_archive.manifest file from the archive directory.
func readWALArchiveManifest(archiveDir string) ([]WALArchiveEntry, error) {
	manifestPath := filepath.Join(archiveDir, "wal_archive.manifest")
	data, err := os.ReadFile(manifestPath)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, err
	}

	var manifest walArchiveManifestFile
	if err := json.Unmarshal(data, &manifest); err != nil {
		return nil, err
	}

	return manifest.Files, nil
}
