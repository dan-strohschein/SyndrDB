package bundlestore

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"syndrdb/src/pkg/common/helpers"
)

/*
FILE PATH RESOLVER

Centralizes all file path construction for bundle storage.
Implements the directory structure: data_files/<database>/<bundle>/<bundle>_<fileID>.bnd

This follows the DRY principle by providing a single source of truth for all file paths,
and the Open/Closed principle by allowing path extension without modifying callers.
*/

// FilePathResolver handles file path construction for bundle storage
type FilePathResolver struct {
	dataDir string
}

// NewFilePathResolver creates a new file path resolver
func NewFilePathResolver(dataDir string) *FilePathResolver {
	return &FilePathResolver{
		dataDir: dataDir,
	}
}

// GetBundleDirectory returns the directory path for a bundle
// Format: data_files/<database>/<bundle>/
func (fpr *FilePathResolver) GetBundleDirectory(databaseName, bundleName string) string {
	return filepath.Join(fpr.dataDir, databaseName, bundleName)
}

// GetBundleFilePath returns the full path for a specific bundle file
// Format: data_files/<database>/<bundle>/<bundle>_<fileID>.bnd
func (fpr *FilePathResolver) GetBundleFilePath(databaseName, bundleName string, fileID int) string {
	bundleDir := fpr.GetBundleDirectory(databaseName, bundleName)
	fileName := fmt.Sprintf("%s_%06d.bnd", bundleName, fileID)
	return filepath.Join(bundleDir, fileName)
}

// GetBloomFilterPath returns the path for a bloom filter sidecar file
// Format: data_files/<database>/<bundle>/<bundle>_<fileID>.bloom
func (fpr *FilePathResolver) GetBloomFilterPath(databaseName, bundleName string, fileID int) string {
	bundleDir := fpr.GetBundleDirectory(databaseName, bundleName)
	fileName := fmt.Sprintf("%s_%06d.bloom", bundleName, fileID)
	return filepath.Join(bundleDir, fileName)
}

// GetManifestPath returns the path for the bundle manifest file
// Format: data_files/<database>/<bundle>/bundle.manifest
func (fpr *FilePathResolver) GetManifestPath(databaseName, bundleName string) string {
	bundleDir := fpr.GetBundleDirectory(databaseName, bundleName)
	return filepath.Join(bundleDir, "bundle.manifest")
}

// GetCompactionTempPath returns the path for a temporary compaction file
// Format: data_files/<database>/<bundle>/<bundle>_<fileID>.compact.tmp
func (fpr *FilePathResolver) GetCompactionTempPath(databaseName, bundleName string, fileID int) string {
	bundleDir := fpr.GetBundleDirectory(databaseName, bundleName)
	fileName := fmt.Sprintf("%s_%06d.compact.tmp", bundleName, fileID)
	return filepath.Join(bundleDir, fileName)
}

// EnsureBundleDirectory creates the bundle directory if it doesn't exist
func (fpr *FilePathResolver) EnsureBundleDirectory(databaseName, bundleName string) error {
	bundleDir := fpr.GetBundleDirectory(databaseName, bundleName)
	return os.MkdirAll(bundleDir, 0755)
}

// ParseBundleFileName extracts the fileID from a bundle file name
// Input: "Users_000001.bnd" → Output: 1
func ParseBundleFileName(fileName string) (int, error) {
	var fileID int
	_, err := fmt.Sscanf(fileName, "%*[^_]_%06d.bnd", &fileID)
	if err != nil {
		return 0, fmt.Errorf("invalid bundle file name format: %s", fileName)
	}
	return fileID, nil
}

// GetLegacySingleFilePath returns the legacy single-file path for backward compatibility
// Format: data_files/<database>/<database>_<bundle>.bnd
// DEPRECATED: Used only for migration from old single-file format
func (fpr *FilePathResolver) GetLegacySingleFilePath(databaseName, bundleName string) string {
	return filepath.Join(fpr.dataDir, databaseName, fmt.Sprintf("%s_%s.bnd", databaseName, bundleName))
}

// ============================================================================
// STANDALONE HELPER FUNCTIONS (for direct access without resolver instance)
// ============================================================================

// GetBundleDirectory returns the directory path for a bundle (standalone version)
// Format: <DataDir>/<database>/<bundle>/
func GetBundleDirectory(databaseName, bundleName string) string {
	databasePath := strings.TrimSuffix(helpers.GetDatabaseFolderPath(databaseName), "/")
	return filepath.Join(databasePath, bundleName)
}

// GetBundleFilePath returns the full path for a specific bundle file (standalone version)
// Format: data_files/<database>/<bundle>/<fileID>.bnd
func GetBundleFilePath(databaseName, bundleName string, fileID int) string {
	bundleDir := GetBundleDirectory(databaseName, bundleName)
	fileName := fmt.Sprintf("%06d.bnd", fileID)
	return filepath.Join(bundleDir, fileName)
}

// GetManifestPath returns the path for the bundle manifest file (standalone version)
// Format: data_files/<database>/<bundle>/bundle.manifest
func GetManifestPath(databaseName, bundleName string) string {
	bundleDir := GetBundleDirectory(databaseName, bundleName)
	return filepath.Join(bundleDir, "bundle.manifest")
}

// EnsureBundleDirectory creates the bundle directory if it doesn't exist (standalone version)
func EnsureBundleDirectory(databaseName, bundleName string) error {
	bundleDir := GetBundleDirectory(databaseName, bundleName)
	return os.MkdirAll(bundleDir, 0755)
}
