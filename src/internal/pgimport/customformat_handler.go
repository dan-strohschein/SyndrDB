package pgimport

import (
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"

	"go.uber.org/zap"
)

// customFormatMagic is the first 5 bytes of a pg_dump custom-format file.
var customFormatMagic = []byte("PGDMP")

// DetectFormat reads the first bytes of the file to determine if it's plain SQL or custom format.
func DetectFormat(filePath string) (ImportFormat, error) {
	f, err := os.Open(filePath)
	if err != nil {
		return FormatPlain, fmt.Errorf("cannot open file: %w", err)
	}
	defer f.Close()

	header := make([]byte, 5)
	n, err := f.Read(header)
	if err != nil && err != io.EOF {
		return FormatPlain, fmt.Errorf("cannot read file header: %w", err)
	}

	if n >= 5 && string(header[:5]) == string(customFormatMagic) {
		return FormatCustom, nil
	}

	return FormatPlain, nil
}

// ConvertCustomToPlain converts a PostgreSQL custom-format dump to plain SQL
// by shelling out to pg_restore. Returns the path to the temporary plain SQL file
// and a cleanup function that removes the temp file.
func ConvertCustomToPlain(inputPath string, tempDir string, logger *zap.SugaredLogger) (string, func(), error) {
	// Check if pg_restore is available
	pgRestorePath, err := exec.LookPath("pg_restore")
	if err != nil {
		return "", nil, fmt.Errorf(
			"pg_restore is required for PostgreSQL custom-format (-Fc) dumps but was not found in PATH; " +
				"install PostgreSQL client tools or convert the dump first with: " +
				"pg_restore -f output.sql input.dump")
	}

	if logger != nil {
		logger.Infof("pgimport: using pg_restore at %s", pgRestorePath)
	}

	// Create temp file for the plain SQL output
	if tempDir == "" {
		tempDir = os.TempDir()
	}
	tmpFile, err := os.CreateTemp(tempDir, "syndrdb-pgimport-*.sql")
	if err != nil {
		return "", nil, fmt.Errorf("cannot create temp file: %w", err)
	}
	tmpPath := tmpFile.Name()
	tmpFile.Close()

	cleanup := func() {
		os.Remove(tmpPath)
	}

	// Run pg_restore to convert custom → plain SQL
	absInput, err := filepath.Abs(inputPath)
	if err != nil {
		cleanup()
		return "", nil, fmt.Errorf("cannot resolve input path: %w", err)
	}

	cmd := exec.Command(pgRestorePath, "--file="+tmpPath, "--format=plain", absInput)
	output, err := cmd.CombinedOutput()
	if err != nil {
		cleanup()
		return "", nil, fmt.Errorf("pg_restore failed: %w\nOutput: %s", err, string(output))
	}

	if logger != nil {
		logger.Infof("pgimport: converted custom-format dump to plain SQL: %s", tmpPath)
	}

	return tmpPath, cleanup, nil
}
