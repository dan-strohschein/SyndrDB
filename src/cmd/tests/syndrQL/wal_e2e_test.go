package syndrQL

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"syndrdb/src/internal/server"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

/*
WAL E2E TEST SUITE

End-to-end validation of Write-Ahead Logging (WAL) functionality.
Tests ensure that DDL and DML operations are properly logged to the WAL file.
*/

// findWALFile locates the current WAL file in the WAL directory
func findWALFile(walDir string) (string, error) {
	entries, err := os.ReadDir(walDir)
	if err != nil {
		return "", fmt.Errorf("failed to read WAL directory: %w", err)
	}

	// Look for today's WAL file (format: YYYY-MM-DD.wal)
	today := time.Now().Format("2006-01-02")
	expectedFile := filepath.Join(walDir, fmt.Sprintf("%s.wal", today))

	for _, entry := range entries {
		if !entry.IsDir() && strings.HasSuffix(entry.Name(), ".wal") {
			fullPath := filepath.Join(walDir, entry.Name())
			if fullPath == expectedFile {
				return fullPath, nil
			}
		}
	}

	return "", fmt.Errorf("WAL file not found in %s (expected: %s)", walDir, expectedFile)
}

// TestWAL_DatabaseCreation verifies that database creation is logged to WAL
func TestWAL_DatabaseCreation(t *testing.T) {
	fixture := setupFullServer(t)
	ctx := context.Background()

	// Verify WAL manager exists
	require.NotNil(t, fixture.ServiceManager.WALManager, "WAL Manager should be initialized")

	// Get WAL directory
	walDir := filepath.Join(fixture.Settings.LogDir, "wal")
	t.Logf("WAL directory: %s", walDir)

	// Create another database to ensure WAL is written
	dbName := "wal_test_db_2"
	createDBCmd := fmt.Sprintf(`CREATE DATABASE "%s"`, dbName)
	_, err := server.CommandDirector(ctx, nil, *fixture.ServiceManager, createDBCmd,
		fixture.Logger, time.Now(), nil, "127.0.0.1")
	require.NoError(t, err, "Failed to create test database")

	// Flush WAL to ensure all entries are written
	require.NoError(t, fixture.ServiceManager.WALManager.Flush(), "Failed to flush WAL")

	// Check if WAL directory exists
	_, err = os.Stat(walDir)
	require.NoError(t, err, "WAL directory should exist at %s", walDir)

	// Find WAL file
	walFile, err := findWALFile(walDir)
	require.NoError(t, err, "WAL file should exist")
	t.Logf("Found WAL file: %s", walFile)

	// Check file size
	fileInfo, err := os.Stat(walFile)
	require.NoError(t, err, "Failed to stat WAL file")
	t.Logf("WAL file size: %d bytes", fileInfo.Size())

	// File should not be empty
	assert.Greater(t, fileInfo.Size(), int64(0),
		"WAL file should not be empty after database creation")
}

// TestWAL_BundleCreation verifies that bundle creation is logged to WAL
func TestWAL_BundleCreation(t *testing.T) {
	fixture := setupFullServer(t)
	ctx := context.Background()

	// Verify WAL manager exists
	require.NotNil(t, fixture.ServiceManager.WALManager, "WAL Manager should be initialized")

	// Create a bundle
	createBundleCmd := `CREATE BUNDLE "WalTestBundle" WITH FIELDS (
		{"ID", "INT", true, false, 0},
		{"Name", "STRING", true, false, ""},
		{"Value", "INT", false, false, 0}
	)`
	_, err := server.CommandDirector(ctx, fixture.Database, *fixture.ServiceManager, createBundleCmd,
		fixture.Logger, time.Now(), nil, "127.0.0.1")
	require.NoError(t, err, "Failed to create bundle")

	// Flush WAL
	require.NoError(t, fixture.ServiceManager.WALManager.Flush(), "Failed to flush WAL")

	// Find and check WAL file
	walDir := filepath.Join(fixture.Settings.LogDir, "wal")
	walFile, err := findWALFile(walDir)
	require.NoError(t, err, "WAL file should exist")

	fileInfo, err := os.Stat(walFile)
	require.NoError(t, err, "Failed to stat WAL file")
	t.Logf("WAL file size after bundle creation: %d bytes", fileInfo.Size())

	assert.Greater(t, fileInfo.Size(), int64(0),
		"WAL file should contain data after bundle creation")
}

// TestWAL_DocumentInsertions verifies that document insertions are logged to WAL
func TestWAL_DocumentInsertions(t *testing.T) {
	fixture := setupFullServer(t)
	ctx := context.Background()

	// Verify WAL manager exists
	require.NotNil(t, fixture.ServiceManager.WALManager, "WAL Manager should be initialized")

	// Create a bundle
	createBundleCmd := `CREATE BUNDLE "Users" WITH FIELDS (
		{"ID", "INT", true, false, 0},
		{"Username", "STRING", true, false, ""},
		{"Email", "STRING", false, false, ""},
		{"Age", "INT", false, false, 0}
	)`
	_, err := server.CommandDirector(ctx, fixture.Database, *fixture.ServiceManager, createBundleCmd,
		fixture.Logger, time.Now(), nil, "127.0.0.1")
	require.NoError(t, err, "Failed to create bundle")

	// Record file size before insertions
	walDir := filepath.Join(fixture.Settings.LogDir, "wal")
	require.NoError(t, fixture.ServiceManager.WALManager.Flush(), "Failed to flush WAL")
	
	walFile, err := findWALFile(walDir)
	require.NoError(t, err, "WAL file should exist")
	
	fileInfoBefore, err := os.Stat(walFile)
	require.NoError(t, err, "Failed to stat WAL file")
	sizeBefore := fileInfoBefore.Size()

	// Insert documents
	insertCmd1 := `ADD TO "Users" {"ID": 1, "Username": "alice", "Email": "alice@example.com", "Age": 30}`
	_, err = server.CommandDirector(ctx, fixture.Database, *fixture.ServiceManager, insertCmd1,
		fixture.Logger, time.Now(), nil, "127.0.0.1")
	require.NoError(t, err, "Failed to insert first document")

	insertCmd2 := `ADD TO "Users" {"ID": 2, "Username": "bob", "Email": "bob@example.com", "Age": 25}`
	_, err = server.CommandDirector(ctx, fixture.Database, *fixture.ServiceManager, insertCmd2,
		fixture.Logger, time.Now(), nil, "127.0.0.1")
	require.NoError(t, err, "Failed to insert second document")

	// Flush WAL
	require.NoError(t, fixture.ServiceManager.WALManager.Flush(), "Failed to flush WAL")

	// Check file size after insertions
	fileInfoAfter, err := os.Stat(walFile)
	require.NoError(t, err, "Failed to stat WAL file")
	sizeAfter := fileInfoAfter.Size()

	t.Logf("WAL file size - before: %d bytes, after: %d bytes, delta: %d bytes",
		sizeBefore, sizeAfter, sizeAfter-sizeBefore)

	assert.Greater(t, sizeAfter, sizeBefore,
		"WAL file should grow after document insertions")
}

// TestWAL_CompleteWorkflow verifies a complete workflow: DB → Bundle → Documents
func TestWAL_CompleteWorkflow(t *testing.T) {
	fixture := setupFullServer(t)
	ctx := context.Background()

	// Verify WAL manager exists
	require.NotNil(t, fixture.ServiceManager.WALManager, "WAL Manager should be initialized")

	walDir := filepath.Join(fixture.Settings.LogDir, "wal")
	t.Logf("WAL directory: %s", walDir)

	// 1. Create bundles
	createAuthorsCmd := `CREATE BUNDLE "TestAuthors" WITH FIELDS (
		{"ID", "INT", true, false, 0},
		{"Name", "STRING", true, false, ""},
		{"BirthYear", "INT", false, false, 0}
	)`
	_, err := server.CommandDirector(ctx, fixture.Database, *fixture.ServiceManager, createAuthorsCmd,
		fixture.Logger, time.Now(), nil, "127.0.0.1")
	require.NoError(t, err, "Failed to create Authors bundle")

	createBooksCmd := `CREATE BUNDLE "TestBooks" WITH FIELDS (
		{"ID", "INT", true, false, 0},
		{"Title", "STRING", true, false, ""},
		{"AuthorID", "INT", false, false, 0},
		{"Year", "INT", false, false, 0}
	)`
	_, err = server.CommandDirector(ctx, fixture.Database, *fixture.ServiceManager, createBooksCmd,
		fixture.Logger, time.Now(), nil, "127.0.0.1")
	require.NoError(t, err, "Failed to create Books bundle")

	// 2. Insert documents
	insertAuthorCmd := `ADD TO "TestAuthors" {"ID": 1, "Name": "Jane Austen", "BirthYear": 1775}`
	_, err = server.CommandDirector(ctx, fixture.Database, *fixture.ServiceManager, insertAuthorCmd,
		fixture.Logger, time.Now(), nil, "127.0.0.1")
	require.NoError(t, err, "Failed to insert author")

	insertBookCmd := `ADD TO "TestBooks" {"ID": 1, "Title": "Pride and Prejudice", "AuthorID": 1, "Year": 1813}`
	_, err = server.CommandDirector(ctx, fixture.Database, *fixture.ServiceManager, insertBookCmd,
		fixture.Logger, time.Now(), nil, "127.0.0.1")
	require.NoError(t, err, "Failed to insert book")

	// Flush WAL
	require.NoError(t, fixture.ServiceManager.WALManager.Flush(), "Failed to flush WAL")

	// Find and check WAL file
	walFile, err := findWALFile(walDir)
	require.NoError(t, err, "WAL file should exist")
	t.Logf("WAL file location: %s", walFile)

	// Check file size
	fileInfo, err := os.Stat(walFile)
	require.NoError(t, err, "Failed to stat WAL file")
	t.Logf("WAL file size: %d bytes", fileInfo.Size())
	
	assert.Greater(t, fileInfo.Size(), int64(0),
		"WAL file should contain data after complete workflow")

	// Verify WAL file contains data (at minimum, should have binary header)
	content, err := os.ReadFile(walFile)
	require.NoError(t, err, "Failed to read WAL file")
	t.Logf("WAL file content length: %d bytes", len(content))
	
	assert.NotEmpty(t, content, "WAL file should not be empty")
	
	// Check for binary WAL magic number (0x57414C42 = "WALB")
	if len(content) >= 4 {
		// Binary format starts with magic number
		t.Logf("First 4 bytes: %#x", content[0:4])
	}
}

// TestWAL_FileLocation verifies that WAL file is created in the correct location
func TestWAL_FileLocation(t *testing.T) {
	fixture := setupFullServer(t)

	// Verify WAL manager exists
	require.NotNil(t, fixture.ServiceManager.WALManager, "WAL Manager should be initialized")

	// Flush to ensure file is created
	require.NoError(t, fixture.ServiceManager.WALManager.Flush(), "Failed to flush WAL")

	walDir := filepath.Join(fixture.Settings.LogDir, "wal")
	
	// Check WAL directory exists
	_, err := os.Stat(walDir)
	require.NoError(t, err, "WAL directory should exist at %s", walDir)

	// Check for WAL file
	walFile, err := findWALFile(walDir)
	require.NoError(t, err, "WAL file should exist")
	t.Logf("WAL file found at: %s", walFile)

	// Verify it's in the expected location
	expectedDir := filepath.Join(fixture.Settings.LogDir, "wal")
	assert.True(t, strings.HasPrefix(walFile, expectedDir),
		"WAL file should be in WAL directory (%s)", expectedDir)
}
