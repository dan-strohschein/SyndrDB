package btreeindexV2

import (
	"fmt"
	"os"
	"testing"

	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

// TestSimpleDelete tests basic delete functionality with minimal data
func TestSimpleDelete(t *testing.T) {
	testDir := "data/testdb_simple"
	os.RemoveAll(testDir)
	os.MkdirAll(testDir, 0755)
	defer os.RemoveAll(testDir)

	logger, _ := zap.NewDevelopment()

	config := DefaultIndexConfig("testbundle", "testfield", testDir, "testdb")
	config.PageSize = 1024
	config.MaxKeyLength = 256
	config.CacheSize = 10

	idx, err := CreateBTreeIndex(config, logger.Sugar())
	require.NoError(t, err)
	defer idx.Close()

	// Insert just a few keys
	t.Log("Inserting 10 keys...")
	for i := 0; i < 10; i++ {
		key := []byte(fmt.Sprintf("key_%03d", i))
		docID := fmt.Sprintf("doc_%03d", i)
		err := idx.Insert(key, docID)
		require.NoError(t, err, "Insert %d should succeed", i)
	}

	// Search for the keys
	t.Log("Searching for keys...")
	for i := 0; i < 10; i++ {
		key := []byte(fmt.Sprintf("key_%03d", i))
		results, err := idx.Search(key)
		require.NoError(t, err)
		t.Logf("key_%03d: found %d results", i, len(results))
		require.Len(t, results, 1, "Should find exactly 1 result for key_%03d", i)
	}

	// Try to delete one
	t.Log("Attempting to delete key_005...")
	key := []byte("key_005")
	docID := "doc_005"
	err = idx.Delete(key, docID)
	require.NoError(t, err, "Delete should succeed")

	// Search again - should return 0 results (tombstone)
	results, err := idx.Search(key)
	require.NoError(t, err)
	t.Logf("After delete, key_005 has %d results", len(results))
	require.Len(t, results, 0, "Deleted key should return no results")
}
