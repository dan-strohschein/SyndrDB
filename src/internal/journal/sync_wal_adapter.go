package journal

import (
	"fmt"
)

// SyncWALAdapter adapts the existing synchronous WAL to the new WALModeInterface
// This allows the existing WAL to work with the new unified interface without changes
type SyncWALAdapter struct {
	walManager *WALManager
	mode       string
}

// NewSyncWALAdapter creates a new adapter for the existing synchronous WAL
func NewSyncWALAdapter(walManager *WALManager) *SyncWALAdapter {
	return &SyncWALAdapter{
		walManager: walManager,
		mode:       "sync",
	}
}

// WriteEntry implements WALModeInterface for sync WAL
func (swa *SyncWALAdapter) WriteEntry(entry WALEntry) error {
	// For sync WAL, we'll use the ExecuteWithLogging method which handles
	// transaction management internally
	return swa.walManager.ExecuteWithLogging(func(txID string) error {
		// Dispatch to the appropriate logging method based on operation type
		switch entry.Operation {
		case OpInsert:
			return swa.walManager.LogDocumentInsert(txID, entry.BundleName, entry.DocumentID, entry.AfterData)
		case OpUpdate:
			return swa.walManager.LogDocumentUpdate(txID, entry.BundleName, entry.DocumentID, entry.BeforeData, entry.AfterData)
		case OpDelete:
			return swa.walManager.LogDocumentDelete(txID, entry.BundleName, entry.DocumentID, entry.BeforeData)
		case OpCreateBundle:
			return swa.walManager.LogBundleCreate(txID, entry.BundleName, entry.Metadata)
		case OpDeleteBundle:
			return swa.walManager.LogBundleDelete(txID, entry.BundleName, entry.Metadata)
		case OpCreateIndex:
			// For index operations, DocumentID contains the index name
			return swa.walManager.LogIndexCreate(txID, entry.BundleName, entry.DocumentID, entry.AfterData)
		case OpDropIndex:
			// For index operations, DocumentID contains the index name
			return swa.walManager.LogIndexDrop(txID, entry.BundleName, entry.DocumentID, entry.BeforeData)
		default:
			return fmt.Errorf("unsupported WAL operation: %d", entry.Operation)
		}
	})
}

// WriteEntries implements WALModeInterface for sync WAL
func (swa *SyncWALAdapter) WriteEntries(entries []WALEntry) error {
	if len(entries) == 0 {
		return nil
	}

	// For sync WAL, process each entry individually using ExecuteWithLogging
	// This ensures each operation is properly logged and committed
	for i, entry := range entries {
		err := swa.WriteEntry(entry)
		if err != nil {
			return fmt.Errorf("failed to write WAL entry %d: %w", i, err)
		}
	}

	return nil
}

// Flush implements WALModeInterface for sync WAL
func (swa *SyncWALAdapter) Flush() error {
	// Use the existing WALManager flush method
	return swa.walManager.Flush()
}

// IsAsync implements WALModeInterface
func (swa *SyncWALAdapter) IsAsync() bool {
	return false
}

// GetMode implements WALModeInterface
func (swa *SyncWALAdapter) GetMode() string {
	return swa.mode
}

// Close implements WALModeInterface for sync WAL
func (swa *SyncWALAdapter) Close() error {
	// Use the existing WALManager close method
	return swa.walManager.Close()
}
