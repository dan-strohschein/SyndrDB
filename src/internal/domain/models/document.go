package models

import (
	"time"
)

type Document struct {
	DocumentID string
	Fields     map[string]Field
	Data       map[string]interface{} // Raw document data for storage compatibility
	CreatedAt  time.Time
	UpdatedAt  time.Time

	// PooledFields is set when Fields was obtained from document.GetPooledFieldMap()
	// (e.g. in mergeJoinedDocument). ReturnPooledDocument will return that map to the
	// field map pool and clear Fields when this is true.
	PooledFields bool

	// MVCC Version Metadata
	// These fields enable snapshot isolation and version tracking
	CreatedByTxID   uint64 // Transaction that created this version (0 = autocommit/system)
	DeletedByTxID   uint64 // Transaction that deleted this version (0 = not deleted)
	CommitSequence  uint64 // Global sequence when committed (0 = uncommitted)
	VersionSequence uint64 // Version number within document ID (1, 2, 3...)
}

// GetField returns the value for a given field name, or nil if not present.
func (d *Document) GetField(fieldName string) interface{} {
	if d == nil || d.Fields == nil {
		return nil
	}
	return d.Fields[fieldName]
}

// IsDeleted returns true if the document has been deleted
func (d *Document) IsDeleted() bool {
	return d != nil && d.DeletedByTxID > 0
}

// IsCommitted returns true if the document version has been committed
func (d *Document) IsCommitted() bool {
	return d != nil && d.CommitSequence > 0
}

// GetVersionSequence returns the version sequence number
func (d *Document) GetVersionSequence() uint64 {
	if d == nil {
		return 0
	}
	return d.VersionSequence
}

// IsVisibleToSnapshot determines if this document version is visible to a given snapshot
// Implements MVCC visibility rules for snapshot isolation
// snapshotSeq: The snapshot sequence boundary
// txID: The transaction ID for this snapshot
// activeTxIDs: Map of transaction IDs that were active at snapshot time
func (d *Document) IsVisibleToSnapshot(snapshotSeq uint64, txID uint64, activeTxIDs map[uint64]bool) bool {
	if d == nil {
		return false
	}

	// Rule 1: Transaction sees its own uncommitted writes (read-your-own-writes)
	if d.CommitSequence == 0 {
		if d.CreatedByTxID == txID {
			return true
		}
		// Legacy pre-MVCC docs: CommitSequence and CreatedByTxID both 0 → always visible
		if d.CreatedByTxID == 0 {
			return true
		}
		return false
	}

	// Rule 2: Must be committed before snapshot boundary
	if d.CommitSequence > snapshotSeq {
		return false
	}

	// Rule 3: Not created by a transaction that was active at snapshot time
	// Part 6.2 fast path: skip map lookup when no other active transactions
	if activeTxIDs != nil && len(activeTxIDs) > 0 && activeTxIDs[d.CreatedByTxID] {
		return false
	}

	// Rule 4: Not deleted before snapshot
	if d.DeletedByTxID > 0 && d.DeletedByTxID <= snapshotSeq {
		return false
	}

	return true
}
