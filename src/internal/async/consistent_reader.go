package async

import (
	"context"
	"fmt"
	"sync"
	"time"
)

// DataSource represents different sources of data for consistent reading
type DataSource int

const (
	SourcePending DataSource = iota // In-memory pending operations
	SourceDisk                      // Committed data on disk
	SourceWAL                       // WAL replay for recovery
)

// ReadOperation represents a read request that needs consistent data
type ReadOperation struct {
	BundleName  string
	DocumentID  string
	FieldName   string
	Snapshot    *Snapshot
	RequiredSeq uint64 // Minimum sequence number that must be visible
}

// Snapshot represents a consistent point-in-time view of the data
type Snapshot struct {
	SequenceNumber uint64
	Timestamp      time.Time
	VisibleOps     map[uint64]bool // Which operations are visible in this snapshot
}

// ConsistentReader provides a unified interface for reading data consistently
// across pending operations, disk storage, and WAL recovery
type ConsistentReader interface {
	// Read retrieves data ensuring consistency across all sources
	Read(ctx context.Context, operation ReadOperation) (interface{}, DataSource, error)

	// CreateSnapshot creates a new consistent snapshot for multi-operation reads
	CreateSnapshot() *Snapshot

	// IsVisible checks if an operation is visible in the given snapshot
	IsVisible(snapshot *Snapshot, sequenceNumber uint64) bool
}

// MultiSourceReader implements ConsistentReader by checking multiple data sources
type MultiSourceReader struct {
	mu               sync.RWMutex
	pendingTracker   *PendingOperationTracker
	diskReader       DiskReader
	walReader        WALReader
	currentSnapshot  *Snapshot
	snapshotInterval time.Duration
}

// PendingOperationTracker keeps track of operations that are pending async completion
type PendingOperationTracker struct {
	mu         sync.RWMutex
	operations map[uint64]*PendingOperation
	byBundle   map[string][]uint64 // Bundle name -> sequence numbers
	byDocument map[string][]uint64 // Document ID -> sequence numbers
}

// PendingOperation represents an operation that's been submitted but not yet persisted
type PendingOperation struct {
	Sequence   uint64
	BundleName string
	DocumentID string
	Operation  string
	Data       interface{}
	Timestamp  time.Time
	Status     PendingStatus
}

// PendingStatus represents the status of a pending operation
type PendingStatus int

const (
	StatusSubmitted PendingStatus = iota
	StatusProcessing
	StatusCompleted
	StatusFailed
)

// DiskReader interface for reading committed data from disk
type DiskReader interface {
	ReadDocument(ctx context.Context, bundleName, documentID string) (interface{}, error)
	ReadField(ctx context.Context, bundleName, documentID, fieldName string) (interface{}, error)
}

// WALReader interface for reading data from WAL during recovery
type WALReader interface {
	ReadFromWAL(ctx context.Context, bundleName, documentID string, upToSequence uint64) (interface{}, error)
}

// NewMultiSourceReader creates a new multi-source consistent reader
func NewMultiSourceReader(diskReader DiskReader, walReader WALReader) *MultiSourceReader {
	return &MultiSourceReader{
		pendingTracker:   NewPendingOperationTracker(),
		diskReader:       diskReader,
		walReader:        walReader,
		snapshotInterval: 100 * time.Millisecond, // Create new snapshots every 100ms
	}
}

// Read implements ConsistentReader interface
func (msr *MultiSourceReader) Read(ctx context.Context, operation ReadOperation) (interface{}, DataSource, error) {
	// Use provided snapshot or create a new one
	snapshot := operation.Snapshot
	if snapshot == nil {
		snapshot = msr.CreateSnapshot()
	}

	// 1. Check pending operations first (highest priority)
	if data, found := msr.readFromPending(operation, snapshot); found {
		return data, SourcePending, nil
	}

	// 2. Try reading from disk (most common case)
	data, err := msr.readFromDisk(ctx, operation)
	if err == nil {
		return data, SourceDisk, nil
	}

	// 3. Fallback to WAL replay (recovery scenarios)
	if msr.walReader != nil {
		data, err = msr.readFromWAL(ctx, operation, snapshot)
		if err == nil {
			return data, SourceWAL, nil
		}
	}

	return nil, SourceDisk, fmt.Errorf("data not found in any source for bundle: %s, document: %s",
		operation.BundleName, operation.DocumentID)
}

// CreateSnapshot creates a new consistent snapshot
func (msr *MultiSourceReader) CreateSnapshot() *Snapshot {
	msr.mu.Lock()
	defer msr.mu.Unlock()

	now := time.Now()

	// If we have a recent snapshot, reuse it for efficiency
	if msr.currentSnapshot != nil &&
		now.Sub(msr.currentSnapshot.Timestamp) < msr.snapshotInterval {
		return msr.currentSnapshot
	}

	// Create a new snapshot based on current pending operations
	visibleOps := make(map[uint64]bool)
	msr.pendingTracker.mu.RLock()
	for seq, op := range msr.pendingTracker.operations {
		// Only include completed operations in the snapshot
		visibleOps[seq] = op.Status == StatusCompleted
	}
	msr.pendingTracker.mu.RUnlock()

	msr.currentSnapshot = &Snapshot{
		SequenceNumber: 0, // TODO: I need to get this from the sequence generator
		Timestamp:      now,
		VisibleOps:     visibleOps,
	}

	return msr.currentSnapshot
}

// IsVisible checks if an operation is visible in the given snapshot
func (msr *MultiSourceReader) IsVisible(snapshot *Snapshot, sequenceNumber uint64) bool {
	if snapshot == nil {
		return true // If no snapshot, assume everything is visible
	}

	visible, exists := snapshot.VisibleOps[sequenceNumber]
	return exists && visible
}

// readFromPending checks pending operations for the requested data
// This function follows SyndrDB's comprehensive error handling by safely accessing pending operations
// and returning the most recent visible operation's data for consistent reads
func (msr *MultiSourceReader) readFromPending(operation ReadOperation, snapshot *Snapshot) (interface{}, bool) {
	if msr.pendingTracker == nil {
		return nil, false
	}

	// Get all pending operations for the requested document
	pendingOps := msr.pendingTracker.GetPendingForDocument(operation.DocumentID)
	if len(pendingOps) == 0 {
		return nil, false
	}

	// Find the most recent operation that's visible in our snapshot
	// and matches our bundle/field criteria
	var mostRecentOp *PendingOperation
	var mostRecentSeq uint64 = 0

	for _, op := range pendingOps {
		// Skip operations not visible in this snapshot
		if !msr.IsVisible(snapshot, op.Sequence) {
			continue
		}

		// Skip operations that don't match our bundle
		if op.BundleName != operation.BundleName {
			continue
		}

		// Skip operations that don't meet our minimum sequence requirement
		if op.Sequence < operation.RequiredSeq {
			continue
		}

		// Skip failed operations - they shouldn't be visible
		if op.Status == StatusFailed {
			continue
		}

		// Find the most recent (highest sequence) operation
		if op.Sequence > mostRecentSeq {
			mostRecentOp = op
			mostRecentSeq = op.Sequence
		}
	}

	// If no suitable operation found, return not found
	if mostRecentOp == nil {
		return nil, false
	}

	// Extract the specific field data if requested, otherwise return full document
	if operation.FieldName != "" {
		// Try to extract field from operation data
		if docData, ok := mostRecentOp.Data.(map[string]interface{}); ok {
			if fieldValue, exists := docData[operation.FieldName]; exists {
				return fieldValue, true
			}
		}
		// Field not found in pending operation
		return nil, false
	}

	// Return the full document data
	return mostRecentOp.Data, true
}

// readFromDisk reads data from the committed disk storage
func (msr *MultiSourceReader) readFromDisk(ctx context.Context, operation ReadOperation) (interface{}, error) {
	if operation.FieldName != "" {
		return msr.diskReader.ReadField(ctx, operation.BundleName, operation.DocumentID, operation.FieldName)
	}
	return msr.diskReader.ReadDocument(ctx, operation.BundleName, operation.DocumentID)
}

// readFromWAL reads data from WAL replay
func (msr *MultiSourceReader) readFromWAL(ctx context.Context, operation ReadOperation, snapshot *Snapshot) (interface{}, error) {
	maxSequence := snapshot.SequenceNumber
	if operation.RequiredSeq > maxSequence {
		maxSequence = operation.RequiredSeq
	}

	return msr.walReader.ReadFromWAL(ctx, operation.BundleName, operation.DocumentID, maxSequence)
}

// NewPendingOperationTracker creates a new pending operation tracker
func NewPendingOperationTracker() *PendingOperationTracker {
	return &PendingOperationTracker{
		operations: make(map[uint64]*PendingOperation),
		byBundle:   make(map[string][]uint64),
		byDocument: make(map[string][]uint64),
	}
}

// AddPendingOperation adds a new pending operation to track
func (pot *PendingOperationTracker) AddPendingOperation(op *PendingOperation) {
	pot.mu.Lock()
	defer pot.mu.Unlock()

	pot.operations[op.Sequence] = op
	pot.byBundle[op.BundleName] = append(pot.byBundle[op.BundleName], op.Sequence)
	pot.byDocument[op.DocumentID] = append(pot.byDocument[op.DocumentID], op.Sequence)
}

// UpdateOperationStatus updates the status of a pending operation
func (pot *PendingOperationTracker) UpdateOperationStatus(sequence uint64, status PendingStatus) {
	pot.mu.Lock()
	defer pot.mu.Unlock()

	if op, exists := pot.operations[sequence]; exists {
		op.Status = status

		// Remove completed/failed operations after some time to prevent memory leaks
		// TODO: I need to implement proper cleanup of old operations
	}
}

// GetPendingForDocument returns all pending operations for a specific document
func (pot *PendingOperationTracker) GetPendingForDocument(documentID string) []*PendingOperation {
	pot.mu.RLock()
	defer pot.mu.RUnlock()

	sequences := pot.byDocument[documentID]
	result := make([]*PendingOperation, 0, len(sequences))

	for _, seq := range sequences {
		if op, exists := pot.operations[seq]; exists {
			result = append(result, op)
		}
	}

	return result
}

// GetPendingForBundle returns all pending operations for a specific bundle
func (pot *PendingOperationTracker) GetPendingForBundle(bundleName string) []*PendingOperation {
	pot.mu.RLock()
	defer pot.mu.RUnlock()

	sequences := pot.byBundle[bundleName]
	result := make([]*PendingOperation, 0, len(sequences))

	for _, seq := range sequences {
		if op, exists := pot.operations[seq]; exists {
			result = append(result, op)
		}
	}

	return result
}

// RemoveCompletedOperation removes an operation from tracking once it's been persisted
// This prevents memory leaks from accumulating completed operations
func (pot *PendingOperationTracker) RemoveCompletedOperation(sequence uint64) {
	pot.mu.Lock()
	defer pot.mu.Unlock()

	op, exists := pot.operations[sequence]
	if !exists {
		return
	}

	// Remove from main operations map
	delete(pot.operations, sequence)

	// Remove from bundle index
	if bundleSeqs, exists := pot.byBundle[op.BundleName]; exists {
		for i, seq := range bundleSeqs {
			if seq == sequence {
				pot.byBundle[op.BundleName] = append(bundleSeqs[:i], bundleSeqs[i+1:]...)
				break
			}
		}
		// Clean up empty bundle entries
		if len(pot.byBundle[op.BundleName]) == 0 {
			delete(pot.byBundle, op.BundleName)
		}
	}

	// Remove from document index
	if docSeqs, exists := pot.byDocument[op.DocumentID]; exists {
		for i, seq := range docSeqs {
			if seq == sequence {
				pot.byDocument[op.DocumentID] = append(docSeqs[:i], docSeqs[i+1:]...)
				break
			}
		}
		// Clean up empty document entries
		if len(pot.byDocument[op.DocumentID]) == 0 {
			delete(pot.byDocument, op.DocumentID)
		}
	}
}

// CleanupOldOperations removes operations older than the specified duration
// This should be called periodically to prevent memory leaks
func (pot *PendingOperationTracker) CleanupOldOperations(maxAge time.Duration) int {
	pot.mu.Lock()
	defer pot.mu.Unlock()

	cutoffTime := time.Now().Add(-maxAge)
	removedCount := 0

	// Collect sequences to remove (avoid modifying map while iterating)
	var sequencesToRemove []uint64
	for seq, op := range pot.operations {
		if op.Timestamp.Before(cutoffTime) &&
			(op.Status == StatusCompleted || op.Status == StatusFailed) {
			sequencesToRemove = append(sequencesToRemove, seq)
		}
	}

	// Remove collected sequences
	for _, seq := range sequencesToRemove {
		if op := pot.operations[seq]; op != nil {
			pot.removeOperationFromIndices(op, seq)
			delete(pot.operations, seq)
			removedCount++
		}
	}

	return removedCount
}

// GetOperationCount returns the total number of pending operations
func (pot *PendingOperationTracker) GetOperationCount() int {
	pot.mu.RLock()
	defer pot.mu.RUnlock()
	return len(pot.operations)
}

// GetOperationsByStatus returns all operations with the specified status
func (pot *PendingOperationTracker) GetOperationsByStatus(status PendingStatus) []*PendingOperation {
	pot.mu.RLock()
	defer pot.mu.RUnlock()

	result := make([]*PendingOperation, 0)
	for _, op := range pot.operations {
		if op.Status == status {
			result = append(result, op)
		}
	}

	return result
}

// MarkOperationCompleted marks an operation as completed and optionally removes it
func (pot *PendingOperationTracker) MarkOperationCompleted(sequence uint64, removeAfterCompletion bool) bool {
	pot.mu.Lock()
	defer pot.mu.Unlock()

	op, exists := pot.operations[sequence]
	if !exists {
		return false
	}

	op.Status = StatusCompleted

	if removeAfterCompletion {
		pot.removeOperationFromIndices(op, sequence)
		delete(pot.operations, sequence)
	}

	return true
}

// UpdateSnapshot creates a new snapshot with current operation visibility
func (msr *MultiSourceReader) UpdateSnapshot() *Snapshot {
	return msr.CreateSnapshot()
}

// GetPendingOperationCount returns the number of pending operations for monitoring
func (msr *MultiSourceReader) GetPendingOperationCount() int {
	if msr.pendingTracker == nil {
		return 0
	}
	return msr.pendingTracker.GetOperationCount()
}

// removeOperationFromIndices removes an operation from bundle and document indices
func (pot *PendingOperationTracker) removeOperationFromIndices(op *PendingOperation, sequence uint64) {
	// Remove from bundle index
	if bundleSeqs, exists := pot.byBundle[op.BundleName]; exists {
		for i, seq := range bundleSeqs {
			if seq == sequence {
				pot.byBundle[op.BundleName] = append(bundleSeqs[:i], bundleSeqs[i+1:]...)
				break
			}
		}
		if len(pot.byBundle[op.BundleName]) == 0 {
			delete(pot.byBundle, op.BundleName)
		}
	}

	// Remove from document index
	if docSeqs, exists := pot.byDocument[op.DocumentID]; exists {
		for i, seq := range docSeqs {
			if seq == sequence {
				pot.byDocument[op.DocumentID] = append(docSeqs[:i], docSeqs[i+1:]...)
				break
			}
		}
		if len(pot.byDocument[op.DocumentID]) == 0 {
			delete(pot.byDocument, op.DocumentID)
		}
	}
}
