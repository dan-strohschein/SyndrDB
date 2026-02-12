package planner

import (
	"context"
	"fmt"
	"syndrdb/src/internal/domain/models"
	"syndrdb/src/internal/query/documentscanner"
)

// FullScanIterator streams documents from a FullScanNode one at a time.
// Internally, a producer goroutine calls ScanAllDocuments and pushes results
// onto a buffered channel; Next() pulls from that channel.
type FullScanIterator struct {
	node *FullScanNode
	ctx  context.Context

	// Channel-based bridge between scanner callback and pull-based Next()
	docCh  chan *models.Document
	errCh  chan error
	done   chan struct{} // Closed by Close() to signal producer to stop
	closed bool
	eof    bool

	// MVCC snapshot info from context
	snapshotInfo *SnapshotInfo
}

// NewFullScanIterator creates a FullScanIterator for the given FullScanNode.
func NewFullScanIterator(node *FullScanNode) *FullScanIterator {
	return &FullScanIterator{
		node: node,
	}
}

func (it *FullScanIterator) Init(ctx context.Context) error {
	it.ctx = ctx
	it.docCh = make(chan *models.Document, 256) // Buffer one batch of docs
	it.errCh = make(chan error, 1)
	it.done = make(chan struct{})
	it.snapshotInfo = GetSnapshotInfoFromContext(ctx)

	// Set MVCC snapshot on scanner
	if it.snapshotInfo != nil {
		if smartScanner, ok := it.node.DocumentScanner.(interface {
			SetSnapshot(snapshotSeq uint64, txID uint64, activeTxIDs map[uint64]bool)
		}); ok {
			smartScanner.SetSnapshot(it.snapshotInfo.SnapshotSequence, it.snapshotInfo.TransactionID, it.snapshotInfo.ActiveTxIDs)
		}
	}

	// Set projection
	if it.node.BundleServiceInt != nil {
		it.node.BundleServiceInt.SetProjectionFieldsForBundle(it.node.Bundle.Name, it.node.ProjectionFields)
	}
	if smartScanner, ok := it.node.DocumentScanner.(interface {
		GetBundle() documentscanner.BundleInterface
	}); ok {
		if bundleAdapter, ok := smartScanner.GetBundle().(*documentscanner.BundleAdapter); ok {
			bundleAdapter.SetProjectionFields(it.node.ProjectionFields)
		}
	}

	// Launch producer goroutine
	go it.produce()
	return nil
}

// produce runs in a goroutine, scanning documents and pushing them onto docCh.
func (it *FullScanIterator) produce() {
	defer close(it.docCh)

	// Cleanup projection when producer exits
	defer func() {
		if it.node.BundleServiceInt != nil {
			it.node.BundleServiceInt.SetProjectionFieldsForBundle(it.node.Bundle.Name, nil)
		}
		if smartScanner, ok := it.node.DocumentScanner.(interface {
			GetBundle() documentscanner.BundleInterface
		}); ok {
			if bundleAdapter, ok := smartScanner.GetBundle().(*documentscanner.BundleAdapter); ok {
				bundleAdapter.SetProjectionFields(nil)
			}
		}
	}()

	if it.node.DocumentScanner == nil {
		it.errCh <- fmt.Errorf("document scanner is required for iterator scan")
		return
	}

	var scanResult *documentscanner.ScanResult
	var err error

	if it.node.Predicate != nil {
		scanResult, err = it.node.DocumentScanner.ScanWithPredicate(it.node.Predicate)
	} else if it.node.MaxDocuments > 0 {
		if scannerWithLimit, ok := it.node.DocumentScanner.(interface {
			ScanAllDocumentsWithLimit(int) (*documentscanner.ScanResult, error)
		}); ok {
			scanResult, err = scannerWithLimit.ScanAllDocumentsWithLimit(it.node.MaxDocuments)
		} else {
			scanResult, err = it.node.DocumentScanner.ScanAllDocuments()
		}
	} else {
		scanResult, err = it.node.DocumentScanner.ScanAllDocuments()
	}

	if err != nil {
		it.errCh <- err
		return
	}

	for _, doc := range scanResult.Documents {
		select {
		case <-it.done:
			return // Consumer closed early
		case <-it.ctx.Done():
			it.errCh <- it.ctx.Err()
			return
		case it.docCh <- doc:
		}
	}
}

func (it *FullScanIterator) Next() (*models.Document, error) {
	if it.closed || it.eof {
		return nil, nil
	}

	select {
	case doc, ok := <-it.docCh:
		if !ok {
			// Channel closed: check for error
			it.eof = true
			select {
			case err := <-it.errCh:
				return nil, err
			default:
				return nil, nil // Clean EOF
			}
		}
		return doc, nil
	case err := <-it.errCh:
		it.eof = true
		return nil, err
	case <-it.ctx.Done():
		it.eof = true
		return nil, it.ctx.Err()
	}
}

func (it *FullScanIterator) Close() error {
	if it.closed {
		return nil
	}
	it.closed = true

	// Signal producer to stop
	close(it.done)

	// Drain the channel so producer goroutine can exit if blocked on send
	go func() {
		for range it.docCh {
		}
	}()

	return nil
}
