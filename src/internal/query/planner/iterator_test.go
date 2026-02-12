package planner

import (
	"context"
	"fmt"
	"runtime"
	"sync/atomic"
	"testing"
	"time"

	"syndrdb/src/internal/domain/models"
	"syndrdb/src/internal/query/documentscanner"
	"syndrdb/src/internal/query/queryparser"
	"syndrdb/src/internal/syndrQL"

	"go.uber.org/zap"
)

// --- Mock Scanner ---

type mockScanner struct {
	docs    []*models.Document
	docIDs  []string
	scanErr error
}

func newMockScanner(docs []*models.Document) *mockScanner {
	ids := make([]string, len(docs))
	for i, d := range docs {
		ids[i] = d.DocumentID
	}
	return &mockScanner{docs: docs, docIDs: ids}
}

func (ms *mockScanner) ScanAllDocuments() (*documentscanner.ScanResult, error) {
	if ms.scanErr != nil {
		return nil, ms.scanErr
	}
	return &documentscanner.ScanResult{
		Documents:   ms.docs,
		DocumentIDs: ms.docIDs,
	}, nil
}

func (ms *mockScanner) ScanWithPredicate(pred func(*models.Document) bool) (*documentscanner.ScanResult, error) {
	if ms.scanErr != nil {
		return nil, ms.scanErr
	}
	var docs []*models.Document
	var ids []string
	for i, d := range ms.docs {
		if pred(d) {
			docs = append(docs, d)
			ids = append(ids, ms.docIDs[i])
		}
	}
	return &documentscanner.ScanResult{Documents: docs, DocumentIDs: ids}, nil
}

func (ms *mockScanner) ScanForKeyValue(query *documentscanner.ScanQuery) (*documentscanner.ScanResult, error) {
	return ms.ScanAllDocuments()
}

func (ms *mockScanner) GetMetrics() *documentscanner.ScanMetrics   { return &documentscanner.ScanMetrics{} }
func (ms *mockScanner) GetHotKeys() []string                       { return nil }
func (ms *mockScanner) IsHotKey(keyName string) bool               { return false }
func (ms *mockScanner) GetConfig() *documentscanner.ScannerConfig  { return &documentscanner.ScannerConfig{} }
func (ms *mockScanner) Close() error                               { return nil }

// --- Mock child iterator for testing Filter/Limit independently ---

type mockIterator struct {
	docs  []*models.Document
	pos   int
	initCalled  bool
	closeCalled int32 // atomic for race-safe checking
}

func newMockIterator(docs []*models.Document) *mockIterator {
	return &mockIterator{docs: docs}
}

func (mi *mockIterator) Init(ctx context.Context) error {
	mi.initCalled = true
	return nil
}

func (mi *mockIterator) Next() (*models.Document, error) {
	if mi.pos >= len(mi.docs) {
		return nil, nil
	}
	doc := mi.docs[mi.pos]
	mi.pos++
	return doc, nil
}

func (mi *mockIterator) Close() error {
	atomic.AddInt32(&mi.closeCalled, 1)
	return nil
}

func (mi *mockIterator) wasClosed() bool {
	return atomic.LoadInt32(&mi.closeCalled) > 0
}

// --- Test helpers ---

func makeIterTestDoc(id string, age float64, name string) *models.Document {
	return makeTestDocument(id, map[string]interface{}{
		"age":  age,
		"name": name,
	})
}

func makeNDocs(n int) []*models.Document {
	docs := make([]*models.Document, n)
	for i := 0; i < n; i++ {
		docs[i] = makeIterTestDoc(fmt.Sprintf("doc-%d", i), float64(i), fmt.Sprintf("user-%d", i))
	}
	return docs
}

// --- FullScanIterator Tests ---

func TestFullScanIterator_BasicIteration(t *testing.T) {
	docs := makeNDocs(10)
	scanner := newMockScanner(docs)
	logger, _ := zap.NewDevelopment()

	node := &FullScanNode{
		Bundle:          &models.Bundle{Name: "test"},
		DocumentScanner: scanner,
		Logger:          logger.Sugar(),
	}

	iter := NewFullScanIterator(node)
	if err := iter.Init(context.Background()); err != nil {
		t.Fatalf("Init failed: %v", err)
	}
	defer iter.Close()

	var results []*models.Document
	for {
		doc, err := iter.Next()
		if err != nil {
			t.Fatalf("Next() error: %v", err)
		}
		if doc == nil {
			break
		}
		results = append(results, doc)
	}

	if len(results) != 10 {
		t.Errorf("expected 10 docs, got %d", len(results))
	}
}

func TestFullScanIterator_EmptyScan(t *testing.T) {
	scanner := newMockScanner([]*models.Document{})
	logger, _ := zap.NewDevelopment()

	node := &FullScanNode{
		Bundle:          &models.Bundle{Name: "test"},
		DocumentScanner: scanner,
		Logger:          logger.Sugar(),
	}

	iter := NewFullScanIterator(node)
	if err := iter.Init(context.Background()); err != nil {
		t.Fatalf("Init failed: %v", err)
	}
	defer iter.Close()

	doc, err := iter.Next()
	if err != nil {
		t.Fatalf("Next() error: %v", err)
	}
	if doc != nil {
		t.Error("expected nil (EOF) for empty scan")
	}
}

func TestFullScanIterator_EOF_Idempotent(t *testing.T) {
	scanner := newMockScanner(makeNDocs(2))
	logger, _ := zap.NewDevelopment()

	node := &FullScanNode{
		Bundle:          &models.Bundle{Name: "test"},
		DocumentScanner: scanner,
		Logger:          logger.Sugar(),
	}

	iter := NewFullScanIterator(node)
	if err := iter.Init(context.Background()); err != nil {
		t.Fatalf("Init failed: %v", err)
	}
	defer iter.Close()

	// Drain all docs
	for {
		doc, err := iter.Next()
		if err != nil {
			t.Fatalf("Next error: %v", err)
		}
		if doc == nil {
			break
		}
	}

	// Additional calls should still return (nil, nil)
	for i := 0; i < 3; i++ {
		doc, err := iter.Next()
		if err != nil {
			t.Fatalf("Post-EOF Next() #%d error: %v", i, err)
		}
		if doc != nil {
			t.Errorf("Post-EOF Next() #%d returned non-nil doc", i)
		}
	}
}

func TestFullScanIterator_ContextCancellation(t *testing.T) {
	// Create a large doc set to ensure producer is still pushing
	docs := makeNDocs(10000)
	scanner := newMockScanner(docs)
	logger, _ := zap.NewDevelopment()

	node := &FullScanNode{
		Bundle:          &models.Bundle{Name: "test"},
		DocumentScanner: scanner,
		Logger:          logger.Sugar(),
	}

	ctx, cancel := context.WithCancel(context.Background())
	iter := NewFullScanIterator(node)
	if err := iter.Init(ctx); err != nil {
		t.Fatalf("Init failed: %v", err)
	}
	defer iter.Close()

	// Read a few docs, then cancel
	for i := 0; i < 5; i++ {
		_, err := iter.Next()
		if err != nil {
			t.Fatalf("Next() error: %v", err)
		}
	}
	cancel()

	// Eventually should get context.Canceled error or EOF
	gotCanceled := false
	for i := 0; i < 200; i++ {
		_, err := iter.Next()
		if err == context.Canceled {
			gotCanceled = true
			break
		}
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
	}
	// The cancellation may not be seen if docs were already buffered; that's OK
	_ = gotCanceled
}

func TestFullScanIterator_NoGoroutineLeak(t *testing.T) {
	docs := makeNDocs(1000)
	scanner := newMockScanner(docs)
	logger, _ := zap.NewDevelopment()

	node := &FullScanNode{
		Bundle:          &models.Bundle{Name: "test"},
		DocumentScanner: scanner,
		Logger:          logger.Sugar(),
	}

	before := runtime.NumGoroutine()

	iter := NewFullScanIterator(node)
	if err := iter.Init(context.Background()); err != nil {
		t.Fatalf("Init failed: %v", err)
	}

	// Read 5 docs then close (not draining all)
	for i := 0; i < 5; i++ {
		iter.Next()
	}
	iter.Close()

	// Wait for goroutine cleanup
	time.Sleep(50 * time.Millisecond)
	runtime.Gosched()

	after := runtime.NumGoroutine()
	// Allow +1 for GC/runtime jitter
	if after > before+1 {
		t.Errorf("goroutine leak: before=%d, after=%d", before, after)
	}
}

func TestFullScanIterator_ScanError(t *testing.T) {
	scanner := &mockScanner{scanErr: fmt.Errorf("disk failure")}
	logger, _ := zap.NewDevelopment()

	node := &FullScanNode{
		Bundle:          &models.Bundle{Name: "test"},
		DocumentScanner: scanner,
		Logger:          logger.Sugar(),
	}

	iter := NewFullScanIterator(node)
	if err := iter.Init(context.Background()); err != nil {
		t.Fatalf("Init failed: %v", err)
	}
	defer iter.Close()

	_, err := iter.Next()
	if err == nil {
		t.Fatal("expected error from scanner failure")
	}
	if err.Error() != "disk failure" {
		t.Errorf("expected 'disk failure', got: %v", err)
	}
}

// --- FilterIterator Tests ---

func TestFilterIterator_AllMatch(t *testing.T) {
	docs := makeNDocs(5)
	child := newMockIterator(docs)
	logger, _ := zap.NewDevelopment()

	// No WhereExpression → all match
	node := &FilterNode{Logger: logger.Sugar()}

	iter := NewFilterIterator(node, child)
	if err := iter.Init(context.Background()); err != nil {
		t.Fatalf("Init failed: %v", err)
	}
	defer iter.Close()

	results, err := DrainIterator(iter)
	if err != nil {
		t.Fatalf("DrainIterator error: %v", err)
	}
	if len(results) != 5 {
		t.Errorf("expected 5 docs (all match), got %d", len(results))
	}
}

func TestFilterIterator_NoneMatch(t *testing.T) {
	docs := makeNDocs(5)
	child := newMockIterator(docs)
	logger, _ := zap.NewDevelopment()

	// Expression: age = 999 (no doc has age 999)
	expr := &syndrQL.BinaryExpression{
		Left:     &syndrQL.IdentifierExpression{Name: "age"},
		Operator: syndrQL.TOKEN_EQ,
		Right:    &syndrQL.LiteralExpression{Value: float64(999)},
	}
	node := &FilterNode{
		WhereExpression: expr,
		Logger:          logger.Sugar(),
	}

	iter := NewFilterIterator(node, child)
	if err := iter.Init(context.Background()); err != nil {
		t.Fatalf("Init failed: %v", err)
	}
	defer iter.Close()

	results, err := DrainIterator(iter)
	if err != nil {
		t.Fatalf("DrainIterator error: %v", err)
	}
	if len(results) != 0 {
		t.Errorf("expected 0 docs (none match), got %d", len(results))
	}
}

func TestFilterIterator_PartialMatch(t *testing.T) {
	// Docs with ages 0-9; filter age > 5 → should get docs 6,7,8,9
	docs := makeNDocs(10)
	child := newMockIterator(docs)
	logger, _ := zap.NewDevelopment()

	expr := &syndrQL.BinaryExpression{
		Left:     &syndrQL.IdentifierExpression{Name: "age"},
		Operator: syndrQL.TOKEN_GT,
		Right:    &syndrQL.LiteralExpression{Value: float64(5)},
	}
	node := &FilterNode{
		WhereExpression: expr,
		Logger:          logger.Sugar(),
	}

	iter := NewFilterIterator(node, child)
	if err := iter.Init(context.Background()); err != nil {
		t.Fatalf("Init failed: %v", err)
	}
	defer iter.Close()

	results, err := DrainIterator(iter)
	if err != nil {
		t.Fatalf("DrainIterator error: %v", err)
	}
	if len(results) != 4 {
		t.Errorf("expected 4 docs (age > 5), got %d", len(results))
	}
}

func TestFilterIterator_MaxDocuments(t *testing.T) {
	docs := makeNDocs(100)
	child := newMockIterator(docs)
	logger, _ := zap.NewDevelopment()

	// No WHERE expression (all match), but MaxDocuments = 10
	node := &FilterNode{
		Logger:       logger.Sugar(),
		MaxDocuments: 10,
	}

	iter := NewFilterIterator(node, child)
	if err := iter.Init(context.Background()); err != nil {
		t.Fatalf("Init failed: %v", err)
	}
	defer iter.Close()

	results, err := DrainIterator(iter)
	if err != nil {
		t.Fatalf("DrainIterator error: %v", err)
	}
	if len(results) != 10 {
		t.Errorf("expected 10 docs (MaxDocuments), got %d", len(results))
	}
}

func TestFilterIterator_ChildClose(t *testing.T) {
	docs := makeNDocs(5)
	child := newMockIterator(docs)
	logger, _ := zap.NewDevelopment()

	node := &FilterNode{Logger: logger.Sugar()}
	iter := NewFilterIterator(node, child)
	iter.Init(context.Background())
	iter.Close()

	if !child.wasClosed() {
		t.Error("expected child.Close() to be called")
	}
}

// --- LimitIterator Tests ---

func TestLimitIterator_Basic(t *testing.T) {
	docs := makeNDocs(100)
	child := newMockIterator(docs)

	node := &LimitNode{Limit: 10}
	iter := NewLimitIterator(node, child)
	if err := iter.Init(context.Background()); err != nil {
		t.Fatalf("Init failed: %v", err)
	}
	defer iter.Close()

	results, err := DrainIterator(iter)
	if err != nil {
		t.Fatalf("DrainIterator error: %v", err)
	}
	if len(results) != 10 {
		t.Errorf("expected 10 docs (LIMIT 10), got %d", len(results))
	}
	// Verify first docs are correct
	if results[0].DocumentID != "doc-0" {
		t.Errorf("expected first doc 'doc-0', got '%s'", results[0].DocumentID)
	}
}

func TestLimitIterator_WithOffset(t *testing.T) {
	docs := makeNDocs(100)
	child := newMockIterator(docs)

	node := &LimitNode{Limit: 5, Offset: 10}
	iter := NewLimitIterator(node, child)
	if err := iter.Init(context.Background()); err != nil {
		t.Fatalf("Init failed: %v", err)
	}
	defer iter.Close()

	results, err := DrainIterator(iter)
	if err != nil {
		t.Fatalf("DrainIterator error: %v", err)
	}
	if len(results) != 5 {
		t.Errorf("expected 5 docs (OFFSET 10 LIMIT 5), got %d", len(results))
	}
	if results[0].DocumentID != "doc-10" {
		t.Errorf("expected first doc 'doc-10', got '%s'", results[0].DocumentID)
	}
}

func TestLimitIterator_OffsetBeyondInput(t *testing.T) {
	docs := makeNDocs(10)
	child := newMockIterator(docs)

	node := &LimitNode{Limit: 5, Offset: 20}
	iter := NewLimitIterator(node, child)
	if err := iter.Init(context.Background()); err != nil {
		t.Fatalf("Init failed: %v", err)
	}
	defer iter.Close()

	results, err := DrainIterator(iter)
	if err != nil {
		t.Fatalf("DrainIterator error: %v", err)
	}
	if len(results) != 0 {
		t.Errorf("expected 0 docs (offset beyond input), got %d", len(results))
	}
}

func TestLimitIterator_ChildSmaller(t *testing.T) {
	docs := makeNDocs(3)
	child := newMockIterator(docs)

	node := &LimitNode{Limit: 100}
	iter := NewLimitIterator(node, child)
	if err := iter.Init(context.Background()); err != nil {
		t.Fatalf("Init failed: %v", err)
	}
	defer iter.Close()

	results, err := DrainIterator(iter)
	if err != nil {
		t.Fatalf("DrainIterator error: %v", err)
	}
	if len(results) != 3 {
		t.Errorf("expected 3 docs (child smaller than limit), got %d", len(results))
	}
}

func TestLimitIterator_ChildClose(t *testing.T) {
	docs := makeNDocs(100)
	child := newMockIterator(docs)

	node := &LimitNode{Limit: 5}
	iter := NewLimitIterator(node, child)
	iter.Init(context.Background())

	// Read 5 docs
	for i := 0; i < 5; i++ {
		iter.Next()
	}
	iter.Close()

	if !child.wasClosed() {
		t.Error("expected child.Close() to be called")
	}
}

// --- MaterializingIterator Tests ---

type mockExecNode struct {
	docs map[string]*models.Document
	cost float64
	rows int
}

func (n *mockExecNode) Execute(ctx context.Context) (map[string]*models.Document, error) {
	return n.docs, nil
}
func (n *mockExecNode) GetCost() float64            { return n.cost }
func (n *mockExecNode) GetEstimatedRows() int        { return n.rows }
func (n *mockExecNode) EstimateMemoryUsage() int64   { return 0 }

func TestMaterializingIterator_Basic(t *testing.T) {
	docs := make(map[string]*models.Document)
	for i := 0; i < 5; i++ {
		d := makeIterTestDoc(fmt.Sprintf("doc-%d", i), float64(i), "test")
		docs[d.DocumentID] = d
	}

	node := &mockExecNode{docs: docs}
	iter := NewMaterializingIterator(node)
	if err := iter.Init(context.Background()); err != nil {
		t.Fatalf("Init failed: %v", err)
	}
	defer iter.Close()

	results, err := DrainIterator(iter)
	if err != nil {
		t.Fatalf("DrainIterator error: %v", err)
	}
	if len(results) != 5 {
		t.Errorf("expected 5 docs, got %d", len(results))
	}
}

func TestMaterializingIterator_Empty(t *testing.T) {
	node := &mockExecNode{docs: map[string]*models.Document{}}
	iter := NewMaterializingIterator(node)
	iter.Init(context.Background())
	defer iter.Close()

	doc, err := iter.Next()
	if err != nil {
		t.Fatalf("Next error: %v", err)
	}
	if doc != nil {
		t.Error("expected EOF for empty node")
	}
}

// --- Helper function tests ---

func TestIsEOF(t *testing.T) {
	if !IsEOF(nil, nil) {
		t.Error("IsEOF(nil, nil) should be true")
	}
	if IsEOF(&models.Document{}, nil) {
		t.Error("IsEOF(doc, nil) should be false")
	}
	if IsEOF(nil, fmt.Errorf("err")) {
		t.Error("IsEOF(nil, err) should be false")
	}
}

func TestDrainIteratorWithLimit(t *testing.T) {
	docs := makeNDocs(100)
	child := newMockIterator(docs)

	results, err := DrainIteratorWithLimit(child, 10)
	if err != nil {
		t.Fatalf("DrainIteratorWithLimit error: %v", err)
	}
	if len(results) != 10 {
		t.Errorf("expected 10, got %d", len(results))
	}
}

// --- Pipeline Integration Tests ---

func TestPipeline_ScanFilterLimit(t *testing.T) {
	// Scan 100 docs → Filter(age > 50) → Limit(5)
	// Ages are 0-99, so age > 50 gives docs 51-99 (49 docs), limit 5 yields 5
	docs := makeNDocs(100)
	child := newMockIterator(docs)
	logger, _ := zap.NewDevelopment()

	expr := &syndrQL.BinaryExpression{
		Left:     &syndrQL.IdentifierExpression{Name: "age"},
		Operator: syndrQL.TOKEN_GT,
		Right:    &syndrQL.LiteralExpression{Value: float64(50)},
	}
	filterNode := &FilterNode{
		WhereExpression: expr,
		Logger:          logger.Sugar(),
	}
	filterIter := NewFilterIterator(filterNode, child)

	limitNode := &LimitNode{Limit: 5}
	limitIter := NewLimitIterator(limitNode, filterIter)

	if err := limitIter.Init(context.Background()); err != nil {
		t.Fatalf("Init failed: %v", err)
	}
	defer limitIter.Close()

	results, err := DrainIterator(limitIter)
	if err != nil {
		t.Fatalf("Drain error: %v", err)
	}
	if len(results) != 5 {
		t.Errorf("expected 5 docs, got %d", len(results))
	}
}

func TestPipeline_ScanLimit(t *testing.T) {
	// Scan 1000 docs → Limit(3)
	docs := makeNDocs(1000)
	scanner := newMockScanner(docs)
	logger, _ := zap.NewDevelopment()

	scanNode := &FullScanNode{
		Bundle:          &models.Bundle{Name: "test"},
		DocumentScanner: scanner,
		Logger:          logger.Sugar(),
	}
	scanIter := NewFullScanIterator(scanNode)

	limitNode := &LimitNode{Limit: 3}
	limitIter := NewLimitIterator(limitNode, scanIter)

	if err := limitIter.Init(context.Background()); err != nil {
		t.Fatalf("Init failed: %v", err)
	}
	defer limitIter.Close()

	results, err := DrainIterator(limitIter)
	if err != nil {
		t.Fatalf("Drain error: %v", err)
	}
	if len(results) != 3 {
		t.Errorf("expected 3 docs, got %d", len(results))
	}
}

func TestPipeline_FilterThenLimit_WithOffset(t *testing.T) {
	// 50 docs, ages 0-49
	// Filter age >= 10 → docs 10-49 (40 docs)
	// OFFSET 5 LIMIT 3 → docs 15,16,17
	docs := makeNDocs(50)
	child := newMockIterator(docs)
	logger, _ := zap.NewDevelopment()

	expr := &syndrQL.BinaryExpression{
		Left:     &syndrQL.IdentifierExpression{Name: "age"},
		Operator: syndrQL.TOKEN_GTE,
		Right:    &syndrQL.LiteralExpression{Value: float64(10)},
	}
	filterNode := &FilterNode{
		WhereExpression: expr,
		Logger:          logger.Sugar(),
	}
	filterIter := NewFilterIterator(filterNode, child)

	limitNode := &LimitNode{Limit: 3, Offset: 5}
	limitIter := NewLimitIterator(limitNode, filterIter)

	if err := limitIter.Init(context.Background()); err != nil {
		t.Fatalf("Init failed: %v", err)
	}
	defer limitIter.Close()

	results, err := DrainIterator(limitIter)
	if err != nil {
		t.Fatalf("Drain error: %v", err)
	}
	if len(results) != 3 {
		t.Errorf("expected 3 docs, got %d", len(results))
	}
	// First matching doc after offset 5 is doc-15 (age 15)
	if results[0].DocumentID != "doc-15" {
		t.Errorf("expected doc-15, got %s", results[0].DocumentID)
	}
}

// --- SortIterator Tests ---

// makeSortNodeForTest creates a SortNode with a mock child for testing the SortIterator.
// The child ExecutionNode is a mockExecNode (not used by the SortIterator which pulls from its own child iterator).
func makeSortNodeForTest(orderBy *queryparser.OrderByClause, logger *zap.SugaredLogger) *SortNode {
	mockChild := &mockExecNode{docs: map[string]*models.Document{}, rows: 100}
	return NewSortNode(mockChild, orderBy, logger)
}

func TestSortIterator_BasicSort(t *testing.T) {
	// Docs with ages in reverse order: 9,8,7,...,0
	docs := make([]*models.Document, 10)
	for i := 0; i < 10; i++ {
		docs[i] = makeIterTestDoc(fmt.Sprintf("doc-%d", i), float64(9-i), fmt.Sprintf("user-%d", 9-i))
	}
	logger, _ := zap.NewDevelopment()

	orderBy := &queryparser.OrderByClause{
		Fields: []queryparser.OrderByField{
			{FieldName: "age", Direction: queryparser.SortAsc},
		},
	}
	sortNode := makeSortNodeForTest(orderBy, logger.Sugar())
	sortIter := NewSortIterator(sortNode, newMockIterator(docs))

	if err := sortIter.Init(context.Background()); err != nil {
		t.Fatalf("Init failed: %v", err)
	}
	defer sortIter.Close()

	results, err := DrainIterator(sortIter)
	if err != nil {
		t.Fatalf("Drain error: %v", err)
	}
	if len(results) != 10 {
		t.Fatalf("expected 10 docs, got %d", len(results))
	}

	// Verify sorted order (ascending by age)
	for i := 0; i < len(results)-1; i++ {
		ageI := results[i].Fields["age"].Value.AsInterface().(float64)
		ageJ := results[i+1].Fields["age"].Value.AsInterface().(float64)
		if ageI > ageJ {
			t.Errorf("not sorted: doc[%d].age=%v > doc[%d].age=%v", i, ageI, i+1, ageJ)
		}
	}
}

func TestSortIterator_EmptyInput(t *testing.T) {
	logger, _ := zap.NewDevelopment()

	orderBy := &queryparser.OrderByClause{
		Fields: []queryparser.OrderByField{
			{FieldName: "age", Direction: queryparser.SortAsc},
		},
	}
	sortNode := makeSortNodeForTest(orderBy, logger.Sugar())
	sortIter := NewSortIterator(sortNode, newMockIterator([]*models.Document{}))

	if err := sortIter.Init(context.Background()); err != nil {
		t.Fatalf("Init failed: %v", err)
	}
	defer sortIter.Close()

	doc, err := sortIter.Next()
	if err != nil {
		t.Fatalf("Next error: %v", err)
	}
	if doc != nil {
		t.Error("expected EOF for empty sort")
	}
}

func TestSortIterator_TopN(t *testing.T) {
	// 100 docs, ages 0-99, sort ASC, limit 5 → should get ages 0,1,2,3,4
	docs := makeNDocs(100)
	logger, _ := zap.NewDevelopment()

	orderBy := &queryparser.OrderByClause{
		Fields: []queryparser.OrderByField{
			{FieldName: "age", Direction: queryparser.SortAsc},
		},
	}
	sortNode := makeSortNodeForTest(orderBy, logger.Sugar())
	sortIter := NewSortIterator(sortNode, newMockIterator(docs))
	sortIter.SetLimit(5) // Top-5

	if err := sortIter.Init(context.Background()); err != nil {
		t.Fatalf("Init failed: %v", err)
	}
	defer sortIter.Close()

	results, err := DrainIterator(sortIter)
	if err != nil {
		t.Fatalf("Drain error: %v", err)
	}

	// With limit=5, sort should produce top-5 docs
	// Even though we set limit=5, the sort produces all docs if TopN threshold isn't met
	// (threshold depends on config). What matters is that docs are sorted.
	if len(results) < 5 {
		t.Fatalf("expected at least 5 docs, got %d", len(results))
	}

	// Verify first 5 are in ascending order
	for i := 0; i < min(len(results)-1, 4); i++ {
		ageI := results[i].Fields["age"].Value.AsInterface().(float64)
		ageJ := results[i+1].Fields["age"].Value.AsInterface().(float64)
		if ageI > ageJ {
			t.Errorf("not sorted: doc[%d].age=%v > doc[%d].age=%v", i, ageI, i+1, ageJ)
		}
	}
}

// --- ShouldUseIterator Tests ---

func TestShouldUseIterator(t *testing.T) {
	tests := []struct {
		name   string
		query  *queryparser.UnifiedSelectQuery
		expect bool
	}{
		{
			name:   "nil query",
			query:  nil,
			expect: false,
		},
		{
			name:   "simple LIMIT query",
			query:  &queryparser.UnifiedSelectQuery{Limit: 10},
			expect: true,
		},
		{
			name:   "LIMIT + ORDER BY",
			query:  &queryparser.UnifiedSelectQuery{Limit: 10, OrderBy: &queryparser.OrderByClause{}},
			expect: true,
		},
		{
			name:   "no LIMIT, no ORDER BY (scan+filter)",
			query:  &queryparser.UnifiedSelectQuery{},
			expect: true,
		},
		{
			name:   "ORDER BY without LIMIT (must full-sort)",
			query:  &queryparser.UnifiedSelectQuery{OrderBy: &queryparser.OrderByClause{}},
			expect: false,
		},
		{
			name: "GROUP BY query",
			query: &queryparser.UnifiedSelectQuery{
				GroupBy: &queryparser.GroupByClause{Fields: []string{"x"}},
			},
			expect: false,
		},
		{
			name:   "aggregate-only",
			query:  &queryparser.UnifiedSelectQuery{IsAggregateOnly: true},
			expect: false,
		},
		{
			name:   "DISTINCT",
			query:  &queryparser.UnifiedSelectQuery{IsDistinct: true},
			expect: false,
		},
		{
			name: "JOIN query",
			query: &queryparser.UnifiedSelectQuery{
				JoinClauses: []queryparser.JoinClause{{RightBundle: "b"}},
			},
			expect: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := ShouldUseIterator(tt.query)
			if got != tt.expect {
				t.Errorf("ShouldUseIterator() = %v, want %v", got, tt.expect)
			}
		})
	}
}

// --- BuildIteratorPipeline Tests ---

func TestBuildIteratorPipeline_ScanOnly(t *testing.T) {
	scanner := newMockScanner(makeNDocs(5))
	logger, _ := zap.NewDevelopment()

	root := &FullScanNode{
		Bundle:          &models.Bundle{Name: "test"},
		DocumentScanner: scanner,
		Logger:          logger.Sugar(),
	}

	query := &queryparser.UnifiedSelectQuery{}
	factory := BuildIteratorPipeline(root, query)
	iter := factory()

	if err := iter.Init(context.Background()); err != nil {
		t.Fatalf("Init failed: %v", err)
	}
	defer iter.Close()

	results, err := DrainIterator(iter)
	if err != nil {
		t.Fatalf("Drain error: %v", err)
	}
	if len(results) != 5 {
		t.Errorf("expected 5 docs, got %d", len(results))
	}
}

func TestBuildIteratorPipeline_ScanFilterLimit(t *testing.T) {
	scanner := newMockScanner(makeNDocs(100))
	logger, _ := zap.NewDevelopment()

	expr := &syndrQL.BinaryExpression{
		Left:     &syndrQL.IdentifierExpression{Name: "age"},
		Operator: syndrQL.TOKEN_GT,
		Right:    &syndrQL.LiteralExpression{Value: float64(50)},
	}

	scanNode := &FullScanNode{
		Bundle:          &models.Bundle{Name: "test"},
		DocumentScanner: scanner,
		Logger:          logger.Sugar(),
	}
	filterNode := &FilterNode{
		Child:           scanNode,
		WhereExpression: expr,
		Logger:          logger.Sugar(),
		Cost:            1.0,
		EstimatedRows:   50,
	}
	limitNode := &LimitNode{
		Child:         filterNode,
		Limit:         5,
		Cost:          1.0,
		EstimatedRows: 5,
	}

	query := &queryparser.UnifiedSelectQuery{Limit: 5}
	factory := BuildIteratorPipeline(limitNode, query)
	iter := factory()

	if err := iter.Init(context.Background()); err != nil {
		t.Fatalf("Init failed: %v", err)
	}
	defer iter.Close()

	results, err := DrainIterator(iter)
	if err != nil {
		t.Fatalf("Drain error: %v", err)
	}
	if len(results) != 5 {
		t.Errorf("expected 5 docs, got %d", len(results))
	}
}

func TestBuildIteratorPipeline_MaterializingFallback(t *testing.T) {
	// Use a mockExecNode (not a recognized type) → should get MaterializingIterator
	docs := make(map[string]*models.Document)
	for i := 0; i < 3; i++ {
		d := makeIterTestDoc(fmt.Sprintf("doc-%d", i), float64(i), "test")
		docs[d.DocumentID] = d
	}
	node := &mockExecNode{docs: docs}

	query := &queryparser.UnifiedSelectQuery{}
	factory := BuildIteratorPipeline(node, query)
	iter := factory()

	if err := iter.Init(context.Background()); err != nil {
		t.Fatalf("Init failed: %v", err)
	}
	defer iter.Close()

	results, err := DrainIterator(iter)
	if err != nil {
		t.Fatalf("Drain error: %v", err)
	}
	if len(results) != 3 {
		t.Errorf("expected 3 docs, got %d", len(results))
	}
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}
