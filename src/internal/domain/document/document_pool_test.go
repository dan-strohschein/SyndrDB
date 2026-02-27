package document

import (
	"strings"
	"sync"
	"syndrdb/src/internal/domain/models"
	"testing"
	"time"
)

// ---------------------------------------------------------------------------
// A. DocumentPool Core (Get/Put Document)
// ---------------------------------------------------------------------------

func TestGetDocument_ReturnsCleanDocument(t *testing.T) {
	pool := NewDocumentPool()
	doc := pool.GetDocument()

	if doc.DocumentID != "" {
		t.Errorf("expected empty DocumentID, got %q", doc.DocumentID)
	}
	if doc.Values != nil {
		t.Errorf("expected nil Values, got %v", doc.Values)
	}
	if doc.CachedJSON != nil {
		t.Errorf("expected nil CachedJSON, got %v", doc.CachedJSON)
	}
	if doc.PooledValues {
		t.Error("expected PooledValues=false")
	}
}

func TestPutDocument_ClearsAllFields(t *testing.T) {
	pool := NewDocumentPool()
	doc := pool.GetDocument()

	// Populate fields
	doc.DocumentID = "test-id-123"
	doc.Data = map[string]interface{}{"key": "val"}
	doc.CachedJSON = map[string][]byte{"f": []byte("data")}
	doc.Values = []models.FieldValue{models.NewStringValue("hello")}

	pool.PutDocument(doc)
	doc2 := pool.GetDocument()

	if doc2.DocumentID != "" {
		t.Errorf("expected cleared DocumentID, got %q", doc2.DocumentID)
	}
	if doc2.Data != nil {
		t.Errorf("expected nil Data, got %v", doc2.Data)
	}
	if doc2.CachedJSON != nil {
		t.Errorf("expected nil CachedJSON, got %v", doc2.CachedJSON)
	}
	if doc2.Values != nil {
		t.Errorf("expected nil Values, got %v", doc2.Values)
	}
}

func TestPutDocument_NilSafe(t *testing.T) {
	pool := NewDocumentPool()
	// Must not panic
	pool.PutDocument(nil)
}

func TestPutDocument_ReturnsPooledValues(t *testing.T) {
	pool := NewDocumentPool()
	doc := pool.GetDocument()

	// Simulate pooled values path
	vals := pool.GetValuesSlice(8)
	vals = vals[:4]
	doc.Values = vals
	doc.PooledValues = true

	pool.PutDocument(doc)

	// After returning, the values slice should be back in the pool.
	// We can verify the pool is functional by getting another slice.
	got := pool.GetValuesSlice(4)
	if got == nil {
		t.Error("expected non-nil values slice from pool after return")
	}
}

func TestPutDocument_SkipsNonPooledValues(t *testing.T) {
	pool := NewDocumentPool()
	doc := pool.GetDocument()

	doc.Values = []models.FieldValue{models.NewIntValue(42)}
	doc.PooledValues = false

	// Should not panic and should clear the document
	pool.PutDocument(doc)

	doc2 := pool.GetDocument()
	if doc2.Values != nil {
		t.Error("expected nil Values after put")
	}
}

// ---------------------------------------------------------------------------
// B. Values Slice Pool
// ---------------------------------------------------------------------------

func TestGetValuesSlice_DefaultCapacity(t *testing.T) {
	pool := NewDocumentPool()
	s := pool.GetValuesSlice(8)

	if len(s) != 0 {
		t.Errorf("expected len=0, got %d", len(s))
	}
	if cap(s) < 8 {
		t.Errorf("expected cap>=8, got %d", cap(s))
	}
}

func TestGetValuesSlice_LargerThanPool(t *testing.T) {
	pool := NewDocumentPool()
	// Pool default cap is 16; request 100
	s := pool.GetValuesSlice(100)

	if cap(s) < 100 {
		t.Errorf("expected cap>=100, got %d", cap(s))
	}
	if len(s) != 0 {
		t.Errorf("expected len=0, got %d", len(s))
	}
}

func TestGetValuesSlice_ExactMinCap(t *testing.T) {
	pool := NewDocumentPool()

	for _, minCap := range []int{1, 5, 16, 32, 64} {
		s := pool.GetValuesSlice(minCap)
		if cap(s) < minCap {
			t.Errorf("minCap=%d: expected cap>=%d, got %d", minCap, minCap, cap(s))
		}
	}
}

func TestPutValuesSlice_NilSafe(t *testing.T) {
	pool := NewDocumentPool()
	// Must not panic
	pool.PutValuesSlice(nil)
}

func TestPutValuesSlice_ResetsLength(t *testing.T) {
	pool := NewDocumentPool()
	s := pool.GetValuesSlice(8)
	s = append(s, models.NewIntValue(1), models.NewIntValue(2), models.NewIntValue(3))
	if len(s) != 3 {
		t.Fatalf("expected len=3, got %d", len(s))
	}

	pool.PutValuesSlice(s)
	s2 := pool.GetValuesSlice(3)

	if len(s2) != 0 {
		t.Errorf("expected len=0 after put+get, got %d", len(s2))
	}
}

// ---------------------------------------------------------------------------
// C. Global Singleton API
// ---------------------------------------------------------------------------

func TestGlobalPooledDocument_RoundTrip(t *testing.T) {
	doc := GetPooledDocument()
	doc.DocumentID = "global-test"
	doc.Data = map[string]interface{}{"x": 1}
	ReturnPooledDocument(doc)

	doc2 := GetPooledDocument()
	if doc2.DocumentID != "" {
		t.Errorf("expected cleared DocumentID, got %q", doc2.DocumentID)
	}
	if doc2.Data != nil {
		t.Errorf("expected nil Data, got %v", doc2.Data)
	}
	ReturnPooledDocument(doc2)
}

func TestGlobalPooledValuesSlice_RoundTrip(t *testing.T) {
	s := GetPooledValuesSlice(10)
	if cap(s) < 10 {
		t.Errorf("expected cap>=10, got %d", cap(s))
	}
	s = append(s, models.NewStringValue("test"))
	ReturnPooledValuesSlice(s)

	s2 := GetPooledValuesSlice(5)
	if len(s2) != 0 {
		t.Errorf("expected len=0, got %d", len(s2))
	}
	ReturnPooledValuesSlice(s2)
}

func TestFreeDocuments_BatchReturn(t *testing.T) {
	docs := make([]*models.Document, 5)
	for i := range docs {
		docs[i] = GetPooledDocument()
		docs[i].DocumentID = "batch-doc"
	}

	FreeDocuments(docs)

	// Pool should still be functional
	doc := GetPooledDocument()
	if doc.DocumentID != "" {
		t.Errorf("expected cleared DocumentID, got %q", doc.DocumentID)
	}
	ReturnPooledDocument(doc)
}

func TestFreeDocuments_NilSlice(t *testing.T) {
	// Must not panic
	FreeDocuments(nil)
}

func TestFreeDocuments_NilElements(t *testing.T) {
	docs := []*models.Document{
		GetPooledDocument(),
		nil,
		GetPooledDocument(),
		nil,
	}
	// Must not panic
	FreeDocuments(docs)
}

func TestFreeDocuments_EmptySlice(t *testing.T) {
	// Must not panic
	FreeDocuments([]*models.Document{})
}

// ---------------------------------------------------------------------------
// D. DocumentFactory
// ---------------------------------------------------------------------------

func makeTestSchema(fields ...string) *models.BundleFieldSchema {
	names := make([]string, 0, len(fields)+1)
	names = append(names, "DocumentID")
	names = append(names, fields...)

	nameToIndex := make(map[string]int, len(names))
	lowerNameToIndex := make(map[string]int, len(names))
	for i, n := range names {
		nameToIndex[n] = i
		lowerNameToIndex[strings.ToLower(n)] = i
	}
	return &models.BundleFieldSchema{
		Names:            names,
		NameToIndex:      nameToIndex,
		LowerNameToIndex: lowerNameToIndex,
	}
}

func TestNewDocumentFactory_ReturnsNonNil(t *testing.T) {
	f := NewDocumentFactory()
	if f == nil {
		t.Fatal("expected non-nil DocumentFactory")
	}
}

func TestNewDocument_SetsDocumentID(t *testing.T) {
	f := NewDocumentFactory()
	schema := makeTestSchema("Name", "Age")
	cmd := models.DocumentCommand{
		Fields: []models.KeyValue{
			{Key: "Name", Value: "Alice"},
			{Key: "Age", Value: int64(30)},
		},
	}

	doc := f.NewDocument(cmd, schema)
	defer ReturnPooledDocument(doc)

	if doc.DocumentID == "" {
		t.Error("expected non-empty DocumentID")
	}
}

func TestNewDocument_SetsTimestamps(t *testing.T) {
	f := NewDocumentFactory()
	schema := makeTestSchema("X")
	cmd := models.DocumentCommand{
		Fields: []models.KeyValue{{Key: "X", Value: "val"}},
	}

	before := time.Now().Add(-time.Second)
	doc := f.NewDocument(cmd, schema)
	after := time.Now().Add(time.Second)
	defer ReturnPooledDocument(doc)

	if doc.CreatedAt.IsZero() {
		t.Error("CreatedAt is zero")
	}
	if doc.UpdatedAt.IsZero() {
		t.Error("UpdatedAt is zero")
	}
	if doc.CreatedAt.Before(before) || doc.CreatedAt.After(after) {
		t.Errorf("CreatedAt %v outside expected window [%v, %v]", doc.CreatedAt, before, after)
	}
}

func TestNewDocument_ValuesMatchSchema(t *testing.T) {
	f := NewDocumentFactory()
	schema := makeTestSchema("Name", "Age")
	cmd := models.DocumentCommand{
		Fields: []models.KeyValue{
			{Key: "Name", Value: "Bob"},
			{Key: "Age", Value: int64(25)},
		},
	}

	doc := f.NewDocument(cmd, schema)
	defer ReturnPooledDocument(doc)

	if len(doc.Values) != len(schema.Names) {
		t.Fatalf("expected Values len=%d, got %d", len(schema.Names), len(doc.Values))
	}

	// Index 0 = DocumentID
	if doc.Values[0].Type != models.FieldTypeString {
		t.Errorf("Values[0] (DocumentID) type=%v, want FieldTypeString", doc.Values[0].Type)
	}
	if s, ok := doc.Values[0].AsString(); !ok || s != doc.DocumentID {
		t.Errorf("Values[0] (DocumentID) = %q, want %q", s, doc.DocumentID)
	}
}

func TestNewDocument_FieldsFromCommand(t *testing.T) {
	f := NewDocumentFactory()
	schema := makeTestSchema("Name", "Score")
	cmd := models.DocumentCommand{
		Fields: []models.KeyValue{
			{Key: "Name", Value: "Charlie"},
			{Key: "Score", Value: float64(99.5)},
		},
	}

	doc := f.NewDocument(cmd, schema)
	defer ReturnPooledDocument(doc)

	// Name at index 1
	nameIdx := schema.NameToIndex["Name"]
	if s, ok := doc.Values[nameIdx].AsString(); !ok || s != "Charlie" {
		t.Errorf("Name value = %q, want %q", s, "Charlie")
	}

	// Score at index 2
	scoreIdx := schema.NameToIndex["Score"]
	if v, ok := doc.Values[scoreIdx].AsFloat(); !ok || v != 99.5 {
		t.Errorf("Score value = %v, want 99.5", v)
	}
}

func TestNewDocument_MissingFieldsAreNil(t *testing.T) {
	f := NewDocumentFactory()
	schema := makeTestSchema("Name", "Age", "Email")
	cmd := models.DocumentCommand{
		Fields: []models.KeyValue{
			{Key: "Name", Value: "Dan"},
			// Age and Email missing
		},
	}

	doc := f.NewDocument(cmd, schema)
	defer ReturnPooledDocument(doc)

	ageIdx := schema.NameToIndex["Age"]
	if doc.Values[ageIdx].Type != models.FieldTypeNil {
		t.Errorf("missing Age should be FieldTypeNil, got %v", doc.Values[ageIdx].Type)
	}

	emailIdx := schema.NameToIndex["Email"]
	if doc.Values[emailIdx].Type != models.FieldTypeNil {
		t.Errorf("missing Email should be FieldTypeNil, got %v", doc.Values[emailIdx].Type)
	}
}

func TestNewDocumentWithID_UsesProvidedID(t *testing.T) {
	f := NewDocumentFactory()
	schema := makeTestSchema("X")
	cmd := models.DocumentCommand{
		Fields: []models.KeyValue{{Key: "X", Value: "val"}},
	}

	doc := f.NewDocumentWithID(cmd, "custom-id-42", schema)
	defer ReturnPooledDocument(doc)

	if doc.DocumentID != "custom-id-42" {
		t.Errorf("expected DocumentID=%q, got %q", "custom-id-42", doc.DocumentID)
	}
}

func TestNewDocumentWithID_ValuesMatchSchema(t *testing.T) {
	f := NewDocumentFactory()
	schema := makeTestSchema("Color")
	cmd := models.DocumentCommand{
		Fields: []models.KeyValue{{Key: "Color", Value: "blue"}},
	}

	doc := f.NewDocumentWithID(cmd, "my-id", schema)
	defer ReturnPooledDocument(doc)

	if len(doc.Values) != len(schema.Names) {
		t.Fatalf("expected Values len=%d, got %d", len(schema.Names), len(doc.Values))
	}

	// DocumentID at index 0
	if s, ok := doc.Values[0].AsString(); !ok || s != "my-id" {
		t.Errorf("Values[0] (DocumentID) = %q, want %q", s, "my-id")
	}

	// Color at index 1
	colorIdx := schema.NameToIndex["Color"]
	if s, ok := doc.Values[colorIdx].AsString(); !ok || s != "blue" {
		t.Errorf("Color value = %q, want %q", s, "blue")
	}
}

// ---------------------------------------------------------------------------
// E. MakeDocumentValues
// ---------------------------------------------------------------------------

func TestMakeDocumentValues_NilSchema(t *testing.T) {
	f := &DocumentFactoryImpl{}
	result := f.MakeDocumentValues(models.DocumentCommand{}, "id", nil)
	if result != nil {
		t.Errorf("expected nil for nil schema, got %v", result)
	}
}

func TestMakeDocumentValues_EmptySchema(t *testing.T) {
	f := &DocumentFactoryImpl{}
	schema := &models.BundleFieldSchema{
		Names:            []string{},
		NameToIndex:      map[string]int{},
		LowerNameToIndex: map[string]int{},
	}
	result := f.MakeDocumentValues(models.DocumentCommand{}, "id", schema)
	if result != nil {
		t.Errorf("expected nil for empty schema, got %v", result)
	}
}

func TestMakeDocumentValues_DocumentIDPlacement(t *testing.T) {
	f := &DocumentFactoryImpl{}
	schema := makeTestSchema("Name")
	cmd := models.DocumentCommand{
		Fields: []models.KeyValue{{Key: "Name", Value: "test"}},
	}

	values := f.MakeDocumentValues(cmd, "doc-abc", schema)

	docIDIdx := schema.NameToIndex["DocumentID"]
	if s, ok := values[docIDIdx].AsString(); !ok || s != "doc-abc" {
		t.Errorf("DocumentID at index %d = %q, want %q", docIDIdx, s, "doc-abc")
	}
}

func TestMakeDocumentValues_AllFieldsPresent(t *testing.T) {
	f := &DocumentFactoryImpl{}
	schema := makeTestSchema("A", "B", "C")
	cmd := models.DocumentCommand{
		Fields: []models.KeyValue{
			{Key: "A", Value: "alpha"},
			{Key: "B", Value: int64(42)},
			{Key: "C", Value: true},
		},
	}

	values := f.MakeDocumentValues(cmd, "id", schema)

	if len(values) != len(schema.Names) {
		t.Fatalf("values len=%d, want %d", len(values), len(schema.Names))
	}

	aIdx := schema.NameToIndex["A"]
	if s, ok := values[aIdx].AsString(); !ok || s != "alpha" {
		t.Errorf("A = %q, want %q", s, "alpha")
	}

	bIdx := schema.NameToIndex["B"]
	if v, ok := values[bIdx].AsInt(); !ok || v != 42 {
		t.Errorf("B = %v, want 42", v)
	}

	cIdx := schema.NameToIndex["C"]
	if v, ok := values[cIdx].AsBool(); !ok || v != true {
		t.Errorf("C = %v, want true", v)
	}
}

func TestMakeDocumentValues_MissingFields(t *testing.T) {
	f := &DocumentFactoryImpl{}
	schema := makeTestSchema("Present", "Missing")
	cmd := models.DocumentCommand{
		Fields: []models.KeyValue{{Key: "Present", Value: "here"}},
	}

	values := f.MakeDocumentValues(cmd, "id", schema)

	missingIdx := schema.NameToIndex["Missing"]
	if values[missingIdx].Type != models.FieldTypeNil {
		t.Errorf("missing field type=%v, want FieldTypeNil", values[missingIdx].Type)
	}
}

func TestMakeDocumentValues_NilFieldValue(t *testing.T) {
	f := &DocumentFactoryImpl{}
	schema := makeTestSchema("Field")
	cmd := models.DocumentCommand{
		Fields: []models.KeyValue{{Key: "Field", Value: nil}},
	}

	values := f.MakeDocumentValues(cmd, "id", schema)

	fieldIdx := schema.NameToIndex["Field"]
	if values[fieldIdx].Type != models.FieldTypeNil {
		t.Errorf("nil field value type=%v, want FieldTypeNil", values[fieldIdx].Type)
	}
}

func TestMakeDocumentValues_CaseInsensitiveDocID(t *testing.T) {
	f := &DocumentFactoryImpl{}

	// Schema with lowercase "documentid" as the name
	schema := &models.BundleFieldSchema{
		Names:            []string{"documentid", "Name"},
		NameToIndex:      map[string]int{"documentid": 0, "Name": 1},
		LowerNameToIndex: map[string]int{"documentid": 0, "name": 1},
	}
	cmd := models.DocumentCommand{
		Fields: []models.KeyValue{{Key: "Name", Value: "test"}},
	}

	values := f.MakeDocumentValues(cmd, "my-doc-id", schema)

	if s, ok := values[0].AsString(); !ok || s != "my-doc-id" {
		t.Errorf("documentid (lowercase) = %q, want %q", s, "my-doc-id")
	}
}

// ---------------------------------------------------------------------------
// F. Concurrency Safety
// ---------------------------------------------------------------------------

func TestDocumentPool_ConcurrentGetPut(t *testing.T) {
	pool := NewDocumentPool()
	const goroutines = 100
	const iterations = 100

	var wg sync.WaitGroup
	wg.Add(goroutines)

	for g := 0; g < goroutines; g++ {
		go func() {
			defer wg.Done()
			for i := 0; i < iterations; i++ {
				doc := pool.GetDocument()
				doc.DocumentID = "concurrent-test"
				doc.Data = map[string]interface{}{"k": i}
				pool.PutDocument(doc)
			}
		}()
	}

	wg.Wait()
}

func TestValuesPool_ConcurrentGetPut(t *testing.T) {
	pool := NewDocumentPool()
	const goroutines = 100
	const iterations = 100

	var wg sync.WaitGroup
	wg.Add(goroutines)

	for g := 0; g < goroutines; g++ {
		go func() {
			defer wg.Done()
			for i := 0; i < iterations; i++ {
				s := pool.GetValuesSlice(8)
				s = append(s, models.NewIntValue(int64(i)))
				pool.PutValuesSlice(s)
			}
		}()
	}

	wg.Wait()
}

func TestGlobalPool_ConcurrentMixed(t *testing.T) {
	const goroutines = 100
	const iterations = 50

	var wg sync.WaitGroup
	wg.Add(goroutines * 3)

	// Concurrent GetPooledDocument / ReturnPooledDocument
	for g := 0; g < goroutines; g++ {
		go func() {
			defer wg.Done()
			for i := 0; i < iterations; i++ {
				doc := GetPooledDocument()
				doc.DocumentID = "mixed"
				ReturnPooledDocument(doc)
			}
		}()
	}

	// Concurrent GetPooledValuesSlice / ReturnPooledValuesSlice
	for g := 0; g < goroutines; g++ {
		go func() {
			defer wg.Done()
			for i := 0; i < iterations; i++ {
				s := GetPooledValuesSlice(4)
				s = append(s, models.NewStringValue("v"))
				ReturnPooledValuesSlice(s)
			}
		}()
	}

	// Concurrent FreeDocuments
	for g := 0; g < goroutines; g++ {
		go func() {
			defer wg.Done()
			for i := 0; i < iterations; i++ {
				docs := []*models.Document{GetPooledDocument(), GetPooledDocument()}
				FreeDocuments(docs)
			}
		}()
	}

	wg.Wait()
}

func TestDocumentFactory_ConcurrentNewDocument(t *testing.T) {
	f := NewDocumentFactory()
	schema := makeTestSchema("Name", "Value")
	const goroutines = 100
	const iterations = 50

	var wg sync.WaitGroup
	wg.Add(goroutines)

	for g := 0; g < goroutines; g++ {
		go func(id int) {
			defer wg.Done()
			for i := 0; i < iterations; i++ {
				cmd := models.DocumentCommand{
					Fields: []models.KeyValue{
						{Key: "Name", Value: "goroutine"},
						{Key: "Value", Value: int64(id*1000 + i)},
					},
				}
				doc := f.NewDocument(cmd, schema)
				if doc.DocumentID == "" {
					t.Error("concurrent NewDocument produced empty DocumentID")
				}
				if len(doc.Values) != len(schema.Names) {
					t.Errorf("concurrent NewDocument values len=%d, want %d", len(doc.Values), len(schema.Names))
				}
				ReturnPooledDocument(doc)
			}
		}(g)
	}

	wg.Wait()
}
