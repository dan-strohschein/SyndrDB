package view

import (
	"testing"

	"go.uber.org/zap"
)

func testLogger() *zap.SugaredLogger {
	logger, _ := zap.NewDevelopment()
	return logger.Sugar()
}

func TestCreateView_Success(t *testing.T) {
	logger := testLogger()
	registry := NewViewRegistry(logger)
	store := &mockStore{}
	svc := NewViewServiceWithRegistry(logger, registry, store)

	v, err := svc.CreateView("ActiveUsers", "testdb", `SELECT * FROM "Users" WHERE "Status" == "Active"`, "admin")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if v.ViewName != "ActiveUsers" {
		t.Errorf("expected ViewName='ActiveUsers', got '%s'", v.ViewName)
	}
	if v.Type != ViewTypeRegular {
		t.Errorf("expected ViewTypeRegular, got %v", v.Type)
	}
	if len(v.ReferencedBundles) != 1 || v.ReferencedBundles[0] != "Users" {
		t.Errorf("expected referenced bundle [Users], got %v", v.ReferencedBundles)
	}
	if !registry.IsView("testdb", "ActiveUsers") {
		t.Error("view not registered in registry")
	}
}

func TestCreateView_Duplicate(t *testing.T) {
	logger := testLogger()
	registry := NewViewRegistry(logger)
	store := &mockStore{}
	svc := NewViewServiceWithRegistry(logger, registry, store)

	_, err := svc.CreateView("V1", "testdb", `SELECT * FROM "A"`, "admin")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	_, err = svc.CreateView("V1", "testdb", `SELECT * FROM "B"`, "admin")
	if err == nil {
		t.Fatal("expected duplicate error, got nil")
	}
}

func TestCreateView_CaseConflict(t *testing.T) {
	logger := testLogger()
	registry := NewViewRegistry(logger)
	store := &mockStore{}
	svc := NewViewServiceWithRegistry(logger, registry, store)

	_, err := svc.CreateView("MyView", "testdb", `SELECT * FROM "X"`, "admin")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	_, err = svc.CreateView("myview", "testdb", `SELECT * FROM "Y"`, "admin")
	if err == nil {
		t.Fatal("expected case conflict error, got nil")
	}
}

func TestCreateView_RejectsAggregates(t *testing.T) {
	logger := testLogger()
	registry := NewViewRegistry(logger)
	store := &mockStore{}
	svc := NewViewServiceWithRegistry(logger, registry, store)

	_, err := svc.CreateView("AggView", "testdb", `SELECT COUNT(*) FROM "Users" GROUP BY "Role"`, "admin")
	if err == nil {
		t.Fatal("expected aggregate rejection error, got nil")
	}
}

func TestDropView_Success(t *testing.T) {
	logger := testLogger()
	registry := NewViewRegistry(logger)
	store := &mockStore{}
	svc := NewViewServiceWithRegistry(logger, registry, store)

	_, err := svc.CreateView("V1", "testdb", `SELECT * FROM "A"`, "admin")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	err = svc.DropView("V1", "testdb", false)
	if err != nil {
		t.Fatalf("unexpected drop error: %v", err)
	}

	if registry.IsView("testdb", "V1") {
		t.Error("view still registered after drop")
	}
}

func TestDropView_NotFound(t *testing.T) {
	logger := testLogger()
	registry := NewViewRegistry(logger)
	store := &mockStore{}
	svc := NewViewServiceWithRegistry(logger, registry, store)

	err := svc.DropView("NonExistent", "testdb", false)
	if err == nil {
		t.Fatal("expected not found error, got nil")
	}
}

func TestDropView_TypeMismatch(t *testing.T) {
	logger := testLogger()
	registry := NewViewRegistry(logger)
	store := &mockStore{}
	svc := NewViewServiceWithRegistry(logger, registry, store)

	_, err := svc.CreateView("V1", "testdb", `SELECT * FROM "A"`, "admin")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Try to drop as materialized - should fail
	err = svc.DropView("V1", "testdb", true)
	if err == nil {
		t.Fatal("expected type mismatch error, got nil")
	}
}

func TestListViews(t *testing.T) {
	logger := testLogger()
	registry := NewViewRegistry(logger)
	store := &mockStore{}
	svc := NewViewServiceWithRegistry(logger, registry, store)

	// Empty list
	views, err := svc.ListViews("testdb")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(views) != 0 {
		t.Errorf("expected 0 views, got %d", len(views))
	}

	// Create two views
	svc.CreateView("V1", "testdb", `SELECT * FROM "A"`, "admin")
	svc.CreateView("V2", "testdb", `SELECT * FROM "B"`, "admin")

	views, err = svc.ListViews("testdb")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(views) != 2 {
		t.Errorf("expected 2 views, got %d", len(views))
	}
}

func TestExtractReferencedBundles(t *testing.T) {
	tests := []struct {
		name       string
		definition string
		expected   []string
	}{
		{"simple", `SELECT * FROM "Users"`, []string{"Users"}},
		{"join", `SELECT * FROM "Users" JOIN "Orders" ON "Users"."id" == "Orders"."uid"`, []string{"Users", "Orders"}},
		{"duplicate", `SELECT * FROM "A" JOIN "A" ON "A"."x" == "A"."y"`, []string{"A"}},
		{"no bundle", `SELECT 1`, nil},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := extractReferencedBundles(tt.definition)
			if len(got) != len(tt.expected) {
				t.Errorf("expected %d bundles, got %d: %v", len(tt.expected), len(got), got)
				return
			}
			for i, b := range got {
				if b != tt.expected[i] {
					t.Errorf("bundle[%d]: expected '%s', got '%s'", i, tt.expected[i], b)
				}
			}
		})
	}
}

func TestGetView(t *testing.T) {
	logger := testLogger()
	registry := NewViewRegistry(logger)
	store := &mockStore{}
	svc := NewViewServiceWithRegistry(logger, registry, store)

	svc.CreateView("V1", "testdb", `SELECT * FROM "A"`, "admin")

	v, err := svc.GetView("testdb", "V1")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if v.ViewName != "V1" {
		t.Errorf("expected V1, got %s", v.ViewName)
	}

	_, err = svc.GetView("testdb", "Nonexistent")
	if err == nil {
		t.Fatal("expected error for nonexistent view")
	}
}

// mockStore implements ViewStore for testing
type mockStore struct {
	views map[string]*View
}

func (m *mockStore) SaveView(view *View) error {
	if m.views == nil {
		m.views = make(map[string]*View)
	}
	key := view.DatabaseName + ":" + view.ViewName
	m.views[key] = view
	return nil
}

func (m *mockStore) LoadView(databaseName, viewName string) (*View, error) {
	if m.views == nil {
		return nil, nil
	}
	key := databaseName + ":" + viewName
	v, ok := m.views[key]
	if !ok {
		return nil, nil
	}
	return v, nil
}

func (m *mockStore) DeleteView(databaseName, viewName string) error {
	if m.views != nil {
		key := databaseName + ":" + viewName
		delete(m.views, key)
	}
	return nil
}

func (m *mockStore) ListViews(databaseName string) ([]*View, error) {
	return nil, nil
}

func (m *mockStore) SyncToCatalog(view *View) error {
	return nil
}
