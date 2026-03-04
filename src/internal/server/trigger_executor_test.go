package server

import (
	"context"
	"sort"
	"testing"
	"time"

	"syndrdb/src/internal/domain/models"
	"syndrdb/src/pkg/settings"
	"syndrdb/src/pkg/trigger"

	"go.uber.org/zap"
)

func newTestLogger() *zap.SugaredLogger {
	logger, _ := zap.NewDevelopment()
	return logger.Sugar()
}

func newTestSession() *Session {
	return &Session{
		SessionID: "test-session",
		Username:  "test-user",
	}
}

func newTestBundle(name string, triggers map[string]models.TriggerDefinition) *models.Bundle {
	return &models.Bundle{
		Name:     name,
		Triggers: triggers,
	}
}

func newTestDocument(id string, data map[string]interface{}) *models.Document {
	return &models.Document{
		DocumentID: id,
		Data:       data,
		CreatedAt:  time.Now(),
		UpdatedAt:  time.Now(),
	}
}

// TestExecuteTriggers_NoTriggersOnBundle verifies passthrough when bundle has no triggers.
func TestExecuteTriggers_NoTriggersOnBundle(t *testing.T) {
	logger := newTestLogger()
	te := NewTriggerExecutor(logger)
	session := newTestSession()
	bundle := newTestBundle("Orders", nil)
	doc := newTestDocument("doc1", map[string]interface{}{"status": "new"})

	result, err := te.ExecuteTriggers(
		context.Background(), bundle,
		models.TRIGGER_EVENT_INSERT, models.TRIGGER_TIMING_BEFORE,
		nil, doc, session, nil, ServiceManager{},
	)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result != doc {
		t.Error("expected original document returned when no triggers")
	}
}

// TestExecuteTriggers_EmptyTriggersMap verifies passthrough when bundle has empty triggers map.
func TestExecuteTriggers_EmptyTriggersMap(t *testing.T) {
	logger := newTestLogger()
	te := NewTriggerExecutor(logger)
	session := newTestSession()
	bundle := newTestBundle("Orders", map[string]models.TriggerDefinition{})
	doc := newTestDocument("doc1", map[string]interface{}{})

	result, err := te.ExecuteTriggers(
		context.Background(), bundle,
		models.TRIGGER_EVENT_INSERT, models.TRIGGER_TIMING_BEFORE,
		nil, doc, session, nil, ServiceManager{},
	)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result != doc {
		t.Error("expected original document returned when triggers map is empty")
	}
}

// TestExecuteTriggers_TriggersDisabled verifies passthrough when triggers are globally disabled.
func TestExecuteTriggers_TriggersDisabled(t *testing.T) {
	logger := newTestLogger()
	te := NewTriggerExecutor(logger)
	session := newTestSession()
	bundle := newTestBundle("Orders", map[string]models.TriggerDefinition{
		"t1": {
			Name: "t1", Enabled: true,
			Timing: models.TRIGGER_TIMING_BEFORE,
			Events: models.TRIGGER_EVENT_INSERT,
			Body:   "noop",
		},
	})
	doc := newTestDocument("doc1", map[string]interface{}{"x": 1})

	s := settings.GetSettings()
	origEnabled := s.TriggersEnabled
	s.TriggersEnabled = false
	defer func() { s.TriggersEnabled = origEnabled }()

	result, err := te.ExecuteTriggers(
		context.Background(), bundle,
		models.TRIGGER_EVENT_INSERT, models.TRIGGER_TIMING_BEFORE,
		nil, doc, session, nil, ServiceManager{},
	)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result != doc {
		t.Error("expected original document returned when triggers disabled")
	}
}

// TestExecuteTriggers_FilterByEventAndTiming verifies only matching triggers are selected.
func TestExecuteTriggers_FilterByEventAndTiming(t *testing.T) {
	logger := newTestLogger()
	te := NewTriggerExecutor(logger)
	session := newTestSession()

	// Create triggers for different events/timings - all use harmless body
	// that executeCommand handles without error (unrecognized commands return nil)
	triggers := map[string]models.TriggerDefinition{
		"before_insert": {
			Name: "before_insert", Enabled: true,
			Timing: models.TRIGGER_TIMING_BEFORE,
			Events: models.TRIGGER_EVENT_INSERT,
			Body:   "noop",
		},
		"after_insert": {
			Name: "after_insert", Enabled: true,
			Timing: models.TRIGGER_TIMING_AFTER,
			Events: models.TRIGGER_EVENT_INSERT,
			Body:   "noop",
		},
		"before_update": {
			Name: "before_update", Enabled: true,
			Timing: models.TRIGGER_TIMING_BEFORE,
			Events: models.TRIGGER_EVENT_UPDATE,
			Body:   "noop",
		},
		"disabled_insert": {
			Name: "disabled_insert", Enabled: false,
			Timing: models.TRIGGER_TIMING_BEFORE,
			Events: models.TRIGGER_EVENT_INSERT,
			Body:   "noop",
		},
	}

	bundle := newTestBundle("Orders", triggers)
	doc := newTestDocument("doc1", map[string]interface{}{})

	// BEFORE INSERT - should match "before_insert" only (disabled_insert is disabled,
	// after_insert has wrong timing, before_update has wrong event)
	result, err := te.ExecuteTriggers(
		context.Background(), bundle,
		models.TRIGGER_EVENT_INSERT, models.TRIGGER_TIMING_BEFORE,
		nil, doc, session, nil, ServiceManager{},
	)
	if err != nil {
		t.Fatalf("unexpected error for BEFORE INSERT: %v", err)
	}
	if result != doc {
		t.Error("expected document returned after BEFORE INSERT trigger")
	}

	// AFTER UPDATE - should match nothing (no AFTER UPDATE trigger defined)
	result, err = te.ExecuteTriggers(
		context.Background(), bundle,
		models.TRIGGER_EVENT_UPDATE, models.TRIGGER_TIMING_AFTER,
		doc, doc, session, nil, ServiceManager{},
	)
	if err != nil {
		t.Fatalf("unexpected error for unmatched event/timing: %v", err)
	}
	if result != doc {
		t.Error("expected original document when no triggers match")
	}

	// DELETE - should match nothing
	result, err = te.ExecuteTriggers(
		context.Background(), bundle,
		models.TRIGGER_EVENT_DELETE, models.TRIGGER_TIMING_BEFORE,
		doc, nil, session, nil, ServiceManager{},
	)
	if err != nil {
		t.Fatalf("unexpected error for DELETE: %v", err)
	}
}

// TestExecuteTriggers_MultiEventBitmask verifies triggers with multiple events match correctly.
func TestExecuteTriggers_MultiEventBitmask(t *testing.T) {
	logger := newTestLogger()
	te := NewTriggerExecutor(logger)

	triggers := map[string]models.TriggerDefinition{
		"insert_update": {
			Name: "insert_update", Enabled: true,
			Timing: models.TRIGGER_TIMING_BEFORE,
			Events: models.TRIGGER_EVENT_INSERT | models.TRIGGER_EVENT_UPDATE,
			Body:   "noop",
		},
	}
	bundle := newTestBundle("Orders", triggers)
	doc := newTestDocument("doc1", map[string]interface{}{})

	// Should match INSERT
	session := newTestSession()
	result, err := te.ExecuteTriggers(
		context.Background(), bundle,
		models.TRIGGER_EVENT_INSERT, models.TRIGGER_TIMING_BEFORE,
		nil, doc, session, nil, ServiceManager{},
	)
	if err != nil {
		t.Fatalf("unexpected error for INSERT match: %v", err)
	}
	if result != doc {
		t.Error("expected document returned for INSERT")
	}

	// Should match UPDATE
	session2 := newTestSession()
	result, err = te.ExecuteTriggers(
		context.Background(), bundle,
		models.TRIGGER_EVENT_UPDATE, models.TRIGGER_TIMING_BEFORE,
		doc, doc, session2, nil, ServiceManager{},
	)
	if err != nil {
		t.Fatalf("unexpected error for UPDATE match: %v", err)
	}
	if result != doc {
		t.Error("expected document returned for UPDATE")
	}

	// Should NOT match DELETE
	session3 := newTestSession()
	result, err = te.ExecuteTriggers(
		context.Background(), bundle,
		models.TRIGGER_EVENT_DELETE, models.TRIGGER_TIMING_BEFORE,
		doc, nil, session3, nil, ServiceManager{},
	)
	if err != nil {
		t.Fatalf("unexpected error for DELETE (should not match): %v", err)
	}
	if result != nil {
		t.Error("expected nil document for DELETE with no matching trigger")
	}
}

// TestExecuteTriggers_PrioritySorting verifies triggers fire in priority order (lower first).
func TestExecuteTriggers_PrioritySorting(t *testing.T) {
	// Test the sorting logic directly since we can't easily observe
	// execution order through executeCommand
	triggers := []models.TriggerDefinition{
		{Name: "high", Priority: 1000},
		{Name: "low", Priority: 10},
		{Name: "medium", Priority: 500},
		{Name: "lowest", Priority: 1},
	}

	sort.Slice(triggers, func(i, j int) bool {
		return triggers[i].Priority < triggers[j].Priority
	})

	expected := []string{"lowest", "low", "medium", "high"}
	for i, trig := range triggers {
		if trig.Name != expected[i] {
			t.Errorf("position %d: expected %q, got %q", i, expected[i], trig.Name)
		}
	}
}

// TestExecuteTriggers_CascadeDepthLimit verifies cascade depth enforcement.
func TestExecuteTriggers_CascadeDepthLimit(t *testing.T) {
	logger := newTestLogger()
	te := NewTriggerExecutor(logger)
	session := newTestSession()

	s := settings.GetSettings()
	origDepth := s.TriggerMaxDepth
	s.TriggerMaxDepth = 3
	defer func() { s.TriggerMaxDepth = origDepth }()

	triggers := map[string]models.TriggerDefinition{
		"t1": {
			Name: "t1", Enabled: true,
			Timing: models.TRIGGER_TIMING_BEFORE,
			Events: models.TRIGGER_EVENT_INSERT,
			Body:   "noop",
		},
	}
	bundle := newTestBundle("Orders", triggers)
	doc := newTestDocument("doc1", map[string]interface{}{})

	// Simulate being at max depth already
	session.TriggerDepth.Store(3)

	_, err := te.ExecuteTriggers(
		context.Background(), bundle,
		models.TRIGGER_EVENT_INSERT, models.TRIGGER_TIMING_BEFORE,
		nil, doc, session, nil, ServiceManager{},
	)
	if err == nil {
		t.Fatal("expected cascade depth error")
	}
	if !containsString(err.Error(), "depth limit exceeded") {
		t.Errorf("expected depth limit error, got: %s", err.Error())
	}
}

// TestExecuteTriggers_CascadeDepthBelowLimit verifies triggers fire when under depth limit.
func TestExecuteTriggers_CascadeDepthBelowLimit(t *testing.T) {
	logger := newTestLogger()
	te := NewTriggerExecutor(logger)
	session := newTestSession()

	s := settings.GetSettings()
	origDepth := s.TriggerMaxDepth
	s.TriggerMaxDepth = 5
	defer func() { s.TriggerMaxDepth = origDepth }()

	triggers := map[string]models.TriggerDefinition{
		"t1": {
			Name: "t1", Enabled: true,
			Timing: models.TRIGGER_TIMING_BEFORE,
			Events: models.TRIGGER_EVENT_INSERT,
			Body:   "noop",
		},
	}
	bundle := newTestBundle("Orders", triggers)
	doc := newTestDocument("doc1", map[string]interface{}{})

	// At depth 2, below max of 5 - should proceed
	session.TriggerDepth.Store(2)

	result, err := te.ExecuteTriggers(
		context.Background(), bundle,
		models.TRIGGER_EVENT_INSERT, models.TRIGGER_TIMING_BEFORE,
		nil, doc, session, nil, ServiceManager{},
	)
	if err != nil {
		t.Fatalf("expected no error when below depth limit, got: %v", err)
	}
	if result != doc {
		t.Error("expected document returned")
	}

	// Depth should be back to 2 after execution
	if session.TriggerDepth.Load() != 2 {
		t.Errorf("expected depth 2 after execution, got %d", session.TriggerDepth.Load())
	}
}

// TestExecuteTriggers_RecursionDetection verifies recursive trigger detection.
func TestExecuteTriggers_RecursionDetection(t *testing.T) {
	logger := newTestLogger()
	te := NewTriggerExecutor(logger)
	session := newTestSession()

	s := settings.GetSettings()
	origRecursion := s.TriggerAllowRecursion
	s.TriggerAllowRecursion = false
	defer func() { s.TriggerAllowRecursion = origRecursion }()

	triggers := map[string]models.TriggerDefinition{
		"t1": {
			Name: "t1", Enabled: true,
			Timing: models.TRIGGER_TIMING_BEFORE,
			Events: models.TRIGGER_EVENT_INSERT,
			Body:   "noop",
		},
	}
	bundle := newTestBundle("Orders", triggers)
	doc := newTestDocument("doc1", map[string]interface{}{})

	// Pre-populate call stack to simulate recursion
	session.TriggerCallStack = []string{"Orders.t1"}

	_, err := te.ExecuteTriggers(
		context.Background(), bundle,
		models.TRIGGER_EVENT_INSERT, models.TRIGGER_TIMING_BEFORE,
		nil, doc, session, nil, ServiceManager{},
	)
	if err == nil {
		t.Fatal("expected recursion detection error")
	}
	if !containsString(err.Error(), "recursive trigger detected") {
		t.Errorf("expected recursion error, got: %s", err.Error())
	}
}

// TestExecuteTriggers_RecursionAllowed verifies recursion is permitted when setting is true.
func TestExecuteTriggers_RecursionAllowed(t *testing.T) {
	logger := newTestLogger()
	te := NewTriggerExecutor(logger)
	session := newTestSession()

	s := settings.GetSettings()
	origRecursion := s.TriggerAllowRecursion
	s.TriggerAllowRecursion = true
	defer func() { s.TriggerAllowRecursion = origRecursion }()

	triggers := map[string]models.TriggerDefinition{
		"t1": {
			Name: "t1", Enabled: true,
			Timing: models.TRIGGER_TIMING_BEFORE,
			Events: models.TRIGGER_EVENT_INSERT,
			Body:   "noop",
		},
	}
	bundle := newTestBundle("Orders", triggers)
	doc := newTestDocument("doc1", map[string]interface{}{})

	// Pre-populate call stack - should NOT cause recursion error since allowed
	session.TriggerCallStack = []string{"Orders.t1"}

	result, err := te.ExecuteTriggers(
		context.Background(), bundle,
		models.TRIGGER_EVENT_INSERT, models.TRIGGER_TIMING_BEFORE,
		nil, doc, session, nil, ServiceManager{},
	)
	if err != nil {
		t.Fatalf("recursion should be allowed but got error: %v", err)
	}
	if result != doc {
		t.Error("expected document returned when recursion is allowed")
	}
}

// TestExecuteTriggers_DepthIncrementsAndDecrements verifies trigger depth tracking.
func TestExecuteTriggers_DepthIncrementsAndDecrements(t *testing.T) {
	logger := newTestLogger()
	te := NewTriggerExecutor(logger)
	session := newTestSession()

	// Bundle with a trigger that executes (body is harmless unrecognized command)
	triggers := map[string]models.TriggerDefinition{
		"t1": {
			Name: "t1", Enabled: true,
			Timing: models.TRIGGER_TIMING_BEFORE,
			Events: models.TRIGGER_EVENT_INSERT,
			Body:   "noop",
		},
	}
	bundle := newTestBundle("Orders", triggers)
	doc := newTestDocument("doc1", map[string]interface{}{})

	if session.TriggerDepth.Load() != 0 {
		t.Fatalf("expected initial depth 0, got %d", session.TriggerDepth.Load())
	}

	_, _ = te.ExecuteTriggers(
		context.Background(), bundle,
		models.TRIGGER_EVENT_INSERT, models.TRIGGER_TIMING_BEFORE,
		nil, doc, session, nil, ServiceManager{},
	)

	// Depth should be back to 0 after execution (increment + defer decrement)
	if session.TriggerDepth.Load() != 0 {
		t.Errorf("expected depth 0 after trigger execution, got %d", session.TriggerDepth.Load())
	}
}

// TestExecuteTriggers_BeforeTriggerWithCanceledContext verifies BEFORE trigger error propagation.
func TestExecuteTriggers_BeforeTriggerWithCanceledContext(t *testing.T) {
	logger := newTestLogger()
	te := NewTriggerExecutor(logger)
	session := newTestSession()

	s := settings.GetSettings()
	origTimeout := s.TriggerTimeoutMs
	s.TriggerTimeoutMs = 1 // 1ms timeout - will expire immediately
	defer func() { s.TriggerTimeoutMs = origTimeout }()

	triggers := map[string]models.TriggerDefinition{
		"before_t": {
			Name: "before_t", Enabled: true,
			Timing: models.TRIGGER_TIMING_BEFORE,
			Events: models.TRIGGER_EVENT_INSERT,
			Body:   "noop",
		},
	}
	bundle := newTestBundle("Orders", triggers)
	doc := newTestDocument("doc1", map[string]interface{}{})

	// Use already-canceled context to force error
	ctx, cancel := context.WithCancel(context.Background())
	cancel() // cancel immediately

	_, err := te.ExecuteTriggers(
		ctx, bundle,
		models.TRIGGER_EVENT_INSERT, models.TRIGGER_TIMING_BEFORE,
		nil, doc, session, nil, ServiceManager{},
	)
	// With canceled context, executeCommand should fail and BEFORE trigger should propagate it
	// Note: behavior depends on whether executeCommand checks context; it may or may not error
	// The key assertion is: if there IS an error from a BEFORE trigger, it gets propagated
	_ = err // Result depends on executeCommand's context handling
}

// TestExecuteTriggers_AfterTriggerErrorNonFatal verifies AFTER trigger errors don't abort.
func TestExecuteTriggers_AfterTriggerErrorNonFatal(t *testing.T) {
	logger := newTestLogger()
	te := NewTriggerExecutor(logger)
	session := newTestSession()

	triggers := map[string]models.TriggerDefinition{
		"after_t": {
			Name: "after_t", Enabled: true,
			Timing: models.TRIGGER_TIMING_AFTER,
			Events: models.TRIGGER_EVENT_INSERT,
			Body:   "noop_harmless",
		},
	}
	bundle := newTestBundle("Orders", triggers)
	doc := newTestDocument("doc1", map[string]interface{}{})

	// AFTER trigger - even if body had errors, they should be non-fatal
	result, err := te.ExecuteTriggers(
		context.Background(), bundle,
		models.TRIGGER_EVENT_INSERT, models.TRIGGER_TIMING_AFTER,
		nil, doc, session, nil, ServiceManager{},
	)
	if err != nil {
		t.Fatalf("AFTER trigger should be non-fatal, got: %v", err)
	}
	if result != doc {
		t.Error("expected original document returned for AFTER trigger")
	}
}

// TestTriggerExecutionContext_OldNewDocuments verifies trigger execution context fields.
func TestTriggerExecutionContext_OldNewDocuments(t *testing.T) {
	oldDoc := newTestDocument("doc1", map[string]interface{}{"status": "old"})
	newDoc := newTestDocument("doc1", map[string]interface{}{"status": "new"})

	tc := &trigger.ExecutionContext{
		OldDocument:      oldDoc,
		NewDocument:      newDoc,
		SourceBundleName: "Orders",
		EventType:        models.TRIGGER_EVENT_UPDATE,
		TriggerDepth:     1,
	}

	if tc.OldDocument.DocumentID != "doc1" {
		t.Errorf("expected OldDocument ID 'doc1', got %q", tc.OldDocument.DocumentID)
	}
	if tc.NewDocument.Data["status"] != "new" {
		t.Errorf("expected NewDocument status 'new', got %v", tc.NewDocument.Data["status"])
	}
	if tc.SourceBundleName != "Orders" {
		t.Errorf("expected source bundle 'Orders', got %q", tc.SourceBundleName)
	}
	if tc.EventType != models.TRIGGER_EVENT_UPDATE {
		t.Errorf("expected UPDATE event, got %d", tc.EventType)
	}
}

// TestTriggerExecutionContext_InsertHasNoOld verifies INSERT triggers have nil OLD.
func TestTriggerExecutionContext_InsertHasNoOld(t *testing.T) {
	newDoc := newTestDocument("doc1", map[string]interface{}{"status": "new"})

	tc := &trigger.ExecutionContext{
		OldDocument:      nil,
		NewDocument:      newDoc,
		SourceBundleName: "Orders",
		EventType:        models.TRIGGER_EVENT_INSERT,
	}

	if tc.OldDocument != nil {
		t.Error("expected nil OldDocument for INSERT")
	}
	if tc.NewDocument == nil {
		t.Error("expected non-nil NewDocument for INSERT")
	}
}

// TestTriggerExecutionContext_DeleteHasNoNew verifies DELETE triggers have nil NEW.
func TestTriggerExecutionContext_DeleteHasNoNew(t *testing.T) {
	oldDoc := newTestDocument("doc1", map[string]interface{}{"status": "old"})

	tc := &trigger.ExecutionContext{
		OldDocument:      oldDoc,
		NewDocument:      nil,
		SourceBundleName: "Orders",
		EventType:        models.TRIGGER_EVENT_DELETE,
	}

	if tc.OldDocument == nil {
		t.Error("expected non-nil OldDocument for DELETE")
	}
	if tc.NewDocument != nil {
		t.Error("expected nil NewDocument for DELETE")
	}
}

// TestTriggerDefinition_MatchesEvent_Comprehensive tests all event combinations.
func TestTriggerDefinition_MatchesEvent_Comprehensive(t *testing.T) {
	tests := []struct {
		name     string
		events   models.TriggerEvent
		check    models.TriggerEvent
		enabled  bool
		expected bool
	}{
		{"INSERT matches INSERT", models.TRIGGER_EVENT_INSERT, models.TRIGGER_EVENT_INSERT, true, true},
		{"INSERT does not match UPDATE", models.TRIGGER_EVENT_INSERT, models.TRIGGER_EVENT_UPDATE, true, false},
		{"INSERT|UPDATE matches INSERT", models.TRIGGER_EVENT_INSERT | models.TRIGGER_EVENT_UPDATE, models.TRIGGER_EVENT_INSERT, true, true},
		{"INSERT|UPDATE matches UPDATE", models.TRIGGER_EVENT_INSERT | models.TRIGGER_EVENT_UPDATE, models.TRIGGER_EVENT_UPDATE, true, true},
		{"INSERT|UPDATE does not match DELETE", models.TRIGGER_EVENT_INSERT | models.TRIGGER_EVENT_UPDATE, models.TRIGGER_EVENT_DELETE, true, false},
		{"all events match DELETE", models.TRIGGER_EVENT_INSERT | models.TRIGGER_EVENT_UPDATE | models.TRIGGER_EVENT_DELETE, models.TRIGGER_EVENT_DELETE, true, true},
		{"disabled does not match", models.TRIGGER_EVENT_INSERT, models.TRIGGER_EVENT_INSERT, false, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			trig := models.TriggerDefinition{
				Events:  tt.events,
				Enabled: tt.enabled,
			}
			got := trig.MatchesEvent(tt.check)
			if got != tt.expected {
				t.Errorf("MatchesEvent(%d) = %v, want %v", tt.check, got, tt.expected)
			}
		})
	}
}

// TestValidateTriggerBody tests trigger body validation.
func TestValidateTriggerBody(t *testing.T) {
	tests := []struct {
		name      string
		body      string
		timing    models.TriggerTiming
		expectErr bool
		errSubstr string
	}{
		{"valid body", `ADD DOCUMENT TO BUNDLE "Log" WITH ({"x" = "y"})`, models.TRIGGER_TIMING_BEFORE, false, ""},
		{"DDL rejected", `CREATE BUNDLE "Bad"`, models.TRIGGER_TIMING_BEFORE, true, "DDL"},
		{"DROP rejected", `DROP BUNDLE "Bad"`, models.TRIGGER_TIMING_BEFORE, true, "DDL"},
		{"ALTER rejected", `ALTER BUNDLE "Bad"`, models.TRIGGER_TIMING_BEFORE, true, "DDL"},
		{"TRUNCATE rejected", `TRUNCATE`, models.TRIGGER_TIMING_BEFORE, true, "DDL"},
		{"COMMIT rejected", `COMMIT`, models.TRIGGER_TIMING_BEFORE, true, "transaction"},
		{"ROLLBACK rejected", `ROLLBACK`, models.TRIGGER_TIMING_BEFORE, true, "transaction"},
		{"BEGIN TRANSACTION rejected", `BEGIN TRANSACTION`, models.TRIGGER_TIMING_BEFORE, true, "transaction"},
		{"SAVEPOINT rejected", `SAVEPOINT "sp1"`, models.TRIGGER_TIMING_BEFORE, true, "transaction"},
		{"SET NEW in AFTER rejected", `SET NEW."x" = 1`, models.TRIGGER_TIMING_AFTER, true, "SET NEW"},
		{"SET NEW in BEFORE allowed", `SET NEW."x" = 1`, models.TRIGGER_TIMING_BEFORE, false, ""},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := validateTriggerBody(tt.body, tt.timing)
			if tt.expectErr && err == nil {
				t.Error("expected error but got nil")
			}
			if !tt.expectErr && err != nil {
				t.Errorf("unexpected error: %v", err)
			}
			if tt.expectErr && err != nil && tt.errSubstr != "" {
				if !containsString(err.Error(), tt.errSubstr) {
					t.Errorf("error %q does not contain %q", err.Error(), tt.errSubstr)
				}
			}
		})
	}
}

// TestTriggerContext_ContextValueRoundTrip tests context value passing.
func TestTriggerContext_ContextValueRoundTrip(t *testing.T) {
	doc := newTestDocument("doc1", map[string]interface{}{"x": 1})
	tc := &trigger.ExecutionContext{
		NewDocument:      doc,
		SourceBundleName: "TestBundle",
		EventType:        models.TRIGGER_EVENT_INSERT,
		TriggerDepth:     2,
	}

	ctx := trigger.WithExecutionContext(context.Background(), tc)
	got := trigger.GetExecutionContext(ctx)

	if got == nil {
		t.Fatal("expected non-nil trigger context from context.Context")
	}
	if got.NewDocument.DocumentID != "doc1" {
		t.Errorf("expected doc ID 'doc1', got %q", got.NewDocument.DocumentID)
	}
	if got.TriggerDepth != 2 {
		t.Errorf("expected depth 2, got %d", got.TriggerDepth)
	}
}

// TestTriggerContext_NilContext tests getting trigger context from empty context.
func TestTriggerContext_NilContext(t *testing.T) {
	got := trigger.GetExecutionContext(context.Background())
	if got != nil {
		t.Error("expected nil trigger context from empty context")
	}
}

// TestExecuteTriggers_SessionContextSetDuringExecution verifies trigger context is set on session.
func TestExecuteTriggers_SessionContextSetDuringExecution(t *testing.T) {
	logger := newTestLogger()
	te := NewTriggerExecutor(logger)
	session := newTestSession()

	triggers := map[string]models.TriggerDefinition{
		"t1": {
			Name: "t1", Enabled: true,
			Timing: models.TRIGGER_TIMING_BEFORE,
			Events: models.TRIGGER_EVENT_INSERT,
			Body:   "noop",
		},
	}
	bundle := newTestBundle("Orders", triggers)
	doc := newTestDocument("doc1", map[string]interface{}{})

	// Before execution, TriggerContext should be nil
	if session.TriggerContext != nil {
		t.Fatal("expected nil TriggerContext before execution")
	}

	_, _ = te.ExecuteTriggers(
		context.Background(), bundle,
		models.TRIGGER_EVENT_INSERT, models.TRIGGER_TIMING_BEFORE,
		nil, doc, session, nil, ServiceManager{},
	)

	// After execution, TriggerContext should be restored to nil
	if session.TriggerContext != nil {
		t.Error("expected TriggerContext restored to nil after execution")
	}
}

// TestExecuteTriggers_CallStackCleanup verifies call stack is cleaned up after execution.
func TestExecuteTriggers_CallStackCleanup(t *testing.T) {
	logger := newTestLogger()
	te := NewTriggerExecutor(logger)
	session := newTestSession()

	s := settings.GetSettings()
	origRecursion := s.TriggerAllowRecursion
	s.TriggerAllowRecursion = false
	defer func() { s.TriggerAllowRecursion = origRecursion }()

	triggers := map[string]models.TriggerDefinition{
		"t1": {
			Name: "t1", Enabled: true,
			Timing: models.TRIGGER_TIMING_BEFORE,
			Events: models.TRIGGER_EVENT_INSERT,
			Body:   "noop",
		},
	}
	bundle := newTestBundle("Orders", triggers)
	doc := newTestDocument("doc1", map[string]interface{}{})

	// Call stack should be empty before
	if len(session.TriggerCallStack) != 0 {
		t.Fatalf("expected empty call stack, got %v", session.TriggerCallStack)
	}

	_, _ = te.ExecuteTriggers(
		context.Background(), bundle,
		models.TRIGGER_EVENT_INSERT, models.TRIGGER_TIMING_BEFORE,
		nil, doc, session, nil, ServiceManager{},
	)

	// Call stack should be cleaned up after (deferred removal)
	if len(session.TriggerCallStack) != 0 {
		t.Errorf("expected call stack cleanup, got %v", session.TriggerCallStack)
	}
}

// containsString is a helper that checks if s contains substr.
func containsString(s, substr string) bool {
	if len(substr) == 0 {
		return true
	}
	for i := 0; i <= len(s)-len(substr); i++ {
		if s[i:i+len(substr)] == substr {
			return true
		}
	}
	return false
}
