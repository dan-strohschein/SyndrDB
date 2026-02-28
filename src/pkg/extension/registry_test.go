package extension

import (
	"context"
	"errors"
	"testing"
)

// --- Test helpers ---

type stubCommandExtension struct {
	prefixes []string
	handler  func(ctx context.Context, command string, extCtx ExtensionContext) (interface{}, error)
}

func (s *stubCommandExtension) CommandPrefixes() []string { return s.prefixes }
func (s *stubCommandExtension) HandleCommand(ctx context.Context, command string, extCtx ExtensionContext) (interface{}, error) {
	return s.handler(ctx, command, extCtx)
}

type stubLifecycleHook struct {
	startCalled bool
	stopCalled  bool
	startErr    error
	stopErr     error
}

func (s *stubLifecycleHook) OnServerStart(ctx context.Context, extCtx ExtensionContext) error {
	s.startCalled = true
	return s.startErr
}

func (s *stubLifecycleHook) OnServerStop(ctx context.Context) error {
	s.stopCalled = true
	return s.stopErr
}

// --- Tests ---

func TestRegistrySingleton(t *testing.T) {
	defer Reset()

	r1 := GetRegistry()
	r2 := GetRegistry()
	if r1 != r2 {
		t.Fatal("GetRegistry must return the same instance")
	}
}

func TestRegisterCommandExtension(t *testing.T) {
	defer Reset()

	reg := GetRegistry()
	ext := &stubCommandExtension{
		prefixes: []string{"repl", "fulltext"},
		handler: func(ctx context.Context, command string, extCtx ExtensionContext) (interface{}, error) {
			return "handled:" + command, nil
		},
	}
	reg.RegisterCommand(ext)

	handler, found := reg.FindCommandHandler("repl status")
	if !found {
		t.Fatal("expected to find handler for 'repl status'")
	}
	result, err := handler.HandleCommand(context.Background(), "repl status", nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result != "handled:repl status" {
		t.Fatalf("unexpected result: %v", result)
	}

	// Also match by second prefix
	handler2, found2 := reg.FindCommandHandler("fulltext search foo")
	if !found2 {
		t.Fatal("expected to find handler for 'fulltext search foo'")
	}
	if handler2 != ext {
		t.Fatal("expected same extension instance")
	}
}

func TestFindCommandHandlerNoMatch(t *testing.T) {
	defer Reset()

	reg := GetRegistry()
	reg.RegisterCommand(&stubCommandExtension{
		prefixes: []string{"repl"},
		handler:  func(ctx context.Context, command string, extCtx ExtensionContext) (interface{}, error) { return nil, nil },
	})

	_, found := reg.FindCommandHandler("select * from foo")
	if found {
		t.Fatal("expected no match for 'select * from foo'")
	}
}

func TestFindCommandHandlerCaseInsensitive(t *testing.T) {
	defer Reset()

	reg := GetRegistry()
	reg.RegisterCommand(&stubCommandExtension{
		prefixes: []string{"repl"},
		handler:  func(ctx context.Context, command string, extCtx ExtensionContext) (interface{}, error) { return nil, nil },
	})

	// FindCommandHandler lowercases the command, so "REPL STATUS" should match "repl"
	_, found := reg.FindCommandHandler("REPL STATUS")
	if !found {
		t.Fatal("expected case-insensitive match for 'REPL STATUS'")
	}
}

func TestLifecycleHooks(t *testing.T) {
	defer Reset()

	reg := GetRegistry()
	hook1 := &stubLifecycleHook{}
	hook2 := &stubLifecycleHook{}
	reg.RegisterLifecycleHook(hook1)
	reg.RegisterLifecycleHook(hook2)

	err := reg.NotifyServerStart(context.Background(), nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !hook1.startCalled || !hook2.startCalled {
		t.Fatal("expected both hooks to have OnServerStart called")
	}

	err = reg.NotifyServerStop(context.Background())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !hook1.stopCalled || !hook2.stopCalled {
		t.Fatal("expected both hooks to have OnServerStop called")
	}
}

func TestLifecycleHookStartError(t *testing.T) {
	defer Reset()

	reg := GetRegistry()
	hook1 := &stubLifecycleHook{startErr: errors.New("init failed")}
	hook2 := &stubLifecycleHook{}
	reg.RegisterLifecycleHook(hook1)
	reg.RegisterLifecycleHook(hook2)

	err := reg.NotifyServerStart(context.Background(), nil)
	if err == nil {
		t.Fatal("expected error from failing hook")
	}
	if err.Error() != "init failed" {
		t.Fatalf("unexpected error message: %v", err)
	}
	// hook2 should NOT be called because hook1 failed
	if hook2.startCalled {
		t.Fatal("expected hook2 not to be called after hook1 failure")
	}
}

func TestErrNotHandled(t *testing.T) {
	if ErrNotHandled == nil {
		t.Fatal("ErrNotHandled must not be nil")
	}
	if !errors.Is(ErrNotHandled, ErrNotHandled) {
		t.Fatal("ErrNotHandled must match itself via errors.Is")
	}
}

func TestReset(t *testing.T) {
	reg := GetRegistry()
	reg.RegisterCommand(&stubCommandExtension{
		prefixes: []string{"test"},
		handler:  func(ctx context.Context, command string, extCtx ExtensionContext) (interface{}, error) { return nil, nil },
	})
	reg.RegisterLifecycleHook(&stubLifecycleHook{})

	Reset()

	reg2 := GetRegistry()
	if reg == reg2 {
		t.Fatal("Reset should create a new registry instance")
	}
	if reg2.HasCommandExtensions() {
		t.Fatal("new registry should have no command extensions")
	}
}

func TestHasCommandExtensions(t *testing.T) {
	defer Reset()

	reg := GetRegistry()
	if reg.HasCommandExtensions() {
		t.Fatal("fresh registry should have no extensions")
	}

	reg.RegisterCommand(&stubCommandExtension{
		prefixes: []string{"test"},
		handler:  func(ctx context.Context, command string, extCtx ExtensionContext) (interface{}, error) { return nil, nil },
	})

	if !reg.HasCommandExtensions() {
		t.Fatal("registry should have extensions after registration")
	}
}

func TestExtensionContextGlobal(t *testing.T) {
	defer func() {
		SetExtensionContext(nil)
	}()

	if GetExtensionContext() != nil {
		t.Fatal("initial ExtensionContext should be nil")
	}

	ctx := &stubExtensionContext{}
	SetExtensionContext(ctx)

	got := GetExtensionContext()
	if got != ctx {
		t.Fatal("GetExtensionContext should return what was set")
	}
}

type stubExtensionContext struct{}

func (s *stubExtensionContext) ExecuteQuery(ctx context.Context, sql string) (interface{}, error) {
	return nil, nil
}
func (s *stubExtensionContext) Logger() interface{}        { return nil }
func (s *stubExtensionContext) Settings() interface{}      { return nil }
func (s *stubExtensionContext) SessionInfo() *SessionInfo  { return nil }

// --- Result transform test helpers ---

type stubResultTransformer struct {
	shouldTransformResult bool
	transformCalled       bool
	lastBundleName        string
}

func (s *stubResultTransformer) ShouldTransform(bundleName string) bool {
	s.lastBundleName = bundleName
	return s.shouldTransformResult
}

func (s *stubResultTransformer) TransformResult(ctx context.Context, bundleName string, row map[string]interface{}, session *SessionInfo) map[string]interface{} {
	s.transformCalled = true
	row["_masked"] = true
	return row
}

// --- Audit listener test helpers ---

type stubAuditListener struct {
	events []map[string]interface{}
}

func (s *stubAuditListener) OnCommandExecuted(ctx context.Context, eventType string, detail map[string]interface{}) {
	detail["_eventType"] = eventType
	s.events = append(s.events, detail)
}

// --- New registry method tests ---

func TestRegisterResultTransformer(t *testing.T) {
	defer Reset()

	reg := GetRegistry()
	if reg.HasResultTransformers() {
		t.Fatal("fresh registry should have no result transformers")
	}

	transformer := &stubResultTransformer{shouldTransformResult: true}
	reg.RegisterResultTransformer(transformer)

	if !reg.HasResultTransformers() {
		t.Fatal("registry should have result transformers after registration")
	}

	transformers := reg.GetResultTransformers()
	if len(transformers) != 1 {
		t.Fatalf("expected 1 transformer, got %d", len(transformers))
	}
}

func TestResultTransformerShouldTransform(t *testing.T) {
	defer Reset()

	reg := GetRegistry()
	transformer := &stubResultTransformer{shouldTransformResult: true}
	reg.RegisterResultTransformer(transformer)

	transformers := reg.GetResultTransformers()
	if !transformers[0].ShouldTransform("employees") {
		t.Fatal("expected ShouldTransform to return true")
	}
	if transformer.lastBundleName != "employees" {
		t.Fatalf("expected bundle name 'employees', got '%s'", transformer.lastBundleName)
	}
}

func TestResultTransformerTransformResult(t *testing.T) {
	defer Reset()

	reg := GetRegistry()
	transformer := &stubResultTransformer{shouldTransformResult: true}
	reg.RegisterResultTransformer(transformer)

	transformers := reg.GetResultTransformers()
	row := map[string]interface{}{"name": "John", "ssn": "123-45-6789"}
	session := &SessionInfo{Username: "testuser", IsAdmin: false}
	result := transformers[0].TransformResult(context.Background(), "employees", row, session)

	if !transformer.transformCalled {
		t.Fatal("expected TransformResult to be called")
	}
	if result["_masked"] != true {
		t.Fatal("expected _masked field to be set")
	}
}

func TestRegisterAuditListener(t *testing.T) {
	defer Reset()

	reg := GetRegistry()
	listener := &stubAuditListener{}
	reg.RegisterAuditListener(listener)

	detail := map[string]interface{}{"command": "SELECT * FROM users"}
	reg.NotifyCommandExecuted(context.Background(), "SELECT", detail)

	if len(listener.events) != 1 {
		t.Fatalf("expected 1 event, got %d", len(listener.events))
	}
	if listener.events[0]["_eventType"] != "SELECT" {
		t.Fatalf("expected event type 'SELECT', got '%v'", listener.events[0]["_eventType"])
	}
}

func TestNotifyCommandExecutedMultipleListeners(t *testing.T) {
	defer Reset()

	reg := GetRegistry()
	listener1 := &stubAuditListener{}
	listener2 := &stubAuditListener{}
	reg.RegisterAuditListener(listener1)
	reg.RegisterAuditListener(listener2)

	detail := map[string]interface{}{"command": "INSERT INTO users"}
	reg.NotifyCommandExecuted(context.Background(), "INSERT", detail)

	if len(listener1.events) != 1 || len(listener2.events) != 1 {
		t.Fatal("expected both listeners to receive the event")
	}
}

func TestGetResultTransformersReturnsSnapshot(t *testing.T) {
	defer Reset()

	reg := GetRegistry()
	reg.RegisterResultTransformer(&stubResultTransformer{shouldTransformResult: true})

	snapshot := reg.GetResultTransformers()
	// Register another after getting snapshot
	reg.RegisterResultTransformer(&stubResultTransformer{shouldTransformResult: false})

	if len(snapshot) != 1 {
		t.Fatal("snapshot should not be affected by later registrations")
	}
	if len(reg.GetResultTransformers()) != 2 {
		t.Fatal("registry should have 2 transformers")
	}
}
