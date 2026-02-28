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
func (s *stubExtensionContext) Logger() interface{}   { return nil }
func (s *stubExtensionContext) Settings() interface{} { return nil }
