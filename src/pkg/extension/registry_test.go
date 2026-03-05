package extension

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"
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
func (s *stubExtensionContext) Logger() interface{}           { return nil }
func (s *stubExtensionContext) Settings() interface{}         { return nil }
func (s *stubExtensionContext) SessionInfo() *SessionInfo     { return nil }
func (s *stubExtensionContext) BundleService() interface{}    { return nil }
func (s *stubExtensionContext) DatabaseService() interface{}  { return nil }

// --- Storage encryptor test helpers ---

type stubStorageEncryptor struct {
	encryptCalled bool
	decryptCalled bool
	enabled       bool
}

func (s *stubStorageEncryptor) EncryptBlock(plaintext []byte, scope string) ([]byte, error) {
	s.encryptCalled = true
	// Simple XOR "encryption" for testing
	out := make([]byte, len(plaintext))
	for i, b := range plaintext {
		out[i] = b ^ 0xFF
	}
	return out, nil
}

func (s *stubStorageEncryptor) DecryptBlock(ciphertext []byte, scope string) ([]byte, error) {
	s.decryptCalled = true
	out := make([]byte, len(ciphertext))
	for i, b := range ciphertext {
		out[i] = b ^ 0xFF
	}
	return out, nil
}

func (s *stubStorageEncryptor) EncryptionEnabled(scope string) bool {
	return s.enabled
}

func TestRegisterStorageEncryptor(t *testing.T) {
	defer Reset()

	reg := GetRegistry()
	if reg.HasStorageEncryptors() {
		t.Fatal("fresh registry should have no storage encryptors")
	}

	enc := &stubStorageEncryptor{enabled: true}
	reg.RegisterStorageEncryptor(enc)

	if !reg.HasStorageEncryptors() {
		t.Fatal("registry should have storage encryptors after registration")
	}
}

func TestHasStorageEncryptors(t *testing.T) {
	defer Reset()

	reg := GetRegistry()
	if reg.HasStorageEncryptors() {
		t.Fatal("fresh registry should have no storage encryptors")
	}

	reg.RegisterStorageEncryptor(&stubStorageEncryptor{enabled: true})
	if !reg.HasStorageEncryptors() {
		t.Fatal("registry should have storage encryptors after registration")
	}
}

func TestGetStorageEncryptorReturnsFirst(t *testing.T) {
	defer Reset()

	reg := GetRegistry()

	// No encryptors registered
	if reg.GetStorageEncryptor() != nil {
		t.Fatal("expected nil when no encryptors registered")
	}

	enc1 := &stubStorageEncryptor{enabled: true}
	enc2 := &stubStorageEncryptor{enabled: false}
	reg.RegisterStorageEncryptor(enc1)
	reg.RegisterStorageEncryptor(enc2)

	got := reg.GetStorageEncryptor()
	if got != enc1 {
		t.Fatal("GetStorageEncryptor should return the first registered encryptor")
	}
}

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

// --- Index extension test helpers ---

type stubIndexExtension struct {
	indexType string
	built     bool
	dropped   bool
	changed   bool
}

func (s *stubIndexExtension) IndexType() string { return s.indexType }
func (s *stubIndexExtension) BuildIndex(ctx context.Context, bundleName string, fieldNames []string, options map[string]interface{}, extCtx ExtensionContext) error {
	s.built = true
	return nil
}
func (s *stubIndexExtension) DropIndex(ctx context.Context, bundleName string, indexName string, extCtx ExtensionContext) error {
	s.dropped = true
	return nil
}
func (s *stubIndexExtension) OnDocumentChange(ctx context.Context, bundleName string, docID string, changeType string, doc map[string]interface{}) error {
	s.changed = true
	return nil
}

// --- Planner extension test helpers ---

type stubPlannerExtension struct {
	planned bool
}

func (s *stubPlannerExtension) PlanQuery(ctx context.Context, bundleName string, query interface{}) (interface{}, bool) {
	s.planned = true
	return "custom-plan", true
}

// --- Temporal extension test helpers ---

type stubTemporalExtension struct {
	writeCalled  bool
	deleteCalled bool
	temporal     bool
}

func (s *stubTemporalExtension) OnDocumentWrite(ctx context.Context, bundleName string, docID string, oldDoc map[string]interface{}, newDoc map[string]interface{}) error {
	s.writeCalled = true
	return nil
}
func (s *stubTemporalExtension) OnDocumentDelete(ctx context.Context, bundleName string, docID string, oldDoc map[string]interface{}) error {
	s.deleteCalled = true
	return nil
}
func (s *stubTemporalExtension) IsTemporalBundle(bundleName string) bool {
	return s.temporal
}
func (s *stubTemporalExtension) FilterTemporalDocs(ctx context.Context, bundleName string, docs []map[string]interface{}, clause interface{}) ([]map[string]interface{}, error) {
	return docs, nil
}

// --- Index extension tests ---

func TestRegisterIndexExtension(t *testing.T) {
	defer Reset()

	reg := GetRegistry()
	ext := &stubIndexExtension{indexType: "fulltext"}
	reg.RegisterIndexExtension(ext)

	exts := reg.GetIndexExtensions()
	if len(exts) != 1 {
		t.Fatalf("expected 1 index extension, got %d", len(exts))
	}
}

func TestFindIndexExtension(t *testing.T) {
	defer Reset()

	reg := GetRegistry()
	ext := &stubIndexExtension{indexType: "fulltext"}
	reg.RegisterIndexExtension(ext)

	found, ok := reg.FindIndexExtension("fulltext")
	if !ok {
		t.Fatal("expected to find fulltext index extension")
	}
	if found.IndexType() != "fulltext" {
		t.Fatalf("expected index type 'fulltext', got '%s'", found.IndexType())
	}

	_, ok = reg.FindIndexExtension("spatial")
	if ok {
		t.Fatal("expected not to find spatial index extension")
	}
}

func TestIndexExtensionBuildAndDrop(t *testing.T) {
	defer Reset()

	reg := GetRegistry()
	ext := &stubIndexExtension{indexType: "fulltext"}
	reg.RegisterIndexExtension(ext)

	found, _ := reg.FindIndexExtension("fulltext")
	_ = found.BuildIndex(context.Background(), "articles", []string{"title", "body"}, nil, nil)
	if !ext.built {
		t.Fatal("expected BuildIndex to be called")
	}

	_ = found.DropIndex(context.Background(), "articles", "idx_articles_fts", nil)
	if !ext.dropped {
		t.Fatal("expected DropIndex to be called")
	}
}

// --- Planner extension tests ---

func TestRegisterPlannerExtension(t *testing.T) {
	defer Reset()

	reg := GetRegistry()
	ext := &stubPlannerExtension{}
	reg.RegisterPlannerExtension(ext)

	exts := reg.GetPlannerExtensions()
	if len(exts) != 1 {
		t.Fatalf("expected 1 planner extension, got %d", len(exts))
	}
}

func TestPlannerExtensionPlanQuery(t *testing.T) {
	defer Reset()

	reg := GetRegistry()
	ext := &stubPlannerExtension{}
	reg.RegisterPlannerExtension(ext)

	exts := reg.GetPlannerExtensions()
	plan, ok := exts[0].PlanQuery(context.Background(), "articles", nil)
	if !ok {
		t.Fatal("expected PlanQuery to return true")
	}
	if plan != "custom-plan" {
		t.Fatalf("expected 'custom-plan', got '%v'", plan)
	}
}

// --- Temporal extension tests ---

func TestRegisterTemporalExtension(t *testing.T) {
	defer Reset()

	reg := GetRegistry()
	ext := &stubTemporalExtension{temporal: true}
	reg.RegisterTemporalExtension(ext)

	exts := reg.GetTemporalExtensions()
	if len(exts) != 1 {
		t.Fatalf("expected 1 temporal extension, got %d", len(exts))
	}
}

func TestGetTemporalExtensionSingleProvider(t *testing.T) {
	defer Reset()

	reg := GetRegistry()

	// No extensions registered
	if reg.GetTemporalExtension() != nil {
		t.Fatal("expected nil when no temporal extensions registered")
	}

	ext1 := &stubTemporalExtension{temporal: true}
	ext2 := &stubTemporalExtension{temporal: false}
	reg.RegisterTemporalExtension(ext1)
	reg.RegisterTemporalExtension(ext2)

	got := reg.GetTemporalExtension()
	if got != ext1 {
		t.Fatal("GetTemporalExtension should return the first registered extension")
	}
}

func TestTemporalExtensionIsTemporalBundle(t *testing.T) {
	defer Reset()

	reg := GetRegistry()
	ext := &stubTemporalExtension{temporal: true}
	reg.RegisterTemporalExtension(ext)

	got := reg.GetTemporalExtension()
	if !got.IsTemporalBundle("employees") {
		t.Fatal("expected IsTemporalBundle to return true")
	}
}

func TestTemporalExtensionWriteAndDelete(t *testing.T) {
	defer Reset()

	reg := GetRegistry()
	ext := &stubTemporalExtension{temporal: true}
	reg.RegisterTemporalExtension(ext)

	got := reg.GetTemporalExtension()
	_ = got.OnDocumentWrite(context.Background(), "employees", "doc1", map[string]interface{}{"name": "old"}, map[string]interface{}{"name": "new"})
	if !ext.writeCalled {
		t.Fatal("expected OnDocumentWrite to be called")
	}

	_ = got.OnDocumentDelete(context.Background(), "employees", "doc1", map[string]interface{}{"name": "old"})
	if !ext.deleteCalled {
		t.Fatal("expected OnDocumentDelete to be called")
	}
}

// --- Replication extension test helpers ---

type stubReplicationExtension struct {
	leader     bool
	follower   bool
	mode       string
	lastEntry  WALEntryInfo
	entryCalls int
}

func (s *stubReplicationExtension) OnWALEntry(entry WALEntryInfo) error {
	s.lastEntry = entry
	s.entryCalls++
	return nil
}
func (s *stubReplicationExtension) IsLeader() bool         { return s.leader }
func (s *stubReplicationExtension) IsFollower() bool       { return s.follower }
func (s *stubReplicationExtension) ReplicationMode() string { return s.mode }

// --- Read router extension test helpers ---

type stubReadRouterExtension struct {
	shouldRoute bool
	routeResult interface{}
	routeCalls  int
}

func (s *stubReadRouterExtension) RouteRead(ctx context.Context, command string, extCtx ExtensionContext) (interface{}, bool) {
	s.routeCalls++
	if s.shouldRoute {
		return s.routeResult, true
	}
	return nil, false
}

// --- Replication extension tests ---

func TestRegisterReplicationExtension(t *testing.T) {
	defer Reset()

	reg := GetRegistry()
	if reg.HasReplicationExtension() {
		t.Fatal("fresh registry should have no replication extensions")
	}

	ext := &stubReplicationExtension{leader: true, mode: "async"}
	reg.RegisterReplicationExtension(ext)

	if !reg.HasReplicationExtension() {
		t.Fatal("registry should have replication extension after registration")
	}
}

func TestGetReplicationExtensionSingleProvider(t *testing.T) {
	defer Reset()

	reg := GetRegistry()

	if reg.GetReplicationExtension() != nil {
		t.Fatal("expected nil when no replication extensions registered")
	}

	ext1 := &stubReplicationExtension{leader: true, mode: "async"}
	ext2 := &stubReplicationExtension{follower: true, mode: "semisync"}
	reg.RegisterReplicationExtension(ext1)
	reg.RegisterReplicationExtension(ext2)

	got := reg.GetReplicationExtension()
	if got != ext1 {
		t.Fatal("GetReplicationExtension should return the first registered extension")
	}
}

func TestReplicationExtensionMethods(t *testing.T) {
	defer Reset()

	reg := GetRegistry()
	ext := &stubReplicationExtension{leader: true, mode: "semisync"}
	reg.RegisterReplicationExtension(ext)

	got := reg.GetReplicationExtension()
	if !got.IsLeader() {
		t.Fatal("expected IsLeader to return true")
	}
	if got.IsFollower() {
		t.Fatal("expected IsFollower to return false")
	}
	if got.ReplicationMode() != "semisync" {
		t.Fatalf("expected mode 'semisync', got '%s'", got.ReplicationMode())
	}

	entry := WALEntryInfo{LSN: 42, Operation: 1, TxID: "tx1", BundleName: "users", DocumentID: "doc1", RawBytes: []byte("test")}
	_ = got.OnWALEntry(entry)
	if ext.entryCalls != 1 {
		t.Fatal("expected OnWALEntry to be called once")
	}
	if ext.lastEntry.LSN != 42 {
		t.Fatalf("expected LSN 42, got %d", ext.lastEntry.LSN)
	}
}

// --- Read router extension tests ---

func TestRegisterReadRouterExtension(t *testing.T) {
	defer Reset()

	reg := GetRegistry()
	if reg.HasReadRouterExtension() {
		t.Fatal("fresh registry should have no read router extensions")
	}

	ext := &stubReadRouterExtension{shouldRoute: true, routeResult: "routed"}
	reg.RegisterReadRouterExtension(ext)

	if !reg.HasReadRouterExtension() {
		t.Fatal("registry should have read router extension after registration")
	}
}

func TestGetReadRouterExtensionSingleProvider(t *testing.T) {
	defer Reset()

	reg := GetRegistry()

	if reg.GetReadRouterExtension() != nil {
		t.Fatal("expected nil when no read router extensions registered")
	}

	ext1 := &stubReadRouterExtension{shouldRoute: true, routeResult: "result1"}
	ext2 := &stubReadRouterExtension{shouldRoute: false}
	reg.RegisterReadRouterExtension(ext1)
	reg.RegisterReadRouterExtension(ext2)

	got := reg.GetReadRouterExtension()
	if got != ext1 {
		t.Fatal("GetReadRouterExtension should return the first registered extension")
	}
}

func TestReadRouterExtensionRouteRead(t *testing.T) {
	defer Reset()

	reg := GetRegistry()
	ext := &stubReadRouterExtension{shouldRoute: true, routeResult: "follower-result"}
	reg.RegisterReadRouterExtension(ext)

	got := reg.GetReadRouterExtension()
	result, handled := got.RouteRead(context.Background(), "SELECT * FROM users", nil)
	if !handled {
		t.Fatal("expected RouteRead to return handled=true")
	}
	if result != "follower-result" {
		t.Fatalf("expected 'follower-result', got '%v'", result)
	}
	if ext.routeCalls != 1 {
		t.Fatal("expected RouteRead to be called once")
	}
}

func TestReadRouterExtensionDecline(t *testing.T) {
	defer Reset()

	reg := GetRegistry()
	ext := &stubReadRouterExtension{shouldRoute: false}
	reg.RegisterReadRouterExtension(ext)

	got := reg.GetReadRouterExtension()
	result, handled := got.RouteRead(context.Background(), "SELECT * FROM users", nil)
	if handled {
		t.Fatal("expected RouteRead to return handled=false")
	}
	if result != nil {
		t.Fatal("expected nil result when not handled")
	}
}

// --- Milestone 5: Execution Extension ---

type stubExecutionExtension struct {
	shouldParallel bool
	result         interface{}
}

func (s *stubExecutionExtension) ParallelExecute(ctx context.Context, plan interface{}, extCtx ExtensionContext) (interface{}, bool) {
	if s.shouldParallel {
		return s.result, true
	}
	return nil, false
}
func (s *stubExecutionExtension) ShouldParallelize(plan interface{}) bool { return s.shouldParallel }

func TestRegisterExecutionExtension(t *testing.T) {
	defer Reset()
	reg := GetRegistry()
	if reg.HasExecutionExtension() {
		t.Fatal("fresh registry should have no execution extensions")
	}
	ext := &stubExecutionExtension{shouldParallel: true, result: "parallel-result"}
	reg.RegisterExecutionExtension(ext)
	if !reg.HasExecutionExtension() {
		t.Fatal("registry should have execution extension after registration")
	}
	got := reg.GetExecutionExtension()
	if got != ext {
		t.Fatal("GetExecutionExtension should return the registered extension")
	}
}

func TestExecutionExtensionMethods(t *testing.T) {
	defer Reset()
	reg := GetRegistry()
	ext := &stubExecutionExtension{shouldParallel: true, result: "parallel-result"}
	reg.RegisterExecutionExtension(ext)
	got := reg.GetExecutionExtension()
	if !got.ShouldParallelize(nil) {
		t.Fatal("expected ShouldParallelize to return true")
	}
	result, handled := got.ParallelExecute(context.Background(), nil, nil)
	if !handled {
		t.Fatal("expected ParallelExecute to return handled=true")
	}
	if result != "parallel-result" {
		t.Fatalf("expected 'parallel-result', got '%v'", result)
	}
}

// --- Milestone 5: Query Governor Extension ---

type stubQueryGovernor struct {
	activeQueries []ActiveQueryInfo
	killCalled    bool
	rejectQuery   bool
}

func (s *stubQueryGovernor) OnQueryStart(ctx context.Context, command string, session *SessionInfo) (string, error) {
	if s.rejectQuery {
		return "", errors.New("query rejected: too many concurrent queries")
	}
	return "q-123", nil
}
func (s *stubQueryGovernor) OnQueryEnd(queryID string, elapsed time.Duration, rowsScanned int64, err error) {
}
func (s *stubQueryGovernor) GetActiveQueries() []ActiveQueryInfo { return s.activeQueries }
func (s *stubQueryGovernor) KillQuery(queryID string) error {
	s.killCalled = true
	return nil
}

func TestRegisterQueryGovernorExtension(t *testing.T) {
	defer Reset()
	reg := GetRegistry()
	if reg.HasQueryGovernorExtension() {
		t.Fatal("fresh registry should have no query governors")
	}
	ext := &stubQueryGovernor{}
	reg.RegisterQueryGovernorExtension(ext)
	if !reg.HasQueryGovernorExtension() {
		t.Fatal("registry should have query governor after registration")
	}
	got := reg.GetQueryGovernorExtension()
	if got != ext {
		t.Fatal("GetQueryGovernorExtension should return the registered extension")
	}
}

func TestQueryGovernorMethods(t *testing.T) {
	defer Reset()
	reg := GetRegistry()
	ext := &stubQueryGovernor{}
	reg.RegisterQueryGovernorExtension(ext)
	got := reg.GetQueryGovernorExtension()
	qid, err := got.OnQueryStart(context.Background(), "SELECT 1", nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if qid != "q-123" {
		t.Fatalf("expected query ID 'q-123', got '%s'", qid)
	}
	_ = got.KillQuery("q-123")
	if !ext.killCalled {
		t.Fatal("expected KillQuery to be called")
	}
}

// --- Milestone 5: Metrics Exporter Extension ---

type stubMetricsExporter struct {
	counters   int
	histograms int
}

func (s *stubMetricsExporter) RecordMetric(name string, value float64, labels map[string]string) {}
func (s *stubMetricsExporter) IncrementCounter(name string, labels map[string]string)            { s.counters++ }
func (s *stubMetricsExporter) RecordHistogram(name string, value float64, labels map[string]string) {
	s.histograms++
}
func (s *stubMetricsExporter) ServeMetrics() ([]byte, error) {
	return []byte("# HELP test\n"), nil
}

func TestRegisterMetricsExporterExtension(t *testing.T) {
	defer Reset()
	reg := GetRegistry()
	if reg.HasMetricsExporterExtension() {
		t.Fatal("fresh registry should have no metrics exporters")
	}
	ext := &stubMetricsExporter{}
	reg.RegisterMetricsExporterExtension(ext)
	if !reg.HasMetricsExporterExtension() {
		t.Fatal("registry should have metrics exporter after registration")
	}
}

func TestMetricsExporterMethods(t *testing.T) {
	defer Reset()
	reg := GetRegistry()
	ext := &stubMetricsExporter{}
	reg.RegisterMetricsExporterExtension(ext)
	got := reg.GetMetricsExporterExtension()
	got.IncrementCounter("queries_total", nil)
	got.RecordHistogram("query_duration", 1.5, nil)
	if ext.counters != 1 || ext.histograms != 1 {
		t.Fatal("expected counter and histogram to be incremented")
	}
	data, err := got.ServeMetrics()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(data) == 0 {
		t.Fatal("expected non-empty metrics output")
	}
}

// --- Milestone 5: Document Security Extension ---

type stubDocumentSecurity struct {
	shouldFilterResult bool
	filterCalled       bool
}

func (s *stubDocumentSecurity) FilterDocuments(ctx context.Context, bundleName string, docs []map[string]interface{}, session *SessionInfo) []map[string]interface{} {
	s.filterCalled = true
	// Filter out docs without "visible" field
	var out []map[string]interface{}
	for _, d := range docs {
		if d["visible"] == true {
			out = append(out, d)
		}
	}
	return out
}
func (s *stubDocumentSecurity) ShouldFilter(bundleName string) bool { return s.shouldFilterResult }

func TestRegisterDocumentSecurityExtension(t *testing.T) {
	defer Reset()
	reg := GetRegistry()
	if reg.HasDocumentSecurityExtension() {
		t.Fatal("fresh registry should have no document security extensions")
	}
	ext := &stubDocumentSecurity{shouldFilterResult: true}
	reg.RegisterDocumentSecurityExtension(ext)
	if !reg.HasDocumentSecurityExtension() {
		t.Fatal("registry should have document security extension after registration")
	}
}

func TestDocumentSecurityMethods(t *testing.T) {
	defer Reset()
	reg := GetRegistry()
	ext := &stubDocumentSecurity{shouldFilterResult: true}
	reg.RegisterDocumentSecurityExtension(ext)
	got := reg.GetDocumentSecurityExtension()
	if !got.ShouldFilter("users") {
		t.Fatal("expected ShouldFilter to return true")
	}
	docs := []map[string]interface{}{
		{"id": "1", "visible": true},
		{"id": "2", "visible": false},
	}
	filtered := got.FilterDocuments(context.Background(), "users", docs, nil)
	if len(filtered) != 1 {
		t.Fatalf("expected 1 visible doc, got %d", len(filtered))
	}
}

// --- Milestone 5: CDC Extension ---

type stubCDCExtension struct {
	events []DataChangeEvent
}

func (s *stubCDCExtension) OnDataChange(ctx context.Context, change DataChangeEvent) {
	s.events = append(s.events, change)
}

func TestRegisterCDCExtension(t *testing.T) {
	defer Reset()
	reg := GetRegistry()
	if reg.HasCDCExtension() {
		t.Fatal("fresh registry should have no CDC extensions")
	}
	ext := &stubCDCExtension{}
	reg.RegisterCDCExtension(ext)
	if !reg.HasCDCExtension() {
		t.Fatal("registry should have CDC extension after registration")
	}
}

func TestCDCExtensionMultiListener(t *testing.T) {
	defer Reset()
	reg := GetRegistry()
	ext1 := &stubCDCExtension{}
	ext2 := &stubCDCExtension{}
	reg.RegisterCDCExtension(ext1)
	reg.RegisterCDCExtension(ext2)
	exts := reg.GetCDCExtensions()
	if len(exts) != 2 {
		t.Fatalf("expected 2 CDC extensions, got %d", len(exts))
	}
	change := DataChangeEvent{Operation: "INSERT", BundleName: "users", DocumentID: "doc1"}
	for _, e := range exts {
		e.OnDataChange(context.Background(), change)
	}
	if len(ext1.events) != 1 || len(ext2.events) != 1 {
		t.Fatal("expected both CDC listeners to receive event")
	}
}

// --- Milestone 5: Adaptive Optimizer Extension ---

type stubAdaptiveOptimizer struct {
	recorded    int
	shouldReplan bool
}

func (s *stubAdaptiveOptimizer) RecordExecution(planHash uint64, stats ExecutionStats) {
	s.recorded++
}
func (s *stubAdaptiveOptimizer) SuggestPlanChange(planHash uint64) (bool, string) {
	return s.shouldReplan, "cost drift"
}
func (s *stubAdaptiveOptimizer) GetExecutionHistory(planHash uint64) []ExecutionStats {
	return []ExecutionStats{{RowsScanned: 100, ElapsedMs: 5.0}}
}

func TestRegisterAdaptiveOptimizerExtension(t *testing.T) {
	defer Reset()
	reg := GetRegistry()
	if reg.HasAdaptiveOptimizerExtension() {
		t.Fatal("fresh registry should have no adaptive optimizers")
	}
	ext := &stubAdaptiveOptimizer{shouldReplan: true}
	reg.RegisterAdaptiveOptimizerExtension(ext)
	if !reg.HasAdaptiveOptimizerExtension() {
		t.Fatal("registry should have adaptive optimizer after registration")
	}
}

func TestAdaptiveOptimizerMethods(t *testing.T) {
	defer Reset()
	reg := GetRegistry()
	ext := &stubAdaptiveOptimizer{shouldReplan: true}
	reg.RegisterAdaptiveOptimizerExtension(ext)
	got := reg.GetAdaptiveOptimizerExtension()
	got.RecordExecution(42, ExecutionStats{RowsScanned: 1000, ElapsedMs: 10.0})
	if ext.recorded != 1 {
		t.Fatal("expected RecordExecution to be called")
	}
	replan, reason := got.SuggestPlanChange(42)
	if !replan {
		t.Fatal("expected SuggestPlanChange to return true")
	}
	if reason != "cost drift" {
		t.Fatalf("expected reason 'cost drift', got '%s'", reason)
	}
	history := got.GetExecutionHistory(42)
	if len(history) != 1 {
		t.Fatalf("expected 1 history entry, got %d", len(history))
	}
}

// --- Milestone 5: MatView Refresh Extension ---

type stubMatViewRefresh struct {
	sourceBundles map[string]bool
	changeCalled  bool
	refreshCalled bool
}

func (s *stubMatViewRefresh) OnSourceChange(ctx context.Context, bundleName string, docID string, changeType string, doc map[string]interface{}) error {
	s.changeCalled = true
	return nil
}
func (s *stubMatViewRefresh) IsSourceBundle(bundleName string) bool {
	return s.sourceBundles[bundleName]
}
func (s *stubMatViewRefresh) RefreshView(ctx context.Context, viewName string) error {
	s.refreshCalled = true
	return nil
}

func TestRegisterMatViewRefreshExtension(t *testing.T) {
	defer Reset()
	reg := GetRegistry()
	if reg.HasMatViewRefreshExtension() {
		t.Fatal("fresh registry should have no matview refresh extensions")
	}
	ext := &stubMatViewRefresh{sourceBundles: map[string]bool{"orders": true}}
	reg.RegisterMatViewRefreshExtension(ext)
	if !reg.HasMatViewRefreshExtension() {
		t.Fatal("registry should have matview refresh extension after registration")
	}
}

func TestMatViewRefreshMethods(t *testing.T) {
	defer Reset()
	reg := GetRegistry()
	ext := &stubMatViewRefresh{sourceBundles: map[string]bool{"orders": true}}
	reg.RegisterMatViewRefreshExtension(ext)
	got := reg.GetMatViewRefreshExtension()
	if !got.IsSourceBundle("orders") {
		t.Fatal("expected IsSourceBundle to return true for 'orders'")
	}
	if got.IsSourceBundle("users") {
		t.Fatal("expected IsSourceBundle to return false for 'users'")
	}
	_ = got.OnSourceChange(context.Background(), "orders", "doc1", "INSERT", nil)
	if !ext.changeCalled {
		t.Fatal("expected OnSourceChange to be called")
	}
	_ = got.RefreshView(context.Background(), "order_summary")
	if !ext.refreshCalled {
		t.Fatal("expected RefreshView to be called")
	}
}

// --- Milestone 6.1: Sharding Extension ---

type stubShardingExtension struct {
	sharded    map[string]bool
	shardNames map[string][]string
}

func (s *stubShardingExtension) IsShardedBundle(bundleName string) bool {
	return s.sharded[bundleName]
}
func (s *stubShardingExtension) ResolveWriteShard(bundleName string, doc map[string]interface{}) (string, error) {
	return bundleName + "__shard_0", nil
}
func (s *stubShardingExtension) ResolveReadShards(bundleName string, predicates interface{}) []string {
	if names, ok := s.shardNames[bundleName]; ok {
		return names
	}
	return nil
}
func (s *stubShardingExtension) GetShardBundles(bundleName string) []string {
	if names, ok := s.shardNames[bundleName]; ok {
		return names
	}
	return nil
}
func (s *stubShardingExtension) GetShardPolicy(bundleName string) *ShardPolicyInfo {
	if !s.sharded[bundleName] {
		return nil
	}
	return &ShardPolicyInfo{BundleName: bundleName, ShardKey: "id", ShardCount: 2}
}
func (s *stubShardingExtension) ExecuteShardedQuery(ctx context.Context, bundleName string, query interface{},
	rawCommand string, session *SessionInfo, executor ShardQueryExecutorFn) ([]map[string]interface{}, int, bool) {
	return nil, 0, false
}

func TestRegisterShardingExtension(t *testing.T) {
	defer Reset()
	reg := GetRegistry()
	if reg.HasShardingExtension() {
		t.Fatal("fresh registry should have no sharding extensions")
	}
	ext := &stubShardingExtension{
		sharded:    map[string]bool{"orders": true},
		shardNames: map[string][]string{"orders": {"orders__shard_0", "orders__shard_1"}},
	}
	reg.RegisterShardingExtension(ext)
	if !reg.HasShardingExtension() {
		t.Fatal("registry should have sharding extension after registration")
	}
	got := reg.GetShardingExtension()
	if got != ext {
		t.Fatal("GetShardingExtension should return the registered extension")
	}
}

func TestShardingExtensionMethods(t *testing.T) {
	defer Reset()
	reg := GetRegistry()
	ext := &stubShardingExtension{
		sharded:    map[string]bool{"orders": true},
		shardNames: map[string][]string{"orders": {"orders__shard_0", "orders__shard_1"}},
	}
	reg.RegisterShardingExtension(ext)
	got := reg.GetShardingExtension()
	if !got.IsShardedBundle("orders") {
		t.Fatal("expected orders to be sharded")
	}
	if got.IsShardedBundle("users") {
		t.Fatal("expected users to not be sharded")
	}
	shardName, err := got.ResolveWriteShard("orders", map[string]interface{}{"id": 42})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if shardName != "orders__shard_0" {
		t.Fatalf("expected 'orders__shard_0', got '%s'", shardName)
	}
	shards := got.GetShardBundles("orders")
	if len(shards) != 2 {
		t.Fatalf("expected 2 shards, got %d", len(shards))
	}
	policy := got.GetShardPolicy("orders")
	if policy == nil {
		t.Fatal("expected non-nil shard policy")
	}
	if policy.ShardKey != "id" {
		t.Fatalf("expected shard key 'id', got '%s'", policy.ShardKey)
	}
}

// --- Milestone 6.2: Distributed Transaction Extension ---

type stubDistributedTxExtension struct {
	beginCalls   int
	lastSession  string
	lastParticipants []string
	states       map[string]DistributedTxState
}

func (s *stubDistributedTxExtension) TrackBegin(sessionID string, participants []string) string {
	s.beginCalls++
	s.lastSession = sessionID
	s.lastParticipants = participants
	dtxID := fmt.Sprintf("dtx-%d", s.beginCalls)
	if s.states == nil {
		s.states = make(map[string]DistributedTxState)
	}
	s.states[dtxID] = DTxStateActive
	return dtxID
}
func (s *stubDistributedTxExtension) TrackPrepared(dtxID string) {
	s.states[dtxID] = DTxStatePrepared
}
func (s *stubDistributedTxExtension) TrackCommit(dtxID string) {
	s.states[dtxID] = DTxStateCommitted
}
func (s *stubDistributedTxExtension) TrackAbort(dtxID string) {
	s.states[dtxID] = DTxStateAborted
}
func (s *stubDistributedTxExtension) GetState(dtxID string) (DistributedTxState, error) {
	st, ok := s.states[dtxID]
	if !ok {
		return 0, fmt.Errorf("dtx not found: %s", dtxID)
	}
	return st, nil
}
func (s *stubDistributedTxExtension) ListActive() []DistributedTxInfo {
	var out []DistributedTxInfo
	for id, st := range s.states {
		if st == DTxStateActive || st == DTxStatePrepared {
			out = append(out, DistributedTxInfo{DTXID: id, State: st})
		}
	}
	return out
}

func TestRegisterDistributedTxExtension(t *testing.T) {
	defer Reset()
	reg := GetRegistry()
	if reg.HasDistributedTxExtension() {
		t.Fatal("fresh registry should have no distributed tx extensions")
	}
	ext := &stubDistributedTxExtension{}
	reg.RegisterDistributedTxExtension(ext)
	if !reg.HasDistributedTxExtension() {
		t.Fatal("registry should have distributed tx extension after registration")
	}
	got := reg.GetDistributedTxExtension()
	if got != ext {
		t.Fatal("GetDistributedTxExtension should return the registered extension")
	}
}

func TestDistributedTxExtensionMethods(t *testing.T) {
	defer Reset()
	reg := GetRegistry()
	ext := &stubDistributedTxExtension{}
	reg.RegisterDistributedTxExtension(ext)
	got := reg.GetDistributedTxExtension()

	dtxID := got.TrackBegin("sess-1", []string{"shard_0", "shard_1"})
	if dtxID != "dtx-1" {
		t.Fatalf("expected dtx-1, got %s", dtxID)
	}
	st, err := got.GetState(dtxID)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if st != DTxStateActive {
		t.Fatalf("expected DTxStateActive, got %d", st)
	}

	got.TrackPrepared(dtxID)
	st, _ = got.GetState(dtxID)
	if st != DTxStatePrepared {
		t.Fatalf("expected DTxStatePrepared, got %d", st)
	}

	got.TrackCommit(dtxID)
	st, _ = got.GetState(dtxID)
	if st != DTxStateCommitted {
		t.Fatalf("expected DTxStateCommitted, got %d", st)
	}

	active := got.ListActive()
	if len(active) != 0 {
		t.Fatalf("expected 0 active after commit, got %d", len(active))
	}
}

// --- Milestone 6.6: Columnar Processing Extension ---

type stubColumnarProcessingExtension struct {
	bundles map[string]bool
	memUsed int64
}

func (s *stubColumnarProcessingExtension) IsColumnarBundle(bundleName string) bool {
	return s.bundles[bundleName]
}
func (s *stubColumnarProcessingExtension) ExecuteColumnar(ctx context.Context, bundleName string, query interface{}, session *SessionInfo) ([]map[string]interface{}, int, bool) {
	if !s.bundles[bundleName] {
		return nil, 0, false
	}
	return []map[string]interface{}{{"id": "1", "name": "test"}}, 1, true
}
func (s *stubColumnarProcessingExtension) InvalidateSegments(bundleName string) {}
func (s *stubColumnarProcessingExtension) GetBundleInfo(bundleName string) *ColumnarBundleInfo {
	if !s.bundles[bundleName] {
		return nil
	}
	return &ColumnarBundleInfo{BundleName: bundleName, SegmentCount: 2, TotalRows: 1000}
}
func (s *stubColumnarProcessingExtension) GetSegmentStats(bundleName string) []ColumnarSegmentStats {
	return []ColumnarSegmentStats{{SegmentID: 0, RowCount: 500}}
}
func (s *stubColumnarProcessingExtension) GetMemoryUsage() int64 { return s.memUsed }

func TestRegisterColumnarProcessingExtension(t *testing.T) {
	defer Reset()
	reg := GetRegistry()
	if reg.HasColumnarProcessingExtension() {
		t.Fatal("fresh registry should have no columnar processing extensions")
	}
	ext := &stubColumnarProcessingExtension{bundles: map[string]bool{"orders": true}, memUsed: 1024}
	reg.RegisterColumnarProcessingExtension(ext)
	if !reg.HasColumnarProcessingExtension() {
		t.Fatal("registry should have columnar processing extension after registration")
	}
	got := reg.GetColumnarProcessingExtension()
	if got != ext {
		t.Fatal("GetColumnarProcessingExtension should return the registered extension")
	}
}

func TestColumnarProcessingExtensionMethods(t *testing.T) {
	defer Reset()
	reg := GetRegistry()
	ext := &stubColumnarProcessingExtension{bundles: map[string]bool{"orders": true}, memUsed: 2048}
	reg.RegisterColumnarProcessingExtension(ext)
	got := reg.GetColumnarProcessingExtension()

	if !got.IsColumnarBundle("orders") {
		t.Fatal("expected orders to be columnar")
	}
	if got.IsColumnarBundle("users") {
		t.Fatal("expected users to not be columnar")
	}

	result, count, handled := got.ExecuteColumnar(context.Background(), "orders", nil, nil)
	if !handled {
		t.Fatal("expected columnar execution to be handled")
	}
	if count != 1 || len(result) != 1 {
		t.Fatalf("expected 1 result, got %d/%d", count, len(result))
	}

	info := got.GetBundleInfo("orders")
	if info == nil || info.SegmentCount != 2 {
		t.Fatal("expected bundle info with 2 segments")
	}
	if got.GetBundleInfo("users") != nil {
		t.Fatal("expected nil bundle info for non-columnar bundle")
	}

	stats := got.GetSegmentStats("orders")
	if len(stats) != 1 || stats[0].RowCount != 500 {
		t.Fatal("unexpected segment stats")
	}

	if got.GetMemoryUsage() != 2048 {
		t.Fatalf("expected memory usage 2048, got %d", got.GetMemoryUsage())
	}
}

// --- Milestone 6.7: Query Result Cache Extension ---

type stubQueryResultCache struct {
	lookupCalls  int
	storeCalls   int
	flushCalls   int
	shouldResult bool
}

func (s *stubQueryResultCache) LookupResult(ctx context.Context, queryHash uint64, bundleName string, session *SessionInfo) ([]map[string]interface{}, int, bool) {
	s.lookupCalls++
	if s.shouldResult {
		return []map[string]interface{}{{"id": "1"}}, 1, true
	}
	return nil, 0, false
}
func (s *stubQueryResultCache) StoreResult(ctx context.Context, queryHash uint64, bundleName string, session *SessionInfo, result []map[string]interface{}, isAggregate bool) {
	s.storeCalls++
}
func (s *stubQueryResultCache) InvalidateBundle(bundleName string)    {}
func (s *stubQueryResultCache) ShouldCache(bundleName string) bool    { return true }
func (s *stubQueryResultCache) GetStats() ResultCacheStats             { return ResultCacheStats{Hits: 10, Misses: 5, HitRate: 0.67} }
func (s *stubQueryResultCache) FlushAll()                              { s.flushCalls++ }
func (s *stubQueryResultCache) FlushBundle(bundleName string)          { s.flushCalls++ }

func TestRegisterQueryResultCacheExtension(t *testing.T) {
	defer Reset()
	reg := GetRegistry()
	if reg.HasQueryResultCacheExtension() {
		t.Fatal("fresh registry should have no result cache extensions")
	}
	ext := &stubQueryResultCache{shouldResult: true}
	reg.RegisterQueryResultCacheExtension(ext)
	if !reg.HasQueryResultCacheExtension() {
		t.Fatal("registry should have result cache extension after registration")
	}
	got := reg.GetQueryResultCacheExtension()
	if got != ext {
		t.Fatal("GetQueryResultCacheExtension should return the registered extension")
	}
}

func TestQueryResultCacheExtensionMethods(t *testing.T) {
	defer Reset()
	reg := GetRegistry()
	ext := &stubQueryResultCache{shouldResult: true}
	reg.RegisterQueryResultCacheExtension(ext)
	got := reg.GetQueryResultCacheExtension()

	// Test LookupResult hit
	result, count, hit := got.LookupResult(context.Background(), 42, "users", nil)
	if !hit {
		t.Fatal("expected cache hit")
	}
	if count != 1 {
		t.Fatalf("expected count 1, got %d", count)
	}
	if len(result) != 1 {
		t.Fatalf("expected 1 result, got %d", len(result))
	}
	if ext.lookupCalls != 1 {
		t.Fatal("expected LookupResult to be called once")
	}

	// Test StoreResult
	got.StoreResult(context.Background(), 42, "users", nil, result, false)
	if ext.storeCalls != 1 {
		t.Fatal("expected StoreResult to be called once")
	}

	// Test GetStats
	stats := got.GetStats()
	if stats.Hits != 10 || stats.Misses != 5 {
		t.Fatalf("unexpected stats: %+v", stats)
	}

	// Test FlushAll
	got.FlushAll()
	if ext.flushCalls != 1 {
		t.Fatal("expected FlushAll to be called once")
	}
}

// --- Milestone 7.3: Field Encryption Extension ---

type stubFieldEncryptionExtension struct {
	policies map[string]bool
	encCalls int
}

func (s *stubFieldEncryptionExtension) EncryptFieldValues(ctx context.Context, bundleName string, fields map[string]interface{}) error {
	s.encCalls++
	return nil
}
func (s *stubFieldEncryptionExtension) HasFLEPolicy(bundleName string) bool {
	return s.policies[bundleName]
}

func TestRegisterFieldEncryptionExtension(t *testing.T) {
	defer Reset()
	reg := GetRegistry()
	if reg.HasFieldEncryptionExtension() {
		t.Fatal("fresh registry should have no field encryption extensions")
	}
	ext := &stubFieldEncryptionExtension{policies: map[string]bool{"employees": true}}
	reg.RegisterFieldEncryptionExtension(ext)
	if !reg.HasFieldEncryptionExtension() {
		t.Fatal("registry should have field encryption extension after registration")
	}
	got := reg.GetFieldEncryptionExtension()
	if got != ext {
		t.Fatal("GetFieldEncryptionExtension should return the registered extension")
	}
}

func TestFieldEncryptionExtensionSingleProvider(t *testing.T) {
	defer Reset()
	reg := GetRegistry()
	ext := &stubFieldEncryptionExtension{policies: map[string]bool{"employees": true}}
	reg.RegisterFieldEncryptionExtension(ext)
	got := reg.GetFieldEncryptionExtension()

	if !got.HasFLEPolicy("employees") {
		t.Fatal("expected HasFLEPolicy to return true for 'employees'")
	}
	if got.HasFLEPolicy("orders") {
		t.Fatal("expected HasFLEPolicy to return false for 'orders'")
	}

	err := got.EncryptFieldValues(context.Background(), "employees", map[string]interface{}{"ssn": "123-45-6789"})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if ext.encCalls != 1 {
		t.Fatal("expected EncryptFieldValues to be called once")
	}
}

// --- Milestone 6.4: Online Schema Change Extension ---

type stubOnlineSchemaChangeExtension struct {
	migrating map[string]bool
}

func (s *stubOnlineSchemaChangeExtension) IsMigrating(bundleName string) bool {
	return s.migrating[bundleName]
}
func (s *stubOnlineSchemaChangeExtension) GetMigrationState(bundleName string) *SchemaChangeInfo {
	if !s.migrating[bundleName] {
		return nil
	}
	return &SchemaChangeInfo{BundleName: bundleName, State: "COPYING", DocsCopied: 500, DocsTotal: 1000}
}
func (s *stubOnlineSchemaChangeExtension) ListMigrations() []SchemaChangeInfo {
	var out []SchemaChangeInfo
	for name := range s.migrating {
		out = append(out, SchemaChangeInfo{BundleName: name, State: "COPYING"})
	}
	return out
}

func TestRegisterOnlineSchemaChangeExtension(t *testing.T) {
	defer Reset()
	reg := GetRegistry()
	if reg.HasOnlineSchemaChangeExtension() {
		t.Fatal("fresh registry should have no online schema change extensions")
	}
	ext := &stubOnlineSchemaChangeExtension{migrating: map[string]bool{"employees": true}}
	reg.RegisterOnlineSchemaChangeExtension(ext)
	if !reg.HasOnlineSchemaChangeExtension() {
		t.Fatal("registry should have online schema change extension after registration")
	}
	got := reg.GetOnlineSchemaChangeExtension()
	if got != ext {
		t.Fatal("GetOnlineSchemaChangeExtension should return the registered extension")
	}
}

func TestOnlineSchemaChangeExtensionMethods(t *testing.T) {
	defer Reset()
	reg := GetRegistry()
	ext := &stubOnlineSchemaChangeExtension{migrating: map[string]bool{"employees": true}}
	reg.RegisterOnlineSchemaChangeExtension(ext)
	got := reg.GetOnlineSchemaChangeExtension()

	if !got.IsMigrating("employees") {
		t.Fatal("expected IsMigrating to return true for 'employees'")
	}
	if got.IsMigrating("orders") {
		t.Fatal("expected IsMigrating to return false for 'orders'")
	}

	state := got.GetMigrationState("employees")
	if state == nil {
		t.Fatal("expected non-nil migration state")
	}
	if state.State != "COPYING" || state.DocsCopied != 500 || state.DocsTotal != 1000 {
		t.Fatalf("unexpected state: %+v", state)
	}

	if got.GetMigrationState("orders") != nil {
		t.Fatal("expected nil state for non-migrating bundle")
	}

	migrations := got.ListMigrations()
	if len(migrations) != 1 {
		t.Fatalf("expected 1 migration, got %d", len(migrations))
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

// --- Milestone 7.1: CRDT Replication Extension ---

type stubCRDTReplicationExtension struct {
	crdtBundles map[string]bool
}

func (s *stubCRDTReplicationExtension) IsCRDTBundle(bundleName string) bool {
	return s.crdtBundles[bundleName]
}
func (s *stubCRDTReplicationExtension) OnDocumentWrite(ctx context.Context, bundleName, docID string,
	fields map[string]interface{}, operation string) (map[string]interface{}, error) {
	fields["__crdt_meta"] = `{"type":"lww"}`
	return fields, nil
}
func (s *stubCRDTReplicationExtension) MergeRemoteState(ctx context.Context, bundleName, docID string,
	remoteFields map[string]interface{}) (map[string]interface{}, error) {
	return remoteFields, nil
}
func (s *stubCRDTReplicationExtension) IsPrimary() bool { return true }
func (s *stubCRDTReplicationExtension) GetPeerInfo() []CRDTPeerInfo {
	return []CRDTPeerInfo{{NodeID: "peer-1", Host: "10.0.0.2", Status: "online", Port: 27017, GossipPort: 9090}}
}

func TestRegisterCRDTReplicationExtension(t *testing.T) {
	defer Reset()
	reg := GetRegistry()
	if reg.HasCRDTReplicationExtension() {
		t.Fatal("fresh registry should have no CRDT replication extensions")
	}
	ext := &stubCRDTReplicationExtension{crdtBundles: map[string]bool{"users": true}}
	reg.RegisterCRDTReplicationExtension(ext)
	if !reg.HasCRDTReplicationExtension() {
		t.Fatal("registry should have CRDT replication extension after registration")
	}
	got := reg.GetCRDTReplicationExtension()
	if got != ext {
		t.Fatal("GetCRDTReplicationExtension should return the registered extension")
	}
}

func TestCRDTReplicationExtensionMethods(t *testing.T) {
	defer Reset()
	reg := GetRegistry()
	ext := &stubCRDTReplicationExtension{crdtBundles: map[string]bool{"users": true}}
	reg.RegisterCRDTReplicationExtension(ext)
	got := reg.GetCRDTReplicationExtension()

	if !got.IsCRDTBundle("users") {
		t.Fatal("expected IsCRDTBundle to return true for 'users'")
	}
	if got.IsCRDTBundle("orders") {
		t.Fatal("expected IsCRDTBundle to return false for 'orders'")
	}
	if !got.IsPrimary() {
		t.Fatal("expected IsPrimary to return true")
	}

	peers := got.GetPeerInfo()
	if len(peers) != 1 || peers[0].NodeID != "peer-1" {
		t.Fatalf("unexpected peers: %+v", peers)
	}

	fields := map[string]interface{}{"name": "Alice"}
	enriched, err := got.OnDocumentWrite(context.Background(), "users", "doc-1", fields, "INSERT")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if _, ok := enriched["__crdt_meta"]; !ok {
		t.Fatal("OnDocumentWrite should inject __crdt_meta")
	}
}
