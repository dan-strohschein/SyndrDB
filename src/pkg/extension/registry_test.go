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
