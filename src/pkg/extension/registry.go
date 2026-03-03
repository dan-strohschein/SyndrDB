package extension

import (
	"context"
	"strings"
	"sync"
)

// ExtensionRegistry is the central registry for enterprise extensions.
// It is a global singleton, accessed via GetRegistry().
type ExtensionRegistry struct {
	mu                 sync.RWMutex
	commands           []CommandExtension
	lifecycleHooks     []LifecycleHook
	resultTransformers []ResultTransformExtension
	auditListeners     []AuditEventExtension
	storageEncryptors  []StorageEncryptionExtension
	indexExtensions    []IndexExtension
	plannerExtensions  []PlannerExtension
	temporalExtensions    []TemporalExtension
	replicationExtensions []ReplicationExtension
	readRouterExtensions  []ReadRouterExtension
	// Milestone 5
	executionExtensions   []ExecutionExtension
	queryGovernors        []QueryGovernorExtension
	metricsExporters      []MetricsExporterExtension
	docSecurityExtensions []DocumentSecurityExtension
	cdcExtensions         []CDCExtension
	adaptiveOptimizers    []AdaptiveOptimizerExtension
	matViewRefreshExts    []MatViewRefreshExtension
}

var (
	registryInstance *ExtensionRegistry
	registryOnce     sync.Once
)

// GetRegistry returns the singleton ExtensionRegistry (creates on first call).
func GetRegistry() *ExtensionRegistry {
	registryOnce.Do(func() {
		registryInstance = &ExtensionRegistry{}
	})
	return registryInstance
}

// Reset clears all registrations. Intended for testing only.
func Reset() {
	registryOnce = sync.Once{}
	registryInstance = nil
}

// RegisterCommand adds a CommandExtension to the registry.
func (r *ExtensionRegistry) RegisterCommand(ext CommandExtension) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.commands = append(r.commands, ext)
}

// RegisterLifecycleHook adds a LifecycleHook to the registry.
func (r *ExtensionRegistry) RegisterLifecycleHook(hook LifecycleHook) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.lifecycleHooks = append(r.lifecycleHooks, hook)
}

// FindCommandHandler returns the first CommandExtension whose prefixes match
// the given command string. Returns (nil, false) if no match is found.
func (r *ExtensionRegistry) FindCommandHandler(command string) (CommandExtension, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	cmdLower := strings.ToLower(command)
	for _, ext := range r.commands {
		for _, prefix := range ext.CommandPrefixes() {
			if strings.HasPrefix(cmdLower, prefix) {
				return ext, true
			}
		}
	}
	return nil, false
}

// NotifyServerStart calls OnServerStart on all registered lifecycle hooks
// in registration order. Returns the first error encountered.
func (r *ExtensionRegistry) NotifyServerStart(ctx context.Context, extCtx ExtensionContext) error {
	r.mu.RLock()
	hooks := make([]LifecycleHook, len(r.lifecycleHooks))
	copy(hooks, r.lifecycleHooks)
	r.mu.RUnlock()

	for _, hook := range hooks {
		if err := hook.OnServerStart(ctx, extCtx); err != nil {
			return err
		}
	}
	return nil
}

// NotifyServerStop calls OnServerStop on all registered lifecycle hooks
// in registration order. Returns the first error encountered.
func (r *ExtensionRegistry) NotifyServerStop(ctx context.Context) error {
	r.mu.RLock()
	hooks := make([]LifecycleHook, len(r.lifecycleHooks))
	copy(hooks, r.lifecycleHooks)
	r.mu.RUnlock()

	for _, hook := range hooks {
		if err := hook.OnServerStop(ctx); err != nil {
			return err
		}
	}
	return nil
}

// HasCommandExtensions returns true if any command extensions are registered.
func (r *ExtensionRegistry) HasCommandExtensions() bool {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return len(r.commands) > 0
}

// RegisterResultTransformer adds a ResultTransformExtension to the registry.
func (r *ExtensionRegistry) RegisterResultTransformer(ext ResultTransformExtension) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.resultTransformers = append(r.resultTransformers, ext)
}

// RegisterAuditListener adds an AuditEventExtension to the registry.
func (r *ExtensionRegistry) RegisterAuditListener(ext AuditEventExtension) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.auditListeners = append(r.auditListeners, ext)
}

// HasResultTransformers returns true if any result transform extensions are registered.
func (r *ExtensionRegistry) HasResultTransformers() bool {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return len(r.resultTransformers) > 0
}

// GetResultTransformers returns a snapshot of all registered result transformers.
func (r *ExtensionRegistry) GetResultTransformers() []ResultTransformExtension {
	r.mu.RLock()
	defer r.mu.RUnlock()
	out := make([]ResultTransformExtension, len(r.resultTransformers))
	copy(out, r.resultTransformers)
	return out
}

// GetAuditListeners returns a snapshot of all registered audit listeners.
func (r *ExtensionRegistry) GetAuditListeners() []AuditEventExtension {
	r.mu.RLock()
	defer r.mu.RUnlock()
	out := make([]AuditEventExtension, len(r.auditListeners))
	copy(out, r.auditListeners)
	return out
}

// NotifyCommandExecuted dispatches a command event to all audit listeners.
func (r *ExtensionRegistry) NotifyCommandExecuted(ctx context.Context, eventType string, detail map[string]interface{}) {
	r.mu.RLock()
	listeners := make([]AuditEventExtension, len(r.auditListeners))
	copy(listeners, r.auditListeners)
	r.mu.RUnlock()

	for _, listener := range listeners {
		listener.OnCommandExecuted(ctx, eventType, detail)
	}
}

// RegisterStorageEncryptor adds a StorageEncryptionExtension to the registry.
func (r *ExtensionRegistry) RegisterStorageEncryptor(ext StorageEncryptionExtension) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.storageEncryptors = append(r.storageEncryptors, ext)
}

// HasStorageEncryptors returns true if any storage encryption extensions are registered.
func (r *ExtensionRegistry) HasStorageEncryptors() bool {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return len(r.storageEncryptors) > 0
}

// GetStorageEncryptor returns the first registered storage encryptor (single-provider model).
func (r *ExtensionRegistry) GetStorageEncryptor() StorageEncryptionExtension {
	r.mu.RLock()
	defer r.mu.RUnlock()
	if len(r.storageEncryptors) == 0 {
		return nil
	}
	return r.storageEncryptors[0]
}

// Global ExtensionContext accessor for use by CommandDirector (set during server init).
var (
	globalExtCtx   ExtensionContext
	globalExtCtxMu sync.RWMutex
)

// SetExtensionContext stores the ExtensionContext globally for CommandDirector access.
func SetExtensionContext(ctx ExtensionContext) {
	globalExtCtxMu.Lock()
	defer globalExtCtxMu.Unlock()
	globalExtCtx = ctx
}

// GetExtensionContext returns the global ExtensionContext.
func GetExtensionContext() ExtensionContext {
	globalExtCtxMu.RLock()
	defer globalExtCtxMu.RUnlock()
	return globalExtCtx
}

// --- Index Extension ---

// RegisterIndexExtension adds an IndexExtension to the registry.
func (r *ExtensionRegistry) RegisterIndexExtension(ext IndexExtension) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.indexExtensions = append(r.indexExtensions, ext)
}

// GetIndexExtensions returns a snapshot of all registered index extensions.
func (r *ExtensionRegistry) GetIndexExtensions() []IndexExtension {
	r.mu.RLock()
	defer r.mu.RUnlock()
	out := make([]IndexExtension, len(r.indexExtensions))
	copy(out, r.indexExtensions)
	return out
}

// FindIndexExtension returns the first IndexExtension matching the given index type.
func (r *ExtensionRegistry) FindIndexExtension(indexType string) (IndexExtension, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	for _, ext := range r.indexExtensions {
		if ext.IndexType() == indexType {
			return ext, true
		}
	}
	return nil, false
}

// --- Planner Extension ---

// RegisterPlannerExtension adds a PlannerExtension to the registry.
func (r *ExtensionRegistry) RegisterPlannerExtension(ext PlannerExtension) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.plannerExtensions = append(r.plannerExtensions, ext)
}

// GetPlannerExtensions returns a snapshot of all registered planner extensions.
func (r *ExtensionRegistry) GetPlannerExtensions() []PlannerExtension {
	r.mu.RLock()
	defer r.mu.RUnlock()
	out := make([]PlannerExtension, len(r.plannerExtensions))
	copy(out, r.plannerExtensions)
	return out
}

// --- Temporal Extension ---

// RegisterTemporalExtension adds a TemporalExtension to the registry.
func (r *ExtensionRegistry) RegisterTemporalExtension(ext TemporalExtension) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.temporalExtensions = append(r.temporalExtensions, ext)
}

// GetTemporalExtensions returns a snapshot of all registered temporal extensions.
func (r *ExtensionRegistry) GetTemporalExtensions() []TemporalExtension {
	r.mu.RLock()
	defer r.mu.RUnlock()
	out := make([]TemporalExtension, len(r.temporalExtensions))
	copy(out, r.temporalExtensions)
	return out
}

// GetTemporalExtension returns the first registered temporal extension (single-provider model).
func (r *ExtensionRegistry) GetTemporalExtension() TemporalExtension {
	r.mu.RLock()
	defer r.mu.RUnlock()
	if len(r.temporalExtensions) == 0 {
		return nil
	}
	return r.temporalExtensions[0]
}

// --- Replication Extension ---

// RegisterReplicationExtension adds a ReplicationExtension to the registry.
func (r *ExtensionRegistry) RegisterReplicationExtension(ext ReplicationExtension) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.replicationExtensions = append(r.replicationExtensions, ext)
}

// HasReplicationExtension returns true if a replication extension is registered.
func (r *ExtensionRegistry) HasReplicationExtension() bool {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return len(r.replicationExtensions) > 0
}

// GetReplicationExtension returns the first registered replication extension (single-provider model).
func (r *ExtensionRegistry) GetReplicationExtension() ReplicationExtension {
	r.mu.RLock()
	defer r.mu.RUnlock()
	if len(r.replicationExtensions) == 0 {
		return nil
	}
	return r.replicationExtensions[0]
}

// --- Read Router Extension ---

// RegisterReadRouterExtension adds a ReadRouterExtension to the registry.
func (r *ExtensionRegistry) RegisterReadRouterExtension(ext ReadRouterExtension) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.readRouterExtensions = append(r.readRouterExtensions, ext)
}

// HasReadRouterExtension returns true if a read router extension is registered.
func (r *ExtensionRegistry) HasReadRouterExtension() bool {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return len(r.readRouterExtensions) > 0
}

// GetReadRouterExtension returns the first registered read router extension (single-provider model).
func (r *ExtensionRegistry) GetReadRouterExtension() ReadRouterExtension {
	r.mu.RLock()
	defer r.mu.RUnlock()
	if len(r.readRouterExtensions) == 0 {
		return nil
	}
	return r.readRouterExtensions[0]
}

// --- Execution Extension (Milestone 5) ---

// RegisterExecutionExtension adds an ExecutionExtension to the registry.
func (r *ExtensionRegistry) RegisterExecutionExtension(ext ExecutionExtension) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.executionExtensions = append(r.executionExtensions, ext)
}

// HasExecutionExtension returns true if an execution extension is registered.
func (r *ExtensionRegistry) HasExecutionExtension() bool {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return len(r.executionExtensions) > 0
}

// GetExecutionExtension returns the first registered execution extension (single-provider model).
func (r *ExtensionRegistry) GetExecutionExtension() ExecutionExtension {
	r.mu.RLock()
	defer r.mu.RUnlock()
	if len(r.executionExtensions) == 0 {
		return nil
	}
	return r.executionExtensions[0]
}

// --- Query Governor Extension (Milestone 5) ---

// RegisterQueryGovernorExtension adds a QueryGovernorExtension to the registry.
func (r *ExtensionRegistry) RegisterQueryGovernorExtension(ext QueryGovernorExtension) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.queryGovernors = append(r.queryGovernors, ext)
}

// HasQueryGovernorExtension returns true if a query governor is registered.
func (r *ExtensionRegistry) HasQueryGovernorExtension() bool {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return len(r.queryGovernors) > 0
}

// GetQueryGovernorExtension returns the first registered query governor (single-provider model).
func (r *ExtensionRegistry) GetQueryGovernorExtension() QueryGovernorExtension {
	r.mu.RLock()
	defer r.mu.RUnlock()
	if len(r.queryGovernors) == 0 {
		return nil
	}
	return r.queryGovernors[0]
}

// --- Metrics Exporter Extension (Milestone 5) ---

// RegisterMetricsExporterExtension adds a MetricsExporterExtension to the registry.
func (r *ExtensionRegistry) RegisterMetricsExporterExtension(ext MetricsExporterExtension) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.metricsExporters = append(r.metricsExporters, ext)
}

// HasMetricsExporterExtension returns true if a metrics exporter is registered.
func (r *ExtensionRegistry) HasMetricsExporterExtension() bool {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return len(r.metricsExporters) > 0
}

// GetMetricsExporterExtension returns the first registered metrics exporter (single-provider model).
func (r *ExtensionRegistry) GetMetricsExporterExtension() MetricsExporterExtension {
	r.mu.RLock()
	defer r.mu.RUnlock()
	if len(r.metricsExporters) == 0 {
		return nil
	}
	return r.metricsExporters[0]
}

// --- Document Security Extension (Milestone 5) ---

// RegisterDocumentSecurityExtension adds a DocumentSecurityExtension to the registry.
func (r *ExtensionRegistry) RegisterDocumentSecurityExtension(ext DocumentSecurityExtension) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.docSecurityExtensions = append(r.docSecurityExtensions, ext)
}

// HasDocumentSecurityExtension returns true if a document security extension is registered.
func (r *ExtensionRegistry) HasDocumentSecurityExtension() bool {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return len(r.docSecurityExtensions) > 0
}

// GetDocumentSecurityExtension returns the first registered document security extension (single-provider model).
func (r *ExtensionRegistry) GetDocumentSecurityExtension() DocumentSecurityExtension {
	r.mu.RLock()
	defer r.mu.RUnlock()
	if len(r.docSecurityExtensions) == 0 {
		return nil
	}
	return r.docSecurityExtensions[0]
}

// --- CDC Extension (Milestone 5) ---

// RegisterCDCExtension adds a CDCExtension to the registry.
func (r *ExtensionRegistry) RegisterCDCExtension(ext CDCExtension) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.cdcExtensions = append(r.cdcExtensions, ext)
}

// HasCDCExtension returns true if any CDC extensions are registered.
func (r *ExtensionRegistry) HasCDCExtension() bool {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return len(r.cdcExtensions) > 0
}

// GetCDCExtensions returns a snapshot of all registered CDC extensions (multi-listener).
func (r *ExtensionRegistry) GetCDCExtensions() []CDCExtension {
	r.mu.RLock()
	defer r.mu.RUnlock()
	out := make([]CDCExtension, len(r.cdcExtensions))
	copy(out, r.cdcExtensions)
	return out
}

// --- Adaptive Optimizer Extension (Milestone 5) ---

// RegisterAdaptiveOptimizerExtension adds an AdaptiveOptimizerExtension to the registry.
func (r *ExtensionRegistry) RegisterAdaptiveOptimizerExtension(ext AdaptiveOptimizerExtension) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.adaptiveOptimizers = append(r.adaptiveOptimizers, ext)
}

// HasAdaptiveOptimizerExtension returns true if an adaptive optimizer is registered.
func (r *ExtensionRegistry) HasAdaptiveOptimizerExtension() bool {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return len(r.adaptiveOptimizers) > 0
}

// GetAdaptiveOptimizerExtension returns the first registered adaptive optimizer (single-provider model).
func (r *ExtensionRegistry) GetAdaptiveOptimizerExtension() AdaptiveOptimizerExtension {
	r.mu.RLock()
	defer r.mu.RUnlock()
	if len(r.adaptiveOptimizers) == 0 {
		return nil
	}
	return r.adaptiveOptimizers[0]
}

// --- MatView Refresh Extension (Milestone 5) ---

// RegisterMatViewRefreshExtension adds a MatViewRefreshExtension to the registry.
func (r *ExtensionRegistry) RegisterMatViewRefreshExtension(ext MatViewRefreshExtension) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.matViewRefreshExts = append(r.matViewRefreshExts, ext)
}

// HasMatViewRefreshExtension returns true if a mat view refresh extension is registered.
func (r *ExtensionRegistry) HasMatViewRefreshExtension() bool {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return len(r.matViewRefreshExts) > 0
}

// GetMatViewRefreshExtension returns the first registered mat view refresh extension (single-provider model).
func (r *ExtensionRegistry) GetMatViewRefreshExtension() MatViewRefreshExtension {
	r.mu.RLock()
	defer r.mu.RUnlock()
	if len(r.matViewRefreshExts) == 0 {
		return nil
	}
	return r.matViewRefreshExts[0]
}
