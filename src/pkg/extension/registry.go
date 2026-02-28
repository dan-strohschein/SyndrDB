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
