package extension

import (
	"context"
	"strings"
	"sync"
)

// ExtensionRegistry is the central registry for enterprise extensions.
// It is a global singleton, accessed via GetRegistry().
type ExtensionRegistry struct {
	mu             sync.RWMutex
	commands       []CommandExtension
	lifecycleHooks []LifecycleHook
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
