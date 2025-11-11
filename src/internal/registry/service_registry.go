package registry

/*
SERVICE REGISTRY - DEPENDENCY INJECTION PATTERN

This file implements a global service registry using the dependency injection pattern
to break circular dependencies between packages. It allows services to register and
retrieve dependencies without direct package imports.

DESIGN PRINCIPLES:
- Single Responsibility: Only manages service registration/lookup
- Open/Closed: New services can be added without modifying existing code
- Dependency Inversion: Services depend on registry interface, not concrete types

ARCHITECTURE:
The registry uses a type-safe approach with generic getters for each service type.
This prevents runtime type assertion errors while maintaining flexibility.

USAGE:
1. During initialization: registry.SetWALManager(walManager)
2. In services: walManager := registry.GetWALManager()

TODO: I could add service lifecycle management (Start/Stop) for coordinated
shutdown sequences across all registered services
*/

import (
	"sync"

	"syndrdb/src/internal/journal"

	"go.uber.org/zap"
)

// ServiceRegistry provides global access to system services
// This breaks circular dependencies by providing a shared registry
// that all packages can access without importing each other
type ServiceRegistry struct {
	walManager *journal.WALManager
	logger     *zap.SugaredLogger
	mu         sync.RWMutex
}

var (
	instance *ServiceRegistry
	once     sync.Once
)

// GetRegistry returns the singleton service registry instance
// This is safe for concurrent access from multiple goroutines
func GetRegistry() *ServiceRegistry {
	once.Do(func() {
		instance = &ServiceRegistry{}
	})
	return instance
}

// SetWALManager registers the WAL manager in the global registry
// This should be called during server initialization before services start
//
// Parameters:
//   - walManager: The system-wide WAL manager instance
//
// Thread-safe: Uses write lock to prevent concurrent modifications
func (r *ServiceRegistry) SetWALManager(walManager *journal.WALManager) {
	r.mu.Lock()
	defer r.mu.Unlock()

	r.walManager = walManager

	if r.logger != nil {
		r.logger.Infof("WAL Manager registered in service registry")
	}
}

// GetWALManager retrieves the registered WAL manager
// Returns nil if WAL manager has not been registered yet
//
// Returns:
//   - *journal.WALManager: The registered WAL manager, or nil if not available
//
// Thread-safe: Uses read lock to allow concurrent access
func (r *ServiceRegistry) GetWALManager() *journal.WALManager {
	r.mu.RLock()
	defer r.mu.RUnlock()

	return r.walManager
}

// SetLogger registers the system logger in the registry
// This allows services to log even during early initialization
//
// Parameters:
//   - logger: The system-wide logger instance
//
// Thread-safe: Uses write lock to prevent concurrent modifications
func (r *ServiceRegistry) SetLogger(logger *zap.SugaredLogger) {
	r.mu.Lock()
	defer r.mu.Unlock()

	r.logger = logger
}

// GetLogger retrieves the registered logger
// Returns nil if logger has not been registered yet
//
// Returns:
//   - *zap.SugaredLogger: The registered logger, or nil if not available
//
// Thread-safe: Uses read lock to allow concurrent access
func (r *ServiceRegistry) GetLogger() *zap.SugaredLogger {
	r.mu.RLock()
	defer r.mu.RUnlock()

	return r.logger
}

// IsWALAvailable returns true if WAL manager is registered and available
// This is a convenience method to check availability without nil checks
//
// Returns:
//   - bool: true if WAL manager is available, false otherwise
//
// Thread-safe: Uses read lock for safe concurrent access
func (r *ServiceRegistry) IsWALAvailable() bool {
	r.mu.RLock()
	defer r.mu.RUnlock()

	return r.walManager != nil
}

// Reset clears all registered services
// This is primarily used for testing to ensure clean state between tests
//
// WARNING: Should only be called during shutdown or in test cleanup
//
// Thread-safe: Uses write lock to prevent concurrent modifications
func (r *ServiceRegistry) Reset() {
	r.mu.Lock()
	defer r.mu.Unlock()

	r.walManager = nil
	r.logger = nil
}
