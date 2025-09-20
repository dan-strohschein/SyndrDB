package server

import (
	"sync"
	defaultdb "syndrdb/src/internal/defaultDB"
	"syndrdb/src/internal/domain/bundle"
	"syndrdb/src/internal/domain/database"
	"syndrdb/src/internal/journal"

	"go.uber.org/zap"
)

type ServiceManager struct {
	// Add fields for managing services
	DatabaseService        *database.DatabaseService
	BundleService          *bundle.BundleService
	InternalCatalogService *defaultdb.CatalogService
	WALManager             *journal.WALManager
	logger                 *zap.SugaredLogger
}

// Private instance and mutex for thread safety
var (
	instance *ServiceManager
	once     sync.Once
	mu       sync.RWMutex
)

// GetServiceManager returns the singleton instance of ServiceManager
func GetServiceManager() *ServiceManager {
	mu.RLock()
	defer mu.RUnlock()

	if instance == nil {
		// If someone tries to get the instance before initialization,
		// return a basic empty instance
		return &ServiceManager{}
	}
	return instance
}

// InitServiceManager initializes the ServiceManager singleton with services
func InitServiceManager(dbService *database.DatabaseService, bundleService *bundle.BundleService,
	catalogService *defaultdb.CatalogService,
	logger *zap.SugaredLogger) *ServiceManager {
	// Use sync.Once to ensure this only happens one time
	once.Do(func() {
		mu.Lock()
		defer mu.Unlock()

		// Initialize WAL Manager
		walManager, err := journal.NewWALManager(logger)
		if err != nil {
			if logger != nil {
				logger.Errorf("Failed to initialize WAL Manager: %v", err)
			}
			// Continue without WAL for now, but log the error
			walManager = nil
		}

		instance = &ServiceManager{
			DatabaseService:        dbService,
			BundleService:          bundleService,
			InternalCatalogService: catalogService,
			WALManager:             walManager,
			logger:                 logger,
		}

		if logger != nil {
			if walManager != nil {
				logger.Info("ServiceManager singleton initialized with WAL Manager")
			} else {
				logger.Warn("ServiceManager singleton initialized without WAL Manager")
			}
		}
	})

	return instance
}

// ResetServiceManager is useful for testing - it resets the singleton
func ResetServiceManager() {
	mu.Lock()
	defer mu.Unlock()
	instance = nil
	// Reset the sync.Once so InitServiceManager can be called again
	once = sync.Once{}
}
