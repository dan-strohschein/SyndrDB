package directors

import (
	"sync"

	"go.uber.org/zap"
)

type ServiceManager struct {
	// Add fields for managing services
	DatabaseService *DatabaseService
	BundleService   *BundleService
	logger          *zap.SugaredLogger
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
func InitServiceManager(dbService *DatabaseService, bundleService *BundleService, logger *zap.SugaredLogger) *ServiceManager {
	// Use sync.Once to ensure this only happens one time
	once.Do(func() {
		mu.Lock()
		defer mu.Unlock()

		instance = &ServiceManager{
			DatabaseService: dbService,
			BundleService:   bundleService,
			logger:          logger,
		}

		if logger != nil {
			logger.Info("ServiceManager singleton initialized")
		}
	})

	return instance
}

// ResetServiceManager is useful for testing - it resets the singleton
func ResetServiceManager() {
	mu.Lock()
	defer mu.Unlock()
	instance = nil
}
