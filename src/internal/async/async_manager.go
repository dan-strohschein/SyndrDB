package async

import (
	"context"
	"fmt"
	"sync"
	"time"
)

// AsyncManager coordinates all async components and provides a unified interface
// This is the main entry point for the async system
type AsyncManager struct {
	sequenceGen      *SequenceGenerator
	walWriter        *AsyncWALWriter
	consistentReader *MultiSourceReader
	workerPools      map[string]*WorkerPool // Named worker pools for different operation types
	config           AsyncManagerConfig     // HIGH-007: Store config for access to max pools
	mu               sync.RWMutex
	started          bool
	metrics          *AsyncManagerMetrics
}

// AsyncManagerMetrics tracks overall async system performance
type AsyncManagerMetrics struct {
	mu                sync.RWMutex
	totalOperations   uint64
	operationsByType  map[string]uint64
	avgProcessingTime time.Duration
	systemStartTime   time.Time
	lastOperationTime time.Time
}

// AsyncManagerConfig contains configuration for the async manager
type AsyncManagerConfig struct {
	WALConfig       AsyncWALConfig
	DefaultPoolSize int
	MaxPools        int
}

// NewAsyncManager creates a new async manager with the specified configuration
func NewAsyncManager(config AsyncManagerConfig, diskReader DiskReader, walReader WALReader) *AsyncManager {
	sequenceGen := NewSequenceGenerator()

	// Set defaults
	if config.DefaultPoolSize == 0 {
		config.DefaultPoolSize = 4
	}
	if config.MaxPools == 0 {
		config.MaxPools = 10
	}

	walWriter := NewAsyncWALWriter(config.WALConfig, sequenceGen)
	consistentReader := NewMultiSourceReader(diskReader, walReader)

	return &AsyncManager{
		sequenceGen:      sequenceGen,
		walWriter:        walWriter,
		consistentReader: consistentReader,
		workerPools:      make(map[string]*WorkerPool),
		config:           config, // HIGH-007: Store config for access to max pools
		started:          false,
		metrics: &AsyncManagerMetrics{
			operationsByType: make(map[string]uint64),
			systemStartTime:  time.Now(),
		},
	}
}

// Start initializes and starts all async components
func (am *AsyncManager) Start() error {
	am.mu.Lock()
	defer am.mu.Unlock()

	if am.started {
		return fmt.Errorf("async manager already started")
	}

	// Start WAL writer
	am.walWriter.Start()

	// Start any existing worker pools
	for name, pool := range am.workerPools {
		pool.Start()
		// TODO: I need to add logging here to track which pools are starting
		_ = name // Avoid unused variable warning for now
	}

	am.started = true
	am.metrics.systemStartTime = time.Now()

	return nil
}

// Stop gracefully shuts down all async components
func (am *AsyncManager) Stop(timeout time.Duration) error {
	am.mu.Lock()
	defer am.mu.Unlock()

	if !am.started {
		return nil
	}

	var errors []error

	// Stop WAL writer
	if err := am.walWriter.Stop(timeout); err != nil {
		errors = append(errors, fmt.Errorf("WAL writer stop error: %w", err))
	}

	// Stop all worker pools
	for name, pool := range am.workerPools {
		if err := pool.Stop(timeout); err != nil {
			errors = append(errors, fmt.Errorf("worker pool %s stop error: %w", name, err))
		}
	}

	am.started = false

	if len(errors) > 0 {
		return fmt.Errorf("async manager stop errors: %v", errors)
	}

	return nil
}

// GetOrCreateWorkerPool gets an existing worker pool or creates a new one
func (am *AsyncManager) GetOrCreateWorkerPool(name string, config WorkerPoolConfig) (*WorkerPool, error) {
	am.mu.Lock()
	defer am.mu.Unlock()

	// Return existing pool if it exists
	if pool, exists := am.workerPools[name]; exists {
		return pool, nil
	}

	// HIGH-007: Use configurable max pools from config
	maxPools := 10 // Default fallback
	if am.config.MaxPools > 0 {
		maxPools = am.config.MaxPools
	}

	if len(am.workerPools) >= maxPools {
		return nil, fmt.Errorf("maximum number of worker pools (%d) reached", maxPools)
	}

	// Create new pool
	pool := NewWorkerPool(config)
	am.workerPools[name] = pool

	// Start the pool if the manager is already started
	if am.started {
		pool.Start()
	}

	return pool, nil
}

// WriteWALAsync submits a WAL entry for asynchronous writing
func (am *AsyncManager) WriteWALAsync(walEntry []byte, bundleName, operation string) (*WALOperation, error) {
	if !am.started {
		return nil, fmt.Errorf("async manager not started")
	}

	am.updateMetrics("WAL", time.Now())
	return am.walWriter.WriteAsync(walEntry, bundleName, operation)
}

// ReadConsistent performs a consistent read across all data sources
func (am *AsyncManager) ReadConsistent(ctx context.Context, operation ReadOperation) (interface{}, DataSource, error) {
	if !am.started {
		return nil, SourceDisk, fmt.Errorf("async manager not started")
	}

	startTime := time.Now()
	result, source, err := am.consistentReader.Read(ctx, operation)
	am.updateMetrics("READ", startTime)

	return result, source, err
}

// CreateSnapshot creates a new consistent snapshot for multi-operation reads
func (am *AsyncManager) CreateSnapshot() *Snapshot {
	return am.consistentReader.CreateSnapshot()
}

// GetSequenceGenerator returns the sequence generator for external use
func (am *AsyncManager) GetSequenceGenerator() *SequenceGenerator {
	return am.sequenceGen
}

// GetMetrics returns comprehensive metrics from all async components
func (am *AsyncManager) GetMetrics() AsyncSystemMetrics {
	am.metrics.mu.RLock()
	defer am.metrics.mu.RUnlock()

	walMetrics := am.walWriter.GetMetrics()

	// Collect worker pool metrics
	poolMetrics := make(map[string]WorkerMetrics)
	am.mu.RLock()
	for name, pool := range am.workerPools {
		poolMetrics[name] = pool.GetMetrics()
	}
	am.mu.RUnlock()

	return AsyncSystemMetrics{
		TotalOperations:   am.metrics.totalOperations,
		OperationsByType:  copyStringUintMap(am.metrics.operationsByType),
		AvgProcessingTime: am.metrics.avgProcessingTime,
		SystemUptime:      time.Since(am.metrics.systemStartTime),
		WALMetrics: WALMetricsSnapshot{
			OperationsTotal:  walMetrics.operationsTotal,
			OperationsFailed: walMetrics.operationsFailed,
			AvgWriteTime:     walMetrics.avgWriteTime,
			BatchesProcessed: walMetrics.batchesProcessed,
			AvgBatchSize:     walMetrics.avgBatchSize,
		},
		PoolMetrics: poolMetrics,
	}
}

// AsyncSystemMetrics contains comprehensive metrics for the entire async system
type AsyncSystemMetrics struct {
	TotalOperations   uint64
	OperationsByType  map[string]uint64
	AvgProcessingTime time.Duration
	SystemUptime      time.Duration
	WALMetrics        WALMetricsSnapshot
	PoolMetrics       map[string]WorkerMetrics
}

// WALMetricsSnapshot is a copy of WAL metrics without mutex
type WALMetricsSnapshot struct {
	OperationsTotal  uint64
	OperationsFailed uint64
	AvgWriteTime     time.Duration
	BatchesProcessed uint64
	AvgBatchSize     float64
}

// updateMetrics updates the async manager's internal metrics
func (am *AsyncManager) updateMetrics(operationType string, startTime time.Time) {
	am.metrics.mu.Lock()
	defer am.metrics.mu.Unlock()

	am.metrics.totalOperations++
	am.metrics.operationsByType[operationType]++
	am.metrics.lastOperationTime = time.Now()

	processingTime := time.Since(startTime)
	if am.metrics.avgProcessingTime == 0 {
		am.metrics.avgProcessingTime = processingTime
	} else {
		am.metrics.avgProcessingTime = (am.metrics.avgProcessingTime + processingTime) / 2
	}
}

// copyStringUintMap creates a copy of a map to avoid exposing internal state
func copyStringUintMap(original map[string]uint64) map[string]uint64 {
	copy := make(map[string]uint64)
	for k, v := range original {
		copy[k] = v
	}
	return copy
}

// IsStarted returns whether the async manager is currently started
func (am *AsyncManager) IsStarted() bool {
	am.mu.RLock()
	defer am.mu.RUnlock()

	return am.started
}

// GetWorkerPoolNames returns the names of all configured worker pools
func (am *AsyncManager) GetWorkerPoolNames() []string {
	am.mu.RLock()
	defer am.mu.RUnlock()

	names := make([]string, 0, len(am.workerPools))
	for name := range am.workerPools {
		names = append(names, name)
	}

	return names
}
