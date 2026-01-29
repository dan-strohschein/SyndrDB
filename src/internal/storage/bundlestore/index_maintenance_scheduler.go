package bundlestore

import (
	"container/heap"
	"context"
	"fmt"
	"math"
	"sync"
	"sync/atomic"
	"time"

	"go.uber.org/zap"
)

/*
INDEX MAINTENANCE SCHEDULER

PostgreSQL autovacuum-inspired parallel index rebuild worker pool.
Manages automatic index rebuilding when staleness thresholds are exceeded.

POSTGRESQL AUTOVACUUM PARALLELS:
- autovacuum_max_workers → workerCount (default: 2)
- autovacuum_naptime → evaluationInterval (default: 5 minutes)
- Priority-based scheduling (high staleness + high query frequency = urgent)
- Prevents duplicate work on same index
- Load-aware pausing during high query activity

PRIORITY CALCULATION:
priority = stalenessRate * log(queryCount + 1)
- Balances staleness severity with query impact
- Favors frequently-queried indexes with high staleness
- Logarithmic scaling prevents query count from dominating
*/

// IndexMaintenanceScheduler manages parallel index rebuild workers with priority queue
type IndexMaintenanceScheduler struct {
	bundleService BundleServiceInterface
	logger        *zap.SugaredLogger

	// Configuration
	workerCount          int
	evaluationInterval   time.Duration
	stalenessThreshold   float64       // Default: 0.20 (20%)
	minRebuildInterval   time.Duration // Default: 1 hour
	pauseThreshold       int32         // Active query threshold
	queryFrequencyWeight float64       // Weight for query frequency (default: 1.0)

	// Work coordination
	requestQueue  *IndexMaintenanceQueue
	queueMutex    sync.Mutex
	workAvailable chan struct{} // Signal for workers when new work arrives
	workerWg      sync.WaitGroup
	ctx           context.Context
	cancel        context.CancelFunc

	// Track active work to prevent duplicates
	activeWork sync.Map // indexKey (db:bundle:index) -> bool

	// Metrics
	totalRebuildsScheduled int64
	totalRebuildsCompleted int64
	totalRebuildsFailed    int64
}

// BundleServiceInterface defines the interface for calling rebuild functions
// This avoids circular imports
type BundleServiceInterface interface {
	RebuildHashIndexFromBundle(bundle interface{}, indexName string, databaseName string) (interface{}, error)
	RebuildBTreeIndexFromBundle(bundle interface{}, indexName string) (interface{}, error)
	GetBundleByName(bundleName string) (interface{}, error)
}

// IndexMaintenanceRequest represents a request to rebuild an index
type IndexMaintenanceRequest struct {
	DatabaseName string
	BundleName   string
	IndexName    string
	IndexType    string // "hash" or "btree"
	Priority     float64
	StalenessRate float64
	QueryCount   int64
	RequestTime  time.Time
}

// IndexMaintenanceQueue implements heap.Interface for priority queue
type IndexMaintenanceQueue []*IndexMaintenanceRequest

func (q IndexMaintenanceQueue) Len() int { return len(q) }

func (q IndexMaintenanceQueue) Less(i, j int) bool {
	// Higher priority comes first
	if q[i].Priority != q[j].Priority {
		return q[i].Priority > q[j].Priority
	}
	// If equal priority, older requests first (FIFO)
	return q[i].RequestTime.Before(q[j].RequestTime)
}

func (q IndexMaintenanceQueue) Swap(i, j int) {
	q[i], q[j] = q[j], q[i]
}

func (q *IndexMaintenanceQueue) Push(x interface{}) {
	*q = append(*q, x.(*IndexMaintenanceRequest))
}

func (q *IndexMaintenanceQueue) Pop() interface{} {
	old := *q
	n := len(old)
	item := old[n-1]
	*q = old[0 : n-1]
	return item
}

// NewIndexMaintenanceScheduler creates a new index maintenance scheduler
func NewIndexMaintenanceScheduler(
	bundleService BundleServiceInterface,
	workerCount int,
	stalenessThreshold float64,
	minRebuildInterval time.Duration,
	pauseThreshold int,
	queryFrequencyWeight float64,
	logger *zap.SugaredLogger,
) *IndexMaintenanceScheduler {
	ctx, cancel := context.WithCancel(context.Background())

	return &IndexMaintenanceScheduler{
		bundleService:        bundleService,
		logger:               logger,
		workerCount:          workerCount,
		evaluationInterval:   5 * time.Minute,
		stalenessThreshold:   stalenessThreshold,
		minRebuildInterval:   minRebuildInterval,
		pauseThreshold:       int32(pauseThreshold),
		queryFrequencyWeight: queryFrequencyWeight,
		requestQueue:         &IndexMaintenanceQueue{},
		workAvailable:        make(chan struct{}, 1),
		ctx:                  ctx,
		cancel:               cancel,
	}
}

// Start launches the worker pool
func (s *IndexMaintenanceScheduler) Start() {
	s.logger.Infof("Starting index maintenance scheduler with %d workers", s.workerCount)

	// Initialize the priority queue
	heap.Init(s.requestQueue)

	// Start worker goroutines
	for i := 0; i < s.workerCount; i++ {
		s.workerWg.Add(1)
		go s.worker(i)
	}

	s.logger.Info("Index maintenance scheduler started successfully")
}

// Stop gracefully shuts down the scheduler and all workers
func (s *IndexMaintenanceScheduler) Stop() {
	s.logger.Info("Stopping index maintenance scheduler...")
	s.cancel()
	s.workerWg.Wait()
	s.logger.Info("Index maintenance scheduler stopped")
}

// ScheduleRebuild adds an index rebuild request to the priority queue
func (s *IndexMaintenanceScheduler) ScheduleRebuild(req IndexMaintenanceRequest) error {
	indexKey := fmt.Sprintf("%s:%s:%s", req.DatabaseName, req.BundleName, req.IndexName)

	// Check if already rebuilding
	if _, exists := s.activeWork.Load(indexKey); exists {
		s.logger.Debugf("Index %s already has active rebuild, skipping", indexKey)
		return nil
	}

	// Calculate priority: staleness * log(queryCount + 1) * queryFrequencyWeight
	req.Priority = req.StalenessRate * math.Log(float64(req.QueryCount+1)) * s.queryFrequencyWeight
	req.RequestTime = time.Now()

	// Add to priority queue
	s.queueMutex.Lock()
	heap.Push(s.requestQueue, &req)
	queueLen := s.requestQueue.Len()
	s.queueMutex.Unlock()

	atomic.AddInt64(&s.totalRebuildsScheduled, 1)

	s.logger.Infof("Scheduled index rebuild: %s (staleness: %.1f%%, queries: %d, priority: %.2f, queue: %d)",
		indexKey, req.StalenessRate*100, req.QueryCount, req.Priority, queueLen)

	// Signal workers
	select {
	case s.workAvailable <- struct{}{}:
	default:
	}

	return nil
}

// worker is the main loop for an index maintenance worker goroutine
func (s *IndexMaintenanceScheduler) worker(id int) {
	defer s.workerWg.Done()

	s.logger.Debugf("Index maintenance worker %d started", id)

	for {
		select {
		case <-s.ctx.Done():
			s.logger.Debugf("Worker %d shutting down", id)
			return

		case <-s.workAvailable:
			// Try to get work from queue
			s.queueMutex.Lock()
			if s.requestQueue.Len() == 0 {
				s.queueMutex.Unlock()
				continue
			}

			req := heap.Pop(s.requestQueue).(*IndexMaintenanceRequest)
			queueLen := s.requestQueue.Len()
			s.queueMutex.Unlock()

			// If more work available, signal another worker
			if queueLen > 0 {
				select {
				case s.workAvailable <- struct{}{}:
				default:
				}
			}

			// Process the rebuild request
			s.processRebuild(id, req)
		}
	}
}

// processRebuild handles rebuilding a single index
func (s *IndexMaintenanceScheduler) processRebuild(workerID int, req *IndexMaintenanceRequest) {
	indexKey := fmt.Sprintf("%s:%s:%s", req.DatabaseName, req.BundleName, req.IndexName)

	s.logger.Infof("Worker %d: starting rebuild for %s (type: %s, staleness: %.1f%%, priority: %.2f)",
		workerID, indexKey, req.IndexType, req.StalenessRate*100, req.Priority)

	// Mark as active
	s.activeWork.Store(indexKey, true)
	defer s.activeWork.Delete(indexKey)

	startTime := time.Now()

	// Get bundle
	bundleInterface, err := s.bundleService.GetBundleByName(req.BundleName)
	if err != nil {
		s.logger.Errorf("Worker %d: failed to get bundle %s: %v", workerID, req.BundleName, err)
		atomic.AddInt64(&s.totalRebuildsFailed, 1)
		return
	}

	// Call appropriate rebuild function based on index type
	var rebuildErr error
	switch req.IndexType {
	case "hash":
		_, rebuildErr = s.bundleService.RebuildHashIndexFromBundle(bundleInterface, req.IndexName, req.DatabaseName)
	case "btree":
		_, rebuildErr = s.bundleService.RebuildBTreeIndexFromBundle(bundleInterface, req.IndexName)
	default:
		s.logger.Errorf("Worker %d: unknown index type %s", workerID, req.IndexType)
		atomic.AddInt64(&s.totalRebuildsFailed, 1)
		return
	}

	duration := time.Since(startTime)

	if rebuildErr != nil {
		s.logger.Errorf("Worker %d: rebuild failed for %s: %v", workerID, indexKey, rebuildErr)
		atomic.AddInt64(&s.totalRebuildsFailed, 1)
		return
	}

	// Update metrics
	atomic.AddInt64(&s.totalRebuildsCompleted, 1)

	s.logger.Infof("Worker %d: completed rebuild for %s in %v",
		workerID, indexKey, duration)
}

// GetMetrics returns scheduler metrics
func (s *IndexMaintenanceScheduler) GetMetrics() map[string]interface{} {
	s.queueMutex.Lock()
	queueLen := s.requestQueue.Len()
	s.queueMutex.Unlock()

	return map[string]interface{}{
		"scheduled": atomic.LoadInt64(&s.totalRebuildsScheduled),
		"completed": atomic.LoadInt64(&s.totalRebuildsCompleted),
		"failed":    atomic.LoadInt64(&s.totalRebuildsFailed),
		"queue_len": queueLen,
	}
}
