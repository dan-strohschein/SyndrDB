package bundlestore

import (
	"container/heap"
	"context"
	"sync"
	"time"

	"go.uber.org/zap"
)

/*
COMPACTION SCHEDULER

PostgreSQL autovacuum-inspired parallel compaction worker pool.
Manages multiple workers, priority queue, and work coordination.

POSTGRESQL AUTOVACUUM PARALLELS:
- autovacuum_max_workers → maxWorkers (default: 3)
- autovacuum_naptime → evaluationInterval (default: 1 minute)
- Priority-based scheduling (high tombstone ratio = urgent)
- Prevents duplicate work on same bundle
*/

// CompactionScheduler manages parallel compaction workers with priority queue
type CompactionScheduler struct {
	compactor *BundleCompactor
	logger    *zap.SugaredLogger

	// Worker pool configuration
	maxWorkers         int
	evaluationInterval time.Duration

	// Work coordination
	requestQueue  *CompactionQueue
	queueMutex    sync.Mutex
	workAvailable chan struct{} // Signal for workers when new work arrives
	workerWg      sync.WaitGroup
	ctx           context.Context
	cancel        context.CancelFunc

	// Track active work to prevent duplicates
	activeWork sync.Map // bundleKey (db:bundle) -> bool
}

// NewCompactionScheduler creates a new compaction scheduler with worker pool
func NewCompactionScheduler(compactor *BundleCompactor, maxWorkers int, logger *zap.SugaredLogger) *CompactionScheduler {
	ctx, cancel := context.WithCancel(context.Background())

	return &CompactionScheduler{
		compactor:          compactor,
		logger:             logger,
		maxWorkers:         maxWorkers,
		evaluationInterval: 60 * time.Second, // 1 minute like autovacuum_naptime
		requestQueue:       &CompactionQueue{},
		workAvailable:      make(chan struct{}, 1),
		ctx:                ctx,
		cancel:             cancel,
	}
}

// Start launches the worker pool and periodic evaluation
func (cs *CompactionScheduler) Start() {
	cs.logger.Infof("Starting compaction scheduler with %d workers", cs.maxWorkers)

	// Initialize the priority queue
	heap.Init(cs.requestQueue)

	// Start worker goroutines
	for i := 0; i < cs.maxWorkers; i++ {
		cs.workerWg.Add(1)
		go cs.worker(i)
	}

	// Start periodic evaluation (like autovacuum launcher)
	go cs.evaluationLoop()

	cs.logger.Info("Compaction scheduler started successfully")
}

// Stop gracefully shuts down the scheduler and all workers
func (cs *CompactionScheduler) Stop() {
	cs.logger.Info("Stopping compaction scheduler...")
	cs.cancel()
	cs.workerWg.Wait()
	cs.logger.Info("Compaction scheduler stopped")
}

// ScheduleCompaction adds a compaction request to the priority queue
func (cs *CompactionScheduler) ScheduleCompaction(req CompactionRequest) {
	bundleKey := req.DatabaseName + ":" + req.BundleName

	// Check if already compacting
	if _, exists := cs.activeWork.Load(bundleKey); exists {
		cs.logger.Debugf("Bundle %s already has active compaction, skipping", bundleKey)
		return
	}

	// Add to priority queue
	cs.queueMutex.Lock()
	heap.Push(cs.requestQueue, req)
	queueSize := cs.requestQueue.Len()
	cs.queueMutex.Unlock()

	cs.logger.Debugf("Scheduled compaction for %s (priority: %d, queue size: %d)",
		bundleKey, req.Priority, queueSize)

	// Signal workers that work is available
	select {
	case cs.workAvailable <- struct{}{}:
	default:
	}
}

// worker is the main loop for a compaction worker goroutine
func (cs *CompactionScheduler) worker(workerID int) {
	defer cs.workerWg.Done()

	cs.logger.Debugf("Compaction worker %d started", workerID)

	for {
		select {
		case <-cs.ctx.Done():
			cs.logger.Debugf("Compaction worker %d shutting down", workerID)
			return

		case <-cs.workAvailable:
			// Try to get work from the queue
			cs.queueMutex.Lock()
			if cs.requestQueue.Len() == 0 {
				cs.queueMutex.Unlock()
				continue
			}
			req := heap.Pop(cs.requestQueue).(CompactionRequest)
			cs.queueMutex.Unlock()

			bundleKey := req.DatabaseName + ":" + req.BundleName

			// Mark as active to prevent duplicate work
			if _, loaded := cs.activeWork.LoadOrStore(bundleKey, true); loaded {
				cs.logger.Debugf("Worker %d: bundle %s already being compacted, skipping", workerID, bundleKey)
				continue
			}

			// Perform compaction
			cs.logger.Infof("Worker %d: starting compaction for %s (trigger: %s, priority: %d)",
				workerID, bundleKey, req.Trigger.String(), req.Priority)

			startTime := time.Now()
			err := cs.compactor.Compact(req.DatabaseName, req.BundleName, req.Trigger)
			duration := time.Since(startTime)

			// Remove from active work
			cs.activeWork.Delete(bundleKey)

			if err != nil {
				cs.logger.Errorf("Worker %d: compaction failed for %s: %v", workerID, bundleKey, err)
			} else {
				cs.logger.Infof("Worker %d: compaction completed for %s in %s",
					workerID, bundleKey, duration)
			}

			// Check if more work is available
			cs.queueMutex.Lock()
			hasMore := cs.requestQueue.Len() > 0
			cs.queueMutex.Unlock()

			if hasMore {
				select {
				case cs.workAvailable <- struct{}{}:
				default:
				}
			}
		}
	}
}

// evaluationLoop periodically checks all bundles for compaction needs
// Similar to PostgreSQL's autovacuum launcher process
func (cs *CompactionScheduler) evaluationLoop() {
	ticker := time.NewTicker(cs.evaluationInterval)
	defer ticker.Stop()

	cs.logger.Debugf("Compaction evaluation loop started (interval: %s)", cs.evaluationInterval)

	for {
		select {
		case <-cs.ctx.Done():
			cs.logger.Debug("Compaction evaluation loop shutting down")
			return

		case <-ticker.C:
			cs.evaluateBundles()
		}
	}
}

// evaluateBundles checks all bundles and schedules compaction if needed
func (cs *CompactionScheduler) evaluateBundles() {
	// TODO: This will be enhanced to iterate through all bundles in the database
	// For now, this is a placeholder that can be called manually via EvaluateBundle()
	cs.logger.Debug("Periodic bundle evaluation triggered")
}

// EvaluateBundle checks if a specific bundle needs compaction and schedules it
// This should be called periodically or after significant write activity
func (cs *CompactionScheduler) EvaluateBundle(databaseName, bundleName string) {
	shouldCompact, trigger, err := cs.compactor.ShouldCompact(databaseName, bundleName)
	if err != nil {
		cs.logger.Warnf("Failed to evaluate compaction for %s:%s: %v", databaseName, bundleName, err)
		return
	}

	if !shouldCompact {
		return
	}

	// Calculate priority (0-100 based on tombstone ratio)
	priority := cs.compactor.CalculatePriority(databaseName, bundleName)

	// Schedule compaction
	req := CompactionRequest{
		DatabaseName: databaseName,
		BundleName:   bundleName,
		Trigger:      trigger,
		Priority:     priority,
		RequestedAt:  time.Now(),
	}
	cs.ScheduleCompaction(req)
}

// GetQueueSize returns the current size of the compaction queue
func (cs *CompactionScheduler) GetQueueSize() int {
	cs.queueMutex.Lock()
	defer cs.queueMutex.Unlock()
	return cs.requestQueue.Len()
}

// GetActiveCompactionCount returns the number of currently running compactions
func (cs *CompactionScheduler) GetActiveCompactionCount() int {
	count := 0
	cs.activeWork.Range(func(key, value interface{}) bool {
		count++
		return true
	})
	return count
}

// CompactionQueue is a priority queue of compaction requests
// Higher priority values = more urgent (processed first)
type CompactionQueue []CompactionRequest

func (pq CompactionQueue) Len() int { return len(pq) }

func (pq CompactionQueue) Less(i, j int) bool {
	// Higher priority = more urgent (max heap)
	if pq[i].Priority != pq[j].Priority {
		return pq[i].Priority > pq[j].Priority
	}
	// If same priority, older request goes first (FIFO)
	return pq[i].RequestedAt.Before(pq[j].RequestedAt)
}

func (pq CompactionQueue) Swap(i, j int) {
	pq[i], pq[j] = pq[j], pq[i]
}

func (pq *CompactionQueue) Push(x interface{}) {
	*pq = append(*pq, x.(CompactionRequest))
}

func (pq *CompactionQueue) Pop() interface{} {
	old := *pq
	n := len(old)
	item := old[n-1]
	*pq = old[0 : n-1]
	return item
}
