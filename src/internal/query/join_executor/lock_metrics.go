// Package joinexecutor provides query join execution strategies.
//
// File: lock_metrics.go
//
// This file implements lock contention metrics tracking for join operations.
// It monitors TryLock/TryRLock failure rates across hash table cache and
// hash table read operations to detect and warn about lock contention issues.
//
// When the failure rate exceeds the configurable threshold (default 50%),
// warnings are logged to help diagnose concurrency bottlenecks.
//
// This is a diagnostic tool - high failure rates indicate that the system
// is experiencing significant lock contention under load.
package joinexecutor

import (
	"log"
	"sync"
	"sync/atomic"
	"time"

	"syndrdb/src/pkg/settings"
)

// LockMetrics tracks TryLock failure rates for concurrency diagnostics.
// Thread-safe for concurrent updates.
type LockMetrics struct {
	// Cache lock metrics
	cacheAttempts atomic.Int64
	cacheFailures atomic.Int64

	// Hash table read lock metrics
	hashTableAttempts atomic.Int64
	hashTableFailures atomic.Int64

	// Last warning times to avoid log spam
	lastCacheWarning     time.Time
	lastHashTableWarning time.Time
	warningMu            sync.Mutex

	// Minimum interval between warnings
	warningInterval time.Duration
}

// Global lock metrics instance
var globalLockMetrics *LockMetrics
var lockMetricsOnce sync.Once

// GetLockMetrics returns the singleton lock metrics instance.
func GetLockMetrics() *LockMetrics {
	lockMetricsOnce.Do(func() {
		globalLockMetrics = &LockMetrics{
			warningInterval: 30 * time.Second, // Warn at most every 30 seconds
		}
	})
	return globalLockMetrics
}

// RecordCacheAttempt records a TryLock attempt on the cache.
// Pass true for failure if TryLock returned false.
func (m *LockMetrics) RecordCacheAttempt(failure bool) {
	m.cacheAttempts.Add(1)
	if failure {
		m.cacheFailures.Add(1)
		m.checkCacheThreshold()
	}
}

// RecordHashTableAttempt records a TryRLock attempt on a hash table.
// Pass true for failure if TryRLock returned false.
func (m *LockMetrics) RecordHashTableAttempt(failure bool) {
	m.hashTableAttempts.Add(1)
	if failure {
		m.hashTableFailures.Add(1)
		m.checkHashTableThreshold()
	}
}

// checkCacheThreshold checks if cache lock failures exceed threshold and warns.
func (m *LockMetrics) checkCacheThreshold() {
	attempts := m.cacheAttempts.Load()
	if attempts < 100 {
		// Not enough samples for meaningful rate
		return
	}

	failures := m.cacheFailures.Load()
	rate := float64(failures) / float64(attempts)
	threshold := settings.GetSettings().TryLockFailureWarningThreshold

	if rate > threshold {
		m.warnCache(rate, attempts, failures)
	}
}

// checkHashTableThreshold checks if hash table lock failures exceed threshold and warns.
func (m *LockMetrics) checkHashTableThreshold() {
	attempts := m.hashTableAttempts.Load()
	if attempts < 100 {
		// Not enough samples for meaningful rate
		return
	}

	failures := m.hashTableFailures.Load()
	rate := float64(failures) / float64(attempts)
	threshold := settings.GetSettings().TryLockFailureWarningThreshold

	if rate > threshold {
		m.warnHashTable(rate, attempts, failures)
	}
}

// warnCache logs a warning about high cache lock contention.
func (m *LockMetrics) warnCache(rate float64, attempts, failures int64) {
	m.warningMu.Lock()
	defer m.warningMu.Unlock()

	now := time.Now()
	if now.Sub(m.lastCacheWarning) < m.warningInterval {
		return // Rate limit warnings
	}
	m.lastCacheWarning = now

	log.Printf("[WARN] High hash table cache lock contention: %.1f%% failure rate (%d/%d attempts). "+
		"Consider increasing cache shards or reducing concurrent joins.",
		rate*100, failures, attempts)
}

// warnHashTable logs a warning about high hash table read lock contention.
func (m *LockMetrics) warnHashTable(rate float64, attempts, failures int64) {
	m.warningMu.Lock()
	defer m.warningMu.Unlock()

	now := time.Now()
	if now.Sub(m.lastHashTableWarning) < m.warningInterval {
		return // Rate limit warnings
	}
	m.lastHashTableWarning = now

	log.Printf("[WARN] High hash table read lock contention: %.1f%% failure rate (%d/%d attempts). "+
		"This may indicate excessive concurrent probing of the same hash table.",
		rate*100, failures, attempts)
}

// GetCacheStats returns cache lock attempt statistics: (attempts, failures, rate).
func (m *LockMetrics) GetCacheStats() (int64, int64, float64) {
	attempts := m.cacheAttempts.Load()
	failures := m.cacheFailures.Load()
	rate := 0.0
	if attempts > 0 {
		rate = float64(failures) / float64(attempts)
	}
	return attempts, failures, rate
}

// GetHashTableStats returns hash table lock attempt statistics: (attempts, failures, rate).
func (m *LockMetrics) GetHashTableStats() (int64, int64, float64) {
	attempts := m.hashTableAttempts.Load()
	failures := m.hashTableFailures.Load()
	rate := 0.0
	if attempts > 0 {
		rate = float64(failures) / float64(attempts)
	}
	return attempts, failures, rate
}

// Reset clears all metrics. Useful for testing or after configuration changes.
func (m *LockMetrics) Reset() {
	m.cacheAttempts.Store(0)
	m.cacheFailures.Store(0)
	m.hashTableAttempts.Store(0)
	m.hashTableFailures.Store(0)

	m.warningMu.Lock()
	m.lastCacheWarning = time.Time{}
	m.lastHashTableWarning = time.Time{}
	m.warningMu.Unlock()
}
