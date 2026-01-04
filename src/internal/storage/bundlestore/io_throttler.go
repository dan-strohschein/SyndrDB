package bundlestore

import (
	"fmt"
	"sync"
	"time"

	"go.uber.org/zap"
)

/*
I/O THROTTLER

Token bucket algorithm for rate-limiting compaction I/O operations.
PostgreSQL autovacuum_cost_delay inspired throttling with normal/degraded modes.

POSTGRESQL AUTOVACUUM PARALLELS:
- autovacuum_cost_delay → Sleep duration when bucket is empty
- autovacuum_cost_limit → Token bucket capacity
- autovacuum_vacuum_cost_page_hit/dirty → Cost per operation

TOKEN BUCKET ALGORITHM:
- Tokens refill at a constant rate (bytesPerSecond)
- Each I/O operation consumes tokens equal to bytes transferred
- When bucket is empty, operation blocks until tokens available
- Supports dynamic rate adjustment (normal ↔ degraded mode)
*/

// IOThrottler implements token bucket rate limiting for I/O operations
type IOThrottler struct {
	mu sync.Mutex

	// Token bucket state
	tokens         float64   // Current available tokens (bytes)
	maxTokens      float64   // Bucket capacity (bytes)
	refillRate     float64   // Tokens added per second (bytes/sec)
	lastRefillTime time.Time // Last time tokens were refilled

	// Configuration
	normalRateMBps   float64 // Normal mode rate (MB/s)
	degradedRateMBps float64 // Degraded mode rate (MB/s)
	isDegraded       bool    // Current throttling mode

	// Statistics
	totalBytesThrottled int64         // Total bytes processed with throttling
	totalSleepTime      time.Duration // Total time spent sleeping
	throttleCount       int64         // Number of times throttled

	logger *zap.SugaredLogger
}

// NewIOThrottler creates a new I/O throttler with token bucket algorithm
func NewIOThrottler(normalRateMBps, degradedRateMBps float64, logger *zap.SugaredLogger) *IOThrottler {
	normalRateBytes := normalRateMBps * 1024 * 1024 // Convert MB/s to bytes/s

	// Start in normal mode
	refillRate := normalRateBytes

	// Bucket capacity = 2 seconds worth of tokens (allows bursts)
	maxTokens := refillRate * 2.0

	return &IOThrottler{
		tokens:           maxTokens, // Start with full bucket
		maxTokens:        maxTokens,
		refillRate:       refillRate,
		lastRefillTime:   time.Now(),
		normalRateMBps:   normalRateMBps,
		degradedRateMBps: degradedRateMBps,
		isDegraded:       false,
		logger:           logger,
	}
}

// Throttle blocks until sufficient tokens are available for the given number of bytes
// This is the main entry point for rate-limiting I/O operations
func (t *IOThrottler) Throttle(bytes int64) {
	if bytes <= 0 {
		return
	}

	t.mu.Lock()
	defer t.mu.Unlock()

	// Refill tokens based on elapsed time
	t.refillTokens()

	// If we have enough tokens, consume and return immediately
	if t.tokens >= float64(bytes) {
		t.tokens -= float64(bytes)
		t.totalBytesThrottled += bytes
		return
	}

	// Not enough tokens - need to wait
	tokensNeeded := float64(bytes) - t.tokens

	// Calculate sleep duration based on refill rate
	// sleepDuration = tokensNeeded / refillRate
	sleepDuration := time.Duration(float64(time.Second) * (tokensNeeded / t.refillRate))

	// Track statistics
	t.totalSleepTime += sleepDuration
	t.throttleCount++

	if t.logger != nil {
		t.logger.Debugf("I/O throttle: sleeping for %s (bytes: %d, tokens: %.0f, refillRate: %.0f/s)",
			sleepDuration, bytes, t.tokens, t.refillRate)
	}

	// Release lock while sleeping (allows other operations to check status)
	t.mu.Unlock()
	time.Sleep(sleepDuration)
	t.mu.Lock()

	// Refill again after sleep and consume tokens
	t.refillTokens()
	t.tokens -= float64(bytes)
	if t.tokens < 0 {
		t.tokens = 0 // Prevent negative tokens
	}

	t.totalBytesThrottled += bytes
}

// refillTokens adds tokens to the bucket based on elapsed time
// Must be called with lock held
func (t *IOThrottler) refillTokens() {
	now := time.Now()
	elapsed := now.Sub(t.lastRefillTime).Seconds()

	if elapsed <= 0 {
		return
	}

	// Add tokens based on refill rate and elapsed time
	tokensToAdd := t.refillRate * elapsed
	t.tokens += tokensToAdd

	// Cap at maximum bucket capacity (prevents unbounded burst)
	if t.tokens > t.maxTokens {
		t.tokens = t.maxTokens
	}

	t.lastRefillTime = now
}

// SetDegradedMode switches to degraded I/O mode (lower rate)
// Similar to PostgreSQL reducing autovacuum_cost_limit during high load
func (t *IOThrottler) SetDegradedMode(degraded bool) {
	t.mu.Lock()
	defer t.mu.Unlock()

	if t.isDegraded == degraded {
		return // No change
	}

	// Refill tokens before changing rate
	t.refillTokens()

	t.isDegraded = degraded

	if degraded {
		// Switch to degraded mode (lower rate)
		t.refillRate = t.degradedRateMBps * 1024 * 1024
		t.logger.Infof("I/O throttler switched to DEGRADED mode (%.1f MB/s)", t.degradedRateMBps)
	} else {
		// Switch to normal mode (higher rate)
		t.refillRate = t.normalRateMBps * 1024 * 1024
		t.logger.Infof("I/O throttler switched to NORMAL mode (%.1f MB/s)", t.normalRateMBps)
	}

	// Adjust bucket capacity for new rate (2 seconds worth)
	t.maxTokens = t.refillRate * 2.0

	// Cap current tokens if they exceed new capacity
	if t.tokens > t.maxTokens {
		t.tokens = t.maxTokens
	}
}

// GetStatistics returns current throttling statistics
func (t *IOThrottler) GetStatistics() ThrottleStats {
	t.mu.Lock()
	defer t.mu.Unlock()

	return ThrottleStats{
		TotalBytesThrottled: t.totalBytesThrottled,
		TotalSleepTime:      t.totalSleepTime,
		ThrottleCount:       t.throttleCount,
		CurrentMode:         t.getMode(),
		CurrentRateMBps:     t.refillRate / (1024 * 1024),
		AvailableTokens:     int64(t.tokens),
	}
}

// getMode returns the current throttling mode
// Must be called with lock held
func (t *IOThrottler) getMode() string {
	if t.isDegraded {
		return "degraded"
	}
	return "normal"
}

// Reset resets throttling statistics and refills the bucket
func (t *IOThrottler) Reset() {
	t.mu.Lock()
	defer t.mu.Unlock()

	t.totalBytesThrottled = 0
	t.totalSleepTime = 0
	t.throttleCount = 0
	t.tokens = t.maxTokens // Refill bucket
	t.lastRefillTime = time.Now()

	t.logger.Debug("I/O throttler statistics reset")
}

// ThrottleStats holds I/O throttling statistics
type ThrottleStats struct {
	TotalBytesThrottled int64         // Total bytes processed
	TotalSleepTime      time.Duration // Total time spent throttled
	ThrottleCount       int64         // Number of throttle events
	CurrentMode         string        // "normal" or "degraded"
	CurrentRateMBps     float64       // Current rate in MB/s
	AvailableTokens     int64         // Currently available tokens (bytes)
}

// String returns a human-readable summary of throttle statistics
func (s ThrottleStats) String() string {
	avgSleep := time.Duration(0)
	if s.ThrottleCount > 0 {
		avgSleep = s.TotalSleepTime / time.Duration(s.ThrottleCount)
	}

	return fmt.Sprintf("mode=%s rate=%.1f MB/s bytes=%d throttles=%d totalSleep=%s avgSleep=%s tokens=%d",
		s.CurrentMode, s.CurrentRateMBps, s.TotalBytesThrottled, s.ThrottleCount,
		s.TotalSleepTime, avgSleep, s.AvailableTokens)
}
