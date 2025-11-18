package graphQL

import (
	"fmt"
	"sync"
	"syndrdb/src/pkg/settings"
	"time"

	"go.uber.org/zap"
)

// TokenBucket implements the token bucket rate limiting algorithm
// Tokens accumulate over time up to a capacity, and are consumed by operations
type TokenBucket struct {
	tokens       float64    // Current number of tokens (can be fractional)
	capacity     float64    // Maximum number of tokens
	refillRate   float64    // Tokens per second
	lastRefill   time.Time  // Last time tokens were refilled
	lastActivity time.Time  // Last time this bucket was used
	mu           sync.Mutex // Protects concurrent access
}

// NewTokenBucket creates a new token bucket with the specified capacity and refill rate
func NewTokenBucket(capacity int, tokensPerMinute int) *TokenBucket {
	now := time.Now()
	return &TokenBucket{
		tokens:       float64(capacity), // Start with full capacity
		capacity:     float64(capacity),
		refillRate:   float64(tokensPerMinute) / 60.0, // Convert to tokens per second
		lastRefill:   now,
		lastActivity: now,
	}
}

// TryConsume attempts to consume the specified number of tokens
// Returns true if successful, false if insufficient tokens
func (tb *TokenBucket) TryConsume(tokens int) bool {
	tb.mu.Lock()
	defer tb.mu.Unlock()

	// Refill tokens based on elapsed time
	now := time.Now()
	elapsed := now.Sub(tb.lastRefill).Seconds()

	// Add tokens based on refill rate (partial accumulation)
	tb.tokens += elapsed * tb.refillRate
	if tb.tokens > tb.capacity {
		tb.tokens = tb.capacity
	}
	tb.lastRefill = now

	// Check if we have enough tokens
	if tb.tokens >= float64(tokens) {
		tb.tokens -= float64(tokens)
		tb.lastActivity = now
		return true
	}

	// Update activity even on failure (for inactivity tracking)
	tb.lastActivity = now
	return false
}

// GetAvailableTokens returns the current number of available tokens
func (tb *TokenBucket) GetAvailableTokens() float64 {
	tb.mu.Lock()
	defer tb.mu.Unlock()

	// Refill tokens based on elapsed time
	now := time.Now()
	elapsed := now.Sub(tb.lastRefill).Seconds()

	tokens := tb.tokens + (elapsed * tb.refillRate)
	if tokens > tb.capacity {
		tokens = tb.capacity
	}

	return tokens
}

// GetLastActivity returns the last time this bucket was used
func (tb *TokenBucket) GetLastActivity() time.Time {
	tb.mu.Lock()
	defer tb.mu.Unlock()
	return tb.lastActivity
}

// RateLimiter manages rate limiting for GraphQL operations
type RateLimiter struct {
	config        *settings.GraphQLSecurityConfig
	logger        *zap.SugaredLogger
	buckets       map[string]*TokenBucket // Key: username or IP address
	mu            sync.RWMutex            // Protects buckets map
	stopCleanup   chan struct{}
	cleanupTicker *time.Ticker
}

// NewRateLimiter creates a new rate limiter with the specified configuration
func NewRateLimiter(config *settings.GraphQLSecurityConfig, logger *zap.SugaredLogger) *RateLimiter {
	rl := &RateLimiter{
		config:      config,
		logger:      logger,
		buckets:     make(map[string]*TokenBucket),
		stopCleanup: make(chan struct{}),
	}

	// Start cleanup goroutine to remove inactive buckets
	rl.startCleanupRoutine()

	return rl
}

// startCleanupRoutine starts a background goroutine to clean up inactive buckets
func (rl *RateLimiter) startCleanupRoutine() {
	rl.cleanupTicker = time.NewTicker(rl.config.InactivityTimeout)

	go func() {
		for {
			select {
			case <-rl.cleanupTicker.C:
				rl.cleanupInactiveBuckets()
			case <-rl.stopCleanup:
				rl.cleanupTicker.Stop()
				return
			}
		}
	}()
}

// cleanupInactiveBuckets removes buckets that haven't been used in the configured timeout
func (rl *RateLimiter) cleanupInactiveBuckets() {
	rl.mu.Lock()
	defer rl.mu.Unlock()

	now := time.Now()
	inactiveCutoff := now.Add(-rl.config.InactivityTimeout)
	removedCount := 0

	for key, bucket := range rl.buckets {
		if bucket.GetLastActivity().Before(inactiveCutoff) {
			delete(rl.buckets, key)
			removedCount++
		}
	}

	if removedCount > 0 {
		rl.logger.Debugf("Rate limiter cleanup: removed %d inactive buckets, %d remaining",
			removedCount, len(rl.buckets))
	}
}

// Stop stops the cleanup routine
func (rl *RateLimiter) Stop() {
	close(rl.stopCleanup)
}

// CheckRateLimit checks if the operation is allowed under rate limiting
// Returns true if allowed, false if rate limit exceeded
func (rl *RateLimiter) CheckRateLimit(username, clientIP, role string, isAdmin bool, operationType string, isDDL bool) (bool, error) {
	// Admin bypass
	if isAdmin {
		return true, nil
	}

	// Determine bucket key (username for authenticated, IP for anonymous)
	bucketKey := username
	if username == "" || role == "anonymous" {
		bucketKey = fmt.Sprintf("ip:%s", clientIP)
	}

	// Get or create bucket for this user/IP
	bucket := rl.getOrCreateBucket(bucketKey, role)

	// Determine operation cost
	cost := rl.getOperationCost(operationType, isDDL)

	// Try to consume tokens
	allowed := bucket.TryConsume(cost)

	if !allowed {
		rl.logger.Warnw("GraphQL rate limit exceeded",
			"username", username,
			"clientIP", clientIP,
			"role", role,
			"operationType", operationType,
			"cost", cost,
			"availableTokens", bucket.GetAvailableTokens(),
		)
	}

	return allowed, nil
}

// getOrCreateBucket gets an existing bucket or creates a new one for the user/IP
func (rl *RateLimiter) getOrCreateBucket(key, role string) *TokenBucket {
	// Try to get existing bucket (read lock)
	rl.mu.RLock()
	bucket, exists := rl.buckets[key]
	rl.mu.RUnlock()

	if exists {
		return bucket
	}

	// Create new bucket (write lock)
	rl.mu.Lock()
	defer rl.mu.Unlock()

	// Double-check in case another goroutine created it
	if bucket, exists := rl.buckets[key]; exists {
		return bucket
	}

	// Determine capacity and rate based on role
	var capacity int
	var queryRate int

	switch role {
	case "anonymous":
		queryRate = rl.config.AnonymousQueryRateLimit
		capacity = int(float64(queryRate) * rl.config.AnonymousBurstMultiplier)
	default: // authenticated
		queryRate = rl.config.AuthenticatedQueryRateLimit
		capacity = int(float64(queryRate) * rl.config.AuthenticatedBurstMultiplier)
	}

	// Use query rate as base rate (mutations cost more tokens via getOperationCost)
	tokensPerMinute := queryRate

	bucket = NewTokenBucket(capacity, tokensPerMinute)
	rl.buckets[key] = bucket

	rl.logger.Debugf("Created rate limit bucket: key=%s, role=%s, capacity=%d, rate=%d/min",
		key, role, capacity, tokensPerMinute)

	return bucket
}

// getOperationCost returns the token cost for an operation
func (rl *RateLimiter) getOperationCost(operationType string, isDDL bool) int {
	// DDL operations cost 10x
	if isDDL {
		return rl.config.DDLCost
	}

	// Mutation operations cost 5x
	if operationType == "mutation" {
		return rl.config.MutationCost
	}

	// Query operations cost 1x
	return rl.config.QueryCost
}

// GetBucketStats returns statistics about a specific bucket (for monitoring)
func (rl *RateLimiter) GetBucketStats(username, clientIP string) map[string]interface{} {
	bucketKey := username
	if username == "" {
		bucketKey = fmt.Sprintf("ip:%s", clientIP)
	}

	rl.mu.RLock()
	bucket, exists := rl.buckets[bucketKey]
	rl.mu.RUnlock()

	if !exists {
		return nil
	}

	return map[string]interface{}{
		"key":             bucketKey,
		"availableTokens": bucket.GetAvailableTokens(),
		"capacity":        bucket.capacity,
		"lastActivity":    bucket.GetLastActivity(),
	}
}

// GetGlobalStats returns global rate limiter statistics
func (rl *RateLimiter) GetGlobalStats() map[string]interface{} {
	rl.mu.RLock()
	defer rl.mu.RUnlock()

	return map[string]interface{}{
		"totalBuckets": len(rl.buckets),
	}
}
