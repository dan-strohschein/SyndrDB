package server

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"os"
	"sync"
	"sync/atomic"
	"syndrdb/src/internal/domain/models"
	"syndrdb/src/internal/journal"
	"syndrdb/src/internal/storage/buffer"
	"syndrdb/src/internal/syndrQL"
	"syndrdb/src/pkg/common/helpers"
	"syndrdb/src/pkg/constants"
	"syndrdb/src/pkg/errors"
	"syndrdb/src/pkg/fatal"
	"syndrdb/src/pkg/settings"
	"syndrdb/src/pkg/trigger"
	"time"

	"github.com/dan-strohschein/HVJson/hvjson"
	"go.uber.org/zap"
)

// TransactionStatus represents the status of a multi-statement transaction
type TransactionStatus int

const (
	TransactionStatusInactive  TransactionStatus = iota // No active transaction
	TransactionStatusActive                             // Transaction active, operations being buffered
	TransactionStatusCommitted                          // Transaction successfully committed
	TransactionStatusAborted                            // Transaction aborted/rolled back
)

func (ts TransactionStatus) String() string {
	switch ts {
	case TransactionStatusInactive:
		return "INACTIVE"
	case TransactionStatusActive:
		return "ACTIVE"
	case TransactionStatusCommitted:
		return "COMMITTED"
	case TransactionStatusAborted:
		return "ABORTED"
	default:
		return "UNKNOWN"
	}
}

// Savepoint represents a savepoint within a transaction
type Savepoint struct {
	Name string // Alphanumeric name of the savepoint
	LSN  uint64 // WAL LSN at savepoint creation
}

// SessionState represents the current state of a session
type SessionState int

const (
	SessionStateActive SessionState = iota
	SessionStateIdle
	SessionStateExecuting
	SessionStateError
	SessionStateExpired
	SessionStateTerminated
)

func (s SessionState) String() string {
	switch s {
	case SessionStateActive:
		return "ACTIVE"
	case SessionStateIdle:
		return "IDLE"
	case SessionStateExecuting:
		return "EXECUTING"
	case SessionStateError:
		return "ERROR"
	case SessionStateExpired:
		return "EXPIRED"
	case SessionStateTerminated:
		return "TERMINATED"
	default:
		return "UNKNOWN"
	}
}

// QueryInfo represents information about a query
type QueryInfo struct {
	QueryID      string
	Query        string
	StartTime    time.Time
	EndTime      *time.Time
	Status       string // "EXECUTING", "COMPLETED", "FAILED"
	AffectedRows int
	Error        error
}

// LockInfo represents a lock held by the session
type LockInfo struct {
	LockID       string
	LockType     string // "DOCUMENT", "BUNDLE", "TABLE"
	ResourceName string
	LockMode     string // "READ", "WRITE", "EXCLUSIVE"
	AcquiredAt   time.Time
}

// Session represents a user session with comprehensive state tracking
type Session struct {
	SessionID          string
	UserID             string
	Username           string
	DatabaseName       string
	Database           *models.Database
	DatabaseFolderPath string // STEP 4: Cached database folder path (avoid repeated filepath.Join)
	ConnectionID       string // Associated connection ID

	// Security binding to prevent session hijacking
	ClientIP         string // IP address bound to this session
	UserAgent        string // User agent fingerprint for additional validation
	IPValidationHash string // Hash of IP+session for integrity checking

	// State tracking
	State        SessionState
	CreatedAt    time.Time
	LastActivity time.Time
	ExpiresAt    time.Time

	// Query tracking
	LastSuccessfulQuery *QueryInfo
	CurrentQuery        *QueryInfo
	QueryHistory        []*QueryInfo

	// Lock management
	DocumentLocks map[string]*LockInfo // documentID -> LockInfo
	BundleLocks   map[string]*LockInfo // bundleName -> LockInfo

	// Error tracking
	LastError         error
	ErrorCount        int
	ConsecutiveErrors int

	// Resource management
	BufferPool         *buffer.BufferPool
	TempFiles          []string
	ActiveTransactions map[string]context.CancelFunc // txID -> cancel function

	// Transaction state (for multi-statement transactions)
	TransactionActive    bool              // Whether a transaction is currently active
	ActiveTransactionID  string            // WAL transaction ID for the active transaction
	TransactionStartLSN  uint64            // LSN when transaction started (for rollback)
	TransactionStartTime time.Time         // When the transaction started (for idle timeout)
	PendingOperations    []string          // Buffered commands within the transaction
	CurrentSavepoint     *Savepoint        // Single-level savepoint (nil if no savepoint set)
	TransactionStatus    TransactionStatus // Current status of the transaction
	// PHASE 2: MVCC - TransactionBuffer tracks document locations for commit sequence assignment
	TransactionBuffer *TransactionBuffer // Tracks document locations written in this transaction

	// PHASE 1: MVCC - Snapshot created on BEGIN TRANSACTION, stored for read-path visibility
	MVCCSnapshot *journal.Snapshot

	// Transaction Isolation Levels
	DefaultIsolationLevel  syndrQL.IsolationLevel // Session-level default (persists across transactions)
	TransactionIsolation   syndrQL.IsolationLevel // Current transaction's isolation level
	PendingIsolationLevel  syndrQL.IsolationLevel // Set via SET TRANSACTION before BEGIN (cleared on BEGIN)

	// Prepared statement cache (session-scoped)
	PreparedStatements *syndrQL.ShardedPreparedStatementCache // Session-isolated prepared statement cache

	// Session configuration
	Timeout         time.Duration
	MaxQueryHistory int

	// Role caching for GraphQL security (5-minute TTL)
	CachedRole        string             // Cached user role string
	IsAdmin           bool               // Cached admin status
	RoleCacheTime     time.Time          // When the role was cached
	RoleCacheTTL      time.Duration      // TTL for role cache (default: 5 minutes)
	PermissionService *PermissionService // Reference to permission service for role lookup

	// Server-side cursor management
	Cursors *CursorManager

	// Trigger execution tracking
	TriggerDepth     atomic.Int32               // Current cascade depth
	TriggerCallStack []string                   // "bundle.trigger" names on stack (recursion detection)
	TriggerMutex     sync.Mutex                 // Protects TriggerCallStack
	TriggerContext   *trigger.ExecutionContext   // Current trigger context (for SET NEW)

	// Synchronization
	mu     sync.RWMutex
	Logger *zap.SugaredLogger
}

// SessionManager manages all active sessions
// LockReleaser interface allows SessionManager to release locks without importing storage package
// This avoids circular dependencies while enabling proper lock cleanup on session termination
type LockReleaser interface {
	ReleaseLocksForSession(sessionID string) int
	ReleaseLocks(txID string) int
}

type SessionManager struct {
	// PHASE 7: Sharded session storage for reduced lock contention
	// Each shard has its own mutex, reducing contention by ~64x
	sessions *ShardedSessionMap // sessionID -> Session (64 shards)

	// PHASE 7: Lock-free secondary indexes using sync.Map
	userSessions       *UserSessionIndex       // username -> list of sessions
	connectionSessions *ConnectionSessionIndex // connectionID -> Session

	// Legacy field for backward compatibility (redirects to userSessions)
	// sessionsByUser is deprecated - use userSessions instead

	// Light mutex only for non-session state (config, stats counters)
	// Session operations use the sharded session map's per-shard locks
	mu     sync.RWMutex
	logger *zap.SugaredLogger

	// Session count for maxSessions check (atomic for lock-free reads)
	sessionCount atomic.Int64

	// Configuration
	defaultTimeout  time.Duration
	maxSessions     int
	cleanupInterval time.Duration

	// Cleanup
	stopCleanup chan struct{}
	cleanupWG   sync.WaitGroup

	// Temp file cleanup queue (async, non-blocking)
	tempFileCleanupQueue chan []string
	tempFileCleanupWG    sync.WaitGroup
	stopTempFileCleanup  chan struct{}

	// Lock management - set after initialization to avoid circular dependencies
	lockReleaser LockReleaser

	// PERFORMANCE FIX: Eager sync.Map compaction on disconnect batches
	// Go's sync.Map.Delete() doesn't free memory - it marks entries as "expunged" but
	// they remain in internal structures. When clients disconnect and reconnect rapidly,
	// the accumulated tombstones degrade Load() performance significantly.
	// This counter tracks deletions since last compaction and triggers compaction
	// when a threshold is reached (default: 10 deletions).
	deletionsSinceCompact atomic.Int64

	// PERFORMANCE FIX: Callback for aggressive cache cleanup when all sessions disconnect
	// When session count reaches 0, we trigger full cache cleanup to free document memory
	// accumulated during the previous test/session cycle. This prevents latency degradation
	// across consecutive test runs that disconnect and reconnect all clients.
	onAllSessionsDisconnected func()

	// PERFORMANCE FIX: Flush gate to prevent new sessions during cache cleanup
	// When all sessions disconnect and we're flushing caches, new connections must wait
	// until the flush completes. Without this, clients that reconnect quickly can race
	// with the flush, leading to inconsistent latency (sometimes 20ms, sometimes 60ms).
	flushMu sync.RWMutex // Write lock held during flush, read lock for session creation
}

// NewSessionManager creates a new session manager
func NewSessionManager(logger *zap.SugaredLogger, defaultTimeout time.Duration, maxSessions int) *SessionManager {
	sm := &SessionManager{
		sessions:             NewShardedSessionMap(),
		userSessions:         NewUserSessionIndex(),
		connectionSessions:   NewConnectionSessionIndex(),
		logger:               logger,
		defaultTimeout:       defaultTimeout,
		maxSessions:          maxSessions,
		cleanupInterval:      time.Minute * 5, // Cleanup every 5 minutes
		stopCleanup:          make(chan struct{}),
		tempFileCleanupQueue: make(chan []string, 100), // Buffered channel for async cleanup
		stopTempFileCleanup:  make(chan struct{}),
	} // Start cleanup routine
	sm.startCleanupRoutine()
	sm.startTempFileCleanupWorker()

	return sm
}

// SetLockReleaser sets the lock releaser for the session manager
// This must be called after initialization to allow proper lock cleanup on session termination
// The LockReleaser is typically the storage.LockManager
func (sm *SessionManager) SetLockReleaser(lr LockReleaser) {
	sm.mu.Lock()
	defer sm.mu.Unlock()
	sm.lockReleaser = lr
	if sm.logger != nil {
		sm.logger.Info("SessionManager: LockReleaser set for proper lock cleanup on session termination")
	}
}

// SetOnAllSessionsDisconnected sets a callback that is triggered when the last session disconnects.
// This is used for aggressive cache cleanup between test runs or when all clients disconnect,
// helping prevent latency degradation from accumulated cached document data.
func (sm *SessionManager) SetOnAllSessionsDisconnected(callback func()) {
	sm.mu.Lock()
	defer sm.mu.Unlock()
	sm.onAllSessionsDisconnected = callback
	if sm.logger != nil {
		sm.logger.Info("SessionManager: Cache cleanup callback registered for all-sessions-disconnected event")
	}
}

// generateSecureSessionID generates a cryptographically secure session ID
func generateSecureSessionID() string {
	// Generate 32 random bytes (256 bits of entropy)
	bytes := make([]byte, 32)
	if _, err := rand.Read(bytes); err != nil {
		// Fallback to time-based ID if crypto/rand fails
		return fmt.Sprintf("sess_fallback_%d", time.Now().UnixNano())
	}

	// Encode as hexadecimal string
	return fmt.Sprintf("sess_%s", hex.EncodeToString(bytes))
}

// generateIPValidationHash creates a hash for IP address validation
func generateIPValidationHash(sessionID, clientIP, userAgent string) string {
	hasher := sha256.New()
	hasher.Write([]byte(sessionID))
	hasher.Write([]byte(clientIP))
	hasher.Write([]byte(userAgent))
	hasher.Write([]byte(time.Now().Format("2006-01-02"))) // Add date salt for daily rotation
	return hex.EncodeToString(hasher.Sum(nil))
}

// validateSessionBinding validates IP and user agent binding for session security
// RACE FIX: This function now requires the caller to hold session.mu.RLock or session.mu.Lock
// to avoid race conditions when reading session fields
func validateSessionBinding(session *Session, clientIP, userAgent string) error {
	if session == nil {
		return errors.New(errors.ERR_VALIDATION_FIELD,
			"session is nil", errors.LayerAPI)
	}

	// CRITICAL: Caller must hold session.mu lock to safely read these fields
	// Check IP address binding
	if session.ClientIP != clientIP {
		return errors.New(errors.ERR_AUTH_SESSION_EXPIRED,
			fmt.Sprintf("session IP mismatch: expected %s, got %s", session.ClientIP, clientIP),
			errors.LayerAuth).WithContext("expected_ip", session.ClientIP).WithContext("actual_ip", clientIP)
	}

	// Check user agent binding (allow some variance for browser updates)
	if session.UserAgent != userAgent {
		// Calculate similarity score for user agent (basic check for major differences)
		if !isUserAgentSimilar(session.UserAgent, userAgent) {
			return errors.New(errors.ERR_AUTH_SESSION_EXPIRED,
				"session user agent mismatch: significant difference detected",
				errors.LayerAuth)
		}
	}

	// Validate IP hash for integrity
	expectedHash := generateIPValidationHash(session.SessionID, clientIP, userAgent)
	if session.IPValidationHash != expectedHash {
		// Re-generate hash with current date in case it's a date change
		currentHash := generateIPValidationHash(session.SessionID, clientIP, session.UserAgent)
		if session.IPValidationHash != currentHash {
			return errors.New(errors.ERR_AUTH_SESSION_EXPIRED,
				"session validation hash mismatch: potential tampering detected",
				errors.LayerAuth)
		}
	}

	return nil
}

// isUserAgentSimilar checks if user agents are similar enough (basic similarity check)
func isUserAgentSimilar(original, current string) bool {
	if original == current {
		return true
	}

	// Basic similarity check - allow minor version differences
	// Extract major components and compare
	originalLen := len(original)
	currentLen := len(current)

	// If length difference is too large, consider them different
	if originalLen > 0 && currentLen > 0 {
		lengthDiff := originalLen - currentLen
		if lengthDiff < 0 {
			lengthDiff = -lengthDiff
		}

		// Allow up to 30% length difference for minor version updates
		maxDiff := originalLen * 30 / 100
		if lengthDiff <= maxDiff {
			return true
		}
	}

	return false
}

// CreateSession creates a new session for a user with IP binding and user-agent fingerprinting
func (sm *SessionManager) CreateSession(username, userID, databaseName string, database *models.Database, connectionID string, timeout time.Duration, clientIP, userAgent string) (*Session, error) {
	// PERFORMANCE FIX: Wait if cache flush is in progress
	// This prevents new sessions from racing with the all-sessions-disconnected flush,
	// which would cause inconsistent latency across test runs.
	sm.flushMu.RLock()
	defer sm.flushMu.RUnlock()

	// PHASE 7: Use atomic counter for lock-free session count check
	currentCount := sm.sessionCount.Load()
	if currentCount >= int64(sm.maxSessions) {
		return nil, errors.New(errors.ERR_RESOURCE_EXHAUSTED,
			fmt.Sprintf("maximum number of sessions (%d) reached", sm.maxSessions),
			errors.LayerAPI).WithContext("max_sessions", fmt.Sprintf("%d", sm.maxSessions))
	}

	// Generate secure session ID
	sessionID := generateSecureSessionID()

	if timeout <= 0 {
		timeout = sm.defaultTimeout
	}

	// Generate IP validation hash for integrity checking
	ipValidationHash := generateIPValidationHash(sessionID, clientIP, userAgent)

	// Initialize prepared statement cache if enabled
	var preparedStmtCache *syndrQL.ShardedPreparedStatementCache
	settingsArgs := settings.GetSettings()
	if settingsArgs.PreparedStatementCacheEnabled {
		preparedStmtCache = syndrQL.NewShardedPreparedStatementCache(
			settingsArgs.PreparedStatementCacheCapacity,
			sm.logger.With("sessionID", sessionID, "component", "prepared_stmt_cache"),
		)
	}

	session := &Session{
		SessionID:          sessionID,
		UserID:             userID,
		Username:           username,
		DatabaseName:       databaseName,
		Database:           database,
		DatabaseFolderPath: helpers.GetDatabaseFolderPath(databaseName), // STEP 4: Cache on creation
		ConnectionID:       connectionID,
		ClientIP:           clientIP,
		UserAgent:          userAgent,
		IPValidationHash:   ipValidationHash,
		State:              SessionStateActive,
		CreatedAt:          time.Now(),
		LastActivity:       time.Now(),
		ExpiresAt:          time.Now().Add(timeout),
		QueryHistory:       make([]*QueryInfo, 0, 100),
		DocumentLocks:      make(map[string]*LockInfo),
		BundleLocks:        make(map[string]*LockInfo),
		ActiveTransactions: make(map[string]context.CancelFunc),
		TempFiles:          make([]string, 0, 10),
		Timeout:            timeout,
		MaxQueryHistory:    100, // Keep last 100 queries
		Logger:             sm.logger.With("sessionID", sessionID, "username", username, "clientIP", clientIP),

		// Initialize prepared statement cache
		PreparedStatements: preparedStmtCache,

		// Initialize role caching fields
		RoleCacheTTL:      5 * time.Minute, // 5-minute TTL for role cache
		CachedRole:        "",              // Will be populated on first GetRole() call
		IsAdmin:           false,           // Will be set by GetRole()
		RoleCacheTime:     time.Time{},     // Zero time means cache is empty
		PermissionService: nil,             // Will be set by server initialization

		// Server-side cursor management
		Cursors: NewCursorManager(
			settingsArgs.MaxOpenCursorsPerSession,
			sm.logger.With("sessionID", sessionID, "component", "cursors"),
		),
	}

	// PHASE 7: Store session using sharded map and lock-free indexes
	sm.sessions.Set(sessionID, session)
	sm.connectionSessions.Set(connectionID, session)
	sm.userSessions.Add(username, session)
	sm.sessionCount.Add(1)

	// METRICS: Track session creation
	metrics := GetGlobalServerMetrics()
	metrics.SessionsCreated.Add(1)
	metrics.SessionsActive.Add(1)

	session.Logger.Infow("Session created with IP binding",
		"sessionID", sessionID,
		"username", username,
		"database", databaseName,
		"clientIP", clientIP,
		"userAgent", func() string {
			if len(userAgent) > constants.UserAgentMaxLength {
				return userAgent[:constants.UserAgentMaxLength]
			}
			return userAgent
		}(), // Truncate user agent for logging
		"timeout", timeout,
		"expiresAt", session.ExpiresAt)

	return session, nil
}

// GetSession retrieves a session by ID
// PHASE 7: Lock-free lookup using sharded session map (only touches 1 of 64 shards)
func (sm *SessionManager) GetSession(sessionID string) (*Session, bool) {
	return sm.sessions.Get(sessionID)
}

// GetSessionByConnection retrieves a session by connection ID
// PHASE 7: Lock-free lookup using sync.Map
func (sm *SessionManager) GetSessionByConnection(connectionID string) (*Session, bool) {
	return sm.connectionSessions.Get(connectionID)
}

// GetUserSessions retrieves all sessions for a user
// PHASE 7: Lock-free lookup using sync.Map with internal mutex for session list
func (sm *SessionManager) GetUserSessions(username string) []*Session {
	sessions := sm.userSessions.Get(username)
	if sessions == nil {
		return []*Session{}
	}
	return sessions
}

// GetActiveSessionsByTxID returns a map of active transaction IDs to session IDs
// This is used by the lock manager's CleanupOrphanedLocks to determine which
// transactions are still active and should not have their locks released
func (sm *SessionManager) GetActiveSessionsByTxID() map[string]string {
	activeTxMap := make(map[string]string)

	// PHASE 7: Iterate through sharded sessions
	sm.sessions.Range(func(sessionID string, session *Session) bool {
		if session.IsInTransaction() {
			activeTxMap[session.ActiveTransactionID] = session.SessionID
		}
		return true // continue iteration
	})

	return activeTxMap
}

// GetAllActiveSessionIDs returns all session IDs that are currently active
// This includes sessions processing regular queries (auto-commit mode), not just
// those with explicit transactions. Used by lock cleanup to avoid incorrectly
// releasing locks from sessions that are actively processing queries.
// CRITICAL: Without this, sessions running queries outside explicit transactions
// would have their locks released by CleanupOrphanedLocks, causing data corruption
// or query failures.
// PHASE 7: Uses sharded session iteration for reduced lock contention
func (sm *SessionManager) GetAllActiveSessionIDs() map[string]bool {
	activeIDs := make(map[string]bool)

	// PHASE 7: Iterate through sharded sessions
	sm.sessions.Range(func(sessionID string, session *Session) bool {
		// Only include sessions that are not expired or terminated
		session.mu.RLock()
		state := session.State
		session.mu.RUnlock()

		if state != SessionStateExpired && state != SessionStateTerminated {
			activeIDs[sessionID] = true
		}
		return true // continue iteration
	})

	return activeIDs
}

// InvalidateSession invalidates a specific session
// PHASE 7: Uses sharded session lookup
func (sm *SessionManager) InvalidateSession(sessionID string) error {
	session, exists := sm.sessions.Get(sessionID)
	if !exists {
		return errors.New(errors.ERR_AUTH_SESSION_EXPIRED,
			fmt.Sprintf("session %s not found", sessionID),
			errors.LayerAuth).WithContext("session_id", sessionID)
	}

	// METRICS: Track session termination
	metrics := GetGlobalServerMetrics()
	metrics.SessionsTerminated.Add(1)
	metrics.SessionsActive.Add(^uint64(0)) // Atomic decrement

	return sm.cleanupSession(session)
}

// InvalidateUserSessions invalidates all sessions for a user
// PHASE 7: Uses lock-free user session lookup
func (sm *SessionManager) InvalidateUserSessions(username string) error {
	sessions := sm.userSessions.Get(username)
	if sessions == nil {
		return nil
	}

	var lastError error
	for _, session := range sessions {
		if err := sm.cleanupSession(session); err != nil {
			lastError = err
		}
	}

	return lastError
}

// TerminateUserSessions forcefully terminates all active sessions for a user
// Sends a graceful disconnect message to each client before closing the connection
// This method is used when FORCE option is specified in RBAC commands
// Parameters:
//   - username: The username whose sessions should be terminated
//   - connectionMap: Map of connectionID to Connection objects (from Server.ActiveConnections)
//
// Returns:
//   - count: Number of sessions terminated
//   - error: Any error that occurred during termination
//
// TODO: I can add session termination event broadcast for monitoring systems
// TODO: I can add configurable termination grace period before forced disconnect
// PHASE 7: Uses lock-free user session lookup
func (sm *SessionManager) TerminateUserSessions(username string, connectionMap map[string]*Connection) (int, error) {
	sessions := sm.userSessions.Get(username)
	if sessions == nil {
		return 0, nil
	}

	terminatedCount := 0
	var lastError error

	for _, session := range sessions {
		// Get the connection for this session
		if conn, exists := connectionMap[session.ConnectionID]; exists && conn != nil {
			// Send graceful disconnect message to client before closing
			if conn.Writer != nil {
				sm.logger.Infow("Sending termination message to client",
					"username", username,
					"sessionID", session.SessionID,
					"connectionID", session.ConnectionID)

				// Send error message using the same format as sendError
				response := map[string]interface{}{
					"status":  "error",
					"message": "The administrator forcefully removed your session from the server",
				}
				if jsonResponse, err := hvjson.Marshal(response); err == nil {
					conn.Writer.WriteString(string(jsonResponse) + "\n")
					conn.Writer.Flush()
				}
			}

			// Close the connection
			if conn.Conn != nil {
				conn.Conn.Close()
			}
		}

		// Cleanup the session
		if err := sm.cleanupSession(session); err != nil {
			sm.logger.Warnw("Error cleaning up terminated session",
				"username", username,
				"sessionID", session.SessionID,
				"error", err)
			lastError = err
		} else {
			terminatedCount++
		}
	}

	sm.logger.Infow("Terminated user sessions",
		"username", username,
		"count", terminatedCount)

	return terminatedCount, lastError
}

// UpdateActivity updates the last activity time for a session with security validation
// DEADLOCK FIX: Acquire session.mu first, then validate, to avoid races and ensure atomicity
// PHASE 7: Uses lock-free sharded session lookup
func (sm *SessionManager) UpdateActivity(sessionID, clientIP, userAgent string) error {
	session, exists := sm.sessions.Get(sessionID)

	if !exists {
		return errors.New(errors.ERR_AUTH_SESSION_EXPIRED,
			fmt.Sprintf("session %s not found", sessionID),
			errors.LayerAuth).WithContext("session_id", sessionID)
	}

	// RACE FIX: Acquire session lock BEFORE validating to avoid race conditions
	session.mu.Lock()
	defer session.mu.Unlock()

	// Validate session binding with lock held (required by validateSessionBinding)
	if err := validateSessionBinding(session, clientIP, userAgent); err != nil {
		session.Logger.Warnw("Session security validation failed during activity update",
			"error", err,
			"clientIP", clientIP,
			"expectedIP", session.ClientIP,
			"userAgent", userAgent[:func() int {
				if len(userAgent) > constants.UserAgentTruncateLength {
					return constants.UserAgentTruncateLength
				}
				return len(userAgent)
			}()])
		return errors.WrapWithMessage(err, errors.ERR_VALIDATION_FIELD,
			"session security validation failed", errors.LayerAuth)
	}

	session.updateActivityTimestampsLocked()

	return nil
}

// SetDatabaseContext changes the database context for an existing session
// PHASE 7: Uses lock-free sharded session lookup
func (sm *SessionManager) SetDatabaseContext(sessionID string, databaseName string, database *models.Database) error {
	session, exists := sm.sessions.Get(sessionID)

	if !exists {
		return errors.New(errors.ERR_AUTH_SESSION_EXPIRED,
			fmt.Sprintf("session %s not found", sessionID),
			errors.LayerAuth).WithContext("session_id", sessionID)
	}

	session.mu.Lock()
	defer session.mu.Unlock()

	// Store previous database for logging
	previousDatabase := session.DatabaseName

	// Update the database context
	session.DatabaseName = databaseName
	session.Database = database
	session.DatabaseFolderPath = helpers.GetDatabaseFolderPath(databaseName) // STEP 4: Update cache

	// Update activity time when database context changes
	session.updateActivityTimestampsLocked()

	session.Logger.Infow("Database context changed",
		"previousDatabase", previousDatabase,
		"newDatabase", databaseName)

	return nil
}

// ValidateSessionSecurity validates session security binding
// RACE FIX: Acquire session lock before validation to avoid race conditions
// PHASE 7: Uses lock-free sharded session lookup
func (sm *SessionManager) ValidateSessionSecurity(sessionID, clientIP, userAgent string) error {
	session, exists := sm.sessions.Get(sessionID)

	if !exists {
		return errors.New(errors.ERR_AUTH_SESSION_EXPIRED,
			fmt.Sprintf("session %s not found", sessionID),
			errors.LayerAuth).WithContext("session_id", sessionID)
	}

	// RACE FIX: Acquire session lock BEFORE validating (required by validateSessionBinding)
	session.mu.RLock()
	defer session.mu.RUnlock()

	return validateSessionBinding(session, clientIP, userAgent)
}

// cleanupSession performs cleanup for a session
// PHASE 7: Uses sharded session map and lock-free indexes for removal
func (sm *SessionManager) cleanupSession(session *Session) error {
	session.mu.Lock()
	defer session.mu.Unlock()

	session.Logger.Infow("Cleaning up session", "state", session.State.String())

	// Set state to terminated
	session.State = SessionStateTerminated

	// Close all open cursors to release suspended iterators
	if session.Cursors != nil {
		session.Cursors.CloseAll()
	}

	// Cancel active transactions
	for txID, cancelFunc := range session.ActiveTransactions {
		session.Logger.Infow("Canceling active transaction", "transactionID", txID)
		cancelFunc()
	}
	session.ActiveTransactions = make(map[string]context.CancelFunc)

	// Release all locks
	for lockID, lockInfo := range session.DocumentLocks {
		session.Logger.Infow("Releasing document lock", "lockID", lockID, "resource", lockInfo.ResourceName)
	}
	session.DocumentLocks = make(map[string]*LockInfo)

	for lockID, lockInfo := range session.BundleLocks {
		session.Logger.Infow("Releasing bundle lock", "lockID", lockID, "resource", lockInfo.ResourceName)
	}
	session.BundleLocks = make(map[string]*LockInfo)

	// CRITICAL: Call LockManager.ReleaseLocksForSession() to release actual locks held.
	// Previously this code only cleared the session's internal tracking maps without releasing
	// the locks in the LockManager, causing orphaned locks and severe contention under high concurrency.
	if sm.lockReleaser != nil {
		sm.lockReleaser.ReleaseLocksForSession(session.SessionID)
		if session.ActiveTransactionID != "" {
			sm.lockReleaser.ReleaseLocks(session.ActiveTransactionID)
		}
	}

	// Clean up temp files (async, non-blocking)
	// Copy temp files slice to avoid race conditions and allow safe async processing
	tempFiles := make([]string, len(session.TempFiles))
	copy(tempFiles, session.TempFiles)
	session.TempFiles = []string{} // Clear the slice immediately

	// Enqueue files for async deletion (non-blocking)
	if len(tempFiles) > 0 {
		session.Logger.Infow("Enqueueing temp files for async cleanup", "count", len(tempFiles))
		select {
		case sm.tempFileCleanupQueue <- tempFiles:
			// Successfully enqueued
		default:
			// Queue is full - log warning but don't block
			// This prevents session cleanup from hanging if cleanup worker is overloaded
			session.Logger.Warnw("Temp file cleanup queue is full, files will not be cleaned up",
				"count", len(tempFiles),
				"files", tempFiles)
		}
	}

	// Clean up buffer pool if exists
	if session.BufferPool != nil {
		session.Logger.Info("Releasing buffer pool resources")

		// Flush any dirty buffers to ensure data integrity
		if err := session.BufferPool.FlushAllDirty(); err != nil {
			session.Logger.Warnw("Failed to flush dirty buffers during session cleanup",
				"error", err)
			// Continue with cleanup even if flush fails
		}

		// Attempt to clear the buffer pool (graceful - won't fail if buffers are in use)
		if err := session.BufferPool.ClearBufferPool(); err != nil {
			// If buffers are still in use, log warning but don't block session cleanup
			// This can happen if other goroutines are still accessing buffers
			session.Logger.Debugw("Could not fully clear buffer pool (buffers may still be in use)",
				"error", err)
		}

		session.BufferPool = nil
	}

	// CRITICAL FIX: Clean up PreparedStatements cache to prevent memory leak
	// Each session creates a ShardedPreparedStatementCache with 8 shards that persists
	// indefinitely if not explicitly cleared. This was causing ~100KB+ leak per disconnected session.
	if session.PreparedStatements != nil {
		session.PreparedStatements.Clear()
		session.PreparedStatements = nil
	}

	// Clean up query history to prevent memory accumulation
	// Each session can hold up to 100 QueryInfo objects (~15KB per session)
	session.QueryHistory = nil
	session.CurrentQuery = nil
	session.LastSuccessfulQuery = nil

	// Clear role cache to release references
	session.CachedRole = ""
	session.IsAdmin = false
	session.RoleCacheTime = time.Time{}

	// Defensive MVCC cleanup - ensure snapshot is released if session terminated mid-transaction
	if session.MVCCSnapshot != nil {
		session.MVCCSnapshot = nil
	}
	if session.TransactionBuffer != nil {
		session.TransactionBuffer.Clear()
		session.TransactionBuffer = nil
	}

	// PHASE 7: Remove from session manager using sharded structures
	sm.sessions.Delete(session.SessionID)
	sm.connectionSessions.Delete(session.ConnectionID)
	sm.userSessions.Remove(session.Username, session.SessionID)
	newCount := sm.sessionCount.Add(-1)

	// PERFORMANCE FIX: Trigger aggressive cache cleanup when all sessions disconnect
	// This clears accumulated document data in caches that would otherwise cause
	// latency degradation across consecutive test runs with disconnect/reconnect cycles.
	//
	// IMPORTANT: Uses flushMu write lock to block new session creation during flush.
	// This prevents the race where new clients connect and populate caches during flush,
	// which caused inconsistent latency (sometimes 20ms, sometimes 60ms) across test runs.
	if newCount == 0 {
		sm.mu.RLock()
		callback := sm.onAllSessionsDisconnected
		sm.mu.RUnlock()
		if callback != nil {
			// Acquire write lock to block all CreateSession calls during flush
			sm.flushMu.Lock()
			sm.logger.Info("All sessions disconnected - triggering cache cleanup (blocking new sessions)")
			callback()
			sm.logger.Info("Cache cleanup completed - new sessions now allowed")
			sm.flushMu.Unlock()
		}
	}

	// PERFORMANCE FIX: Trigger eager compaction after disconnect batches
	// Go's sync.Map.Delete() doesn't free memory - it marks entries as "expunged"
	// causing Load() performance to degrade over time. By compacting after every
	// 10 session cleanups, we prevent tombstone accumulation during rapid
	// disconnect/reconnect cycles (e.g., test runs with 30 clients that reconnect).
	deletions := sm.deletionsSinceCompact.Add(1)
	if deletions >= 10 {
		// Reset counter first to prevent concurrent compactions
		sm.deletionsSinceCompact.Store(0)
		// Compact in a goroutine to avoid blocking the cleanup path
		go func() {
			connCount, userCount := sm.CompactIndexes()
			sm.logger.Debugf("Eager session index compaction: %d connections, %d users (triggered after %d deletions)",
				connCount, userCount, deletions)
		}()
	}

	session.Logger.Info("Session cleanup completed")
	return nil
}

// startCleanupRoutine starts the background cleanup routine
func (sm *SessionManager) startCleanupRoutine() {
	sm.cleanupWG.Add(1)
	go func() {
		defer func() {
			if r := recover(); r != nil {
				fatal.LogFatalAndExit(r)
			}
		}()
		defer sm.cleanupWG.Done()

		ticker := time.NewTicker(sm.cleanupInterval)
		defer ticker.Stop()

		for {
			select {
			case <-ticker.C:
				sm.cleanupExpiredSessions()
			case <-sm.stopCleanup:
				return
			}
		}
	}()
}

// startTempFileCleanupWorker starts the background worker for async temp file cleanup
// This worker processes temp file deletion requests in batches to avoid blocking session cleanup
func (sm *SessionManager) startTempFileCleanupWorker() {
	sm.tempFileCleanupWG.Add(1)
	go func() {
		defer func() {
			if r := recover(); r != nil {
				sm.logger.Errorw("Temp file cleanup worker panicked, restarting", "error", r)
				// Restart the worker on panic
				sm.startTempFileCleanupWorker()
			}
			sm.tempFileCleanupWG.Done()
		}()

		batchSize := 20 // Process up to 20 files per batch
		maxRetries := 3
		retryDelays := []time.Duration{
			constants.RetryDelayShort * time.Millisecond,
			constants.RetryDelayMedium * time.Millisecond,
			constants.RetryDelayLong * time.Millisecond,
		}

		for {
			select {
			case files := <-sm.tempFileCleanupQueue:
				// Process files in batches
				for i := 0; i < len(files); i += batchSize {
					end := i + batchSize
					if end > len(files) {
						end = len(files)
					}
					batch := files[i:end]

					// Delete files in this batch
					for _, filePath := range batch {
						err := sm.deleteTempFileWithRetry(filePath, maxRetries, retryDelays)
						if err != nil {
							sm.logger.Warnw("Failed to delete temp file after retries",
								"file", filePath,
								"retries", maxRetries,
								"error", err)
						} else {
							sm.logger.Debugw("Successfully deleted temp file", "file", filePath)
						}
					}
				}

			case <-sm.stopTempFileCleanup:
				// Process any remaining files in queue before stopping
				for {
					select {
					case files := <-sm.tempFileCleanupQueue:
						for _, filePath := range files {
							_ = sm.deleteTempFileWithRetry(filePath, maxRetries, retryDelays)
						}
					default:
						return
					}
				}
			}
		}
	}()
}

// deleteTempFileWithRetry attempts to delete a temp file with exponential backoff retry logic
func (sm *SessionManager) deleteTempFileWithRetry(filePath string, maxRetries int, retryDelays []time.Duration) error {
	var lastErr error
	for attempt := 0; attempt < maxRetries; attempt++ {
		err := os.Remove(filePath)
		if err == nil {
			return nil // Success
		}

		lastErr = err

		// If file doesn't exist, consider it a success (already deleted)
		if os.IsNotExist(err) {
			return nil
		}

		// Wait before retry (except on last attempt)
		if attempt < maxRetries-1 {
			delay := retryDelays[attempt]
			if delay > 0 {
				time.Sleep(delay)
			}
		}
	}

	return errors.WrapWithMessage(lastErr, errors.ERR_INTERNAL_STORAGE,
		fmt.Sprintf("failed to delete temp file after %d attempts", maxRetries),
		errors.LayerAPI).WithContext("max_retries", fmt.Sprintf("%d", maxRetries))
}

// cleanupExpiredSessions removes expired sessions
// PHASE 7: Uses sharded session iteration and lock-free cleanup
func (sm *SessionManager) cleanupExpiredSessions() {
	now := time.Now()

	// PHASE 1: Find expired sessions using sharded iteration
	// Check both ExpiresAt and LastActivity as a safety net — if either indicates
	// the session has exceeded its timeout, consider it expired.
	expiredSessions := sm.sessions.CollectExpired(func(session *Session) bool {
		session.mu.RLock()
		isExpired := now.After(session.ExpiresAt) ||
			now.Sub(session.LastActivity) > session.Timeout
		session.mu.RUnlock()
		return isExpired
	})

	// PHASE 2: Cleanup expired sessions
	if len(expiredSessions) > 0 {
		for _, session := range expiredSessions {
			// Mark as expired with proper locking
			session.mu.Lock()
			session.State = SessionStateExpired
			expiresAt := session.ExpiresAt
			sessionID := session.SessionID
			username := session.Username
			session.mu.Unlock()

			sm.logger.Infow("Cleaning up expired session",
				"sessionID", sessionID,
				"username", username,
				"expiredAt", expiresAt)

			// PHASE 7: cleanupSession no longer requires sm.mu to be held
			sm.cleanupSession(session)
		}

		sm.logger.Infow("Cleaned up expired sessions", "count", len(expiredSessions))
	}
}

// Stop stops the session manager and cleans up all sessions
func (sm *SessionManager) Stop() {
	// Stop cleanup routines
	close(sm.stopCleanup)
	close(sm.stopTempFileCleanup)

	// Wait for both cleanup workers to finish
	sm.cleanupWG.Wait()
	sm.tempFileCleanupWG.Wait()

	// PHASE 7: Collect all sessions first, then clean them up
	// This avoids deadlock from holding read lock while cleanup tries to write lock
	var sessionsToCleanup []*Session
	sm.sessions.Range(func(sessionID string, session *Session) bool {
		sessionsToCleanup = append(sessionsToCleanup, session)
		return true
	})

	// Now cleanup each session outside of Range iteration
	for _, session := range sessionsToCleanup {
		sm.cleanupSession(session)
	}
}

// CompactIndexes recreates the session sync.Map indexes with only current entries.
// PERFORMANCE FIX: Go's sync.Map.Delete() doesn't free memory - it marks entries as
// "expunged" but they remain in internal structures. After many connect/disconnect
// cycles, the sync.Maps accumulate cruft that degrades Load() performance.
// This should be called periodically (e.g., every 60 seconds) to maintain performance.
// Returns the number of entries in each compacted index.
//
// PERFORMANCE FIX: Now also compacts the main ShardedSessionMap to reclaim memory
// from accumulated empty bucket slots after many connect/disconnect cycles.
func (sm *SessionManager) CompactIndexes() (connectionCount, userCount int) {
	connectionCount = sm.connectionSessions.Compact()
	userCount = sm.userSessions.Compact()
	// Compact the main session storage as well (reclaims map bucket memory)
	sm.sessions.Compact()
	return connectionCount, userCount
}

// GetSessionStats returns statistics about sessions
// PHASE 7: Uses sharded session iteration
func (sm *SessionManager) GetSessionStats() map[string]interface{} {
	// PHASE 3: Use pooled map to reduce allocations
	stats := GetResponseMap()

	// Get stats from sharded map
	shardStats := sm.sessions.GetStats()
	stats["total_sessions"] = shardStats["total_sessions"]
	stats["shard_count"] = shardStats["shard_count"]
	stats["min_shard_size"] = shardStats["min_shard_size"]
	stats["max_shard_size"] = shardStats["max_shard_size"]
	stats["avg_shard_size"] = shardStats["avg_shard_size"]

	// Count active users
	activeUsers := 0
	sm.userSessions.Range(func(username string, sessions []*Session) bool {
		if len(sessions) > 0 {
			activeUsers++
		}
		return true
	})
	stats["active_users"] = activeUsers

	stats["max_sessions"] = sm.maxSessions
	stats["default_timeout"] = sm.defaultTimeout.String()
	stats["cleanup_interval"] = sm.cleanupInterval.String()

	// Count sessions by state
	stateCounts := make(map[string]int)
	sm.sessions.Range(func(sessionID string, session *Session) bool {
		session.mu.RLock()
		state := session.State.String()
		session.mu.RUnlock()
		stateCounts[state]++
		return true
	})
	stats["sessions_by_state"] = stateCounts

	return stats
}

// Session methods for managing state and resources

// UpdateActivityTimestamps atomically updates both LastActivity and ExpiresAt.
// This ensures any session operation (query, cursor fetch, lock op, etc.) properly
// extends the session lifetime. Must be called with s.mu already held.
func (s *Session) updateActivityTimestampsLocked() {
	now := time.Now()
	s.LastActivity = now
	s.ExpiresAt = now.Add(s.Timeout)
}

// StartQuery starts a new query execution
func (s *Session) StartQuery(queryID, query string) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.State = SessionStateExecuting
	s.updateActivityTimestampsLocked()

	s.CurrentQuery = &QueryInfo{
		QueryID:   queryID,
		Query:     query,
		StartTime: time.Now(),
		Status:    "EXECUTING",
	}

	s.Logger.Debugf("Started query execution '%s'", query)
}

// CompleteQuery marks the current query as completed
func (s *Session) CompleteQuery(affectedRows int) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.CurrentQuery == nil {
		return
	}

	now := time.Now()
	s.CurrentQuery.EndTime = &now
	s.CurrentQuery.Status = "COMPLETED"
	s.CurrentQuery.AffectedRows = affectedRows

	// Move to successful query and history
	s.LastSuccessfulQuery = s.CurrentQuery
	s.addToHistory(s.CurrentQuery)

	s.CurrentQuery = nil
	s.State = SessionStateActive
	s.updateActivityTimestampsLocked()

	// Reset consecutive error count on success
	s.ConsecutiveErrors = 0

	// s.Logger.Infow("Completed query execution",
	//
	//	"queryID", s.LastSuccessfulQuery.QueryID,
	//	"affectedRows", affectedRows,
	//	"duration", s.LastSuccessfulQuery.EndTime.Sub(s.LastSuccessfulQuery.StartTime))
}

// FailQuery marks the current query as failed
func (s *Session) FailQuery(err error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.CurrentQuery == nil {
		return
	}

	now := time.Now()
	s.CurrentQuery.EndTime = &now
	s.CurrentQuery.Status = "FAILED"
	s.CurrentQuery.Error = err

	// Update error tracking
	s.LastError = err
	s.ErrorCount++
	s.ConsecutiveErrors++

	// Save queryID before setting CurrentQuery to nil
	queryID := s.CurrentQuery.QueryID
	s.addToHistory(s.CurrentQuery)
	s.CurrentQuery = nil

	// Set state based on error count
	if s.ConsecutiveErrors >= 5 {
		s.State = SessionStateError
		s.Logger.Errorw("Session entered error state due to consecutive failures",
			"consecutiveErrors", s.ConsecutiveErrors)
	} else {
		s.State = SessionStateActive
	}

	s.updateActivityTimestampsLocked()

	// Log the error - use Warnw for user validation errors (no stack trace),
	// Errorw for internal/system errors (includes stack trace for debugging)
	if sdbErr, ok := err.(errors.SyndrDBError); ok && sdbErr.Code().IsValidationError() {
		// User validation error - log as warning without stack trace
		s.Logger.Warnw("Query validation failed",
			"queryID", queryID,
			"error", err.Error(),
			"errorCode", sdbErr.Code().String(),
			"consecutiveErrors", s.ConsecutiveErrors)
	} else {
		// Internal/system error - log as error with full details
		s.Logger.Errorw("Query execution failed",
			"queryID", queryID,
			"error", err,
			"consecutiveErrors", s.ConsecutiveErrors)
	}
}

// addToHistory adds a query to the history (must be called with lock held)
func (s *Session) addToHistory(query *QueryInfo) {
	s.QueryHistory = append(s.QueryHistory, query)

	// Keep only the last MaxQueryHistory queries
	if len(s.QueryHistory) > s.MaxQueryHistory {
		s.QueryHistory = s.QueryHistory[len(s.QueryHistory)-s.MaxQueryHistory:]
	}
}

// AcquireDocumentLock acquires a lock on a document
func (s *Session) AcquireDocumentLock(documentID, lockMode string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	lockID := fmt.Sprintf("doc_%s_%s", documentID, lockMode)

	// Check if we already have this lock
	if _, exists := s.DocumentLocks[lockID]; exists {
		return errors.New(errors.ERR_INTERNAL_LOCK,
			fmt.Sprintf("document lock already held: %s", documentID),
			errors.LayerTransaction).WithContext("document_id", documentID)
	}

	lockInfo := &LockInfo{
		LockID:       lockID,
		LockType:     "DOCUMENT",
		ResourceName: documentID,
		LockMode:     lockMode,
		AcquiredAt:   time.Now(),
	}

	s.DocumentLocks[lockID] = lockInfo
	s.Logger.Infow("Acquired document lock",
		"documentID", documentID,
		"lockMode", lockMode,
		"lockID", lockID)

	return nil
}

// ReleaseDocumentLock releases a lock on a document
func (s *Session) ReleaseDocumentLock(documentID, lockMode string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	lockID := fmt.Sprintf("doc_%s_%s", documentID, lockMode)

	if _, exists := s.DocumentLocks[lockID]; !exists {
		return errors.New(errors.ERR_INTERNAL_LOCK,
			fmt.Sprintf("document lock not held: %s", documentID),
			errors.LayerTransaction).WithContext("document_id", documentID)
	}

	delete(s.DocumentLocks, lockID)
	s.Logger.Infow("Released document lock",
		"documentID", documentID,
		"lockMode", lockMode,
		"lockID", lockID)

	return nil
}

// AcquireBundleLock acquires a lock on a bundle
func (s *Session) AcquireBundleLock(bundleName, lockMode string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	lockID := fmt.Sprintf("bundle_%s_%s", bundleName, lockMode)

	// Check if we already have this lock
	if _, exists := s.BundleLocks[lockID]; exists {
		return errors.New(errors.ERR_INTERNAL_LOCK,
			fmt.Sprintf("bundle lock already held: %s", bundleName),
			errors.LayerTransaction).WithContext("bundle", bundleName)
	}

	lockInfo := &LockInfo{
		LockID:       lockID,
		LockType:     "BUNDLE",
		ResourceName: bundleName,
		LockMode:     lockMode,
		AcquiredAt:   time.Now(),
	}

	s.BundleLocks[lockID] = lockInfo
	s.Logger.Infow("Acquired bundle lock",
		"bundleName", bundleName,
		"lockMode", lockMode,
		"lockID", lockID)

	return nil
}

// ReleaseBundleLock releases a lock on a bundle
func (s *Session) ReleaseBundleLock(bundleName, lockMode string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	lockID := fmt.Sprintf("bundle_%s_%s", bundleName, lockMode)

	if _, exists := s.BundleLocks[lockID]; !exists {
		return errors.New(errors.ERR_INTERNAL_LOCK,
			fmt.Sprintf("bundle lock not held: %s", bundleName),
			errors.LayerTransaction).WithContext("bundle", bundleName)
	}

	delete(s.BundleLocks, lockID)
	s.Logger.Infow("Released bundle lock",
		"bundleName", bundleName,
		"lockMode", lockMode,
		"lockID", lockID)

	return nil
}

// AddTransaction adds a transaction to the session
func (s *Session) AddTransaction(txID string, cancelFunc context.CancelFunc) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.ActiveTransactions[txID] = cancelFunc
	s.Logger.Infow("Added transaction to session", "transactionID", txID)
}

// RemoveTransaction removes a transaction from the session
func (s *Session) RemoveTransaction(txID string) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if _, exists := s.ActiveTransactions[txID]; exists {
		delete(s.ActiveTransactions, txID)
		s.Logger.Infow("Removed transaction from session", "transactionID", txID)
	}
}

// AddTempFile adds a temporary file to the session
func (s *Session) AddTempFile(filePath string) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.TempFiles = append(s.TempFiles, filePath)
	s.Logger.Infow("Added temp file to session", "file", filePath)
}

// GetSessionInfo returns current session information
func (s *Session) GetSessionInfo() map[string]interface{} {
	s.mu.RLock()
	defer s.mu.RUnlock()

	// PHASE 3: Use pooled map to reduce allocations
	info := GetResponseMap()
	info["sessionID"] = s.SessionID
	info["username"] = s.Username
	info["database"] = s.DatabaseName
	info["clientIP"] = s.ClientIP
	info["userAgent"] = func() string {
		if len(s.UserAgent) > constants.UserAgentMaxLength {
			return s.UserAgent[:constants.UserAgentMaxLength] + "..."
		}
		return s.UserAgent
	}()
	info["state"] = s.State.String()
	info["createdAt"] = s.CreatedAt
	info["lastActivity"] = s.LastActivity
	info["expiresAt"] = s.ExpiresAt
	info["errorCount"] = s.ErrorCount
	info["consecutiveErrors"] = s.ConsecutiveErrors
	info["documentLocks"] = len(s.DocumentLocks)
	info["bundleLocks"] = len(s.BundleLocks)
	info["activeTransactions"] = len(s.ActiveTransactions)
	info["tempFiles"] = len(s.TempFiles)
	info["queryHistoryCount"] = len(s.QueryHistory)

	if s.CurrentQuery != nil {
		// PHASE 3: Use pooled map for nested query info
		currentQuery := GetResponseMap()
		currentQuery["queryID"] = s.CurrentQuery.QueryID
		currentQuery["query"] = s.CurrentQuery.Query
		currentQuery["startTime"] = s.CurrentQuery.StartTime
		currentQuery["status"] = s.CurrentQuery.Status
		info["currentQuery"] = currentQuery
	}

	if s.LastSuccessfulQuery != nil {
		// PHASE 3: Use pooled map for last successful query info
		lastQuery := GetResponseMap()
		lastQuery["queryID"] = s.LastSuccessfulQuery.QueryID
		lastQuery["query"] = s.LastSuccessfulQuery.Query
		lastQuery["affectedRows"] = s.LastSuccessfulQuery.AffectedRows
		lastQuery["endTime"] = s.LastSuccessfulQuery.EndTime
		info["lastSuccessfulQuery"] = lastQuery
	}

	if s.LastError != nil {
		info["lastError"] = s.LastError.Error()
	}

	return info
}

// GetRole returns the cached role for this session, fetching from PermissionService if cache is expired
// This implements the lazy role caching pattern with 5-minute TTL
func (s *Session) GetRole() (string, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	// Check if role cache is still valid
	if !s.RoleCacheTime.IsZero() && time.Since(s.RoleCacheTime) < s.RoleCacheTTL {
		// Cache is still fresh, return cached role
		return s.CachedRole, nil
	}

	// Cache expired or empty, fetch role from PermissionService
	if s.PermissionService == nil {
		// No permission service available, default to "anonymous"
		s.CachedRole = "anonymous"
		s.IsAdmin = false
		s.RoleCacheTime = time.Now()
		return s.CachedRole, nil
	}

	// Query permission service to determine if user is admin
	isAdmin, err := s.PermissionService.UserHasPermission(s.Username, "Administrator")
	if err != nil {
		// Error checking permissions, but don't fail - default to authenticated non-admin
		if s.Username != "" {
			s.CachedRole = "authenticated"
			s.IsAdmin = false
		} else {
			s.CachedRole = "anonymous"
			s.IsAdmin = false
		}
		s.RoleCacheTime = time.Now()
		return s.CachedRole, err
	}

	// Set role based on admin status
	if isAdmin {
		s.CachedRole = "admin"
		s.IsAdmin = true
	} else if s.Username != "" {
		s.CachedRole = "authenticated"
		s.IsAdmin = false
	} else {
		s.CachedRole = "anonymous"
		s.IsAdmin = false
	}

	// Update cache timestamp
	s.RoleCacheTime = time.Now()

	return s.CachedRole, nil
}

// GetIsAdmin returns the cached admin status for this session
func (s *Session) GetIsAdmin() bool {
	s.mu.RLock()
	defer s.mu.RUnlock()

	// If cache is expired, trigger a role refresh (but don't block on it)
	if s.RoleCacheTime.IsZero() || time.Since(s.RoleCacheTime) >= s.RoleCacheTTL {
		// Cache is stale, but return current value and let next GetRole() refresh it
		// This prevents blocking during IsAdmin checks
		return s.IsAdmin
	}

	return s.IsAdmin
}

// InvalidateRoleCache clears the role cache, forcing a refresh on next GetRole() call
// This is called when GRANT/REVOKE commands modify user permissions
func (s *Session) InvalidateRoleCache() {
	s.mu.Lock()
	defer s.mu.Unlock()

	// Clear cache timestamp to force refresh
	s.RoleCacheTime = time.Time{} // Zero value
	s.CachedRole = ""
	s.IsAdmin = false
}

// GetSessionsByUser returns all active sessions for a given username
// PHASE 7: Uses lock-free user session index
func (sm *SessionManager) GetSessionsByUser(username string) []*Session {
	// userSessions.Get() already returns a copy
	sessions := sm.userSessions.Get(username)
	if sessions == nil {
		return []*Session{}
	}
	return sessions
}

// Transaction Management Methods for Session

// IsInTransaction returns true if the session has an active transaction
func (s *Session) IsInTransaction() bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.TransactionActive && s.TransactionStatus == TransactionStatusActive
}

// GetActiveTransactionID returns the current transaction ID (empty string if not in transaction)
func (s *Session) GetActiveTransactionID() string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.ActiveTransactionID
}

// IsIdleExpired checks if the transaction has exceeded the idle timeout
func (s *Session) IsIdleExpired(timeout time.Duration) bool {
	s.mu.RLock()
	defer s.mu.RUnlock()

	if !s.TransactionActive {
		return false
	}

	return time.Since(s.TransactionStartTime) > timeout
}

// BeginTransaction initializes transaction state for the session.
// snapshot is the MVCC snapshot for this transaction (created on BEGIN); may be nil for non-MVCC paths.
// isolationLevel is optional; if provided, sets the transaction's isolation level.
func (s *Session) BeginTransaction(txID string, startLSN uint64, snapshot *journal.Snapshot, isolationLevel ...syndrQL.IsolationLevel) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.TransactionActive = true
	s.ActiveTransactionID = txID
	s.TransactionStartLSN = startLSN
	s.TransactionStartTime = time.Now()
	s.PendingOperations = make([]string, 0)
	s.CurrentSavepoint = nil
	s.TransactionStatus = TransactionStatusActive
	s.MVCCSnapshot = snapshot

	// Set transaction isolation level:
	// 1. Explicit parameter from BEGIN TRANSACTION ISOLATION LEVEL
	// 2. Pending level from SET TRANSACTION ISOLATION LEVEL
	// 3. Leave as IsolationDefault (will use session/server default)
	if len(isolationLevel) > 0 && isolationLevel[0] != syndrQL.IsolationDefault {
		s.TransactionIsolation = isolationLevel[0]
	} else if s.PendingIsolationLevel != syndrQL.IsolationDefault {
		s.TransactionIsolation = s.PendingIsolationLevel
	} else {
		s.TransactionIsolation = syndrQL.IsolationDefault
	}
	s.PendingIsolationLevel = syndrQL.IsolationDefault // Clear pending

	// PHASE 2: MVCC - Initialize transaction buffer for document location tracking
	s.TransactionBuffer = NewTransactionBuffer()
}

// GetMVCCSnapshot returns the MVCC snapshot for this session's active transaction, or nil.
func (s *Session) GetMVCCSnapshot() *journal.Snapshot {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.MVCCSnapshot
}

// CommitTransaction marks the transaction as committed and clears state
func (s *Session) CommitTransaction() {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.TransactionStatus = TransactionStatusCommitted
	// Close all cursors on transaction end (PostgreSQL semantics)
	if s.Cursors != nil {
		s.Cursors.CloseAll()
	}
	s.clearTransactionState()
}

// AbortTransaction marks the transaction as aborted and clears state
func (s *Session) AbortTransaction() {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.TransactionStatus = TransactionStatusAborted
	// Close all cursors on transaction end (PostgreSQL semantics)
	if s.Cursors != nil {
		s.Cursors.CloseAll()
	}
	s.clearTransactionState()
}

// clearTransactionState resets all transaction-related fields (must hold mu.Lock)
func (s *Session) clearTransactionState() {
	s.TransactionActive = false
	s.ActiveTransactionID = ""
	s.TransactionStartLSN = 0
	s.TransactionStartTime = time.Time{}
	s.PendingOperations = nil
	s.CurrentSavepoint = nil
	s.MVCCSnapshot = nil
	s.TransactionIsolation = syndrQL.IsolationDefault
	// PHASE 2: MVCC - Clear transaction buffer
	if s.TransactionBuffer != nil {
		s.TransactionBuffer.Clear()
		s.TransactionBuffer = nil
	}
}

// GetEffectiveIsolationLevel returns the effective isolation level for the current transaction.
// Priority: TransactionIsolation > DefaultIsolationLevel > server default (REPEATABLE READ).
// READ UNCOMMITTED is mapped to READ COMMITTED (like PostgreSQL).
func (s *Session) GetEffectiveIsolationLevel() syndrQL.IsolationLevel {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.getEffectiveIsolationLevelLocked()
}

// getEffectiveIsolationLevelLocked is the internal version that requires mu to be held.
func (s *Session) getEffectiveIsolationLevelLocked() syndrQL.IsolationLevel {
	level := s.TransactionIsolation
	if level == syndrQL.IsolationDefault {
		level = s.DefaultIsolationLevel
	}
	if level == syndrQL.IsolationDefault {
		level = syndrQL.IsolationRepeatableRead // Server default
	}
	// Map READ UNCOMMITTED → READ COMMITTED (like PostgreSQL)
	if level == syndrQL.IsolationReadUncommitted {
		level = syndrQL.IsolationReadCommitted
	}
	return level
}

// IsReadCommitted returns true if the effective isolation level is READ COMMITTED.
func (s *Session) IsReadCommitted() bool {
	return s.GetEffectiveIsolationLevel() == syndrQL.IsolationReadCommitted
}

// SetTransactionIsolation sets the isolation level for the next transaction (via SET TRANSACTION).
// This is a pending level that takes effect on the next BEGIN.
func (s *Session) SetTransactionIsolation(level syndrQL.IsolationLevel) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.PendingIsolationLevel = level
}

// SetDefaultIsolationLevel sets the session-level default isolation level.
func (s *Session) SetDefaultIsolationLevel(level syndrQL.IsolationLevel) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.DefaultIsolationLevel = level
}

// AddPendingOperation adds a command to the transaction's pending operations buffer
func (s *Session) AddPendingOperation(command string) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.PendingOperations == nil {
		s.PendingOperations = make([]string, 0)
	}
	s.PendingOperations = append(s.PendingOperations, command)
}

// SetSavepoint sets a savepoint for the transaction (replaces any existing savepoint)
func (s *Session) SetSavepoint(name string, lsn uint64) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.CurrentSavepoint = &Savepoint{
		Name: name,
		LSN:  lsn,
	}
}

// GetSavepoint returns the current savepoint, or nil if none exists
func (s *Session) GetSavepoint() *Savepoint {
	s.mu.RLock()
	defer s.mu.RUnlock()

	return s.CurrentSavepoint
}

// ClearSavepoint removes the current savepoint
func (s *Session) ClearSavepoint() {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.CurrentSavepoint = nil
}

// GetTransactionInfo returns current transaction information for debugging/monitoring
func (s *Session) GetTransactionInfo() map[string]interface{} {
	s.mu.RLock()
	defer s.mu.RUnlock()

	info := map[string]interface{}{
		"active":            s.TransactionActive,
		"status":            s.TransactionStatus.String(),
		"transaction_id":    s.ActiveTransactionID,
		"start_lsn":         s.TransactionStartLSN,
		"start_time":        s.TransactionStartTime,
		"pending_ops_count": len(s.PendingOperations),
	}

	if s.CurrentSavepoint != nil {
		info["savepoint_name"] = s.CurrentSavepoint.Name
		info["savepoint_lsn"] = s.CurrentSavepoint.LSN
	}

	return info
}
