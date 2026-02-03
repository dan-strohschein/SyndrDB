package server

/*
SHARDED SESSION MANAGER

This file implements 64-shard session storage for reduced lock contention.
Phase 7 of the Lock Contention Fix plan.

DESIGN:
- 64 shards for session storage (shard by sessionID using xxhash)
- sync.Map for secondary indexes (connectionSessions, userSessions)
- Each shard has its own RWMutex for fine-grained locking
- Lookup by sessionID only touches 1 of 64 shards

PERFORMANCE:
- ~64x reduction in contention for session lookups
- Lock-free reads for connection and user session lookups via sync.Map
- Supports 150+ concurrent connections with minimal contention

Thread Safety:
- Session lookup by ID: RLock on single shard
- Session creation: Lock on single shard + sync.Map stores
- Session deletion: Lock on single shard + sync.Map deletes
*/

import (
	"sync"

	"github.com/cespare/xxhash/v2"
)

const (
	// SessionShardCount is the number of shards for session storage
	// 64 provides good distribution and minimal false sharing on typical cache lines
	SessionShardCount = 64
)

// sessionShard represents a single shard of the session map
type sessionShard struct {
	mu       sync.RWMutex
	sessions map[string]*Session
}

// ShardedSessionMap provides sharded session storage for reduced lock contention
type ShardedSessionMap struct {
	shards [SessionShardCount]sessionShard
}

// NewShardedSessionMap creates a new sharded session map
func NewShardedSessionMap() *ShardedSessionMap {
	ssm := &ShardedSessionMap{}
	for i := 0; i < SessionShardCount; i++ {
		ssm.shards[i].sessions = make(map[string]*Session)
	}
	return ssm
}

// shardIndex returns the shard index for a given session ID
func (ssm *ShardedSessionMap) shardIndex(sessionID string) uint64 {
	return xxhash.Sum64String(sessionID) & (SessionShardCount - 1)
}

// Get retrieves a session by ID
func (ssm *ShardedSessionMap) Get(sessionID string) (*Session, bool) {
	idx := ssm.shardIndex(sessionID)
	shard := &ssm.shards[idx]

	shard.mu.RLock()
	session, exists := shard.sessions[sessionID]
	shard.mu.RUnlock()

	return session, exists
}

// Set stores a session by ID
func (ssm *ShardedSessionMap) Set(sessionID string, session *Session) {
	idx := ssm.shardIndex(sessionID)
	shard := &ssm.shards[idx]

	shard.mu.Lock()
	shard.sessions[sessionID] = session
	shard.mu.Unlock()
}

// Delete removes a session by ID
// Returns the deleted session and whether it existed
func (ssm *ShardedSessionMap) Delete(sessionID string) (*Session, bool) {
	idx := ssm.shardIndex(sessionID)
	shard := &ssm.shards[idx]

	shard.mu.Lock()
	session, exists := shard.sessions[sessionID]
	if exists {
		delete(shard.sessions, sessionID)
	}
	shard.mu.Unlock()

	return session, exists
}

// Len returns the total number of sessions across all shards
func (ssm *ShardedSessionMap) Len() int {
	total := 0
	for i := 0; i < SessionShardCount; i++ {
		ssm.shards[i].mu.RLock()
		total += len(ssm.shards[i].sessions)
		ssm.shards[i].mu.RUnlock()
	}
	return total
}

// Range iterates over all sessions with a callback
// The callback should not modify the session map
// Returns early if callback returns false
func (ssm *ShardedSessionMap) Range(fn func(sessionID string, session *Session) bool) {
	for i := 0; i < SessionShardCount; i++ {
		shard := &ssm.shards[i]
		shard.mu.RLock()
		for sessionID, session := range shard.sessions {
			if !fn(sessionID, session) {
				shard.mu.RUnlock()
				return
			}
		}
		shard.mu.RUnlock()
	}
}

// RangeWithLock iterates over all sessions, holding the lock for each shard
// Use this when the callback needs to modify session state
// The callback receives a cleanup function to call after modifications
func (ssm *ShardedSessionMap) RangeWithLock(fn func(sessionID string, session *Session) bool) {
	for i := 0; i < SessionShardCount; i++ {
		shard := &ssm.shards[i]
		shard.mu.Lock()
		for sessionID, session := range shard.sessions {
			if !fn(sessionID, session) {
				shard.mu.Unlock()
				return
			}
		}
		shard.mu.Unlock()
	}
}

// CollectExpired returns sessions that match the predicate
// This is used for cleanup of expired sessions
func (ssm *ShardedSessionMap) CollectExpired(predicate func(*Session) bool) []*Session {
	var expired []*Session

	for i := 0; i < SessionShardCount; i++ {
		shard := &ssm.shards[i]
		shard.mu.RLock()
		for _, session := range shard.sessions {
			if predicate(session) {
				expired = append(expired, session)
			}
		}
		shard.mu.RUnlock()
	}

	return expired
}

// DeleteMultiple removes multiple sessions atomically within each shard
// This is optimized for batch cleanup operations
func (ssm *ShardedSessionMap) DeleteMultiple(sessionIDs []string) {
	// Group by shard for efficiency
	shardGroups := make(map[uint64][]string)
	for _, id := range sessionIDs {
		idx := ssm.shardIndex(id)
		shardGroups[idx] = append(shardGroups[idx], id)
	}

	// Delete from each shard
	for idx, ids := range shardGroups {
		shard := &ssm.shards[idx]
		shard.mu.Lock()
		for _, id := range ids {
			delete(shard.sessions, id)
		}
		shard.mu.Unlock()
	}
}

// GetStats returns statistics about the sharded map
func (ssm *ShardedSessionMap) GetStats() map[string]interface{} {
	totalSessions := 0
	minShard := 0
	maxShard := 0
	shardCounts := make([]int, SessionShardCount)

	for i := 0; i < SessionShardCount; i++ {
		ssm.shards[i].mu.RLock()
		count := len(ssm.shards[i].sessions)
		ssm.shards[i].mu.RUnlock()

		shardCounts[i] = count
		totalSessions += count
		if count > shardCounts[maxShard] {
			maxShard = i
		}
		if count < shardCounts[minShard] {
			minShard = i
		}
	}

	return map[string]interface{}{
		"total_sessions": totalSessions,
		"shard_count":    SessionShardCount,
		"min_shard_size": shardCounts[minShard],
		"max_shard_size": shardCounts[maxShard],
		"avg_shard_size": float64(totalSessions) / float64(SessionShardCount),
	}
}

// Compact recreates the underlying maps for all shards with only current entries.
// PERFORMANCE FIX: Go's map delete() doesn't shrink the bucket array. After many
// connect/disconnect cycles, the shard maps accumulate empty bucket slots that
// degrade iteration and memory performance. This method recreates each shard's
// map with only current sessions, reclaiming memory and restoring lookup speed.
// Returns the total number of sessions after compaction.
func (ssm *ShardedSessionMap) Compact() int {
	totalSessions := 0

	for i := 0; i < SessionShardCount; i++ {
		shard := &ssm.shards[i]

		shard.mu.Lock()
		// Create new map sized exactly for current sessions
		newMap := make(map[string]*Session, len(shard.sessions))
		for sessionID, session := range shard.sessions {
			newMap[sessionID] = session
		}
		shard.sessions = newMap
		totalSessions += len(newMap)
		shard.mu.Unlock()
	}

	return totalSessions
}

// UserSessionIndex provides lock-free access to user -> sessions mapping
// Uses sync.Map for concurrent read/write access
type UserSessionIndex struct {
	// username -> *userSessionEntry
	index sync.Map
}

// userSessionEntry holds sessions for a user with its own mutex
type userSessionEntry struct {
	mu       sync.RWMutex
	sessions []*Session
}

// NewUserSessionIndex creates a new user session index
func NewUserSessionIndex() *UserSessionIndex {
	return &UserSessionIndex{}
}

// Add adds a session to the user's session list
func (usi *UserSessionIndex) Add(username string, session *Session) {
	// Get or create entry
	entryI, _ := usi.index.LoadOrStore(username, &userSessionEntry{
		sessions: make([]*Session, 0, 4),
	})
	entry := entryI.(*userSessionEntry)

	entry.mu.Lock()
	entry.sessions = append(entry.sessions, session)
	entry.mu.Unlock()
}

// Remove removes a session from the user's session list
func (usi *UserSessionIndex) Remove(username string, sessionID string) {
	entryI, exists := usi.index.Load(username)
	if !exists {
		return
	}
	entry := entryI.(*userSessionEntry)

	entry.mu.Lock()
	for i, s := range entry.sessions {
		if s.SessionID == sessionID {
			// Remove by swapping with last element
			entry.sessions[i] = entry.sessions[len(entry.sessions)-1]
			entry.sessions = entry.sessions[:len(entry.sessions)-1]
			break
		}
	}
	// If empty, could delete from index, but leave for simplicity
	entry.mu.Unlock()
}

// Get returns all sessions for a user (copy of slice)
func (usi *UserSessionIndex) Get(username string) []*Session {
	entryI, exists := usi.index.Load(username)
	if !exists {
		return nil
	}
	entry := entryI.(*userSessionEntry)

	entry.mu.RLock()
	result := make([]*Session, len(entry.sessions))
	copy(result, entry.sessions)
	entry.mu.RUnlock()

	return result
}

// Range iterates over all users and their sessions
func (usi *UserSessionIndex) Range(fn func(username string, sessions []*Session) bool) {
	usi.index.Range(func(key, value interface{}) bool {
		username := key.(string)
		entry := value.(*userSessionEntry)

		entry.mu.RLock()
		sessions := make([]*Session, len(entry.sessions))
		copy(sessions, entry.sessions)
		entry.mu.RUnlock()

		return fn(username, sessions)
	})
}

// Compact recreates the sync.Map with only current entries (users with active sessions).
// PERFORMANCE FIX: Go's sync.Map.Delete() doesn't free memory - it marks entries as
// "expunged" but they remain in internal structures. After many user disconnects, the
// sync.Map accumulates cruft that degrades Load() performance.
// Also removes entries with empty session lists to save memory.
// Returns the number of entries in the compacted map.
func (usi *UserSessionIndex) Compact() int {
	var newMap sync.Map
	count := 0

	usi.index.Range(func(key, value interface{}) bool {
		entry := value.(*userSessionEntry)

		entry.mu.RLock()
		hasActiveSessions := len(entry.sessions) > 0
		entry.mu.RUnlock()

		// Only keep entries with active sessions
		if hasActiveSessions {
			newMap.Store(key, value)
			count++
		}
		return true
	})

	usi.index = newMap
	return count
}

// ConnectionSessionIndex provides lock-free access to connectionID -> session mapping
type ConnectionSessionIndex struct {
	// connectionID -> *Session
	index sync.Map
}

// NewConnectionSessionIndex creates a new connection session index
func NewConnectionSessionIndex() *ConnectionSessionIndex {
	return &ConnectionSessionIndex{}
}

// Set associates a connection with a session
func (csi *ConnectionSessionIndex) Set(connectionID string, session *Session) {
	csi.index.Store(connectionID, session)
}

// Get retrieves a session by connection ID
func (csi *ConnectionSessionIndex) Get(connectionID string) (*Session, bool) {
	sessionI, exists := csi.index.Load(connectionID)
	if !exists {
		return nil, false
	}
	return sessionI.(*Session), true
}

// Delete removes a connection mapping
func (csi *ConnectionSessionIndex) Delete(connectionID string) {
	csi.index.Delete(connectionID)
}

// Range iterates over all connections
func (csi *ConnectionSessionIndex) Range(fn func(connectionID string, session *Session) bool) {
	csi.index.Range(func(key, value interface{}) bool {
		return fn(key.(string), value.(*Session))
	})
}

// Compact recreates the sync.Map with only current entries.
// PERFORMANCE FIX: Go's sync.Map.Delete() doesn't free memory - it marks entries as
// "expunged" but they remain in internal structures. After many disconnects, the
// sync.Map accumulates cruft that degrades Load() performance.
// Returns the number of entries in the compacted map.
func (csi *ConnectionSessionIndex) Compact() int {
	var newMap sync.Map
	count := 0

	csi.index.Range(func(key, value interface{}) bool {
		newMap.Store(key, value)
		count++
		return true
	})

	csi.index = newMap
	return count
}
