/*
semi_join_nodes.go

This file implements semi-join and anti-join execution nodes for correlated
subquery optimization.

DESIGN PRINCIPLES:
- Hash-Based Execution: O(N+M) complexity using hash tables
- NULL-Aware Semantics: Proper SQL three-valued logic for anti-joins
- Deduplication: Semi-joins return unique outer rows (no duplicates)
- Memory Efficient: Build hash table from smaller side when possible

EXECUTION STRATEGY:
Semi-Join (EXISTS, IN):
1. Build hash table from inner (right) side
2. Probe with outer (left) side
3. Return outer row on first match (deduplication)
4. Continue to next outer row

Anti-Join (NOT EXISTS, NOT IN):
5. Build hash table from inner side
6. Track NULL presence in inner side
7. Probe with outer side
8. Return outer row if NO match found
9. Handle NULL semantics: NOT IN returns empty if inner has NULL

This implements PostgreSQL's hash semi-join with SyndrDB adaptations
for document-based storage.
*/

package planner

import (
	"fmt"
	"sync"
	"syndrdb/src/internal/domain/models"

	"go.uber.org/zap"
)

// SemiJoinType defines the type of semi-join operation
type SemiJoinType int

const (
	SEMI_JOIN SemiJoinType = iota // EXISTS, IN
	ANTI_JOIN                     // NOT EXISTS, NOT IN
)

// SemiJoinNode represents a semi-join execution node
type SemiJoinNode struct {
	joinType       SemiJoinType
	outerDocuments []*models.Document
	innerDocuments []*models.Document
	joinFields     []string // Fields to join on
	hashNullAware  bool     // Use NULL-aware semantics for NOT IN
	logger         *zap.SugaredLogger
}

// NewSemiJoinNode creates a new semi-join execution node
func NewSemiJoinNode(
	joinType SemiJoinType,
	outerDocs []*models.Document,
	innerDocs []*models.Document,
	joinFields []string,
	logger *zap.SugaredLogger,
) *SemiJoinNode {
	return &SemiJoinNode{
		joinType:       joinType,
		outerDocuments: outerDocs,
		innerDocuments: innerDocs,
		joinFields:     joinFields,
		hashNullAware:  joinType == ANTI_JOIN, // NULL-aware only for anti-joins
		logger:         logger,
	}
}

// Execute performs the semi-join operation
func (sjn *SemiJoinNode) Execute() ([]*models.Document, error) {
	if len(sjn.joinFields) == 0 {
		return nil, fmt.Errorf("semi-join requires at least one join field")
	}

	sjn.logger.Debugf("Executing %s on %d outer x %d inner documents (fields: %v)",
		joinTypeName(sjn.joinType), len(sjn.outerDocuments), len(sjn.innerDocuments), sjn.joinFields)

	// Build hash table from inner side
	hashTable, hasNull := sjn.buildHashTable()

	// Probe with outer side
	result := make([]*models.Document, 0, len(sjn.outerDocuments)/2) // Estimate 50% selectivity

	for _, outerDoc := range sjn.outerDocuments {
		key := sjn.buildJoinKey(outerDoc)

		// Handle NULL in outer row
		if key == nil {
			// NULL key handling depends on join type
			if sjn.joinType == ANTI_JOIN {
				// NULL on outer side of anti-join never matches (SQL semantics)
				// NOT IN with NULL on left side returns NULL (three-valued logic)
				// We treat NULL as no match -> include in result
				result = append(result, outerDoc)
			}
			// For semi-join, NULL key never matches, skip
			continue
		}

		matched := hashTable.Probe(key)

		switch sjn.joinType {
		case SEMI_JOIN:
			if matched {
				result = append(result, outerDoc)
			}

		case ANTI_JOIN:
			// NOT IN semantics: if inner has NULL, return empty result
			if sjn.hashNullAware && hasNull {
				sjn.logger.Debug("Anti-join detected NULL in inner side, returning empty result (SQL NOT IN semantics)")
				return []*models.Document{}, nil
			}

			if !matched {
				result = append(result, outerDoc)
			}
		}
	}

	sjn.logger.Debugf("%s completed: %d results from %d outer documents",
		joinTypeName(sjn.joinType), len(result), len(sjn.outerDocuments))

	return result, nil
}

// buildHashTable constructs a hash table from inner documents
func (sjn *SemiJoinNode) buildHashTable() (*HashTable, bool) {
	hashTable := NewHashTable()
	hasNull := false

	for _, innerDoc := range sjn.innerDocuments {
		key := sjn.buildJoinKey(innerDoc)

		if key == nil {
			hasNull = true
			// Still insert NULL keys for probing in semi-join
			// (though they'll never match NULL from outer)
		}

		hashTable.Insert(key)
	}

	sjn.logger.Debugf("Built hash table with %d entries (hasNull: %v)", hashTable.Size(), hasNull)
	return hashTable, hasNull
}

func (sjn *SemiJoinNode) buildJoinKey(doc *models.Document) interface{} {
	getVal := func(name string) (interface{}, bool) {
		if fv, ok := doc.GetFieldValue(nil, name); ok && !fv.IsNil() {
			return fv.AsInterface(), true
		}
		if doc.Data != nil {
			v, ok := doc.Data[name]
			return v, ok && v != nil
		}
		return nil, false
	}
	if len(sjn.joinFields) == 1 {
		val, ok := getVal(sjn.joinFields[0])
		if !ok {
			return nil
		}
		return val
	}
	keyParts := make([]interface{}, len(sjn.joinFields))
	for i, fieldName := range sjn.joinFields {
		val, ok := getVal(fieldName)
		if !ok {
			return nil
		}
		keyParts[i] = val
	}
	return &CompositeKey{Parts: keyParts}
}

// HashTable implements a simple hash table for semi-join probing
type HashTable struct {
	entries map[interface{}]bool
	mu      sync.RWMutex
}

// NewHashTable creates a new hash table
func NewHashTable() *HashTable {
	return &HashTable{
		entries: make(map[interface{}]bool),
	}
}

// Insert adds a key to the hash table
func (ht *HashTable) Insert(key interface{}) {
	ht.mu.Lock()
	defer ht.mu.Unlock()

	if key == nil {
		// Store NULL as special marker
		ht.entries[nil] = true
		return
	}

	ht.entries[hashKey(key)] = true
}

// Probe checks if a key exists in the hash table
func (ht *HashTable) Probe(key interface{}) bool {
	ht.mu.RLock()
	defer ht.mu.RUnlock()

	if key == nil {
		return false // NULL never matches (SQL semantics)
	}

	return ht.entries[hashKey(key)]
}

// Size returns the number of entries
func (ht *HashTable) Size() int {
	ht.mu.RLock()
	defer ht.mu.RUnlock()
	return len(ht.entries)
}

// CompositeKey represents a multi-field join key
type CompositeKey struct {
	Parts []interface{}
}

// hashKey converts a key to a hashable form
func hashKey(key interface{}) interface{} {
	switch k := key.(type) {
	case *CompositeKey:
		// For composite keys, create a string representation
		// TODO: I will implement more efficient composite key hashing using hash functions
		// when profiling shows this is a performance bottleneck
		return fmt.Sprintf("%v", k.Parts)
	default:
		return key
	}
}

// Helper functions

func joinTypeName(joinType SemiJoinType) string {
	switch joinType {
	case SEMI_JOIN:
		return "semi-join"
	case ANTI_JOIN:
		return "anti-join"
	default:
		return "unknown-join"
	}
}

// Estimators for query planning

// EstimateSemiJoinCardinality estimates the output cardinality of a semi-join
func EstimateSemiJoinCardinality(outerSize, innerSize int64, selectivity float64) int64 {
	// Semi-join: each outer row appears at most once
	// Estimated output = outer_size * probability_of_match
	estimated := int64(float64(outerSize) * selectivity)
	if estimated > outerSize {
		estimated = outerSize // Cannot exceed outer size
	}
	return estimated
}

// EstimateAntiJoinCardinality estimates the output cardinality of an anti-join
func EstimateAntiJoinCardinality(outerSize, innerSize int64, selectivity float64) int64 {
	// Anti-join: inverse of semi-join
	// Estimated output = outer_size * (1 - probability_of_match)
	estimated := int64(float64(outerSize) * (1.0 - selectivity))
	if estimated < 0 {
		estimated = 0
	}
	return estimated
}

// EstimateSemiJoinCost estimates the execution cost of a semi-join
func EstimateSemiJoinCost(outerSize, innerSize int64) float64 {
	// Cost model: O(N + M) for hash-based semi-join
	// Build hash table: M * BUILD_COST
	// Probe: N * PROBE_COST
	const BUILD_COST = 1.2 // Building hash table slightly more expensive
	const PROBE_COST = 1.0

	buildCost := float64(innerSize) * BUILD_COST
	probeCost := float64(outerSize) * PROBE_COST

	return buildCost + probeCost
}
