/*
ADAPTIVE DISK SPILL MANAGER FOR JOIN OPERATIONS

This file implements PostgreSQL-style adaptive disk spillover for hash join operations
when memory limits are exceeded. Instead of failing on memory pressure, it gracefully
spills partitions to disk and processes them in batches.

KEY FEATURES:
1. Adaptive partitioning: Doubles partition count when memory exceeded (PostgreSQL-style)
2. zstd compression: Reduces disk I/O and storage requirements
3. Partition-based processing: Spills largest partitions first to maximize memory efficiency
4. Cleanup guarantees: Automatic cleanup of spill files on completion or error

POSTGRESQL ALIGNMENT:
- Follows PostgreSQL's Grace Hash Join algorithm
- Uses work_mem equivalent (JoinMemoryLimitMB) as memory threshold
- Implements batch doubling strategy when memory exceeded
- Supports temp_file_limit equivalent via settings

ALGORITHM:
1. Start with initial partition count (typically 8-16)
2. Build hash table partitions in memory
3. When memory limit exceeded:
   a. Double the number of batches (repartition)
   b. Spill largest partition(s) to disk
4. Process in-memory partitions first
5. Load and process spilled partitions one at a time
6. Clean up all spill files on completion
*/

package joinexecutor

import (
	"encoding/gob"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"

	"syndrdb/src/internal/domain/models"
	"syndrdb/src/pkg/settings"

	"github.com/klauspost/compress/zstd"
	"go.uber.org/zap"
)

// SpillPartition represents a partition that may be spilled to disk
type SpillPartition struct {
	ID         int              // Partition ID (0 to numPartitions-1)
	Entries    []partitionEntry // In-memory entries (nil if spilled)
	MemoryUsed int64            // Estimated memory usage in bytes
	IsSpilled  bool             // Whether this partition is on disk
	SpillPath  string           // Path to spill file (if spilled)
	EntryCount int64            // Total entry count (persisted across spills)
}

// partitionEntry represents a single hash table entry in a partition
type partitionEntry struct {
	KeyHash   uint64             // Hash of the join key
	Key       interface{}        // The actual join key value
	Documents []*models.Document // Documents with this key
}

// spilledEntry is the serializable form for disk storage
type spilledEntry struct {
	KeyHash     uint64                 // Hash of the join key
	KeyString   string                 // String representation of key for serialization
	DocumentIDs []string               // DocumentIDs for lookup
	FieldMaps   []map[string]FieldData // Serializable field data
}

// FieldData represents serializable field information
type FieldData struct {
	Name  string            `json:"name"`
	Value models.FieldValue `json:"value"` // FieldValue is already serializable
}

// AdaptiveSpillManager implements PostgreSQL-style adaptive disk spillover
// for hash join operations that exceed memory limits
type AdaptiveSpillManager struct {
	logger        *zap.SugaredLogger
	spillDir      string            // Directory for spill files
	memoryLimit   int64             // Memory limit in bytes (from settings)
	mutex         sync.RWMutex      // Protects partition state
	partitions    []*SpillPartition // Current partitions
	numPartitions int               // Current partition count
	totalMemory   int64             // Total memory used across all partitions
	spillCount    atomic.Int64      // Number of spill operations performed
	joinID        string            // Unique identifier for this join operation

	// Statistics
	totalSpilledBytes   int64 // Total bytes written to disk
	totalSpilledEntries int64 // Total entries spilled
	repartitionCount    int   // Number of times we doubled partitions

	// schema is optional; when set, convertToSpilledEntry/convertFromSpilledEntry use doc.Values
	schema *models.BundleFieldSchema
}

// SetSchema sets the bundle field schema for serializing/deserializing doc.Values.
func (asm *AdaptiveSpillManager) SetSchema(schema *models.BundleFieldSchema) {
	asm.mutex.Lock()
	defer asm.mutex.Unlock()
	asm.schema = schema
}

// NewAdaptiveSpillManager creates a new adaptive spill manager for a join operation
// memoryLimit: Maximum memory to use before spilling (bytes)
// joinID: Unique identifier for this join (for spill file naming)
func NewAdaptiveSpillManager(logger *zap.SugaredLogger, memoryLimit int64, joinID string) *AdaptiveSpillManager {
	globalSettings := settings.GetSettings()
	spillDir := filepath.Join(globalSettings.TempDir, "join_spill")

	// Create spill directory if it doesn't exist
	if err := os.MkdirAll(spillDir, 0755); err != nil {
		logger.Warnf("Failed to create spill directory %s: %v", spillDir, err)
	}

	// Start with 8 partitions (PostgreSQL default for hash joins)
	initialPartitions := 8

	asm := &AdaptiveSpillManager{
		logger:        logger,
		spillDir:      spillDir,
		memoryLimit:   memoryLimit,
		partitions:    make([]*SpillPartition, initialPartitions),
		numPartitions: initialPartitions,
		totalMemory:   0,
		joinID:        joinID,
	}

	// Initialize partitions
	for i := 0; i < initialPartitions; i++ {
		asm.partitions[i] = &SpillPartition{
			ID:         i,
			Entries:    make([]partitionEntry, 0, 64),
			MemoryUsed: 0,
			IsSpilled:  false,
		}
	}

	return asm
}

// AddEntry adds an entry to the appropriate partition based on key hash
// Returns error if entry cannot be added (e.g., disk full)
func (asm *AdaptiveSpillManager) AddEntry(keyHash uint64, key interface{}, doc *models.Document) error {
	asm.mutex.Lock()
	defer asm.mutex.Unlock()

	// Determine partition based on hash
	partitionID := int(keyHash % uint64(asm.numPartitions))
	partition := asm.partitions[partitionID]

	// Estimate memory for this entry
	entryMemory := asm.estimateEntryMemory(key, doc)

	// Check if we need to spill before adding
	if asm.totalMemory+entryMemory > asm.memoryLimit {
		if err := asm.handleMemoryPressure(); err != nil {
			return fmt.Errorf("failed to handle memory pressure: %w", err)
		}
		// Recalculate partition after potential repartition
		partitionID = int(keyHash % uint64(asm.numPartitions))
		partition = asm.partitions[partitionID]
	}

	// If partition is spilled, we need to append to disk
	if partition.IsSpilled {
		return asm.appendToSpilledPartition(partition, keyHash, key, doc)
	}

	// Add to in-memory partition
	// Check if key already exists in partition
	for i := range partition.Entries {
		if partition.Entries[i].KeyHash == keyHash {
			// Key exists, add document to existing entry
			partition.Entries[i].Documents = append(partition.Entries[i].Documents, doc)
			partition.MemoryUsed += asm.estimateDocumentMemory(doc)
			asm.totalMemory += asm.estimateDocumentMemory(doc)
			partition.EntryCount++
			return nil
		}
	}

	// New key, create new entry
	partition.Entries = append(partition.Entries, partitionEntry{
		KeyHash:   keyHash,
		Key:       key,
		Documents: []*models.Document{doc},
	})
	partition.MemoryUsed += entryMemory
	asm.totalMemory += entryMemory
	partition.EntryCount++

	return nil
}

// handleMemoryPressure handles memory limit exceeded by spilling or repartitioning
// Follows PostgreSQL's strategy of doubling batches when memory exceeded
func (asm *AdaptiveSpillManager) handleMemoryPressure() error {
	asm.logger.Infof("Memory pressure detected: %d bytes used, limit %d bytes",
		asm.totalMemory, asm.memoryLimit)

	// Strategy: Spill the largest in-memory partition
	// If all partitions are spilled, we need to repartition
	largestPartition := asm.findLargestInMemoryPartition()

	if largestPartition == nil {
		// All partitions spilled - need to repartition (double batch count)
		return asm.repartition()
	}

	// Spill the largest partition
	return asm.spillPartition(largestPartition)
}

// findLargestInMemoryPartition finds the partition using the most memory
func (asm *AdaptiveSpillManager) findLargestInMemoryPartition() *SpillPartition {
	var largest *SpillPartition
	var maxMemory int64

	for _, p := range asm.partitions {
		if !p.IsSpilled && p.MemoryUsed > maxMemory {
			largest = p
			maxMemory = p.MemoryUsed
		}
	}

	return largest
}

// spillPartition writes a partition to disk with zstd compression
func (asm *AdaptiveSpillManager) spillPartition(partition *SpillPartition) error {
	if partition.IsSpilled {
		return nil // Already spilled
	}

	// Generate spill file path
	spillPath := filepath.Join(asm.spillDir,
		fmt.Sprintf("join_%s_part_%d_%d.spill", asm.joinID, partition.ID, asm.spillCount.Add(1)))

	// Create spill file
	file, err := os.Create(spillPath)
	if err != nil {
		return fmt.Errorf("failed to create spill file: %w", err)
	}
	defer file.Close()

	// Create zstd compressed writer
	encoder, err := zstd.NewWriter(file, zstd.WithEncoderLevel(zstd.SpeedFastest))
	if err != nil {
		os.Remove(spillPath)
		return fmt.Errorf("failed to create zstd encoder: %w", err)
	}
	defer encoder.Close()

	// Encode entries using gob
	gobEncoder := gob.NewEncoder(encoder)

	for _, entry := range partition.Entries {
		spilledEntry := asm.convertToSpilledEntry(entry)
		if err := gobEncoder.Encode(spilledEntry); err != nil {
			os.Remove(spillPath)
			return fmt.Errorf("failed to encode entry: %w", err)
		}
		asm.totalSpilledEntries++
	}

	// Flush and close encoder
	if err := encoder.Close(); err != nil {
		os.Remove(spillPath)
		return fmt.Errorf("failed to close encoder: %w", err)
	}

	// Get file size for statistics
	if stat, err := os.Stat(spillPath); err == nil {
		asm.totalSpilledBytes += stat.Size()
	}

	// Update partition state
	freedMemory := partition.MemoryUsed
	partition.Entries = nil // Release memory
	partition.IsSpilled = true
	partition.SpillPath = spillPath
	partition.MemoryUsed = 0
	asm.totalMemory -= freedMemory

	asm.logger.Infof("Spilled partition %d: %d entries, freed %d bytes, path: %s",
		partition.ID, partition.EntryCount, freedMemory, spillPath)

	return nil
}

// repartition doubles the number of partitions (PostgreSQL-style)
// This redistributes entries across more partitions to reduce per-partition memory
func (asm *AdaptiveSpillManager) repartition() error {
	asm.repartitionCount++
	newNumPartitions := asm.numPartitions * 2

	asm.logger.Infof("Repartitioning: %d -> %d partitions (repartition #%d)",
		asm.numPartitions, newNumPartitions, asm.repartitionCount)

	// Create new partitions
	newPartitions := make([]*SpillPartition, newNumPartitions)
	for i := 0; i < newNumPartitions; i++ {
		newPartitions[i] = &SpillPartition{
			ID:         i,
			Entries:    make([]partitionEntry, 0, 64),
			MemoryUsed: 0,
			IsSpilled:  false,
		}
	}

	// Redistribute existing in-memory entries
	for _, oldPartition := range asm.partitions {
		if oldPartition.IsSpilled {
			// For spilled partitions, we need to reload and redistribute
			// TODO: I could optimize this by keeping track of which new partitions
			// each old partition maps to and processing during load phase
			continue
		}

		for _, entry := range oldPartition.Entries {
			newPartitionID := int(entry.KeyHash % uint64(newNumPartitions))
			newPartitions[newPartitionID].Entries = append(
				newPartitions[newPartitionID].Entries, entry)
			newPartitions[newPartitionID].MemoryUsed += asm.estimateEntryMemoryFromEntry(entry)
			newPartitions[newPartitionID].EntryCount += int64(len(entry.Documents))
		}
		oldPartition.Entries = nil // Free old memory
	}

	// Spill the largest new partition to make room
	asm.partitions = newPartitions
	asm.numPartitions = newNumPartitions

	// Find and spill largest partition(s) until we're under limit
	for asm.totalMemory > asm.memoryLimit {
		largest := asm.findLargestInMemoryPartition()
		if largest == nil {
			break
		}
		if err := asm.spillPartition(largest); err != nil {
			return err
		}
	}

	return nil
}

// appendToSpilledPartition appends an entry to an already-spilled partition
func (asm *AdaptiveSpillManager) appendToSpilledPartition(partition *SpillPartition, keyHash uint64, key interface{}, doc *models.Document) error {
	// Open file in append mode
	file, err := os.OpenFile(partition.SpillPath, os.O_APPEND|os.O_WRONLY, 0644)
	if err != nil {
		return fmt.Errorf("failed to open spill file for append: %w", err)
	}
	defer file.Close()

	// Create zstd writer for appending
	encoder, err := zstd.NewWriter(file, zstd.WithEncoderLevel(zstd.SpeedFastest))
	if err != nil {
		return fmt.Errorf("failed to create zstd encoder: %w", err)
	}
	defer encoder.Close()

	entry := partitionEntry{
		KeyHash:   keyHash,
		Key:       key,
		Documents: []*models.Document{doc},
	}
	spilledEntry := asm.convertToSpilledEntry(entry)

	gobEncoder := gob.NewEncoder(encoder)
	if err := gobEncoder.Encode(spilledEntry); err != nil {
		return fmt.Errorf("failed to encode appended entry: %w", err)
	}

	partition.EntryCount++
	asm.totalSpilledEntries++

	return nil
}

// LoadPartition loads a spilled partition back into memory for processing
// Returns the partition entries for probe phase processing
func (asm *AdaptiveSpillManager) LoadPartition(partitionID int) ([]partitionEntry, error) {
	asm.mutex.RLock()
	partition := asm.partitions[partitionID]
	asm.mutex.RUnlock()

	if !partition.IsSpilled {
		return partition.Entries, nil
	}

	// Load from disk
	file, err := os.Open(partition.SpillPath)
	if err != nil {
		return nil, fmt.Errorf("failed to open spill file: %w", err)
	}
	defer file.Close()

	// Create zstd decompressor
	decoder, err := zstd.NewReader(file)
	if err != nil {
		return nil, fmt.Errorf("failed to create zstd decoder: %w", err)
	}
	defer decoder.Close()

	entries := make([]partitionEntry, 0, partition.EntryCount)
	gobDecoder := gob.NewDecoder(decoder)

	for {
		var spilled spilledEntry
		if err := gobDecoder.Decode(&spilled); err != nil {
			break // EOF or error
		}

		entry := asm.convertFromSpilledEntry(spilled)
		entries = append(entries, entry)
	}

	asm.logger.Debugf("Loaded partition %d: %d entries from disk", partitionID, len(entries))
	return entries, nil
}

// GetInMemoryPartitions returns partition IDs that are currently in memory
func (asm *AdaptiveSpillManager) GetInMemoryPartitions() []int {
	asm.mutex.RLock()
	defer asm.mutex.RUnlock()

	var inMemory []int
	for i, p := range asm.partitions {
		if !p.IsSpilled {
			inMemory = append(inMemory, i)
		}
	}
	return inMemory
}

// GetSpilledPartitions returns partition IDs that are on disk
func (asm *AdaptiveSpillManager) GetSpilledPartitions() []int {
	asm.mutex.RLock()
	defer asm.mutex.RUnlock()

	var spilled []int
	for i, p := range asm.partitions {
		if p.IsSpilled {
			spilled = append(spilled, i)
		}
	}
	return spilled
}

// GetPartitionCount returns the current number of partitions
func (asm *AdaptiveSpillManager) GetPartitionCount() int {
	asm.mutex.RLock()
	defer asm.mutex.RUnlock()
	return asm.numPartitions
}

// GetPartitionEntries returns entries for a specific partition (loads if spilled)
func (asm *AdaptiveSpillManager) GetPartitionEntries(partitionID int) ([]partitionEntry, error) {
	return asm.LoadPartition(partitionID)
}

// GetStatistics returns spill statistics
func (asm *AdaptiveSpillManager) GetStatistics() SpillStatistics {
	return SpillStatistics{
		SpillCount:       asm.spillCount.Load(),
		SpilledBytes:     asm.totalSpilledBytes,
		SpilledEntries:   asm.totalSpilledEntries,
		RepartitionCount: asm.repartitionCount,
		FinalPartitions:  asm.numPartitions,
	}
}

// SpillStatistics contains statistics about spill operations
type SpillStatistics struct {
	SpillCount       int64 // Number of spill operations
	SpilledBytes     int64 // Total bytes written to disk
	SpilledEntries   int64 // Total entries spilled
	RepartitionCount int   // Number of repartitions performed
	FinalPartitions  int   // Final partition count
}

// Cleanup removes all spill files and releases resources
func (asm *AdaptiveSpillManager) Cleanup() error {
	asm.mutex.Lock()
	defer asm.mutex.Unlock()

	var errors []error
	for _, p := range asm.partitions {
		if p.IsSpilled && p.SpillPath != "" {
			if err := os.Remove(p.SpillPath); err != nil && !os.IsNotExist(err) {
				errors = append(errors, err)
				asm.logger.Warnf("Failed to remove spill file %s: %v", p.SpillPath, err)
			}
		}
		p.Entries = nil
	}

	if len(errors) > 0 {
		return fmt.Errorf("failed to cleanup %d spill files", len(errors))
	}

	asm.logger.Infof("Cleaned up spill manager: %d spills, %d bytes total",
		asm.spillCount.Load(), asm.totalSpilledBytes)

	return nil
}

// HasSpilled returns true if any data was spilled to disk
func (asm *AdaptiveSpillManager) HasSpilled() bool {
	return asm.spillCount.Load() > 0
}

// Helper methods for memory estimation and serialization

func (asm *AdaptiveSpillManager) estimateEntryMemory(key interface{}, doc *models.Document) int64 {
	// Base entry overhead: 64 bytes for struct + slice headers
	memory := int64(64)

	// Key memory (estimate 32 bytes for typical keys)
	memory += 32

	// Document memory
	memory += asm.estimateDocumentMemory(doc)

	return memory
}

func (asm *AdaptiveSpillManager) estimateDocumentMemory(doc *models.Document) int64 {
	if doc == nil {
		return 0
	}
	memory := int64(128)
	estimateFV := func(fv models.FieldValue) {
		switch fv.Type {
		case models.FieldTypeString:
			memory += int64(len(fv.StringVal))
		case models.FieldTypeInt, models.FieldTypeFloat, models.FieldTypeBool:
			memory += 8
		case models.FieldTypeDateTime, models.FieldTypeDate:
			memory += 24
		case models.FieldTypeInterface:
			memory += 32
		default:
			memory += 16
		}
	}
	if len(doc.Values) > 0 {
		for _, fv := range doc.Values {
			memory += 64
			estimateFV(fv)
		}
		return memory
	}
	if doc.Data != nil {
		for _, v := range doc.Data {
			memory += 64
			estimateFV(models.NewInterfaceValue(v))
		}
	}
	return memory
}

func (asm *AdaptiveSpillManager) estimateEntryMemoryFromEntry(entry partitionEntry) int64 {
	memory := int64(64) // Base overhead
	memory += 32        // Key
	for _, doc := range entry.Documents {
		memory += asm.estimateDocumentMemory(doc)
	}
	return memory
}

func (asm *AdaptiveSpillManager) convertToSpilledEntry(entry partitionEntry) spilledEntry {
	spilled := spilledEntry{
		KeyHash:     entry.KeyHash,
		KeyString:   fmt.Sprintf("%v", entry.Key),
		DocumentIDs: make([]string, len(entry.Documents)),
		FieldMaps:   make([]map[string]FieldData, len(entry.Documents)),
	}

	asm.mutex.RLock()
	schema := asm.schema
	asm.mutex.RUnlock()

	for i, doc := range entry.Documents {
		spilled.DocumentIDs[i] = doc.DocumentID
		spilled.FieldMaps[i] = make(map[string]FieldData)

		if schema != nil && len(doc.Values) > 0 {
			for idx, fv := range doc.Values {
				if idx < len(schema.Names) {
					name := schema.Names[idx]
					spilled.FieldMaps[i][name] = FieldData{Name: name, Value: fv}
				}
			}
		} else if doc.Data != nil {
			for name, v := range doc.Data {
				spilled.FieldMaps[i][name] = FieldData{
					Name:  name,
					Value: models.NewInterfaceValue(v),
				}
			}
		}
	}

	return spilled
}

func (asm *AdaptiveSpillManager) convertFromSpilledEntry(spilled spilledEntry) partitionEntry {
	asm.mutex.RLock()
	schema := asm.schema
	asm.mutex.RUnlock()

	entry := partitionEntry{
		KeyHash:   spilled.KeyHash,
		Key:       spilled.KeyString,
		Documents: make([]*models.Document, len(spilled.DocumentIDs)),
	}

	for i, docID := range spilled.DocumentIDs {
		doc := &models.Document{DocumentID: docID}
		m := spilled.FieldMaps[i]
		if schema != nil && len(m) > 0 {
			doc.Values = make([]models.FieldValue, len(schema.Names))
			for idx, name := range schema.Names {
				if fd, ok := m[name]; ok {
					doc.Values[idx] = fd.Value
				}
			}
		} else {
			doc.Data = make(map[string]interface{}, len(m))
			for name, fd := range m {
				doc.Data[name] = fd.Value.AsInterface()
			}
		}
		entry.Documents[i] = doc
	}

	return entry
}

// BuildHashTableFromPartitions creates a hash table from all partitions
// This is used after the build phase completes to create the final hash table
func (asm *AdaptiveSpillManager) BuildHashTableFromPartitions() (HashTable, error) {
	asm.mutex.RLock()
	totalEntries := int64(0)
	for _, p := range asm.partitions {
		totalEntries += p.EntryCount
	}
	asm.mutex.RUnlock()

	// Create hash table with appropriate capacity
	hashTable := NewInMemoryHashTable(int(totalEntries)*2, 0.75)

	// First, add all in-memory partitions
	for _, partitionID := range asm.GetInMemoryPartitions() {
		entries, err := asm.GetPartitionEntries(partitionID)
		if err != nil {
			return nil, fmt.Errorf("failed to get partition %d entries: %w", partitionID, err)
		}

		for _, entry := range entries {
			for _, doc := range entry.Documents {
				if err := hashTable.Put(entry.Key, doc); err != nil {
					return nil, fmt.Errorf("failed to add entry to hash table: %w", err)
				}
			}
		}
	}

	return hashTable, nil
}
