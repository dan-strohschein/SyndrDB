package brinindex

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"

	"go.uber.org/zap"
)

// BRINIndex implements a Block Range INdex.
// Each entry stores the min/max value for a contiguous range of pages.
// On range queries, non-overlapping ranges are skipped entirely.
//
// A BRIN index for 1M documents in 4096-doc pages is ~250 entries (a few KB).
type BRINIndex struct {
	config  BRINConfig
	entries []BRINEntry
	mu      sync.RWMutex
	isDirty bool
	logger  *zap.SugaredLogger
	stats   BRINStats
}

// BRINConfig holds configuration for a BRIN index.
type BRINConfig struct {
	IndexName     string
	BundleName    string
	DatabaseName  string
	FieldName     string // The column being indexed
	DataDir       string // Directory for persistence
	PagesPerRange uint32 // Number of document pages per BRIN range (default: 128)
	Logger        *zap.SugaredLogger
}

// BRINEntry stores the summary for one contiguous block range.
type BRINEntry struct {
	RangeStart  uint32      `json:"range_start"`
	RangeEnd    uint32      `json:"range_end"`
	HasNulls    bool        `json:"has_nulls"`
	AllNulls    bool        `json:"all_nulls"`
	MinValue    interface{} `json:"min_value"`
	MaxValue    interface{} `json:"max_value"`
	MinFloat    float64     `json:"min_float"`
	MaxFloat    float64     `json:"max_float"`
	MinString   string      `json:"min_string"`
	MaxString   string      `json:"max_string"`
	IsNumeric   bool        `json:"is_numeric"`
	IsString    bool        `json:"is_string"`
	DocCount    int         `json:"doc_count"`
	LastUpdated time.Time   `json:"last_updated"`
}

// BRINStats tracks BRIN index usage statistics.
type BRINStats struct {
	TotalScans    uint64
	RangesScanned uint64
	RangesSkipped uint64
	TotalInserts  uint64
	LastScanTime  time.Time
}

// PageRange represents a contiguous range of page IDs to scan.
type PageRange struct {
	StartPageID uint32
	EndPageID   uint32
}

// NewBRINIndex creates a new BRIN index.
func NewBRINIndex(config BRINConfig) (*BRINIndex, error) {
	if config.FieldName == "" {
		return nil, fmt.Errorf("field name cannot be empty")
	}
	if config.PagesPerRange == 0 {
		config.PagesPerRange = 128
	}
	if config.Logger == nil {
		return nil, fmt.Errorf("logger cannot be nil")
	}

	return &BRINIndex{
		config:  config,
		entries: make([]BRINEntry, 0, 256),
		logger:  config.Logger,
	}, nil
}

// OpenBRINIndex loads an existing BRIN index from disk.
func OpenBRINIndex(config BRINConfig) (*BRINIndex, error) {
	idx, err := NewBRINIndex(config)
	if err != nil {
		return nil, err
	}

	filePath := idx.getFilePath()
	data, err := os.ReadFile(filePath)
	if err != nil {
		if os.IsNotExist(err) {
			return idx, nil
		}
		return nil, fmt.Errorf("failed to read BRIN index file: %w", err)
	}

	if err := json.Unmarshal(data, &idx.entries); err != nil {
		return nil, fmt.Errorf("failed to deserialize BRIN index: %w", err)
	}

	idx.logger.Debugf("Opened BRIN index '%s' with %d range entries", config.IndexName, len(idx.entries))
	return idx, nil
}

func (idx *BRINIndex) getFilePath() string {
	return filepath.Join(idx.config.DataDir,
		fmt.Sprintf("%s_%s.brin", idx.config.BundleName, idx.config.FieldName))
}

// UpdateRange updates the min/max for the range containing the given pageID.
// Called on INSERT/UPDATE.
func (idx *BRINIndex) UpdateRange(pageID uint32, value interface{}) {
	idx.mu.Lock()
	defer idx.mu.Unlock()

	rangeIdx := idx.findOrCreateRange(pageID)
	entry := &idx.entries[rangeIdx]

	isNumeric, floatVal := toFloat64BRIN(value)
	isString := false
	strVal := ""
	if !isNumeric {
		if value != nil {
			strVal = fmt.Sprintf("%v", value)
			isString = true
		}
	}

	if value == nil {
		entry.HasNulls = true
		entry.DocCount++
		entry.LastUpdated = time.Now()
		idx.isDirty = true
		return
	}

	// First non-null value in this range
	if entry.DocCount == 0 || (entry.MinValue == nil && !entry.AllNulls) {
		entry.MinValue = value
		entry.MaxValue = value
		entry.IsNumeric = isNumeric
		entry.IsString = isString
		if isNumeric {
			entry.MinFloat = floatVal
			entry.MaxFloat = floatVal
		}
		if isString {
			entry.MinString = strVal
			entry.MaxString = strVal
		}
		entry.DocCount++
		entry.LastUpdated = time.Now()
		idx.isDirty = true
		return
	}

	// Update min/max
	if isNumeric && entry.IsNumeric {
		if floatVal < entry.MinFloat {
			entry.MinFloat = floatVal
			entry.MinValue = value
		}
		if floatVal > entry.MaxFloat {
			entry.MaxFloat = floatVal
			entry.MaxValue = value
		}
	} else if isString && entry.IsString {
		if strVal < entry.MinString {
			entry.MinString = strVal
			entry.MinValue = value
		}
		if strVal > entry.MaxString {
			entry.MaxString = strVal
			entry.MaxValue = value
		}
	}

	entry.DocCount++
	entry.LastUpdated = time.Now()
	idx.isDirty = true
	idx.stats.TotalInserts++
}

func (idx *BRINIndex) findOrCreateRange(pageID uint32) int {
	ppr := idx.config.PagesPerRange
	rangeStart := (pageID / ppr) * ppr
	rangeEnd := rangeStart + ppr - 1

	lo, hi := 0, len(idx.entries)
	for lo < hi {
		mid := (lo + hi) / 2
		if idx.entries[mid].RangeStart < rangeStart {
			lo = mid + 1
		} else {
			hi = mid
		}
	}

	if lo < len(idx.entries) && idx.entries[lo].RangeStart == rangeStart {
		return lo
	}

	newEntry := BRINEntry{
		RangeStart:  rangeStart,
		RangeEnd:    rangeEnd,
		LastUpdated: time.Now(),
	}
	idx.entries = append(idx.entries, BRINEntry{})
	copy(idx.entries[lo+1:], idx.entries[lo:])
	idx.entries[lo] = newEntry
	return lo
}

// ScanRanges returns page ranges that might contain documents matching [queryMin, queryMax].
// Ranges whose [min, max] does not overlap the query range are excluded.
// Pass nil for queryMin or queryMax to indicate an open-ended range.
func (idx *BRINIndex) ScanRanges(queryMin, queryMax interface{}) []PageRange {
	idx.mu.RLock()
	defer idx.mu.RUnlock()

	idx.stats.TotalScans++

	qMinNumeric, qMinFloat := toFloat64BRIN(queryMin)
	qMaxNumeric, qMaxFloat := toFloat64BRIN(queryMax)

	var result []PageRange

	for i := range idx.entries {
		entry := &idx.entries[i]

		if entry.DocCount == 0 || entry.AllNulls {
			idx.stats.RangesSkipped++
			continue
		}

		// Numeric fast path
		if entry.IsNumeric {
			skip := false
			if qMinNumeric && entry.MaxFloat < qMinFloat {
				skip = true
			}
			if qMaxNumeric && entry.MinFloat > qMaxFloat {
				skip = true
			}
			if skip {
				idx.stats.RangesSkipped++
				continue
			}
		} else if entry.IsString {
			skip := false
			if queryMin != nil {
				qMinStr := fmt.Sprintf("%v", queryMin)
				if entry.MaxString < qMinStr {
					skip = true
				}
			}
			if queryMax != nil {
				qMaxStr := fmt.Sprintf("%v", queryMax)
				if entry.MinString > qMaxStr {
					skip = true
				}
			}
			if skip {
				idx.stats.RangesSkipped++
				continue
			}
		}
		// Mixed/unknown types: don't skip (conservative)

		idx.stats.RangesScanned++
		result = append(result, PageRange{
			StartPageID: entry.RangeStart,
			EndPageID:   entry.RangeEnd,
		})
	}

	idx.stats.LastScanTime = time.Now()
	return result
}

// ScanRangesForOperator handles single-sided range predicates (>, >=, <, <=).
func (idx *BRINIndex) ScanRangesForOperator(operator string, value interface{}) []PageRange {
	switch operator {
	case ">", ">=":
		return idx.ScanRanges(value, nil)
	case "<", "<=":
		return idx.ScanRanges(nil, value)
	case "=":
		return idx.ScanRanges(value, value)
	default:
		return idx.AllRanges()
	}
}

// AllRanges returns every range in the index (full scan fallback).
func (idx *BRINIndex) AllRanges() []PageRange {
	idx.mu.RLock()
	defer idx.mu.RUnlock()

	result := make([]PageRange, len(idx.entries))
	for i, entry := range idx.entries {
		result[i] = PageRange{
			StartPageID: entry.RangeStart,
			EndPageID:   entry.RangeEnd,
		}
	}
	return result
}

// Flush persists the BRIN index to disk.
func (idx *BRINIndex) Flush() error {
	idx.mu.Lock()
	defer idx.mu.Unlock()

	if !idx.isDirty {
		return nil
	}

	data, err := json.Marshal(idx.entries)
	if err != nil {
		return fmt.Errorf("failed to serialize BRIN index: %w", err)
	}

	filePath := idx.getFilePath()
	dir := filepath.Dir(filePath)
	if err := os.MkdirAll(dir, 0755); err != nil {
		return fmt.Errorf("failed to create BRIN index directory: %w", err)
	}

	if err := os.WriteFile(filePath, data, 0644); err != nil {
		return fmt.Errorf("failed to write BRIN index file: %w", err)
	}

	idx.isDirty = false
	return nil
}

// Close flushes and releases resources.
func (idx *BRINIndex) Close() error {
	return idx.Flush()
}

// GetStats returns current BRIN index statistics.
func (idx *BRINIndex) GetStats() BRINStats {
	idx.mu.RLock()
	defer idx.mu.RUnlock()
	return idx.stats
}

// EntryCount returns the number of range entries.
func (idx *BRINIndex) EntryCount() int {
	idx.mu.RLock()
	defer idx.mu.RUnlock()
	return len(idx.entries)
}

// GetConfig returns the BRIN index configuration.
func (idx *BRINIndex) GetConfig() BRINConfig {
	return idx.config
}

// toFloat64BRIN converts a value to float64 for BRIN comparisons.
func toFloat64BRIN(v interface{}) (bool, float64) {
	if v == nil {
		return false, 0
	}
	switch val := v.(type) {
	case int:
		return true, float64(val)
	case int32:
		return true, float64(val)
	case int64:
		return true, float64(val)
	case float32:
		return true, float64(val)
	case float64:
		return true, val
	case uint:
		return true, float64(val)
	case uint32:
		return true, float64(val)
	case uint64:
		return true, float64(val)
	default:
		return false, 0
	}
}
