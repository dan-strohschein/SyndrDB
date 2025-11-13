package documentscanner

import (
	"fmt"
	"reflect"
	"runtime"
	"strings"
	"time"

	"syndrdb/src/internal/domain/models"

	"go.uber.org/zap"
)

// SmartBundleScanner implements intelligent document scanning with batching, caching, and hot key optimization
// This scanner uses PostgreSQL-inspired techniques: sequential I/O, vectorized processing, and predicate pushdown
type SmartBundleScanner struct {
	bundle        BundleInterface    // The bundle to scan
	config        *ScannerConfig     // Configuration parameters
	hotKeyTracker *HotKeyTracker     // Tracks hot keys and query patterns
	cache         CacheInterface     // Cache for frequently accessed data
	logger        *zap.SugaredLogger // Logger for debugging and monitoring

	// Performance metrics
	metrics   *ScanMetrics // Current scanner metrics
	startTime time.Time    // When scanner was created
}

// NewSmartBundleScanner creates a new smart bundle scanner
// bundle: The bundle to scan
// config: Configuration parameters (use DefaultScannerConfig() for defaults)
// cache: Cache implementation for frequently accessed data
// logger: Logger for debugging and monitoring
func NewSmartBundleScanner(
	bundle BundleInterface,
	config *ScannerConfig,
	cache CacheInterface,
	logger *zap.SugaredLogger,
) *SmartBundleScanner {

	if config == nil {
		config = DefaultScannerConfig()
	}

	scanner := &SmartBundleScanner{
		bundle:        bundle,
		config:        config,
		cache:         cache,
		logger:        logger,
		hotKeyTracker: NewHotKeyTracker(logger, config.HotThreshold, config.OptimizeAfter),
		metrics:       &ScanMetrics{},
		startTime:     time.Now(),
	}

	logger.Infof("Created SmartBundleScanner for bundle '%s' with batch size %d",
		bundle.GetName(), config.BatchSize)

	return scanner
}

// ScanForKeyValue performs an optimized scan for documents matching a key-value query
// This is the primary scanning method that implements PostgreSQL-inspired optimizations
func (sbs *SmartBundleScanner) ScanForKeyValue(query *ScanQuery) (*ScanResult, error) {
	startTime := time.Now()

	// Validate query
	if query == nil || query.KeyName == "" {
		return nil, fmt.Errorf("invalid query: key name is required")
	}

	sbs.logger.Debugf("Starting scan for key='%s', value='%v', operator='%s'",
		query.KeyName, query.Value, query.Operator)

	// Check cache first for hot keys
	var cacheHits int
	if sbs.hotKeyTracker.IsHotKey(query.KeyName) {
		if cachedResult := sbs.checkCache(query); cachedResult != nil {
			sbs.logger.Debugf("Cache hit for hot key '%s'", query.KeyName)
			cacheHits++
			latency := time.Since(startTime)
			sbs.hotKeyTracker.RecordQuery(query.KeyName, query.Value, latency)
			sbs.hotKeyTracker.RecordCacheHit(query.KeyName)
			sbs.updateMetrics(cachedResult, cacheHits, latency)
			return cachedResult, nil
		}
	}

	// Perform batched scan - this is our core optimization strategy
	result, err := sbs.performBatchedScan(query)
	if err != nil {
		sbs.metrics.ScanErrors++
		sbs.metrics.LastError = err.Error()
		return nil, fmt.Errorf("scan failed: %w", err)
	}

	// Record query metrics and learn patterns
	latency := time.Since(startTime)
	sbs.hotKeyTracker.RecordQuery(query.KeyName, query.Value, latency)
	sbs.updateMetrics(result, cacheHits, latency)

	// Cache result if key is becoming hot
	if sbs.shouldCacheResult(query.KeyName, result) {
		sbs.cacheResult(query, result)
	}

	sbs.logger.Debugf("Scan completed: found %d documents in %v",
		len(result.Documents), latency)

	return result, nil
}

// ScanWithPredicate performs a scan using a custom predicate function
// This provides flexibility for complex queries that don't fit the key-value model
func (sbs *SmartBundleScanner) ScanWithPredicate(predicate func(*models.Document) bool) (*ScanResult, error) {
	startTime := time.Now()

	if predicate == nil {
		return nil, fmt.Errorf("predicate function is required")
	}

	sbs.logger.Debug("Starting predicate-based scan")

	result := &ScanResult{
		Documents:   make([]*models.Document, 0),
		DocumentIDs: make([]string, 0),
		ScanLatency: 0,
		CacheHits:   0,
	}

	// Use batched processing for predicate scans too
	batchCount := 0
	for batch := range sbs.getBatchedDocuments() {
		batchCount++

		// Apply predicate to each document in the batch
		for _, doc := range batch {
			if predicate(doc) {
				result.Documents = append(result.Documents, doc)
				result.DocumentIDs = append(result.DocumentIDs, doc.DocumentID)
			}
		} // Memory pressure management
		if len(result.Documents) > sbs.config.MemoryThreshold {
			sbs.logger.Debugf("Memory threshold reached (%d docs), triggering GC",
				sbs.config.MemoryThreshold)
			runtime.GC()
			sbs.metrics.MemoryPressureGCs++
		}
	}

	// Finalize results
	result.ScanLatency = time.Since(startTime)
	result.BatchesUsed = batchCount
	result.TotalScanned = sbs.bundle.GetTotalDocuments()

	sbs.updateMetrics(result, 0, result.ScanLatency)

	sbs.logger.Debugf("Predicate scan completed: found %d documents in %v",
		len(result.Documents), result.ScanLatency)

	return result, nil
}

// ScanForInList performs an optimized scan for documents matching an IN query
// This method implements hash set lookups for efficient membership testing
// Parameters:
//   - field: The field name to check
//   - values: The list of values to match against
//   - caseInsensitive: Whether to perform case-insensitive string matching
//   - negate: true for NOT IN, false for IN
//
// Returns:
//   - *ScanResult: The scan results with matching documents
//   - error: Any error that occurred during scanning
//
// TODO: Implement bloom filter optimization for >1000 values to reduce memory usage
// TODO: Add hot query caching for frequently-used IN lists
// TODO: Implement parallel processing for >5000 values using worker pools
func (sbs *SmartBundleScanner) ScanForInList(field string, values []interface{}, caseInsensitive bool, negate bool) (*ScanResult, error) {
	startTime := time.Now()

	if field == "" {
		return nil, fmt.Errorf("field name is required for IN query")
	}

	if len(values) == 0 {
		return nil, fmt.Errorf("IN list cannot be empty")
	}

	sbs.logger.Debugf("Starting IN scan for field='%s', values count=%d, case-insensitive=%v, negate=%v",
		field, len(values), caseInsensitive, negate)

	// Convert values to hash set for O(1) lookups
	valueSet := make(map[interface{}]bool, len(values))
	for _, v := range values {
		valueSet[v] = true
	}

	// TODO: For >1000 values, consider using bloom filter to reduce memory
	// This would provide probabilistic membership testing with much lower memory footprint
	if len(values) > 1000 {
		sbs.logger.Infof("Large IN query detected (%d values). Consider implementing bloom filter optimization.", len(values))
		// TODO: Implement: bloomFilter := createBloomFilter(values)
	}

	// TODO: Check if this IN query pattern is hot and cached
	// Hot query caching would store results for frequently-used IN lists
	// if cachedResult := sbs.checkInQueryCache(field, values); cachedResult != nil {
	//     return cachedResult, nil
	// }

	result := &ScanResult{
		Documents:   make([]*models.Document, 0),
		DocumentIDs: make([]string, 0),
		ScanLatency: 0,
		CacheHits:   0,
	}

	batchCount := 0
	totalScanned := 0

	// TODO: For >5000 values, implement parallel processing with worker pools
	// This would split the document scan across multiple goroutines
	if len(values) > 5000 {
		sbs.logger.Infof("Very large IN query (%d values). Parallel processing recommended.", len(values))
		// TODO: Implement: return sbs.parallelInScan(field, valueSet, caseInsensitive, negate)
	}

	// Process documents in batches
	for batch := range sbs.getBatchedDocuments() {
		batchCount++

		// Check each document in the batch
		for _, doc := range batch {
			totalScanned++

			if doc.Data == nil {
				continue
			}

			docValue, exists := doc.Data[field]
			if !exists {
				continue
			}

			// Perform membership check with hash set lookup (O(1))
			matched := false

			if caseInsensitive {
				// Case-insensitive string matching
				if docStr, ok := docValue.(string); ok {
					docStrLower := strings.ToLower(docStr)
					for value := range valueSet {
						if valueStr, ok := value.(string); ok {
							if strings.ToLower(valueStr) == docStrLower {
								matched = true
								break
							}
						}
					}
				} else {
					// Non-string with case-insensitive flag - use exact match
					matched = valueSet[docValue]
				}
			} else {
				// Case-sensitive or non-string comparison
				matched = valueSet[docValue]
			}

			// Apply negation if NOT IN
			if negate {
				matched = !matched
			}

			if matched {
				result.Documents = append(result.Documents, doc)
				result.DocumentIDs = append(result.DocumentIDs, doc.DocumentID)
			}
		}

		// Memory pressure management
		if len(result.Documents) > sbs.config.MemoryThreshold {
			sbs.logger.Debugf("Memory threshold reached (%d docs), triggering GC",
				sbs.config.MemoryThreshold)
			runtime.GC()
			sbs.metrics.MemoryPressureGCs++
		}
	}

	// Finalize results
	result.BatchesUsed = batchCount
	result.TotalScanned = totalScanned
	result.ScanLatency = time.Since(startTime)

	sbs.updateMetrics(result, 0, result.ScanLatency)

	sbs.logger.Debugf("IN scan completed: found %d/%d documents in %v",
		len(result.Documents), totalScanned, result.ScanLatency)

	// TODO: Cache result if this IN query pattern is becoming hot
	// if sbs.shouldCacheInQuery(field, values, result) {
	//     sbs.cacheInQueryResult(field, values, result)
	// }

	return result, nil
}

// performBatchedScan executes the core batched scanning algorithm
// This implements PostgreSQL-style sequential scanning with predicate pushdown
func (sbs *SmartBundleScanner) performBatchedScan(query *ScanQuery) (*ScanResult, error) {
	result := &ScanResult{
		Documents:   make([]*models.Document, 0),
		DocumentIDs: make([]string, 0),
		ScanLatency: 0,
		CacheHits:   0,
	}

	batchCount := 0
	totalScanned := 0

	// Process documents in batches - this is our key performance optimization
	// Batching provides better cache locality and reduces I/O overhead
	for batch := range sbs.getBatchedDocuments() {
		batchCount++

		// Vectorized processing within the batch
		// Process multiple documents per CPU instruction cycle when possible
		for _, doc := range batch {
			totalScanned++

			// Early predicate pushdown - filter as soon as possible
			if sbs.documentMatchesQuery(doc, query) {
				result.Documents = append(result.Documents, doc)
				result.DocumentIDs = append(result.DocumentIDs, doc.DocumentID)
			}
		}

		// Memory pressure relief - prevent memory spikes for large result sets
		if len(result.Documents) > sbs.config.MemoryThreshold {
			sbs.logger.Debugf("Memory threshold reached (%d docs), triggering GC",
				sbs.config.MemoryThreshold)
			runtime.GC()
			sbs.metrics.MemoryPressureGCs++
		}

		// Break early if we have enough results (optimization for LIMIT-style queries)
		// This is similar to PostgreSQL's ability to stop scanning when enough rows are found
		if len(result.Documents) > 50000 {
			sbs.logger.Debugf("Large result set (%d docs), stopping scan", len(result.Documents))
			break
		}
	}

	result.BatchesUsed = batchCount
	result.TotalScanned = totalScanned

	return result, nil
}

// getBatchedDocuments returns a channel that delivers documents in batches
// This implements PostgreSQL-style sequential I/O optimization
// The channel pattern allows for concurrent processing and memory management
func (sbs *SmartBundleScanner) getBatchedDocuments() <-chan []*models.Document {
	// Buffer the channel to allow producer-consumer parallelism
	// This decouples document loading from document processing
	batchChan := make(chan []*models.Document, sbs.config.ChannelBuffer)

	go func() {
		defer close(batchChan)

		documentIDs := sbs.bundle.GetDocumentIDs()
		currentBatch := make([]*models.Document, 0, sbs.config.BatchSize)

		sbs.logger.Debugf("Starting batched document loading: %d total documents, batch size %d",
			len(documentIDs), sbs.config.BatchSize)

		// Load documents in batches to optimize I/O patterns
		for _, docID := range documentIDs {
			doc := sbs.bundle.GetDocument(docID)
			if doc != nil {
				currentBatch = append(currentBatch, doc)

				// Send batch when full
				if len(currentBatch) >= sbs.config.BatchSize {
					batchChan <- currentBatch
					currentBatch = make([]*models.Document, 0, sbs.config.BatchSize)
				}
			}
		}

		// Send final partial batch
		if len(currentBatch) > 0 {
			batchChan <- currentBatch
		}

		sbs.logger.Debugf("Completed batched document loading")
	}()

	return batchChan
}

// documentMatchesQuery determines if a document matches the query criteria
// This implements efficient value comparison with type safety
func (sbs *SmartBundleScanner) documentMatchesQuery(doc *models.Document, query *ScanQuery) bool {
	if doc.Data == nil {
		return false
	}

	docValue, exists := doc.Data[query.KeyName]
	if !exists {
		return false
	}

	// Use optimized comparison based on operator
	return sbs.compareValues(docValue, query.Value, query.Operator, query.CaseSensitive)
}

// compareValues performs optimized value comparison with proper type handling
// This handles the different comparison operators and data types efficiently
func (sbs *SmartBundleScanner) compareValues(docValue, queryValue interface{}, operator string, caseSensitive bool) bool {
	// Handle nil values
	if docValue == nil || queryValue == nil {
		return docValue == queryValue
	}

	// Get reflection types for comparison
	docType := reflect.TypeOf(docValue)
	queryType := reflect.TypeOf(queryValue)

	// Type compatibility check
	if docType != queryType {
		// Try string conversion for mixed types
		docStr := fmt.Sprintf("%v", docValue)
		queryStr := fmt.Sprintf("%v", queryValue)
		return sbs.compareStrings(docStr, queryStr, operator, caseSensitive)
	}

	// Optimized comparison based on type and operator
	switch operator {
	case "=", "==", "":
		return sbs.compareEqual(docValue, queryValue, caseSensitive)
	case "!=", "<>":
		return !sbs.compareEqual(docValue, queryValue, caseSensitive)
	case ">":
		return sbs.compareGreater(docValue, queryValue)
	case ">=":
		return sbs.compareGreaterEqual(docValue, queryValue)
	case "<":
		return sbs.compareLess(docValue, queryValue)
	case "<=":
		return sbs.compareLessEqual(docValue, queryValue)
	case "LIKE", "like":
		return sbs.compareLike(docValue, queryValue, caseSensitive)
	default:
		sbs.logger.Warnf("Unknown operator '%s', defaulting to equality", operator)
		return sbs.compareEqual(docValue, queryValue, caseSensitive)
	}
}

// compareEqual performs equality comparison with case sensitivity handling
func (sbs *SmartBundleScanner) compareEqual(docValue, queryValue interface{}, caseSensitive bool) bool {
	if docStr, ok := docValue.(string); ok {
		if queryStr, ok := queryValue.(string); ok {
			return sbs.compareStrings(docStr, queryStr, "=", caseSensitive)
		}
	}
	return docValue == queryValue
}

// compareStrings performs string comparison with case sensitivity and operator support
func (sbs *SmartBundleScanner) compareStrings(docStr, queryStr, operator string, caseSensitive bool) bool {
	if !caseSensitive {
		docStr = strings.ToLower(docStr)
		queryStr = strings.ToLower(queryStr)
	}

	switch operator {
	case "=", "==", "":
		return docStr == queryStr
	case "!=", "<>":
		return docStr != queryStr
	case ">":
		return docStr > queryStr
	case ">=":
		return docStr >= queryStr
	case "<":
		return docStr < queryStr
	case "<=":
		return docStr <= queryStr
	case "LIKE", "like":
		return sbs.matchLikePattern(docStr, queryStr)
	default:
		return docStr == queryStr
	}
}

// compareGreater, compareLess, etc. implement numeric and string ordering
func (sbs *SmartBundleScanner) compareGreater(docValue, queryValue interface{}) bool {
	return sbs.compareNumericOrString(docValue, queryValue, ">")
}

func (sbs *SmartBundleScanner) compareGreaterEqual(docValue, queryValue interface{}) bool {
	return sbs.compareNumericOrString(docValue, queryValue, ">=")
}

func (sbs *SmartBundleScanner) compareLess(docValue, queryValue interface{}) bool {
	return sbs.compareNumericOrString(docValue, queryValue, "<")
}

func (sbs *SmartBundleScanner) compareLessEqual(docValue, queryValue interface{}) bool {
	return sbs.compareNumericOrString(docValue, queryValue, "<=")
}

func (sbs *SmartBundleScanner) compareLike(docValue, queryValue interface{}, caseSensitive bool) bool {
	docStr := fmt.Sprintf("%v", docValue)
	queryStr := fmt.Sprintf("%v", queryValue)
	return sbs.compareStrings(docStr, queryStr, "LIKE", caseSensitive)
}

// compareNumericOrString handles comparison for numeric types and strings
func (sbs *SmartBundleScanner) compareNumericOrString(docValue, queryValue interface{}, operator string) bool {
	// Try numeric comparison first
	if docNum, docOk := sbs.toFloat64(docValue); docOk {
		if queryNum, queryOk := sbs.toFloat64(queryValue); queryOk {
			switch operator {
			case ">":
				return docNum > queryNum
			case ">=":
				return docNum >= queryNum
			case "<":
				return docNum < queryNum
			case "<=":
				return docNum <= queryNum
			}
		}
	}

	// Fall back to string comparison
	docStr := fmt.Sprintf("%v", docValue)
	queryStr := fmt.Sprintf("%v", queryValue)
	return sbs.compareStrings(docStr, queryStr, operator, true)
}

// toFloat64 attempts to convert a value to float64 for numeric comparison
func (sbs *SmartBundleScanner) toFloat64(value interface{}) (float64, bool) {
	switch v := value.(type) {
	case int:
		return float64(v), true
	case int32:
		return float64(v), true
	case int64:
		return float64(v), true
	case float32:
		return float64(v), true
	case float64:
		return v, true
	default:
		return 0, false
	}
}

// matchLikePattern implements simple LIKE pattern matching (% and _ wildcards)
func (sbs *SmartBundleScanner) matchLikePattern(text, pattern string) bool {
	// Simple implementation - can be optimized with proper regex or KMP algorithm
	// For now, just convert % to .* and _ to . and use string matching
	if !strings.Contains(pattern, "%") && !strings.Contains(pattern, "_") {
		return text == pattern
	}

	// Convert SQL LIKE pattern to simple substring matching
	if strings.HasPrefix(pattern, "%") && strings.HasSuffix(pattern, "%") {
		substring := pattern[1 : len(pattern)-1]
		return strings.Contains(text, substring)
	}

	if strings.HasPrefix(pattern, "%") {
		suffix := pattern[1:]
		return strings.HasSuffix(text, suffix)
	}

	if strings.HasSuffix(pattern, "%") {
		prefix := pattern[:len(pattern)-1]
		return strings.HasPrefix(text, prefix)
	}

	// For more complex patterns, fall back to exact match
	return text == pattern
}

// Cache management methods

func (sbs *SmartBundleScanner) checkCache(query *ScanQuery) *ScanResult {
	if sbs.cache == nil {
		return nil
	}

	cacheKey := sbs.buildCacheKey(query)
	if cached, found := sbs.cache.Get(cacheKey); found {
		if result, ok := cached.(*ScanResult); ok {
			return result
		}
	}
	return nil
}

func (sbs *SmartBundleScanner) cacheResult(query *ScanQuery, result *ScanResult) {
	if sbs.cache == nil {
		return
	}

	cacheKey := sbs.buildCacheKey(query)
	sbs.cache.Put(cacheKey, result)
}

func (sbs *SmartBundleScanner) buildCacheKey(query *ScanQuery) string {
	return fmt.Sprintf("%s:%s:%v:%s", sbs.bundle.GetName(), query.KeyName, query.Value, query.Operator)
}

func (sbs *SmartBundleScanner) shouldCacheResult(keyName string, result *ScanResult) bool {
	// Cache if:
	// 1. Key is hot or becoming hot
	// 2. Result set is reasonable size (not too big for memory)
	// 3. Cache has space

	isHot := sbs.hotKeyTracker.IsHotKey(keyName)
	reasonableSize := len(result.Documents) < 1000
	hasSpace := sbs.cache.Size() < sbs.config.CacheSize

	return (isHot || sbs.hotKeyTracker.GetKeyStats(keyName) != nil) && reasonableSize && hasSpace
}

// Metrics and status methods

func (sbs *SmartBundleScanner) updateMetrics(result *ScanResult, cacheHits int, latency time.Duration) {
	sbs.metrics.TotalScans++
	sbs.metrics.CacheHits += int64(cacheHits)

	// Update average latency
	if sbs.metrics.TotalScans == 1 {
		sbs.metrics.AverageLatency = latency
	} else {
		// Running average calculation
		total := time.Duration(sbs.metrics.TotalScans-1) * sbs.metrics.AverageLatency
		sbs.metrics.AverageLatency = (total + latency) / time.Duration(sbs.metrics.TotalScans)
	}

	// Update cache hit rate
	if sbs.metrics.TotalScans > 0 {
		sbs.metrics.CacheHitRate = float64(sbs.metrics.CacheHits) / float64(sbs.metrics.TotalScans)
	}

	// Update hot keys info
	hotKeys := sbs.hotKeyTracker.GetHotKeys()
	sbs.metrics.HotKeysIdentified = len(hotKeys)
	if len(hotKeys) > 5 {
		sbs.metrics.TopHotKeys = hotKeys[:5] // Top 5 hot keys
	} else {
		sbs.metrics.TopHotKeys = hotKeys
	}

	// Update batch size info
	if result != nil {
		if sbs.metrics.TotalScans == 1 {
			sbs.metrics.AverageBatchSize = result.BatchesUsed
		} else {
			total := (sbs.metrics.TotalScans - 1) * int64(sbs.metrics.AverageBatchSize)
			sbs.metrics.AverageBatchSize = int((total + int64(result.BatchesUsed)) / sbs.metrics.TotalScans)
		}
	}
}

// Interface implementation methods

func (sbs *SmartBundleScanner) GetMetrics() *ScanMetrics {
	// Return a copy to prevent external modification
	metricsCopy := *sbs.metrics
	metricsCopy.TopHotKeys = make([]string, len(sbs.metrics.TopHotKeys))
	copy(metricsCopy.TopHotKeys, sbs.metrics.TopHotKeys)
	return &metricsCopy
}

func (sbs *SmartBundleScanner) GetHotKeys() []string {
	return sbs.hotKeyTracker.GetHotKeys()
}

func (sbs *SmartBundleScanner) IsHotKey(keyName string) bool {
	return sbs.hotKeyTracker.IsHotKey(keyName)
}

func (sbs *SmartBundleScanner) GetConfig() *ScannerConfig {
	// Return a copy to prevent external modification
	configCopy := *sbs.config
	return &configCopy
}

func (sbs *SmartBundleScanner) Close() error {
	sbs.logger.Infof("Closing SmartBundleScanner for bundle '%s'", sbs.bundle.GetName())

	// Log final statistics
	uptime := time.Since(sbs.startTime)
	hotKeys := sbs.hotKeyTracker.GetHotKeys()

	sbs.logger.Infof("Scanner stats: %d scans, %d hot keys, %.2f%% cache hit rate, uptime %v",
		sbs.metrics.TotalScans,
		len(hotKeys),
		sbs.metrics.CacheHitRate*100,
		uptime)

	// Clear cache if we own it
	if sbs.cache != nil {
		sbs.cache.Clear()
	}

	return nil
}
