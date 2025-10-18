# SyndrDB Document Scanner

A high-performance document scanning module for SyndrDB that implements PostgreSQL-inspired optimization techniques adapted for document databases.

## Overview

The Document Scanner module provides intelligent document scanning capabilities with:

- **Batched I/O Processing**: Loads documents in batches to optimize I/O patterns
- **Hot Key Detection**: Learns query patterns and identifies frequently accessed keys
- **Smart Caching**: Caches results for hot keys to reduce scan overhead
- **Performance Metrics**: Tracks scan performance and provides optimization recommendations
- **Memory Management**: Handles large result sets with controlled memory usage

## Architecture

The module follows the Single Responsibility and Open/Closed principles with clear separation of concerns:

```
interfaces.go    - Core interfaces and configuration types
smart_scanner.go - Main scanning implementation with PostgreSQL-inspired optimizations
hot_key_tracker.go - Learning component that identifies query patterns
cache.go        - Simple LRU cache implementation
factory.go      - Factory pattern for scanner creation and Bundle adapter
metrics.go      - Metrics aggregation and optimization recommendations
integration.go  - Integration examples and suggested Bundle model extensions
```

## Key Components

### SmartBundleScanner

The main scanning engine that implements:

- **Sequential I/O**: Reads documents in large, contiguous batches (default: 1000 documents)
- **Predicate Pushdown**: Applies filters as early as possible in the scan pipeline
- **Vectorized Processing**: Processes multiple documents per operation cycle
- **Memory Pressure Management**: Triggers garbage collection when result sets exceed thresholds

### HotKeyTracker

Machine learning component that:

- Tracks query frequency per key
- Identifies hot keys (default: 10+ queries)
- Suggests optimization opportunities (default: after 50 queries)
- Maintains query statistics and cardinality estimates

### MetricsManager

System-wide metrics aggregation that provides:

- Global performance metrics across all scanners
- Bundle-specific performance insights
- Optimization recommendations for query planners
- Real-time monitoring of scanner health

## Configuration

Default configuration provides balanced performance:

```go
config := documentscanner.DefaultScannerConfig()
// BatchSize: 1000 documents per batch
// HotThreshold: 10 queries to mark as hot key
// OptimizeAfter: 50 queries before suggesting optimization
// MemoryThreshold: 10,000 results before triggering GC
// CacheSize: 5,000 cached items
```

## Usage Example

```go
// Create integration
integration := documentscanner.NewScannerIntegration(logger)
defer integration.Close()

// Create scanner for a bundle
scanner, err := integration.CreateScannerForBundle(bundle)
if err != nil {
    return err
}

// Perform a scan
query := &documentscanner.ScanQuery{
    KeyName:       "status",
    Value:         "active",
    Operator:      "=",
    CaseSensitive: false,
}

result, err := scanner.ScanForKeyValue(query)
if err != nil {
    return err
}

// Access results
fmt.Printf("Found %d documents in %v\\n", 
    len(result.Documents), result.ScanLatency)

// Get performance metrics
metrics := scanner.GetMetrics()
fmt.Printf("Cache hit rate: %.2f%%\\n", metrics.CacheHitRate*100)

// Get optimization recommendations
recommendations := integration.GetMetricsManager().GetOptimizationRecommendations()
for _, rec := range recommendations {
    fmt.Printf("Recommendation: %s for %s\\n", rec.Type, rec.BundleName)
}
```

## Performance Characteristics

### Scanning Performance

- **Small scans** (< 1000 docs): ~1-5ms latency
- **Medium scans** (1000-10000 docs): ~10-50ms latency  
- **Large scans** (10000+ docs): ~100ms+ latency with memory management

### Memory Usage

- **Batch processing**: Peak memory = batch_size * avg_document_size
- **Result caching**: Controlled by cache_size setting
- **Hot key tracking**: ~1KB per tracked key
- **Automatic GC**: Triggered at memory_threshold results

### Concurrency

- **Thread-safe**: All components support concurrent access
- **Batch parallelism**: Producer-consumer pattern for batch processing
- **Lock contention**: Minimized with read-write locks
- **Goroutine limits**: Configurable to prevent resource exhaustion

## Integration with SyndrDB

The scanner module integrates with existing SyndrDB components through:

### Bundle Integration

The `BundleAdapter` allows the scanner to work with existing Bundle models:

```go
adapter := documentscanner.NewBundleAdapter(bundle)
scanner, err := factory.CreateScanner(adapter, config)
```

### Suggested Bundle Extensions

For optimal integration, consider adding these methods to the Bundle model:

```go
// SCANNER INTEGRATION: New methods to support document scanner interface
func (b *Bundle) GetDocumentIDs() []string
func (b *Bundle) GetDocument(docID string) *Document  
func (b *Bundle) GetAllDocuments() map[string]*Document
func (b *Bundle) GetTotalDocuments() int
func (b *Bundle) CreateScanner(logger *zap.SugaredLogger, config *ScannerConfig) (DocumentScannerInterface, error)
```

### Query Planner Integration

The scanner exposes metrics that inform query optimization:

```go
// Get scanner recommendations
recommendations := metricsManager.GetOptimizationRecommendations()

// Use recommendations to:
// 1. Create indexes for hot keys
// 2. Adjust cache sizes
// 3. Partition large bundles
// 4. Optimize memory settings
```

## Design Philosophy

This implementation follows PostgreSQL's approach to table scanning:

1. **Minimize Random I/O**: Batch loading ensures sequential access patterns
2. **Early Filtering**: Apply predicates as close to storage as possible  
3. **Adaptive Optimization**: Learn from query patterns and adapt behavior
4. **Memory Awareness**: Control memory usage to prevent system instability
5. **Metrics-Driven**: Provide data for intelligent optimization decisions

The scanner bridges the gap between document database flexibility and relational database performance optimization techniques.