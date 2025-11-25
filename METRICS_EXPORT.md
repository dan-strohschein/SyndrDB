# SyndrDB Metrics Export System

This document describes how to use the memory-mapped metrics export system for real-time monitoring.

## Overview

The metrics export system consists of two components:
1. **MemoryMappedExporter** - Server-side component that exports metrics to a binary file
2. **metrics_reader** - Standalone tool to read and display the metrics

## Binary Format

The metrics file uses a compact binary format (Little Endian):
```
[version:4 bytes][timestamp:8 bytes][count:8 bytes][metrics array]
```

Each metric in the array:
```
[key_length:4 bytes][key:N bytes][value:8 bytes]
```

## Server Integration

### 1. Import the monitoring package

```go
import (
	"syndrdb/src/internal/monitoring"
	"time"
)
```

### 2. Initialize the exporter in your server startup

```go
// In your server initialization (e.g., main.go or server.go)
func startServer(logger *zap.SugaredLogger) {
	// ... existing server initialization ...
	
	// Initialize metrics exporter
	metricsPath := "/tmp/syndrdb_metrics.mmap"
	metricsInterval := 1 * time.Second  // Update every second
	
	exporter, err := monitoring.NewMemoryMappedExporter(metricsPath, metricsInterval, logger)
	if err != nil {
		logger.Fatalf("Failed to create metrics exporter: %v", err)
	}
	
	if err := exporter.Start(); err != nil {
		logger.Fatalf("Failed to start metrics exporter: %v", err)
	}
	
	// Ensure cleanup on shutdown
	defer func() {
		if err := exporter.Stop(); err != nil {
			logger.Errorf("Error stopping metrics exporter: %v", err)
		}
	}()
	
	logger.Infof("Metrics exporter started at %s", metricsPath)
	
	// ... rest of server startup ...
}
```

### 3. Optional: Make paths configurable

Add to your server configuration:
```go
type ServerConfig struct {
	// ... existing fields ...
	MetricsExportPath     string        `yaml:"metrics_export_path"`
	MetricsExportInterval time.Duration `yaml:"metrics_export_interval"`
}
```

Example YAML config:
```yaml
metrics_export_path: "/var/run/syndrdb/metrics.mmap"
metrics_export_interval: 1s
```

## Using the Metrics Reader

### Build the tool

```bash
cd src/cmd/metrics_reader
go build -o metrics_reader
```

### Single read

```bash
./metrics_reader -file /tmp/syndrdb_metrics.mmap
```

Output example:
```
═══════════════════════════════════════════════════════════════
SyndrDB Metrics Snapshot
═══════════════════════════════════════════════════════════════
Version:   1
Timestamp: 2025-11-23T15:30:45-08:00
Metrics:   115 total
───────────────────────────────────────────────────────────────

╔═ query
║  query_executions_total                                     1542
║  query_plan_cache_hits                                       892
║  query_plan_cache_misses                                     650
║  query_timeouts_total                                          2
║  query_memory_limit_exceeded                                   0
║  ...
╚═

╔═ hash_index
║  hash_index_puts_total                                      5234
║  hash_index_gets_total                                     12841
║  hash_index_cache_hits                                      9201
║  ...
╚═
```

### Continuous monitoring (watch mode)

```bash
./metrics_reader -file /tmp/syndrdb_metrics.mmap -watch
```

Refreshes every second by default. Change interval:
```bash
./metrics_reader -file /tmp/syndrdb_metrics.mmap -watch -interval 2s
```

### JSON output for scripting

```bash
./metrics_reader -file /tmp/syndrdb_metrics.mmap -json
```

Output:
```json
{
  "version": 1,
  "timestamp": 1700772645,
  "metrics": {
    "query_executions_total": 1542,
    "query_plan_cache_hits": 892,
    "hash_index_puts_total": 5234,
    ...
  }
}
```

### Using with jq for filtering

```bash
# Get specific metric
./metrics_reader -file /tmp/syndrdb_metrics.mmap -json | jq '.metrics.query_executions_total'

# Get all query metrics
./metrics_reader -file /tmp/syndrdb_metrics.mmap -json | jq '.metrics | with_entries(select(.key | startswith("query_")))'

# Calculate cache hit rate
./metrics_reader -file /tmp/syndrdb_metrics.mmap -json | jq '
  (.metrics.query_plan_cache_hits / (.metrics.query_plan_cache_hits + .metrics.query_plan_cache_misses)) * 100
'
```

## Integration with Monitoring Systems

### Prometheus

Create a custom exporter that reads the mmap file and exposes Prometheus metrics:

```go
import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

// Poll the mmap file and update Prometheus gauges
func updatePrometheusMetrics() {
	snapshot, _ := readMetricsFile("/tmp/syndrdb_metrics.mmap")
	
	for key, value := range snapshot.Metrics {
		gauge := promauto.NewGauge(prometheus.GaugeOpts{
			Name: "syndrdb_" + key,
		})
		gauge.Set(float64(value))
	}
}
```

### Grafana

Use the Prometheus exporter above or create a custom data source that reads the mmap file directly.

### DataDog / CloudWatch

Write a lightweight agent that periodically reads the mmap file and sends metrics:

```bash
#!/bin/bash
# Example DataDog integration
while true; do
  metrics_reader -file /tmp/syndrdb_metrics.mmap -json | \
    jq -r '.metrics | to_entries[] | "syndrdb.\(.key):\(.value)|g"' | \
    while read metric; do
      echo $metric | nc -w 1 -u localhost 8125  # DataDog StatsD
    done
  sleep 10
done
```

## Performance Characteristics

- **File size**: ~10-20KB (115 metrics @ ~100-150 bytes each)
- **Update frequency**: Configurable (default: 1 second)
- **Write overhead**: ~100-200μs per export (atomic file replacement)
- **Read overhead**: ~50-100μs (memory-mapped read)
- **Memory footprint**: Minimal (~50KB for exporter goroutine)

## Troubleshooting

### Permission denied

```bash
# Ensure directory is writable
sudo mkdir -p /var/run/syndrdb
sudo chown $USER /var/run/syndrdb
```

### File not updating

Check exporter logs:
```bash
tail -f /var/log/syndrdb/server.log | grep "metrics exporter"
```

### Stale metrics

The timestamp in the file shows when it was last updated. If it's more than a few seconds old, the exporter may have stopped.

## Security Considerations

1. **File permissions**: The metrics file contains operational data but no sensitive information. Set appropriate permissions:
   ```bash
   chmod 644 /tmp/syndrdb_metrics.mmap
   ```

2. **Path configuration**: Use a dedicated directory with restricted access in production:
   ```bash
   /var/run/syndrdb/metrics.mmap
   ```

3. **Network exposure**: The mmap file is local-only. For remote monitoring, use an authenticated proxy or VPN.

## Available Metrics

See the full list in `/src/internal/server/memory_metrics.go`:

- **Hash Index**: puts, gets, deletes, cache hits/misses, latency histograms
- **B-Tree Index**: inserts, searches, deletes, node operations, latency histograms
- **Query Execution**: total queries, cache performance, timeouts, memory limits, latency buckets
- **Transactions**: begun, committed, rolled back, aborted
- **WAL**: writes, flushes, syncs, segment rotations, replay metrics, replication lag
- **Compaction**: total operations, bytes read/written, duration histograms, trigger types
- **Sessions**: active, created, terminated, auth failures
- **Documents**: inserts, updates, deletes, reads (global, per-database, per-bundle)

Total: **115+ metrics** (and growing with Plans A-E enhancements)
