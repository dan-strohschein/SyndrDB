// SyndrDB Metrics Reader
//
// A standalone tool to read and display metrics from SyndrDB's memory-mapped metrics file.
// This tool can run continuously (watch mode) or perform a single read.
//
// Usage:
//
//	metrics_reader -file /tmp/syndrdb_metrics.mmap           # Single read
//	metrics_reader -file /tmp/syndrdb_metrics.mmap -watch    # Continuous monitoring
//	metrics_reader -file /tmp/syndrdb_metrics.mmap -watch -interval 2s
//	metrics_reader -file /tmp/syndrdb_metrics.mmap -json     # JSON output
//
// The tool reads the binary format produced by the MemoryMappedExporter and displays
// metrics in a human-readable format with category grouping and value formatting.
package main

import (
	"encoding/binary"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"sort"
	"time"
)

// MetricsSnapshot represents a point-in-time snapshot of all metrics
type MetricsSnapshot struct {
	Version   uint32            `json:"version"`
	Timestamp int64             `json:"timestamp"`
	Metrics   map[string]uint64 `json:"metrics"`
}

func main() {
	// Command-line flags
	filePath := flag.String("file", "/tmp/syndrdb_metrics.mmap", "Path to metrics file")
	watch := flag.Bool("watch", false, "Continuously watch for metrics updates")
	interval := flag.Duration("interval", 1*time.Second, "Refresh interval in watch mode")
	jsonOutput := flag.Bool("json", false, "Output in JSON format")
	flag.Parse()

	if *watch {
		watchMetrics(*filePath, *interval, *jsonOutput)
	} else {
		snapshot, err := readMetrics(*filePath)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Error reading metrics: %v\n", err)
			os.Exit(1)
		}
		displayMetrics(snapshot, *jsonOutput)
	}
}

// readMetrics reads and parses the metrics file
func readMetrics(filePath string) (*MetricsSnapshot, error) {
	data, err := os.ReadFile(filePath)
	if err != nil {
		return nil, fmt.Errorf("failed to read file: %w", err)
	}

	if len(data) < 20 {
		return nil, fmt.Errorf("file too small (minimum 20 bytes required)")
	}

	snapshot := &MetricsSnapshot{
		Metrics: make(map[string]uint64),
	}

	offset := 0

	// Read version (4 bytes)
	snapshot.Version = binary.LittleEndian.Uint32(data[offset : offset+4])
	offset += 4

	// Read timestamp (8 bytes)
	snapshot.Timestamp = int64(binary.LittleEndian.Uint64(data[offset : offset+8]))
	offset += 8

	// Read count (8 bytes)
	count := binary.LittleEndian.Uint64(data[offset : offset+8])
	offset += 8

	// Read each metric
	for i := uint64(0); i < count; i++ {
		if offset+4 > len(data) {
			return nil, fmt.Errorf("unexpected end of file reading metric %d key length", i)
		}

		// Read key length (4 bytes)
		keyLen := binary.LittleEndian.Uint32(data[offset : offset+4])
		offset += 4

		if offset+int(keyLen) > len(data) {
			return nil, fmt.Errorf("unexpected end of file reading metric %d key", i)
		}

		// Read key (variable bytes)
		key := string(data[offset : offset+int(keyLen)])
		offset += int(keyLen)

		if offset+8 > len(data) {
			return nil, fmt.Errorf("unexpected end of file reading metric %d value", i)
		}

		// Read value (8 bytes)
		value := binary.LittleEndian.Uint64(data[offset : offset+8])
		offset += 8

		snapshot.Metrics[key] = value
	}

	return snapshot, nil
}

// displayMetrics displays the metrics snapshot
func displayMetrics(snapshot *MetricsSnapshot, jsonOutput bool) {
	if jsonOutput {
		encoder := json.NewEncoder(os.Stdout)
		encoder.SetIndent("", "  ")
		encoder.Encode(snapshot)
		return
	}

	// Human-readable output
	fmt.Printf("═══════════════════════════════════════════════════════════════\n")
	fmt.Printf("SyndrDB Metrics Snapshot\n")
	fmt.Printf("═══════════════════════════════════════════════════════════════\n")
	fmt.Printf("Version:   %d\n", snapshot.Version)
	fmt.Printf("Timestamp: %s\n", time.Unix(snapshot.Timestamp, 0).Format(time.RFC3339))
	fmt.Printf("Metrics:   %d total\n", len(snapshot.Metrics))
	fmt.Printf("───────────────────────────────────────────────────────────────\n\n")

	// Group metrics by category
	categories := groupMetricsByCategory(snapshot.Metrics)

	// Sort category names
	categoryNames := make([]string, 0, len(categories))
	for category := range categories {
		categoryNames = append(categoryNames, category)
	}
	sort.Strings(categoryNames)

	// Display each category
	for _, category := range categoryNames {
		metrics := categories[category]
		fmt.Printf("╔═ %s\n", category)

		// Sort metric names
		metricNames := make([]string, 0, len(metrics))
		for name := range metrics {
			metricNames = append(metricNames, name)
		}
		sort.Strings(metricNames)

		for _, name := range metricNames {
			value := metrics[name]
			fmt.Printf("║  %-45s %15s\n", name, formatValue(value))
		}
		fmt.Printf("╚═\n\n")
	}
}

// groupMetricsByCategory groups metrics by their prefix
func groupMetricsByCategory(metrics map[string]uint64) map[string]map[string]uint64 {
	categories := make(map[string]map[string]uint64)

	for key, value := range metrics {
		category := extractCategory(key)
		if categories[category] == nil {
			categories[category] = make(map[string]uint64)
		}
		categories[category][key] = value
	}

	return categories
}

// extractCategory extracts the category from a metric name
func extractCategory(metricName string) string {
	prefixes := []string{
		"hash_index_", "btree_", "query_", "transaction", "wal_",
		"compaction_", "session", "document_", "db_", "bundle_",
	}

	for _, prefix := range prefixes {
		if len(metricName) >= len(prefix) && metricName[:len(prefix)] == prefix {
			return prefix[:len(prefix)-1] // Remove trailing underscore
		}
	}

	return "other"
}

// formatValue formats a metric value for display
func formatValue(value uint64) string {
	if value > 1_000_000_000 {
		return fmt.Sprintf("%d (%.2fB)", value, float64(value)/1_000_000_000)
	} else if value > 1_000_000 {
		return fmt.Sprintf("%d (%.2fM)", value, float64(value)/1_000_000)
	} else if value > 1_000 {
		return fmt.Sprintf("%d (%.2fK)", value, float64(value)/1_000)
	}
	return fmt.Sprintf("%d", value)
}

// watchMetrics continuously monitors the metrics file
func watchMetrics(filePath string, interval time.Duration, jsonOutput bool) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		if !jsonOutput {
			// Clear screen (ANSI escape code)
			fmt.Print("\033[2J\033[H")
		}

		snapshot, err := readMetrics(filePath)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Error reading metrics: %v\n", err)
		} else {
			displayMetrics(snapshot, jsonOutput)
		}

		<-ticker.C
	}
}
