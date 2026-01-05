#!/bin/bash

# FSYNC PERFORMANCE BENCHMARK RUNNER
# 
# This script runs the comprehensive fsync benchmark suite and outputs
# detailed performance metrics to validate the <10ms write latency target.

set -e

echo "=========================================="
echo "SyndrDB Fsync Performance Benchmark Suite"
echo "=========================================="
echo ""

# Navigate to project root
cd "$(dirname "$0")"

echo "Running baseline fsync performance tests..."
go test -v -run TestFdatasyncBasic ./src/cmd/tests/fsync_bench_test.go

echo ""
echo "Running comparative benchmarks..."
go test -bench=Benchmark -benchmem -benchtime=100x ./src/cmd/tests/fsync_bench_test.go

echo ""
echo "=========================================="
echo "Benchmark suite completed!"
echo "=========================================="
