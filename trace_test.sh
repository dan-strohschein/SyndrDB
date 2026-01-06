#!/bin/bash

# Trace Test Script for ADD DOCUMENT Performance Analysis
# This script builds the server with tracing enabled and captures a single ADD DOCUMENT operation

set -e

echo "🔧 Building server with trace support..."
./build.sh

echo ""
echo "🚀 Starting server with execution tracing enabled..."
echo "   Trace will capture all operations for detailed analysis"
echo ""

# Start server in background with tracing enabled
ENABLE_TRACE=true ./bin/server &
SERVER_PID=$!

echo "   Server PID: $SERVER_PID"
echo ""

# Wait for server to start
echo "⏳ Waiting for server to initialize..."
sleep 3

# Send the test ADD DOCUMENT command via client
echo "📝 Sending ADD DOCUMENT command..."
echo ""
echo 'ADD DOCUMENT TO BUNDLE "users" WITH ({"name" = "Gloria Robertson"}, {"email" = "tiffany98@example.org"}, {"created_at" = "2024-07-02T20:38:15.975875"}, {"last_login" = "2025-12-21T20:38:07.447450"});' | ./bin/client

echo ""
echo "✅ Command sent successfully"
echo ""

# Give a moment for trace to flush
sleep 1

# Stop the server gracefully
echo "🛑 Stopping server..."
kill -TERM $SERVER_PID
wait $SERVER_PID 2>/dev/null || true

echo ""
echo "📊 Trace Analysis:"
echo "   Trace files generated: trace_add_document_*.out"
echo ""
echo "   To analyze the trace, run:"
echo "   go tool trace trace_add_document_*.out"
echo ""
echo "   This will open a web browser with:"
echo "   - View trace: Timeline showing every operation"
echo "   - Goroutine analysis: See blocking and execution time"
echo "   - Network blocking profile: I/O operations"
echo "   - Synchronization blocking profile: Lock contention"
echo ""
echo "   Look for the longest spans in the timeline view to find the bottleneck!"
