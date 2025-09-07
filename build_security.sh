#!/bin/bash

# SyndrDB Security Enhancement Build Script
# This script builds the enhanced security components with comprehensive audit logging

echo "=== SyndrDB Security Enhancement Build ==="
echo

# Build the server with enhanced security
echo "1. Building server with security audit logging..."
go build -o bin/server/server src/cmd/server/main.go
if [ $? -eq 0 ]; then
    echo "   ✓ Server built successfully"
else
    echo "   ✗ Server build failed"
    exit 1
fi

# Build the security audit demo
echo "2. Building security audit demo..."
go build -o bin/security_audit_demo src/cmd/security_audit_demo/main.go
if [ $? -eq 0 ]; then
    echo "   ✓ Security audit demo built successfully"
else
    echo "   ✗ Security audit demo build failed"
    exit 1
fi

# Build existing demos
echo "3. Building brute force demo..."
go build -o bin/brute_force_demo src/cmd/brute_force_demo/main.go
if [ $? -eq 0 ]; then
    echo "   ✓ Brute force demo built successfully"
else
    echo "   ✗ Brute force demo build failed"
    exit 1
fi

echo
echo "=== Build Summary ==="
echo "✓ Server: Enhanced with comprehensive security audit logging"
echo "  - Brute force protection with progressive delays"
echo "  - Account lockout after 5 failed attempts (15 min lockout)"
echo "  - Asynchronous security event logging"
echo "  - Thread-safe audit operations"
echo "  - Comprehensive security context logging"
echo
echo "✓ Security Audit Demo: Complete demonstration of audit system"
echo "  - User authentication success/failure logging"
echo "  - Progressive delay event logging" 
echo "  - Account lockout audit trails"
echo "  - JSON-formatted security events"
echo "  - Timestamped audit logs with unique event IDs"
echo
echo "✓ Brute Force Demo: Original brute force protection demo"
echo
echo "Security Features Implemented:"
echo "• Progressive authentication delays: 2s → 4s → 8s → 16s → 32s → 60s"
echo "• Account lockout: 5 failed attempts = 15 minute lockout"
echo "• IP-based rate limiting with exponential backoff"
echo "• Comprehensive security audit logging"
echo "• Asynchronous audit processing (non-blocking)"
echo "• Thread-safe audit operations"
echo "• JSON-formatted audit logs with rotation"
echo "• Detailed security event context and metadata"
echo
echo "Available binaries:"
echo "  bin/server/server              - Enhanced SyndrDB server"
echo "  bin/security_audit_demo        - Security audit logging demo"
echo "  bin/brute_force_demo          - Brute force protection demo"
echo
echo "=== Build Complete ==="
