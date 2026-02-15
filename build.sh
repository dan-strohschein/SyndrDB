#!/bin/bash

set -e

# Check if current directory is 'src', if not, cd into it
if [ "$(basename "$PWD")" != "src" ]; then
  if [ -d "src" ]; then
    cd src
  else
    echo "Error: 'src' directory not found."
    exit 1
  fi
fi

# Build the syndrdb server
go build -o ../bin/server/server cmd/server/main.go

echo "Build complete: ../bin/server/server"

# Build the syndrdb client
go build -o ../bin/client/client ./cmd/client/

echo "Build complete: ../bin/client/client"

# Build the syndrdb tests. Going all in on custom tests for now.
# make circle back and update this with the real golang test framework later.
go build -o ../bin/tests/test_runner cmd/tests/*.go


# Check for CLI parameters
if [ $# -gt 0 ]; then
    case "$1" in
        "test")
            echo ""
            echo "=========================================="
            echo "Executing SyndrDB Test Suite..."
            echo "=========================================="
            echo ""
            
            # Change to the directory containing the test runner
            cd ../bin/tests
            
            # Execute the test runner
            if [ -x "./test_runner" ]; then
                ./test_runner
                test_exit_code=$?
                
                echo ""
                if [ $test_exit_code -eq 0 ]; then
                    echo "=========================================="
                    echo "✅ Test execution completed successfully!"
                    echo "=========================================="
                else
                    echo "=========================================="
                    echo "❌ Test execution failed with exit code $test_exit_code"
                    echo "=========================================="
                    exit $test_exit_code
                fi
            else
                echo "Error: Test runner not found or not executable at ./test_runner"
                exit 1
            fi
            ;;
        "help"|"-h"|"--help")
            echo ""
            echo "SyndrDB Build Script Usage:"
            echo "  ./build.sh         - Build server, client, and tests"
            echo "  ./build.sh test    - Build and execute tests"
            echo "  ./build.sh help    - Show this help message"
            echo ""
            ;;
        *)
            echo "Unknown parameter: $1"
            echo "Use './build.sh help' for usage information"
            exit 1
            ;;
    esac
else
    echo ""
    echo "Build completed. Use './build.sh test' to run tests."
fi