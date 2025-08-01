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
go build -o ../bin/client/client cmd/client/main.go

echo "Build complete: ../bin/client/client"

# Build the syndrdb tests. Going all in on custom tests for now.
# make circle back and update this with the real golang test framework later.
go build -o ../bin/tests/test_runner cmd/tests/*.go


echo "Tests built and executed."