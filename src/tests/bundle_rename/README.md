# Bundle Rename Test Suite

This directory contains comprehensive tests for the bundle rename functionality with operation locking and system catalog updates.

## Test Files

### operation_lock_test.go
Unit tests for the `BundleOperationLock` mechanism that coordinates concurrent access to bundles.

**Test Coverage:**
- Basic read/write lock acquisition and release
- Multiple concurrent readers (allowed)
- Blocking new operations during rename
- Waiting for active operations to complete
- Timeout behavior when operations don't complete
- Proper cleanup after administrative operations

**Run with:**
```bash
go test -v ./src/tests/bundle_rename/operation_lock_test.go
```

### catalog_update_test.go
Integration tests for bundle renaming with system catalog updates.

**Test Coverage:**
- Bundle rename updates catalog Name field
- Bundle rename updates catalog FilePath field
- Catalog persistence across operations
- Validation of bundle names
- Concurrent operation blocking during rename

**Run with:**
```bash
go test -v ./src/tests/bundle_rename/catalog_update_test.go
```

## Running All Tests

To run all bundle rename tests:
```bash
go test -v ./src/tests/bundle_rename/...
```

To run with race detection:
```bash
go test -race -v ./src/tests/bundle_rename/...
```

## Test Requirements

These tests require:
- Go 1.21 or higher
- Proper workspace setup with all dependencies
- Write access to temporary directory for test databases
- Sufficient memory for concurrent operation tests

## Future Enhancements

Areas marked with TODO comments for future expansion:
- Stress tests with hundreds of concurrent operations
- Long-running operation tests
- Distributed system tests (multi-server)
- Performance benchmarks
- Catalog consistency verification after server restart
- Recovery tests after failures

## Notes

- Tests use temporary directories that are cleaned up after each test
- Some integration tests may be slow due to database initialization
- Concurrent operation tests may show timing variations on different systems
- All tests follow the Single Responsibility Principle with focused test cases
