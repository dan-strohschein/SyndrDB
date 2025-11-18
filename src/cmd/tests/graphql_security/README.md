# GraphQL Security Tests

This directory contains comprehensive integration tests and performance benchmarks for SyndrDB's GraphQL security layers.

## Test Files

| File | Type | Tests | Description |
|------|------|-------|-------------|
| `basic_test.go` | Integration | 1 | Environment setup validation |
| `security_test.go` | Integration | 7 | All 5 security layers tested independently |
| `schema_security_test.go` | Integration | 5 | End-to-end tests with real bundle schemas |
| `benchmark_test.go` | Performance | 13 | Comprehensive performance benchmarks |
| `helpers.go` | Utility | - | Two-phase test environment setup |

**Test Status:** ✅ All 25 tests/benchmarks passing

## Test Coverage

1. **Complexity Limiting** (`complexity_test.go`)
   - Admin bypass
   - Role-based complexity limits
   - Depth limiting
   - Warning thresholds

2. **Rate Limiting** (`rate_limit_test.go`)
   - Per-user token buckets
   - Role-based rate limits
   - Mutation cost multipliers (5x)
   - DDL cost multipliers (10x)
   - Token refill over time
   - Per-user isolation

3. **Query Timeout** (`timeout_test.go`)
   - Role-based timeout limits
   - Context cancellation
   - Timeout enforcement

4. **Query Monitoring** (`monitoring_test.go`)
   - Metric recording
   - Success/failure tracking
   - DDL detection
   - Memory-based purging
   - Age-based purging

5. **Integration** (`integration_test.go`)
   - All layers working together
   - Layer execution ordering
   - Complete user workflows (admin/authenticated/anonymous)
   - Role cache invalidation
   - Concurrent request handling

## Running Tests

```bash
# Run all integration tests
go test ./src/cmd/tests/graphql_security -v

# Run only benchmarks (skip integration tests)
go test -run='^$' -bench=. -benchmem ./src/cmd/tests/graphql_security

# Run specific benchmark
go test -run='^$' -bench='BenchmarkRoleCache_Hit' -benchmem -benchtime=5s ./src/cmd/tests/graphql_security

# Run with profiling
go test -run='^$' -bench=. -benchmem -cpuprofile=cpu.prof ./src/cmd/tests/graphql_security
go test -run='^$' -bench=. -benchmem -memprofile=mem.prof ./src/cmd/tests/graphql_security
```

## Performance Results

See [BENCHMARK_RESULTS.md](./BENCHMARK_RESULTS.md) for detailed performance analysis.

**Key Metrics:**
- Role cache hit: **16.56 ns/op** (60M+ ops/sec)
- Rate limit check: **125 ns/op** (8M ops/sec, 10 concurrent users)
- Overall admin query: **22μs/op** (45K queries/sec)
- Zero allocations for role caching ✅
- All performance targets met or exceeded ✅
