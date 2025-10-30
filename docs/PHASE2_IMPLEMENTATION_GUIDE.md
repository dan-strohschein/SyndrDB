# Phase 2 Implementation Guide - Feature Flag Integration

## Quick Start

This guide provides step-by-step instructions for implementing Phase 2: Feature flag integration in `command_director.go`.

**Estimated Time**: 2 hours  
**Difficulty**: Moderate  
**Prerequisites**: Phase 1 complete ✅

---

## Step 1: Add Configuration Flag (15 minutes)

### Option A: Environment Variable
Add to your shell config or `.env`:
```bash
export SYNDRDB_USE_NEW_PARSER=false
```

### Option B: Configuration File
Add to `config.yaml`:
```yaml
parser:
  use_new_syndrql_parser: false  # Start with false for safety
  fallback_on_error: true         # Enable fallback mechanism
```

### Implementation in Code
```go
// In settings package or configuration loader
func ShouldUseNewParser() bool {
    // Check environment variable first
    if val := os.Getenv("SYNDRDB_USE_NEW_PARSER"); val != "" {
        return strings.ToLower(val) == "true"
    }
    
    // Check configuration file
    if config := LoadConfig(); config != nil {
        return config.Parser.UseNewSyndrQLParser
    }
    
    // Default to false (legacy parser) for safety
    return false
}
```

---

## Step 2: Modify SelectDocuments() in command_director.go (45 minutes)

### Location
File: `src/internal/server/command_director.go`  
Function: `SelectDocuments()`

### Current Code (Approximate)
```go
func (cd *CommandDirector) SelectDocuments(query string) (*QueryResult, error) {
    // Parse query
    unifiedQuery, err := queryparser.ParseUnifiedSelectQuery(query, cd.logger)
    if err != nil {
        return nil, fmt.Errorf("failed to parse query: %w", err)
    }
    
    // Create plan
    planner := queryplanner.NewUnifiedQueryPlanner(cd.bundleService, cd.logger)
    plan, err := planner.CreatePlan(unifiedQuery)
    if err != nil {
        return nil, fmt.Errorf("failed to create plan: %w", err)
    }
    
    // Execute plan
    return plan.RootNode.Execute()
}
```

### New Code (With Feature Flag)
```go
func (cd *CommandDirector) SelectDocuments(query string) (*QueryResult, error) {
    var unifiedQuery *queryparser.UnifiedSelectQuery
    var err error
    
    // Check feature flag
    if settings.ShouldUseNewParser() {
        cd.logger.Debugf("Using new SyndrQL parser for query: %s", query)
        unifiedQuery, err = cd.parseWithNewParser(query)
        
        if err != nil {
            // Log failure and fall back
            cd.logger.Warnf("New parser failed: %v. Falling back to legacy parser.", err)
            cd.recordParserFallback(query, err)
            unifiedQuery, err = queryparser.ParseUnifiedSelectQuery(query, cd.logger)
        } else {
            cd.recordParserSuccess(query)
        }
    } else {
        // Use legacy parser
        cd.logger.Debugf("Using legacy parser for query: %s", query)
        unifiedQuery, err = queryparser.ParseUnifiedSelectQuery(query, cd.logger)
    }
    
    if err != nil {
        return nil, fmt.Errorf("failed to parse query: %w", err)
    }
    
    // Rest of the function remains unchanged
    planner := queryplanner.NewUnifiedQueryPlanner(cd.bundleService, cd.logger)
    plan, err := planner.CreatePlan(unifiedQuery)
    if err != nil {
        return nil, fmt.Errorf("failed to create plan: %w", err)
    }
    
    return plan.RootNode.Execute()
}

// Helper method: Parse with new parser
func (cd *CommandDirector) parseWithNewParser(query string) (*queryparser.UnifiedSelectQuery, error) {
    // Import syndrQL package
    // import "syndrdb/src/internal/syndrQL"
    
    // Tokenize
    tokenizer := syndrQL.NewTokenizer(query)
    tokens, err := tokenizer.Tokenize()
    if err != nil {
        return nil, fmt.Errorf("tokenization failed: %w", err)
    }
    
    // Parse
    parser := syndrQL.NewSelectParser(tokens)
    stmt, err := parser.Parse()
    if err != nil {
        return nil, fmt.Errorf("parsing failed: %w", err)
    }
    
    // Convert to UnifiedSelectQuery
    adapter := syndrQL.NewSelectStatementAdapter(cd.logger)
    unifiedQuery, err := adapter.ToUnifiedSelectQuery(stmt)
    if err != nil {
        return nil, fmt.Errorf("conversion failed: %w", err)
    }
    
    // Optional: Validate in dev/test environments
    if cd.isDevEnvironment() {
        if validationErr := adapter.ValidateConversion(stmt, unifiedQuery); validationErr != nil {
            cd.logger.Errorf("Validation failed: %v", validationErr)
            // Don't fail - just log
        }
    }
    
    return unifiedQuery, nil
}
```

---

## Step 3: Add Metrics and Logging (30 minutes)

### Add Metrics Recording
```go
// Add to CommandDirector struct
type CommandDirector struct {
    // ... existing fields ...
    
    parserMetrics *ParserMetrics
}

// Metrics struct
type ParserMetrics struct {
    NewParserAttempts   atomic.Int64
    NewParserSuccesses  atomic.Int64
    NewParserFailures   atomic.Int64
    FallbacksTriggered  atomic.Int64
    
    // Pattern-specific metrics
    PatternCounts map[string]*atomic.Int64
    mu            sync.RWMutex
}

// Record success
func (cd *CommandDirector) recordParserSuccess(query string) {
    if cd.parserMetrics != nil {
        cd.parserMetrics.NewParserAttempts.Add(1)
        cd.parserMetrics.NewParserSuccesses.Add(1)
    }
}

// Record fallback
func (cd *CommandDirector) recordParserFallback(query string, err error) {
    if cd.parserMetrics != nil {
        cd.parserMetrics.NewParserAttempts.Add(1)
        cd.parserMetrics.NewParserFailures.Add(1)
        cd.parserMetrics.FallbacksTriggered.Add(1)
    }
    
    cd.logger.Warnf("Parser fallback for query '%s': %v", query, err)
}

// Get metrics (for monitoring endpoint)
func (cd *CommandDirector) GetParserMetrics() map[string]int64 {
    if cd.parserMetrics == nil {
        return nil
    }
    
    return map[string]int64{
        "new_parser_attempts":  cd.parserMetrics.NewParserAttempts.Load(),
        "new_parser_successes": cd.parserMetrics.NewParserSuccesses.Load(),
        "new_parser_failures":  cd.parserMetrics.NewParserFailures.Load(),
        "fallbacks_triggered":  cd.parserMetrics.FallbacksTriggered.Load(),
    }
}
```

### Add Debug Logging
```go
func (cd *CommandDirector) parseWithNewParser(query string) (*queryparser.UnifiedSelectQuery, error) {
    startTime := time.Now()
    
    // ... parsing code ...
    
    if err != nil {
        cd.logger.Debugf("New parser failed in %v: %v", time.Since(startTime), err)
        return nil, err
    }
    
    cd.logger.Infof("New parser succeeded in %v (Pattern: %s, Complexity: %d)", 
        time.Since(startTime), stmt.Pattern, stmt.Complexity)
    
    return unifiedQuery, nil
}
```

---

## Step 4: Add Unit Tests (30 minutes)

### Test File: command_director_parser_test.go

```go
package server

import (
    "testing"
    "syndrdb/src/internal/syndrQL"
    "go.uber.org/zap"
)

func TestSelectDocuments_WithNewParser(t *testing.T) {
    // Set environment variable to enable new parser
    os.Setenv("SYNDRDB_USE_NEW_PARSER", "true")
    defer os.Unsetenv("SYNDRDB_USE_NEW_PARSER")
    
    logger := zap.NewNop().Sugar()
    cd := NewCommandDirector(logger, nil) // Pass actual dependencies
    
    query := "SELECT * FROM users WHERE age > 18"
    
    // This should use the new parser
    result, err := cd.SelectDocuments(query)
    
    if err != nil {
        t.Fatalf("Expected no error, got: %v", err)
    }
    
    if result == nil {
        t.Fatal("Expected non-nil result")
    }
    
    // Verify metrics
    metrics := cd.GetParserMetrics()
    if metrics["new_parser_attempts"] != 1 {
        t.Errorf("Expected 1 attempt, got %d", metrics["new_parser_attempts"])
    }
}

func TestSelectDocuments_WithFallback(t *testing.T) {
    // Enable new parser
    os.Setenv("SYNDRDB_USE_NEW_PARSER", "true")
    defer os.Unsetenv("SYNDRDB_USE_NEW_PARSER")
    
    logger := zap.NewNop().Sugar()
    cd := NewCommandDirector(logger, nil)
    
    // Invalid query that new parser can't handle
    query := "SELECT * FROM users WITH INVALID SYNTAX"
    
    // Should fall back to legacy parser (which will also fail, but that's ok)
    _, _ = cd.SelectDocuments(query)
    
    // Verify fallback was triggered
    metrics := cd.GetParserMetrics()
    if metrics["fallbacks_triggered"] != 1 {
        t.Errorf("Expected 1 fallback, got %d", metrics["fallbacks_triggered"])
    }
}

func TestParseWithNewParser_Success(t *testing.T) {
    logger := zap.NewNop().Sugar()
    cd := NewCommandDirector(logger, nil)
    
    query := "SELECT name, age FROM users WHERE age >= 18 LIMIT 10"
    
    unifiedQuery, err := cd.parseWithNewParser(query)
    
    if err != nil {
        t.Fatalf("Expected no error, got: %v", err)
    }
    
    if unifiedQuery.FromBundle != "users" {
        t.Errorf("Expected FromBundle 'users', got '%s'", unifiedQuery.FromBundle)
    }
    
    if unifiedQuery.Limit != 10 {
        t.Errorf("Expected Limit 10, got %d", unifiedQuery.Limit)
    }
}
```

---

## Step 5: Integration Testing (15 minutes)

### Manual Testing Checklist

1. **Start with flag OFF**
   ```bash
   export SYNDRDB_USE_NEW_PARSER=false
   ./bin/server
   ```
   - Run existing test suite
   - Verify all queries work
   - Check logs for "Using legacy parser"

2. **Enable flag**
   ```bash
   export SYNDRDB_USE_NEW_PARSER=true
   ./bin/server
   ```
   - Run same test suite
   - Check logs for "Using new SyndrQL parser"
   - Verify metrics show success/fallback counts

3. **Test fallback mechanism**
   - Send intentionally malformed query
   - Verify fallback to legacy parser
   - Check metrics show fallback triggered

4. **Performance testing**
   ```bash
   go test -bench=BenchmarkSelectDocuments -benchtime=10s
   ```
   - Compare old vs new parser performance
   - Should be similar or better

---

## Step 6: Deployment Strategy (15 minutes)

### Phase A: Deploy with Flag OFF (Week 1-2)
```yaml
parser:
  use_new_syndrql_parser: false
```
- Deploy to production
- Monitor stability
- Collect baseline metrics

### Phase B: Enable for Simple Queries (Week 3-4)
```go
func ShouldUseNewParser(query string) bool {
    if !config.Parser.UseNewSyndrQLParser {
        return false
    }
    
    // Only enable for simple SELECT * queries initially
    if strings.HasPrefix(query, "SELECT * FROM") && !strings.Contains(query, "WHERE") {
        return true
    }
    
    return false
}
```

### Phase C: Gradual Expansion (Week 5-8)
- Week 5: Enable for simple WHERE clauses
- Week 6: Enable for complex WHERE clauses  
- Week 7: Enable for ORDER BY queries
- Week 8: Enable for all queries

### Phase D: Full Migration (Week 9-10)
```yaml
parser:
  use_new_syndrql_parser: true  # All queries use new parser
```

---

## Monitoring and Alerting

### Key Metrics to Watch
```
syndrdb.parser.new_attempts{} 
syndrdb.parser.new_successes{}
syndrdb.parser.new_failures{}
syndrdb.parser.fallbacks{}
syndrdb.parser.latency{}
```

### Alert Thresholds
- **Fallback rate > 5%**: Investigate parser issues
- **Latency increase > 20%**: Performance regression
- **Success rate < 95%**: Parsing errors

---

## Rollback Plan

### Immediate Rollback (< 5 minutes)
```bash
# Set flag to false
export SYNDRDB_USE_NEW_PARSER=false

# Or in config
parser:
  use_new_syndrql_parser: false

# Restart server
./bin/server restart
```

### No code changes required - just flip the flag!

---

## Troubleshooting

### Problem: New parser fails for specific query pattern
**Solution**: Add pattern detection and fallback
```go
if isUnsupportedPattern(query) {
    cd.logger.Debugf("Query pattern not yet supported, using legacy parser")
    return queryparser.ParseUnifiedSelectQuery(query, cd.logger)
}
```

### Problem: Performance regression
**Solution**: Profile and optimize
```bash
go test -bench=. -cpuprofile=cpu.prof
go tool pprof cpu.prof
```

### Problem: High fallback rate
**Solution**: Analyze logs for common failures
```bash
grep "Parser fallback" server.log | cut -d':' -f2 | sort | uniq -c | sort -rn
```

---

## Checklist Before Going to Production

- [ ] Configuration flag implemented
- [ ] SelectDocuments() modified with feature flag
- [ ] Fallback mechanism tested
- [ ] Metrics collection implemented
- [ ] Unit tests added and passing
- [ ] Integration tests passed
- [ ] Manual testing completed
- [ ] Rollback plan documented
- [ ] Monitoring alerts configured
- [ ] Documentation updated

---

## Next Steps After Phase 2

Once Phase 2 is stable and deployed:
1. Monitor metrics for 2 weeks
2. Gradually increase parser usage
3. Collect performance data
4. Proceed to Phase 3 (Expression Evaluator Integration)

---

## Need Help?

- See `integration_example.go` for code examples
- See `PHASE1_INTEGRATION_SUMMARY.md` for technical details
- See `README.md` for overview

**Estimated Total Time**: 2-3 hours implementation + 2 weeks monitoring
