# SyndrDB Allocation Analysis - Phase 2 (Post-ValueToString Optimization)
**Date**: November 19, 2025  
**Baseline**: 342 allocs/op, 73 KB/op  
**Total Allocations**: ~4.8M allocations per 100-iteration benchmark  

---

## Executive Summary

After successfully implementing ValueToString optimization (reducing fmt.Sprintf allocations by 83%), we've identified the next highest-impact optimization targets through comprehensive profiling. Current allocation hotspots reveal three critical areas:

1. **🔴 CRITICAL #1: reflect.copyVal** - 491K allocations (10.21%)
2. **🔴 CRITICAL #2: Query Tokenization** - 409K allocations (8.51% from nextToken alone)
3. **🔴 CRITICAL #3: JSON Serialization** - 627K allocations (13.03%) in hot paths

**Expected total reduction**: 600K-800K allocations (**13-17% reduction**) by addressing top 3 priorities.

---

## Current Allocation Profile

### Top Allocation Sources (by count)

| Function | Allocs Count | % of Total | Category |
|---------|--------------|------------|----------|
| `reflect.copyVal` | 491,769 | 10.21% | **Reflection** |
| `Tokenizer.nextToken` | 409,600 | 8.51% | **Query Parsing** |
| `strings.(*Builder).grow` | 248,147 | 5.15% | String Building |
| `reflect.packEface` | 240,295 | 4.99% | **Reflection** |
| `zap.sliceArrayEncoder.AppendString` | 229,376 | 4.76% | Logging |
| `Tokenizer.readString` | 229,376 | 4.76% | **Query Parsing** |
| `fmt.Sprintf` | 184,269 | 3.83% | String Formatting (✅ 83% reduced!) |

**Reflection total**: ~732K allocations (15.2%)  
**Tokenization total**: ~639K allocations (13.3%)  
**fmt.Sprintf**: Down from 908K to 184K ✅

### Top Allocation Sources (by space)

| Function | Alloc Space | % of Total | Impact |
|---------|-------------|------------|--------|
| `NewBufferPool` | 602.75 MB | 42.48% | One-time pool initialization |
| `NewHashMemTable` | 190.23 MB | 13.41% | Index MemTable (one-time) |
| `bufio.NewWriterSize` | 130.85 MB | 9.22% | **I/O buffers** |
| `JSONSerializer.SerializeBundleMetadata` | 94.19 MB | 6.64% | **Serialization** |
| `MarshalIndent` | 77.17 MB | 5.44% | **JSON serialization** |

---

## 🔴 CRITICAL #1: Reflection Overhead (732K allocations, 15.2%)

### Problem Analysis

**Location**: Throughout bundle operations and value comparisons
- `reflect.copyVal`: 491,769 allocations (10.21%)
- `reflect.packEface`: 240,295 allocations (4.99%)

**Root Cause**: Using `interface{}` everywhere forces runtime type reflection for:
1. Document field value storage
2. Index key comparisons
3. Query value matching
4. JOIN condition evaluation

**Example Hot Path** (from hash_join.go):
```go
// Current: Forces reflection on every comparison
func (h *hashJoin) buildHashTable(docs []map[string]interface{}) {
    for _, doc := range docs {
        keyValue := doc[h.leftKey]  // interface{} -> forces reflection
        keyStr := conversion.ValueToString(keyValue)  // Still needs type assertion
        h.table[keyStr] = append(h.table[keyStr], doc)
    }
}
```

### Recommended Solution: Typed Document Values

**Approach**: Create a discriminated union type for document field values instead of `interface{}`.

```go
// New typed value system
type FieldValue struct {
    Type  FieldType
    // Only one of these is populated based on Type
    Str   string
    Int   int64
    Float float64
    Bool  bool
    Bytes []byte
    Null  bool
}

type FieldType uint8
const (
    TypeString FieldType = iota
    TypeInt
    TypeFloat
    TypeBool
    TypeBytes
    TypeNull
)

// Zero reflection comparison
func (fv FieldValue) String() string {
    switch fv.Type {
    case TypeString:
        return fv.Str
    case TypeInt:
        return strconv.FormatInt(fv.Int, 10)
    case TypeFloat:
        return strconv.FormatFloat(fv.Float, 'f', -1, 64)
    case TypeBool:
        if fv.Bool { return "true" }
        return "false"
    case TypeNull:
        return "NULL"
    default:
        return string(fv.Bytes)
    }
}

// Type-aware equality without reflection
func (fv FieldValue) Equals(other FieldValue) bool {
    if fv.Type != other.Type {
        return false
    }
    switch fv.Type {
    case TypeString:
        return fv.Str == other.Str
    case TypeInt:
        return fv.Int == other.Int
    case TypeFloat:
        return fv.Float == other.Float
    case TypeBool:
        return fv.Bool == other.Bool
    case TypeNull:
        return true
    case TypeBytes:
        return bytes.Equal(fv.Bytes, other.Bytes)
    default:
        return false
    }
}
```

**Migration Strategy**:
1. Create `pkg/common/types/field_value.go` with FieldValue type
2. Update Document type: `type Document map[string]FieldValue` (instead of `interface{}`)
3. Update bundle deserialization to populate FieldValue instead of interface{}
4. Update all comparison operations to use FieldValue.Equals()
5. Update hash key generation to use FieldValue.String()

**Why This is Better Than Alternatives**:

| Alternative | Why Rejected |
|-------------|--------------|
| **Code generation** | Too complex, hard to maintain, doesn't solve runtime deserialization |
| **Generics (Go 1.18+)** | Can't eliminate deserialization reflection, still needs type switches |
| **Pre-compiled queries** | Doesn't help with dynamic queries, large refactor |
| **Keep interface{}** | Current state - 15% of all allocations |

**Expected Impact**: 
- **-400K to -500K allocations** (eliminate most reflect.copyVal and reflect.packEface)
- **-20-25% of comparison overhead**
- **Faster hash table lookups** (no reflection in hash key generation)

---

## 🔴 CRITICAL #2: Query Tokenization (639K allocations, 13.3%)

### Problem Analysis

**Location**: `tokenizer.go`
- `Tokenizer.nextToken`: 409,600 allocations (8.51%)
- `Tokenizer.readString`: 229,376 allocations (4.76%)
- `Tokenizer.readIdentifier`: Included in above

**Root Cause**: Every token allocates:
1. **Token struct** (even though it's created on stack, the Value field is a string slice which allocates)
2. **String slices** for every token value: `tok.Value = t.query[startPos:t.pos]`
3. **Operator string conversions**: `string(t.ch)` allocates

**Example** (from tokenizer.go line 84):
```go
func (t *Tokenizer) nextToken() Token {
    var tok Token  // Stack allocated ✅
    // ...
    case '=':
        tok = t.newToken(TOKEN_ASSIGN, string(t.ch))  // ❌ Allocates string
    // ...
    case TOKEN_IDENT:
        tok.Value = t.query[startPos:t.pos]  // ❌ String slice allocation
}
```

**Profiling Evidence**:
- ~65K allocations flat in nextToken (string() calls for operators)
- ~344K cumulative allocations (includes readString and downstream)

### Recommended Solution: Token Pooling + String Interning

**Phase 1: Token Pool** (Quick win)

```go
var tokenPool = sync.Pool{
    New: func() interface{} {
        return &Token{}
    },
}

func (t *Tokenizer) nextToken() *Token {
    tok := tokenPool.Get().(*Token)
    tok.Reset()  // Clear previous values
    
    // Populate token...
    tok.Type = tokenType
    tok.StartPos = startPos
    tok.EndPos = t.pos
    // Don't copy string yet!
    
    return tok
}

// Defer string extraction until needed
func (tok *Token) Value(query string) string {
    return query[tok.StartPos:tok.EndPos]
}

// Return token to pool after parsing complete
func (t *Tokenizer) releaseTokens(tokens []*Token) {
    for _, tok := range tokens {
        tokenPool.Put(tok)
    }
}
```

**Phase 2: String Interning for Keywords** (Larger impact)

```go
// Pre-allocated keyword strings (compile-time constants)
var keywords = map[string]string{
    "SELECT": "SELECT",
    "FROM": "FROM",
    "WHERE": "WHERE",
    "JOIN": "JOIN",
    // ... all SQL keywords
}

func (t *Tokenizer) readIdentifier() string {
    startPos := t.pos - 1
    for isLetter(t.ch) || isDigit(t.ch) || t.ch == '_' {
        t.readChar()
    }
    
    ident := t.query[startPos:t.pos]
    
    // Return interned string for keywords (zero allocation)
    if interned, ok := keywords[strings.ToUpper(ident)]; ok {
        return interned
    }
    
    // Only allocate for non-keywords (table names, field names)
    return ident
}
```

**Phase 3: Operator String Table** (Small but clean)

```go
// Pre-allocated operator strings
var operators = [256]string{
    '=': "=",
    '!': "!",
    '<': "<",
    '>': ">",
    '+': "+",
    '-': "-",
    // ... fill in ASCII table
}

func (t *Tokenizer) newToken(tokenType TokenType, value string) *Token {
    tok := tokenPool.Get().(*Token)
    tok.Type = tokenType
    
    // Use pre-allocated string for single-char operators
    if len(value) == 1 && value[0] < 128 {
        tok.Value = operators[value[0]]  // Zero allocation!
    } else {
        tok.Value = value
    }
    
    return tok
}
```

**Why This is Better Than Alternatives**:

| Alternative | Why Rejected |
|-------------|--------------|
| **Scanner interface (like text/scanner)** | Still allocates Token.Value, no real gain |
| **Single large buffer** | Complex offset tracking, error-prone, limited benefit |
| **Streaming parser** | Major rewrite, breaks existing AST structure |
| **bytecode compilation** | Massive complexity, only helps repeated queries |

**Expected Impact**:
- **Phase 1 (Token pool)**: -65K allocations (operator strings)
- **Phase 2 (Keyword interning)**: -200K allocations (~50% of identifier reads)
- **Phase 3 (Operator table)**: -32K allocations (single-char operators)
- **Total**: **-300K allocations (47% reduction in tokenizer)**

---

## 🔴 CRITICAL #3: JSON Serialization in Hot Paths (627K allocations, 13%)

### Problem Analysis

**Location**: `format.go:JSONSerializer.SerializeBundleMetadata()`
- Total allocations: 627,208 (13.03%)
- Space: 94.19 MB (6.64%)

**Called From**:
- `AppendDocumentToBundleFile()` - **on every document insert**
- `UpdateBundleFile()` - on bundle metadata updates
- `FlushMetadataUpdates()` - periodic flushing

**Root Cause**:
```go
func (j *JSONSerializer) SerializeBundleMetadata(bundle *models.Bundle) ([]byte, error) {
    metadata := map[string]interface{}{  // ❌ Allocation #1: Map creation
        "BundleID": bundle.BundleID,
        "Name": bundle.Name,
        "Fields": bundle.Fields,  // ❌ Allocations #2-N: Interface boxing
        // ... 12 more fields
    }
    return json.MarshalIndent(metadata, "", "  ")  // ❌ 100+ allocations internally
}
```

**Why It's in Hot Path**:
Every `AddDocumentToBundle` call triggers metadata serialization even though metadata rarely changes.

### Recommended Solution: Binary Serialization + Metadata Caching

**Phase 1: Switch to Binary (Immediate Win)**

```go
// BinarySerializer already exists! Just use it for metadata
type BundleService struct {
    // ...
    metadataSerializer storage.BundleSerializer  // Separate from data serializer
}

func NewBundleService(...) *BundleService {
    return &BundleService{
        // ...
        // Use binary for metadata (fast), JSON for data (human-readable if needed)
        metadataSerializer: format.NewBinarySerializer(),
    }
}

func (bs *BundleService) saveMetadata(bundle *models.Bundle) error {
    data, err := bs.metadataSerializer.SerializeBundleMetadata(bundle)
    // ... write to disk
}
```

**Binary vs JSON Performance** (from existing code):
- BSON: ~100-150K allocations for same data
- JSON: ~630K allocations
- **Reduction**: **~480K allocations (76% improvement)**

**Phase 2: Metadata Change Detection** (Eliminate Unnecessary Serialization)

```go
type Bundle struct {
    // ... existing fields
    metadataHash  uint64  // Hash of metadata fields
    metadataDirty bool    // Flag indicating metadata changed
}

func (bs *BundleService) AddDocumentToBundle(bundleID string, doc *models.Document) error {
    bundle := bs.GetBundle(bundleID)
    
    // Only serialize metadata if it actually changed
    if bundle.metadataDirty {
        if err := bs.saveMetadata(bundle); err != nil {
            return err
        }
        bundle.metadataDirty = false
    }
    
    // Continue with document insert...
}

func (b *Bundle) UpdateField(field *models.Field) {
    b.Fields = append(b.Fields, field)
    b.metadataDirty = true  // Mark for serialization
    b.metadataHash = 0      // Invalidate hash
}
```

**Phase 3: Metadata Caching** (For Read-Heavy Workloads)

```go
type BundleService struct {
    // ...
    metadataCache sync.Map  // map[string][]byte - bundleID -> serialized metadata
}

func (bs *BundleService) getSerializedMetadata(bundle *models.Bundle) ([]byte, error) {
    // Check cache first
    if cached, ok := bs.metadataCache.Load(bundle.BundleID); ok {
        if !bundle.metadataDirty {
            return cached.([]byte), nil  // Cache HIT!
        }
    }
    
    // Cache MISS or dirty - serialize
    data, err := bs.metadataSerializer.SerializeBundleMetadata(bundle)
    if err != nil {
        return nil, err
    }
    
    // Update cache
    bs.metadataCache.Store(bundle.BundleID, data)
    return data, nil
}
```

**Why This is Better Than Alternatives**:

| Alternative | Why Rejected |
|-------------|--------------|
| **Custom JSON encoder** | Still slower than BSON, complex to maintain |
| **Protocol Buffers** | External dependency, compilation step, overkill |
| **MessagePack** | Similar to BSON but not already in codebase |
| **Keep JSON** | 630K allocations, 94MB wasted |

**Expected Impact**:
- **Phase 1 (Binary serialization)**: -480K allocations (76% reduction)
- **Phase 2 (Change detection)**: -90% of remaining metadata writes (only on metadata changes)
- **Phase 3 (Caching)**: Negligible allocation impact, big read performance win
- **Total**: **-550K allocations from JSON serialization**

---

## 🟠 MEDIUM PRIORITY #4: I/O Buffer Allocations (130.85 MB)

### Problem

`bufio.NewWriterSize` allocates 130.85 MB (9.22%) for write buffers.

### Recommendation

**Use sync.Pool for bufio.Writer reuse**:

```go
var writerPool = sync.Pool{
    New: func() interface{} {
        return bufio.NewWriterSize(nil, 64*1024)
    },
}

func (bs *BundleStorageEngine) writeWithPooledBuffer(w io.Writer, data []byte) error {
    writer := writerPool.Get().(*bufio.Writer)
    defer writerPool.Put(writer)
    
    writer.Reset(w)
    _, err := writer.Write(data)
    if err != nil {
        return err
    }
    return writer.Flush()
}
```

**Expected Impact**: -120MB of allocations (~80% of bufio overhead)

---

## 🟠 MEDIUM PRIORITY #5: Logging Allocations (229K allocations, 4.76%)

### Problem

`zap.sliceArrayEncoder.AppendString`: 229,376 allocations from structured logging.

### Recommendation

**Reduce log verbosity in hot paths**:

```go
// Current: Logs every document add
logger.Info("Trying to add document", zap.String("bundle", bundleName))

// Better: Only log at Debug level in production
if logger.Core().Enabled(zapcore.DebugLevel) {
    logger.Debug("Adding document", zap.String("bundle", bundleName))
}

// Or use sampling
sampledLogger := logger.WithOptions(zap.WrapCore(func(core zapcore.Core) zapcore.Core {
    return zapcore.NewSamplerWithOptions(core, time.Second, 100, 10)
}))
```

**Expected Impact**: -150K to -200K allocations (depends on log level configuration)

---

## Implementation Priority & Roadmap

### Phase 1 (Week 1-2): Low-Hanging Fruit
**Target**: -550K allocations (11.4%)

1. ✅ **JSON → Binary Serialization** (2-3 days)
   - Switch metadata serializer to BSON
   - Add change detection flag
   - **Impact**: -480K allocations

2. ✅ **Tokenizer - Keyword Interning** (1-2 days)
   - Create keyword string table
   - Update readIdentifier()
   - **Impact**: -200K allocations

3. ✅ **bufio.Writer Pooling** (1 day)
   - Add sync.Pool for writers
   - Update storage engine
   - **Impact**: -120MB space

**Total Phase 1**: **-550K allocations, -120MB space, ~85% success probability**

### Phase 2 (Week 3-4): Medium Complexity
**Target**: -300K allocations (6.2%)

4. ⏳ **Token Pooling** (3-4 days)
   - Implement sync.Pool for tokens
   - Update parser to release tokens
   - Add operator string table
   - **Impact**: -100K allocations

5. ⏳ **Reduce Logging in Hot Paths** (2 days)
   - Add log level checks
   - Implement sampling logger
   - **Impact**: -150K allocations

**Total Phase 2**: **-250K allocations, ~70% success probability**

### Phase 3 (Week 5-8): High Complexity, High Impact
**Target**: -400K allocations (8.3%)

6. 🔄 **Typed Document Values** (2-3 weeks)
   - Create FieldValue type
   - Migrate Document type
   - Update all comparisons
   - Update deserialization
   - **Impact**: -400K allocations
   - **Risk**: High (large refactor, extensive testing needed)

**Total Phase 3**: **-400K allocations, ~60% success probability (testing/debugging)**

---

## Total Expected Impact Summary

| Metric | Current | After Phase 1 | After Phase 2 | After Phase 3 |
|--------|---------|---------------|---------------|---------------|
| Allocs/op | 342 | ~290-310 | ~260-280 | **~220-250** |
| Reduction | - | -10-15% | -18-24% | **-27-36%** |
| Total Allocs | 4.8M | 4.25M | 4.05M | **3.95M** |
| Memory | 73 KB | ~65 KB | ~60 KB | **~55 KB** |

**Conservative estimate**: **-600K to -800K allocations** (12-17% reduction) from Phases 1-2  
**Aggressive estimate**: **-1.0M to -1.2M allocations** (21-25% reduction) including Phase 3

---

## Notes on Implementation

### Success Criteria Per Phase

**Phase 1**:
- Binary serialization: Benchmark shows <150K allocations in JSONSerializer functions
- Tokenizer interning: Token allocations drop by 150K+
- bufio pooling: Memory profile shows <20MB in bufio allocations

**Phase 2**:
- Token pooling: nextToken allocations <30K
- Logging: Hot path log allocations <50K

**Phase 3**:
- Typed values: reflect.copyVal and reflect.packEface combined <100K

### Risk Mitigation

1. **Binary Serialization Risk**: BSON serializer already exists and tested
2. **Tokenizer Risk**: Can implement incrementally (interning first, then pooling)
3. **Typed Values Risk**: Highest risk - create feature branch, extensive testing
   - Consider A/B testing with interface{} fallback
   - May need gradual migration bundle-by-bundle

### Benchmarking Strategy

```bash
# Before each phase
go test -bench=BenchmarkSelect_AllFields_Small -benchmem -benchtime=100x \
  -memprofile=before_phase_N.prof ./src/cmd/tests/syndrQL/...

# After each phase
go test -bench=BenchmarkSelect_AllFields_Small -benchmem -benchtime=100x \
  -memprofile=after_phase_N.prof ./src/cmd/tests/syndrQL/...

# Compare
go tool pprof -base before_phase_N.prof after_phase_N.prof
```

---

## Conclusion

The ValueToString optimization successfully reduced fmt.Sprintf allocations by 83% (908K → 184K). The next optimization wave focuses on three high-impact areas:

1. **Reflection overhead** (15.2%) - Largest impact but highest complexity
2. **Query tokenization** (13.3%) - Medium impact, medium complexity
3. **JSON serialization** (13%) - High impact, low complexity ✅ **START HERE**

**Recommended approach**: Start with JSON→Binary (Phase 1, item 1) for immediate 10% gain with minimal risk, then progressively tackle tokenizer and reflection optimizations.

**Realistic 12-week goal**: Reduce from **342 allocs/op → 220-250 allocs/op** (27-36% reduction), bringing total reduction from original baseline (967 allocs) to **~75-80% total reduction**.
