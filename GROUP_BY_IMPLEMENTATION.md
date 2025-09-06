# GROUP BY Implementation in SyndrDB

## Overview

This document describes the comprehensive GROUP BY implementation in SyndrDB, following PostgreSQL's algorithms and optimization strategies. The implementation provides SQL-like aggregation capabilities for document-based data with efficient execution strategies.

## Implementation Components

### 1. Query Parser (`groupby_parser.go`)

**Location**: `src/internal/query/queryparser/groupby_parser.go`

**Purpose**: Parses GROUP BY queries and validates syntax

**Key Features**:
- SQL-like GROUP BY syntax parsing
- Aggregate function parsing (COUNT, SUM, AVG, MIN, MAX)
- HAVING clause support
- ORDER BY integration
- Query validation (ensuring non-aggregated fields are grouped)
- Execution strategy selection

**Sample Syntax**:
```sql
SELECT field1, COUNT(*), SUM(field2) 
FROM "BundleName" 
GROUP BY field1 
HAVING COUNT(*) > 5 
ORDER BY COUNT(*) DESC
```

### 2. Execution Engine (`groupby_executor.go`)

**Location**: `src/internal/query/executor/groupby_executor.go`

**Purpose**: Executes GROUP BY queries using PostgreSQL-compatible strategies

**Execution Strategies**:

#### Hash Aggregate Strategy
- **Use Case**: Large datasets with sufficient memory
- **Algorithm**: In-memory hash table for grouping
- **Performance**: O(n) average case, O(n²) worst case
- **Memory**: High memory usage, configurable limits

#### Sort + GroupAggregate Strategy  
- **Use Case**: Memory-constrained environments or pre-sorted data
- **Algorithm**: Sort data first, then sequential grouping
- **Performance**: O(n log n) for sorting, O(n) for grouping
- **Memory**: Lower memory footprint

### 3. Integration (`command_director.go`)

**Location**: `src/internal/server/command_director.go`

**Purpose**: Integrates GROUP BY functionality into SyndrDB query processing

**Integration Points**:
- Query detection and routing
- Bundle access and document retrieval
- Result formatting and response generation

## Supported Aggregate Functions

### COUNT Functions
```sql
COUNT(*)        -- Count all rows in group
COUNT(field)    -- Count non-null values in field
```

### Numeric Aggregates
```sql
SUM(field)      -- Sum of numeric values
AVG(field)      -- Average of numeric values
MIN(field)      -- Minimum value
MAX(field)      -- Maximum value
```

### Advanced Features
```sql
-- Aggregate aliases
SELECT category, COUNT(*) as total_count FROM "Products" GROUP BY category

-- Multiple aggregates
SELECT region, COUNT(*), SUM(sales), AVG(sales) FROM "Revenue" GROUP BY region

-- Multiple GROUP BY fields
SELECT region, category, COUNT(*) FROM "Sales" GROUP BY region, category
```

## Query Examples

### Basic Grouping
```sql
-- Count products by category
SELECT category, COUNT(*) FROM "Products" GROUP BY category

-- Sales totals by region
SELECT region, SUM(amount) FROM "Sales" GROUP BY region
```

### Complex Queries
```sql
-- Department statistics with filtering
SELECT dept, COUNT(*) as emp_count, AVG(salary) as avg_salary 
FROM "Employees" 
GROUP BY dept 
HAVING COUNT(*) > 5

-- Top categories by revenue
SELECT category, SUM(price * quantity) as revenue 
FROM "Orders" 
GROUP BY category 
ORDER BY revenue DESC
```

### Multi-Field Grouping
```sql
-- Sales analysis by region and product category
SELECT region, category, COUNT(*), SUM(amount), AVG(amount)
FROM "Sales" 
GROUP BY region, category 
ORDER BY region, SUM(amount) DESC
```

## Performance Characteristics

### Strategy Selection Algorithm

The system automatically selects the optimal execution strategy based on:

1. **Data Size**: Large datasets favor Hash Aggregate
2. **Memory Availability**: Memory constraints favor Sort + GroupAggregate  
3. **Data Distribution**: High cardinality favors Hash Aggregate
4. **Sort Order**: Pre-sorted data favors Sort + GroupAggregate

### Memory Management

```go
// Configurable memory limits
type GroupByExecutor struct {
    workMem  int    // Memory limit in MB (default: 64MB)
    tempDir  string // Temporary file directory for spilling
}
```

### Performance Optimizations

1. **Memory-Efficient Aggregation**: Incremental calculation of aggregates
2. **Hash Table Optimization**: Efficient grouping key generation
3. **Sort Optimization**: Leverages existing document ordering when possible
4. **Spill-to-Disk**: Automatic handling of memory pressure (future enhancement)

## Error Handling

### Parser Validation
- Non-aggregated fields must appear in GROUP BY clause
- Aggregate functions validated for proper syntax
- Bundle name validation and existence checking

### Execution Validation
- Document field existence checking
- Numeric conversion for mathematical aggregates
- Graceful handling of null/missing values

### Example Error Cases
```sql
-- Error: 'name' not in GROUP BY
SELECT category, name, COUNT(*) FROM "Products" GROUP BY category

-- Error: Invalid aggregate function
SELECT category, INVALID(price) FROM "Products" GROUP BY category
```

## Testing

### Test Suite (`test_groupby_functionality.go`)

**Location**: `src/cmd/tests/test_groupby_functionality.go`

**Test Categories**:
1. **Parser Tests**: Query syntax validation
2. **Execution Tests**: Strategy testing and comparison
3. **Aggregate Tests**: All aggregate function validation
4. **Complex Query Tests**: HAVING, ORDER BY, multi-field grouping

### Running Tests

```bash
# Build and run test suite
./test_groupby_demo.sh

# Or run directly
go build -o ./bin/tests/test_runner ./src/cmd/tests/
./bin/tests/test_runner
```

## Architecture Integration

### Query Processing Flow

1. **Detection**: Command director detects GROUP BY keywords
2. **Parsing**: GROUP BY parser validates and structures query
3. **Strategy Selection**: Automatic or manual strategy selection
4. **Execution**: Strategy-specific execution with result aggregation
5. **Post-Processing**: HAVING clause filtering and ORDER BY sorting
6. **Response**: Formatted results returned to client

### Data Flow

```
Query String → Parser → Validator → Strategy Selector → Executor → Post-Processor → Results
```

## Future Enhancements

### Planned Features
1. **Grouping Sets**: Support for ROLLUP, CUBE, and GROUPING SETS
2. **Window Functions**: ROW_NUMBER(), RANK(), DENSE_RANK()
3. **Parallel Processing**: Multi-threaded execution for large datasets
4. **Index Integration**: Leveraging indexes for faster grouping
5. **Spill-to-Disk**: Automatic memory pressure handling

### Optimization Opportunities
1. **Vectorized Aggregation**: SIMD optimizations for numeric operations
2. **Columnar Processing**: Efficient processing of document fields
3. **Compressed Aggregation**: Memory-efficient intermediate results
4. **Adaptive Strategies**: Dynamic strategy switching based on runtime characteristics

## Compatibility

### PostgreSQL Compatibility
- Standard SQL GROUP BY syntax
- Common aggregate functions
- HAVING clause semantics
- ORDER BY integration

### SyndrDB Extensions
- Document-based field access
- Bundle-based data organization
- Flexible schema handling
- JSON-like data processing

## Configuration

### Environment Variables
```bash
SYNDRDB_WORK_MEM=64        # Work memory in MB
SYNDRDB_TEMP_DIR=./temp    # Temporary file directory
SYNDRDB_STRATEGY=auto      # Default execution strategy
```

### Query Hints (Future)
```sql
-- Force specific strategy
SELECT /*+ HASH_AGGREGATE */ category, COUNT(*) FROM "Products" GROUP BY category
SELECT /*+ SORT_AGGREGATE */ category, COUNT(*) FROM "Products" GROUP BY category
```

This comprehensive GROUP BY implementation brings powerful SQL-like aggregation capabilities to SyndrDB while maintaining the flexibility and performance characteristics of the document-based architecture.
