# Transaction System

## Overview

SyndrDB provides a robust transaction system that ensures data consistency and integrity through ACID-compliant operations. Transactions allow you to group multiple operations together, ensuring that either all changes are applied (commit) or none are applied (rollback).

## Table of Contents

- [Key Features](#key-features)
- [Transaction Commands](#transaction-commands)
- [Architecture](#architecture)
- [Usage Examples](#usage-examples)
- [Best Practices](#best-practices)
- [Performance Characteristics](#performance-characteristics)
- [Error Handling](#error-handling)
- [Limitations](#limitations)

## Key Features

### ACID Compliance

- **Atomicity**: All operations within a transaction succeed or fail as a unit
- **Consistency**: Data remains in a valid state before and after transactions
- **Isolation**: Transactions don't interfere with each other
- **Durability**: Committed changes persist through system failures via Write-Ahead Logging (WAL)

### Buffer-Aware Rollback

The transaction system intelligently handles both buffered and flushed documents:

- **Buffered Documents**: Discarded in-memory without disk I/O during rollback
- **Flushed Documents**: Automatically detected and deleted from disk during rollback
- **Mixed Scenarios**: Seamlessly handles transactions where some documents are buffered and others are flushed

### Write-Ahead Logging (WAL)

All transaction operations are logged to the WAL before being applied:

- **Crash Recovery**: System can recover to a consistent state after unexpected failures
- **LSN Tracking**: Log Sequence Numbers ensure ordered replay of operations
- **Automatic Cleanup**: WAL entries are managed automatically by the system

## Transaction Commands

### BEGIN TRANSACTION

Starts a new transaction session.

**Syntax:**
```sql
BEGIN TRANSACTION;
```

**Returns:**
- Success message with unique transaction ID
- Transaction ID format: `TX_<timestamp>_<random>`

**Example:**
```sql
BEGIN TRANSACTION;
-- Output: Transaction started with ID: TX_1765503068569_abc123
```

### COMMIT

Commits all changes made within the current transaction, making them permanent.

**Syntax:**
```sql
COMMIT;
```

**Behavior:**
- All buffered documents are flushed to disk
- WAL entries are persisted
- Indexes are updated
- Transaction ID is cleared

**Example:**
```sql
BEGIN TRANSACTION;
INSERT INTO Users ({"UserID": "user1", "Name": "Alice"});
INSERT INTO Users ({"UserID": "user2", "Name": "Bob"});
COMMIT;
-- Output: Transaction committed successfully. Changes persisted to disk.
```

### ROLLBACK

Rolls back all changes made within the current transaction, discarding them.

**Syntax:**
```sql
ROLLBACK;
```

**Behavior:**
1. Collects all document IDs associated with the transaction
2. Flushes write buffers to ensure all transaction documents are on disk
3. Deletes flushed documents from storage
4. Discards buffered documents from memory
5. Cleans up internal tracking structures
6. Clears transaction ID

**Example:**
```sql
BEGIN TRANSACTION;
INSERT INTO Users ({"UserID": "user3", "Name": "Charlie"});
INSERT INTO Users ({"UserID": "user4", "Name": "Diana"});
ROLLBACK;
-- Output: Transaction rolled back successfully. All changes have been discarded.
```

## Architecture

### Transaction Lifecycle

```
┌─────────────────┐
│  BEGIN TRANSACTION │
└────────┬────────┘
         │
         v
┌─────────────────┐
│   Active Transaction  │
│   (TX_ID assigned)    │
└────────┬────────┘
         │
         v
    ┌────┴────┐
    │ Operations │
    │  INSERT   │
    │  UPDATE   │
    │  DELETE   │
    └────┬────┘
         │
         v
    ┌────┴────┐
    │ Decision │
    └────┬────┘
         │
    ┌────┴────────┐
    │             │
    v             v
┌────────┐    ┌──────────┐
│ COMMIT │    │ ROLLBACK │
└────┬───┘    └────┬─────┘
     │             │
     v             v
┌─────────┐   ┌──────────┐
│ Persist │   │ Discard  │
│ Changes │   │ Changes  │
└─────────┘   └──────────┘
```

### Document Tracking

Each document inserted during a transaction is tagged with:

- **Transaction ID**: Unique identifier for the transaction
- **Discard Flag**: Internal flag for rollback tracking
- **Buffer State**: Whether document is buffered or flushed

### Write Buffer Integration

The transaction system integrates seamlessly with SyndrDB's write buffer:

```
INSERT Operation
       │
       v
┌──────────────┐
│ Write Buffer │ ← Tagged with TX_ID
└──────┬───────┘
       │
       v
  Auto-Flush?
   (Size/Time)
       │
   ┌───┴───┐
   │       │
   v       v
  Yes     No
   │       │
   v       └─→ Stays in buffer
  Disk
   │
   └─→ Marked as flushed
```

### Rollback Process

```
ROLLBACK Command
       │
       v
┌──────────────────┐
│ Collect Doc IDs  │ ← Query by TX_ID
│ from Transaction │
└────────┬─────────┘
         │
         v
┌──────────────────┐
│  Flush Buffers   │ ← Ensure all docs on disk
└────────┬─────────┘
         │
         v
┌──────────────────┐
│ Delete Documents │ ← Physical deletion
│  from Storage    │
└────────┬─────────┘
         │
         v
┌──────────────────┐
│  Clear Internal  │ ← Cleanup tracking
│   Structures     │
└────────┬─────────┘
         │
         v
┌──────────────────┐
│  Clear TX_ID     │ ← End transaction
└──────────────────┘
```

## Usage Examples

### Example 1: Basic Transaction

```sql
-- Start a transaction
BEGIN TRANSACTION;

-- Insert multiple users
INSERT INTO Users ({"UserID": "u001", "Name": "Alice", "Email": "alice@example.com"});
INSERT INTO Users ({"UserID": "u002", "Name": "Bob", "Email": "bob@example.com"});
INSERT INTO Users ({"UserID": "u003", "Name": "Charlie", "Email": "charlie@example.com"});

-- Commit the changes
COMMIT;
```

### Example 2: Transaction with Rollback

```sql
-- Start a transaction
BEGIN TRANSACTION;

-- Insert some data
INSERT INTO Orders ({"OrderID": "o001", "UserID": "u001", "Amount": 100.00});
INSERT INTO Orders ({"OrderID": "o002", "UserID": "u002", "Amount": 250.00});

-- Decide to cancel - rollback all changes
ROLLBACK;

-- All inserts are discarded, no data persisted
```

### Example 3: Mixed Operations

```sql
-- Start transaction
BEGIN TRANSACTION;

-- Insert many documents (some will auto-flush)
INSERT INTO Products ({"ProductID": "p001", "Name": "Widget A", "Price": 10.99});
INSERT INTO Products ({"ProductID": "p002", "Name": "Widget B", "Price": 15.99});
-- ... insert 50+ more products ...
INSERT INTO Products ({"ProductID": "p052", "Name": "Widget Z", "Price": 25.99});

-- Some documents are buffered, some flushed to disk
-- Rollback handles both correctly
ROLLBACK;
```

### Example 4: Error Recovery

```sql
BEGIN TRANSACTION;

-- Insert valid data
INSERT INTO Accounts ({"AccountID": "a001", "Balance": 1000.00});

-- Attempt invalid operation (constraint violation)
INSERT INTO Accounts ({"AccountID": "a001", "Balance": 500.00});  -- Duplicate ID!

-- Error occurred, rollback to maintain consistency
ROLLBACK;
```

### Example 5: Batch Import

```sql
BEGIN TRANSACTION;

-- Import large dataset
INSERT INTO Inventory ({"SKU": "sku001", "Quantity": 100, "Location": "A1"});
INSERT INTO Inventory ({"SKU": "sku002", "Quantity": 250, "Location": "A2"});
INSERT INTO Inventory ({"SKU": "sku003", "Quantity": 75, "Location": "B1"});
-- ... thousands more rows ...

-- Verify data looks correct, then commit
COMMIT;
```

## Best Practices

### 1. Keep Transactions Short

```sql
-- GOOD: Short, focused transaction
BEGIN TRANSACTION;
INSERT INTO Users ({"UserID": "u001", "Name": "Alice"});
UPDATE Users SET Status = "active" WHERE UserID = "u001";
COMMIT;

-- AVOID: Long-running transaction
BEGIN TRANSACTION;
-- Many operations over extended time
-- Risk of buffer overflow, locking issues
```

### 2. Use Transactions for Related Operations

```sql
-- GOOD: Group related operations
BEGIN TRANSACTION;
INSERT INTO Orders ({"OrderID": "o001", "UserID": "u001", "Total": 100});
INSERT INTO OrderItems ({"OrderID": "o001", "ProductID": "p001", "Qty": 2});
INSERT INTO OrderItems ({"OrderID": "o001", "ProductID": "p002", "Qty": 1});
COMMIT;
```

### 3. Handle Errors Appropriately

```sql
BEGIN TRANSACTION;

-- Check for errors after each operation
INSERT INTO Accounts ({"AccountID": "a001", "Balance": 1000});

-- If any error occurs, rollback immediately
ROLLBACK;

-- Otherwise, commit
COMMIT;
```

### 4. Don't Nest Transactions

```sql
-- NOT SUPPORTED: Nested transactions
BEGIN TRANSACTION;
    BEGIN TRANSACTION;  -- ❌ Will error
    COMMIT;
COMMIT;

-- Use single transaction scope
BEGIN TRANSACTION;
-- All operations
COMMIT;
```

### 5. Monitor Transaction Size

For large batch operations:
- Consider breaking into multiple transactions
- Monitor write buffer usage
- Each transaction handles ~100-1000 documents optimally

```sql
-- For 10,000 documents, use batches:
BEGIN TRANSACTION;
-- Insert 1000 documents
COMMIT;

BEGIN TRANSACTION;
-- Insert next 1000 documents
COMMIT;
-- Repeat...
```

## Performance Characteristics

### Commit Performance

- **Buffered Documents**: ~84ms for 100 documents (typical)
- **Write Operations**: Includes buffer flush, index updates, WAL sync
- **Scalability**: Linear with document count

### Rollback Performance

- **Buffered Only**: Near-instant (no disk I/O)
- **Flushed Documents**: ~421ms for 100 documents (typical)
- **Ratio**: Rollback is ~5x slower than commit (due to deletion operations)
- **Why acceptable**: Rollback includes WAL replay, physical deletion, index cleanup

### Memory Usage

- **Per Transaction**: ~1-5KB overhead for tracking
- **Per Document**: Minimal (just TX_ID tag)
- **Write Buffer**: Standard buffer rules apply (32KB default)

### Benchmarks

From real-world testing:

| Operation | 100 Documents | 1,000 Documents | Notes |
|-----------|--------------|-----------------|-------|
| INSERT + COMMIT | 84ms | ~840ms | Linear scaling |
| INSERT + ROLLBACK (buffered) | <1ms | <10ms | In-memory only |
| INSERT + ROLLBACK (flushed) | 421ms | ~4,200ms | Includes deletion |

## Error Handling

### Common Errors

#### 1. No Active Transaction

```sql
COMMIT;
-- Error: no active transaction to commit
```

**Solution**: Always use `BEGIN TRANSACTION` first.

#### 2. Transaction Already Active

```sql
BEGIN TRANSACTION;
BEGIN TRANSACTION;
-- Error: transaction already in progress
```

**Solution**: Commit or rollback existing transaction first.

#### 3. Transaction Timeout

```sql
BEGIN TRANSACTION;
-- Wait too long...
-- System may auto-rollback after timeout
```

**Solution**: Complete transactions promptly.

#### 4. Write Buffer Overflow

```sql
BEGIN TRANSACTION;
-- Insert 100,000 documents rapidly
-- May exceed write buffer capacity
```

**Solution**: Break into smaller transactions or increase buffer size.

### Error Recovery

When errors occur within a transaction:

1. **Automatic Rollback**: Some errors trigger automatic rollback
2. **Manual Rollback**: Always safe to call `ROLLBACK` after any error
3. **WAL Protection**: All operations are logged for recovery

```sql
BEGIN TRANSACTION;

-- Error occurs here
INSERT INTO Users ({"UserID": NULL});  -- NULL constraint violation

-- Explicitly rollback to clean state
ROLLBACK;

-- Start fresh transaction
BEGIN TRANSACTION;
-- Correct operation
INSERT INTO Users ({"UserID": "u001", "Name": "Alice"});
COMMIT;
```

## Limitations

### Current Limitations

1. **No Nested Transactions**: Each session supports one active transaction at a time
2. **No Savepoints**: Cannot create intermediate rollback points within a transaction
3. **Single Session Scope**: Transactions are session-specific, not cross-session
4. **Buffer Constraints**: Very large transactions may trigger multiple auto-flushes
5. **No Distributed Transactions**: Currently single-node only

### Unsupported Operations

The following are not yet supported within transactions:

- ❌ DDL operations (CREATE BUNDLE, DROP BUNDLE, etc.)
- ❌ Schema modifications during active transactions
- ❌ Cross-database transactions
- ❌ Two-phase commit (2PC)

### Future Enhancements

Planned improvements:

- **Savepoints**: `SAVEPOINT name` and `ROLLBACK TO name`
- **Nested Transactions**: Support for sub-transactions
- **Read Isolation Levels**: Configurable isolation (READ COMMITTED, SERIALIZABLE, etc.)
- **Distributed Transactions**: Multi-node transaction coordination
- **Transaction Timeouts**: Configurable automatic rollback after timeout
- **Transaction Logs**: Query transaction history and audit trail

## Advanced Topics

### Transaction Isolation

Current implementation provides **READ COMMITTED** isolation:

- Transactions see only committed data from other transactions
- Within a transaction, all operations see consistent snapshot
- No phantom reads within same transaction scope

### WAL Integration

Every transactional operation is logged:

```
BEGIN TRANSACTION
    ↓
[WAL] BEGIN TX_123
    ↓
INSERT operation
    ↓
[WAL] INSERT TX_123 doc_001
    ↓
COMMIT
    ↓
[WAL] COMMIT TX_123
```

In case of crash:
1. System reads WAL on restart
2. Incomplete transactions are rolled back
3. Committed transactions are replayed if needed

### Monitoring Transactions

Check active transaction:

```sql
-- In application logs, look for:
-- "Transaction started with ID: TX_..."
-- "Transaction committed successfully"
-- "Transaction rolled back successfully"
```

### Transaction Best Practices Summary

✅ **DO:**
- Use transactions for related operations
- Keep transactions short
- Handle errors with ROLLBACK
- Monitor transaction performance
- Break large batches into multiple transactions

❌ **DON'T:**
- Leave transactions open indefinitely
- Nest transactions
- Mix DDL with DML in transactions
- Ignore error messages
- Use transactions for single operations (unnecessary overhead)

## Examples by Use Case

### Use Case 1: E-commerce Order Processing

```sql
BEGIN TRANSACTION;

-- Create order
INSERT INTO Orders ({
    "OrderID": "ord_001",
    "UserID": "user_123",
    "Total": 299.97,
    "Status": "pending",
    "CreatedAt": "2025-12-11T10:00:00Z"
});

-- Add order items
INSERT INTO OrderItems ({
    "OrderItemID": "item_001",
    "OrderID": "ord_001",
    "ProductID": "prod_001",
    "Quantity": 2,
    "Price": 99.99
});

INSERT INTO OrderItems ({
    "OrderItemID": "item_002",
    "OrderID": "ord_001",
    "ProductID": "prod_002",
    "Quantity": 1,
    "Price": 99.99
});

-- Update inventory
UPDATE Inventory SET Quantity = Quantity - 2 WHERE ProductID = "prod_001";
UPDATE Inventory SET Quantity = Quantity - 1 WHERE ProductID = "prod_002";

-- All operations successful - commit
COMMIT;
```

### Use Case 2: User Registration with Profile

```sql
BEGIN TRANSACTION;

-- Create user account
INSERT INTO Users ({
    "UserID": "u_12345",
    "Email": "alice@example.com",
    "PasswordHash": "hashed_password",
    "Status": "active",
    "CreatedAt": "2025-12-11T10:00:00Z"
});

-- Create user profile
INSERT INTO UserProfiles ({
    "ProfileID": "p_12345",
    "UserID": "u_12345",
    "FirstName": "Alice",
    "LastName": "Smith",
    "Phone": "+1-555-0123"
});

-- Assign default role
INSERT INTO UserRoles ({
    "UserID": "u_12345",
    "RoleID": "basic_user"
});

COMMIT;
```

### Use Case 3: Bulk Data Import with Validation

```sql
BEGIN TRANSACTION;

-- Import batch of products
INSERT INTO Products ({"ProductID": "p001", "Name": "Product 1", "Price": 10.99});
INSERT INTO Products ({"ProductID": "p002", "Name": "Product 2", "Price": 15.99});
INSERT INTO Products ({"ProductID": "p003", "Name": "Product 3", "Price": 20.99});
-- ... more inserts ...

-- Verify count
SELECT COUNT(*) FROM Products WHERE ImportBatch = "batch_001";

-- If validation fails, rollback
-- If validation passes, commit
COMMIT;
```

## Troubleshooting

### Problem: "No active transaction"

**Symptom**: Error when trying to COMMIT or ROLLBACK

**Solution**:
```sql
-- Always begin before commit/rollback
BEGIN TRANSACTION;
-- operations
COMMIT;
```

### Problem: Rollback taking too long

**Symptom**: ROLLBACK operation seems slow

**Explanation**: Rollback of flushed documents requires:
- Physical file deletion
- Index cleanup  
- WAL synchronization

**Solution**: This is expected behavior. For 100 documents, ~400-500ms is normal.

### Problem: Transaction data lost after crash

**Symptom**: Uncommitted transaction data disappeared

**Explanation**: This is correct behavior - only COMMITTED transactions are durable.

**Solution**: Always COMMIT transactions you want to persist.

### Problem: Buffer overflow during large transaction

**Symptom**: Performance degrades with many inserts

**Solution**:
```sql
-- Break into smaller transactions
BEGIN TRANSACTION;
-- Insert 500 documents
COMMIT;

BEGIN TRANSACTION;
-- Insert next 500 documents
COMMIT;
```

## Conclusion

The SyndrDB transaction system provides robust ACID guarantees while maintaining high performance. By following the best practices outlined in this document and understanding the system's architecture, you can build reliable applications that maintain data integrity even in the face of errors or system failures.

For additional support or questions, refer to:
- WAL Documentation: `docs/user/wal.md`
- Write Buffer Documentation: `docs/optimizations.md`
- Error Handling Guide: `docs/user/errors.md`

---

**Document Version**: 1.0  
**Last Updated**: December 11, 2025  
**SyndrDB Version**: Current
