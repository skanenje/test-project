# Integration Tests

## Purpose

The `integration` test package contains end-to-end tests verifying multiple components work together. Tests typically:
- Create a full database instance
- Execute sequences of operations
- Verify end-to-end behavior
- Test component interactions

## Test Files

### database_test.go
Core database operations:
- Table creation
- INSERT/SELECT/UPDATE/DELETE
- Data integrity
- Index functionality

### executor_test.go
SQL statement execution:
- CREATE TABLE execution
- INSERT execution
- SELECT queries
- UPDATE/DELETE operations
- JOIN operations

### eventstore_test.go
Event sourcing:
- Event recording
- Event log persistence
- Event replay
- Recovery from log

### snapshot_test.go
Snapshot functionality:
- Snapshot creation
- Persistence
- Recovery
- Combining snapshots with replay

### recovery_test.go
Database recovery:
- Clean shutdown recovery
- Partial write recovery
- Corrupted snapshot recovery
- Index recovery

### schema_evolution_test.go
Schema changes:
- Schema versioning
- Column addition
- Backward compatibility
- Event replay with changes

### migration_test.go
Data migrations:
- Schema migration
- Data transformation
- Migration rollback

## Running Tests

```bash
# Run all integration tests
go test ./tests/integration -v

# Run specific test
go test ./tests/integration -run TestDatabaseInsertSelect -v

# Run with coverage
go test ./tests/integration -v -cover
```

## Test Pattern

```go
func TestSomething(t *testing.T) {
    // Setup
    tmpDir := t.TempDir()
    db, err := database.New(tmpDir)
    if err != nil {
        t.Fatal(err)
    }
    defer db.Close()

    // Test code
    db.CreateTable("users", cols)
    db.Insert("users", row)

    // Assert
    results, err := db.Select("users", where)
    if err != nil {
        t.Fatalf("unexpected error: %v", err)
    }
    if len(results) != 1 {
        t.Errorf("got %d rows, want 1", len(results))
    }
}
```

## Common Assertions

```go
if err != nil {
    t.Fatalf("unexpected error: %v", err)
}

if got != want {
    t.Errorf("got %v, want %v", got, want)
}

if len(rows) != expectedLen {
    t.Errorf("got %d rows, want %d", len(rows), expectedLen)
}
```

## Coverage

```bash
go test ./tests/integration -coverprofile=coverage.out
go tool cover -html=coverage.out
```

## Contributing Tests

1. Create temp database with `t.TempDir()`
2. Set up test data
3. Execute the operation
4. Assert results match expectations
5. Defer `db.Close()` for cleanup
