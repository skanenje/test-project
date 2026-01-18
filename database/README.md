# Database Package

## Purpose

The `database` package is the core orchestrator of the RDBMS. It coordinates between the schema catalog, event storage, query execution, and indexing systems. The database uses **event sourcing** where all changes are recorded as immutable events, providing complete audit trails and enabling recovery.

## Key Concepts

### Event Sourcing

Every operation (INSERT, UPDATE, DELETE) is recorded as an event in an append-only log:
- Complete audit trails of all changes
- Recovery from failures by replaying events
- Time-travel queries
- Schema evolution tracking

### Snapshots

Periodic snapshots of complete database state avoid replaying all events on startup.

### Indexes

In-memory indexes on configured columns enable fast lookups, updated on every write.

## Key Types

```go
type Database struct {
    mu              sync.RWMutex
    eventStore      *storage.EventStore
    queryEngine     *storage.QueryEngine
    snapshotManager *storage.SnapshotManager
    catalog         *catalog.Catalog
    indexes         map[string]map[string]*index.Index
    nextRowID       map[string]int64
    snapshotInterval int64
}
```

## Main Operations

- `New(dataDir string) (*Database, error)` - Create database
- `(db *Database) CreateTable(name string, cols []schema.Column) error` - Create table
- `(db *Database) Insert(table string, row storage.Row) (int64, error)` - Insert row
- `(db *Database) Select(table string, where *parser.WhereClause) ([]storage.RowWithID, error)` - Query
- `(db *Database) Update(table, column string, value, newValue interface{}) (int, error)` - Update
- `(db *Database) Delete(table string, where *parser.WhereClause) (int, error)` - Delete
- `(db *Database) Join(table1, table2, col1, col2 string) ([]map[string]interface{}, error)` - Join

## Integration Points

- **Parser Package**: Provides parsed statements
- **Executor Package**: Executes parsed statements
- **Storage Package**: Persists events and snapshots
- **Index Package**: Provides fast lookups
- **Catalog Package**: Validates schema correctness
