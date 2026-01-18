# Storage Package

## Purpose

The `storage` package handles all physical data persistence. It implements four core components:
1. **Engine** - Low-level row storage on disk
2. **EventStore** - Immutable event log for event sourcing
3. **SnapshotManager** - Snapshots for fast recovery
4. **QueryEngine** - Combines snapshots and event replay

## Key Concepts

### Event Sourcing

All changes stored as events in append-only log:
- **Durability** - Every change persisted before returning
- **Auditability** - Complete history of all changes
- **Recovery** - Rebuild any state by replaying events
- **Time Travel** - Query database at any point in time

### Snapshots

Periodic full copies of database state enable:
- Fast recovery without replaying all events
- Snapshots created every N events

### Row Storage Format

```
[deleted_flag: 1 byte][data_length: 4 bytes][json_data: variable bytes]
```

## Key Types

```go
type Engine struct {
    dataDir string
}

type Row map[string]interface{}

type RowWithID struct {
    ID  int64
    Row Row
}

type EventStore struct {
    mu            sync.RWMutex
    log           *eventlog.Log
    schemaVersion int
    rowVersions   map[string]map[int64]uint64
}

type SnapshotManager struct {
    dataDir string
    mu      sync.RWMutex
}

type Snapshot struct {
    ID       uint64
    EventID  uint64
    Tables   map[string][]RowWithID
    Indexes  map[string]map[string][]int64
}

type QueryEngine struct {
    eventStore      *EventStore
    snapshotManager *SnapshotManager
}
```

## Main Functions

### Engine
- `NewEngine(dataDir string) (*Engine, error)`
- `(e *Engine) InsertRow(table string, row Row) (int64, error)`
- `(e *Engine) ReadRow(table string, rowID int64) (Row, error)`
- `(e *Engine) UpdateRow(table string, rowID int64, row Row) error`
- `(e *Engine) DeleteRow(table string, rowID int64) error`
- `(e *Engine) Scan(table string) ([]RowWithID, error)`

### EventStore
- `NewEventStore(dataDir string) (*EventStore, error)`
- `(es *EventStore) Append(event *eventlog.Event) error`
- `(es *EventStore) Read() ([]*eventlog.Event, error)`
- `(es *EventStore) GetSchemaVersion() int`

### SnapshotManager
- `NewSnapshotManager(dataDir string) (*SnapshotManager, error)`
- `(sm *SnapshotManager) Save(snap *Snapshot) error`
- `(sm *SnapshotManager) Latest() (*Snapshot, error)`

## Data Directory Structure

```
data/
├── events.log
├── events.index
├── snapshots/
│   ├── snapshot_1.json
│   └── snapshot_2.json
├── users.db
├── orders.db
└── _catalog.json
```

## Integration Points

- **Database Package**: Uses storage for persistence
- **EventLog Package**: Manages event log files
- **Snapshot Package**: Manages snapshots
