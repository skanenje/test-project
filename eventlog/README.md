# Event Log Package

## Purpose

The `eventlog` package implements the append-only event log that forms the foundation of the event-sourced architecture. Every database change is recorded as an immutable event, providing a complete audit trail and enabling recovery.

## Event Types

### SchemaCreated
Recorded when a new table is created.

### RowInserted
Recorded when a new row is inserted.

### RowUpdated
Recorded when a row is modified.

### RowDeleted
Recorded when a row is deleted (logically).

### SchemaEvolved
Recorded when table schema is modified.

### SnapshotCreated
Recorded when a database snapshot is taken.

## Key Types

```go
type EventType string

type Event struct {
    ID        uint64           // Sequential, monotonic ID (1-indexed)
    Type      EventType        // Event type
    Timestamp time.Time        // When occurred
    Version   int              // Schema version
    TxID      string           // Transaction ID (UUID)
    Payload   EventPayload     // Event-specific data
    Checksum  string           // SHA256 for integrity
}

type Log struct {
    dataDir  string
    filename string
    events   []*Event
    mu       sync.RWMutex
}
```

## Main Functions

- `NewLog(dataDir, filename string) (*Log, error)` - Create/load log
- `(l *Log) Append(event *Event) error` - Record event
- `(l *Log) Read() ([]*Event, error)` - Get all events
- `(l *Log) ReadFrom(eventID uint64) ([]*Event, error)` - Get events after ID
- `(l *Log) GetEvent(id uint64) (*Event, error)` - Get specific event
- `(l *Log) Length() uint64` - Total number of events

## Usage Example

```go
log, err := eventlog.NewLog("/path/to/data", "events.log")
if err != nil {
    log.Fatal(err)
}

event := &eventlog.Event{
    Type:      eventlog.SchemaCreated,
    Timestamp: time.Now(),
    Version:   1,
    TxID:      uuid.New().String(),
    Payload: eventlog.SchemaCreatedPayload{
        TableName: "users",
        Columns: []eventlog.ColumnDefinition{
            {Name: "id", Type: "INT", PrimaryKey: true},
            {Name: "name", Type: "TEXT"},
        },
    },
}
err = log.Append(event)

events, err := log.Read()
for _, e := range events {
    fmt.Printf("Event %d: %s\n", e.ID, e.Type)
}
```

## Event ID Properties

- **Sequential** - Each event gets the next available ID
- **Monotonic Increasing** - IDs never decrease or skip
- **1-Indexed** - First event has ID 1
- **Unique** - No two events share the same ID

## File Format

Stored as JSON lines (one event per line):
- Incremental writes (append-only)
- Easy recovery from partial writes
- Human-readable for debugging
- Efficient replay from checkpoints

## Integration Points

- **Storage Package**: EventStore wraps log
- **Database Package**: Records operations as events
- **Snapshot Package**: Snapshots reference event IDs
- **Recovery Package**: Replays events for recovery
