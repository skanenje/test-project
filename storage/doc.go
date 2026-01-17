// Package storage provides low-level storage operations and event-sourced persistence.
//
// The storage package is responsible for physical data storage, event sourcing, snapshots,
// and query execution. It implements the core persistence layer of the database using an
// append-only event log architecture.
//
// Key Components:
//   - EventStore: Wraps the event log with database-aware operations
//   - QueryEngine: Executes queries using snapshots and event replay
//   - SnapshotManager: Creates and manages database state snapshots
//   - Engine: Legacy row-based storage (used for snapshots)
//
// Architecture:
//   - Event-Sourced: All changes stored as immutable events
//   - Snapshot-Based Queries: Snapshots provide fast query starting points
//   - Event Replay: Queries replay events from snapshots to current state
//   - Deterministic: Event replay is deterministic for consistency
//
// Key Responsibilities:
//   - Storing and retrieving events from the event log
//   - Creating and loading snapshots for performance
//   - Replaying events to reconstruct database state
//   - Executing queries with snapshot + replay strategy
//   - Managing schema versions and migrations
//   - Detecting and recovering from event corruption
//   - Providing row-based storage for snapshots
//
// Storage Format:
//   - Events: Newline-delimited JSON in events.log
//   - Snapshots: JSON files with complete table state
//   - Rows: Binary format with deleted flags and JSON data
//
// Usage Example:
//
//	// Create event store
//	eventStore, err := storage.NewEventStore("./data")
//	if err != nil {
//		log.Fatal(err)
//	}
//
//	// Record an event
//	payload := map[string]interface{}{
//		"table_name": "users",
//		"row_id": 1,
//		"data": map[string]interface{}{"id": 1, "name": "Alice"},
//	}
//	event, err := eventStore.RecordRowInserted("users", 1, payload, "tx-123")
//
//	// Create query engine
//	snapshotManager, _ := storage.NewSnapshotManager("./data")
//	queryEngine := storage.NewQueryEngine(eventStore, snapshotManager)
//
//	// Query data
//	rows, err := queryEngine.QueryTable("users", nil)
//
// The storage package is the foundation of the database's persistence layer. It works
// closely with the eventlog package for event storage and is used by the database
// package to provide high-level operations. It enables features like time-travel queries,
// audit trails, and schema evolution through event replay.
package storage
