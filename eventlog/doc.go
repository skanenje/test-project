// Package eventlog provides an append-only event log for durable event storage.
//
// The eventlog package implements a write-ahead log (WAL) that stores all database
// events in a persistent, append-only format. Each event is assigned a sequential ID,
// timestamped, and includes a SHA256 checksum for integrity verification.
//
// Key Features:
//   - Append-Only: Events are never modified, only appended
//   - Integrity Checking: SHA256 checksums verify event integrity
//   - Atomic Writes: Events are written atomically with fsync for durability
//   - Batch Operations: Supports batch appends for transaction grouping
//   - Corruption Detection: Can detect and report corrupted events
//
// Event Types:
//   - SCHEMA_CREATED: Table schema creation
//   - ROW_INSERTED: Row insertion
//   - ROW_UPDATED: Row modification
//   - ROW_DELETED: Row deletion
//   - SCHEMA_EVOLVED: Schema changes
//   - SNAPSHOT_CREATED: Snapshot creation
//
// Key Responsibilities:
//   - Appending events atomically to disk
//   - Reading events sequentially or from a specific event ID
//   - Validating event checksums on read
//   - Managing event IDs (monotonic, sequential)
//   - Detecting and reporting corruption
//
// Usage Example:
//
//	log, err := eventlog.NewLog("./data", "events.log")
//	if err != nil {
//		log.Fatal(err)
//	}
//	defer log.Close()
//
//	payload := map[string]interface{}{
//		"table_name": "users",
//		"row_id": 1,
//		"data": map[string]interface{}{"id": 1, "name": "Alice"},
//	}
//	event, err := log.Append(eventlog.RowInserted, payload, "tx-123", 1)
//
//	events, errors := log.Read()
//
// The eventlog package is used by the storage package's EventStore to provide
// durable event storage. It forms the foundation of the event-sourced architecture,
// enabling complete audit trails and time-travel queries.
package eventlog
