package eventlog

import (
	"time"
)

// EventType represents the type of event in the log
type EventType string

const (
	// SchemaCreated: A new table schema was created
	SchemaCreated EventType = "SCHEMA_CREATED"
	// RowInserted: A new row was inserted
	RowInserted EventType = "ROW_INSERTED"
	// RowUpdated: A row was updated
	RowUpdated EventType = "ROW_UPDATED"
	// RowDeleted: A row was deleted (logically)
	RowDeleted EventType = "ROW_DELETED"
	// SchemaEvolved: Schema was modified (columns added/removed/altered)
	SchemaEvolved EventType = "SCHEMA_EVOLVED"
	// SnapshotCreated: A snapshot of current state was created
	SnapshotCreated EventType = "SNAPSHOT_CREATED"
)

// Event represents an immutable database event
type Event struct {
	// Core metadata
	ID        uint64    `json:"id"`        // Sequential event ID (monotonic, 1-indexed)
	Type      EventType `json:"type"`      // Event type
	Timestamp time.Time `json:"timestamp"` // When event occurred
	Version   int       `json:"version"`   // Schema version this event targets

	// Transaction metadata for grouping related operations
	TxID string `json:"tx_id,omitempty"` // Transaction ID (UUID)

	// Payload - varies by event type
	Payload EventPayload `json:"payload"`

	// Data integrity
	Checksum string `json:"checksum"` // SHA256 of the event (excluding checksum field)
}

// EventPayload is a generic container for event-specific data
type EventPayload interface{}

// SchemaCreatedPayload - when SCHEMA_CREATED event occurs
type SchemaCreatedPayload struct {
	TableName  string             `json:"table_name"`
	Columns    []ColumnDefinition `json:"columns"`
	PrimaryKey string             `json:"primary_key,omitempty"`
}

// ColumnDefinition represents a column in a table
type ColumnDefinition struct {
	Name       string      `json:"name"`
	Type       string      `json:"type"` // INT, VARCHAR, FLOAT, BOOL, etc.
	Nullable   bool        `json:"nullable"`
	PrimaryKey bool        `json:"primary_key"`
	Unique     bool        `json:"unique"`
	Default    interface{} `json:"default,omitempty"`
}

// RowInsertedPayload - when ROW_INSERTED event occurs
type RowInsertedPayload struct {
	TableName string                 `json:"table_name"`
	RowID     int64                  `json:"row_id"` // Internal row identifier
	Data      map[string]interface{} `json:"data"`   // Column name -> value
}

// RowUpdatedPayload - when ROW_UPDATED event occurs
type RowUpdatedPayload struct {
	TableName string                 `json:"table_name"`
	RowID     int64                  `json:"row_id"`
	Changes   map[string]interface{} `json:"changes"`    // Only changed columns
	OldValues map[string]interface{} `json:"old_values"` // For audit trail (optional)
}

// RowDeletedPayload - when ROW_DELETED event occurs
type RowDeletedPayload struct {
	TableName string `json:"table_name"`
	RowID     int64  `json:"row_id"`
	// Optional: store deleted data for recovery
	DeletedData map[string]interface{} `json:"deleted_data,omitempty"`
}

// SchemaEvolvedPayload - when SCHEMA_EVOLVED event occurs
type SchemaEvolvedPayload struct {
	TableName string             `json:"table_name"`
	Evolution SchemaEvolution    `json:"evolution"`
	OldSchema []ColumnDefinition `json:"old_schema"`
	NewSchema []ColumnDefinition `json:"new_schema"`
}

// SchemaEvolution describes what changed in the schema
type SchemaEvolution struct {
	AddedColumns    []ColumnDefinition   `json:"added_columns,omitempty"`
	RemovedColumns  []string             `json:"removed_columns,omitempty"` // Column names
	ModifiedColumns []ColumnModification `json:"modified_columns,omitempty"`
	RenamedColumns  map[string]string    `json:"renamed_columns,omitempty"` // old name -> new name
}

// ColumnModification describes changes to a column
type ColumnModification struct {
	Name   string           `json:"name"`
	OldDef ColumnDefinition `json:"old_definition"`
	NewDef ColumnDefinition `json:"new_definition"`
}

// SnapshotCreatedPayload - when SNAPSHOT_CREATED event occurs
type SnapshotCreatedPayload struct {
	SnapshotID     string    `json:"snapshot_id"`   // UUID
	BaseEventID    uint64    `json:"base_event_id"` // Last event included in snapshot
	CreatedAt      time.Time `json:"created_at"`
	SnapshotPath   string    `json:"snapshot_path"`   // Relative path to snapshot file
	DataHash       string    `json:"data_hash"`       // Hash of entire snapshot state
	EventsIncluded int64     `json:"events_included"` // Number of events replayed
}

// EventError wraps an event that failed to process
type EventError struct {
	EventID   uint64
	Type      EventType
	Error     string
	Timestamp time.Time
}
