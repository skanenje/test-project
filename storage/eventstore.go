package storage

import (
	"encoding/json"
	"fmt"
	"rdbms/eventlog"
	"sync"
)

// EventStore wraps the event log and provides database-aware operations
type EventStore struct {
	mu  sync.RWMutex
	log *eventlog.Log

	// Current schema version (incremented on schema changes)
	schemaVersion int

	// Track row versions for optimistic concurrency (rowID -> latestEventID)
	rowVersions map[string]map[int64]uint64
}

// NewEventStore creates a new event store backed by an event log
func NewEventStore(dataDir string) (*EventStore, error) {
	log, err := eventlog.NewLog(dataDir, "events.log")
	if err != nil {
		return nil, err
	}

	es := &EventStore{
		log:           log,
		schemaVersion: 1, // Start at version 1
		rowVersions:   make(map[string]map[int64]uint64),
	}

	// Load existing row versions from log
	events, _ := log.Read()
	es.rebuildRowVersions(events)

	return es, nil
}

// rebuildRowVersions reconstructs row version map from events
func (es *EventStore) rebuildRowVersions(events []*eventlog.Event) {
	for _, e := range events {
		switch e.Type {
		case eventlog.SchemaCreated:
			// New table
		case eventlog.RowInserted:
			payload := e.Payload.(map[string]interface{})
			tableName := payload["table_name"].(string)
			rowID := int64(payload["row_id"].(float64))

			if _, exists := es.rowVersions[tableName]; !exists {
				es.rowVersions[tableName] = make(map[int64]uint64)
			}
			es.rowVersions[tableName][rowID] = e.ID

		case eventlog.RowUpdated:
			payload := e.Payload.(map[string]interface{})
			tableName := payload["table_name"].(string)
			rowID := int64(payload["row_id"].(float64))

			if _, exists := es.rowVersions[tableName]; !exists {
				es.rowVersions[tableName] = make(map[int64]uint64)
			}
			es.rowVersions[tableName][rowID] = e.ID

		case eventlog.RowDeleted:
			payload := e.Payload.(map[string]interface{})
			tableName := payload["table_name"].(string)
			rowID := int64(payload["row_id"].(float64))

			if _, exists := es.rowVersions[tableName]; !exists {
				es.rowVersions[tableName] = make(map[int64]uint64)
			}
			es.rowVersions[tableName][rowID] = e.ID

		case eventlog.SchemaEvolved:
			es.schemaVersion++
		}
	}
}

// RecordSchemaCreated logs a table creation event
func (es *EventStore) RecordSchemaCreated(tableName string, columns []eventlog.ColumnDefinition, primaryKey string, txID string) (*eventlog.Event, error) {
	es.mu.Lock()
	defer es.mu.Unlock()

	payload := &eventlog.SchemaCreatedPayload{
		TableName:  tableName,
		Columns:    columns,
		PrimaryKey: primaryKey,
	}

	// Marshal payload to JSON (eventlog.Append expects JSON-serializable payload)
	payloadJSON, _ := json.Marshal(payload)
	var payloadData map[string]interface{}
	json.Unmarshal(payloadJSON, &payloadData)

	event, err := es.log.Append(eventlog.SchemaCreated, payloadData, txID, es.schemaVersion)
	if err != nil {
		return nil, err
	}

	// Initialize row version tracking for this table
	if _, exists := es.rowVersions[tableName]; !exists {
		es.rowVersions[tableName] = make(map[int64]uint64)
	}

	return event, nil
}

// RecordRowInserted logs a row insertion event
func (es *EventStore) RecordRowInserted(tableName string, rowID int64, data Row, txID string) (*eventlog.Event, error) {
	es.mu.Lock()
	defer es.mu.Unlock()

	payload := &eventlog.RowInsertedPayload{
		TableName: tableName,
		RowID:     rowID,
		Data:      data,
	}

	payloadJSON, _ := json.Marshal(payload)
	var payloadData map[string]interface{}
	json.Unmarshal(payloadJSON, &payloadData)

	event, err := es.log.Append(eventlog.RowInserted, payloadData, txID, es.schemaVersion)
	if err != nil {
		return nil, err
	}

	// Track row version
	if _, exists := es.rowVersions[tableName]; !exists {
		es.rowVersions[tableName] = make(map[int64]uint64)
	}
	es.rowVersions[tableName][rowID] = event.ID

	return event, nil
}

// RecordRowUpdated logs a row update event
func (es *EventStore) RecordRowUpdated(tableName string, rowID int64, changes map[string]interface{}, oldValues map[string]interface{}, txID string) (*eventlog.Event, error) {
	es.mu.Lock()
	defer es.mu.Unlock()

	payload := &eventlog.RowUpdatedPayload{
		TableName: tableName,
		RowID:     rowID,
		Changes:   changes,
		OldValues: oldValues,
	}

	payloadJSON, _ := json.Marshal(payload)
	var payloadData map[string]interface{}
	json.Unmarshal(payloadJSON, &payloadData)

	event, err := es.log.Append(eventlog.RowUpdated, payloadData, txID, es.schemaVersion)
	if err != nil {
		return nil, err
	}

	// Track row version
	if _, exists := es.rowVersions[tableName]; !exists {
		es.rowVersions[tableName] = make(map[int64]uint64)
	}
	es.rowVersions[tableName][rowID] = event.ID

	return event, nil
}

// RecordRowDeleted logs a row deletion event
func (es *EventStore) RecordRowDeleted(tableName string, rowID int64, deletedData Row, txID string) (*eventlog.Event, error) {
	es.mu.Lock()
	defer es.mu.Unlock()

	payload := &eventlog.RowDeletedPayload{
		TableName:   tableName,
		RowID:       rowID,
		DeletedData: deletedData,
	}

	payloadJSON, _ := json.Marshal(payload)
	var payloadData map[string]interface{}
	json.Unmarshal(payloadJSON, &payloadData)

	event, err := es.log.Append(eventlog.RowDeleted, payloadData, txID, es.schemaVersion)
	if err != nil {
		return nil, err
	}

	// Mark row as deleted in version tracking
	if _, exists := es.rowVersions[tableName]; !exists {
		es.rowVersions[tableName] = make(map[int64]uint64)
	}
	es.rowVersions[tableName][rowID] = event.ID

	return event, nil
}

// RecordSchemaEvolved logs a schema evolution event
func (es *EventStore) RecordSchemaEvolved(tableName string, oldSchema []eventlog.ColumnDefinition, newSchema []eventlog.ColumnDefinition, evolution eventlog.SchemaEvolution, txID string) (*eventlog.Event, error) {
	es.mu.Lock()
	defer es.mu.Unlock()

	payload := &eventlog.SchemaEvolvedPayload{
		TableName: tableName,
		Evolution: evolution,
		OldSchema: oldSchema,
		NewSchema: newSchema,
	}

	payloadJSON, _ := json.Marshal(payload)
	var payloadData map[string]interface{}
	json.Unmarshal(payloadJSON, &payloadData)

	event, err := es.log.Append(eventlog.SchemaEvolved, payloadData, txID, es.schemaVersion)
	if err != nil {
		return nil, err
	}

	es.schemaVersion++

	return event, nil
}

// GetAllEvents returns all events from the log
func (es *EventStore) GetAllEvents() ([]*eventlog.Event, []eventlog.EventError) {
	es.mu.RLock()
	defer es.mu.RUnlock()
	return es.log.Read()
}

// GetEventsFrom returns events starting from a specific event ID
func (es *EventStore) GetEventsFrom(eventID uint64) ([]*eventlog.Event, error) {
	es.mu.RLock()
	defer es.mu.RUnlock()
	return es.log.ReadFrom(eventID)
}

// GetLastEventID returns the ID of the last event
func (es *EventStore) GetLastEventID() uint64 {
	es.mu.RLock()
	defer es.mu.RUnlock()
	return es.log.LastID()
}

// GetSchemaVersion returns the current schema version
func (es *EventStore) GetSchemaVersion() int {
	es.mu.RLock()
	defer es.mu.RUnlock()
	return es.schemaVersion
}

// GetRowVersion returns the latest event ID for a specific row
func (es *EventStore) GetRowVersion(tableName string, rowID int64) (uint64, error) {
	es.mu.RLock()
	defer es.mu.RUnlock()

	if rowMap, exists := es.rowVersions[tableName]; exists {
		if version, exists := rowMap[rowID]; exists {
			return version, nil
		}
	}

	return 0, fmt.Errorf("row %d not found in table %s", rowID, tableName)
}

// Close closes the event store
func (es *EventStore) Close() error {
	es.mu.Lock()
	defer es.mu.Unlock()
	return es.log.Close()
}
