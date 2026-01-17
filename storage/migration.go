package storage

import (
	"fmt"
	"sync"

	"rdbms/eventlog"
	"rdbms/schema"
)

// MigrationHandler manages schema migrations during state derivation
// Transparently applies migrations when replaying rows from older schema versions
type MigrationHandler struct {
	mu       sync.RWMutex
	registry *schema.SchemaRegistry
}

// NewMigrationHandler creates a new migration handler
func NewMigrationHandler(registry *schema.SchemaRegistry) *MigrationHandler {
	return &MigrationHandler{
		registry: registry,
	}
}

// MigrateRowIfNeeded applies schema migrations to a row if needed
func (mh *MigrationHandler) MigrateRowIfNeeded(tableName string, row Row, fromVersion, toVersion int) (Row, error) {
	mh.mu.RLock()
	defer mh.mu.RUnlock()

	if fromVersion == toVersion {
		return row, nil
	}

	// Convert Row map to generic map for migration
	genericRow := make(map[string]interface{})
	for k, v := range row {
		genericRow[k] = v
	}

	// Apply migrations
	migratedRow, err := mh.registry.MigrateRow(tableName, genericRow, fromVersion, toVersion)
	if err != nil {
		return nil, err
	}

	// Convert back to Row
	result := Row(migratedRow)
	return result, nil
}

// ReplayEventsWithMigrations replays events and applies migrations for target schema version
func ReplayEventsWithMigrations(events []*eventlog.Event, targetSchemaVersion int, migrationHandler *MigrationHandler) (*DerivedState, error) {
	state := &DerivedState{
		Tables:      make(map[string]map[int64]Row),
		DeletedRows: make(map[string]map[int64]bool),
	}

	// Track current schema version per table
	tableSchemaVersions := make(map[string]int)

	for _, e := range events {
		switch e.Type {
		case eventlog.SchemaCreated:
			payload := e.Payload.(map[string]interface{})
			tableName := payload["table_name"].(string)

			// Initialize table
			if _, exists := state.Tables[tableName]; !exists {
				state.Tables[tableName] = make(map[int64]Row)
				state.DeletedRows[tableName] = make(map[int64]bool)
			}

			tableSchemaVersions[tableName] = e.Version

		case eventlog.RowInserted:
			payload := e.Payload.(map[string]interface{})
			tableName := payload["table_name"].(string)
			rowID := int64(payload["row_id"].(float64))
			dataRaw := payload["data"].(map[string]interface{})
			row := Row(dataRaw)

			// Initialize table if needed
			if _, exists := state.Tables[tableName]; !exists {
				state.Tables[tableName] = make(map[int64]Row)
				state.DeletedRows[tableName] = make(map[int64]bool)
			}

			// Apply migration if needed
			if eventSchemaVer := e.Version; eventSchemaVer < targetSchemaVersion && migrationHandler != nil {
				migratedRow, err := migrationHandler.MigrateRowIfNeeded(tableName, row, eventSchemaVer, targetSchemaVersion)
				if err != nil {
					// Log but continue - don't fail entire replay
					fmt.Printf("Warning: migration failed for row %d in table %s: %v\n", rowID, tableName, err)
				} else {
					row = migratedRow
				}
			}

			state.Tables[tableName][rowID] = row
			delete(state.DeletedRows[tableName], rowID)

		case eventlog.RowUpdated:
			payload := e.Payload.(map[string]interface{})
			tableName := payload["table_name"].(string)
			rowID := int64(payload["row_id"].(float64))
			changesRaw := payload["changes"].(map[string]interface{})

			// Initialize table if needed
			if _, exists := state.Tables[tableName]; !exists {
				state.Tables[tableName] = make(map[int64]Row)
				state.DeletedRows[tableName] = make(map[int64]bool)
			}

			if _, exists := state.Tables[tableName][rowID]; !exists {
				state.Tables[tableName][rowID] = make(Row)
			}

			// Apply changes (already in target schema)
			for k, v := range changesRaw {
				state.Tables[tableName][rowID][k] = v
			}

		case eventlog.RowDeleted:
			payload := e.Payload.(map[string]interface{})
			tableName := payload["table_name"].(string)
			rowID := int64(payload["row_id"].(float64))

			// Initialize table if needed
			if _, exists := state.Tables[tableName]; !exists {
				state.Tables[tableName] = make(map[int64]Row)
				state.DeletedRows[tableName] = make(map[int64]bool)
			}

			state.DeletedRows[tableName][rowID] = true

		case eventlog.SchemaEvolved:
			payload := e.Payload.(map[string]interface{})
			tableName := payload["table_name"].(string)
			tableSchemaVersions[tableName] = e.Version
		}
	}

	return state, nil
}

// GetSchemaVersionHistory returns all schema versions encountered in events
func GetSchemaVersionHistory(events []*eventlog.Event) map[string][]int {
	history := make(map[string][]int) // table -> versions

	for _, e := range events {
		switch e.Type {
		case eventlog.SchemaCreated:
			payload := e.Payload.(map[string]interface{})
			tableName := payload["table_name"].(string)
			history[tableName] = append(history[tableName], e.Version)

		case eventlog.SchemaEvolved:
			payload := e.Payload.(map[string]interface{})
			tableName := payload["table_name"].(string)
			if !contains(history[tableName], e.Version) {
				history[tableName] = append(history[tableName], e.Version)
			}
		}
	}

	return history
}

// contains checks if a slice contains a value
func contains(slice []int, value int) bool {
	for _, v := range slice {
		if v == value {
			return true
		}
	}
	return false
}
