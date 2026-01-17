package storage

import (
	"encoding/json"
	"rdbms/eventlog"
)

// DerivedState represents the current state of the database derived from events
type DerivedState struct {
	// Tables: tableName -> rowID -> current row data
	Tables map[string]map[int64]Row

	// DeletedRows: tableName -> set of deleted rowIDs
	DeletedRows map[string]map[int64]bool
}

// ReplayEvents derives the current state by replaying all events
func ReplayEvents(events []*eventlog.Event) (*DerivedState, error) {
	return ReplayEventsUpTo(events, 0) // 0 means all events
}

// ReplayEventsUpTo derives state by replaying events up to a specific event ID
func ReplayEventsUpTo(events []*eventlog.Event, upToEventID uint64) (*DerivedState, error) {
	state := &DerivedState{
		Tables:      make(map[string]map[int64]Row),
		DeletedRows: make(map[string]map[int64]bool),
	}

	for _, e := range events {
		// If upToEventID specified, stop after that event
		if upToEventID > 0 && e.ID > upToEventID {
			break
		}

		switch e.Type {
		case eventlog.SchemaCreated:
			payload := e.Payload.(map[string]interface{})
			tableName := payload["table_name"].(string)

			// Initialize table
			if _, exists := state.Tables[tableName]; !exists {
				state.Tables[tableName] = make(map[int64]Row)
				state.DeletedRows[tableName] = make(map[int64]bool)
			}

		case eventlog.RowInserted:
			payload := e.Payload.(map[string]interface{})
			tableName := payload["table_name"].(string)
			rowID := int64(payload["row_id"].(float64))

			// Parse data
			dataRaw := payload["data"].(map[string]interface{})
			row := Row(dataRaw)

			// Initialize table if needed
			if _, exists := state.Tables[tableName]; !exists {
				state.Tables[tableName] = make(map[int64]Row)
				state.DeletedRows[tableName] = make(map[int64]bool)
			}

			// Insert row
			state.Tables[tableName][rowID] = row
			delete(state.DeletedRows[tableName], rowID) // Mark as not deleted

		case eventlog.RowUpdated:
			payload := e.Payload.(map[string]interface{})
			tableName := payload["table_name"].(string)
			rowID := int64(payload["row_id"].(float64))

			// Parse changes
			changesRaw := payload["changes"].(map[string]interface{})

			// Apply changes to existing row
			if _, exists := state.Tables[tableName]; !exists {
				state.Tables[tableName] = make(map[int64]Row)
				state.DeletedRows[tableName] = make(map[int64]bool)
			}

			if _, exists := state.Tables[tableName][rowID]; !exists {
				state.Tables[tableName][rowID] = make(Row)
			}

			// Merge changes into row
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

			// Mark as deleted
			state.DeletedRows[tableName][rowID] = true
		}
	}

	return state, nil
}

// GetTableRows returns only non-deleted rows for a table
func (s *DerivedState) GetTableRows(tableName string) []RowWithID {
	var result []RowWithID

	if tableRows, exists := s.Tables[tableName]; exists {
		deletedSet := s.DeletedRows[tableName]

		for rowID, row := range tableRows {
			// Skip deleted rows
			if deletedSet[rowID] {
				continue
			}
			result = append(result, RowWithID{ID: rowID, Row: row})
		}
	}

	return result
}

// GetRow returns a single row if it exists and is not deleted
func (s *DerivedState) GetRow(tableName string, rowID int64) (Row, bool) {
	if tableRows, exists := s.Tables[tableName]; exists {
		if row, exists := tableRows[rowID]; exists {
			// Check if deleted
			if !s.DeletedRows[tableName][rowID] {
				return row, true
			}
		}
	}
	return nil, false
}

// ConvertPayload converts a map-based payload to a specific type
// Used for unmarshaling generic JSON payloads
func ConvertPayload(payload map[string]interface{}, target interface{}) error {
	data, _ := json.Marshal(payload)
	return json.Unmarshal(data, target)
}
