package storage

import "rdbms/eventlog"

// replayEventsOntoState merges new events onto an existing state
func replayEventsOntoState(baseState *DerivedState, events []*eventlog.Event) (*DerivedState, error) {
	state := &DerivedState{
		Tables:      baseState.Tables,      // Reference same tables
		DeletedRows: baseState.DeletedRows, // Reference same deletions
	}

	for _, e := range events {
		switch e.Type {
		case eventlog.SchemaCreated:
			payload := e.Payload.(map[string]interface{})
			tableName := payload["table_name"].(string)

			if _, exists := state.Tables[tableName]; !exists {
				state.Tables[tableName] = make(map[int64]Row)
				state.DeletedRows[tableName] = make(map[int64]bool)
			}

		case eventlog.RowInserted:
			payload := e.Payload.(map[string]interface{})
			tableName := payload["table_name"].(string)
			rowID := int64(payload["row_id"].(float64))
			dataRaw := payload["data"].(map[string]interface{})

			if _, exists := state.Tables[tableName]; !exists {
				state.Tables[tableName] = make(map[int64]Row)
				state.DeletedRows[tableName] = make(map[int64]bool)
			}

			state.Tables[tableName][rowID] = Row(dataRaw)
			delete(state.DeletedRows[tableName], rowID)

		case eventlog.RowUpdated:
			payload := e.Payload.(map[string]interface{})
			tableName := payload["table_name"].(string)
			rowID := int64(payload["row_id"].(float64))
			changesRaw := payload["changes"].(map[string]interface{})

			if _, exists := state.Tables[tableName]; !exists {
				state.Tables[tableName] = make(map[int64]Row)
				state.DeletedRows[tableName] = make(map[int64]bool)
			}

			if _, exists := state.Tables[tableName][rowID]; !exists {
				state.Tables[tableName][rowID] = make(Row)
			}

			for k, v := range changesRaw {
				state.Tables[tableName][rowID][k] = v
			}

		case eventlog.RowDeleted:
			payload := e.Payload.(map[string]interface{})
			tableName := payload["table_name"].(string)
			rowID := int64(payload["row_id"].(float64))

			if _, exists := state.Tables[tableName]; !exists {
				state.Tables[tableName] = make(map[int64]Row)
				state.DeletedRows[tableName] = make(map[int64]bool)
			}

			state.DeletedRows[tableName][rowID] = true
		}
	}

	return state, nil
}
