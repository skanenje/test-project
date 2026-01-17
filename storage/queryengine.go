package storage

import (
	"sync"

	"rdbms/eventlog"
)

// QueryEngine provides efficient querying of the database state
// It uses snapshots for performance and replays events for freshness
type QueryEngine struct {
	mu                sync.RWMutex
	eventStore        *EventStore
	snapshotManager   *SnapshotManager
	cachedState       *DerivedState
	cachedUpToEventID uint64
	enableSnapshots   bool
}

// NewQueryEngine creates a new query engine
func NewQueryEngine(eventStore *EventStore, snapshotManager *SnapshotManager) *QueryEngine {
	return &QueryEngine{
		eventStore:      eventStore,
		snapshotManager: snapshotManager,
		enableSnapshots: true,
	}
}

// GetCurrentState returns the current database state
// Strategy:
// 1. If snapshots enabled and one exists, restore it
// 2. Replay events since snapshot
// 3. Cache the result
func (qe *QueryEngine) GetCurrentState() (*DerivedState, error) {
	qe.mu.Lock()
	defer qe.mu.Unlock()

	var baseState *DerivedState
	var baseEventID uint64

	// Try to restore from latest snapshot
	if qe.enableSnapshots {
		if snap, meta, err := qe.snapshotManager.RestoreLatestSnapshot(); err == nil {
			baseState = snap
			baseEventID = meta.BaseEventID
		}
	}

	// If no snapshot, start from scratch
	if baseState == nil {
		baseState = &DerivedState{
			Tables:      make(map[string]map[int64]Row),
			DeletedRows: make(map[string]map[int64]bool),
		}
		baseEventID = 0
	}

	// Get events since snapshot
	events, err := qe.eventStore.GetEventsFrom(baseEventID + 1)
	if err != nil {
		return nil, err
	}

	// Replay events onto base state
	if len(events) > 0 {
		mergedEvents := append([]*eventlog.Event{}, events...) // Copy for safety
		replayedState, err := replayEventsOntoState(baseState, mergedEvents)
		if err != nil {
			return nil, err
		}
		baseState = replayedState
	}

	qe.cachedState = baseState
	qe.cachedUpToEventID = qe.eventStore.GetLastEventID()

	return baseState, nil
}

// GetTableRows returns all active rows for a table
func (qe *QueryEngine) GetTableRows(tableName string) ([]RowWithID, error) {
	state, err := qe.GetCurrentState()
	if err != nil {
		return nil, err
	}

	return state.GetTableRows(tableName), nil
}

// GetRow returns a single row by ID
func (qe *QueryEngine) GetRow(tableName string, rowID int64) (Row, bool, error) {
	state, err := qe.GetCurrentState()
	if err != nil {
		return nil, false, err
	}

	row, exists := state.GetRow(tableName, rowID)
	return row, exists, nil
}

// InvalidateCache clears the cached state
// Called when snapshots are created or when explicit refresh is needed
func (qe *QueryEngine) InvalidateCache() {
	qe.mu.Lock()
	defer qe.mu.Unlock()
	qe.cachedState = nil
	qe.cachedUpToEventID = 0
}

// SetSnapshotsEnabled enables/disables snapshot usage
func (qe *QueryEngine) SetSnapshotsEnabled(enabled bool) {
	qe.mu.Lock()
	defer qe.mu.Unlock()
	qe.enableSnapshots = enabled
	if enabled {
		qe.InvalidateCache()
	}
}

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
