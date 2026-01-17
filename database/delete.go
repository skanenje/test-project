package database

import (
	"fmt"
	"rdbms/parser"
)

// Delete deletes rows matching WHERE clause
// Now emits ROW_DELETED events instead of direct mutations
func (db *Database) Delete(tableName string, where *parser.WhereClause) (int, error) {
	db.mu.Lock()
	defer db.mu.Unlock()

	if !db.catalog.TableExists(tableName) {
		return 0, fmt.Errorf("table '%s' does not exist", tableName)
	}

	if where == nil {
		return 0, fmt.Errorf("DELETE requires WHERE clause")
	}

	// Get current state
	state, err := db.queryEngine.GetCurrentState()
	if err != nil {
		return 0, err
	}

	// Find rows matching WHERE clause
	rows := state.GetTableRows(tableName)

	count := 0
	txID := fmt.Sprintf("tx_%d", db.eventStore.GetLastEventID())

	for _, r := range rows {
		if val, exists := r.Row[where.Column]; exists && valuesEqual(val, where.Value) {
			// Remove from indexes
			for colName, idx := range db.indexes[tableName] {
				if colVal, exists := r.Row[colName]; exists {
					idx.Remove(colVal, r.ID)
				}
			}

			// Record the deletion event (preserve row data for recovery)
			_, err := db.eventStore.RecordRowDeleted(tableName, r.ID, r.Row, txID)
			if err != nil {
				return count, err
			}

			count++
		}
	}

	// Invalidate query cache
	db.queryEngine.InvalidateCache()

	return count, nil
}
