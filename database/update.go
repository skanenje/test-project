package database

import (
	"fmt"
	"rdbms/parser"
	"rdbms/storage"
)

// Update updates rows matching WHERE clause
// Now emits ROW_UPDATED events instead of direct mutations
func (db *Database) Update(tableName string, setColumn string, setValue interface{}, where *parser.WhereClause) (int, error) {
	db.mu.Lock()
	defer db.mu.Unlock()

	table, err := db.catalog.GetTable(tableName)
	if err != nil {
		return 0, err
	}

	if where == nil {
		return 0, fmt.Errorf("UPDATE requires WHERE clause")
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
			// Remove old from indexes
			for colName, idx := range db.indexes[tableName] {
				if colVal, exists := r.Row[colName]; exists {
					idx.Remove(colVal, r.ID)
				}
			}

			// Create new row with updated column
			newRow := make(storage.Row)
			for k, v := range r.Row {
				newRow[k] = v
			}

			oldValue := r.Row[setColumn]
			newRow[setColumn] = setValue

			if err := db.validateRow(table, newRow); err != nil {
				return count, err
			}

			// Record the update event
			changes := map[string]interface{}{setColumn: setValue}
			oldValues := map[string]interface{}{setColumn: oldValue}

			_, err := db.eventStore.RecordRowUpdated(tableName, r.ID, changes, oldValues, txID)
			if err != nil {
				return count, err
			}

			// Add new to indexes
			for colName, idx := range db.indexes[tableName] {
				if colVal, exists := newRow[colName]; exists {
					idx.Add(colVal, r.ID)
				}
			}

			count++
		}
	}

	// Invalidate query cache
	db.queryEngine.InvalidateCache()

	return count, nil
}
