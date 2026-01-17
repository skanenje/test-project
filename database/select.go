package database

import (
	"fmt"
	"rdbms/parser"
	"rdbms/storage"
)

// Select selects rows from a table with optional WHERE clause (uses index if available)
// Now derives state from event log via query engine
func (db *Database) Select(tableName string, where *parser.WhereClause) ([]storage.Row, error) {
	db.mu.RLock()
	defer db.mu.RUnlock()

	if !db.catalog.TableExists(tableName) {
		return nil, fmt.Errorf("table '%s' does not exist", tableName)
	}

	// Get current state from query engine (uses snapshots + events)
	state, err := db.queryEngine.GetCurrentState()
	if err != nil {
		return nil, err
	}

	var rowsWithID []storage.RowWithID

	// Try to use index for WHERE clause
	if where != nil {
		if idx, hasIndex := db.indexes[tableName][where.Column]; hasIndex {
			// Index hit! Fetch only matching row IDs from index
			if rowIDs, found := idx.Lookup(where.Value); found {
				for _, rowID := range rowIDs {
					if row, exists := state.GetRow(tableName, rowID); exists {
						rowsWithID = append(rowsWithID, storage.RowWithID{ID: rowID, Row: row})
					}
				}
			}
		} else {
			// No index, fall back to full scan with WHERE filter
			allRows := state.GetTableRows(tableName)
			for _, r := range allRows {
				if val, exists := r.Row[where.Column]; exists {
					if valuesEqual(val, where.Value) {
						rowsWithID = append(rowsWithID, r)
					}
				}
			}
		}
	} else {
		// No WHERE clause, return all rows
		rowsWithID = state.GetTableRows(tableName)
	}

	var rows []storage.Row
	for _, r := range rowsWithID {
		rows = append(rows, r.Row)
	}

	return rows, nil
}
