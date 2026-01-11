package database

import (
	"fmt"
	"rdbms/parser"
	"rdbms/storage"
)

// Select selects rows from a table with optional WHERE clause (uses index if available)
func (db *Database) Select(tableName string, where *parser.WhereClause) ([]storage.Row, error) {
	if !db.catalog.TableExists(tableName) {
		return nil, fmt.Errorf("table '%s' does not exist", tableName)
	}

	var rowsWithID []storage.RowWithID

	// Try to use index for WHERE clause
	if where != nil {
		if idx, hasIndex := db.indexes[tableName][where.Column]; hasIndex {
			// Index hit! Fetch only matching rows
			if rowIDs, found := idx.Lookup(where.Value); found {
				allRows, err := db.storage.ScanAll(tableName)
				if err != nil {
					return nil, err
				}

				rowIDMap := make(map[int64]bool)
				for _, id := range rowIDs {
					rowIDMap[id] = true
				}

				for _, r := range allRows {
					if rowIDMap[r.ID] {
						rowsWithID = append(rowsWithID, r)
					}
				}
			}
		} else {
			// No index, fall back to full scan
			allRows, err := db.storage.ScanAll(tableName)
			if err != nil {
				return nil, err
			}

			for _, r := range allRows {
				if val, exists := r.Row[where.Column]; exists {
					if valuesEqual(val, where.Value) {
						rowsWithID = append(rowsWithID, r)
					}
				}
			}
		}
	} else {
		// No WHERE clause, full scan
		var err error
		rowsWithID, err = db.storage.ScanAll(tableName)
		if err != nil {
			return nil, err
		}
	}

	var rows []storage.Row
	for _, r := range rowsWithID {
		rows = append(rows, r.Row)
	}

	return rows, nil
}
