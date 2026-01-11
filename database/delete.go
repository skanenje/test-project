package database

import (
	"fmt"
	"rdbms/parser"
)

// Delete deletes rows matching WHERE clause
func (db *Database) Delete(tableName string, where *parser.WhereClause) (int, error) {
	if !db.catalog.TableExists(tableName) {
		return 0, fmt.Errorf("table '%s' does not exist", tableName)
	}

	if where == nil {
		return 0, fmt.Errorf("DELETE requires WHERE clause")
	}

	rows, err := db.storage.ScanAll(tableName)
	if err != nil {
		return 0, err
	}

	count := 0
	for _, r := range rows {
		if val, exists := r.Row[where.Column]; exists && valuesEqual(val, where.Value) {
			// Remove from indexes
			for colName, idx := range db.indexes[tableName] {
				if colVal, exists := r.Row[colName]; exists {
					idx.Remove(colVal, r.ID)
				}
			}

			db.storage.DeleteRow(tableName, r.ID)
			count++
		}
	}

	return count, nil
}
