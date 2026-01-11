package database

import (
	"fmt"
	"rdbms/parser"
	"rdbms/storage"
)

// Update updates rows matching WHERE clause
func (db *Database) Update(tableName string, setColumn string, setValue interface{}, where *parser.WhereClause) (int, error) {
	table, err := db.catalog.GetTable(tableName)
	if err != nil {
		return 0, err
	}

	if where == nil {
		return 0, fmt.Errorf("UPDATE requires WHERE clause")
	}

	rows, err := db.storage.ScanAll(tableName)
	if err != nil {
		return 0, err
	}

	count := 0
	for _, r := range rows {
		if val, exists := r.Row[where.Column]; exists && valuesEqual(val, where.Value) {
			// Remove old from indexes
			for colName, idx := range db.indexes[tableName] {
				if colVal, exists := r.Row[colName]; exists {
					idx.Remove(colVal, r.ID)
				}
			}

			newRow := make(storage.Row)
			for k, v := range r.Row {
				newRow[k] = v
			}
			newRow[setColumn] = setValue

			if err := db.validateRow(table, newRow); err != nil {
				return count, err
			}

			newRowID, _ := db.storage.UpdateRow(tableName, r.ID, newRow)

			// Add new to indexes
			for colName, idx := range db.indexes[tableName] {
				if colVal, exists := newRow[colName]; exists {
					idx.Add(colVal, newRowID)
				}
			}

			count++
		}
	}

	return count, nil
}
