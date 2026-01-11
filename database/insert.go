package database

import (
	"fmt"
	"rdbms/storage"
)

// Insert inserts a row into a table
func (db *Database) Insert(tableName string, row storage.Row) (int64, error) {
	table, err := db.catalog.GetTable(tableName)
	if err != nil {
		return 0, err
	}

	// Validate columns
	if err := db.validateRow(table, row); err != nil {
		return 0, err
	}

	// Check primary key uniqueness
	if table.PrimaryKey != "" {
		if idx, exists := db.indexes[tableName][table.PrimaryKey]; exists {
			pkValue := row[table.PrimaryKey]
			if idx.Exists(pkValue) {
				return 0, fmt.Errorf("primary key violation: duplicate value '%v'", pkValue)
			}
		}
	}

	// Check unique constraints
	for _, col := range table.Columns {
		if col.Unique && !col.PrimaryKey {
			if idx, exists := db.indexes[tableName][col.Name]; exists {
				value := row[col.Name]
				if idx.Exists(value) {
					return 0, fmt.Errorf("unique constraint violation on column '%s'", col.Name)
				}
			}
		}
	}

	// Insert row
	rowID, err := db.storage.InsertRow(tableName, row)
	if err != nil {
		return 0, err
	}

	// Update indexes
	for colName, idx := range db.indexes[tableName] {
		if val, exists := row[colName]; exists {
			idx.Add(val, rowID)
		}
	}

	return rowID, nil
}
