package database

import (
	"rdbms/schema"
)

// rebuildAllIndexes rebuilds indexes for all tables
func (db *Database) rebuildAllIndexes() error {
	tables := db.catalog.GetAllTables()
	for tableName, table := range tables {
		if err := db.rebuildIndexes(tableName, table); err != nil {
			return err
		}
	}
	return nil
}

// rebuildIndexes rebuilds indexes for a specific table
func (db *Database) rebuildIndexes(tableName string, table *schema.Table) error {
	db.indexes[tableName] = make(map[string]*index.Index)

	// Create index structures for PK and unique columns
	for _, col := range table.Columns {
		if col.PrimaryKey || col.Unique {
			db.indexes[tableName][col.Name] = index.New(col.Name)
		}
	}

	// Populate indexes from existing data
	rows, err := db.storage.ScanAll(tableName)
	if err != nil {
		return err
	}

	for _, r := range rows {
		for colName, idx := range db.indexes[tableName] {
			if val, exists := r.Row[colName]; exists {
				idx.Add(val, r.ID)
			}
		}
	}

	return nil
}
