package database

import (
	"rdbms/index"
	"rdbms/schema"
)

// rebuildAllIndexes rebuilds indexes for all tables from current state
func (db *Database) rebuildAllIndexes() error {
	tables := db.catalog.GetAllTables()
	for tableName, table := range tables {
		if err := db.rebuildIndexes(tableName, table); err != nil {
			return err
		}
	}
	return nil
}

// rebuildIndexes rebuilds indexes for a specific table from event-derived state
func (db *Database) rebuildIndexes(tableName string, table *schema.Table) error {
	db.indexes[tableName] = make(map[string]*index.Index)
	db.nextRowID[tableName] = 0

	// Create index structures for PK and unique columns
	for _, col := range table.Columns {
		if col.PrimaryKey || col.Unique {
			db.indexes[tableName][col.Name] = index.New(col.Name)
		}
	}

	// Populate indexes from current derived state
	state, err := db.queryEngine.GetCurrentState()
	if err != nil {
		return err
	}

	rows := state.GetTableRows(tableName)
	for _, r := range rows {
		// Track next row ID
		if r.ID >= db.nextRowID[tableName] {
			db.nextRowID[tableName] = r.ID + 1
		}

		// Add to indexes
		for colName, idx := range db.indexes[tableName] {
			if val, exists := r.Row[colName]; exists {
				idx.Add(val, r.ID)
			}
		}
	}

	return nil
}
