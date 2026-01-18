package database

import (
	"fmt"
	"rdbms/storage"
)

// Insert inserts a row into a table
// Now emits a ROW_INSERTED event instead of directly mutating storage
func (db *Database) Insert(tableName string, row storage.Row) (int64, error) {
	db.mu.Lock()
	defer db.mu.Unlock()

	table, err := db.catalog.GetTable(tableName)
	if err != nil {
		return 0, err
	}

	// Validate columns
	if err := db.validateRow(table, row); err != nil {
		return 0, err
	}

	// Get current state to check constraints
	state, err := db.queryEngine.GetCurrentState()
	if err != nil {
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

	// Generate row ID
	rowID := db.nextRowID[tableName]
	db.nextRowID[tableName]++

	// Record the insertion event
	txID := fmt.Sprintf("tx_%d", db.eventStore.GetLastEventID())
	_, err = db.eventStore.RecordRowInserted(tableName, rowID, row, txID)
	if err != nil {
		return 0, err
	}

	// Update indexes
	for colName, idx := range db.indexes[tableName] {
		if val, exists := row[colName]; exists {
			idx.Add(val, rowID)
		}
	}

	// Invalidate query cache
	db.queryEngine.InvalidateCache()

	// Check if we should create a snapshot
	lastEventID := db.eventStore.GetLastEventID()
	if lastEventID > 0 && lastEventID%uint64(db.snapshotInterval) == 0 {
		if state != nil {
			db.snapshotManager.CreateSnapshot(state, lastEventID, int64(lastEventID))
		}
	}

	return rowID, nil
}
