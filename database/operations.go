package database

import (
	"fmt"
	"rdbms/eventlog"
	"rdbms/index"
	"rdbms/schema"
	"rdbms/storage"
)

// CreateTable creates a new table
// Now emits SCHEMA_CREATED event in addition to catalog entry
func (db *Database) CreateTable(tableName string, columns []schema.Column) error {
	db.mu.Lock()
	defer db.mu.Unlock()

	if err := db.catalog.CreateTable(tableName, columns); err != nil {
		return err
	}

	// Create indexes for PK and unique columns
	db.indexes[tableName] = make(map[string]*index.Index)
	db.nextRowID[tableName] = 0 // Initialize next row ID

	for _, col := range columns {
		if col.PrimaryKey || col.Unique {
			db.indexes[tableName][col.Name] = index.New(col.Name)
		}
	}

	// Record schema creation event
	colDefs := make([]eventlog.ColumnDefinition, len(columns))
	primaryKey := ""
	for i, col := range columns {
		colDefs[i] = eventlog.ColumnDefinition{
			Name:       col.Name,
			Type:       string(col.Type),
			Nullable:   true, // Default to nullable
			PrimaryKey: col.PrimaryKey,
			Unique:     col.Unique,
		}
		if col.PrimaryKey {
			primaryKey = col.Name
		}
	}

	txID := fmt.Sprintf("tx_%d", db.eventStore.GetLastEventID())
	_, err := db.eventStore.RecordSchemaCreated(tableName, colDefs, primaryKey, txID)
	if err != nil {
		return err
	}

	return nil
}

// GetTable retrieves a table schema
func (db *Database) GetTable(tableName string) (*schema.Table, error) {
	db.mu.RLock()
	defer db.mu.RUnlock()
	return db.catalog.GetTable(tableName)
}

// validateRow validates a row against schema
func (db *Database) validateRow(table *schema.Table, row storage.Row) error {
	// Check all schema columns are present
	for _, col := range table.Columns {
		val, exists := row[col.Name]
		if !exists {
			return fmt.Errorf("missing column: %s", col.Name)
		}

		// Basic type checking
		switch col.Type {
		case schema.TypeInt:
			if _, ok := val.(float64); !ok { // JSON numbers become float64
				return fmt.Errorf("column '%s' expects INT", col.Name)
			}
		case schema.TypeText:
			if _, ok := val.(string); !ok {
				return fmt.Errorf("column '%s' expects TEXT", col.Name)
			}
		case schema.TypeBool:
			if _, ok := val.(bool); !ok {
				return fmt.Errorf("column '%s' expects BOOL", col.Name)
			}
		}
	}

	// Check no extra columns
	if len(row) != len(table.Columns) {
		return fmt.Errorf("row has extra columns")
	}

	return nil
}

// Helper to compare values
func valuesEqual(a, b interface{}) bool {
	return fmt.Sprintf("%v", a) == fmt.Sprintf("%v", b)
}
