package database

import (
	"fmt"
	"rdbms/schema"
	"rdbms/storage"
)

// CreateTable creates a new table
func (db *Database) CreateTable(tableName string, columns []schema.Column) error {
	if err := db.catalog.CreateTable(tableName, columns); err != nil {
		return err
	}

	// Create indexes for PK and unique columns
	db.indexes[tableName] = make(map[string]*index.Index)
	for _, col := range columns {
		if col.PrimaryKey || col.Unique {
			db.indexes[tableName][col.Name] = index.New(col.Name)
		}
	}

	return nil
}

// GetTable retrieves a table schema
func (db *Database) GetTable(tableName string) (*schema.Table, error) {
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
