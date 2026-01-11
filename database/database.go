package database

import (
	"fmt"
	"rdbms/catalog"
	"rdbms/parser"
	"rdbms/schema"
	"rdbms/storage"
)

// Database is the main database interface
type Database struct {
	storage *storage.Engine
	catalog *catalog.Catalog
}

// New creates a new database instance
func New(dataDir string) (*Database, error) {
	storageEngine, err := storage.NewEngine(dataDir)
	if err != nil {
		return nil, err
	}

	cat, err := catalog.New(dataDir)
	if err != nil {
		return nil, err
	}

	return &Database{
		storage: storageEngine,
		catalog: cat,
	}, nil
}

// CreateTable creates a new table
func (db *Database) CreateTable(tableName string, columns []schema.Column) error {
	return db.catalog.CreateTable(tableName, columns)
}

// GetTable retrieves a table schema
func (db *Database) GetTable(tableName string) (*schema.Table, error) {
	return db.catalog.GetTable(tableName)
}

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

	return db.storage.InsertRow(tableName, row)
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

// Select selects rows from a table with optional WHERE clause
func (db *Database) Select(tableName string, where *parser.WhereClause) ([]storage.Row, error) {
	if !db.catalog.TableExists(tableName) {
		return nil, fmt.Errorf("table '%s' does not exist", tableName)
	}

	rowsWithID, err := db.storage.ScanAll(tableName)
	if err != nil {
		return nil, err
	}

	var rows []storage.Row
	for _, r := range rowsWithID {
		// Apply WHERE filter if present
		if where != nil {
			if val, exists := r.Row[where.Column]; exists {
				if !valuesEqual(val, where.Value) {
					continue
				}
			} else {
				continue
			}
		}
		rows = append(rows, r.Row)
	}

	return rows, nil
}

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
			db.storage.DeleteRow(tableName, r.ID)
			count++
		}
	}

	return count, nil
}

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
			newRow := make(storage.Row)
			for k, v := range r.Row {
				newRow[k] = v
			}
			newRow[setColumn] = setValue

			if err := db.validateRow(table, newRow); err != nil {
				return count, err
			}

			db.storage.UpdateRow(tableName, r.ID, newRow)
			count++
		}
	}

	return count, nil
}

// Helper to compare values
func valuesEqual(a, b interface{}) bool {
	return fmt.Sprintf("%v", a) == fmt.Sprintf("%v", b)
}
