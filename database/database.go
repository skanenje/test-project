package database

import (
	"fmt"
	"rdbms/catalog"
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

// SelectAll selects all rows from a table
func (db *Database) SelectAll(tableName string) ([]storage.Row, error) {
	if !db.catalog.TableExists(tableName) {
		return nil, fmt.Errorf("table '%s' does not exist", tableName)
	}

	rowsWithID, err := db.storage.ScanAll(tableName)
	if err != nil {
		return nil, err
	}

	rows := make([]storage.Row, len(rowsWithID))
	for i, r := range rowsWithID {
		rows[i] = r.Row
	}

	return rows, nil
}

// Delete deletes a row by ID
func (db *Database) Delete(tableName string, rowID int64) error {
	if !db.catalog.TableExists(tableName) {
		return fmt.Errorf("table '%s' does not exist", tableName)
	}
	return db.storage.DeleteRow(tableName, rowID)
}

// Update updates a row
func (db *Database) Update(tableName string, rowID int64, newData storage.Row) (int64, error) {
	table, err := db.catalog.GetTable(tableName)
	if err != nil {
		return 0, err
	}

	if err := db.validateRow(table, newData); err != nil {
		return 0, err
	}

	return db.storage.UpdateRow(tableName, rowID, newData)
}
