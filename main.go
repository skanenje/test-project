// Simple RDBMS - Phase 1: Storage Layer + Schema + Basic CRUD
//
// This module implements:
// - Table schema definition (columns + types)
// - Row storage (append-only binary file per table)
// - Basic CRUD: insert, scan all, delete by row ID

package main

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
)

// ColumnType represents supported data types
type ColumnType string

const (
	TypeInt  ColumnType = "INT"
	TypeText ColumnType = "TEXT"
	TypeBool ColumnType = "BOOL"
)

// Column defines a table column
type Column struct {
	Name string     `json:"name"`
	Type ColumnType `json:"type"`
}

// TableSchema holds table metadata
type TableSchema struct {
	Name    string   `json:"name"`
	Columns []Column `json:"columns"`
}

// Row represents a database row (map of column name to value)
type Row map[string]interface{}

// StorageEngine handles physical storage of rows on disk
//
// Format: Each row is stored as:
// [deleted_flag: 1 byte][data_length: 4 bytes][json_data: variable bytes]
//
// deleted_flag: 0 = active, 1 = deleted
// data_length: length of json_data in bytes (uint32)
// json_data: row data as JSON
type StorageEngine struct {
	dataDir string
}

// NewStorageEngine creates a new storage engine
func NewStorageEngine(dataDir string) (*StorageEngine, error) {
	if err := os.MkdirAll(dataDir, 0755); err != nil {
		return nil, err
	}
	return &StorageEngine{dataDir: dataDir}, nil
}

// tableFile returns the file path for a table's data
func (s *StorageEngine) tableFile(tableName string) string {
	return filepath.Join(s.dataDir, tableName+".db")
}

// InsertRow inserts a row and returns its row ID (byte offset)
func (s *StorageEngine) InsertRow(tableName string, row Row) (int64, error) {
	filepath := s.tableFile(tableName)

	// Serialize row as JSON
	jsonData, err := json.Marshal(row)
	if err != nil {
		return 0, err
	}

	// Open file for appending
	f, err := os.OpenFile(filepath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return 0, err
	}
	defer f.Close()

	// Get current position (this is the row ID)
	rowID, err := f.Seek(0, io.SeekEnd)
	if err != nil {
		return 0, err
	}

	// Write: [deleted_flag=0][length][data]
	deletedFlag := byte(0)
	dataLength := uint32(len(jsonData))

	if err := binary.Write(f, binary.LittleEndian, deletedFlag); err != nil {
		return 0, err
	}
	if err := binary.Write(f, binary.LittleEndian, dataLength); err != nil {
		return 0, err
	}
	if _, err := f.Write(jsonData); err != nil {
		return 0, err
	}

	return rowID, nil
}

// RowWithID pairs a row with its ID
type RowWithID struct {
	ID  int64
	Row Row
}

// ScanAll scans all rows in a table (skips deleted rows)
func (s *StorageEngine) ScanAll(tableName string) ([]RowWithID, error) {
	filepath := s.tableFile(tableName)

	// If file doesn't exist, return empty
	if _, err := os.Stat(filepath); os.IsNotExist(err) {
		return []RowWithID{}, nil
	}

	f, err := os.Open(filepath)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	var rows []RowWithID

	for {
		// Get current position (row ID)
		rowID, err := f.Seek(0, io.SeekCurrent)
		if err != nil {
			return nil, err
		}

		// Read deleted flag
		var deletedFlag byte
		if err := binary.Read(f, binary.LittleEndian, &deletedFlag); err != nil {
			if err == io.EOF {
				break
			}
			return nil, err
		}

		// Read data length
		var dataLength uint32
		if err := binary.Read(f, binary.LittleEndian, &dataLength); err != nil {
			return nil, err
		}

		// Read JSON data
		jsonData := make([]byte, dataLength)
		if _, err := io.ReadFull(f, jsonData); err != nil {
			return nil, err
		}

		// Skip deleted rows
		if deletedFlag == 0 {
			var row Row
			if err := json.Unmarshal(jsonData, &row); err != nil {
				return nil, err
			}
			rows = append(rows, RowWithID{ID: rowID, Row: row})
		}
	}

	return rows, nil
}

// DeleteRow marks a row as deleted
func (s *StorageEngine) DeleteRow(tableName string, rowID int64) error {
	filepath := s.tableFile(tableName)

	f, err := os.OpenFile(filepath, os.O_RDWR, 0644)
	if err != nil {
		return err
	}
	defer f.Close()

	// Seek to row ID and write deleted_flag = 1
	if _, err := f.Seek(rowID, io.SeekStart); err != nil {
		return err
	}

	deletedFlag := byte(1)
	return binary.Write(f, binary.LittleEndian, deletedFlag)
}

// UpdateRow updates a row by deleting old and inserting new
func (s *StorageEngine) UpdateRow(tableName string, rowID int64, newData Row) (int64, error) {
	// Mark old as deleted
	if err := s.DeleteRow(tableName, rowID); err != nil {
		return 0, err
	}
	// Insert new version
	return s.InsertRow(tableName, newData)
}

// Database is the main database interface
type Database struct {
	storage *StorageEngine
	schemas map[string]*TableSchema
	dataDir string
}

// NewDatabase creates a new database instance
func NewDatabase(dataDir string) (*Database, error) {
	storage, err := NewStorageEngine(dataDir)
	if err != nil {
		return nil, err
	}

	db := &Database{
		storage: storage,
		schemas: make(map[string]*TableSchema),
		dataDir: dataDir,
	}

	if err := db.loadCatalog(); err != nil {
		return nil, err
	}

	return db, nil
}

func (db *Database) catalogFile() string {
	return filepath.Join(db.dataDir, "_catalog.json")
}

// loadCatalog loads table schemas from disk
func (db *Database) loadCatalog() error {
	catalogPath := db.catalogFile()

	// If catalog doesn't exist, that's fine (fresh DB)
	if _, err := os.Stat(catalogPath); os.IsNotExist(err) {
		return nil
	}

	data, err := os.ReadFile(catalogPath)
	if err != nil {
		return err
	}

	var catalog map[string]*TableSchema
	if err := json.Unmarshal(data, &catalog); err != nil {
		return err
	}

	db.schemas = catalog
	return nil
}

// saveCatalog saves table schemas to disk
func (db *Database) saveCatalog() error {
	data, err := json.MarshalIndent(db.schemas, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(db.catalogFile(), data, 0644)
}

// CreateTable creates a new table
func (db *Database) CreateTable(tableName string, columns []Column) error {
	if _, exists := db.schemas[tableName]; exists {
		return fmt.Errorf("table '%s' already exists", tableName)
	}

	schema := &TableSchema{
		Name:    tableName,
		Columns: columns,
	}

	db.schemas[tableName] = schema
	return db.saveCatalog()
}

// Insert inserts a row into a table
func (db *Database) Insert(tableName string, row Row) (int64, error) {
	schema, exists := db.schemas[tableName]
	if !exists {
		return 0, fmt.Errorf("table '%s' does not exist", tableName)
	}

	// Validate columns
	if err := db.validateRow(schema, row); err != nil {
		return 0, err
	}

	return db.storage.InsertRow(tableName, row)
}

// validateRow validates a row against schema
func (db *Database) validateRow(schema *TableSchema, row Row) error {
	// Check all schema columns are present
	for _, col := range schema.Columns {
		val, exists := row[col.Name]
		if !exists {
			return fmt.Errorf("missing column: %s", col.Name)
		}

		// Basic type checking
		switch col.Type {
		case TypeInt:
			if _, ok := val.(float64); !ok { // JSON numbers become float64
				return fmt.Errorf("column '%s' expects INT", col.Name)
			}
		case TypeText:
			if _, ok := val.(string); !ok {
				return fmt.Errorf("column '%s' expects TEXT", col.Name)
			}
		case TypeBool:
			if _, ok := val.(bool); !ok {
				return fmt.Errorf("column '%s' expects BOOL", col.Name)
			}
		}
	}

	// Check no extra columns
	if len(row) != len(schema.Columns) {
		return fmt.Errorf("row has extra columns")
	}

	return nil
}

// SelectAll selects all rows from a table
func (db *Database) SelectAll(tableName string) ([]Row, error) {
	if _, exists := db.schemas[tableName]; !exists {
		return nil, fmt.Errorf("table '%s' does not exist", tableName)
	}

	rowsWithID, err := db.storage.ScanAll(tableName)
	if err != nil {
		return nil, err
	}

	rows := make([]Row, len(rowsWithID))
	for i, r := range rowsWithID {
		rows[i] = r.Row
	}

	return rows, nil
}

// Delete deletes a row by ID
func (db *Database) Delete(tableName string, rowID int64) error {
	if _, exists := db.schemas[tableName]; !exists {
		return fmt.Errorf("table '%s' does not exist", tableName)
	}
	return db.storage.DeleteRow(tableName, rowID)
}

// Update updates a row
func (db *Database) Update(tableName string, rowID int64, newData Row) (int64, error) {
	schema, exists := db.schemas[tableName]
	if !exists {
		return 0, fmt.Errorf("table '%s' does not exist", tableName)
	}

	if err := db.validateRow(schema, newData); err != nil {
		return 0, err
	}

	return db.storage.UpdateRow(tableName, rowID, newData)
}

// Demo usage
func main() {
	// Initialize database
	db, err := NewDatabase("./demo_data")
	if err != nil {
		panic(err)
	}

	// Create a users table
	err = db.CreateTable("users", []Column{
		{Name: "id", Type: TypeInt},
		{Name: "name", Type: TypeText},
		{Name: "active", Type: TypeBool},
	})
	if err != nil {
		panic(err)
	}
	fmt.Println("✓ Created table 'users'")

	// Insert rows
	rowID1, _ := db.Insert("users", Row{"id": 1.0, "name": "Alice", "active": true})
	rowID2, _ := db.Insert("users", Row{"id": 2.0, "name": "Bob", "active": false})
	rowID3, _ := db.Insert("users", Row{"id": 3.0, "name": "Charlie", "active": true})
	fmt.Printf("✓ Inserted 3 rows (row_ids: %d, %d, %d)\n", rowID1, rowID2, rowID3)

	// Select all
	fmt.Println("\n📊 SELECT * FROM users:")
	rows, _ := db.SelectAll("users")
	for _, row := range rows {
		fmt.Printf("  %v\n", row)
	}

	// Delete Bob
	db.Delete("users", rowID2)
	fmt.Printf("\n✓ Deleted row %d (Bob)\n", rowID2)

	// Select again
	fmt.Println("\n📊 SELECT * FROM users (after delete):")
	rows, _ = db.SelectAll("users")
