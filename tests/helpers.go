package tests

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"rdbms/catalog"
	"rdbms/database"
	"rdbms/parser"
	"rdbms/schema"
)

// TestDB is a helper struct for database testing
type TestDB struct {
	DB      *database.Database
	DataDir string
	T       *testing.T
}

// NewTestDB creates a new database instance for testing with a temporary directory
func NewTestDB(t *testing.T) *TestDB {
	tempDir := t.TempDir()

	db, err := database.New(tempDir)
	if err != nil {
		t.Fatalf("failed to create test database: %v", err)
	}

	return &TestDB{
		DB:      db,
		DataDir: tempDir,
		T:       t,
	}
}

// CreateTable is a helper to create a table for testing
func (tdb *TestDB) CreateTable(name string, columns []schema.Column, primaryKey string) error {
	return tdb.DB.CreateTable(name, columns)
}

// InsertRow inserts a row into a table
func (tdb *TestDB) InsertRow(tableName string, row map[string]interface{}) (int64, error) {
	rowData := make(map[string]interface{})
	for k, v := range row {
		rowData[k] = v
	}
	return tdb.DB.Insert(tableName, rowData)
}

// SelectAll retrieves all rows from a table
func (tdb *TestDB) SelectAll(tableName string) ([]map[string]interface{}, error) {
	rows, err := tdb.DB.Select(tableName, nil)
	if err != nil {
		return nil, err
	}
	// Convert storage.Row to map[string]interface{}
	results := make([]map[string]interface{}, len(rows))
	for i, row := range rows {
		results[i] = map[string]interface{}(row)
	}
	return results, nil
}

// SelectWhere retrieves rows matching a condition
func (tdb *TestDB) SelectWhere(tableName, column string, value interface{}) ([]map[string]interface{}, error) {
	where := &parser.WhereClause{
		Column: column,
		Value:  value,
	}
	rows, err := tdb.DB.Select(tableName, where)
	if err != nil {
		return nil, err
	}
	// Convert storage.Row to map[string]interface{}
	results := make([]map[string]interface{}, len(rows))
	for i, row := range rows {
		results[i] = map[string]interface{}(row)
	}
	return results, nil
}

// UpdateRows updates rows in a table (single column update)
func (tdb *TestDB) UpdateRows(tableName string, setValues map[string]interface{}, whereValues map[string]interface{}) (int, error) {
	// Extract first set column and value
	var setColumn string
	var setValue interface{}
	for col, val := range setValues {
		setColumn = col
		setValue = val
		break
	}

	// Extract where clause
	var where *parser.WhereClause
	if len(whereValues) > 0 {
		for col, val := range whereValues {
			where = &parser.WhereClause{
				Column: col,
				Value:  val,
			}
			break
		}
	}

	return tdb.DB.Update(tableName, setColumn, setValue, where)
}

// DeleteRows deletes rows from a table
func (tdb *TestDB) DeleteRows(tableName string, whereValues map[string]interface{}) (int, error) {
	// Extract where clause
	var where *parser.WhereClause
	if len(whereValues) > 0 {
		for col, val := range whereValues {
			where = &parser.WhereClause{
				Column: col,
				Value:  val,
			}
			break
		}
	}

	return tdb.DB.Delete(tableName, where)
}

// GetTable retrieves table metadata
func (tdb *TestDB) GetTable(tableName string) (*schema.Table, error) {
	// For now, we'll create a placeholder table info
	// A proper implementation would need Database to expose catalog access
	rows, err := tdb.SelectAll(tableName)
	if err != nil {
		return nil, err
	}
	if len(rows) == 0 {
		return nil, fmt.Errorf("table '%s' not found or empty", tableName)
	}
	// Return a minimal table representation
	return &schema.Table{Name: tableName}, nil
}

// AssertRowCount verifies the number of rows in a table
func (tdb *TestDB) AssertRowCount(tableName string, expectedCount int) {
	rows, err := tdb.SelectAll(tableName)
	if err != nil {
		tdb.T.Fatalf("failed to select from %s: %v", tableName, err)
	}
	if len(rows) != expectedCount {
		tdb.T.Errorf("expected %d rows in %s, got %d", expectedCount, tableName, len(rows))
	}
}

// AssertRowExists verifies a row exists with given column value
func (tdb *TestDB) AssertRowExists(tableName, column string, value interface{}) {
	rows, err := tdb.SelectWhere(tableName, column, value)
	if err != nil {
		tdb.T.Fatalf("failed to select from %s: %v", tableName, err)
	}
	if len(rows) == 0 {
		tdb.T.Errorf("expected row with %s=%v in %s, but none found", column, value, tableName)
	}
}

// Cleanup closes the database and cleans up temporary files
func (tdb *TestDB) Cleanup() {
	if tdb.DB != nil {
		tdb.DB.Close()
	}
	if tdb.DataDir != "" {
		os.RemoveAll(tdb.DataDir)
	}
}

// TestCatalog is a helper struct for catalog testing
type TestCatalog struct {
	Catalog *catalog.Catalog
	DataDir string
	T       *testing.T
}

// NewTestCatalog creates a new catalog instance for testing
func NewTestCatalog(t *testing.T) *TestCatalog {
	tempDir := t.TempDir()

	cat, err := catalog.New(tempDir)
	if err != nil {
		t.Fatalf("failed to create test catalog: %v", err)
	}

	return &TestCatalog{
		Catalog: cat,
		DataDir: tempDir,
		T:       t,
	}
}

// RegisterTable adds a table to the catalog
func (tc *TestCatalog) RegisterTable(tableName string, columns []schema.Column) error {
	return tc.Catalog.CreateTable(tableName, columns)
}

// GetTable retrieves a table from the catalog
func (tc *TestCatalog) GetTable(name string) (*schema.Table, error) {
	return tc.Catalog.GetTable(name)
}

// Cleanup cleans up the test catalog
func (tc *TestCatalog) Cleanup() {
	if tc.DataDir != "" {
		os.RemoveAll(tc.DataDir)
	}
}

// SampleTableColumns returns a sample table with common columns for testing
func SampleTableColumns() []schema.Column {
	return []schema.Column{
		{Name: "id", Type: schema.TypeInt, PrimaryKey: true, Unique: true},
		{Name: "name", Type: schema.TypeText, PrimaryKey: false, Unique: false},
		{Name: "age", Type: schema.TypeInt, PrimaryKey: false, Unique: false},
	}
}

// SampleTaskTableColumns returns columns for a task table
func SampleTaskTableColumns() []schema.Column {
	return []schema.Column{
		{Name: "id", Type: schema.TypeInt, PrimaryKey: true, Unique: true},
		{Name: "title", Type: schema.TypeText, PrimaryKey: false, Unique: false},
		{Name: "completed", Type: schema.TypeBool, PrimaryKey: false, Unique: false},
	}
}

// SampleUserRow returns sample user data
func SampleUserRow(id int, name string, age int) map[string]interface{} {
	return map[string]interface{}{
		"id":   id,
		"name": name,
		"age":  age,
	}
}

// SampleTaskRow returns sample task data
func SampleTaskRow(id int, title string, completed bool) map[string]interface{} {
	return map[string]interface{}{
		"id":        id,
		"title":     title,
		"completed": completed,
	}
}

// EnsureDir creates a directory if it doesn't exist
func EnsureDir(path string) error {
	return os.MkdirAll(path, 0755)
}

// FileExists checks if a file exists
func FileExists(path string) bool {
	_, err := os.Stat(path)
	return err == nil
}

// ReadFileContent reads entire file content
func ReadFileContent(path string) (string, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return "", err
	}
	return string(data), nil
}

// AssertFileExists verifies a file exists and returns its path
func AssertFileExists(t *testing.T, path string) {
	if !FileExists(path) {
		t.Errorf("expected file to exist at %s, but it does not", path)
	}
}

// AssertFileNotExists verifies a file does not exist
func AssertFileNotExists(t *testing.T, path string) {
	if FileExists(path) {
		t.Errorf("expected file not to exist at %s, but it does", path)
	}
}

// GetEventLogPath returns the path to the event log
func GetEventLogPath(dataDir string) string {
	return filepath.Join(dataDir, "events.jsonl")
}

// GetCatalogPath returns the path to the catalog file
func GetCatalogPath(dataDir string) string {
	return filepath.Join(dataDir, "_catalog.json")
}

// GetSnapshotDir returns the path to the snapshots directory
func GetSnapshotDir(dataDir string) string {
	return filepath.Join(dataDir, "snapshots")
}

// AssertEqual checks if two values are equal
func AssertEqual(t *testing.T, expected, actual interface{}, message string) {
	if expected != actual {
		t.Errorf("%s: expected %v, got %v", message, expected, actual)
	}
}

// AssertNoError fails the test if an error is not nil
func AssertNoError(t *testing.T, err error, message string) {
	if err != nil {
		t.Fatalf("%s: %v", message, err)
	}
}

// AssertError fails the test if an error is nil
func AssertError(t *testing.T, err error, message string) {
	if err == nil {
		t.Fatalf("%s: expected error but got nil", message)
	}
}

// AssertErrorContains checks if error message contains a substring
func AssertErrorContains(t *testing.T, err error, substr string) {
	if err == nil {
		t.Fatalf("expected error containing %q, but got nil", substr)
	}
	if !contains(err.Error(), substr) {
		t.Errorf("expected error to contain %q, got: %v", substr, err)
	}
}

// Helper function
func contains(s, substr string) bool {
	for i := 0; i <= len(s)-len(substr); i++ {
		if s[i:i+len(substr)] == substr {
			return true
		}
	}
	return false
}
