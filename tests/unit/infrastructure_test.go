package unit

import (
	"os"
	"path/filepath"
	"testing"

	"rdbms/catalog"
	"rdbms/eventlog"
	"rdbms/index"
	"rdbms/schema"
	"rdbms/tests"
)

// TestEventLogCreate tests creating a new event log
func TestEventLogCreate(t *testing.T) {
	tempDir := t.TempDir()

	log, err := eventlog.NewLog(tempDir, "test_events.jsonl")
	if err != nil {
		t.Fatalf("failed to create event log: %v", err)
	}
	defer log.Close()

	logPath := filepath.Join(tempDir, "test_events.jsonl")
	if !tests.FileExists(logPath) {
		t.Error("event log file was not created")
	}
}

// TestEventLogRead tests reading events from the log
func TestEventLogRead(t *testing.T) {
	tempDir := t.TempDir()

	log, err := eventlog.NewLog(tempDir, "test_events.jsonl")
	if err != nil {
		t.Fatalf("failed to create event log: %v", err)
	}
	defer log.Close()

	// Log file should exist after creation
	logPath := filepath.Join(tempDir, "test_events.jsonl")
	if !tests.FileExists(logPath) {
		t.Error("event log file was not created")
	}
}

// TestEventLogPersistence tests that event log persists
func TestEventLogPersistence(t *testing.T) {
	tempDir := t.TempDir()

	// Create log and verify file exists
	{
		log, err := eventlog.NewLog(tempDir, "test_events.jsonl")
		if err != nil {
			t.Fatalf("failed to create event log: %v", err)
		}
		log.Close()
	}

	// Open log again and verify file still exists
	{
		log, err := eventlog.NewLog(tempDir, "test_events.jsonl")
		if err != nil {
			t.Fatalf("failed to open existing event log: %v", err)
		}
		defer log.Close()

		logPath := filepath.Join(tempDir, "test_events.jsonl")
		if !tests.FileExists(logPath) {
			t.Error("event log file was not persisted")
		}
	}
}

// TestIndexCreate tests creating an index
func TestIndexCreate(t *testing.T) {
	idx := index.New("name")

	if idx.Column != "name" {
		t.Errorf("expected column name 'name', got %s", idx.Column)
	}

	if idx.Data == nil {
		t.Error("expected Data to be initialized")
	}
}

// TestIndexAdd tests adding values to an index
func TestIndexAdd(t *testing.T) {
	idx := index.New("email")

	idx.Add("alice@example.com", 1)
	idx.Add("bob@example.com", 2)
	idx.Add("alice@example.com", 3)

	// Verify data structure
	if len(idx.Data) != 2 {
		t.Errorf("expected 2 unique values, got %d", len(idx.Data))
	}

	// Check that duplicate values are stored
	aliceRowIDs := idx.Data["alice@example.com"]
	if len(aliceRowIDs) != 2 {
		t.Errorf("expected 2 row IDs for alice, got %d", len(aliceRowIDs))
	}
}

// TestIndexLookup tests looking up values in an index
func TestIndexLookup(t *testing.T) {
	idx := index.New("status")

	idx.Add("active", 1)
	idx.Add("active", 2)
	idx.Add("inactive", 3)

	rowIDs, found := idx.Lookup("active")
	if !found {
		t.Error("expected to find 'active' key")
	}

	if len(rowIDs) != 2 {
		t.Errorf("expected 2 rows for active, got %d", len(rowIDs))
	}

	// Test non-existent key
	rowIDs, found = idx.Lookup("nonexistent")
	if found {
		t.Error("expected not to find 'nonexistent' key")
	}
}

// TestIndexRemove tests removing values from an index
func TestIndexRemove(t *testing.T) {
	idx := index.New("id")

	idx.Add(1, 100)
	idx.Add(1, 101)
	idx.Add(2, 200)

	// Remove one row ID
	idx.Remove(1, 100)

	rowIDs, found := idx.Lookup(1)
	if !found || len(rowIDs) != 1 {
		t.Errorf("expected 1 remaining row ID for key 1, got %d", len(rowIDs))
	}

	// Remove all row IDs
	idx.Remove(1, 101)
	rowIDs, found = idx.Lookup(1)
	if found {
		t.Error("expected key 1 to be fully removed")
	}
}

// TestCatalogCreate tests creating a catalog
func TestCatalogCreate(t *testing.T) {
	tempDir := t.TempDir()

	cat, err := catalog.New(tempDir)
	if err != nil {
		t.Fatalf("failed to create catalog: %v", err)
	}

	if cat == nil {
		t.Fatal("expected catalog instance")
	}
}

// TestCatalogRegisterTable tests registering a table in the catalog
func TestCatalogRegisterTable(t *testing.T) {
	tempDir := t.TempDir()

	cat, err := catalog.New(tempDir)
	if err != nil {
		t.Fatalf("failed to create catalog: %v", err)
	}

	columns := []schema.Column{
		{Name: "id", Type: schema.TypeInt, PrimaryKey: true},
		{Name: "name", Type: schema.TypeText, PrimaryKey: false},
	}

	err = cat.CreateTable("users", columns)
	if err != nil {
		t.Fatalf("failed to create table: %v", err)
	}
}

// TestCatalogGetTable tests retrieving a table from the catalog
func TestCatalogGetTable(t *testing.T) {
	tempDir := t.TempDir()

	cat, err := catalog.New(tempDir)
	if err != nil {
		t.Fatalf("failed to create catalog: %v", err)
	}

	columns := []schema.Column{
		{Name: "id", Type: schema.TypeInt, PrimaryKey: true},
		{Name: "name", Type: schema.TypeText, PrimaryKey: false},
		{Name: "price", Type: schema.TypeInt, PrimaryKey: false},
	}

	err = cat.CreateTable("products", columns)
	if err != nil {
		t.Fatalf("failed to create table: %v", err)
	}

	retrieved, err := cat.GetTable("products")
	if err != nil {
		t.Fatalf("failed to get table: %v", err)
	}

	if retrieved == nil {
		t.Fatal("expected table to exist")
	}

	if retrieved.Name != "products" {
		t.Errorf("expected table name 'products', got %s", retrieved.Name)
	}

	if len(retrieved.Columns) != 3 {
		t.Errorf("expected 3 columns, got %d", len(retrieved.Columns))
	}
}

// TestCatalogPersistence tests that catalog persists to disk
func TestCatalogPersistence(t *testing.T) {
	tempDir := t.TempDir()

	// Register table in first catalog instance
	{
		cat, err := catalog.New(tempDir)
		if err != nil {
			t.Fatalf("failed to create catalog: %v", err)
		}

		columns := []schema.Column{
			{Name: "id", Type: schema.TypeInt, PrimaryKey: true},
			{Name: "name", Type: schema.TypeText, PrimaryKey: false},
		}

		err = cat.CreateTable("users", columns)
		if err != nil {
			t.Fatalf("failed to create table: %v", err)
		}
	}

	// Open catalog again and verify table persists
	{
		cat, err := catalog.New(tempDir)
		if err != nil {
			t.Fatalf("failed to open catalog: %v", err)
		}

		retrieved, err := cat.GetTable("users")
		if err != nil {
			t.Fatalf("failed to get table: %v", err)
		}

		if retrieved == nil {
			t.Fatal("expected table to persist in catalog")
		}

		if retrieved.Name != "users" {
			t.Errorf("expected table name 'users', got %s", retrieved.Name)
		}
	}
}

// TestCatalogMultipleTables tests managing multiple tables
func TestCatalogMultipleTables(t *testing.T) {
	tempDir := t.TempDir()

	cat, err := catalog.New(tempDir)
	if err != nil {
		t.Fatalf("failed to create catalog: %v", err)
	}

	tableNames := []string{"users", "orders", "products"}
	for _, name := range tableNames {
		cols := []schema.Column{
			{Name: "id", Type: schema.TypeInt, PrimaryKey: true},
		}
		err := cat.CreateTable(name, cols)
		if err != nil {
			t.Fatalf("failed to create table %s: %v", name, err)
		}
	}

	// Verify all tables can be retrieved
	for _, expectedName := range tableNames {
		retrieved, err := cat.GetTable(expectedName)
		if err != nil {
			t.Errorf("failed to get table %s: %v", expectedName, err)
			continue
		}

		if retrieved == nil {
			t.Errorf("expected table %s to exist", expectedName)
		}
	}
}

// TestCatalogGetNonexistentTable tests getting non-existent table
func TestCatalogGetNonexistentTable(t *testing.T) {
	tempDir := t.TempDir()

	cat, err := catalog.New(tempDir)
	if err != nil {
		t.Fatalf("failed to create catalog: %v", err)
	}

	_, err = cat.GetTable("nonexistent")
	if err == nil {
		t.Error("expected error when getting non-existent table")
	}
}

// TestCatalogFilePath verifies catalog file location
func TestCatalogFilePath(t *testing.T) {
	tempDir := t.TempDir()

	cat, err := catalog.New(tempDir)
	if err != nil {
		t.Fatalf("failed to create catalog: %v", err)
	}

	// Create a table so catalog is persisted
	cols := []schema.Column{
		{Name: "id", Type: schema.TypeInt, PrimaryKey: true},
	}

	err = cat.CreateTable("test", cols)
	if err != nil {
		t.Fatalf("failed to create table: %v", err)
	}

	// Check that catalog file exists
	catalogPath := filepath.Join(tempDir, "_catalog.json")
	if _, err := os.Stat(catalogPath); os.IsNotExist(err) {
		t.Error("catalog file was not created on disk")
	}
}
