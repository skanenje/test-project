package database

import (
	"os"
	"testing"

	"rdbms/parser"
	"rdbms/schema"
	"rdbms/storage"
)

func TestDatabaseWithEventLog(t *testing.T) {
	tmpDir := t.TempDir()
	defer os.RemoveAll(tmpDir)

	// Create database with event log backend
	db, err := New(tmpDir)
	if err != nil {
		t.Fatalf("Failed to create database: %v", err)
	}
	defer db.Close()

	// Create table
	columns := []schema.Column{
		{Name: "id", Type: schema.TypeInt, PrimaryKey: true},
		{Name: "name", Type: schema.TypeText},
		{Name: "active", Type: schema.TypeBool},
	}

	err = db.CreateTable("users", columns)
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	// Verify table exists
	table, err := db.GetTable("users")
	if err != nil {
		t.Fatalf("Failed to get table: %v", err)
	}

	if table.Name != "users" {
		t.Errorf("Expected table name 'users', got %s", table.Name)
	}

	// Verify schema creation event was recorded
	events, _ := db.eventStore.GetAllEvents()
	if len(events) == 0 {
		t.Error("No events recorded for schema creation")
	}

	t.Log("✓ Database with event log test passed")
}

func TestInsertWithEvents(t *testing.T) {
	tmpDir := t.TempDir()
	defer os.RemoveAll(tmpDir)

	db, _ := New(tmpDir)
	defer db.Close()

	// Setup
	columns := []schema.Column{
		{Name: "id", Type: schema.TypeInt, PrimaryKey: true},
		{Name: "name", Type: schema.TypeText},
	}
	db.CreateTable("users", columns)

	// Insert rows
	row1 := storage.Row{"id": float64(1), "name": "Alice"}
	rowID1, err := db.Insert("users", row1)
	if err != nil {
		t.Fatalf("Failed to insert row: %v", err)
	}

	row2 := storage.Row{"id": float64(2), "name": "Bob"}
	rowID2, err := db.Insert("users", row2)
	if err != nil {
		t.Fatalf("Failed to insert row: %v", err)
	}

	// Verify events were recorded
	events, _ := db.eventStore.GetAllEvents()
	insertEvents := 0
	for _, e := range events {
		if e.Type == "ROW_INSERTED" {
			insertEvents++
		}
	}

	if insertEvents != 2 {
		t.Errorf("Expected 2 insert events, got %d", insertEvents)
	}

	// Query inserted rows
	results, err := db.Select("users", nil)
	if err != nil {
		t.Fatalf("Failed to select rows: %v", err)
	}

	if len(results) != 2 {
		t.Errorf("Expected 2 rows, got %d", len(results))
	}

	t.Logf("✓ Insert with events test passed (rowIDs: %d, %d)", rowID1, rowID2)
}

func TestUpdateWithEvents(t *testing.T) {
	tmpDir := t.TempDir()
	defer os.RemoveAll(tmpDir)

	db, _ := New(tmpDir)
	defer db.Close()

	// Setup
	columns := []schema.Column{
		{Name: "id", Type: schema.TypeInt, PrimaryKey: true},
		{Name: "name", Type: schema.TypeText},
		{Name: "age", Type: schema.TypeInt},
	}
	db.CreateTable("users", columns)

	// Insert row
	row := storage.Row{"id": float64(1), "name": "Alice", "age": float64(30)}
	db.Insert("users", row)

	// Update row
	where := &parser.WhereClause{Column: "id", Value: float64(1)}
	count, err := db.Update("users", "age", float64(31), where)
	if err != nil {
		t.Fatalf("Failed to update row: %v", err)
	}

	if count != 1 {
		t.Errorf("Expected 1 row updated, got %d", count)
	}

	// Verify update event was recorded
	events, _ := db.eventStore.GetAllEvents()
	updateEvents := 0
	for _, e := range events {
		if e.Type == "ROW_UPDATED" {
			updateEvents++
		}
	}

	if updateEvents != 1 {
		t.Errorf("Expected 1 update event, got %d", updateEvents)
	}

	t.Log("✓ Update with events test passed")
}

func TestDeleteWithEvents(t *testing.T) {
	tmpDir := t.TempDir()
	defer os.RemoveAll(tmpDir)

	db, _ := New(tmpDir)
	defer db.Close()

	// Setup
	columns := []schema.Column{
		{Name: "id", Type: schema.TypeInt, PrimaryKey: true},
		{Name: "name", Type: schema.TypeText},
	}
	db.CreateTable("users", columns)

	// Insert rows
	db.Insert("users", storage.Row{"id": float64(1), "name": "Alice"})
	db.Insert("users", storage.Row{"id": float64(2), "name": "Bob"})
	db.Insert("users", storage.Row{"id": float64(3), "name": "Charlie"})

	// Delete one row
	where := &parser.WhereClause{Column: "id", Value: float64(2)}
	count, err := db.Delete("users", where)
	if err != nil {
		t.Fatalf("Failed to delete row: %v", err)
	}

	if count != 1 {
		t.Errorf("Expected 1 row deleted, got %d", count)
	}

	// Verify delete event was recorded
	events, _ := db.eventStore.GetAllEvents()
	deleteEvents := 0
	for _, e := range events {
		if e.Type == "ROW_DELETED" {
			deleteEvents++
		}
	}

	if deleteEvents != 1 {
		t.Errorf("Expected 1 delete event, got %d", deleteEvents)
	}

	// Query remaining rows
	results, _ := db.Select("users", nil)
	if len(results) != 2 {
		t.Errorf("Expected 2 remaining rows, got %d", len(results))
	}

	t.Log("✓ Delete with events test passed")
}

func TestStateRecoveryFromLog(t *testing.T) {
	tmpDir := t.TempDir()
	defer os.RemoveAll(tmpDir)

	// Create database and data
	{
		db, _ := New(tmpDir)

		columns := []schema.Column{
			{Name: "id", Type: schema.TypeInt, PrimaryKey: true},
			{Name: "value", Type: schema.TypeInt},
		}
		db.CreateTable("data", columns)

		for i := 1; i <= 5; i++ {
			db.Insert("data", storage.Row{"id": float64(i), "value": float64(i * 10)})
		}

		db.Close()
	}

	// Reopen database - state should be recovered from log
	{
		db, _ := New(tmpDir)
		defer db.Close()

		results, err := db.Select("data", nil)
		if err != nil {
			t.Fatalf("Failed to select rows after recovery: %v", err)
		}

		if len(results) != 5 {
			t.Errorf("Expected 5 rows after recovery, got %d", len(results))
		}

		// Verify correct data was restored
		for i, row := range results {
			value := row["value"].(float64)
			expectedValue := float64((i + 1) * 10)

			if value != expectedValue {
				t.Errorf("Row %d: expected value %.0f, got %.0f", i+1, expectedValue, value)
			}
		}

		t.Log("✓ State recovery from log test passed")
	}
}
