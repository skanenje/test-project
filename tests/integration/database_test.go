package integration

import (
	"testing"

	"rdbms/schema"
	"rdbms/tests"
)

// TestDatabaseCreateTable tests table creation
func TestDatabaseCreateTable(t *testing.T) {
	tdb := tests.NewTestDB(t)
	defer tdb.Cleanup()

	columns := tests.SampleTableColumns()
	err := tdb.CreateTable("users", columns, "id")
	if err != nil {
		t.Fatalf("failed to create table: %v", err)
	}

	// Verify table exists
	table, err := tdb.GetTable("users")
	if err != nil {
		t.Fatalf("failed to get table: %v", err)
	}
	if table == nil {
		t.Fatal("expected table to exist")
	}
	if table.Name != "users" {
		t.Errorf("expected table name 'users', got %s", table.Name)
	}
	if len(table.Columns) != 3 {
		t.Errorf("expected 3 columns, got %d", len(table.Columns))
	}
}

// TestDatabaseInsertSingleRow tests inserting a single row
func TestDatabaseInsertSingleRow(t *testing.T) {
	tdb := tests.NewTestDB(t)
	defer tdb.Cleanup()

	columns := tests.SampleTableColumns()
	tdb.CreateTable("users", columns, "id")

	row := tests.SampleUserRow(1, "Alice", 30)
	rowID, err := tdb.InsertRow("users", row)
	if err != nil {
		t.Fatalf("failed to insert row: %v", err)
	}
	if rowID <= 0 {
		t.Errorf("expected positive row ID, got %d", rowID)
	}

	tdb.AssertRowCount("users", 1)
}

// TestDatabaseInsertMultipleRows tests inserting multiple rows
func TestDatabaseInsertMultipleRows(t *testing.T) {
	tdb := tests.NewTestDB(t)
	defer tdb.Cleanup()

	columns := tests.SampleTableColumns()
	tdb.CreateTable("users", columns, "id")

	rows := []map[string]interface{}{
		tests.SampleUserRow(1, "Alice", 30),
		tests.SampleUserRow(2, "Bob", 25),
		tests.SampleUserRow(3, "Charlie", 35),
	}

	for _, row := range rows {
		_, err := tdb.InsertRow("users", row)
		if err != nil {
			t.Fatalf("failed to insert row: %v", err)
		}
	}

	tdb.AssertRowCount("users", 3)
}

// TestDatabaseSelectAll tests selecting all rows
func TestDatabaseSelectAll(t *testing.T) {
	tdb := tests.NewTestDB(t)
	defer tdb.Cleanup()

	columns := tests.SampleTableColumns()
	tdb.CreateTable("users", columns, "id")

	// Insert test data
	rows := []map[string]interface{}{
		tests.SampleUserRow(1, "Alice", 30),
		tests.SampleUserRow(2, "Bob", 25),
	}
	for _, row := range rows {
		tdb.InsertRow("users", row)
	}

	// Select all
	results, err := tdb.SelectAll("users")
	if err != nil {
		t.Fatalf("failed to select all: %v", err)
	}
	if len(results) != 2 {
		t.Errorf("expected 2 rows, got %d", len(results))
	}
}

// TestDatabaseSelectWithWhere tests filtering with WHERE clause
func TestDatabaseSelectWithWhere(t *testing.T) {
	tdb := tests.NewTestDB(t)
	defer tdb.Cleanup()

	columns := tests.SampleTableColumns()
	tdb.CreateTable("users", columns, "id")

	// Insert test data
	rows := []map[string]interface{}{
		tests.SampleUserRow(1, "Alice", 30),
		tests.SampleUserRow(2, "Bob", 25),
		tests.SampleUserRow(3, "Alice", 35),
	}
	for _, row := range rows {
		tdb.InsertRow("users", row)
	}

	// Select where name = 'Alice'
	results, err := tdb.SelectWhere("users", "name", "Alice")
	if err != nil {
		t.Fatalf("failed to select with where: %v", err)
	}
	if len(results) != 2 {
		t.Errorf("expected 2 rows with name='Alice', got %d", len(results))
	}
}

// TestDatabaseUpdate tests updating rows
func TestDatabaseUpdate(t *testing.T) {
	tdb := tests.NewTestDB(t)
	defer tdb.Cleanup()

	columns := tests.SampleTableColumns()
	tdb.CreateTable("users", columns, "id")

	// Insert test data
	row := tests.SampleUserRow(1, "Alice", 30)
	tdb.InsertRow("users", row)

	// Update row
	affected, err := tdb.UpdateRows("users",
		map[string]interface{}{"age": 31},
		map[string]interface{}{"id": 1},
	)
	if err != nil {
		t.Fatalf("failed to update: %v", err)
	}
	if affected != 1 {
		t.Errorf("expected 1 affected row, got %d", affected)
	}

	tdb.AssertRowCount("users", 1)
}

// TestDatabaseUpdateMultipleRows tests updating multiple rows
func TestDatabaseUpdateMultipleRows(t *testing.T) {
	tdb := tests.NewTestDB(t)
	defer tdb.Cleanup()

	columns := tests.SampleTableColumns()
	tdb.CreateTable("users", columns, "id")

	// Insert test data
	rows := []map[string]interface{}{
		tests.SampleUserRow(1, "Alice", 30),
		tests.SampleUserRow(2, "Bob", 30),
		tests.SampleUserRow(3, "Charlie", 25),
	}
	for _, row := range rows {
		tdb.InsertRow("users", row)
	}

	// Update all rows with age=30 to age=31
	affected, err := tdb.UpdateRows("users",
		map[string]interface{}{"age": 31},
		map[string]interface{}{"age": 30},
	)
	if err != nil {
		t.Fatalf("failed to update: %v", err)
	}
	if affected != 2 {
		t.Errorf("expected 2 affected rows, got %d", affected)
	}

	tdb.AssertRowCount("users", 3)
}

// TestDatabaseDelete tests deleting rows
func TestDatabaseDelete(t *testing.T) {
	tdb := tests.NewTestDB(t)
	defer tdb.Cleanup()

	columns := tests.SampleTableColumns()
	tdb.CreateTable("users", columns, "id")

	// Insert test data
	rows := []map[string]interface{}{
		tests.SampleUserRow(1, "Alice", 30),
		tests.SampleUserRow(2, "Bob", 25),
		tests.SampleUserRow(3, "Charlie", 35),
	}
	for _, row := range rows {
		tdb.InsertRow("users", row)
	}

	// Delete one row
	affected, err := tdb.DeleteRows("users", map[string]interface{}{"id": 2})
	if err != nil {
		t.Fatalf("failed to delete: %v", err)
	}
	if affected != 1 {
		t.Errorf("expected 1 affected row, got %d", affected)
	}

	tdb.AssertRowCount("users", 2)
}

// TestDatabaseDeleteMultipleRows tests deleting multiple rows
func TestDatabaseDeleteMultipleRows(t *testing.T) {
	tdb := tests.NewTestDB(t)
	defer tdb.Cleanup()

	columns := tests.SampleTableColumns()
	tdb.CreateTable("users", columns, "id")

	// Insert test data
	rows := []map[string]interface{}{
		tests.SampleUserRow(1, "Alice", 30),
		tests.SampleUserRow(2, "Bob", 30),
		tests.SampleUserRow(3, "Charlie", 35),
	}
	for _, row := range rows {
		tdb.InsertRow("users", row)
	}

	// Delete all rows with age=30
	affected, err := tdb.DeleteRows("users", map[string]interface{}{"age": 30})
	if err != nil {
		t.Fatalf("failed to delete: %v", err)
	}
	if affected != 2 {
		t.Errorf("expected 2 affected rows, got %d", affected)
	}

	tdb.AssertRowCount("users", 1)
}

// TestDatabaseComplexWorkflow tests a complete CRUD workflow
func TestDatabaseComplexWorkflow(t *testing.T) {
	tdb := tests.NewTestDB(t)
	defer tdb.Cleanup()

	// Create two tables
	userColumns := tests.SampleTableColumns()
	tdb.CreateTable("users", userColumns, "id")

	taskColumns := tests.SampleTaskTableColumns()
	tdb.CreateTable("tasks", taskColumns, "id")

	// Insert users
	tdb.InsertRow("users", tests.SampleUserRow(1, "Alice", 30))
	tdb.InsertRow("users", tests.SampleUserRow(2, "Bob", 25))
	tdb.AssertRowCount("users", 2)

	// Insert tasks
	tdb.InsertRow("tasks", tests.SampleTaskRow(1, "Buy milk", false))
	tdb.InsertRow("tasks", tests.SampleTaskRow(2, "Write code", true))
	tdb.InsertRow("tasks", tests.SampleTaskRow(3, "Deploy app", false))
	tdb.AssertRowCount("tasks", 3)

	// Update a user
	tdb.UpdateRows("users", map[string]interface{}{"age": 31}, map[string]interface{}{"id": 1})

	// Mark task as completed
	tdb.UpdateRows("tasks", map[string]interface{}{"completed": true}, map[string]interface{}{"id": 3})

	// Delete a task
	tdb.DeleteRows("tasks", map[string]interface{}{"id": 2})
	tdb.AssertRowCount("tasks", 2)

	// Verify final state
	users, _ := tdb.SelectAll("users")
	if len(users) != 2 {
		t.Errorf("expected 2 users, got %d", len(users))
	}

	tasks, _ := tdb.SelectAll("tasks")
	if len(tasks) != 2 {
		t.Errorf("expected 2 tasks, got %d", len(tasks))
	}
}

// TestDatabaseMultipleTables tests operations on multiple tables simultaneously
func TestDatabaseMultipleTables(t *testing.T) {
	tdb := tests.NewTestDB(t)
	defer tdb.Cleanup()

	// Create multiple tables
	tables := map[string][]schema.Column{
		"users": tests.SampleTableColumns(),
		"tasks": tests.SampleTaskTableColumns(),
	}

	for tableName, columns := range tables {
		err := tdb.CreateTable(tableName, columns, "id")
		if err != nil {
			t.Fatalf("failed to create table %s: %v", tableName, err)
		}
	}

	// Verify both tables exist
	for tableName := range tables {
		table, err := tdb.GetTable(tableName)
		if err != nil || table == nil {
			t.Errorf("expected table %s to exist", tableName)
		}
	}
}

// TestDatabaseEmptyTable tests operations on empty table
func TestDatabaseEmptyTable(t *testing.T) {
	tdb := tests.NewTestDB(t)
	defer tdb.Cleanup()

	columns := tests.SampleTableColumns()
	tdb.CreateTable("users", columns, "id")

	tdb.AssertRowCount("users", 0)

	rows, err := tdb.SelectAll("users")
	if err != nil {
		t.Fatalf("failed to select from empty table: %v", err)
	}
	if len(rows) != 0 {
		t.Errorf("expected 0 rows from empty table, got %d", len(rows))
	}
}

// TestDatabaseUpdateNonExistent tests updating non-existent rows
func TestDatabaseUpdateNonExistent(t *testing.T) {
	tdb := tests.NewTestDB(t)
	defer tdb.Cleanup()

	columns := tests.SampleTableColumns()
	tdb.CreateTable("users", columns, "id")

	// Insert one row
	tdb.InsertRow("users", tests.SampleUserRow(1, "Alice", 30))

	// Try to update non-existent row
	affected, err := tdb.UpdateRows("users",
		map[string]interface{}{"age": 40},
		map[string]interface{}{"id": 999},
	)
	if err != nil {
		t.Fatalf("update should not error on non-existent rows: %v", err)
	}
	if affected != 0 {
		t.Errorf("expected 0 affected rows, got %d", affected)
	}

	// Original row should still exist
	tdb.AssertRowCount("users", 1)
}

// TestDatabaseDeleteNonExistent tests deleting non-existent rows
func TestDatabaseDeleteNonExistent(t *testing.T) {
	tdb := tests.NewTestDB(t)
	defer tdb.Cleanup()

	columns := tests.SampleTableColumns()
	tdb.CreateTable("users", columns, "id")

	// Insert one row
	tdb.InsertRow("users", tests.SampleUserRow(1, "Alice", 30))

	// Try to delete non-existent row
	affected, err := tdb.DeleteRows("users", map[string]interface{}{"id": 999})
	if err != nil {
		t.Fatalf("delete should not error on non-existent rows: %v", err)
	}
	if affected != 0 {
		t.Errorf("expected 0 affected rows, got %d", affected)
	}

	// Original row should still exist
	tdb.AssertRowCount("users", 1)
}
