package integration

import (
	"testing"

	"rdbms/schema"
)

func TestSchemaRegistry(t *testing.T) {
	sr := schema.NewSchemaRegistry()

	// Register v1 schema
	v1Cols := []schema.Column{
		{Name: "id", Type: schema.TypeInt, PrimaryKey: true},
		{Name: "name", Type: schema.TypeText},
	}
	sr.RegisterSchema("users", 1, v1Cols)

	// Retrieve schema
	s, err := sr.GetSchema("users", 1)
	if err != nil {
		t.Fatalf("Failed to get schema: %v", err)
	}

	if s.Version != 1 {
		t.Errorf("Expected version 1, got %d", s.Version)
	}

	if len(s.Columns) != 2 {
		t.Errorf("Expected 2 columns, got %d", len(s.Columns))
	}

	t.Log("✓ Schema registry test passed")
}

func TestAddColumnMigration(t *testing.T) {
	sr := schema.NewSchemaRegistry()

	// v1: id, name
	v1Cols := []schema.Column{
		{Name: "id", Type: schema.TypeInt, PrimaryKey: true},
		{Name: "name", Type: schema.TypeText},
	}
	sr.RegisterSchema("users", 1, v1Cols)

	// v2: id, name, email (new column)
	v2Cols := []schema.Column{
		{Name: "id", Type: schema.TypeInt, PrimaryKey: true},
		{Name: "name", Type: schema.TypeText},
		{Name: "email", Type: schema.TypeText},
	}
	sr.RegisterSchema("users", 2, v2Cols)

	// Register migration: add email column with default value
	migration := []schema.MigrationOp{
		&schema.AddColumnOp{
			Column:  schema.Column{Name: "email", Type: schema.TypeText},
			Default: "noemail@example.com",
		},
	}
	sr.RegisterMigration("users", 1, 2, migration)

	// Migrate a row
	oldRow := map[string]interface{}{
		"id":   int64(1),
		"name": "Alice",
	}

	newRow, err := sr.MigrateRow("users", oldRow, 1, 2)
	if err != nil {
		t.Fatalf("Migration failed: %v", err)
	}

	if newRow["email"] != "noemail@example.com" {
		t.Errorf("Expected default email, got %v", newRow["email"])
	}

	t.Log("✓ Add column migration test passed")
}

func TestRemoveColumnMigration(t *testing.T) {
	sr := schema.NewSchemaRegistry()

	// v1: id, name, deprecated_field
	v1Cols := []schema.Column{
		{Name: "id", Type: schema.TypeInt, PrimaryKey: true},
		{Name: "name", Type: schema.TypeText},
		{Name: "deprecated_field", Type: schema.TypeText},
	}
	sr.RegisterSchema("users", 1, v1Cols)

	// v2: id, name (removed deprecated_field)
	v2Cols := []schema.Column{
		{Name: "id", Type: schema.TypeInt, PrimaryKey: true},
		{Name: "name", Type: schema.TypeText},
	}
	sr.RegisterSchema("users", 2, v2Cols)

	// Register migration: remove column
	migration := []schema.MigrationOp{
		&schema.RemoveColumnOp{ColumnName: "deprecated_field"},
	}
	sr.RegisterMigration("users", 1, 2, migration)

	// Migrate a row
	oldRow := map[string]interface{}{
		"id":               int64(1),
		"name":             "Alice",
		"deprecated_field": "old_value",
	}

	newRow, err := sr.MigrateRow("users", oldRow, 1, 2)
	if err != nil {
		t.Fatalf("Migration failed: %v", err)
	}

	if _, exists := newRow["deprecated_field"]; exists {
		t.Error("Deprecated field should be removed")
	}

	if newRow["id"] != int64(1) || newRow["name"] != "Alice" {
		t.Error("Other columns should be preserved")
	}

	t.Log("✓ Remove column migration test passed")
}

func TestRenameColumnMigration(t *testing.T) {
	sr := schema.NewSchemaRegistry()

	// v1: id, user_name
	v1Cols := []schema.Column{
		{Name: "id", Type: schema.TypeInt, PrimaryKey: true},
		{Name: "user_name", Type: schema.TypeText},
	}
	sr.RegisterSchema("users", 1, v1Cols)

	// v2: id, name
	v2Cols := []schema.Column{
		{Name: "id", Type: schema.TypeInt, PrimaryKey: true},
		{Name: "name", Type: schema.TypeText},
	}
	sr.RegisterSchema("users", 2, v2Cols)

	// Register migration: rename column
	migration := []schema.MigrationOp{
		&schema.RenameColumnOp{OldName: "user_name", NewName: "name"},
	}
	sr.RegisterMigration("users", 1, 2, migration)

	// Migrate a row
	oldRow := map[string]interface{}{
		"id":        int64(1),
		"user_name": "Alice",
	}

	newRow, err := sr.MigrateRow("users", oldRow, 1, 2)
	if err != nil {
		t.Fatalf("Migration failed: %v", err)
	}

	if _, exists := newRow["user_name"]; exists {
		t.Error("Old column name should not exist")
	}

	if newRow["name"] != "Alice" {
		t.Errorf("Expected renamed column with value Alice, got %v", newRow["name"])
	}

	t.Log("✓ Rename column migration test passed")
}

func TestMultipleVersionMigration(t *testing.T) {
	sr := schema.NewSchemaRegistry()

	// Setup: v1 -> v2 -> v3
	v1Cols := []schema.Column{{Name: "id", Type: schema.TypeInt, PrimaryKey: true}}
	v2Cols := []schema.Column{{Name: "id", Type: schema.TypeInt, PrimaryKey: true}, {Name: "name", Type: schema.TypeText}}
	v3Cols := []schema.Column{{Name: "id", Type: schema.TypeInt, PrimaryKey: true}, {Name: "name", Type: schema.TypeText}, {Name: "email", Type: schema.TypeText}}

	sr.RegisterSchema("users", 1, v1Cols)
	sr.RegisterSchema("users", 2, v2Cols)
	sr.RegisterSchema("users", 3, v3Cols)

	// Migration 1->2: add name
	sr.RegisterMigration("users", 1, 2, []schema.MigrationOp{
		&schema.AddColumnOp{Column: schema.Column{Name: "name", Type: schema.TypeText}, Default: "unnamed"},
	})

	// Migration 2->3: add email
	sr.RegisterMigration("users", 2, 3, []schema.MigrationOp{
		&schema.AddColumnOp{Column: schema.Column{Name: "email", Type: schema.TypeText}, Default: "noemail"},
	})

	// Migrate from v1 directly to v3
	oldRow := map[string]interface{}{"id": int64(1)}
	newRow, err := sr.MigrateRow("users", oldRow, 1, 3)
	if err != nil {
		t.Fatalf("Multi-version migration failed: %v", err)
	}

	if newRow["name"] != "unnamed" {
		t.Errorf("Expected name=unnamed, got %v", newRow["name"])
	}
	if newRow["email"] != "noemail" {
		t.Errorf("Expected email=noemail, got %v", newRow["email"])
	}

	t.Log("✓ Multiple version migration test passed")
}

func TestCompatibilityCheck(t *testing.T) {
	sr := schema.NewSchemaRegistry()

	v1Cols := []schema.Column{{Name: "id", Type: schema.TypeInt, PrimaryKey: true}}
	v2Cols := []schema.Column{{Name: "id", Type: schema.TypeInt, PrimaryKey: true}, {Name: "name", Type: schema.TypeText}}

	sr.RegisterSchema("users", 1, v1Cols)
	sr.RegisterSchema("users", 2, v2Cols)

	// Without migration
	check := sr.CheckCompatibility("users", 1, 2)
	if check.Status != schema.Incompatible {
		t.Errorf("Expected incompatible status without migration, got %s", check.Status)
	}

	// Register migration
	sr.RegisterMigration("users", 1, 2, []schema.MigrationOp{
		&schema.AddColumnOp{Column: schema.Column{Name: "name", Type: schema.TypeText}, Default: "unnamed"},
	})

	// With migration
	check = sr.CheckCompatibility("users", 1, 2)
	if check.Status != schema.MigrationNeeded {
		t.Errorf("Expected migration-needed status, got %s", check.Status)
	}

	// Same version
	check = sr.CheckCompatibility("users", 2, 2)
	if check.Status != schema.Compatible {
		t.Errorf("Expected compatible status for same version, got %s", check.Status)
	}

	t.Log("✓ Compatibility check test passed")
}

func TestGetLatestSchemaVersion(t *testing.T) {
	sr := schema.NewSchemaRegistry()

	v1Cols := []schema.Column{{Name: "id", Type: schema.TypeInt, PrimaryKey: true}}
	v2Cols := []schema.Column{{Name: "id", Type: schema.TypeInt, PrimaryKey: true}, {Name: "name", Type: schema.TypeText}}
	v3Cols := []schema.Column{{Name: "id", Type: schema.TypeInt, PrimaryKey: true}, {Name: "name", Type: schema.TypeText}, {Name: "email", Type: schema.TypeText}}

	sr.RegisterSchema("users", 1, v1Cols)
	sr.RegisterSchema("users", 3, v3Cols) // Out of order registration
	sr.RegisterSchema("users", 2, v2Cols)

	latest := sr.GetLatestSchemaVersion("users")
	if latest != 3 {
		t.Errorf("Expected latest version 3, got %d", latest)
	}

	t.Log("✓ Get latest schema version test passed")
}

func TestSameVersionMigration(t *testing.T) {
	sr := schema.NewSchemaRegistry()

	cols := []schema.Column{{Name: "id", Type: schema.TypeInt, PrimaryKey: true}}
	sr.RegisterSchema("users", 1, cols)

	row := map[string]interface{}{"id": int64(1)}
	migratedRow, err := sr.MigrateRow("users", row, 1, 1)
	if err != nil {
		t.Fatalf("Same version migration should not fail: %v", err)
	}

	if migratedRow["id"] != int64(1) {
		t.Error("Row should be unchanged")
	}

	t.Log("✓ Same version migration test passed")
}

func TestBackwardMigrationFails(t *testing.T) {
	sr := schema.NewSchemaRegistry()

	v1Cols := []schema.Column{{Name: "id", Type: schema.TypeInt, PrimaryKey: true}}
	v2Cols := []schema.Column{{Name: "id", Type: schema.TypeInt, PrimaryKey: true}, {Name: "name", Type: schema.TypeText}}

	sr.RegisterSchema("users", 1, v1Cols)
	sr.RegisterSchema("users", 2, v2Cols)

	row := map[string]interface{}{"id": int64(1), "name": "Alice"}

	// Try to migrate backwards (should fail)
	_, err := sr.MigrateRow("users", row, 2, 1)
	if err == nil {
		t.Error("Backward migration should fail")
	}

	t.Log("✓ Backward migration rejection test passed")
}
