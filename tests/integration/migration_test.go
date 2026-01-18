package integration

import (
	"os"
	"testing"

	"rdbms/eventlog"
	"rdbms/schema"
	"rdbms/storage"
)

func TestMigrationHandler(t *testing.T) {
	registry := schema.NewSchemaRegistry()

	// Setup schemas
	v1Cols := []schema.Column{
		{Name: "id", Type: schema.TypeInt, PrimaryKey: true},
		{Name: "name", Type: schema.TypeText},
	}
	v2Cols := []schema.Column{
		{Name: "id", Type: schema.TypeInt, PrimaryKey: true},
		{Name: "name", Type: schema.TypeText},
		{Name: "email", Type: schema.TypeText},
	}

	registry.RegisterSchema("users", 1, v1Cols)
	registry.RegisterSchema("users", 2, v2Cols)

	// Register migration
	registry.RegisterMigration("users", 1, 2, []schema.MigrationOp{
		&schema.AddColumnOp{
			Column:  schema.Column{Name: "email", Type: schema.TypeText},
			Default: "no-email",
		},
	})

	// Create migration handler
	handler := storage.NewMigrationHandler(registry)

	// Test migrating a row
	row := storage.Row{"id": int64(1), "name": "Alice"}
	migratedRow, err := handler.MigrateRowIfNeeded("users", row, 1, 2)
	if err != nil {
		t.Fatalf("Migration failed: %v", err)
	}

	if migratedRow["email"] != "no-email" {
		t.Errorf("Expected email=no-email, got %v", migratedRow["email"])
	}

	t.Log("✓ Migration handler test passed")
}

func TestReplayEventsWithMigrations(t *testing.T) {
	registry := schema.NewSchemaRegistry()

	// Register schemas
	v1Cols := []schema.Column{
		{Name: "id", Type: schema.TypeInt, PrimaryKey: true},
		{Name: "name", Type: schema.TypeText},
	}
	v2Cols := []schema.Column{
		{Name: "id", Type: schema.TypeInt, PrimaryKey: true},
		{Name: "name", Type: schema.TypeText},
		{Name: "status", Type: schema.TypeText},
	}

	registry.RegisterSchema("users", 1, v1Cols)
	registry.RegisterSchema("users", 2, v2Cols)

	// Register migration
	registry.RegisterMigration("users", 1, 2, []schema.MigrationOp{
		&schema.AddColumnOp{
			Column:  schema.Column{Name: "status", Type: schema.TypeText},
			Default: "active",
		},
	})

	handler := storage.NewMigrationHandler(registry)

	// Create events (in schema v1)
	events := []*eventlog.Event{
		{
			ID:      1,
			Type:    eventlog.SchemaCreated,
			Version: 1,
			Payload: map[string]interface{}{
				"table_name": "users",
				"columns": []interface{}{
					map[string]interface{}{"name": "id", "type": "INT", "primary_key": true},
					map[string]interface{}{"name": "name", "type": "TEXT", "primary_key": false},
				},
			},
		},
		{
			ID:      2,
			Type:    eventlog.RowInserted,
			Version: 1,
			Payload: map[string]interface{}{
				"table_name": "users",
				"row_id":     float64(1),
				"data": map[string]interface{}{
					"id":   int64(1),
					"name": "Alice",
				},
			},
		},
		{
			ID:      3,
			Type:    eventlog.RowInserted,
			Version: 1,
			Payload: map[string]interface{}{
				"table_name": "users",
				"row_id":     float64(2),
				"data": map[string]interface{}{
					"id":   int64(2),
					"name": "Bob",
				},
			},
		},
	}

	// Replay with migration to v2
	state, err := storage.ReplayEventsWithMigrations(events, 2, handler)
	if err != nil {
		t.Fatalf("Replay failed: %v", err)
	}

	rows := state.GetTableRows("users")
	if len(rows) != 2 {
		t.Errorf("Expected 2 rows, got %d", len(rows))
	}

	// Check that migration was applied
	for _, r := range rows {
		if status, exists := r.Row["status"]; !exists || status != "active" {
			t.Errorf("Expected status field to be added with value 'active', got %v", status)
		}
	}

	t.Log("✓ Replay events with migrations test passed")
}

func TestGetSchemaVersionHistory(t *testing.T) {
	events := []*eventlog.Event{
		{
			ID:      1,
			Type:    eventlog.SchemaCreated,
			Version: 1,
			Payload: map[string]interface{}{"table_name": "users"},
		},
		{
			ID:      2,
			Type:    eventlog.SchemaEvolved,
			Version: 2,
			Payload: map[string]interface{}{"table_name": "users"},
		},
		{
			ID:      3,
			Type:    eventlog.SchemaCreated,
			Version: 1,
			Payload: map[string]interface{}{"table_name": "products"},
		},
		{
			ID:      4,
			Type:    eventlog.SchemaEvolved,
			Version: 2,
			Payload: map[string]interface{}{"table_name": "users"},
		},
	}

	history := storage.GetSchemaVersionHistory(events)

	usersVersions := history["users"]
	if len(usersVersions) != 2 {
		t.Errorf("Expected 2 versions for users, got %d", len(usersVersions))
	}

	productsVersions := history["products"]
	if len(productsVersions) != 1 {
		t.Errorf("Expected 1 version for products, got %d", len(productsVersions))
	}

	t.Log("✓ Get schema version history test passed")
}

func TestMigrationFailureRecovery(t *testing.T) {
	tmpDir := t.TempDir()
	defer os.RemoveAll(tmpDir)

	registry := schema.NewSchemaRegistry()

	// Setup schemas
	v1Cols := []schema.Column{
		{Name: "id", Type: schema.TypeInt, PrimaryKey: true},
	}
	v2Cols := []schema.Column{
		{Name: "id", Type: schema.TypeInt, PrimaryKey: true},
		{Name: "name", Type: schema.TypeText},
	}

	registry.RegisterSchema("test", 1, v1Cols)
	registry.RegisterSchema("test", 2, v2Cols)

	// Migration that will fail (no path from v1 to v2 registered)
	handler := storage.NewMigrationHandler(registry)

	// Create events
	events := []*eventlog.Event{
		{
			ID:      1,
			Type:    eventlog.SchemaCreated,
			Version: 1,
			Payload: map[string]interface{}{"table_name": "test"},
		},
		{
			ID:      2,
			Type:    eventlog.RowInserted,
			Version: 1,
			Payload: map[string]interface{}{
				"table_name": "test",
				"row_id":     float64(1),
				"data":       map[string]interface{}{"id": int64(1)},
			},
		},
	}

	// Replay to higher version without migration should gracefully handle errors
	// (it should skip the problematic row but continue)
	state, err := storage.ReplayEventsWithMigrations(events, 2, handler)
	if err != nil {
		t.Fatalf("Replay should not fail on missing migration: %v", err)
	}

	// State should still be created (but without the migrated fields)
	if _, exists := state.Tables["test"]; !exists {
		t.Error("Table should exist in state")
	}

	t.Log("✓ Migration failure recovery test passed")
}

func TestComplexMultiTableMigration(t *testing.T) {
	registry := schema.NewSchemaRegistry()

	// Setup two tables with migrations
	usersV1 := []schema.Column{
		{Name: "id", Type: schema.TypeInt, PrimaryKey: true},
		{Name: "name", Type: schema.TypeText},
	}
	usersV2 := []schema.Column{
		{Name: "id", Type: schema.TypeInt, PrimaryKey: true},
		{Name: "name", Type: schema.TypeText},
		{Name: "email", Type: schema.TypeText},
	}

	productsV1 := []schema.Column{
		{Name: "id", Type: schema.TypeInt, PrimaryKey: true},
		{Name: "title", Type: schema.TypeText},
		{Name: "price", Type: schema.TypeInt},
	}
	productsV2 := []schema.Column{
		{Name: "id", Type: schema.TypeInt, PrimaryKey: true},
		{Name: "title", Type: schema.TypeText},
	} // Removed price column

	registry.RegisterSchema("users", 1, usersV1)
	registry.RegisterSchema("users", 2, usersV2)
	registry.RegisterSchema("products", 1, productsV1)
	registry.RegisterSchema("products", 2, productsV2)

	// Register migrations
	registry.RegisterMigration("users", 1, 2, []schema.MigrationOp{
		&schema.AddColumnOp{Column: schema.Column{Name: "email", Type: schema.TypeText}, Default: "unknown"},
	})
	registry.RegisterMigration("products", 1, 2, []schema.MigrationOp{
		&schema.RemoveColumnOp{ColumnName: "price"},
	})

	handler := storage.NewMigrationHandler(registry)

	// Create events from both tables
	events := []*eventlog.Event{
		{
			ID:      1,
			Type:    eventlog.SchemaCreated,
			Version: 1,
			Payload: map[string]interface{}{"table_name": "users"},
		},
		{
			ID:      2,
			Type:    eventlog.SchemaCreated,
			Version: 1,
			Payload: map[string]interface{}{"table_name": "products"},
		},
		{
			ID:      3,
			Type:    eventlog.RowInserted,
			Version: 1,
			Payload: map[string]interface{}{
				"table_name": "users",
				"row_id":     float64(1),
				"data":       map[string]interface{}{"id": int64(1), "name": "Alice"},
			},
		},
		{
			ID:      4,
			Type:    eventlog.RowInserted,
			Version: 1,
			Payload: map[string]interface{}{
				"table_name": "products",
				"row_id":     float64(1),
				"data":       map[string]interface{}{"id": int64(1), "title": "Widget", "price": int64(99)},
			},
		},
	}

	state, _ := storage.ReplayEventsWithMigrations(events, 2, handler)

	// Check users table
	userRows := state.GetTableRows("users")
	if len(userRows) != 1 {
		t.Errorf("Expected 1 user row, got %d", len(userRows))
	}
	if userRows[0].Row["email"] != "unknown" {
		t.Error("Email column should be added to user")
	}

	// Check products table
	productRows := state.GetTableRows("products")
	if len(productRows) != 1 {
		t.Errorf("Expected 1 product row, got %d", len(productRows))
	}
	if _, exists := productRows[0].Row["price"]; exists {
		t.Error("Price column should be removed from product")
	}

	t.Log("✓ Complex multi-table migration test passed")
}
