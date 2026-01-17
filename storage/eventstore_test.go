package storage

import (
	"os"
	"testing"

	"rdbms/eventlog"
)

func TestEventStoreIntegration(t *testing.T) {
	// Setup
	tmpDir := t.TempDir()
	defer os.RemoveAll(tmpDir)

	es, err := NewEventStore(tmpDir)
	if err != nil {
		t.Fatalf("Failed to create event store: %v", err)
	}
	defer es.Close()

	// Test 1: Record schema creation
	cols := []eventlog.ColumnDefinition{
		{Name: "id", Type: "INT", PrimaryKey: true},
		{Name: "name", Type: "VARCHAR", Nullable: false},
	}

	schemaEvent, err := es.RecordSchemaCreated("users", cols, "id", "tx-1")
	if err != nil {
		t.Fatalf("Failed to record schema: %v", err)
	}
	if schemaEvent.Type != eventlog.SchemaCreated {
		t.Errorf("Expected SchemaCreated event, got %s", schemaEvent.Type)
	}
	if schemaEvent.ID != 1 {
		t.Errorf("Expected event ID 1, got %d", schemaEvent.ID)
	}

	// Test 2: Record row insertion
	row1 := Row{"id": int64(1), "name": "Alice"}
	insertEvent, err := es.RecordRowInserted("users", 0, row1, "tx-2")
	if err != nil {
		t.Fatalf("Failed to record insert: %v", err)
	}
	if insertEvent.Type != eventlog.RowInserted {
		t.Errorf("Expected RowInserted event, got %s", insertEvent.Type)
	}

	// Test 3: Record row update
	changes := map[string]interface{}{"name": "Alice Smith"}
	oldVals := map[string]interface{}{"name": "Alice"}
	updateEvent, err := es.RecordRowUpdated("users", 0, changes, oldVals, "tx-3")
	if err != nil {
		t.Fatalf("Failed to record update: %v", err)
	}
	if updateEvent.Type != eventlog.RowUpdated {
		t.Errorf("Expected RowUpdated event, got %s", updateEvent.Type)
	}

	// Test 4: Record row deletion
	deleteEvent, err := es.RecordRowDeleted("users", 0, row1, "tx-4")
	if err != nil {
		t.Fatalf("Failed to record delete: %v", err)
	}
	if deleteEvent.Type != eventlog.RowDeleted {
		t.Errorf("Expected RowDeleted event, got %s", deleteEvent.Type)
	}

	// Test 5: Retrieve all events
	events, errs := es.GetAllEvents()
	if len(errs) > 0 {
		t.Logf("Warning: %d errors reading log", len(errs))
	}
	if len(events) != 4 {
		t.Errorf("Expected 4 events, got %d", len(events))
	}

	// Test 6: Verify event IDs are sequential
	if events[0].ID != 1 || events[1].ID != 2 || events[2].ID != 3 || events[3].ID != 4 {
		t.Error("Event IDs are not sequential")
	}

	// Test 7: Derive state from events
	state, err := ReplayEvents(events)
	if err != nil {
		t.Fatalf("Failed to derive state: %v", err)
	}

	// After the last delete event, row should be marked deleted
	if !state.DeletedRows["users"][0] {
		t.Error("Expected row to be marked as deleted")
	}

	// Test 8: Verify row version tracking
	version, err := es.GetRowVersion("users", 0)
	if err != nil {
		t.Fatalf("Failed to get row version: %v", err)
	}
	if version != 4 { // Last event (delete) was event ID 4
		t.Errorf("Expected row version 4, got %d", version)
	}

	t.Log("✓ All integration tests passed")
}

func TestStateReplay(t *testing.T) {
	// Create mock events for state replay testing
	events := []*eventlog.Event{
		{
			ID:   1,
			Type: eventlog.SchemaCreated,
			Payload: map[string]interface{}{
				"table_name": "products",
				"columns": []interface{}{
					map[string]interface{}{
						"name": "product_id",
						"type": "INT",
					},
					map[string]interface{}{
						"name": "price",
						"type": "FLOAT",
					},
				},
			},
		},
		{
			ID:   2,
			Type: eventlog.RowInserted,
			Payload: map[string]interface{}{
				"table_name": "products",
				"row_id":     float64(1),
				"data": map[string]interface{}{
					"product_id": int64(1),
					"price":      29.99,
				},
			},
		},
		{
			ID:   3,
			Type: eventlog.RowInserted,
			Payload: map[string]interface{}{
				"table_name": "products",
				"row_id":     float64(2),
				"data": map[string]interface{}{
					"product_id": int64(2),
					"price":      49.99,
				},
			},
		},
		{
			ID:   4,
			Type: eventlog.RowUpdated,
			Payload: map[string]interface{}{
				"table_name": "products",
				"row_id":     float64(1),
				"changes": map[string]interface{}{
					"price": 24.99,
				},
			},
		},
	}

	state, err := ReplayEvents(events)
	if err != nil {
		t.Fatalf("Failed to replay events: %v", err)
	}

	// Verify product count
	rows := state.GetTableRows("products")
	if len(rows) != 2 {
		t.Errorf("Expected 2 active rows, got %d", len(rows))
	}

	// Verify updated price
	row1, exists := state.GetRow("products", 1)
	if !exists {
		t.Fatal("Row 1 not found")
	}
	if price, ok := row1["price"].(float64); ok && price != 24.99 {
		t.Errorf("Expected price 24.99, got %v", price)
	}

	t.Log("✓ State replay tests passed")
}
