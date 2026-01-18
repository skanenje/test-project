package integration

import (
	"os"
	"testing"

	"rdbms/eventlog"
	"rdbms/storage"
)

func TestSnapshotManagerCreation(t *testing.T) {
	tmpDir := t.TempDir()
	defer os.RemoveAll(tmpDir)

	sm, err := storage.NewSnapshotManager(tmpDir)
	if err != nil {
		t.Fatalf("Failed to create snapshot manager: %v", err)
	}

	// Verify snapshots directory created
	if _, err := os.Stat(tmpDir + "/snapshots"); os.IsNotExist(err) {
		t.Error("Snapshots directory not created")
	}

	if sm.GetLatestSnapshotMeta() != nil {
		t.Error("Expected no initial snapshots")
	}

	t.Log("✓ Snapshot manager creation test passed")
}

func TestSnapshotCreateAndRestore(t *testing.T) {
	tmpDir := t.TempDir()
	defer os.RemoveAll(tmpDir)

	sm, _ := storage.NewSnapshotManager(tmpDir)

	// Create sample state
	state := &storage.DerivedState{
		Tables: map[string]map[int64]storage.Row{
			"users": {
				1: storage.Row{"id": int64(1), "name": "Alice"},
				2: storage.Row{"id": int64(2), "name": "Bob"},
			},
		},
		DeletedRows: map[string]map[int64]bool{
			"users": {
				3: true, // Row 3 was deleted
			},
		},
	}

	// Create snapshot
	meta, err := sm.CreateSnapshot(state, 10, 10)
	if err != nil {
		t.Fatalf("Failed to create snapshot: %v", err)
	}

	if meta.SnapshotID == "" {
		t.Error("Snapshot ID is empty")
	}
	if meta.BaseEventID != 10 {
		t.Errorf("Expected base event ID 10, got %d", meta.BaseEventID)
	}
	if meta.DataHash == "" {
		t.Error("Data hash is empty")
	}

	// Verify file was created
	if _, err := os.Stat(meta.SnapshotPath); os.IsNotExist(err) {
		t.Error("Snapshot file not created")
	}

	// Restore snapshot
	restoredState, restoredMeta, err := sm.RestoreFromSnapshot(meta.SnapshotID)
	if err != nil {
		t.Fatalf("Failed to restore snapshot: %v", err)
	}

	if restoredMeta.SnapshotID != meta.SnapshotID {
		t.Error("Snapshot metadata mismatch")
	}

	// Verify state was restored correctly
	if len(restoredState.Tables["users"]) != 2 {
		t.Errorf("Expected 2 users, got %d", len(restoredState.Tables["users"]))
	}

	if row, ok := restoredState.GetRow("users", 1); !ok || row["name"] != "Alice" {
		t.Error("User 1 not restored correctly")
	}

	if !restoredState.DeletedRows["users"][3] {
		t.Error("Deleted row marker not restored")
	}

	t.Log("✓ Snapshot create/restore test passed")
}

func TestSnapshotHistory(t *testing.T) {
	tmpDir := t.TempDir()
	defer os.RemoveAll(tmpDir)

	sm, _ := storage.NewSnapshotManager(tmpDir)

	// Create 3 snapshots
	for i := 1; i <= 3; i++ {
		state := &storage.DerivedState{
			Tables: map[string]map[int64]storage.Row{
				"test": {
					int64(i): storage.Row{"id": int64(i)},
				},
			},
			DeletedRows: make(map[string]map[int64]bool),
		}
		sm.CreateSnapshot(state, uint64(i*10), int64(i*10))
	}

	// Check history
	history := sm.GetSnapshotHistory()
	if len(history) != 3 {
		t.Errorf("Expected 3 snapshots in history, got %d", len(history))
	}

	// Check latest
	latest := sm.GetLatestSnapshotMeta()
	if latest.BaseEventID != 30 {
		t.Errorf("Latest snapshot should have base event 30, got %d", latest.BaseEventID)
	}

	t.Log("✓ Snapshot history test passed")
}

func TestSnapshotPruning(t *testing.T) {
	tmpDir := t.TempDir()
	defer os.RemoveAll(tmpDir)

	sm, _ := storage.NewSnapshotManager(tmpDir)

	// Create 5 snapshots
	for i := 1; i <= 5; i++ {
		state := &storage.DerivedState{
			Tables: map[string]map[int64]storage.Row{
				"test": {
					int64(i): storage.Row{"id": int64(i)},
				},
			},
			DeletedRows: make(map[string]map[int64]bool),
		}
		sm.CreateSnapshot(state, uint64(i*10), int64(i*10))
	}

	// Prune to keep only 2
	err := sm.PruneOldSnapshots(2)
	if err != nil {
		t.Fatalf("Pruning failed: %v", err)
	}

	history := sm.GetSnapshotHistory()
	if len(history) != 2 {
		t.Errorf("Expected 2 snapshots after pruning, got %d", len(history))
	}

	// Verify correct snapshots were kept (most recent)
	if history[0].BaseEventID != 40 || history[1].BaseEventID != 50 {
		t.Error("Wrong snapshots were pruned")
	}

	t.Log("✓ Snapshot pruning test passed")
}

func TestQueryEngineWithSnapshots(t *testing.T) {
	tmpDir := t.TempDir()
	defer os.RemoveAll(tmpDir)

	// Create event store
	es, _ := storage.NewEventStore(tmpDir)
	defer es.Close()

	// Create query engine
	sm, _ := storage.NewSnapshotManager(tmpDir)
	qe := storage.NewQueryEngine(es, sm)

	// Record some events
	cols := []eventlog.ColumnDefinition{
		{Name: "id", Type: "INT", PrimaryKey: true},
		{Name: "value", Type: "INT"},
	}
	es.RecordSchemaCreated("test", cols, "id", "tx-1")

	// Insert 3 rows
	for i := 1; i <= 3; i++ {
		es.RecordRowInserted("test", int64(i), storage.Row{"id": int64(i), "value": int64(i * 10)}, "tx-2")
	}

	// Get state
	state, err := qe.GetCurrentState()
	if err != nil {
		t.Fatalf("Failed to get current state: %v", err)
	}

	rows := state.GetTableRows("test")
	if len(rows) != 3 {
		t.Errorf("Expected 3 rows, got %d", len(rows))
	}

	t.Log("✓ Query engine with snapshots test passed")
}

func TestQueryEngineReplayAfterSnapshot(t *testing.T) {
	tmpDir := t.TempDir()
	defer os.RemoveAll(tmpDir)

	// Create event store
	es, _ := storage.NewEventStore(tmpDir)
	defer es.Close()

	// Create snapshot manager and query engine
	sm, _ := storage.NewSnapshotManager(tmpDir)
	qe := storage.NewQueryEngine(es, sm)

	// Record initial events
	cols := []eventlog.ColumnDefinition{
		{Name: "id", Type: "INT", PrimaryKey: true},
	}
	es.RecordSchemaCreated("data", cols, "id", "tx-1")

	// Insert 5 rows
	for i := 1; i <= 5; i++ {
		es.RecordRowInserted("data", int64(i), storage.Row{"id": int64(i)}, "tx-2")
	}

	// Create snapshot at event 6 (after 1 schema + 5 inserts)
	state1, _ := qe.GetCurrentState()
	_, _ = sm.CreateSnapshot(state1, es.GetLastEventID(), int64(es.GetLastEventID()))

	// Record more events after snapshot
	for i := 6; i <= 8; i++ {
		es.RecordRowInserted("data", int64(i), storage.Row{"id": int64(i)}, "tx-3")
	}

	// Get state again - should replay from snapshot + new events
	state2, err := qe.GetCurrentState()
	if err != nil {
		t.Fatalf("Failed to get state: %v", err)
	}

	rows := state2.GetTableRows("data")
	if len(rows) != 8 {
		t.Errorf("Expected 8 rows after replay, got %d", len(rows))
	}

	t.Log("✓ Query engine replay after snapshot test passed")
}

func TestQueryEngineGetTableRows(t *testing.T) {
	tmpDir := t.TempDir()
	defer os.RemoveAll(tmpDir)

	es, _ := storage.NewEventStore(tmpDir)
	defer es.Close()

	sm, _ := storage.NewSnapshotManager(tmpDir)
	qe := storage.NewQueryEngine(es, sm)

	// Setup
	cols := []eventlog.ColumnDefinition{
		{Name: "id", Type: "INT", PrimaryKey: true},
		{Name: "name", Type: "VARCHAR"},
	}
	es.RecordSchemaCreated("users", cols, "id", "tx-1")

	// Insert and delete rows
	es.RecordRowInserted("users", 1, storage.Row{"id": int64(1), "name": "Alice"}, "tx-2")
	es.RecordRowInserted("users", 2, storage.Row{"id": int64(2), "name": "Bob"}, "tx-2")
	es.RecordRowInserted("users", 3, storage.Row{"id": int64(3), "name": "Charlie"}, "tx-2")
	es.RecordRowDeleted("users", 2, storage.Row{"id": int64(2), "name": "Bob"}, "tx-3")

	// Query
	rows, err := qe.GetTableRows("users")
	if err != nil {
		t.Fatalf("Failed to get table rows: %v", err)
	}

	if len(rows) != 2 {
		t.Errorf("Expected 2 active rows (Bob should be deleted), got %d", len(rows))
	}

	t.Log("✓ Query engine get table rows test passed")
}

func TestQueryEngineGetRow(t *testing.T) {
	tmpDir := t.TempDir()
	defer os.RemoveAll(tmpDir)

	es, _ := storage.NewEventStore(tmpDir)
	defer es.Close()

	sm, _ := storage.NewSnapshotManager(tmpDir)
	qe := storage.NewQueryEngine(es, sm)

	// Setup
	cols := []eventlog.ColumnDefinition{
		{Name: "id", Type: "INT", PrimaryKey: true},
	}
	es.RecordSchemaCreated("test", cols, "id", "tx-1")

	// Insert row
	es.RecordRowInserted("test", 42, storage.Row{"id": int64(42), "value": "important"}, "tx-2")

	// Get specific row
	row, exists, err := qe.GetRow("test", 42)
	if err != nil {
		t.Fatalf("Failed to get row: %v", err)
	}

	if !exists {
		t.Error("Row should exist")
	}

	rowID, ok := row["id"]
	if !ok {
		t.Error("id field not found in row")
	}

	var idVal int64
	switch v := rowID.(type) {
	case int64:
		idVal = v
	case float64:
		idVal = int64(v)
	default:
		t.Errorf("unexpected type for id: %T", v)
	}

	if idVal != int64(42) {
		t.Errorf("Expected id 42, got %d", idVal)
	}

	// Get non-existent row
	row, exists, err = qe.GetRow("test", 999)
	if err != nil {
		t.Fatalf("Failed to query non-existent row: %v", err)
	}

	if exists {
		t.Error("Row should not exist")
	}

	t.Log("✓ Query engine get row test passed")
}
