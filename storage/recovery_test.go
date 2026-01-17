package storage

import (
	"encoding/json"
	"os"
	"testing"
	"time"

	"rdbms/eventlog"
)

func TestComputeEventChecksum(t *testing.T) {
	event := &eventlog.Event{
		ID:        1,
		Type:      eventlog.RowInserted,
		Timestamp: time.Now(),
		Version:   1,
		TxID:      "tx-123",
		Payload: map[string]interface{}{
			"table_name": "users",
			"row_id":     int64(1),
			"data": map[string]interface{}{
				"id":   1,
				"name": "Alice",
			},
		},
	}

	// Compute checksum
	checksum, err := ComputeEventChecksum(event)
	if err != nil {
		t.Fatalf("Failed to compute checksum: %v", err)
	}

	if checksum == "" {
		t.Errorf("Checksum is empty")
	}

	// Verify checksum is hex-encoded
	if len(checksum) != 64 { // SHA256 hex is 64 characters
		t.Errorf("Expected 64-char checksum, got %d", len(checksum))
	}

	t.Logf("✓ Computed checksum: %s", checksum)
}

func TestValidateEventChecksum(t *testing.T) {
	event := &eventlog.Event{
		ID:        1,
		Type:      eventlog.RowInserted,
		Timestamp: time.Now(),
		Version:   1,
		TxID:      "tx-123",
		Payload: map[string]interface{}{
			"table_name": "users",
			"row_id":     int64(1),
			"data": map[string]interface{}{
				"id":   1,
				"name": "Alice",
			},
		},
	}

	// Compute and assign checksum
	checksum, _ := ComputeEventChecksum(event)
	event.Checksum = checksum

	// Validate
	valid, err := ValidateEventChecksum(event)
	if err != nil {
		t.Fatalf("Validation failed: %v", err)
	}

	if !valid {
		t.Errorf("Checksum validation failed for valid event")
	}

	// Test with corrupted checksum
	event.Checksum = "0000000000000000000000000000000000000000000000000000000000000000"
	valid, _ = ValidateEventChecksum(event)
	if valid {
		t.Errorf("Corrupted checksum should not validate")
	}

	t.Log("✓ Checksum validation test passed")
}

func TestDetectCorruption(t *testing.T) {
	now := time.Now()

	// Create valid event
	validEvent := &eventlog.Event{
		ID:        1,
		Type:      eventlog.SchemaCreated,
		Timestamp: now,
		Version:   1,
		TxID:      "tx-1",
		Payload: map[string]interface{}{
			"table_name": "users",
			"columns": []interface{}{
				map[string]interface{}{"name": "id", "type": "INT"},
				map[string]interface{}{"name": "name", "type": "VARCHAR"},
			},
		},
	}
	validEvent.Checksum, _ = ComputeEventChecksum(validEvent)

	// Create corrupted event (invalid payload)
	corruptedEvent := &eventlog.Event{
		ID:        2,
		Type:      eventlog.RowInserted,
		Timestamp: now.Add(time.Second),
		Version:   1,
		TxID:      "tx-2",
		Payload: map[string]interface{}{
			"table_name": "users",
			// Missing "row_id" and "data"
		},
	}
	corruptedEvent.Checksum, _ = ComputeEventChecksum(corruptedEvent)

	// Create event with bad checksum
	checksumEvent := &eventlog.Event{
		ID:        3,
		Type:      eventlog.RowInserted,
		Timestamp: now.Add(2 * time.Second),
		Version:   1,
		TxID:      "tx-3",
		Payload: map[string]interface{}{
			"table_name": "users",
			"row_id":     int64(1),
			"data": map[string]interface{}{
				"id":   1,
				"name": "Bob",
			},
		},
		Checksum: "badbadbadbadbadbadbadbadbadbadbadbadbadbadbadbadbadbadbadbadba",
	}

	events := []*eventlog.Event{validEvent, corruptedEvent, checksumEvent}

	// Detect corruption
	report := DetectCorruption(events, nil)

	if report.TotalEvents != 3 {
		t.Errorf("Expected 3 total events, got %d", report.TotalEvents)
	}

	if report.CorruptedEvents != 2 {
		t.Errorf("Expected 2 corrupted events, got %d", report.CorruptedEvents)
	}

	if len(report.Issues) != 2 {
		t.Errorf("Expected 2 corruption issues, got %d", len(report.Issues))
	}

	if report.FirstIssueAt != 2 {
		t.Errorf("Expected first issue at event 2, got %d", report.FirstIssueAt)
	}

	if !report.CanPartialReplay {
		t.Errorf("Should be able to partially replay")
	}

	t.Logf("✓ Corruption detection found %d corrupted events", report.CorruptedEvents)
}

func TestReplayEventsDeterministic(t *testing.T) {
	now := time.Now()

	// Create schema creation event
	schemaEvent := &eventlog.Event{
		ID:        1,
		Type:      eventlog.SchemaCreated,
		Timestamp: now,
		Version:   1,
		TxID:      "tx-1",
		Payload: map[string]interface{}{
			"table_name": "users",
			"columns": []interface{}{
				map[string]interface{}{"name": "id", "type": "INT"},
				map[string]interface{}{"name": "name", "type": "VARCHAR"},
			},
		},
	}
	schemaEvent.Checksum, _ = ComputeEventChecksum(schemaEvent)

	// Create row insertion event
	insertEvent := &eventlog.Event{
		ID:        2,
		Type:      eventlog.RowInserted,
		Timestamp: now.Add(time.Second),
		Version:   1,
		TxID:      "tx-2",
		Payload: map[string]interface{}{
			"table_name": "users",
			"row_id":     int64(1),
			"data": map[string]interface{}{
				"id":   1,
				"name": "Alice",
			},
		},
	}
	insertEvent.Checksum, _ = ComputeEventChecksum(insertEvent)

	// Create row update event
	updateEvent := &eventlog.Event{
		ID:        3,
		Type:      eventlog.RowUpdated,
		Timestamp: now.Add(2 * time.Second),
		Version:   1,
		TxID:      "tx-3",
		Payload: map[string]interface{}{
			"table_name": "users",
			"row_id":     int64(1),
			"changes": map[string]interface{}{
				"name": "Alice Updated",
			},
		},
	}
	updateEvent.Checksum, _ = ComputeEventChecksum(updateEvent)

	events := []*eventlog.Event{schemaEvent, insertEvent, updateEvent}

	// Replay deterministically
	opts := &DeterministicReplayOptions{
		TargetSchemaVersion: 1,
		SkipCorrupted:       false,
		CollectErrors:       true,
	}

	result := ReplayEventsDeterministic(events, opts, nil)

	if result.EventsProcessed != 3 {
		t.Errorf("Expected 3 events processed, got %d", result.EventsProcessed)
	}

	if result.ErrorsEncountered != 0 {
		t.Errorf("Expected 0 errors, got %d", result.ErrorsEncountered)
	}

	if result.State == nil {
		t.Fatalf("State is nil")
	}

	// Verify table was created
	if _, exists := result.State.Tables["users"]; !exists {
		t.Errorf("Expected table 'users' to be created")
	}

	// Verify row was inserted and updated
	if len(result.State.Tables["users"]) != 1 {
		t.Errorf("Expected 1 row in users table, got %d", len(result.State.Tables["users"]))
	}

	row := result.State.Tables["users"][1]
	if row["name"] != "Alice Updated" {
		t.Errorf("Expected name='Alice Updated', got %v", row["name"])
	}

	if !result.ReplayDeterministic {
		t.Errorf("Expected deterministic replay")
	}

	t.Log("✓ Deterministic replay test passed")
}

func TestReplayWithCorruptedEvents(t *testing.T) {
	now := time.Now()

	// Create schema creation event
	schemaEvent := &eventlog.Event{
		ID:        1,
		Type:      eventlog.SchemaCreated,
		Timestamp: now,
		Version:   1,
		TxID:      "tx-1",
		Payload: map[string]interface{}{
			"table_name": "users",
			"columns": []interface{}{
				map[string]interface{}{"name": "id", "type": "INT"},
				map[string]interface{}{"name": "name", "type": "VARCHAR"},
			},
		},
	}
	schemaEvent.Checksum, _ = ComputeEventChecksum(schemaEvent)

	// Create valid insert event
	insertEvent1 := &eventlog.Event{
		ID:        2,
		Type:      eventlog.RowInserted,
		Timestamp: now.Add(time.Second),
		Version:   1,
		TxID:      "tx-2",
		Payload: map[string]interface{}{
			"table_name": "users",
			"row_id":     int64(1),
			"data": map[string]interface{}{
				"id":   1,
				"name": "Alice",
			},
		},
	}
	insertEvent1.Checksum, _ = ComputeEventChecksum(insertEvent1)

	// Create corrupted event
	corruptedEvent := &eventlog.Event{
		ID:        3,
		Type:      eventlog.RowInserted,
		Timestamp: now.Add(2 * time.Second),
		Version:   1,
		TxID:      "tx-3",
		Payload: map[string]interface{}{
			"table_name": "users",
			// Missing required fields
		},
		Checksum: "badbadbadbadbadbadbadbadbadbadbadbadbadbadbadbadbadbadbadbadbad",
	}

	// Create valid insert event after corruption
	insertEvent2 := &eventlog.Event{
		ID:        4,
		Type:      eventlog.RowInserted,
		Timestamp: now.Add(3 * time.Second),
		Version:   1,
		TxID:      "tx-4",
		Payload: map[string]interface{}{
			"table_name": "users",
			"row_id":     int64(2),
			"data": map[string]interface{}{
				"id":   2,
				"name": "Bob",
			},
		},
	}
	insertEvent2.Checksum, _ = ComputeEventChecksum(insertEvent2)

	events := []*eventlog.Event{schemaEvent, insertEvent1, corruptedEvent, insertEvent2}

	// Replay with partial recovery
	opts := &DeterministicReplayOptions{
		TargetSchemaVersion: 1,
		SkipCorrupted:       true, // Skip corrupted events
		CollectErrors:       true,
	}

	result := ReplayEventsDeterministic(events, opts, nil)

	if result.EventsProcessed != 3 {
		t.Errorf("Expected 3 events processed (skipping corrupted), got %d", result.EventsProcessed)
	}

	if result.CorruptionReport.CorruptedEvents != 1 {
		t.Errorf("Expected 1 corrupted event, got %d", result.CorruptionReport.CorruptedEvents)
	}

	if len(result.State.Tables["users"]) != 2 {
		t.Errorf("Expected 2 rows recovered, got %d", len(result.State.Tables["users"]))
	}

	if result.CorruptionReport.FirstIssueAt != 3 {
		t.Errorf("Expected first corruption at event 3, got %d", result.CorruptionReport.FirstIssueAt)
	}

	t.Logf("✓ Partial recovery test passed - recovered %d of %d events", result.EventsProcessed, len(events))
}

func TestReplayResultFormatting(t *testing.T) {
	now := time.Now()

	// Create a simple event
	event := &eventlog.Event{
		ID:        1,
		Type:      eventlog.SchemaCreated,
		Timestamp: now,
		Version:   1,
		TxID:      "tx-1",
		Payload: map[string]interface{}{
			"table_name": "test",
			"columns": []interface{}{
				map[string]interface{}{"name": "id", "type": "INT"},
			},
		},
	}
	event.Checksum, _ = ComputeEventChecksum(event)

	// Replay
	result := ReplayEventsDeterministic([]*eventlog.Event{event}, nil, nil)

	// Format status
	status := GetDeterministicReplayStatus(result)

	if status == "" {
		t.Errorf("Status should not be empty")
	}

	if len(status) < 20 {
		t.Errorf("Status message too short: %s", status)
	}

	t.Logf("✓ Replay status formatting test passed\n%s", status)
}

func TestDeterministicReplayAcrossVersions(t *testing.T) {
	now := time.Now()

	// Create schema v1
	schemaEventV1 := &eventlog.Event{
		ID:        1,
		Type:      eventlog.SchemaCreated,
		Timestamp: now,
		Version:   1,
		TxID:      "tx-1",
		Payload: map[string]interface{}{
			"table_name": "users",
			"columns": []interface{}{
				map[string]interface{}{"name": "id", "type": "INT"},
				map[string]interface{}{"name": "name", "type": "VARCHAR"},
			},
		},
	}
	schemaEventV1.Checksum, _ = ComputeEventChecksum(schemaEventV1)

	// Insert with v1 schema
	insertEventV1 := &eventlog.Event{
		ID:        2,
		Type:      eventlog.RowInserted,
		Timestamp: now.Add(time.Second),
		Version:   1,
		TxID:      "tx-2",
		Payload: map[string]interface{}{
			"table_name": "users",
			"row_id":     int64(1),
			"data": map[string]interface{}{
				"id":   1,
				"name": "Alice",
			},
		},
	}
	insertEventV1.Checksum, _ = ComputeEventChecksum(insertEventV1)

	// Schema evolution event
	evolveEvent := &eventlog.Event{
		ID:        3,
		Type:      eventlog.SchemaEvolved,
		Timestamp: now.Add(2 * time.Second),
		Version:   2,
		TxID:      "tx-3",
		Payload: map[string]interface{}{
			"table_name": "users",
			"evolution": map[string]interface{}{
				"added_columns": []interface{}{
					map[string]interface{}{"name": "email", "type": "VARCHAR"},
				},
			},
		},
	}
	evolveEvent.Checksum, _ = ComputeEventChecksum(evolveEvent)

	// Insert with v2 schema
	insertEventV2 := &eventlog.Event{
		ID:        4,
		Type:      eventlog.RowInserted,
		Timestamp: now.Add(3 * time.Second),
		Version:   2,
		TxID:      "tx-4",
		Payload: map[string]interface{}{
			"table_name": "users",
			"row_id":     int64(2),
			"data": map[string]interface{}{
				"id":    2,
				"name":  "Bob",
				"email": "bob@example.com",
			},
		},
	}
	insertEventV2.Checksum, _ = ComputeEventChecksum(insertEventV2)

	events := []*eventlog.Event{schemaEventV1, insertEventV1, evolveEvent, insertEventV2}

	// Replay deterministically targeting v2
	opts := &DeterministicReplayOptions{
		TargetSchemaVersion: 2,
		SkipCorrupted:       false,
		CollectErrors:       true,
	}

	result := ReplayEventsDeterministic(events, opts, nil)

	if result.EventsProcessed != 4 {
		t.Errorf("Expected 4 events processed, got %d", result.EventsProcessed)
	}

	// Verify rows exist
	if len(result.State.Tables["users"]) != 2 {
		t.Errorf("Expected 2 rows, got %d", len(result.State.Tables["users"]))
	}

	if result.CorruptionReport.LastValidEvent != 4 {
		t.Errorf("Expected last valid event 4, got %d", result.CorruptionReport.LastValidEvent)
	}

	t.Log("✓ Cross-version deterministic replay test passed")
}

func TestEventStoreReadAllEvents(t *testing.T) {
	// Create temporary directory
	tmpDir, err := os.MkdirTemp("", "eventstore-test-")
	if err != nil {
		t.Fatalf("Failed to create temp directory: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	// Create event store
	es, err := NewEventStore(tmpDir)
	if err != nil {
		t.Fatalf("Failed to create event store: %v", err)
	}

	// Record some events
	event1, _ := es.RecordSchemaCreated("users", []eventlog.ColumnDefinition{
		{Name: "id", Type: "INT", PrimaryKey: true},
		{Name: "name", Type: "VARCHAR"},
	}, "id", "tx-1")

	event2, _ := es.RecordRowInserted("users", 1, map[string]interface{}{
		"id":   1,
		"name": "Alice",
	}, "tx-2")

	// Read all events
	events, err := es.ReadAllEvents()
	if err != nil {
		t.Fatalf("Failed to read events: %v", err)
	}

	if len(events) != 2 {
		t.Errorf("Expected 2 events, got %d", len(events))
	}

	if events[0].ID != event1.ID {
		t.Errorf("First event ID mismatch")
	}

	if events[1].ID != event2.ID {
		t.Errorf("Second event ID mismatch")
	}

	t.Log("✓ Event store read all events test passed")
}

func TestCorruptionReportJSON(t *testing.T) {
	report := &CorruptionReport{
		TotalEvents:     10,
		CorruptedEvents: 2,
		Issues: []CorruptionIssue{
			{
				EventID:   3,
				EventType: "ROW_INSERTED",
				IssueType: "checksum_mismatch",
				Message:   "Checksum validation failed",
				Timestamp: time.Now().String(),
			},
		},
		RecoveredEvents:  8,
		FirstIssueAt:     3,
		LastValidEvent:   10,
		CanPartialReplay: true,
	}

	// Marshal to JSON
	jsonBytes, err := json.Marshal(report)
	if err != nil {
		t.Fatalf("Failed to marshal report: %v", err)
	}

	// Unmarshal to verify
	var unmarshaled CorruptionReport
	err = json.Unmarshal(jsonBytes, &unmarshaled)
	if err != nil {
		t.Fatalf("Failed to unmarshal report: %v", err)
	}

	if unmarshaled.CorruptedEvents != 2 {
		t.Errorf("Unmarshaled corrupted events mismatch")
	}

	t.Log("✓ Corruption report JSON serialization test passed")
}
