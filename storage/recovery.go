package storage

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"rdbms/eventlog"
)

// DeterministicReplayOptions configures deterministic replay behavior
type DeterministicReplayOptions struct {
	// TargetSchemaVersion is the schema version to replay events to (current version if not set)
	TargetSchemaVersion int
	// SkipCorrupted determines if corrupted events should be skipped (partial recovery)
	SkipCorrupted bool
	// CollectErrors determines if errors should be collected (for reporting)
	CollectErrors bool
	// MigrationHandler applies schema migrations during replay
	MigrationHandler *MigrationHandler
}

// CorruptionIssue represents a single corruption problem in the event log
type CorruptionIssue struct {
	EventID   uint64 `json:"event_id"`
	EventType string `json:"event_type"`
	IssueType string `json:"issue_type"` // "checksum_mismatch", "invalid_payload", "schema_violation"
	Message   string `json:"message"`
	Position  int64  `json:"position"` // Byte offset in log file
	Timestamp string `json:"timestamp"`
}

// CorruptionReport contains detailed corruption analysis results
type CorruptionReport struct {
	TotalEvents      int               `json:"total_events"`
	CorruptedEvents  int               `json:"corrupted_events"`
	Issues           []CorruptionIssue `json:"issues"`
	RecoveredEvents  int               `json:"recovered_events"` // When skipping corrupted events
	FirstIssueAt     uint64            `json:"first_issue_at"`   // Event ID of first corruption
	LastValidEvent   uint64            `json:"last_valid_event"` // Last successfully processed event ID
	CanPartialReplay bool              `json:"can_partial_replay"`
}

// ReplayResult combines the derived state with replay diagnostics
type ReplayResult struct {
	State               *DerivedState
	CorruptionReport    *CorruptionReport
	EventsProcessed     int
	ErrorsEncountered   int
	ReplayDeterministic bool // True if replay is guaranteed to produce same result
}

// ComputeEventChecksum computes the SHA256 checksum of an event (excluding the checksum field)
func ComputeEventChecksum(e *eventlog.Event) (string, error) {
	// Create a copy of the event without the checksum field
	eventCopy := map[string]interface{}{
		"id":        e.ID,
		"type":      e.Type,
		"timestamp": e.Timestamp,
		"version":   e.Version,
		"tx_id":     e.TxID,
		"payload":   e.Payload,
	}

	jsonBytes, err := json.Marshal(eventCopy)
	if err != nil {
		return "", fmt.Errorf("failed to marshal event for checksum: %w", err)
	}

	hash := sha256.Sum256(jsonBytes)
	return hex.EncodeToString(hash[:]), nil
}

// ValidateEventChecksum verifies that an event's checksum is valid
func ValidateEventChecksum(e *eventlog.Event) (bool, error) {
	if e.Checksum == "" {
		return false, fmt.Errorf("event %d has no checksum", e.ID)
	}

	computedChecksum, err := ComputeEventChecksum(e)
	if err != nil {
		return false, err
	}

	return computedChecksum == e.Checksum, nil
}

// DetectCorruption analyzes events for corruption and returns a detailed report
func DetectCorruption(events []*eventlog.Event, schemaRegistry interface{}) *CorruptionReport {
	report := &CorruptionReport{
		TotalEvents: len(events),
		Issues:      make([]CorruptionIssue, 0),
	}

	if len(events) == 0 {
		return report
	}

	for i, e := range events {
		// Check checksum integrity
		valid, _ := ValidateEventChecksum(e)
		if !valid {
			issue := CorruptionIssue{
				EventID:   e.ID,
				EventType: string(e.Type),
				IssueType: "checksum_mismatch",
				Message:   fmt.Sprintf("Event %d checksum validation failed", e.ID),
				Timestamp: e.Timestamp.String(),
			}
			report.Issues = append(report.Issues, issue)
			report.CorruptedEvents++

			if report.FirstIssueAt == 0 {
				report.FirstIssueAt = e.ID
			}
			continue
		}

		// Validate payload structure based on event type
		if !validatePayloadStructure(e) {
			issue := CorruptionIssue{
				EventID:   e.ID,
				EventType: string(e.Type),
				IssueType: "invalid_payload",
				Message:   fmt.Sprintf("Event %d has invalid payload structure", e.ID),
				Timestamp: e.Timestamp.String(),
			}
			report.Issues = append(report.Issues, issue)
			report.CorruptedEvents++

			if report.FirstIssueAt == 0 {
				report.FirstIssueAt = e.ID
			}
			continue
		}

		// Track last valid event and recovered count
		report.LastValidEvent = e.ID
		if i > 0 && report.FirstIssueAt > 0 {
			report.RecoveredEvents++
		}
	}

	report.CanPartialReplay = report.CorruptedEvents < len(events)
	return report
}

// validatePayloadStructure verifies that an event's payload has the required fields
func validatePayloadStructure(e *eventlog.Event) bool {
	payload, ok := e.Payload.(map[string]interface{})
	if !ok {
		return false
	}

	switch e.Type {
	case eventlog.SchemaCreated:
		_, hasTable := payload["table_name"]
		_, hasCols := payload["columns"]
		return hasTable && hasCols

	case eventlog.RowInserted:
		_, hasTable := payload["table_name"]
		_, hasRowID := payload["row_id"]
		_, hasData := payload["data"]
		return hasTable && hasRowID && hasData

	case eventlog.RowUpdated:
		_, hasTable := payload["table_name"]
		_, hasRowID := payload["row_id"]
		_, hasChanges := payload["changes"]
		return hasTable && hasRowID && hasChanges

	case eventlog.RowDeleted:
		_, hasTable := payload["table_name"]
		_, hasRowID := payload["row_id"]
		return hasTable && hasRowID

	case eventlog.SchemaEvolved:
		_, hasTable := payload["table_name"]
		_, hasEvolution := payload["evolution"]
		return hasTable && hasEvolution

	default:
		return true // Unknown types are not considered corrupt
	}
}

// ReplayEventsDeterministic replays events with full version tracking and deterministic ordering
func ReplayEventsDeterministic(events []*eventlog.Event, opts *DeterministicReplayOptions, migrationHandler *MigrationHandler) *ReplayResult {
	if opts == nil {
		opts = &DeterministicReplayOptions{}
	}

	result := &ReplayResult{
		State: &DerivedState{
			Tables:      make(map[string]map[int64]Row),
			DeletedRows: make(map[string]map[int64]bool),
		},
		CorruptionReport: &CorruptionReport{
			TotalEvents: len(events),
			Issues:      make([]CorruptionIssue, 0),
		},
		ReplayDeterministic: true,
	}

	// Track schema versions per table for migration
	tableSchemaVersions := make(map[string]int)
	eventProcessingOrder := make([]uint64, 0) // Track processing order

	for _, e := range events {
		// Optional: detect and skip corrupted events
		if opts.SkipCorrupted {
			valid, _ := ValidateEventChecksum(e)
			if !valid {
				result.CorruptionReport.CorruptedEvents++
				if result.CorruptionReport.FirstIssueAt == 0 {
					result.CorruptionReport.FirstIssueAt = e.ID
				}
				result.ErrorsEncountered++

				if opts.CollectErrors {
					result.CorruptionReport.Issues = append(result.CorruptionReport.Issues, CorruptionIssue{
						EventID:   e.ID,
						EventType: string(e.Type),
						IssueType: "checksum_mismatch",
						Message:   fmt.Sprintf("Skipping corrupted event %d (checksum mismatch)", e.ID),
						Timestamp: e.Timestamp.String(),
					})
				}
				continue
			}

			if !validatePayloadStructure(e) {
				result.CorruptionReport.CorruptedEvents++
				if result.CorruptionReport.FirstIssueAt == 0 {
					result.CorruptionReport.FirstIssueAt = e.ID
				}
				result.ErrorsEncountered++

				if opts.CollectErrors {
					result.CorruptionReport.Issues = append(result.CorruptionReport.Issues, CorruptionIssue{
						EventID:   e.ID,
						EventType: string(e.Type),
						IssueType: "invalid_payload",
						Message:   fmt.Sprintf("Skipping corrupted event %d (invalid payload)", e.ID),
						Timestamp: e.Timestamp.String(),
					})
				}
				continue
			}
		}

		eventProcessingOrder = append(eventProcessingOrder, e.ID)
		result.EventsProcessed++
		result.CorruptionReport.LastValidEvent = e.ID

		// Apply event to state based on deterministic version ordering
		applyEventDeterministic(e, result.State, tableSchemaVersions, opts, migrationHandler)
	}

	result.CorruptionReport.CanPartialReplay = result.CorruptionReport.CorruptedEvents < len(events)
	return result
}

// applyEventDeterministic applies a single event to the derived state with version tracking
func applyEventDeterministic(e *eventlog.Event, state *DerivedState, tableVersions map[string]int, opts *DeterministicReplayOptions, migrationHandler *MigrationHandler) {
	payload, ok := e.Payload.(map[string]interface{})
	if !ok {
		return
	}

	switch e.Type {
	case eventlog.SchemaCreated:
		tableName, _ := payload["table_name"].(string)
		if _, exists := state.Tables[tableName]; !exists {
			state.Tables[tableName] = make(map[int64]Row)
			state.DeletedRows[tableName] = make(map[int64]bool)
		}
		tableVersions[tableName] = e.Version

	case eventlog.RowInserted:
		tableName, _ := payload["table_name"].(string)
		var rowID int64
		// Handle both float64 and int64 types
		if rowIDFloat, ok := payload["row_id"].(float64); ok {
			rowID = int64(rowIDFloat)
		} else if rowIDInt, ok := payload["row_id"].(int64); ok {
			rowID = rowIDInt
		}

		dataRaw, _ := payload["data"].(map[string]interface{})
		row := Row(dataRaw)

		if _, exists := state.Tables[tableName]; !exists {
			state.Tables[tableName] = make(map[int64]Row)
			state.DeletedRows[tableName] = make(map[int64]bool)
		}

		// Apply migration if needed
		if migrationHandler != nil && opts.TargetSchemaVersion > 0 {
			eventVersion := e.Version
			if eventVersion != opts.TargetSchemaVersion {
				migratedRow, err := migrationHandler.MigrateRowIfNeeded(tableName, row, eventVersion, opts.TargetSchemaVersion)
				if err == nil {
					row = migratedRow
				}
			}
		}

		state.Tables[tableName][rowID] = row
		delete(state.DeletedRows[tableName], rowID)

	case eventlog.RowUpdated:
		tableName, _ := payload["table_name"].(string)
		var rowID int64
		// Handle both float64 and int64 types
		if rowIDFloat, ok := payload["row_id"].(float64); ok {
			rowID = int64(rowIDFloat)
		} else if rowIDInt, ok := payload["row_id"].(int64); ok {
			rowID = rowIDInt
		}

		changesRaw, _ := payload["changes"].(map[string]interface{})

		if _, exists := state.Tables[tableName]; !exists {
			state.Tables[tableName] = make(map[int64]Row)
			state.DeletedRows[tableName] = make(map[int64]bool)
		}

		if _, exists := state.Tables[tableName][rowID]; !exists {
			state.Tables[tableName][rowID] = make(Row)
		}

		for k, v := range changesRaw {
			state.Tables[tableName][rowID][k] = v
		}

	case eventlog.RowDeleted:
		tableName, _ := payload["table_name"].(string)
		var rowID int64
		// Handle both float64 and int64 types
		if rowIDFloat, ok := payload["row_id"].(float64); ok {
			rowID = int64(rowIDFloat)
		} else if rowIDInt, ok := payload["row_id"].(int64); ok {
			rowID = rowIDInt
		}

		if _, exists := state.Tables[tableName]; !exists {
			state.Tables[tableName] = make(map[int64]Row)
			state.DeletedRows[tableName] = make(map[int64]bool)
		}

		state.DeletedRows[tableName][rowID] = true

	case eventlog.SchemaEvolved:
		tableName, _ := payload["table_name"].(string)
		tableVersions[tableName] = e.Version
	}
}

// GetDeterministicReplayStatus returns current replay status information
func GetDeterministicReplayStatus(result *ReplayResult) string {
	status := fmt.Sprintf("Replay Status:\n")
	status += fmt.Sprintf("  Events Processed: %d / %d\n", result.EventsProcessed, result.CorruptionReport.TotalEvents)
	status += fmt.Sprintf("  Corrupted Events: %d\n", result.CorruptionReport.CorruptedEvents)
	status += fmt.Sprintf("  Errors Encountered: %d\n", result.ErrorsEncountered)
	status += fmt.Sprintf("  Deterministic: %v\n", result.ReplayDeterministic)

	if result.CorruptionReport.FirstIssueAt > 0 {
		status += fmt.Sprintf("  First Issue at Event: %d\n", result.CorruptionReport.FirstIssueAt)
		status += fmt.Sprintf("  Last Valid Event: %d\n", result.CorruptionReport.LastValidEvent)
	}

	if len(result.CorruptionReport.Issues) > 0 {
		status += fmt.Sprintf("\nCorruption Issues (%d):\n", len(result.CorruptionReport.Issues))
		for _, issue := range result.CorruptionReport.Issues {
			status += fmt.Sprintf("  - Event %d (%s): %s - %s\n", issue.EventID, issue.IssueType, issue.EventType, issue.Message)
		}
	}

	return status
}
