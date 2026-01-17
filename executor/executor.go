package executor

import (
	"fmt"
	"strings"

	"rdbms/database"
	"rdbms/parser"
	"rdbms/storage"
)

// Executor executes parsed SQL statements
type Executor struct {
	db               *database.Database
	lastReplayResult *storage.ReplayResult
	migrationHandler *storage.MigrationHandler
	recoveryReport   *storage.CorruptionReport
}

// New creates a new executor
func New(db *database.Database) *Executor {
	return &Executor{
		db:               db,
		lastReplayResult: nil,
		migrationHandler: nil,
		recoveryReport:   nil,
	}
}

// Execute executes a parsed statement
func (e *Executor) Execute(stmt *parser.ParsedStatement) (string, error) {
	switch stmt.Type {
	case "CREATE_TABLE":
		return e.executeCreateTable(stmt)
	case "INSERT":
		return e.executeInsert(stmt)
	case "SELECT":
		return e.executeSelect(stmt)
	case "DELETE":
		return e.executeDelete(stmt)
	case "UPDATE":
		return e.executeUpdate(stmt)
	case "JOIN":
		return e.executeJoin(stmt)
	default:
		return "", fmt.Errorf("unknown statement type: %s", stmt.Type)
	}
}

func (e *Executor) executeCreateTable(stmt *parser.ParsedStatement) (string, error) {
	if err := e.db.CreateTable(stmt.TableName, stmt.Columns); err != nil {
		return "", err
	}
	return fmt.Sprintf("Table '%s' created", stmt.TableName), nil
}

func (e *Executor) executeInsert(stmt *parser.ParsedStatement) (string, error) {
	// Get raw values from parser
	rawValues := stmt.Values["_raw_values"].([]interface{})

	// Get table schema to map values to columns
	table, err := e.db.GetTable(stmt.TableName)
	if err != nil {
		return "", err
	}

	if len(rawValues) != len(table.Columns) {
		return "", fmt.Errorf("value count mismatch")
	}

	// Build row
	row := make(storage.Row)
	for i, col := range table.Columns {
		row[col.Name] = rawValues[i]
	}

	rowID, err := e.db.Insert(stmt.TableName, row)
	if err != nil {
		return "", err
	}

	return fmt.Sprintf("Inserted row with ID %d", rowID), nil
}

func (e *Executor) executeSelect(stmt *parser.ParsedStatement) (string, error) {
	rows, err := e.db.Select(stmt.TableName, stmt.Where)
	if err != nil {
		return "", err
	}
	return formatRows(rows), nil
}

func (e *Executor) executeDelete(stmt *parser.ParsedStatement) (string, error) {
	count, err := e.db.Delete(stmt.TableName, stmt.Where)
	if err != nil {
		return "", err
	}
	return fmt.Sprintf("Deleted %d row(s)", count), nil
}

func (e *Executor) executeUpdate(stmt *parser.ParsedStatement) (string, error) {
	count, err := e.db.Update(stmt.TableName, stmt.SetColumn, stmt.SetValue, stmt.Where)
	if err != nil {
		return "", err
	}
	return fmt.Sprintf("Updated %d row(s)", count), nil
}

// Format rows for display
func formatRows(rows []storage.Row) string {
	if len(rows) == 0 {
		return "No rows returned"
	}

	var result strings.Builder
	for _, row := range rows {
		result.WriteString(fmt.Sprintf("%v\n", row))
	}
	return strings.TrimSpace(result.String())
}

func (e *Executor) executeJoin(stmt *parser.ParsedStatement) (string, error) {
	rows, err := e.db.Join(stmt.TableName, stmt.JoinTable, stmt.JoinCondition, stmt.Where)
	if err != nil {
		return "", err
	}
	return formatRows(rows), nil
}

// SetMigrationHandler sets the migration handler for schema transformations
func (e *Executor) SetMigrationHandler(mh *storage.MigrationHandler) {
	e.migrationHandler = mh
}

// Replay replays all events deterministically from the event store
func (e *Executor) Replay(targetSchemaVersion int) (*storage.ReplayResult, error) {
	// Get the event store from the database
	eventStore := e.db.GetEventStore()
	if eventStore == nil {
		return nil, fmt.Errorf("event store not available")
	}

	// Read all events
	events, err := eventStore.ReadAllEvents()
	if err != nil {
		return nil, fmt.Errorf("failed to read events: %w", err)
	}

	// Perform deterministic replay
	opts := &storage.DeterministicReplayOptions{
		TargetSchemaVersion: targetSchemaVersion,
		SkipCorrupted:       false,
		CollectErrors:       true,
		MigrationHandler:    e.migrationHandler,
	}

	result := storage.ReplayEventsDeterministic(events, opts, e.migrationHandler)
	e.lastReplayResult = result

	return result, nil
}

// ReplayWithRecovery replays events with partial recovery for corrupted events
func (e *Executor) ReplayWithRecovery(targetSchemaVersion int) (*storage.ReplayResult, error) {
	// Get the event store from the database
	eventStore := e.db.GetEventStore()
	if eventStore == nil {
		return nil, fmt.Errorf("event store not available")
	}

	// Read all events
	events, err := eventStore.ReadAllEvents()
	if err != nil {
		return nil, fmt.Errorf("failed to read events: %w", err)
	}

	// Detect corruption first
	corruptionReport := storage.DetectCorruption(events, nil)
	e.recoveryReport = corruptionReport

	// If there's corruption and we can partially recover, do so
	if corruptionReport.CorruptedEvents > 0 && corruptionReport.CanPartialReplay {
		opts := &storage.DeterministicReplayOptions{
			TargetSchemaVersion: targetSchemaVersion,
			SkipCorrupted:       true, // Skip corrupted events
			CollectErrors:       true,
			MigrationHandler:    e.migrationHandler,
		}

		result := storage.ReplayEventsDeterministic(events, opts, e.migrationHandler)
		e.lastReplayResult = result
		return result, nil
	}

	// No corruption or can't partially recover, do full deterministic replay
	opts := &storage.DeterministicReplayOptions{
		TargetSchemaVersion: targetSchemaVersion,
		SkipCorrupted:       false,
		CollectErrors:       true,
		MigrationHandler:    e.migrationHandler,
	}

	result := storage.ReplayEventsDeterministic(events, opts, e.migrationHandler)
	e.lastReplayResult = result

	if corruptionReport.CorruptedEvents > 0 {
		return result, fmt.Errorf("replay completed with %d corrupted events", corruptionReport.CorruptedEvents)
	}

	return result, nil
}

// GetRecoveryReport returns the most recent corruption/recovery report
func (e *Executor) GetRecoveryReport() *storage.CorruptionReport {
	if e.lastReplayResult != nil {
		return e.lastReplayResult.CorruptionReport
	}
	return e.recoveryReport
}

// GetReplayStatus returns formatted status of the last replay operation
func (e *Executor) GetReplayStatus() string {
	if e.lastReplayResult == nil {
		return "No replay has been performed yet"
	}
	return storage.GetDeterministicReplayStatus(e.lastReplayResult)
}

// ValidateEventIntegrity checks all events in the event log for corruption
func (e *Executor) ValidateEventIntegrity() *storage.CorruptionReport {
	eventStore := e.db.GetEventStore()
	if eventStore == nil {
		return nil
	}

	events, err := eventStore.ReadAllEvents()
	if err != nil {
		return nil
	}

	return storage.DetectCorruption(events, nil)
}
