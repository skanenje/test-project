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
	db *database.Database
}

// New creates a new executor
func New(db *database.Database) *Executor {
	return &Executor{db: db}
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
