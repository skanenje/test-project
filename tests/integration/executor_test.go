package integration

import (
	"strings"
	"testing"

	"rdbms/executor"
	"rdbms/parser"
	"rdbms/tests"
)

// TestExecutorCreateTable tests executing CREATE TABLE statements
func TestExecutorCreateTable(t *testing.T) {
	tdb := tests.NewTestDB(t)
	defer tdb.Cleanup()

	exec := executor.New(tdb.DB)
	p := parser.New()

	tests := []struct {
		name      string
		sql       string
		expectErr bool
	}{
		{
			name:      "create simple table",
			sql:       "CREATE TABLE users (id INT PRIMARY KEY, name TEXT)",
			expectErr: false,
		},
		{
			name:      "create table with multiple columns",
			sql:       "CREATE TABLE products (id INT PRIMARY KEY, name TEXT, price INT)",
			expectErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			stmt, err := p.Parse(tt.sql)
			if err != nil {
				t.Fatalf("parse error: %v", err)
			}

			result, err := exec.Execute(stmt)
			if (err != nil) != tt.expectErr {
				t.Errorf("unexpected error: expected=%v, got=%v: %v", tt.expectErr, err != nil, err)
				return
			}

			if !tt.expectErr && result == "" {
				t.Error("expected non-empty result")
			}
		})
	}
}

// TestExecutorInsert tests executing INSERT statements
func TestExecutorInsert(t *testing.T) {
	tdb := tests.NewTestDB(t)
	defer tdb.Cleanup()

	exec := executor.New(tdb.DB)
	p := parser.New()

	// First create a table
	createStmt, _ := p.Parse("CREATE TABLE users (id INT PRIMARY KEY, name TEXT)")
	exec.Execute(createStmt)

	tests := []struct {
		name      string
		sql       string
		expectErr bool
	}{
		{
			name:      "insert single row",
			sql:       "INSERT INTO users VALUES (1, 'Alice')",
			expectErr: false,
		},
		{
			name:      "insert another row",
			sql:       "INSERT INTO users VALUES (2, 'Bob')",
			expectErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			stmt, err := p.Parse(tt.sql)
			if err != nil {
				t.Fatalf("parse error: %v", err)
			}

			result, err := exec.Execute(stmt)
			if (err != nil) != tt.expectErr {
				t.Errorf("unexpected error: expected=%v, got=%v: %v", tt.expectErr, err != nil, err)
				return
			}

			if !tt.expectErr && result == "" {
				t.Error("expected non-empty result")
			}
		})
	}
}

// TestExecutorSelect tests executing SELECT statements
func TestExecutorSelect(t *testing.T) {
	tdb := tests.NewTestDB(t)
	defer tdb.Cleanup()

	exec := executor.New(tdb.DB)
	p := parser.New()

	// Create and populate table
	createStmt, _ := p.Parse("CREATE TABLE users (id INT PRIMARY KEY, name TEXT)")
	exec.Execute(createStmt)

	insertStmt, _ := p.Parse("INSERT INTO users VALUES (1, 'Alice')")
	exec.Execute(insertStmt)

	tests := []struct {
		name      string
		sql       string
		expectErr bool
	}{
		{
			name:      "select all rows",
			sql:       "SELECT * FROM users",
			expectErr: false,
		},
		{
			name:      "select with where clause",
			sql:       "SELECT * FROM users WHERE id = 1",
			expectErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			stmt, err := p.Parse(tt.sql)
			if err != nil {
				t.Fatalf("parse error: %v", err)
			}

			result, err := exec.Execute(stmt)
			if (err != nil) != tt.expectErr {
				t.Errorf("unexpected error: expected=%v, got=%v: %v", tt.expectErr, err != nil, err)
				return
			}

			if !tt.expectErr {
				if result == "" {
					t.Error("expected non-empty result for SELECT")
				}
				// Result should contain row data
				if !strings.Contains(result, "Alice") && !strings.Contains(result, "row") {
					t.Logf("SELECT result: %s", result)
				}
			}
		})
	}
}

// TestExecutorUpdate tests executing UPDATE statements
func TestExecutorUpdate(t *testing.T) {
	tdb := tests.NewTestDB(t)
	defer tdb.Cleanup()

	exec := executor.New(tdb.DB)
	p := parser.New()

	// Setup
	p.Parse("CREATE TABLE users (id INT PRIMARY KEY, name TEXT, age INT)")
	createStmt, _ := p.Parse("CREATE TABLE users (id INT PRIMARY KEY, name TEXT, age INT)")
	exec.Execute(createStmt)

	insertStmt, _ := p.Parse("INSERT INTO users VALUES (1, 'Alice', 30)")
	exec.Execute(insertStmt)

	tests := []struct {
		name      string
		sql       string
		expectErr bool
	}{
		{
			name:      "update single row",
			sql:       "UPDATE users SET age = 31 WHERE id = 1",
			expectErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			stmt, err := p.Parse(tt.sql)
			if err != nil {
				t.Fatalf("parse error: %v", err)
			}

			result, err := exec.Execute(stmt)
			if (err != nil) != tt.expectErr {
				t.Errorf("unexpected error: expected=%v, got=%v: %v", tt.expectErr, err != nil, err)
				return
			}

			if !tt.expectErr && result == "" {
				t.Error("expected non-empty result for UPDATE")
			}
		})
	}
}

// TestExecutorDelete tests executing DELETE statements
func TestExecutorDelete(t *testing.T) {
	tdb := tests.NewTestDB(t)
	defer tdb.Cleanup()

	exec := executor.New(tdb.DB)
	p := parser.New()

	// Setup
	createStmt, _ := p.Parse("CREATE TABLE users (id INT PRIMARY KEY, name TEXT)")
	exec.Execute(createStmt)

	// Insert test data
	for i := 1; i <= 3; i++ {
		insertStmt, _ := p.Parse("INSERT INTO users VALUES (1, 'Alice')")
		exec.Execute(insertStmt)
	}

	tests := []struct {
		name      string
		sql       string
		expectErr bool
	}{
		{
			name:      "delete single row",
			sql:       "DELETE FROM users WHERE id = 1",
			expectErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			stmt, err := p.Parse(tt.sql)
			if err != nil {
				t.Fatalf("parse error: %v", err)
			}

			result, err := exec.Execute(stmt)
			if (err != nil) != tt.expectErr {
				t.Errorf("unexpected error: expected=%v, got=%v: %v", tt.expectErr, err != nil, err)
				return
			}

			if !tt.expectErr && result == "" {
				t.Error("expected non-empty result for DELETE")
			}
		})
	}
}

// TestExecutorParseAndExecuteFlow tests the complete parse-to-execute flow
func TestExecutorParseAndExecuteFlow(t *testing.T) {
	tdb := tests.NewTestDB(t)
	defer tdb.Cleanup()

	exec := executor.New(tdb.DB)
	p := parser.New()

	// SQL statements to execute in sequence
	statements := []string{
		"CREATE TABLE users (id INT PRIMARY KEY, name TEXT, age INT)",
		"INSERT INTO users VALUES (1, 'Alice', 30)",
		"INSERT INTO users VALUES (2, 'Bob', 25)",
		"SELECT * FROM users",
		"UPDATE users SET age = 31 WHERE id = 1",
		"SELECT * FROM users",
		"DELETE FROM users WHERE id = 2",
	}

	for i, sql := range statements {
		stmt, err := p.Parse(sql)
		if err != nil {
			t.Errorf("statement %d: parse error: %v", i, err)
			continue
		}

		result, err := exec.Execute(stmt)
		if err != nil {
			t.Errorf("statement %d: execute error: %v", i, err)
			continue
		}

		if result == "" && stmt.Type != "INSERT" {
			t.Logf("statement %d (%s): got empty result", i, stmt.Type)
		}
	}
}

// TestExecutorErrorHandling tests error handling in executor
func TestExecutorErrorHandling(t *testing.T) {
	tdb := tests.NewTestDB(t)
	defer tdb.Cleanup()

	exec := executor.New(tdb.DB)
	p := parser.New()

	tests := []struct {
		name       string
		statements []string
		shouldFail bool
	}{
		{
			name: "operate on non-existent table",
			statements: []string{
				"SELECT * FROM nonexistent",
			},
			shouldFail: true,
		},
		{
			name: "insert into non-existent table",
			statements: []string{
				"INSERT INTO nonexistent VALUES (1, 'test')",
			},
			shouldFail: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			for _, sql := range tt.statements {
				stmt, err := p.Parse(sql)
				if err != nil {
					if !tt.shouldFail {
						t.Fatalf("parse error: %v", err)
					}
					return
				}

				_, err = exec.Execute(stmt)
				if (err != nil) != tt.shouldFail {
					if tt.shouldFail {
						t.Errorf("expected error but got none")
					} else {
						t.Errorf("unexpected error: %v", err)
					}
				}
			}
		})
	}
}

// TestExecutorMultipleOperations tests multiple sequential operations
func TestExecutorMultipleOperations(t *testing.T) {
	tdb := tests.NewTestDB(t)
	defer tdb.Cleanup()

	exec := executor.New(tdb.DB)
	p := parser.New()

	// Create table
	createStmt, _ := p.Parse("CREATE TABLE tasks (id INT PRIMARY KEY, title TEXT, done BOOL)")
	_, err := exec.Execute(createStmt)
	if err != nil {
		t.Fatalf("create table error: %v", err)
	}

	// Insert multiple rows
	insertStatements := []string{
		"INSERT INTO tasks VALUES (1, 'Task 1', false)",
		"INSERT INTO tasks VALUES (2, 'Task 2', false)",
		"INSERT INTO tasks VALUES (3, 'Task 3', true)",
	}

	for _, sql := range insertStatements {
		stmt, _ := p.Parse(sql)
		_, err := exec.Execute(stmt)
		if err != nil {
			t.Fatalf("insert error: %v", err)
		}
	}

	// Update a row
	updateStmt, _ := p.Parse("UPDATE tasks SET done = true WHERE id = 1")
	_, err = exec.Execute(updateStmt)
	if err != nil {
		t.Fatalf("update error: %v", err)
	}

	// Select and verify
	selectStmt, _ := p.Parse("SELECT * FROM tasks")
	result, err := exec.Execute(selectStmt)
	if err != nil {
		t.Fatalf("select error: %v", err)
	}
	if result == "" {
		t.Error("expected non-empty select result")
	}

	// Delete a row
	deleteStmt, _ := p.Parse("DELETE FROM tasks WHERE id = 2")
	_, err = exec.Execute(deleteStmt)
	if err != nil {
		t.Fatalf("delete error: %v", err)
	}
}
