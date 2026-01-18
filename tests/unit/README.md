# Unit Tests

## Purpose

The `unit` test package contains unit tests for individual functions and components in isolation. These tests verify that:
- Individual functions work correctly with various inputs
- Edge cases are handled
- Error conditions are detected
- Each component can be tested independently

## Test Files

### parser_test.go
Parser unit tests:
- SQL string parsing correctness
- CREATE TABLE parsing
- INSERT parsing
- SELECT parsing
- UPDATE parsing
- DELETE parsing
- JOIN parsing
- Error handling for malformed SQL
- Whitespace and case-insensitivity handling

### infrastructure_test.go
Infrastructure and utility tests:
- File I/O operations
- JSON serialization
- Data structure operations
- Helper function correctness

## Running Tests

```bash
# Run all unit tests
go test ./tests/unit -v

# Run specific test file
go test ./tests/unit -run TestParser -v

# Run specific test
go test ./tests/unit -run TestParserCreateTable -v

# Run with coverage
go test ./tests/unit -v -cover

# Generate coverage report
go test ./tests/unit -coverprofile=coverage.out
go tool cover -html=coverage.out
```

## Unit Test Structure

Unit tests focus on a single function or method:

```go
func TestParserCreateTable(t *testing.T) {
    // Setup
    p := parser.New()
    sql := "CREATE TABLE users (id INT PRIMARY KEY, name TEXT)"

    // Execute
    stmt, err := p.Parse(sql)

    // Assert
    if err != nil {
        t.Fatalf("unexpected error: %v", err)
    }
    if stmt.Type != "CREATE_TABLE" {
        t.Errorf("type: got %s, want CREATE_TABLE", stmt.Type)
    }
    if stmt.TableName != "users" {
        t.Errorf("table: got %s, want users", stmt.TableName)
    }
    if len(stmt.Columns) != 2 {
        t.Errorf("columns: got %d, want 2", len(stmt.Columns))
    }
}
```

## Table-Driven Tests

For testing multiple cases, use table-driven tests:

```go
func TestParserVariouSQL(t *testing.T) {
    tests := []struct {
        name    string
        input   string
        wantType string
        wantErr bool
    }{
        {
            name:     "create table",
            input:    "CREATE TABLE users (id INT)",
            wantType: "CREATE_TABLE",
            wantErr:  false,
        },
        {
            name:     "insert",
            input:    "INSERT INTO users VALUES (1)",
            wantType: "INSERT",
            wantErr:  false,
        },
        {
            name:     "invalid sql",
            input:    "INVALID SYNTAX",
            wantType: "",
            wantErr:  true,
        },
    }

    p := parser.New()
    for _, tt := range tests {
        t.Run(tt.name, func(t *testing.T) {
            stmt, err := p.Parse(tt.input)
            if (err != nil) != tt.wantErr {
                t.Errorf("error = %v, wantErr %v", err, tt.wantErr)
            }
            if err == nil && stmt.Type != tt.wantType {
                t.Errorf("type = %v, want %v", stmt.Type, tt.wantType)
            }
        })
    }
}
```

## Common Unit Test Patterns

### Testing Error Cases

```go
func TestParserInvalidSQL(t *testing.T) {
    p := parser.New()
    invalidSQL := []string{
        "GARBAGE",
        "SELECT",  // incomplete
        "INSERT INTO",  // incomplete
    }

    for _, sql := range invalidSQL {
        _, err := p.Parse(sql)
        if err == nil {
            t.Errorf("Parse(%q) should error", sql)
        }
    }
}
```

### Testing State Changes

```go
func TestIndexAdd(t *testing.T) {
    idx := index.New("email")

    // Initially empty
    if idx.Exists("test@example.com") {
        t.Error("should not exist initially")
    }

    // Add entry
    idx.Add("test@example.com", 1)

    // Now exists
    if !idx.Exists("test@example.com") {
        t.Error("should exist after Add")
    }
}
```

## Coverage Goals

- Aim for >80% code coverage in unit tests
- Focus on:
  - Happy path (normal operation)
  - Edge cases (empty input, nil, boundaries)
  - Error paths (invalid input, failures)
  - Integration with other components

## Debugging Failed Tests

```bash
# Run with verbose output
go test ./tests/unit -v

# Run single test with output
go test ./tests/unit -run TestName -v

# Print debug info in test
t.Logf("Debug: %v", value)  // Only prints on failure

# Run with race detector
go test ./tests/unit -race
```

## Best Practices

1. **One assertion per test** - Keep tests focused
2. **Clear names** - `TestParserCreateTable` not `TestParser1`
3. **Setup and cleanup** - Use defer for cleanup
4. **No external dependencies** - Tests should be fast
5. **Deterministic** - Same input always gives same output
6. **Independent** - Tests don't depend on order
