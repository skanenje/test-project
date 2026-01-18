# Executor Package

## Purpose

The `executor` package translates parsed SQL statements into actual database operations. It bridges the parser (producing `ParsedStatement` objects) and the database (performing the work).

## Execution Pipeline

```
SQL String (user input)
    ↓
Parser.Parse()
    ↓
ParsedStatement
    ↓
Executor.Execute()
    ↓
Database operations
    ↓
Result (string/error)
```

## Key Types

```go
type Executor struct {
    db                *database.Database
    lastReplayResult  *storage.ReplayResult
    migrationHandler  *storage.MigrationHandler
    recoveryReport    *storage.CorruptionReport
}
```

## Main Functions

- `New(db *database.Database) *Executor` - Create executor
- `(e *Executor) Execute(stmt *parser.ParsedStatement) (string, error)` - Execute any statement
- `(e *Executor) executeCreateTable(stmt *ParsedStatement) (string, error)`
- `(e *Executor) executeInsert(stmt *ParsedStatement) (string, error)`
- `(e *Executor) executeSelect(stmt *ParsedStatement) (string, error)`
- `(e *Executor) executeUpdate(stmt *ParsedStatement) (string, error)`
- `(e *Executor) executeDelete(stmt *ParsedStatement) (string, error)`
- `(e *Executor) executeJoin(stmt *ParsedStatement) (string, error)`

## Execution Flow

### CREATE TABLE
1. Extract table name and columns
2. Validate column definitions
3. Call `db.CreateTable()`
4. Return success message

### INSERT
1. Get table schema
2. Map values to columns
3. Validate types
4. Call `db.Insert()`
5. Return inserted row ID

### SELECT
1. Check for WHERE clause
2. Call `db.Select()` or `db.Scan()`
3. Format results as text
4. Return output

### UPDATE
1. Parse column and new value
2. Get matching rows
3. Call `db.Update()`
4. Return count

### DELETE
1. Get matching rows
2. Call `db.Delete()`
3. Return count

### JOIN
1. Extract table names and condition
2. Call `db.Join()`
3. Format joined results
4. Return output

## Usage Example

```go
db, _ := database.New("/path/to/data")
executor := executor.New(db)

parser := parser.New()
stmt, _ := parser.Parse("INSERT INTO users (id, name) VALUES (1, 'Alice')")
result, err := executor.Execute(stmt)
if err != nil {
    fmt.Printf("Error: %v\n", err)
} else {
    fmt.Println(result)
}
```

## Result Formatting

```
Table 'users' created
Row inserted with ID: 1
id | name   | active
---|--------|--------
1  | Alice  | true
2  | Bob    | false
2 row(s) updated
1 row(s) deleted
```

## Error Handling

Common errors:
- Table does not exist
- Column does not exist
- Type mismatch
- Invalid WHERE clause

## Integration Points

- **Parser Package**: Receives `ParsedStatement` objects
- **Database Package**: Executes operations
- **Storage Package**: Accesses storage
- **Main Package**: REPL uses executor
- **Web Package**: REST API uses executor
