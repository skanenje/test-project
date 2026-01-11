# Simple RDBMS

A lightweight relational database management system (RDBMS) written in Go, featuring an interactive SQL REPL with support for basic SQL operations.

## Features

- **SQL Support**: CREATE TABLE, INSERT, SELECT, UPDATE, DELETE
- **WHERE Clause**: Filter data with column-value conditions
- **Interactive REPL**: Command-line interface for executing SQL queries
- **Type Safety**: Schema management with column type definitions
- **In-Memory Storage**: Fast data storage and retrieval
- **Query Parsing**: Full SQL statement parser

## Project Structure

```
├── catalog/          # Database catalog management
├── database/         # Core database engine
├── executor/         # SQL statement execution
├── index/            # Indexing support
├── parser/           # SQL parser
├── schema/           # Schema definitions
├── storage/          # Data storage engine
├── demo_data/        # Example data files
├── main.go           # Entry point and REPL
└── go.mod            # Module definition
```

## Getting Started

### Prerequisites

- Go 1.21 or later

### Installation

```bash
git clone <repository-url>
cd test-project
go mod download
```

### Running the REPL

```bash
go run main.go
```

You'll see the REPL prompt:
```
=== Simple RDBMS REPL ===
Type SQL commands or 'exit' to quit

sql>
```

### Example Usage

```sql
-- Create a table
sql> CREATE TABLE users (id INT, name VARCHAR(100), age INT)

-- Insert data
sql> INSERT INTO users VALUES (1, 'Alice', 30)
sql> INSERT INTO users VALUES (2, 'Bob', 25)

-- Select all records
sql> SELECT * FROM users

-- Select with WHERE clause
sql> SELECT * FROM users WHERE age > 25

-- Update records
sql> UPDATE users SET age = 31 WHERE name = 'Alice'

-- Delete records
sql> DELETE FROM users WHERE id = 2

-- Exit REPL
sql> exit
```

## Core Components

### Parser (`parser/`)
Handles SQL statement parsing and validation. Converts raw SQL strings into executable query objects.

### Database (`database/`)
Manages database initialization, table creation, and transaction handling.

### Executor (`executor/`)
Executes parsed SQL statements against the database. Handles INSERT, SELECT, UPDATE, DELETE operations.

### Schema (`schema/`)
Defines table structures, column types, and metadata.

### Storage (`storage/`)
Low-level data storage and retrieval engine.

### Catalog (`catalog/`)
Maintains metadata about all tables and their schemas.

### Index (`index/`)
Provides indexing capabilities for faster data lookup.

## Development

To extend this RDBMS:

1. **Add SQL Keywords**: Modify `parser/parser.go`
2. **Implement New Operations**: Add executors in `executor/executor.go`
3. **Optimize Storage**: Enhance `storage/engine.go`
4. **Improve Indexing**: Update `index/index.go`

## License

MIT

## Notes

This is a simplified RDBMS implementation designed for educational purposes. It demonstrates core database concepts including parsing, schema management, and query execution.
