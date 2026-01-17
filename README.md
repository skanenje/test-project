# Simple RDBMS

A lightweight relational database management system (RDBMS) written in Go, featuring an event-sourced architecture, schema evolution, and a simple web interface.

## Key Features

- **Event-Sourced Architecture:** All database changes are stored as events in an append-only log, providing a complete audit trail.
- **Schema Evolution:** Supports creating tables and evolving schemas over time using migrations.
- **CRUD Operations:** Supports `INSERT`, `SELECT`, `UPDATE`, and `DELETE` operations.
- **Indexing:** Supports creating indexes on columns for faster lookups.
- **Joins:** Supports `INNER JOIN` operations.
- **Snapshots:** Can create snapshots of the database state to speed up query performance.
- **Web Interface:** A simple web interface to interact with the database.

## Project Structure

```
├── catalog/          # Database catalog management
├── cmd/web/          # Web server code
├── database/         # Core database engine (CRUD, joins, indexing)
├── demo_data/        # Example data files
├── eventlog/         # Event log management
├── executor/         # SQL statement execution
├── index/            # Indexing support
├── parser/           # SQL parser
├── rdbms/            # Main application entry point
├── schema/           # Schema definitions and evolution
├── storage/          # Physical storage (event store, snapshots, query engine)
├── main.go           # Entry point
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

### Running the Web Server

```bash
go run main.go web 8080
```

The web server will be available at `http://localhost:8080`.

### Running Tests

```bash
go test ./...
```

## Example Usage

The web interface provides a simple way to interact with the database. You can execute SQL queries and view the results in your browser.

1.  Start the web server as described above.
2.  Open your browser and navigate to `http://localhost:8080`.
3.  Use the form to execute SQL queries.

**Example Queries:**

```sql
-- Create a table
CREATE TABLE users (id INT, name VARCHAR(100), age INT)

-- Insert data
INSERT INTO users VALUES (1, 'Alice', 30)
INSERT INTO users VALUES (2, 'Bob', 25)

-- Select all records
SELECT * FROM users

-- Select with WHERE clause
SELECT * FROM users WHERE age > 25

-- Update records
UPDATE users SET age = 31 WHERE name = 'Alice'

-- Delete records
DELETE FROM users WHERE id = 2
```

## Core Components

### Parser (`parser/`)
Handles SQL statement parsing and validation. Converts raw SQL strings into executable query objects.

### Database (`database/`)
Manages database initialization, table creation, and transaction handling.

### Executor (`executor/`)
Executes parsed SQL statements against the database. Handles INSERT, SELECT, UPDATE, DELETE operations.

### Schema (`schema/`)
Defines table structures, column types, and metadata. Manages schema evolution and migrations.

### Storage (`storage/`)
Low-level data storage and retrieval engine. Includes the event store, snapshot manager, and query engine.

### Catalog (`catalog/`)
Maintains metadata about all tables and their schemas.

### Index (`index/`)
Provides indexing capabilities for faster data lookup.

### Event Log (`eventlog/`)
Manages the append-only log of database events.

## Development

To extend this RDBMS:

1.  **Add SQL Keywords**: Modify `parser/parser.go`
2.  **Implement New Operations**: Add executors in `executor/executor.go`
3.  **Optimize Storage**: Enhance `storage/engine.go`
4.  **Improve Indexing**: Update `index/index.go`

## License

MIT

## Notes

This is a simplified RDBMS implementation designed for educational purposes. It demonstrates core database concepts including parsing, schema management, query execution, and event sourcing.