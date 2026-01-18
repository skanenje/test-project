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

Each of the following directories contains a `README.md` file that describes its purpose in detail.

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