# Event-Sourced RDBMS

> *A relational database management system built on immutable event streams. Every change is a story; every story is the truth.*

A lightweight, event-sourced relational database management system (RDBMS) written in Go, demonstrating how modern applications leverage immutable event logs as the foundation for auditability, temporal querying, and system resilience.

---

## The Philosophy: Why Event Sourcing Matters

Traditional databases store only the current state—when you update a customer record, the old data is lost. **Event sourcing inverts this model.** Instead of asking "what is the current state?", we ask "what sequence of events led to this state?"

Every mutation—every insert, update, delete—is captured as an immutable event in an append-only log. These events become the **single source of truth**. The current state of any entity is reconstructed by replaying all relevant events from the beginning.

This shift in perspective unlocks capabilities that traditional CRUD systems struggle to provide:

- **Complete Auditability**: Every change is permanently recorded with full context
- **Temporal Queries**: Reconstruct what your data looked like at any point in the past
- **System Recovery**: Replay events to recover from failures without data loss
- **Complex Workflows**: Natural support for systems where history matters—finance, healthcare, e-commerce

This is production architecture used by leaders like Amazon, Shopify, and enterprise financial systems. This implementation demonstrates those principles in an accessible, educational codebase.

---

## Core Features

### 🎯 Event-Sourced Architecture
All database operations are recorded as immutable events in an append-only event store. No updates, no deletes at the storage layer—only appends. The current database state is reconstructed by replaying events, creating a complete, auditable history of every change.

### 🔄 Snapshots for Performance
While replaying events provides auditability, it can be slow on large datasets. **Snapshots** periodically capture the database state, allowing new queries to load a recent snapshot and apply only new events. This combines the completeness of event sourcing with the performance requirements of production systems.

### 📊 Full CRUD + Joins
Standard relational operations: `CREATE TABLE`, `INSERT`, `SELECT`, `UPDATE`, `DELETE`, and `INNER JOIN`. Built on top of the event-sourced foundation.

### ⚡ Intelligent Indexing
Hash-based indexes on configured columns provide O(1) lookups instead of O(n) table scans. Indexes are automatically maintained and rebuilt from snapshots during recovery.

### 🏗️ Schema Evolution
Tables and schemas can evolve over time. Schema changes are recorded as events, enabling backward-compatible migrations and temporal queries across schema versions.

### 🌐 REST API
Simple web server with HTTP endpoints for database operations. Perfect for learning or building microservices.

---

## Architecture Overview

```
┌─────────────────────────────────────────────────────────────┐
│  SQL REPL / REST API                                        │
├─────────────────────────────────────────────────────────────┤
│  Parser → Executor → Database Operations                    │
├─────────────────────────────────────────────────────────────┤
│  Catalog │ Indexes │ Query Engine                           │
├─────────────────────────────────────────────────────────────┤
│  Event Store (Immutable Event Log)  ←→  Snapshot Manager   │
├─────────────────────────────────────────────────────────────┤
│  Physical Storage Layer                                      │
└─────────────────────────────────────────────────────────────┘
```

**Key insight**: The event store is the single source of truth. Everything else—current state, snapshots, indexes—is derived from events. This inversion eliminates data inconsistency and enables powerful debugging and recovery capabilities.

---

## Project Structure

Each directory is a focused module with its own comprehensive documentation:

| Module | Purpose |
|--------|---------|
| **catalog/** | Table schema registry—the single source of truth for table structure |
| **database/** | Core orchestrator coordinating storage, indexing, and execution |
| **parser/** | SQL statement parser converting strings to executable ASTs |
| **executor/** | Transforms parsed statements into database operations |
| **storage/** | Physical persistence layer: event store, snapshots, query engine |
| **schema/** | Data type and table definition structures |
| **eventlog/** | Immutable append-only event log with integrity verification |
| **index/** | Hash-based indexing for fast column lookups |
| **cmd/web/** | REST API server demonstrating HTTP integration |
| **tests/** | Integration, e2e, and unit tests |

See each module's `README.md` for detailed documentation, API examples, and architecture decisions.

---

## Getting Started

### Prerequisites
- Go 1.21+

### Running the REPL

```bash
go run main.go
```

Then execute SQL:
```sql
sql> CREATE TABLE users (id INT PRIMARY KEY, name TEXT, email TEXT)
sql> INSERT INTO users (id, name, email) VALUES (1, 'Alice', 'alice@example.com')
sql> SELECT * FROM users
sql> UPDATE users SET email = 'alice.new@example.com' WHERE id = 1
sql> DELETE FROM users WHERE id = 1
```

### Starting the Web Server

```bash
go run main.go --web
```

Then make HTTP requests:
```bash
curl -X GET http://localhost:8080/tasks
curl -X POST http://localhost:8080/tasks \
  -H "Content-Type: application/json" \
  -d '{"title": "Learn event sourcing", "completed": false}'
```

---

## Understanding Event Sourcing in This Codebase

When you insert a row, here's what actually happens:

1. **Parser** converts SQL into a `ParsedStatement`
2. **Executor** validates the statement and calls `database.Insert()`
3. **Database** creates a `RowInserted` event with the row data
4. **EventStore** appends the event to the append-only log
5. **Storage Engine** persists the event to disk
6. **Indexes** are updated to reflect the new row
7. A new snapshot may be created if threshold is reached

When you query data:

1. **QueryEngine** loads the most recent snapshot
2. Replays all events since the snapshot
3. Returns the reconstructed current state

**The beautiful part**: You can query the database as it was at any point in the past by replaying events up to that timestamp. This is impossible in traditional databases without keeping full history tables.

---

## Design Patterns

This codebase demonstrates several architectural patterns:

### Event Sourcing + CQRS
- **Commands** (INSERT, UPDATE, DELETE) write events
- **Queries** (SELECT) read from rebuilt state
- Events are the source of truth

### Snapshot Pattern
- Periodic snapshots prevent unbounded event replay
- Trade: slight staleness for major performance gain
- Production systems make this configurable

### Immutability at the Core
- Events are immutable, eliminating race conditions
- State reconstruction is deterministic
- Perfect for distributed systems

---

## Learning Path

1. **Start with [database/README.md](database/README.md)** - Understand the orchestrator
2. **Explore [eventlog/README.md](eventlog/README.md)** - See how events work
3. **Read [storage/README.md](storage/README.md)** - Understand snapshots and replay
4. **Check [executor/README.md](executor/README.md)** - See how SQL becomes operations
5. **Review tests/** - See patterns in action

---

## Testing

```bash
# Run all integration tests
go test ./tests/integration -v

# Run with coverage
go test ./tests/integration -v -cover

# Run specific test
go test ./tests/integration -run TestEventReplay -v
```

Integration tests demonstrate:
- Event recording and replay
- Snapshot creation and recovery
- Schema evolution
- Index maintenance
- Full CRUD workflows

---

## Extending the System

### Add a New SQL Operation

1. **Parser** - Add parsing logic to `parser/parser.go`
2. **Executor** - Implement handler in `executor/executor.go`
3. **Database** - Add operation to `database/database.go`
4. **Events** - Create event type in `eventlog/events.go`
5. **Tests** - Add integration test to `tests/integration/`

### Improve Performance

- Optimize snapshot frequency in `database.snapshotInterval`
- Add more indexes via `database.CreateIndex()`
- Implement query optimization in `executor/`

### Add Persistence Features

- Compress snapshots for large datasets
- Implement checkpoint recovery in `storage/`
- Add event log retention policies

---

## Limitations & Future Work

### By Design
- Simplified query language (no complex WHERE conditions, aggregations)
- In-memory indexes (no persistence)
- No distributed consensus or replication

### Future Improvements
- B-tree indexes for range queries
- Event stream compression
- Multi-table transactions
- MVCC for concurrent queries
- Distributed event store

---

## License

MIT

---

## Further Reading

**Want to understand event sourcing deeper?**
- Greg Young's "Event Sourcing" presentation
- Martin Fowler's architecture articles
- AWS EventBridge documentation
- Stripe's and Shopify's engineering blogs

**Patterns this implements:**
- Event Sourcing Pattern
- CQRS (Command Query Responsibility Segregation)
- Snapshot Pattern
- Audit Trail Pattern

This project is an educational implementation demonstrating how foundational database concepts—parsing, indexing, persistence, recovery—come together in a modern event-sourced system.