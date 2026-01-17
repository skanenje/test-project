// Package database provides the core database engine and high-level database operations.
//
// The database package orchestrates all database components including the event store,
// query engine, snapshot manager, catalog, and indexes. It provides a unified interface
// for CRUD operations (Create, Read, Update, Delete), joins, and indexing.
//
// Architecture:
//   - Event-Sourced: All changes are recorded as events in an append-only log
//   - Snapshot-Based: Periodic snapshots speed up query performance
//   - Indexed: Hash-based indexes on columns for fast lookups
//   - Thread-Safe: Uses mutexes to ensure concurrent access safety
//
// Key Responsibilities:
//   - Initializing and managing database components
//   - Providing high-level CRUD operations (Insert, Select, Update, Delete)
//   - Managing table creation and schema operations
//   - Supporting JOIN operations between tables
//   - Maintaining indexes for performance optimization
//   - Coordinating between storage, catalog, and index components
//
// Usage Example:
//
//	db, err := database.New("./data")
//	if err != nil {
//		log.Fatal(err)
//	}
//	defer db.Close()
//
//	// Create a table
//	columns := []schema.Column{
//		{Name: "id", Type: schema.TypeInt, PrimaryKey: true},
//		{Name: "name", Type: schema.TypeText},
//	}
//	err = db.CreateTable("users", columns)
//
//	// Insert data
//	row := storage.Row{"id": 1, "name": "Alice"}
//	rowID, err := db.Insert("users", row)
//
//	// Query data
//	where := &parser.WhereClause{Column: "name", Value: "Alice"}
//	rows, err := db.Select("users", where)
//
// The database package is the main entry point for database operations and coordinates
// with the storage, catalog, index, parser, and schema packages to provide a complete
// database management system.
package database
