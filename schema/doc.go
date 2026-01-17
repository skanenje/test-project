// Package schema provides type definitions and utilities for database schemas.
//
// The schema package defines the core data structures used throughout the database
// to represent tables, columns, and their metadata. It provides type-safe definitions
// for column types, primary keys, and table structures.
//
// Key Types:
//   - ColumnType: Supported data types (INT, TEXT, BOOL)
//   - Column: Column definition with name, type, and constraints
//   - Table: Table metadata including name, columns, and primary key
//
// Supported Column Types:
//   - TypeInt: Integer values
//   - TypeText: Text/string values
//   - TypeBool: Boolean values
//
// Column Constraints:
//   - PrimaryKey: Marks a column as the primary key
//   - Unique: Marks a column as requiring unique values
//
// Key Responsibilities:
//   - Defining column and table data structures
//   - Providing type constants for column types
//   - Structuring schema metadata for serialization
//   - Supporting schema evolution and migration
//
// Usage Example:
//
//	// Define columns
//	columns := []schema.Column{
//		{Name: "id", Type: schema.TypeInt, PrimaryKey: true},
//		{Name: "name", Type: schema.TypeText},
//		{Name: "active", Type: schema.TypeBool},
//	}
//
//	// Create table structure
//	table := &schema.Table{
//		Name:       "users",
//		Columns:    columns,
//		PrimaryKey: "id",
//	}
//
// The schema package is used by virtually all other packages in the database
// system. It provides the foundation for type definitions and is essential for
// catalog management, table creation, and schema evolution.
package schema
