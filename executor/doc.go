// Package executor provides SQL statement execution functionality.
//
// The executor package bridges the gap between parsed SQL statements and database
// operations. It takes ParsedStatement objects from the parser and executes them
// against the database, returning formatted results or error messages.
//
// Key Responsibilities:
//   - Executing CREATE TABLE statements
//   - Executing INSERT, SELECT, UPDATE, DELETE operations
//   - Executing JOIN operations
//   - Formatting query results for display
//   - Managing event replay for recovery and migration scenarios
//   - Validating event integrity
//
// Supported Operations:
//   - CREATE_TABLE: Creates new tables with specified schemas
//   - INSERT: Inserts new rows into tables
//   - SELECT: Queries rows with optional WHERE clauses
//   - UPDATE: Updates rows matching WHERE conditions
//   - DELETE: Deletes rows matching WHERE conditions
//   - JOIN: Performs INNER JOIN operations between tables
//
// Usage Example:
//
//	exec := executor.New(db)
//
//	stmt, err := parser.Parse("INSERT INTO users VALUES (1, 'Alice')")
//	if err != nil {
//		log.Fatal(err)
//	}
//
//	result, err := exec.Execute(stmt)
//	if err != nil {
//		log.Fatal(err)
//	}
//	fmt.Println(result) // "Inserted row with ID 123"
//
// The executor package works closely with the parser package (for SQL parsing)
// and the database package (for actual data operations). It provides a clean
// abstraction layer that separates SQL parsing from execution logic.
package executor
