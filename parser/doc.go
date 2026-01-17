// Package parser provides SQL parsing functionality.
//
// The parser package converts raw SQL strings into structured ParsedStatement objects
// that can be executed by the executor. It supports a subset of SQL including CREATE TABLE,
// INSERT, SELECT, UPDATE, DELETE, and JOIN operations.
//
// Supported SQL Operations:
//   - CREATE TABLE: Define table schemas with columns and types
//   - INSERT INTO: Insert rows with explicit values
//   - SELECT: Query rows with optional WHERE clauses
//   - UPDATE: Update rows with SET and WHERE clauses
//   - DELETE FROM: Delete rows with WHERE clauses
//   - JOIN: INNER JOIN with ON conditions
//
// Key Responsibilities:
//   - Tokenizing and parsing SQL strings
//   - Validating SQL syntax
//   - Converting SQL to structured ParsedStatement objects
//   - Extracting table names, columns, values, and conditions
//   - Handling WHERE clauses for filtering
//   - Parsing JOIN conditions
//
// ParsedStatement Structure:
//   - Type: The operation type (CREATE_TABLE, INSERT, SELECT, etc.)
//   - TableName: The target table
//   - Columns: Column definitions (for CREATE TABLE)
//   - Values: Row data (for INSERT)
//   - Where: WHERE clause conditions (for SELECT, UPDATE, DELETE)
//   - SetColumn/SetValue: Column updates (for UPDATE)
//   - JoinTable/JoinCondition: JOIN information (for JOIN)
//
// Usage Example:
//
//	p := parser.New()
//
//	stmt, err := p.Parse("CREATE TABLE users (id INT, name TEXT)")
//	if err != nil {
//		log.Fatal(err)
//	}
//	// stmt.Type == "CREATE_TABLE"
//	// stmt.TableName == "users"
//	// stmt.Columns contains column definitions
//
//	stmt, err = p.Parse("SELECT * FROM users WHERE name = 'Alice'")
//	// stmt.Type == "SELECT"
//	// stmt.Where.Column == "name"
//	// stmt.Where.Value == "Alice"
//
// The parser package is used by the executor package to convert user SQL input
// into executable statements. It provides a simple, extensible parsing interface
// that can be extended to support additional SQL features.
package parser
