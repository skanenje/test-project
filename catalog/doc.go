// Package catalog provides database catalog management functionality.
//
// The catalog is responsible for maintaining metadata about all tables in the database,
// including their schemas, column definitions, and primary keys. It persists this
// information to disk in JSON format and provides a simple interface for creating,
// retrieving, and querying table schemas.
//
// Key Responsibilities:
//   - Managing table schema definitions
//   - Persisting catalog metadata to disk (_catalog.json)
//   - Loading catalog metadata on database initialization
//   - Validating table creation (e.g., preventing duplicate tables)
//   - Identifying primary keys from column definitions
//
// Usage Example:
//
//	catalog, err := catalog.New("./data")
//	if err != nil {
//		log.Fatal(err)
//	}
//
//	columns := []schema.Column{
//		{Name: "id", Type: schema.TypeInt, PrimaryKey: true},
//		{Name: "name", Type: schema.TypeText},
//	}
//	err = catalog.CreateTable("users", columns)
//
//	table, err := catalog.GetTable("users")
//
// Package catalog works closely with the schema package to define column types
// and table structures. It is used by the database package to manage schema
// metadata throughout the database lifecycle.
package catalog
