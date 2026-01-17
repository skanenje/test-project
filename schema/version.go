package schema

// SchemaVersion represents a specific version of a table's schema
type SchemaVersion struct {
	Version   int
	TableName string
	Columns   []Column
	CreatedAt string // ISO timestamp
	Migration *Migration
}
