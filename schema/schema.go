package schema

// ColumnType represents supported data types
type ColumnType string

const (
	TypeInt  ColumnType = "INT"
	TypeText ColumnType = "TEXT"
	TypeBool ColumnType = "BOOL"
)

// Column defines a table column
type Column struct {
	Name string     `json:"name"`
	Type ColumnType `json:"type"`
}

// Table holds table metadata
type Table struct {
	Name    string   `json:"name"`
	Columns []Column `json:"columns"`
}
