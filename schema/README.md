# Schema Package

## Purpose

The `schema` package defines the data structures for database schemas. It provides the fundamental building blocks for table definitions and columns.

## Key Concepts

### Data Types

Supported types:
- **INT** - Integer values
- **TEXT** - String values
- **BOOL** - Boolean values

### Columns

Each column has:
- **Name** - Column identifier
- **Type** - Data type (INT, TEXT, BOOL)
- **PrimaryKey** - Whether this is the primary key
- **Unique** - Whether values must be unique

### Tables

Each table has:
- **Name** - Unique table identifier
- **Columns** - Ordered list of definitions
- **PrimaryKey** - Name of primary key column

## Key Types

```go
type ColumnType string

const (
    TypeInt  ColumnType = "INT"
    TypeText ColumnType = "TEXT"
    TypeBool ColumnType = "BOOL"
)

type Column struct {
    Name       string     `json:"name"`
    Type       ColumnType `json:"type"`
    PrimaryKey bool       `json:"primary_key"`
    Unique     bool       `json:"unique"`
}

type Table struct {
    Name       string   `json:"name"`
    Columns    []Column `json:"columns"`
    PrimaryKey string   `json:"primary_key"`
}
```

## Usage Example

```go
cols := []schema.Column{
    {Name: "id", Type: schema.TypeInt, PrimaryKey: true},
    {Name: "email", Type: schema.TypeText, Unique: true},
    {Name: "active", Type: schema.TypeBool},
}

table := &schema.Table{
    Name:       "users",
    Columns:    cols,
    PrimaryKey: "id",
}

jsonData, err := json.Marshal(table)
```

## Design Notes

### JSON Serialization

All types include JSON tags for:
- Persisting to catalog file
- REST API transmission
- Event payloads

### No Validation Logic

Validation happens elsewhere:
- Type checking at insert/update time
- Column existence in executor
- Separation of concerns keeps schema lightweight

## Integration Points

- **Catalog Package**: Stores table schemas
- **Parser Package**: References column definitions
- **Database Package**: Uses column types
- **Storage Package**: Stores schemas in events
- **Event Log Package**: Schema in event payloads
