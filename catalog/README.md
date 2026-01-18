# Catalog Package

## Purpose

The `catalog` package manages the database's table schemas and metadata. It acts as the single source of truth for all table definitions, column information, and primary key constraints. The catalog persists schema definitions to disk as JSON and rebuilds them on database startup.

## Key Concepts

### Catalog Structure

The main `Catalog` type maintains an in-memory map of table schemas and provides methods to:
- Create new tables with column definitions
- Look up existing table schemas  
- Validate table existence
- Persist schemas to disk

### Persistence

All table definitions are serialized to `_catalog.json` in the data directory, ensuring schema durability across database restarts.

## Key Types

```go
type Catalog struct {
    schemas map[string]*schema.Table  // Table name -> Table definition
    dataDir string                    // Directory for persistence
}
```

## Main Functions

- `New(dataDir string) (*Catalog, error)` - Creates a new catalog, loading existing schemas
- `(c *Catalog) CreateTable(name string, cols []schema.Column) error` - Register a new table
- `(c *Catalog) GetTable(name string) (*schema.Table, error)` - Retrieve table metadata
- `(c *Catalog) TableExists(name string) bool` - Check if a table exists
- `(c *Catalog) Save() error` - Persist catalog to disk

## Integration Points

- **Database Package**: Validates table existence and retrieves column definitions
- **Parser Package**: Uses catalog to validate column names  
- **Storage Package**: Stores catalog metadata
