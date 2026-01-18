# Index Package

## Purpose

The `index` package provides efficient row lookup by column value. Indexes use hash-based lookups for O(1) average-case lookup instead of O(n) table scans, critical for large tables.

## Key Concepts

### Hash-Based Index

Indexes use a hash map:
- **Key** - String representation of column value
- **Value** - Slice of row IDs with this value

Enables:
- Fast lookup: query value → get all matching row IDs
- Multiple matches: multiple rows can have same value
- Easy updates: add/remove row IDs as data changes

### Index Rebuilding

Indexes are rebuilt from current database state:
- On startup: Read all rows and add to indexes
- After snapshots: Rebuild from snapshot state
- During recovery: Consistent with replayed state

## Key Types

```go
type Index struct {
    Column string             // Column name
    Data   map[string][]int64 // value -> [row_ids]
}
```

## Main Functions

- `New(column string) *Index` - Create new index
- `(idx *Index) Add(value interface{}, rowID int64)` - Add row
- `(idx *Index) Remove(value interface{}, rowID int64)` - Remove row
- `(idx *Index) Lookup(value interface{}) ([]int64, bool)` - Find rows
- `(idx *Index) Exists(value interface{}) bool` - Check existence

## Usage Example

```go
emailIndex := index.New("email")

emailIndex.Add("alice@example.com", 1)
emailIndex.Add("bob@example.com", 2)

rowIDs, found := emailIndex.Lookup("alice@example.com")
if found {
    fmt.Println(rowIDs)  // [1]
}

if emailIndex.Exists("bob@example.com") {
    fmt.Println("Email exists")
}

emailIndex.Remove("alice@example.com", 1)
```

## Performance

| Operation | Complexity | Notes |
|-----------|-----------|-------|
| Lookup | O(1) avg | Hash table lookup |
| Add | O(1) avg | Append to slice |
| Remove | O(k) | k = duplicate count |
| Rebuild | O(n) | n = row count |

## Index Management

Database maintains two-level index map:
```go
indexes map[string]map[string]*index.Index {
    "users": {
        "email": emailIndex,
        "user_id": idIndex,
    },
}
```

## Integration Points

- **Database Package**: Maintains indexes, updates on writes
- **Storage Package**: Rebuilds from snapshot state
- **Query Optimization**: Could be used for faster lookups
