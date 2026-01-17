// Package index provides hash-based indexing for fast column value lookups.
//
// The index package implements in-memory hash indexes that map column values to
// lists of row IDs. This enables O(1) lookups for WHERE clauses on indexed columns,
// significantly improving query performance compared to full table scans.
//
// Key Features:
//   - Hash-Based: Uses hash maps for O(1) value lookups
//   - Multi-Value Support: Maps values to lists of row IDs (handles duplicates)
//   - Rebuildable: Can rebuild indexes from scratch from row data
//   - In-Memory: Fast access but requires rebuilding on restart
//
// Key Responsibilities:
//   - Maintaining hash maps of column values to row IDs
//   - Adding and removing entries as rows are inserted/updated/deleted
//   - Looking up row IDs by column value
//   - Rebuilding indexes from current table state
//
// Usage Example:
//
//	// Create an index on the "name" column
//	idx := index.New("name")
//
//	// Add entries
//	idx.Add("Alice", 1)
//	idx.Add("Bob", 2)
//	idx.Add("Alice", 3) // Multiple rows can have same value
//
//	// Lookup
//	rowIDs, found := idx.Lookup("Alice") // Returns [1, 3]
//
//	// Remove entry
//	idx.Remove("Alice", 1)
//
//	// Rebuild from scratch
//	rows := []storage.RowWithID{
//		{ID: 1, Row: storage.Row{"name": "Alice"}},
//		{ID: 2, Row: storage.Row{"name": "Bob"}},
//	}
//	idx.Rebuild(rows)
//
// The index package is used by the database package to maintain indexes on
// frequently queried columns. Indexes are automatically maintained as data
// changes, but must be rebuilt from storage on database restart.
package index
