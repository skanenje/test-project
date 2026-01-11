package database

import (
	"fmt"
	"rdbms/catalog"
	"rdbms/index"
	"rdbms/parser"
	"rdbms/schema"
	"rdbms/storage"
)

// Database is the main database interface
type Database struct {
	storage *storage.Engine
	catalog *catalog.Catalog
	indexes map[string]map[string]*index.Index // table -> column -> index
}

// New creates a new database instance
func New(dataDir string) (*Database, error) {
	storageEngine, err := storage.NewEngine(dataDir)
	if err != nil {
		return nil, err
	}

	cat, err := catalog.New(dataDir)
	if err != nil {
		return nil, err
	}

	db := &Database{
		storage: storageEngine,
		catalog: cat,
		indexes: make(map[string]map[string]*index.Index),
	}

	// Rebuild indexes for existing tables
	if err := db.rebuildAllIndexes(); err != nil {
		return nil, err
	}

	return db, nil
}

// rebuildAllIndexes rebuilds indexes for all tables
func (db *Database) rebuildAllIndexes() error {
	tables := db.catalog.GetAllTables()
	for tableName, table := range tables {
		if err := db.rebuildIndexes(tableName, table); err != nil {
			return err
		}
	}
	return nil
}

// rebuildIndexes rebuilds indexes for a specific table
func (db *Database) rebuildIndexes(tableName string, table *schema.Table) error {
	db.indexes[tableName] = make(map[string]*index.Index)

	// Create index structures for PK and unique columns
	for _, col := range table.Columns {
		if col.PrimaryKey || col.Unique {
			db.indexes[tableName][col.Name] = index.New(col.Name)
		}
	}

	// Populate indexes from existing data
	rows, err := db.storage.ScanAll(tableName)
	if err != nil {
		return err
	}

	for _, r := range rows {
		for colName, idx := range db.indexes[tableName] {
			if val, exists := r.Row[colName]; exists {
				idx.Add(val, r.ID)
			}
		}
	}

	return nil
}

// CreateTable creates a new table
func (db *Database) CreateTable(tableName string, columns []schema.Column) error {
	if err := db.catalog.CreateTable(tableName, columns); err != nil {
		return err
	}

	// Create indexes for PK and unique columns
	db.indexes[tableName] = make(map[string]*index.Index)
	for _, col := range columns {
		if col.PrimaryKey || col.Unique {
			db.indexes[tableName][col.Name] = index.New(col.Name)
		}
	}

	return nil
}

// GetTable retrieves a table schema
func (db *Database) GetTable(tableName string) (*schema.Table, error) {
	return db.catalog.GetTable(tableName)
}

// Insert inserts a row into a table
func (db *Database) Insert(tableName string, row storage.Row) (int64, error) {
	table, err := db.catalog.GetTable(tableName)
	if err != nil {
		return 0, err
	}

	// Validate columns
	if err := db.validateRow(table, row); err != nil {
		return 0, err
	}

	// Check primary key uniqueness
	if table.PrimaryKey != "" {
		if idx, exists := db.indexes[tableName][table.PrimaryKey]; exists {
			pkValue := row[table.PrimaryKey]
			if idx.Exists(pkValue) {
				return 0, fmt.Errorf("primary key violation: duplicate value '%v'", pkValue)
			}
		}
	}

	// Check unique constraints
	for _, col := range table.Columns {
		if col.Unique && !col.PrimaryKey {
			if idx, exists := db.indexes[tableName][col.Name]; exists {
				value := row[col.Name]
				if idx.Exists(value) {
					return 0, fmt.Errorf("unique constraint violation on column '%s'", col.Name)
				}
			}
		}
	}

	// Insert row
	rowID, err := db.storage.InsertRow(tableName, row)
	if err != nil {
		return 0, err
	}

	// Update indexes
	for colName, idx := range db.indexes[tableName] {
		if val, exists := row[colName]; exists {
			idx.Add(val, rowID)
		}
	}

	return rowID, nil
}

// validateRow validates a row against schema
func (db *Database) validateRow(table *schema.Table, row storage.Row) error {
	// Check all schema columns are present
	for _, col := range table.Columns {
		val, exists := row[col.Name]
		if !exists {
			return fmt.Errorf("missing column: %s", col.Name)
		}

		// Basic type checking
		switch col.Type {
		case schema.TypeInt:
			if _, ok := val.(float64); !ok { // JSON numbers become float64
				return fmt.Errorf("column '%s' expects INT", col.Name)
			}
		case schema.TypeText:
			if _, ok := val.(string); !ok {
				return fmt.Errorf("column '%s' expects TEXT", col.Name)
			}
		case schema.TypeBool:
			if _, ok := val.(bool); !ok {
				return fmt.Errorf("column '%s' expects BOOL", col.Name)
			}
		}
	}

	// Check no extra columns
	if len(row) != len(table.Columns) {
		return fmt.Errorf("row has extra columns")
	}

	return nil
}

// Select selects rows from a table with optional WHERE clause (uses index if available)
func (db *Database) Select(tableName string, where *parser.WhereClause) ([]storage.Row, error) {
	if !db.catalog.TableExists(tableName) {
		return nil, fmt.Errorf("table '%s' does not exist", tableName)
	}

	var rowsWithID []storage.RowWithID

	// Try to use index for WHERE clause
	if where != nil {
		if idx, hasIndex := db.indexes[tableName][where.Column]; hasIndex {
			// Index hit! Fetch only matching rows
			if rowIDs, found := idx.Lookup(where.Value); found {
				allRows, err := db.storage.ScanAll(tableName)
				if err != nil {
					return nil, err
				}

				rowIDMap := make(map[int64]bool)
				for _, id := range rowIDs {
					rowIDMap[id] = true
				}

				for _, r := range allRows {
					if rowIDMap[r.ID] {
						rowsWithID = append(rowsWithID, r)
					}
				}
			}
		} else {
			// No index, fall back to full scan
			allRows, err := db.storage.ScanAll(tableName)
			if err != nil {
				return nil, err
			}

			for _, r := range allRows {
				if val, exists := r.Row[where.Column]; exists {
					if valuesEqual(val, where.Value) {
						rowsWithID = append(rowsWithID, r)
					}
				}
			}
		}
	} else {
		// No WHERE clause, full scan
		var err error
		rowsWithID, err = db.storage.ScanAll(tableName)
		if err != nil {
			return nil, err
		}
	}

	var rows []storage.Row
	for _, r := range rowsWithID {
		rows = append(rows, r.Row)
	}

	return rows, nil
}

// Delete deletes rows matching WHERE clause
func (db *Database) Delete(tableName string, where *parser.WhereClause) (int, error) {
	if !db.catalog.TableExists(tableName) {
		return 0, fmt.Errorf("table '%s' does not exist", tableName)
	}

	if where == nil {
		return 0, fmt.Errorf("DELETE requires WHERE clause")
	}

	rows, err := db.storage.ScanAll(tableName)
	if err != nil {
		return 0, err
	}

	count := 0
	for _, r := range rows {
		if val, exists := r.Row[where.Column]; exists && valuesEqual(val, where.Value) {
			// Remove from indexes
			for colName, idx := range db.indexes[tableName] {
				if colVal, exists := r.Row[colName]; exists {
					idx.Remove(colVal, r.ID)
				}
			}

			db.storage.DeleteRow(tableName, r.ID)
			count++
		}
	}

	return count, nil
}

// Update updates rows matching WHERE clause
func (db *Database) Update(tableName string, setColumn string, setValue interface{}, where *parser.WhereClause) (int, error) {
	table, err := db.catalog.GetTable(tableName)
	if err != nil {
		return 0, err
	}

	if where == nil {
		return 0, fmt.Errorf("UPDATE requires WHERE clause")
	}

	rows, err := db.storage.ScanAll(tableName)
	if err != nil {
		return 0, err
	}

	count := 0
	for _, r := range rows {
		if val, exists := r.Row[where.Column]; exists && valuesEqual(val, where.Value) {
			// Remove old from indexes
			for colName, idx := range db.indexes[tableName] {
				if colVal, exists := r.Row[colName]; exists {
					idx.Remove(colVal, r.ID)
				}
			}

			newRow := make(storage.Row)
			for k, v := range r.Row {
				newRow[k] = v
			}
			newRow[setColumn] = setValue

			if err := db.validateRow(table, newRow); err != nil {
				return count, err
			}

			newRowID, _ := db.storage.UpdateRow(tableName, r.ID, newRow)

			// Add new to indexes
			for colName, idx := range db.indexes[tableName] {
				if colVal, exists := newRow[colName]; exists {
					idx.Add(colVal, newRowID)
				}
			}

			count++
		}
	}

	return count, nil
}

// Helper to compare values
func valuesEqual(a, b interface{}) bool {
	return fmt.Sprintf("%v", a) == fmt.Sprintf("%v", b)
}

// Join performs INNER JOIN using nested-loop algorithm
func (db *Database) Join(leftTable, rightTable string, condition *parser.JoinCondition, where *parser.WhereClause) ([]storage.Row, error) {
	if !db.catalog.TableExists(leftTable) {
		return nil, fmt.Errorf("table '%s' does not exist", leftTable)
	}
	if !db.catalog.TableExists(rightTable) {
		return nil, fmt.Errorf("table '%s' does not exist", rightTable)
	}

	leftRows, err := db.storage.ScanAll(leftTable)
	if err != nil {
		return nil, err
	}

	rightRows, err := db.storage.ScanAll(rightTable)
	if err != nil {
		return nil, err
	}

	var result []storage.Row

	// Nested-loop join
	for _, leftRow := range leftRows {
		for _, rightRow := range rightRows {
			// Check join condition
			leftVal := leftRow.Row[condition.LeftColumn]
			rightVal := rightRow.Row[condition.RightColumn]

			if !valuesEqual(leftVal, rightVal) {
				continue
			}

			// Merge rows with table prefix
			joinedRow := make(storage.Row)
			for k, v := range leftRow.Row {
				joinedRow[leftTable+"."+k] = v
			}
			for k, v := range rightRow.Row {
				joinedRow[rightTable+"."+k] = v
			}

			// Apply WHERE filter if present
			if where != nil {
				if val, exists := joinedRow[where.Column]; exists {
					if !valuesEqual(val, where.Value) {
						continue
					}
				} else {
					continue
				}
			}

			result = append(result, joinedRow)
		}
	}

	return result, nil
}
