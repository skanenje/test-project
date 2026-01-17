package schema

import "fmt"

// Migration describes how to migrate from one schema version to another
type Migration struct {
	FromVersion int
	ToVersion   int
	// Operations applied in order
	Operations []MigrationOp
}

// MigrationOp represents a single schema change operation
type MigrationOp interface{}

// AddColumnOp adds a new column
type AddColumnOp struct {
	Column  Column
	Default interface{} // Value for existing rows
}

// RemoveColumnOp removes a column
type RemoveColumnOp struct {
	ColumnName string
}

// ModifyColumnOp changes column properties
type ModifyColumnOp struct {
	ColumnName string
	OldDef     Column
	NewDef     Column
}

// RenameColumnOp renames a column
type RenameColumnOp struct {
	OldName string
	NewName string
}

// applyMigration applies a single migration to a row
func applyMigration(row map[string]interface{}, migration *Migration) (map[string]interface{}, error) {
	result := make(map[string]interface{})
	for k, v := range row {
		result[k] = v
	}

	for _, op := range migration.Operations {
		switch o := op.(type) {
		case *AddColumnOp:
			// Add new column with default value if not present
			if _, exists := result[o.Column.Name]; !exists {
				result[o.Column.Name] = o.Default
			}

		case *RemoveColumnOp:
			// Remove column
			delete(result, o.ColumnName)

		case *ModifyColumnOp:
			// For now, just keep the value as-is
			// In a real system, might need type conversion
			if val, exists := result[o.ColumnName]; exists {
				result[o.ColumnName] = val
			}

		case *RenameColumnOp:
			// Rename column
			if val, exists := result[o.OldName]; exists {
				result[o.NewName] = val
				delete(result, o.OldName)
			}

		default:
			return nil, fmt.Errorf("unknown migration operation type: %T", op)
		}
	}

	return result, nil
}
