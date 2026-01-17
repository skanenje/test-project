package schema

import (
	"fmt"
	"rdbms/eventlog"
)

// SchemaVersion represents a specific version of a table's schema
type SchemaVersion struct {
	Version   int
	TableName string
	Columns   []Column
	CreatedAt string // ISO timestamp
	Migration *Migration
}

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

// SchemaRegistry tracks all schema versions for all tables
type SchemaRegistry struct {
	// tableName -> version -> SchemaVersion
	schemas map[string]map[int]*SchemaVersion
	// Registered migrations: (tableName, fromVer, toVer) -> Migration
	migrations map[string]*Migration
}

// NewSchemaRegistry creates a new schema registry
func NewSchemaRegistry() *SchemaRegistry {
	return &SchemaRegistry{
		schemas:    make(map[string]map[int]*SchemaVersion),
		migrations: make(map[string]*Migration),
	}
}

// RegisterSchema registers a schema version
func (sr *SchemaRegistry) RegisterSchema(tableName string, version int, columns []Column) {
	if _, exists := sr.schemas[tableName]; !exists {
		sr.schemas[tableName] = make(map[int]*SchemaVersion)
	}

	sr.schemas[tableName][version] = &SchemaVersion{
		Version:   version,
		TableName: tableName,
		Columns:   columns,
	}
}

// RegisterMigration registers a migration between two schema versions
func (sr *SchemaRegistry) RegisterMigration(tableName string, fromVer, toVer int, ops []MigrationOp) {
	migration := &Migration{
		FromVersion: fromVer,
		ToVersion:   toVer,
		Operations:  ops,
	}

	key := fmt.Sprintf("%s_%d_to_%d", tableName, fromVer, toVer)
	sr.migrations[key] = migration

	// Also register in schema version for reference
	if schema, exists := sr.schemas[tableName][toVer]; exists {
		schema.Migration = migration
	}
}

// GetSchema retrieves a schema version
func (sr *SchemaRegistry) GetSchema(tableName string, version int) (*SchemaVersion, error) {
	if tableSchemas, exists := sr.schemas[tableName]; exists {
		if schema, exists := tableSchemas[version]; exists {
			return schema, nil
		}
	}
	return nil, fmt.Errorf("schema not found: %s version %d", tableName, version)
}

// GetLatestSchemaVersion returns the highest version number for a table
func (sr *SchemaRegistry) GetLatestSchemaVersion(tableName string) int {
	if tableSchemas, exists := sr.schemas[tableName]; exists {
		maxVersion := 0
		for version := range tableSchemas {
			if version > maxVersion {
				maxVersion = version
			}
		}
		return maxVersion
	}
	return 0
}

// MigrateRow applies a series of migrations to a row
// Returns the migrated row or error if migration path doesn't exist
func (sr *SchemaRegistry) MigrateRow(tableName string, row map[string]interface{}, fromVersion, toVersion int) (map[string]interface{}, error) {
	if fromVersion == toVersion {
		return row, nil
	}

	if fromVersion > toVersion {
		return nil, fmt.Errorf("cannot migrate backwards from version %d to %d", fromVersion, toVersion)
	}

	migratedRow := make(map[string]interface{})
	for k, v := range row {
		migratedRow[k] = v
	}

	// Apply migrations sequentially
	for version := fromVersion; version < toVersion; version++ {
		key := fmt.Sprintf("%s_%d_to_%d", tableName, version, version+1)
		migration, exists := sr.migrations[key]
		if !exists {
			return nil, fmt.Errorf("no migration path from %s v%d to v%d", tableName, version, version+1)
		}

		// Apply migration operations
		var err error
		migratedRow, err = sr.applyMigration(migratedRow, migration)
		if err != nil {
			return nil, err
		}
	}

	return migratedRow, nil
}

// applyMigration applies a single migration to a row
func (sr *SchemaRegistry) applyMigration(row map[string]interface{}, migration *Migration) (map[string]interface{}, error) {
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

// EventToSchemaVersion converts an event log schema event to a SchemaVersion
func EventToSchemaVersion(event *eventlog.Event) (*SchemaVersion, error) {
	if event.Type != eventlog.SchemaCreated {
		return nil, fmt.Errorf("event is not a schema creation event")
	}

	payload := event.Payload.(map[string]interface{})
	tableName := payload["table_name"].(string)

	// Parse columns
	colsRaw := payload["columns"].([]interface{})
	columns := make([]Column, len(colsRaw))
	for i, colRaw := range colsRaw {
		colMap := colRaw.(map[string]interface{})
		columns[i] = Column{
			Name:       colMap["name"].(string),
			Type:       ColumnType(colMap["type"].(string)),
			PrimaryKey: colMap["primary_key"].(bool),
			Unique:     colMap["unique"].(bool),
		}
	}

	return &SchemaVersion{
		Version:   event.Version,
		TableName: tableName,
		Columns:   columns,
	}, nil
}

// SchemaCompatibilityCheck verifies if a row from an old schema can be read by new schema
type SchemaCompatibilityCheck struct {
	OldSchema *SchemaVersion
	NewSchema *SchemaVersion
	Status    CompatibilityStatus
	Message   string
}

type CompatibilityStatus string

const (
	Compatible      CompatibilityStatus = "COMPATIBLE"
	Incompatible    CompatibilityStatus = "INCOMPATIBLE"
	MigrationNeeded CompatibilityStatus = "MIGRATION_NEEDED"
)

// CheckCompatibility checks if two schemas are compatible
func (sr *SchemaRegistry) CheckCompatibility(tableName string, oldVersion, newVersion int) SchemaCompatibilityCheck {
	check := SchemaCompatibilityCheck{
		Status:  Compatible,
		Message: fmt.Sprintf("%s: v%d -> v%d", tableName, oldVersion, newVersion),
	}

	oldSchema, err := sr.GetSchema(tableName, oldVersion)
	if err != nil {
		check.Status = Incompatible
		check.Message = err.Error()
		return check
	}

	newSchema, err := sr.GetSchema(tableName, newVersion)
	if err != nil {
		check.Status = Incompatible
		check.Message = err.Error()
		return check
	}

	check.OldSchema = oldSchema
	check.NewSchema = newSchema

	// Check if migration exists
	key := fmt.Sprintf("%s_%d_to_%d", tableName, oldVersion, newVersion)
	_, migrationExists := sr.migrations[key]

	if oldVersion < newVersion && !migrationExists {
		check.Status = Incompatible
		check.Message = fmt.Sprintf("No migration registered from v%d to v%d", oldVersion, newVersion)
		return check
	}

	if oldVersion < newVersion {
		check.Status = MigrationNeeded
		check.Message = fmt.Sprintf("Migration available from v%d to v%d", oldVersion, newVersion)
	}

	return check
}
