package schema

import (
	"fmt"
)

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

		// Apply each migration operation sequentially
		var err error
		migratedRow, err = applyMigration(migratedRow, migration)
		if err != nil {
			return nil, err
		}
	}

	return migratedRow, nil
}
