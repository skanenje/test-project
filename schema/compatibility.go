package schema

import "fmt"

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
