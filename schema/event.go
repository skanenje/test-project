package schema

import (
	"fmt"
	"rdbms/eventlog"
)

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
