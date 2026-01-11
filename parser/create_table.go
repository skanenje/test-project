package parser

import (
	"fmt"
	"regexp"
	"strings"

	"rdbms/schema"
)

func (p *Parser) parseCreateTable(sql string) (*ParsedStatement, error) {
	// CREATE TABLE users (id INT PRIMARY KEY, name TEXT UNIQUE, active BOOL)
	re := regexp.MustCompile(`(?i)CREATE TABLE\s+(\w+)\s*\((.*)\)`)
	matches := re.FindStringSubmatch(sql)
	if len(matches) != 3 {
		return nil, fmt.Errorf("invalid CREATE TABLE syntax")
	}

	tableName := matches[1]
	columnsStr := matches[2]

	var columns []schema.Column
	for _, colDef := range strings.Split(columnsStr, ",") {
		parts := strings.Fields(strings.TrimSpace(colDef))
		if len(parts) < 2 {
			return nil, fmt.Errorf("invalid column definition: %s", colDef)
		}

		col := schema.Column{
			Name: parts[0],
			Type: schema.ColumnType(strings.ToUpper(parts[1])),
		}

		// Check for PRIMARY KEY or UNIQUE
		for i := 2; i < len(parts); i++ {
			switch strings.ToUpper(parts[i]) {
			case "PRIMARY":
				if i+1 < len(parts) && strings.ToUpper(parts[i+1]) == "KEY" {
					col.PrimaryKey = true
					i++
				}
			case "UNIQUE":
				col.Unique = true
			}
		}

		columns = append(columns, col)
	}

	return &ParsedStatement{
		Type:      "CREATE_TABLE",
		TableName: tableName,
		Columns:   columns,
	}, nil
}
