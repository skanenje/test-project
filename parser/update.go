package parser

import (
	"fmt"
	"regexp"
	"strings"
)

func (p *Parser) parseUpdate(sql string) (*ParsedStatement, error) {
	// UPDATE users SET name = 'Bob' WHERE id = 1
	re := regexp.MustCompile(`(?i)UPDATE\s+(\w+)\s+SET\s+(\w+)\s*=\s*(.+?)\s+WHERE\s+(\w+)\s*=\s*(.+)`)
	matches := re.FindStringSubmatch(sql)
	if len(matches) != 6 {
		return nil, fmt.Errorf("invalid UPDATE syntax")
	}

	tableName := matches[1]
	setColumn := matches[2]
	setValueStr := strings.TrimSpace(matches[3])
	whereColumn := matches[4]
	whereValueStr := strings.TrimSpace(matches[5])

	setValue := parseValue(setValueStr)
	whereValue := parseValue(whereValueStr)

	return &ParsedStatement{
		Type:      "UPDATE",
		TableName: tableName,
		SetColumn: setColumn,
		SetValue:  setValue,
		Where:     &WhereClause{Column: whereColumn, Value: whereValue},
	}, nil
}
