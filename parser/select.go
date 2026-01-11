package parser

import (
	"fmt"
	"regexp"
	"strings"
)

func (p *Parser) parseSelect(sql string) (*ParsedStatement, error) {
	// SELECT * FROM users
	// SELECT * FROM users WHERE name = 'Alice'
	re := regexp.MustCompile(`(?i)SELECT\s+\*\s+FROM\s+(\w+)(?:\s+WHERE\s+(\w+)\s*=\s*(.+))?`)
	matches := re.FindStringSubmatch(sql)
	if len(matches) < 2 {
		return nil, fmt.Errorf("invalid SELECT syntax")
	}

	tableName := matches[1]
	var where *WhereClause

	if len(matches) == 4 && matches[2] != "" {
		column := matches[2]
		valueStr := strings.TrimSpace(matches[3])
		value := parseValue(valueStr)
		where = &WhereClause{Column: column, Value: value}
	}

	return &ParsedStatement{
		Type:      "SELECT",
		TableName: tableName,
		Where:     where,
	}, nil
}
