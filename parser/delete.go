package parser

import (
	"fmt"
	"regexp"
	"strings"
)

func (p *Parser) parseDelete(sql string) (*ParsedStatement, error) {
	// DELETE FROM users WHERE id = 1
	re := regexp.MustCompile(`(?i)DELETE FROM\s+(\w+)\s+WHERE\s+(\w+)\s*=\s*(.+)`)
	matches := re.FindStringSubmatch(sql)
	if len(matches) != 4 {
		return nil, fmt.Errorf("invalid DELETE syntax (WHERE required)")
	}

	tableName := matches[1]
	column := matches[2]
	valueStr := strings.TrimSpace(matches[3])
	value := parseValue(valueStr)

	return &ParsedStatement{
		Type:      "DELETE",
		TableName: tableName,
		Where:     &WhereClause{Column: column, Value: value},
	}, nil
}
