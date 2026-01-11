package parser

import (
	"fmt"
	"regexp"
)

func (p *Parser) parseInsert(sql string) (*ParsedStatement, error) {
	// INSERT INTO users VALUES (1, 'Alice', true)
	re := regexp.MustCompile(`(?i)INSERT INTO\s+(\w+)\s+VALUES\s*\((.*)\)`)
	matches := re.FindStringSubmatch(sql)
	if len(matches) != 3 {
		return nil, fmt.Errorf("invalid INSERT syntax")
	}

	tableName := matches[1]
	valuesStr := matches[2]
	values := parseValues(valuesStr)

	return &ParsedStatement{
		Type:      "INSERT",
		TableName: tableName,
		Values:    map[string]interface{}{"_raw_values": values},
	}, nil
}
