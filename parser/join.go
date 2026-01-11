package parser

import (
	"fmt"
	"regexp"
	"strings"
)

func (p *Parser) parseJoin(sql string) (*ParsedStatement, error) {
	// SELECT * FROM users JOIN posts ON users.id = posts.user_id
	// SELECT * FROM users JOIN posts ON users.id = posts.user_id WHERE posts.published = true
	re := regexp.MustCompile(`(?i)SELECT\s+\*\s+FROM\s+(\w+)\s+JOIN\s+(\w+)\s+ON\s+(\w+)\.(\w+)\s*=\s*(\w+)\.(\w+)(?:\s+WHERE\s+([\w.]+)\s*=\s*(.+))?`)
	matches := re.FindStringSubmatch(sql)
	if len(matches) < 7 {
		return nil, fmt.Errorf("invalid JOIN syntax")
	}

	leftTable := matches[1]
	rightTable := matches[2]
	leftJoinTable := matches[3]
	leftJoinCol := matches[4]
	rightJoinTable := matches[5]
	rightJoinCol := matches[6]

	// Validate join condition references correct tables
	if leftJoinTable != leftTable {
		return nil, fmt.Errorf("join condition references unknown table '%s'", leftJoinTable)
	}
	if rightJoinTable != rightTable {
		return nil, fmt.Errorf("join condition references unknown table '%s'", rightJoinTable)
	}

	condition := &JoinCondition{
		LeftTable:   leftTable,
		LeftColumn:  leftJoinCol,
		RightTable:  rightTable,
		RightColumn: rightJoinCol,
	}

	var where *WhereClause
	if len(matches) == 9 && matches[7] != "" {
		whereColumn := matches[7]
		whereValueStr := strings.TrimSpace(matches[8])
		whereValue := parseValue(whereValueStr)
		where = &WhereClause{Column: whereColumn, Value: whereValue}
	}

	return &ParsedStatement{
		Type:          "JOIN",
		TableName:     leftTable,
		JoinTable:     rightTable,
		JoinCondition: condition,
		Where:         where,
	}, nil
}
