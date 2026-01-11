package parser

import (
	"fmt"
	"strings"

	"rdbms/schema"
)

// WhereClause represents a simple WHERE condition
type WhereClause struct {
	Column string
	Value  interface{}
}

// ParsedStatement represents a parsed SQL statement
type ParsedStatement struct {
	Type          string // CREATE_TABLE, INSERT, SELECT, UPDATE, DELETE, JOIN
	TableName     string
	Columns       []schema.Column
	Values        map[string]interface{}
	Where         *WhereClause
	SetColumn     string
	SetValue      interface{}
	JoinTable     string
	JoinCondition *JoinCondition
}

// JoinCondition represents ON clause
type JoinCondition struct {
	LeftTable   string
	LeftColumn  string
	RightTable  string
	RightColumn string
}

// Parser handles SQL parsing
type Parser struct{}

// New creates a new parser
func New() *Parser {
	return &Parser{}
}

// Parse parses a SQL statement
func (p *Parser) Parse(sql string) (*ParsedStatement, error) {
	sql = strings.TrimSpace(sql)
	sqlUpper := strings.ToUpper(sql)

	if strings.HasPrefix(sqlUpper, "CREATE TABLE") {
		return p.parseCreateTable(sql)
	} else if strings.HasPrefix(sqlUpper, "INSERT INTO") {
		return p.parseInsert(sql)
	} else if strings.HasPrefix(sqlUpper, "SELECT") {
		// Check for JOIN
		if strings.Contains(sqlUpper, " JOIN ") {
			return p.parseJoin(sql)
		}
		return p.parseSelect(sql)
	} else if strings.HasPrefix(sqlUpper, "DELETE FROM") {
		return p.parseDelete(sql)
	} else if strings.HasPrefix(sqlUpper, "UPDATE") {
		return p.parseUpdate(sql)
	}

	return nil, fmt.Errorf("unsupported SQL command")
}
