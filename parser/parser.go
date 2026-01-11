package parser

import (
	"fmt"
	"regexp"
	"strconv"
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

// Parse comma-separated values
func parseValues(str string) []interface{} {
	var values []interface{}
	parts := strings.Split(str, ",")
	for _, part := range parts {
		values = append(values, parseValue(strings.TrimSpace(part)))
	}
	return values
}

// Parse a single value
func parseValue(str string) interface{} {
	str = strings.TrimSpace(str)

	// String (quoted)
	if (strings.HasPrefix(str, "'") && strings.HasSuffix(str, "'")) ||
		(strings.HasPrefix(str, "\"") && strings.HasSuffix(str, "\"")) {
		return str[1 : len(str)-1]
	}

	// Bool
	if strings.ToLower(str) == "true" {
		return true
	}
	if strings.ToLower(str) == "false" {
		return false
	}

	// Int/Float
	if val, err := strconv.ParseFloat(str, 64); err == nil {
		return val
	}

	// Default: string
	return str
}

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
