# Parser Package

## Purpose

The `parser` package converts SQL query strings into structured `ParsedStatement` objects for execution. It implements a recursive descent parser for core SQL operations.

## Supported SQL Operations

```sql
CREATE TABLE users (id INT PRIMARY KEY, name TEXT, active BOOL)
INSERT INTO users (id, name, active) VALUES (1, 'Alice', true)
SELECT * FROM users WHERE id = 1
UPDATE users SET name = 'Bob' WHERE id = 1
DELETE FROM users WHERE id = 1
SELECT * FROM users JOIN orders ON users.id = orders.user_id
```

## Key Types

```go
type ParsedStatement struct {
    Type           string
    TableName      string
    Columns        []schema.Column
    Values         map[string]interface{}
    Where          *WhereClause
    SetColumn      string
    SetValue       interface{}
    JoinTable      string
    JoinCondition  *JoinCondition
}

type WhereClause struct {
    Column string
    Value  interface{}
}

type JoinCondition struct {
    LeftTable   string
    LeftColumn  string
    RightTable  string
    RightColumn string
}
```

## Main Functions

- `New() *Parser` - Create parser
- `(p *Parser) Parse(sql string) (*ParsedStatement, error)` - Parse SQL string
- `(p *Parser) parseCreateTable(sql string) (*ParsedStatement, error)`
- `(p *Parser) parseInsert(sql string) (*ParsedStatement, error)`
- `(p *Parser) parseSelect(sql string) (*ParsedStatement, error)`
- `(p *Parser) parseUpdate(sql string) (*ParsedStatement, error)`
- `(p *Parser) parseDelete(sql string) (*ParsedStatement, error)`
- `(p *Parser) parseJoin(sql string) (*ParsedStatement, error)`

## Usage Example

```go
parser := parser.New()
stmt, err := parser.Parse("CREATE TABLE users (id INT PRIMARY KEY, name TEXT)")
if err != nil {
    log.Fatal(err)
}
fmt.Println(stmt.Type)        // "CREATE_TABLE"
fmt.Println(stmt.TableName)   // "users"
```

## Parser Limitations

This simplified parser is designed for education:
- No complex expressions (only equality comparisons)
- No multiple WHERE conditions
- No ORDER BY, GROUP BY, aggregations
- Basic error handling

## Integration Points

- **REPL/CLI**: Parses user input
- **Executor Package**: Receives parsed statements
- **Catalog Package**: References table/column names
- **Web Package**: REST API uses parser
