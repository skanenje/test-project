package database

import (
	"fmt"
	"rdbms/parser"
	"rdbms/storage"
)

// Join performs INNER JOIN using nested-loop algorithm
func (db *Database) Join(leftTable, rightTable string, condition *parser.JoinCondition, where *parser.WhereClause) ([]storage.Row, error) {
	if !db.catalog.TableExists(leftTable) {
		return nil, fmt.Errorf("table '%s' does not exist", leftTable)
	}
	if !db.catalog.TableExists(rightTable) {
		return nil, fmt.Errorf("table '%s' does not exist", rightTable)
	}

	leftRows, err := db.storage.ScanAll(leftTable)
	if err != nil {
		return nil, err
	}

	rightRows, err := db.storage.ScanAll(rightTable)
	if err != nil {
		return nil, err
	}

	var result []storage.Row

	// Nested-loop join
	for _, leftRow := range leftRows {
		for _, rightRow := range rightRows {
			// Check join condition
			leftVal := leftRow.Row[condition.LeftColumn]
			rightVal := rightRow.Row[condition.RightColumn]

			if !valuesEqual(leftVal, rightVal) {
				continue
			}

			// Merge rows with table prefix
			joinedRow := make(storage.Row)
			for k, v := range leftRow.Row {
				joinedRow[leftTable+"."+k] = v
			}
			for k, v := range rightRow.Row {
				joinedRow[rightTable+"."+k] = v
			}

			// Apply WHERE filter if present
			if where != nil {
				if val, exists := joinedRow[where.Column]; exists {
					if !valuesEqual(val, where.Value) {
						continue
					}
				} else {
					continue
				}
			}

			result = append(result, joinedRow)
		}
	}

	return result, nil
}
