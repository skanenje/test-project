// Simple RDBMS - Phase 2: SQL Parser + WHERE + REPL
//
// Added in Phase 2:
// - SQL parser (CREATE TABLE, INSERT, SELECT, UPDATE, DELETE)
// - WHERE clause support (column = value)
// - Interactive REPL

package main

import (
	"bufio"
	"fmt"
	"os"
	"strings"

	"rdbms/database"
	"rdbms/executor"
	"rdbms/parser"
)

func runREPL(db *database.Database) {
	p := parser.New()
	exec := executor.New(db)
	scanner := bufio.NewScanner(os.Stdin)

	fmt.Println("=== Simple RDBMS REPL ===")
	fmt.Println("Type SQL commands or 'exit' to quit")
	fmt.Println()

	for {
		fmt.Print("sql> ")
		if !scanner.Scan() {
			break
		}

		input := strings.TrimSpace(scanner.Text())
		if input == "" {
			continue
		}

		if strings.ToLower(input) == "exit" || strings.ToLower(input) == "quit" {
			fmt.Println("Goodbye!")
			break
		}

		stmt, err := p.Parse(input)
		if err != nil {
			fmt.Printf("Parse error: %v\n", err)
			fmt.Println()
			continue
		}

		result, err := exec.Execute(stmt)
		if err != nil {
			fmt.Printf("Error: %v\n", err)
		} else {
			fmt.Println(result)
		}
		fmt.Println()
	}
}

func main() {
	db, err := database.New("./demo_data")
	if err != nil {
		panic(err)
	}

	// Run REPL
	runREPL(db)
}
