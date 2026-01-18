package main

import (
	"bufio"
	"fmt"
	"os"
	"strings"

	"rdbms/cmd/web"
	"rdbms/database"
	"rdbms/executor"
	"rdbms/parser"
)

func runREPL(db *database.Database) {
	p := parser.New()
	exec := executor.New(db)
	scanner := bufio.NewScanner(os.Stdin)

	fmt.Println("=== RDBMS REPL ===")
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

func runWebServer(db *database.Database, port string) {
	if err := web.RunServer(db, port); err != nil {
		fmt.Printf("Error starting web server: %v\n", err)
	}
}

func main() {
	// Use existing modular Database
	db, err := database.New("./demo_data")
	if err != nil {
		fmt.Printf("Failed to create database: %v\n", err)
		os.Exit(1)
	}

	if len(os.Args) > 1 && os.Args[1] == "web" {
		port := "8080"
		if len(os.Args) > 2 {
			port = os.Args[2]
		}
		runWebServer(db, port)
	} else {
		runREPL(db)
	}
}
