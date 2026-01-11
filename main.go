// Simple RDBMS - Phase 1: Storage Layer + Schema + Basic CRUD
//
// Demo application showing basic database operations

package main

import (
	"fmt"
	"rdbms/database"
	"rdbms/schema"
	"rdbms/storage"
)

func main() {
	// Initialize database
	db, err := database.New("./demo_data")
	if err != nil {
		panic(err)
	}

	// Create a users table
	err = db.CreateTable("users", []schema.Column{
		{Name: "id", Type: schema.TypeInt},
		{Name: "name", Type: schema.TypeText},
		{Name: "active", Type: schema.TypeBool},
	})
	if err != nil {
		panic(err)
	}
	fmt.Println("✓ Created table 'users'")

	// Insert rows
	rowID1, _ := db.Insert("users", storage.Row{"id": 1.0, "name": "Alice", "active": true})
	rowID2, _ := db.Insert("users", storage.Row{"id": 2.0, "name": "Bob", "active": false})
	rowID3, _ := db.Insert("users", storage.Row{"id": 3.0, "name": "Charlie", "active": true})
	fmt.Printf("✓ Inserted 3 rows (row_ids: %d, %d, %d)\n", rowID1, rowID2, rowID3)

	// Select all
	fmt.Println("\n📊 SELECT * FROM users:")
	rows, _ := db.SelectAll("users")
	for _, row := range rows {
		fmt.Printf("  %v\n", row)
	}

	// Delete Bob
	db.Delete("users", rowID2)
	fmt.Printf("\n✓ Deleted row %d (Bob)\n", rowID2)

	// Select again
	fmt.Println("\n📊 SELECT * FROM users (after delete):")
	rows, _ = db.SelectAll("users")
	for _, row := range rows {
		fmt.Printf("  %v\n", row)
	}

	// Update Alice
	newRowID, _ := db.Update("users", rowID1, storage.Row{"id": 1.0, "name": "Alice Smith", "active": true})
	fmt.Printf("\n✓ Updated Alice (new row_id: %d)\n", newRowID)

	// Final select
	fmt.Println("\n📊 SELECT * FROM users (after update):")
	rows, _ = db.SelectAll("users")
	for _, row := range rows {
		fmt.Printf("  %v\n", row)
	}
}
