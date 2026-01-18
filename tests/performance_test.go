package tests

import (
	"fmt"
	"testing"
)

// BenchmarkInsert measures the performance of inserting rows
func BenchmarkInsert(b *testing.B) {
	tdb := NewTestDB(b)
	defer tdb.Cleanup()

	columns := SampleTableColumns()
	tdb.CreateTable("users", columns, "id")

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		// Create unique data for each insert
		row := SampleUserRow(i, fmt.Sprintf("User%d", i), 20+(i%50))
		_, err := tdb.InsertRow("users", row)
		if err != nil {
			b.Fatalf("insert failed: %v", err)
		}
	}
}

// BenchmarkSelectIndexed measures lookup performance on a Primary Key (Indexed)
func BenchmarkSelectIndexed(b *testing.B) {
	tdb := NewTestDB(b)
	defer tdb.Cleanup()

	columns := SampleTableColumns()
	tdb.CreateTable("users", columns, "id")

	// Pre-populate database with 1000 rows
	numRows := 1000
	for i := 0; i < numRows; i++ {
		row := SampleUserRow(i, fmt.Sprintf("User%d", i), 20+(i%50))
		tdb.InsertRow("users", row)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		// Select by ID (Primary Key is indexed)
		id := i % numRows
		_, err := tdb.SelectWhere("users", "id", id)
		if err != nil {
			b.Fatalf("select failed: %v", err)
		}
	}
}

// BenchmarkSelectScan measures lookup performance on a non-indexed column
func BenchmarkSelectScan(b *testing.B) {
	tdb := NewTestDB(b)
	defer tdb.Cleanup()

	columns := SampleTableColumns()
	tdb.CreateTable("users", columns, "id")

	// Pre-populate database with 1000 rows
	numRows := 1000
	for i := 0; i < numRows; i++ {
		row := SampleUserRow(i, fmt.Sprintf("User%d", i), 20+(i%50))
		tdb.InsertRow("users", row)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		// Select by Age (Not indexed in SampleTableColumns)
		age := 20 + (i % 50)
		_, err := tdb.SelectWhere("users", "age", age)
		if err != nil {
			b.Fatalf("select failed: %v", err)
		}
	}
}
