package catalog

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"rdbms/schema"
)

// Catalog manages table schemas
type Catalog struct {
	schemas map[string]*schema.Table
	dataDir string
}

// New creates a new catalog
func New(dataDir string) (*Catalog, error) {
	c := &Catalog{
		schemas: make(map[string]*schema.Table),
		dataDir: dataDir,
	}

	if err := c.load(); err != nil {
		return nil, err
	}

	return c, nil
}

func (c *Catalog) catalogFile() string {
	return filepath.Join(c.dataDir, "_catalog.json")
}

// load loads table schemas from disk
func (c *Catalog) load() error {
	catalogPath := c.catalogFile()

	// If catalog doesn't exist, that's fine (fresh DB)
	if _, err := os.Stat(catalogPath); os.IsNotExist(err) {
		return nil
	}

	data, err := os.ReadFile(catalogPath)
	if err != nil {
		return err
	}

	var catalog map[string]*schema.Table
	if err := json.Unmarshal(data, &catalog); err != nil {
		return err
	}

	c.schemas = catalog
	return nil
}

// save saves table schemas to disk
func (c *Catalog) save() error {
	data, err := json.MarshalIndent(c.schemas, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(c.catalogFile(), data, 0644)
}

// CreateTable creates a new table
func (c *Catalog) CreateTable(tableName string, columns []schema.Column) error {
	if _, exists := c.schemas[tableName]; exists {
		return fmt.Errorf("table '%s' already exists", tableName)
	}

	table := &schema.Table{
		Name:    tableName,
		Columns: columns,
	}

	c.schemas[tableName] = table
	return c.save()
}

// GetTable retrieves a table schema
func (c *Catalog) GetTable(tableName string) (*schema.Table, error) {
	table, exists := c.schemas[tableName]
	if !exists {
		return nil, fmt.Errorf("table '%s' does not exist", tableName)
	}
	return table, nil
}

// TableExists checks if a table exists
func (c *Catalog) TableExists(tableName string) bool {
	_, exists := c.schemas[tableName]
	return exists
}
