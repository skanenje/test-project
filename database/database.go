package database

import (
	"rdbms/catalog"
	"rdbms/index"
	"rdbms/storage"
)

// Database is the main database interface
type Database struct {
	storage *storage.Engine
	catalog *catalog.Catalog
	indexes map[string]map[string]*index.Index // table -> column -> index
}

// New creates a new database instance
func New(dataDir string) (*Database, error) {
	storageEngine, err := storage.NewEngine(dataDir)
	if err != nil {
		return nil, err
	}

	cat, err := catalog.New(dataDir)
	if err != nil {
		return nil, err
	}

	db := &Database{
		storage: storageEngine,
		catalog: cat,
		indexes: make(map[string]map[string]*index.Index),
	}

	// Rebuild indexes for existing tables
	if err := db.rebuildAllIndexes(); err != nil {
		return nil, err
	}

	return db, nil
}
