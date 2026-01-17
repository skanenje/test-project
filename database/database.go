package database

import (
	"sync"

	"rdbms/catalog"
	"rdbms/index"
	"rdbms/storage"
)

// Database is the main database interface - now backed by immutable event log
type Database struct {
	mu               sync.RWMutex
	eventStore       *storage.EventStore
	queryEngine      *storage.QueryEngine
	snapshotManager  *storage.SnapshotManager
	catalog          *catalog.Catalog
	indexes          map[string]map[string]*index.Index // table -> column -> index
	nextRowID        map[string]int64                   // table -> next row ID
	snapshotInterval int64                              // Create snapshot every N events
}

// New creates a new database instance backed by event log
func New(dataDir string) (*Database, error) {
	// Initialize event store (append-only log)
	eventStore, err := storage.NewEventStore(dataDir)
	if err != nil {
		return nil, err
	}

	// Initialize snapshot manager
	snapshotManager, err := storage.NewSnapshotManager(dataDir)
	if err != nil {
		return nil, err
	}

	// Initialize query engine (snapshots + event replay)
	queryEngine := storage.NewQueryEngine(eventStore, snapshotManager)

	// Load catalog (schema)
	cat, err := catalog.New(dataDir)
	if err != nil {
		return nil, err
	}

	db := &Database{
		eventStore:       eventStore,
		queryEngine:      queryEngine,
		snapshotManager:  snapshotManager,
		catalog:          cat,
		indexes:          make(map[string]map[string]*index.Index),
		nextRowID:        make(map[string]int64),
		snapshotInterval: 1000, // Snapshot every 1000 events
	}

	// Rebuild indexes from current state
	if err := db.rebuildAllIndexes(); err != nil {
		return nil, err
	}

	return db, nil
}

// GetEventStore returns the underlying event store
func (db *Database) GetEventStore() *storage.EventStore {
	return db.eventStore
}

// Close closes the database
func (db *Database) Close() error {
	return db.eventStore.Close()
}
