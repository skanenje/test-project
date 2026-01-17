package storage

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"
)

// SnapshotManager handles snapshot creation, storage, and restoration
type SnapshotManager struct {
	mu              sync.RWMutex
	dataDir         string
	snapshotDir     string
	latestSnapshot  *SnapshotMeta
	snapshotHistory []SnapshotMeta
}

// NewSnapshotManager creates a new snapshot manager
func NewSnapshotManager(dataDir string) (*SnapshotManager, error) {
	snapshotDir := filepath.Join(dataDir, "snapshots")
	if err := os.MkdirAll(snapshotDir, 0755); err != nil {
		return nil, err
	}

	sm := &SnapshotManager{
		dataDir:         dataDir,
		snapshotDir:     snapshotDir,
		snapshotHistory: make([]SnapshotMeta, 0),
	}

	// Load existing snapshot metadata
	if err := sm.loadSnapshotIndex(); err != nil {
		// If no index exists, that's fine (fresh database)
		_ = err
	}

	return sm, nil
}

// loadSnapshotIndex loads the snapshot index from disk
func (sm *SnapshotManager) loadSnapshotIndex() error {
	indexPath := filepath.Join(sm.snapshotDir, "index.json")
	data, err := os.ReadFile(indexPath)
	if err != nil {
		return err
	}

	var history []SnapshotMeta
	if err := json.Unmarshal(data, &history); err != nil {
		return err
	}

	sm.snapshotHistory = history
	if len(history) > 0 {
		sm.latestSnapshot = &history[len(history)-1]
	}

	return nil
}

// saveSnapshotIndex persists snapshot metadata to disk
func (sm *SnapshotManager) saveSnapshotIndex() error {
	indexPath := filepath.Join(sm.snapshotDir, "index.json")
	data, err := json.MarshalIndent(sm.snapshotHistory, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(indexPath, data, 0644)
}

// CreateSnapshot creates and saves a snapshot of the current state
func (sm *SnapshotManager) CreateSnapshot(state *DerivedState, baseEventID uint64) (*SnapshotMeta, error) {
	sm.mu.Lock()
	defer sm.mu.Unlock()

	// Generate snapshot ID
	snapshotID := fmt.Sprintf("snap_%d_%s", baseEventID, time.Now().Format("20060102_150405"))

	// Compute data hash
	dataHash, err := computeSnapshotHash(state)
	if err != nil {
		return nil, err
	}

	// Create metadata
	meta := SnapshotMeta{
		SnapshotID:     snapshotID,
		BaseEventID:    baseEventID,
		CreatedAt:      time.Now().UTC(),
		SnapshotPath:   filepath.Join(sm.snapshotDir, snapshotID+".json"),
		DataHash:       dataHash,
		EventsIncluded: countEvents(state),
	}

	// Create snapshot data
	snapData := SnapshotData{
		Meta:        meta,
		Tables:      state.Tables,
		DeletedRows: state.DeletedRows,
	}

	// Marshal and save
	data, err := json.MarshalIndent(snapData, "", "  ")
	if err != nil {
		return nil, err
	}

	if err := os.WriteFile(meta.SnapshotPath, data, 0644); err != nil {
		return nil, err
	}

	// Update history
	sm.snapshotHistory = append(sm.snapshotHistory, meta)
	sm.latestSnapshot = &meta

	// Persist index
	if err := sm.saveSnapshotIndex(); err != nil {
		return nil, err
	}

	return &meta, nil
}

// RestoreFromSnapshot loads a snapshot from disk
func (sm *SnapshotManager) RestoreFromSnapshot(snapshotID string) (*DerivedState, *SnapshotMeta, error) {
	sm.mu.RLock()
	defer sm.mu.RUnlock()

	// Find snapshot metadata
	var meta *SnapshotMeta
	for i := range sm.snapshotHistory {
		if sm.snapshotHistory[i].SnapshotID == snapshotID {
			meta = &sm.snapshotHistory[i]
			break
		}
	}

	if meta == nil {
		return nil, nil, fmt.Errorf("snapshot %s not found", snapshotID)
	}

	// Load snapshot file
	data, err := os.ReadFile(meta.SnapshotPath)
	if err != nil {
		return nil, nil, err
	}

	var snapData SnapshotData
	if err := json.Unmarshal(data, &snapData); err != nil {
		return nil, nil, err
	}

	// Validate hash
	computedHash, err := computeSnapshotHash(&DerivedState{
		Tables:      snapData.Tables,
		DeletedRows: snapData.DeletedRows,
	})
	if err != nil {
		return nil, nil, err
	}

	if computedHash != snapData.Meta.DataHash {
		return nil, nil, fmt.Errorf("snapshot data corruption detected: hash mismatch")
	}

	state := &DerivedState{
		Tables:      snapData.Tables,
		DeletedRows: snapData.DeletedRows,
	}

	return state, meta, nil
}

// RestoreLatestSnapshot restores the most recent snapshot
func (sm *SnapshotManager) RestoreLatestSnapshot() (*DerivedState, *SnapshotMeta, error) {
	sm.mu.RLock()
	defer sm.mu.RUnlock()

	if sm.latestSnapshot == nil {
		return nil, nil, fmt.Errorf("no snapshots available")
	}

	return sm.RestoreFromSnapshot(sm.latestSnapshot.SnapshotID)
}

// GetLatestSnapshotMeta returns metadata about the most recent snapshot
func (sm *SnapshotManager) GetLatestSnapshotMeta() *SnapshotMeta {
	sm.mu.RLock()
	defer sm.mu.RUnlock()
	return sm.latestSnapshot
}

// GetSnapshotHistory returns all snapshots in chronological order
func (sm *SnapshotManager) GetSnapshotHistory() []SnapshotMeta {
	sm.mu.RLock()
	defer sm.mu.RUnlock()
	return sm.snapshotHistory
}

// PruneOldSnapshots keeps only the N most recent snapshots
func (sm *SnapshotManager) PruneOldSnapshots(keepCount int) error {
	sm.mu.Lock()
	defer sm.mu.Unlock()

	if len(sm.snapshotHistory) <= keepCount {
		return nil
	}

	// Delete old snapshot files
	for i := 0; i < len(sm.snapshotHistory)-keepCount; i++ {
		os.Remove(sm.snapshotHistory[i].SnapshotPath)
	}

	// Update history
	sm.snapshotHistory = sm.snapshotHistory[len(sm.snapshotHistory)-keepCount:]

	// Update latest
	if len(sm.snapshotHistory) > 0 {
		sm.latestSnapshot = &sm.snapshotHistory[len(sm.snapshotHistory)-1]
	} else {
		sm.latestSnapshot = nil
	}

	// Persist index
	return sm.saveSnapshotIndex()
}
