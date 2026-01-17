package storage

import "time"

// SnapshotMeta contains metadata about a snapshot
type SnapshotMeta struct {
	SnapshotID     string    `json:"snapshot_id"`
	BaseEventID    uint64    `json:"base_event_id"`
	CreatedAt      time.Time `json:"created_at"`
	SnapshotPath   string    `json:"snapshot_path"`
	DataHash       string    `json:"data_hash"`
	EventsIncluded int64     `json:"events_included"`
}

// SnapshotData holds the actual state data
type SnapshotData struct {
	Meta        SnapshotMeta              `json:"meta"`
	Tables      map[string]map[int64]Row  `json:"tables"`
	DeletedRows map[string]map[int64]bool `json:"deleted_rows"`
}
