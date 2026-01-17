package storage

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
)

// computeSnapshotHash computes SHA256 hash of snapshot data
func computeSnapshotHash(state *DerivedState) (string, error) {
	// Marshal state to deterministic JSON for hashing
	data, err := json.Marshal(state)
	if err != nil {
		return "", err
	}

	hash := sha256.Sum256(data)
	return hex.EncodeToString(hash[:]), nil
}

// countEvents counts total number of events represented in the state
func countEvents(state *DerivedState) int64 {
	count := int64(0)
	for _, tableRows := range state.Tables {
		count += int64(len(tableRows))
	}
	for _, deletedSet := range state.DeletedRows {
		count += int64(len(deletedSet))
	}
	return count
}
