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

// This function is removed as it cannot accurately calculate the event count from the state.
// The event count should be tracked during state derivation and passed to the snapshot creation process.
