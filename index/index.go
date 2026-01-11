package index

import (
	"fmt"
	"rdbms/storage"
)

// Index is a hash-based index for fast lookups
type Index struct {
	Column string              // indexed column name
	Data   map[string][]int64  // value -> [row_ids]
}

// New creates a new index
func New(column string) *Index {
	return &Index{
		Column: column,
		Data:   make(map[string][]int64),
	}
}

// Add adds a row to the index
func (idx *Index) Add(value interface{}, rowID int64) {
	key := fmt.Sprintf("%v", value)
	idx.Data[key] = append(idx.Data[key], rowID)
}

// Remove removes a row from the index
func (idx *Index) Remove(value interface{}, rowID int64) {
	key := fmt.Sprintf("%v", value)
	if ids, found := idx.Data[key]; found {
		newIDs := []int64{}
		for _, id := range ids {
			if id != rowID {
				newIDs = append(newIDs, id)
			}
		}
		if len(newIDs) > 0 {
			idx.Data[key] = newIDs
		} else {
			delete(idx.Data, key)
		}
	}
}

// Lookup finds row IDs for a value
func (idx *Index) Lookup(value interface{}) ([]int64, bool) {
	key := fmt.Sprintf("%v", value)
	ids, found := idx.Data[key]
	return ids, found
}

// Exists checks if a value exists in the index
func (idx *Index) Exists(value interface{}) bool {
	key := fmt.Sprintf("%v", value)
	_, found := idx.Data[key]
	return found
}

// Rebuild rebuilds the index from scratch
func (idx *Index) Rebuild(rows []storage.RowWithID) {
	idx.Data = make(map[string][]int64)
	for _, r := range rows {
		if val, exists := r.Row[idx.Column]; exists {
			idx.Add(val, r.ID)
		}
	}
}
