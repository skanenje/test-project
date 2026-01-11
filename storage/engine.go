package storage

import (
	"encoding/binary"
	"encoding/json"
	"io"
	"os"
	"path/filepath"
)

// Row represents a database row (map of column name to value)
type Row map[string]interface{}

// RowWithID pairs a row with its ID
type RowWithID struct {
	ID  int64
	Row Row
}

// Engine handles physical storage of rows on disk
//
// Format: Each row is stored as:
// [deleted_flag: 1 byte][data_length: 4 bytes][json_data: variable bytes]
//
// deleted_flag: 0 = active, 1 = deleted
// data_length: length of json_data in bytes (uint32)
// json_data: row data as JSON
type Engine struct {
	dataDir string
}

// NewEngine creates a new storage engine
func NewEngine(dataDir string) (*Engine, error) {
	if err := os.MkdirAll(dataDir, 0755); err != nil {
		return nil, err
	}
	return &Engine{dataDir: dataDir}, nil
}

// tableFile returns the file path for a table's data
func (e *Engine) tableFile(tableName string) string {
	return filepath.Join(e.dataDir, tableName+".db")
}

// InsertRow inserts a row and returns its row ID (byte offset)
func (e *Engine) InsertRow(tableName string, row Row) (int64, error) {
	filepath := e.tableFile(tableName)

	// Serialize row as JSON
	jsonData, err := json.Marshal(row)
	if err != nil {
		return 0, err
	}

	// Open file for appending
	f, err := os.OpenFile(filepath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return 0, err
	}
	defer f.Close()

	// Get current position (this is the row ID)
	rowID, err := f.Seek(0, io.SeekEnd)
	if err != nil {
		return 0, err
	}

	// Write: [deleted_flag=0][length][data]
	deletedFlag := byte(0)
	dataLength := uint32(len(jsonData))

	if err := binary.Write(f, binary.LittleEndian, deletedFlag); err != nil {
		return 0, err
	}
	if err := binary.Write(f, binary.LittleEndian, dataLength); err != nil {
		return 0, err
	}
	if _, err := f.Write(jsonData); err != nil {
		return 0, err
	}

	return rowID, nil
}

// ScanAll scans all rows in a table (skips deleted rows)
func (e *Engine) ScanAll(tableName string) ([]RowWithID, error) {
	filepath := e.tableFile(tableName)

	// If file doesn't exist, return empty
	if _, err := os.Stat(filepath); os.IsNotExist(err) {
		return []RowWithID{}, nil
	}

	f, err := os.Open(filepath)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	var rows []RowWithID

	for {
		// Get current position (row ID)
		rowID, err := f.Seek(0, io.SeekCurrent)
		if err != nil {
			return nil, err
		}

		// Read deleted flag
		var deletedFlag byte
		if err := binary.Read(f, binary.LittleEndian, &deletedFlag); err != nil {
			if err == io.EOF {
				break
			}
			return nil, err
		}

		// Read data length
		var dataLength uint32
		if err := binary.Read(f, binary.LittleEndian, &dataLength); err != nil {
			return nil, err
		}

		// Read JSON data
		jsonData := make([]byte, dataLength)
		if _, err := io.ReadFull(f, jsonData); err != nil {
			return nil, err
		}

		// Skip deleted rows
		if deletedFlag == 0 {
			var row Row
			if err := json.Unmarshal(jsonData, &row); err != nil {
				return nil, err
			}
			rows = append(rows, RowWithID{ID: rowID, Row: row})
		}
	}

	return rows, nil
}

// DeleteRow marks a row as deleted
func (e *Engine) DeleteRow(tableName string, rowID int64) error {
	filepath := e.tableFile(tableName)

	f, err := os.OpenFile(filepath, os.O_RDWR, 0644)
	if err != nil {
		return err
	}
	defer f.Close()

	// Seek to row ID and write deleted_flag = 1
	if _, err := f.Seek(rowID, io.SeekStart); err != nil {
		return err
	}

	deletedFlag := byte(1)
	return binary.Write(f, binary.LittleEndian, deletedFlag)
}

// UpdateRow updates a row by deleting old and inserting new
func (e *Engine) UpdateRow(tableName string, rowID int64, newData Row) (int64, error) {
	// Mark old as deleted
	if err := e.DeleteRow(tableName, rowID); err != nil {
		return 0, err
	}
	// Insert new version
	return e.InsertRow(tableName, newData)
}
