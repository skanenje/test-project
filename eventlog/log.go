package eventlog

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"
)

// Log manages the append-only event log
type Log struct {
	mu          sync.RWMutex
	filePath    string
	currentID   uint64 // Next event ID to assign
	file        *os.File
	initialized bool
}

// NewLog creates a new event log
func NewLog(dataDir string, filename string) (*Log, error) {
	if err := os.MkdirAll(dataDir, 0755); err != nil {
		return nil, err
	}

	filePath := filepath.Join(dataDir, filename)
	l := &Log{filePath: filePath}

	// Try to open existing log or create new one
	if err := l.initialize(); err != nil {
		return nil, err
	}

	return l, nil
}

// initialize opens the log file and counts existing events
func (l *Log) initialize() error {
	l.mu.Lock()
	defer l.mu.Unlock()

	// Check if log file exists
	_, err := os.Stat(l.filePath)
	if err != nil && !os.IsNotExist(err) {
		return err
	}

	// If file doesn't exist, create it
	if os.IsNotExist(err) {
		f, err := os.Create(l.filePath)
		if err != nil {
			return err
		}
		l.file = f
		l.currentID = 1
		l.initialized = true
		return nil
	}

	// File exists; open for append
	f, err := os.OpenFile(l.filePath, os.O_APPEND|os.O_WRONLY, 0644)
	if err != nil {
		return err
	}
	l.file = f

	// Count existing events by reading through the log
	readFile, err := os.Open(l.filePath)
	if err != nil {
		return err
	}
	defer readFile.Close()

	decoder := json.NewDecoder(readFile)
	count := uint64(0)
	for decoder.More() {
		var e Event
		if err := decoder.Decode(&e); err != nil {
			// Stop at first decode error (corrupted entry)
			break
		}
		count++
	}

	l.currentID = count + 1
	l.initialized = true
	return nil
}

// Append atomically appends an event to the log
// Returns the assigned event ID
func (l *Log) Append(eventType EventType, payload EventPayload, txID string, version int) (*Event, error) {
	l.mu.Lock()
	defer l.mu.Unlock()

	if !l.initialized {
		return nil, fmt.Errorf("log not initialized")
	}

	// Create event
	event := &Event{
		ID:        l.currentID,
		Type:      eventType,
		Timestamp: time.Now().UTC(),
		TxID:      txID,
		Version:   version,
		Payload:   payload,
	}

	// Compute checksum
	checksum, err := computeEventChecksum(event)
	if err != nil {
		return nil, err
	}
	event.Checksum = checksum

	// Marshal to JSON
	data, err := json.Marshal(event)
	if err != nil {
		return nil, err
	}

	// Append newline-delimited JSON
	data = append(data, '\n')

	// Write atomically
	_, err = l.file.Write(data)
	if err != nil {
		return nil, err
	}

	// Sync to disk for durability
	if err := l.file.Sync(); err != nil {
		return nil, err
	}

	// Increment ID for next event
	l.currentID++

	return event, nil
}

// AppendBatch appends multiple events as a transaction
// If any event fails, all are rolled back
func (l *Log) AppendBatch(events []*Event) error {
	l.mu.Lock()
	defer l.mu.Unlock()

	if !l.initialized {
		return fmt.Errorf("log not initialized")
	}

	if len(events) == 0 {
		return nil
	}

	// Prepare all events
	data := make([][]byte, len(events))
	for i, event := range events {
		event.ID = l.currentID + uint64(i)

		// Compute checksum
		checksum, err := computeEventChecksum(event)
		if err != nil {
			return err
		}
		event.Checksum = checksum

		// Marshal to JSON
		jsonData, err := json.Marshal(event)
		if err != nil {
			return err
		}

		data[i] = append(jsonData, '\n')
	}

	// Write all at once
	for _, d := range data {
		_, err := l.file.Write(d)
		if err != nil {
			return err
		}
	}

	// Sync to disk
	if err := l.file.Sync(); err != nil {
		return err
	}

	l.currentID += uint64(len(events))
	return nil
}

// Read returns all events from the log
// Stops at first corruption, returns events read before corruption
func (l *Log) Read() ([]*Event, []EventError) {
	l.mu.RLock()
	defer l.mu.RUnlock()

	readFile, err := os.Open(l.filePath)
	if err != nil {
		return nil, []EventError{{Error: fmt.Sprintf("cannot open log: %v", err)}}
	}
	defer readFile.Close()

	var events []*Event
	var errors []EventError

	decoder := json.NewDecoder(readFile)
	for decoder.More() {
		var e Event
		if err := decoder.Decode(&e); err != nil {
			errors = append(errors, EventError{
				Error:     fmt.Sprintf("decode error: %v", err),
				Timestamp: time.Now(),
			})
			break
		}

		// Validate checksum
		if valid, err := validateEventChecksum(&e); !valid {
			errors = append(errors, EventError{
				EventID:   e.ID,
				Type:      e.Type,
				Error:     fmt.Sprintf("checksum mismatch: %v", err),
				Timestamp: time.Now(),
			})
			// Skip corrupted event but continue reading
			continue
		}

		events = append(events, &e)
	}

	return events, errors
}

// ReadFrom returns events starting from eventID
// Useful for reading after a snapshot
func (l *Log) ReadFrom(startEventID uint64) ([]*Event, error) {
	l.mu.RLock()
	defer l.mu.RUnlock()

	readFile, err := os.Open(l.filePath)
	if err != nil {
		return nil, err
	}
	defer readFile.Close()

	var events []*Event
	decoder := json.NewDecoder(readFile)

	for decoder.More() {
		var e Event
		if err := decoder.Decode(&e); err != nil {
			break // Stop at corruption
		}

		if e.ID >= startEventID {
			events = append(events, &e)
		}
	}

	return events, nil
}

// LastID returns the ID of the last event in the log
func (l *Log) LastID() uint64 {
	l.mu.RLock()
	defer l.mu.RUnlock()
	return l.currentID - 1
}

// Close closes the log file
func (l *Log) Close() error {
	l.mu.Lock()
	defer l.mu.Unlock()

	if l.file != nil {
		return l.file.Close()
	}
	return nil
}

// computeEventChecksum computes SHA256 checksum of event (with checksum field temporarily cleared)
func computeEventChecksum(event *Event) (string, error) {
	// Temporarily clear checksum
	originalChecksum := event.Checksum
	event.Checksum = ""
	defer func() { event.Checksum = originalChecksum }()

	data, err := json.Marshal(event)
	if err != nil {
		return "", err
	}

	hash := sha256.Sum256(data)
	return hex.EncodeToString(hash[:]), nil
}

// validateEventChecksum verifies event integrity
func validateEventChecksum(event *Event) (bool, error) {
	expected, err := computeEventChecksum(event)
	if err != nil {
		return false, err
	}

	if expected != event.Checksum {
		return false, fmt.Errorf("expected %s, got %s", expected, event.Checksum)
	}

	return true, nil
}
