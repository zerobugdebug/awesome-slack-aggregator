package main

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/rs/zerolog/log"
)

// StateData represents the persistent state of the application
type StateData struct {
	// Map of channelID -> latest timestamp
	LatestTimestamps map[string]string `json:"latestTimestamps"`

	// Map of channelID -> map of threadTS -> last check time
	ActiveThreads map[string]map[string]time.Time `json:"activeThreads"`

	// Map of timestamp -> channelID for messages sent by the app
	SentMessages map[string]string `json:"sentMessages"`

	// Last time the state was saved
	LastUpdated time.Time `json:"lastUpdated"`
}

// StateManager handles persistence of app state
type StateManager struct {
	data       StateData
	filePath   string
	mu         sync.RWMutex
	saveTicker *time.Ticker
	done       chan bool
}

// NewStateManager creates a new state manager
func NewStateManager(stateDir string) (*StateManager, error) {
	// Ensure directory exists
	if err := os.MkdirAll(stateDir, 0755); err != nil {
		return nil, fmt.Errorf("failed to create state directory: %w", err)
	}

	filePath := filepath.Join(stateDir, "feed_aggregator_state.json")

	sm := &StateManager{
		data: StateData{
			LatestTimestamps: make(map[string]string),
			ActiveThreads:    make(map[string]map[string]time.Time),
			SentMessages:     make(map[string]string),
			LastUpdated:      time.Now(),
		},
		filePath: filePath,
		done:     make(chan bool),
	}

	// Try to load existing state
	if err := sm.load(); err != nil {
		if !os.IsNotExist(err) {
			log.Warn().Err(err).Str("path", filePath).Msg("Failed to load existing state")
		} else {
			log.Info().Str("path", filePath).Msg("No existing state found, starting fresh")
		}
	} else {
		log.Info().
			Str("path", filePath).
			Int("channels", len(sm.data.LatestTimestamps)).
			Int("threads", countThreads(sm.data.ActiveThreads)).
			Int("sentMessages", len(sm.data.SentMessages)).
			Time("lastUpdated", sm.data.LastUpdated).
			Msg("Loaded existing state")
	}

	return sm, nil
}

// countThreads counts the total number of threads across all channels
func countThreads(activeThreads map[string]map[string]time.Time) int {
	count := 0
	for _, threads := range activeThreads {
		count += len(threads)
	}
	return count
}

// Start begins the periodic saving of state
func (sm *StateManager) Start() {
	sm.saveTicker = time.NewTicker(30 * time.Second)

	go func() {
		for {
			select {
			case <-sm.saveTicker.C:
				if err := sm.save(); err != nil {
					log.Error().Err(err).Msg("Failed to save state")
				}
			case <-sm.done:
				sm.saveTicker.Stop()
				return
			}
		}
	}()

	log.Info().Str("interval", "30s").Msg("State manager auto-save started")
}

// Stop stops the periodic saving and performs a final save
func (sm *StateManager) Stop() {
	sm.done <- true

	// Final save
	if err := sm.save(); err != nil {
		log.Error().Err(err).Msg("Failed to perform final state save")
	} else {
		log.Info().Msg("Final state saved successfully")
	}
}

// load reads the state from disk
func (sm *StateManager) load() error {
	sm.mu.Lock()
	defer sm.mu.Unlock()

	data, err := os.ReadFile(sm.filePath)
	if err != nil {
		return err
	}

	if err := json.Unmarshal(data, &sm.data); err != nil {
		return fmt.Errorf("failed to parse state file: %w", err)
	}

	return nil
}

// save writes the current state to disk
func (sm *StateManager) save() error {
	sm.mu.RLock()
	sm.data.LastUpdated = time.Now()
	data := sm.data
	sm.mu.RUnlock()

	jsonData, err := json.MarshalIndent(data, "", "  ")
	if err != nil {
		return fmt.Errorf("failed to marshal state: %w", err)
	}

	if err := os.WriteFile(sm.filePath, jsonData, 0644); err != nil {
		return fmt.Errorf("failed to write state file: %w", err)
	}

	log.Debug().
		Str("path", sm.filePath).
		Int("channels", len(data.LatestTimestamps)).
		Int("threads", countThreads(data.ActiveThreads)).
		Int("sentMessages", len(data.SentMessages)).
		Msg("State saved successfully")

	return nil
}

// GetLatestTimestamp returns the latest timestamp for a channel
func (sm *StateManager) GetLatestTimestamp(channelID string) string {
	sm.mu.RLock()
	defer sm.mu.RUnlock()

	return sm.data.LatestTimestamps[channelID]
}

// SetLatestTimestamp updates the latest timestamp for a channel
func (sm *StateManager) SetLatestTimestamp(channelID, timestamp string) {
	sm.mu.Lock()
	defer sm.mu.Unlock()

	sm.data.LatestTimestamps[channelID] = timestamp
}

// GetActiveThreads returns a copy of the active threads map
func (sm *StateManager) GetActiveThreads() map[string]map[string]time.Time {
	sm.mu.RLock()
	defer sm.mu.RUnlock()

	// Create a deep copy
	result := make(map[string]map[string]time.Time)
	for channelID, threads := range sm.data.ActiveThreads {
		result[channelID] = make(map[string]time.Time)
		for threadTS, lastChecked := range threads {
			result[channelID][threadTS] = lastChecked
		}
	}

	return result
}

// UpdateThreadTimestamp updates the last check time for a thread
func (sm *StateManager) UpdateThreadTimestamp(channelID, threadTS string, timestamp time.Time) {
	sm.mu.Lock()
	defer sm.mu.Unlock()

	// Initialize the inner map if needed
	if _, exists := sm.data.ActiveThreads[channelID]; !exists {
		sm.data.ActiveThreads[channelID] = make(map[string]time.Time)
	}

	// Update the timestamp
	sm.data.ActiveThreads[channelID][threadTS] = timestamp
}

// TrackSentMessage adds a sent message to the tracking
func (sm *StateManager) TrackSentMessage(messageTS, channelID string) {
	sm.mu.Lock()
	defer sm.mu.Unlock()

	sm.data.SentMessages[messageTS] = channelID
}

// GetSentMessages returns a copy of the sent messages map
func (sm *StateManager) GetSentMessages() map[string]string {
	sm.mu.RLock()
	defer sm.mu.RUnlock()

	// Create a copy
	result := make(map[string]string)
	for ts, channelID := range sm.data.SentMessages {
		result[ts] = channelID
	}

	return result
}

// RemoveSentMessage removes a message from tracking
func (sm *StateManager) RemoveSentMessage(messageTS string) {
	sm.mu.Lock()
	defer sm.mu.Unlock()

	delete(sm.data.SentMessages, messageTS)
}

// IsMessageSent checks if a message was sent by this app
func (sm *StateManager) IsMessageSent(messageTS string) bool {
	sm.mu.RLock()
	defer sm.mu.RUnlock()

	_, exists := sm.data.SentMessages[messageTS]
	return exists
}
