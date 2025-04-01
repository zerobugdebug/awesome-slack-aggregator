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
type ThreadInfo struct {
	LastChecked  time.Time `json:"lastChecked"`
	LastActivity time.Time `json:"lastActivity"`
}

type StateData struct {
	// Map of channelID -> latest timestamp
	LatestTimestamps map[string]string `json:"latestTimestamps"`

	// Map of channelID -> map of threadTS -> thread info
	ActiveThreads map[string]map[string]ThreadInfo `json:"activeThreads"`

	// Map of timestamp -> channelID for messages sent by the app
	SentMessages map[string]string `json:"sentMessages"`

	// Map of channelID -> map of message timestamp -> discovery time
	// Used to track recent messages that might get threads later
	RecentMessages map[string]map[string]time.Time `json:"recentMessages"`

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

	cleanedStateDir := filepath.Clean(stateDir)
	log.Info().Str("stateDir", stateDir).Str("cleanedStateDir", cleanedStateDir).Msg("Folders: ")
	// Ensure directory exists
	if err := os.MkdirAll(cleanedStateDir, 0755); err != nil {
		return nil, fmt.Errorf("failed to create state directory: %w", err)
	}

	filePath := filepath.Join(cleanedStateDir, "slack-feed.state")

	sm := &StateManager{
		data: StateData{
			LatestTimestamps: make(map[string]string),
			ActiveThreads:    make(map[string]map[string]ThreadInfo), // Changed from time.Time to ThreadInfo
			SentMessages:     make(map[string]string),
			RecentMessages:   make(map[string]map[string]time.Time), // Added RecentMessages
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
			Int("threads", countThreads(sm.data.ActiveThreads)). // Changed to use countThreadsInfo
			Int("sentMessages", len(sm.data.SentMessages)).
			Int("recentMessages", countRecentMessages(sm.data.RecentMessages)). // Added recentMessages count
			Time("lastUpdated", sm.data.LastUpdated).
			Msg("Loaded existing state")
	}

	return sm, nil
}

// countThreads counts the total number of ThreadInfo objects across all channels
func countThreads(activeThreads map[string]map[string]ThreadInfo) int {
	count := 0
	for _, threads := range activeThreads {
		count += len(threads)
	}
	return count
}

// countRecentMessages counts the total number of recent messages across all channels
func countRecentMessages(recentMessages map[string]map[string]time.Time) int {
	count := 0
	for _, messages := range recentMessages {
		count += len(messages)
	}
	return count
}

// Start begins the periodic saving of state
func (sm *StateManager) Start() {
	sm.saveTicker = time.NewTicker(1 * time.Second)

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
func (sm *StateManager) GetActiveThreads() map[string]map[string]ThreadInfo {
	sm.mu.RLock()
	defer sm.mu.RUnlock()

	// Create a deep copy
	result := make(map[string]map[string]ThreadInfo)
	for channelID, threads := range sm.data.ActiveThreads {
		result[channelID] = make(map[string]ThreadInfo)
		for threadTS, info := range threads {
			result[channelID][threadTS] = info
		}
	}

	return result
}

// Add method to update activity time without changing check time
func (sm *StateManager) UpdateThreadActivity(channelID, threadTS string, activityTime time.Time) {
	sm.mu.Lock()
	defer sm.mu.Unlock()

	// Initialize the inner map if needed
	if _, exists := sm.data.ActiveThreads[channelID]; !exists {
		sm.data.ActiveThreads[channelID] = make(map[string]ThreadInfo)
	}

	// Get existing info or create new
	info, exists := sm.data.ActiveThreads[channelID][threadTS]
	if !exists {
		info = ThreadInfo{
			LastChecked:  activityTime,
			LastActivity: activityTime,
		}
	} else {
		info.LastActivity = activityTime
	}

	// Update the thread info
	sm.data.ActiveThreads[channelID][threadTS] = info
}

// UpdateThreadTimestamp updates the last check time for a thread
func (sm *StateManager) UpdateThreadTimestamp(channelID, threadTS string, checkTime time.Time) {
	sm.mu.Lock()
	defer sm.mu.Unlock()

	// Initialize the inner map if needed
	if _, exists := sm.data.ActiveThreads[channelID]; !exists {
		sm.data.ActiveThreads[channelID] = make(map[string]ThreadInfo)
	}

	// Get existing info or create new
	info, exists := sm.data.ActiveThreads[channelID][threadTS]
	if !exists {
		info = ThreadInfo{
			LastChecked:  checkTime,
			LastActivity: checkTime,
		}
	} else {
		info.LastChecked = checkTime
	}

	// Update the timestamp
	sm.data.ActiveThreads[channelID][threadTS] = info
}

// RemoveThreadTimestamp removes a thread from tracking
func (sm *StateManager) RemoveThreadTimestamp(channelID, threadTS string) {
	sm.mu.Lock()
	defer sm.mu.Unlock()

	if threads, exists := sm.data.ActiveThreads[channelID]; exists {
		delete(threads, threadTS)

		// If no more threads in channel, remove channel entry
		if len(threads) == 0 {
			delete(sm.data.ActiveThreads, channelID)
		}
	}
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

// TrackRecentMessage adds a message to the recent messages tracking
func (sm *StateManager) TrackRecentMessage(channelID, messageTS string) {
	sm.mu.Lock()
	defer sm.mu.Unlock()

	// Initialize the inner map if needed
	if _, exists := sm.data.RecentMessages[channelID]; !exists {
		sm.data.RecentMessages[channelID] = make(map[string]time.Time)
	}

	// Add the message with current time
	sm.data.RecentMessages[channelID][messageTS] = time.Now()
}

// GetRecentMessages returns a copy of recent messages map
func (sm *StateManager) GetRecentMessages() map[string]map[string]time.Time {
	sm.mu.RLock()
	defer sm.mu.RUnlock()

	// Create a deep copy
	result := make(map[string]map[string]time.Time)
	for channelID, messages := range sm.data.RecentMessages {
		result[channelID] = make(map[string]time.Time)
		for messageTS, discoveryTime := range messages {
			result[channelID][messageTS] = discoveryTime
		}
	}

	return result
}

// CleanupOldRecentMessages removes messages older than the retention period
func (sm *StateManager) CleanupOldRecentMessages(retentionPeriod time.Duration) int {
	sm.mu.Lock()
	defer sm.mu.Unlock()

	removed := 0
	now := time.Now()
	cutoff := now.Add(-retentionPeriod)

	for channelID, messages := range sm.data.RecentMessages {
		for messageTS, discoveryTime := range messages {
			if discoveryTime.Before(cutoff) {
				delete(messages, messageTS)
				removed++
			}
		}

		// Remove empty channel entries
		if len(messages) == 0 {
			delete(sm.data.RecentMessages, channelID)
		}
	}

	return removed
}
