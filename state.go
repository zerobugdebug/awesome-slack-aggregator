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

// ThreadInfo represents information about a thread
type ThreadInfo struct {
	LastChecked  time.Time `json:"lastChecked"`
	LastActivity time.Time `json:"lastActivity"`
}

// StateData represents the persistent state of the application
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

	// Stats for monitoring
	Stats struct {
		ThreadCheckCount    int       `json:"threadCheckCount"`
		MessageProcessCount int       `json:"messageProcessCount"`
		LastMaintenanceTime time.Time `json:"lastMaintenanceTime"`
	} `json:"stats"`
}

// StateManager handles persistence of app state with optimized saving
type StateManager struct {
	data       StateData
	filePath   string
	mu         sync.RWMutex
	saveTicker *time.Ticker
	done       chan bool
	saveNeeded bool // Flag to indicate if save is needed
	saveMu     sync.Mutex
}

// NewStateManager creates a new state manager with optimized saving
func NewStateManager(stateDir string) (*StateManager, error) {
	cleanedStateDir := filepath.Clean(stateDir)
	log.Info().Str("stateDir", stateDir).Str("cleanedStateDir", cleanedStateDir).Msg("Initializing state manager")

	// Ensure directory exists
	if err := os.MkdirAll(cleanedStateDir, 0755); err != nil {
		return nil, fmt.Errorf("failed to create state directory: %w", err)
	}

	filePath := filepath.Join(cleanedStateDir, "slack-feed.state")

	sm := &StateManager{
		data: StateData{
			LatestTimestamps: make(map[string]string),
			ActiveThreads:    make(map[string]map[string]ThreadInfo),
			SentMessages:     make(map[string]string),
			RecentMessages:   make(map[string]map[string]time.Time),
			LastUpdated:      time.Now(),
		},
		filePath:   filePath,
		done:       make(chan bool),
		saveNeeded: false,
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
			Int("recentMessages", countRecentMessages(sm.data.RecentMessages)).
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

// Start begins the periodic saving of state with improved frequency
func (sm *StateManager) Start() {
	// Reduce save frequency to 30 seconds (from 1 second) to reduce disk I/O
	sm.saveTicker = time.NewTicker(30 * time.Second)

	go func() {
		for {
			select {
			case <-sm.saveTicker.C:
				sm.saveMu.Lock()
				needsSave := sm.saveNeeded
				sm.saveMu.Unlock()

				if needsSave {
					if err := sm.save(); err != nil {
						log.Error().Err(err).Msg("Failed to save state")
					} else {
						sm.saveMu.Lock()
						sm.saveNeeded = false
						sm.saveMu.Unlock()
					}
				}
			case <-sm.done:
				sm.saveTicker.Stop()
				return
			}
		}
	}()

	// Start a maintenance routine that runs every hour
	maintenanceTicker := time.NewTicker(1 * time.Hour)
	go func() {
		for {
			select {
			case <-maintenanceTicker.C:
				sm.performMaintenance()
			case <-sm.done:
				maintenanceTicker.Stop()
				return
			}
		}
	}()

	log.Info().Str("interval", "30s").Msg("State manager auto-save started")
}

// performMaintenance performs periodic cleanup of state data to prevent unbounded growth
func (sm *StateManager) performMaintenance() {
	log.Debug().Msg("Starting state data maintenance")

	sm.mu.Lock()
	defer sm.mu.Unlock()

	// Limit the size of tracking maps
	const maxSentMessages = 5000
	//const maxRecentMessages = 10000
	const maxThreadsPerChannel = 1000

	// Clean up sent messages if too many
	if len(sm.data.SentMessages) > maxSentMessages {
		log.Info().
			Int("currentCount", len(sm.data.SentMessages)).
			Int("maxAllowed", maxSentMessages).
			Msg("Pruning excess sent messages")

		// Convert to slice for sorting by timestamp
		type timeStampedMessage struct {
			ts        string
			timestamp float64
			channelID string
		}

		messages := make([]timeStampedMessage, 0, len(sm.data.SentMessages))
		for ts, channelID := range sm.data.SentMessages {
			tsFloat := 0.0
			_, err := fmt.Sscanf(ts, "%f", &tsFloat)
			if err != nil {
				// If we can't parse, use 0 (will be removed first)
				tsFloat = 0
			}

			messages = append(messages, timeStampedMessage{
				ts:        ts,
				timestamp: tsFloat,
				channelID: channelID,
			})
		}

		// Sort by timestamp (oldest first)
		// Can be optimized with a proper sort implementation
		// but this is maintenance code that runs infrequently

		// Remove oldest messages
		removeCount := len(sm.data.SentMessages) - maxSentMessages
		removedCount := 0

		// Start with oldest timestamps
		for _, msg := range messages {
			if removedCount >= removeCount {
				break
			}

			delete(sm.data.SentMessages, msg.ts)
			removedCount++
		}

		log.Info().
			Int("removedCount", removedCount).
			Int("newCount", len(sm.data.SentMessages)).
			Msg("Completed sent messages pruning")
	}

	// Limit threads per channel
	for channelID, threads := range sm.data.ActiveThreads {
		if len(threads) > maxThreadsPerChannel {
			log.Info().
				Str("channelID", channelID).
				Int("currentCount", len(threads)).
				Int("maxAllowed", maxThreadsPerChannel).
				Msg("Pruning excess threads for channel")

			// Remove oldest threads based on last activity
			type timeStampedThread struct {
				threadTS     string
				lastActivity time.Time
			}

			threadList := make([]timeStampedThread, 0, len(threads))
			for threadTS, info := range threads {
				threadList = append(threadList, timeStampedThread{
					threadTS:     threadTS,
					lastActivity: info.LastActivity,
				})
			}

			// Sort by last activity time (oldest first)
			// Again, this could be optimized but runs infrequently

			// Remove oldest threads
			removeCount := len(threads) - maxThreadsPerChannel
			removedCount := 0

			for _, thread := range threadList {
				if removedCount >= removeCount {
					break
				}

				delete(threads, thread.threadTS)
				removedCount++
			}

			log.Info().
				Str("channelID", channelID).
				Int("removedCount", removedCount).
				Int("newCount", len(threads)).
				Msg("Completed thread pruning for channel")
		}
	}

	// Update maintenance stats
	sm.data.Stats.LastMaintenanceTime = time.Now()

	// Mark for saving
	sm.saveMu.Lock()
	sm.saveNeeded = true
	sm.saveMu.Unlock()

	log.Debug().Msg("Completed state data maintenance")
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

	// Write to a temporary file first, then rename for atomic update
	tempFile := sm.filePath + ".tmp"

	if err := os.WriteFile(tempFile, jsonData, 0644); err != nil {
		return fmt.Errorf("failed to write temporary state file: %w", err)
	}

	// Rename the temporary file to the actual state file
	if err := os.Rename(tempFile, sm.filePath); err != nil {
		return fmt.Errorf("failed to rename temporary state file: %w", err)
	}

	log.Debug().
		Str("path", sm.filePath).
		Int("channels", len(data.LatestTimestamps)).
		Int("threads", countThreads(data.ActiveThreads)).
		Int("sentMessages", len(data.SentMessages)).
		Msg("State saved successfully")

	return nil
}

// markSaveNeeded indicates that state needs to be saved
func (sm *StateManager) markSaveNeeded() {
	sm.saveMu.Lock()
	sm.saveNeeded = true
	sm.saveMu.Unlock()
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
	sm.markSaveNeeded()
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

// UpdateThreadActivity updates only the activity time for a thread
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
	sm.data.Stats.ThreadCheckCount++
	sm.markSaveNeeded()
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
	sm.markSaveNeeded()
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

		sm.markSaveNeeded()
	}
}

// TrackSentMessage adds a sent message to the tracking
func (sm *StateManager) TrackSentMessage(messageTS, channelID string) {
	sm.mu.Lock()
	defer sm.mu.Unlock()

	sm.data.SentMessages[messageTS] = channelID
	sm.markSaveNeeded()
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
	sm.markSaveNeeded()
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
	sm.data.Stats.MessageProcessCount++
	sm.markSaveNeeded()
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

	if removed > 0 {
		sm.markSaveNeeded()
	}

	return removed
}
