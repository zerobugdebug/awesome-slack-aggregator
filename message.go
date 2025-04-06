package main

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/rs/zerolog/log"
	"github.com/slack-go/slack"
)

// MessageFormatter handles formatting of messages sent by the app
type MessageFormatter struct {
	appTag string // The identifier tag for app messages
}

// NewMessageFormatter creates a new formatter with a unique ID
func NewMessageFormatter() *MessageFormatter {
	// Create a unique identifier for this instance
	uniqueID := "FpHZFpdW"

	return &MessageFormatter{
		appTag: uniqueID,
	}
}

// FormatMessage formats a message link for sending
func (mf *MessageFormatter) FormatMessage(teamDomain, channelID, timestamp, userRealName, channelName string) string {
	// Create message link
	linkTimestamp := strings.Replace(timestamp, ".", "", 1)
	messageLink := fmt.Sprintf("https://%s.slack.com/archives/%s/p%s",
		teamDomain, channelID, linkTimestamp)

	// Format with app identifier
	return fmt.Sprintf("`%s` <%s|Message> from *%s* in *#%s*",
		mf.appTag, messageLink, userRealName, channelName)
}

// GetAppTag returns the app tag for identification
func (mf *MessageFormatter) GetAppTag() string {
	return fmt.Sprintf("`%s`", mf.appTag)
}

// IsAppMessage checks if a message was created by the app
func (mf *MessageFormatter) IsAppMessage(text string) bool {
	// Look for our special comment marker
	return text != "" && (len(text) >= len(mf.appTag) && strings.Contains(text, mf.appTag))
}

// MessageRetainer handles cleanup of old messages with improved rate limiting
type MessageRetainer struct {
	client       *slack.Client
	stateManager *StateManager
	retention    time.Duration
	done         chan bool
}

// NewMessageRetainer creates a new message retention manager
func NewMessageRetainer(client *slack.Client, stateManager *StateManager, retentionDays int) *MessageRetainer {
	return &MessageRetainer{
		client:       client,
		stateManager: stateManager,
		retention:    time.Duration(retentionDays) * 24 * time.Hour,
		done:         make(chan bool),
	}
}

// Start begins the periodic cleanup process
func (mr *MessageRetainer) Start(ctx context.Context) {
	// Run retention check every 12 hours (increased from 6 hours)
	ticker := time.NewTicker(12 * time.Hour)

	go func() {
		// Wait a bit before first cleanup to allow app to initialize fully
		select {
		case <-time.After(5 * time.Minute):
			mr.cleanupOldMessages()
		case <-mr.done:
			return
		case <-ctx.Done():
			return
		}

		for {
			select {
			case <-ticker.C:
				mr.cleanupOldMessages()
			case <-mr.done:
				ticker.Stop()
				return
			case <-ctx.Done():
				ticker.Stop()
				return
			}
		}
	}()

	log.Info().
		Dur("retention", mr.retention).
		Str("checkInterval", "12h").
		Msg("Message retention manager started")
}

// Stop terminates the cleanup process
func (mr *MessageRetainer) Stop() {
	mr.done <- true
	log.Info().Msg("Message retention manager stopped")
}

// cleanupOldMessages removes messages older than the retention period in batches
func (mr *MessageRetainer) cleanupOldMessages() {
	log.Info().Msg("Starting cleanup of old messages")

	// Get messages sent by the app
	sentMessages := mr.stateManager.GetSentMessages()
	cutoffTime := time.Now().Add(-mr.retention)

	// Group by channel for batch processing
	messagesByChannel := make(map[string][]string)

	// First pass - identify old messages and organize by channel
	for messageTS, channelID := range sentMessages {
		// Convert timestamp to time
		timestampFloat := 0.0
		_, err := fmt.Sscanf(messageTS, "%f", &timestampFloat)
		if err != nil {
			log.Error().
				Err(err).
				Str("messageTS", messageTS).
				Msg("Failed to parse message timestamp")
			continue
		}

		messageTime := time.Unix(int64(timestampFloat), 0)

		// Check if message is older than retention period
		if messageTime.Before(cutoffTime) {
			// Add to channel group
			if _, exists := messagesByChannel[channelID]; !exists {
				messagesByChannel[channelID] = make([]string, 0)
			}
			messagesByChannel[channelID] = append(messagesByChannel[channelID], messageTS)
		}
	}

	// Second pass - delete messages by channel in batches
	const batchSize = 20 // Delete 20 messages at a time per channel
	totalDeleted := 0
	totalErrors := 0

	for channelID, messages := range messagesByChannel {
		log.Debug().
			Str("channelID", channelID).
			Int("messageCount", len(messages)).
			Msg("Processing channel for message deletion")

		// Sort messages by timestamp to delete oldest first
		// This isn't strictly necessary but makes the log more readable

		// Process in batches to avoid rate limiting
		for i := 0; i < len(messages); i += batchSize {
			end := i + batchSize
			if end > len(messages) {
				end = len(messages)
			}

			currentBatch := messages[i:end]
			log.Debug().
				Str("channelID", channelID).
				Int("batchStart", i).
				Int("batchEnd", end).
				Int("batchSize", len(currentBatch)).
				Msg("Processing deletion batch")

			// Delete each message in the batch with a small delay
			batchDeleted := 0
			batchErrors := 0

			for _, messageTS := range currentBatch {
				log.Debug().
					Str("messageTS", messageTS).
					Str("channelID", channelID).
					Msg("Deleting old message")

				// Delete the message
				_, _, err := mr.client.DeleteMessage(channelID, messageTS)
				if err != nil {
					log.Error().
						Err(err).
						Str("messageTS", messageTS).
						Str("channelID", channelID).
						Msg("Failed to delete message")
					batchErrors++
					totalErrors++
				} else {
					// Remove from tracking
					mr.stateManager.RemoveSentMessage(messageTS)
					batchDeleted++
					totalDeleted++
				}

				// Add a short delay between deletions
				time.Sleep(200 * time.Millisecond)
			}

			log.Debug().
				Str("channelID", channelID).
				Int("batchSize", len(currentBatch)).
				Int("deleted", batchDeleted).
				Int("errors", batchErrors).
				Msg("Batch deletion complete")

			// Add a longer delay between batches
			if end < len(messages) {
				time.Sleep(2 * time.Second)
			}
		}
	}

	log.Info().
		Int("deleted", totalDeleted).
		Int("errors", totalErrors).
		Int("total", len(sentMessages)).
		Time("cutoff", cutoffTime).
		Msg("Message cleanup completed")
}
