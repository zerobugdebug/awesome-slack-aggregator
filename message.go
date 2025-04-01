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
	uniqueID := fmt.Sprintf("feed-%d", time.Now().Unix())

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
	return fmt.Sprintf("[🤖] <%s|Message> from *%s* in *#%s* <!-- app-msg:%s -->",
		messageLink, userRealName, channelName, mf.appTag)
}

// IsAppMessage checks if a message was created by the app
func (mf *MessageFormatter) IsAppMessage(text string) bool {
	// Look for our special comment marker
	return text != "" && (len(text) >= 12 && text[:12] == "[🤖]")
}

// MessageRetainer handles cleanup of old messages
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
	// Run retention check every 6 hours
	ticker := time.NewTicker(6 * time.Hour)

	go func() {
		// Run immediate check on startup
		mr.cleanupOldMessages()

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
		Str("checkInterval", "6h").
		Msg("Message retention manager started")
}

// Stop terminates the cleanup process
func (mr *MessageRetainer) Stop() {
	mr.done <- true
	log.Info().Msg("Message retention manager stopped")
}

// cleanupOldMessages removes messages older than the retention period
func (mr *MessageRetainer) cleanupOldMessages() {
	log.Info().Msg("Starting cleanup of old messages")

	// Get messages sent by the app
	sentMessages := mr.stateManager.GetSentMessages()
	cutoffTime := time.Now().Add(-mr.retention)

	deleteCount := 0
	errorCount := 0

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
			log.Debug().
				Str("messageTS", messageTS).
				Str("channelID", channelID).
				Time("messageTime", messageTime).
				Time("cutoff", cutoffTime).
				Msg("Deleting old message")

			// Delete the message
			_, _, err := mr.client.DeleteMessage(channelID, messageTS)
			if err != nil {
				log.Error().
					Err(err).
					Str("messageTS", messageTS).
					Str("channelID", channelID).
					Msg("Failed to delete message")
				errorCount++
			} else {
				// Remove from tracking
				mr.stateManager.RemoveSentMessage(messageTS)
				deleteCount++
			}
		}
	}

	log.Info().
		Int("deleted", deleteCount).
		Int("errors", errorCount).
		Int("total", len(sentMessages)).
		Time("cutoff", cutoffTime).
		Msg("Message cleanup completed")
}
