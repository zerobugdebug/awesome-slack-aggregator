package message

import (
	"context"
	"fmt"
	"time"

	"github.com/rs/zerolog/log"
	"github.com/slack-go/slack"

	"github.com/zerobugdebug/awesome-slack-aggregator/internal/state"
)

// Retainer handles cleanup of old messages with improved rate limiting
type Retainer struct {
	client       *slack.Client
	stateManager *state.Manager
	retention    time.Duration
	done         chan bool
}

// NewRetainer creates a new message retention manager
func NewRetainer(client *slack.Client, stateManager *state.Manager, retentionDays int) *Retainer {
	return &Retainer{
		client:       client,
		stateManager: stateManager,
		retention:    time.Duration(retentionDays) * 24 * time.Hour,
		done:         make(chan bool),
	}
}

// Start begins the periodic cleanup process
func (mr *Retainer) Start(ctx context.Context) {
	// Run retention check every 12 hours
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
func (mr *Retainer) Stop() {
	mr.done <- true
	log.Info().Msg("Message retention manager stopped")
}

// cleanupOldMessages removes messages older than the retention period in batches
func (mr *Retainer) cleanupOldMessages() {
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
