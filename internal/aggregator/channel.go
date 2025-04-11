package aggregator

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/rs/zerolog/log"
	"github.com/slack-go/slack"

	"github.com/zerobugdebug/awesome-slack-aggregator/internal/models"
)

// optimizedPollForMessages periodically checks channels for new messages with batching
func (agg *Aggregator) optimizedPollForMessages(ctx context.Context) {
	// Main polling ticker - reduced frequency to reduce API pressure
	ticker := time.NewTicker(120 * time.Second) // Increased from 2s to 5s
	defer ticker.Stop()

	// Add a delay between batch processing
	batchDelay := 1 * time.Second

	// Track the latest timestamp we've processed for each channel
	latestTimestamps := make(map[string]string)

	// Initialize from persistent storage or use current time
	now := fmt.Sprintf("%d.000000", time.Now().Add(-24*time.Hour).Unix())
	log.Debug().Str("since", now).Msg("Initializing message polling from timestamp")

	// Create a slice of channel IDs to process (only where user is a member)
	channelIDs := make([]string, 0, len(agg.channelInfo))
	for channelID, channel := range agg.channelInfo {
		// Skip DMs with the target user to avoid feedback loops
		if channel.IsIM && channel.User == agg.feedTargetUser && agg.feedTargetUser != "" && agg.feedTargetUser != "self" {
			log.Info().
				Str("channelID", channelID).
				Str("channelName", agg.getChannelDisplayName(channelID)).
				Str("targetUser", agg.feedTargetUser).
				Str("targetUserName", agg.getUserDisplayName(agg.feedTargetUser)).
				Msg("Skipping DM with target user from polling to avoid feedback loops")
			continue
		}

		channelIDs = append(channelIDs, channelID)

		// Get timestamp from persistent storage or use default
		savedTS := agg.stateManager.GetLatestTimestamp(channelID)
		if savedTS != "" {
			latestTimestamps[channelID] = savedTS
			log.Debug().
				Str("channelID", channelID).
				Str("channelName", agg.getChannelDisplayName(channelID)).
				Str("savedTimestamp", savedTS).
				Msg("Loaded timestamp from persistent storage")
		} else {
			latestTimestamps[channelID] = now
			log.Debug().
				Str("channelID", channelID).
				Str("channelName", agg.getChannelDisplayName(channelID)).
				Str("defaultTimestamp", now).
				Msg("Using default timestamp")
		}
	}

	log.Info().
		Int("channel_count", len(channelIDs)).
		Str("interval", "5s"). // Updated interval
		Dur("batchDelay", batchDelay).
		Msg("Starting optimized message polling")

	// Batch size for channel processing
	const batchSize = 1 // Process 1 channel per batch

	for {
		select {
		case <-ctx.Done():
			log.Debug().Msg("Context done, stopping message polling")
			return
		case <-ticker.C:
			// Skip if there are no channels to process
			if len(channelIDs) == 0 {
				log.Debug().Msg("No channels to process")
				continue
			}

			go func() {
				// Process channels in batches
				for i := 0; i < len(channelIDs); i += batchSize {
					// Calculate end of this batch
					end := min(i+batchSize, len(channelIDs))

					// Get batch of channel IDs
					batchChannels := channelIDs[i:end]

					log.Debug().
						Int("batchStart", i).
						Int("batchEnd", end).
						Int("batchSize", len(batchChannels)).
						Int("totalChannels", len(channelIDs)).
						Msg("Processing channel batch")

					// Create a WaitGroup to process this batch of channels
					var wg sync.WaitGroup
					wg.Add(len(batchChannels))

					// Create a time-delayed semaphore to limit concurrent requests
					// This helps distribute API calls over time
					throttle := make(chan struct{}, 1) // Only 1 concurrent request

					// Process each channel in the batch
					for _, channelID := range batchChannels {
						// Add to throttle channel
						throttle <- struct{}{}

						// Process channel in goroutine
						go func(cID string) {
							defer wg.Done()
							defer func() { <-throttle }() // Release throttle when done

							// Skip if the channel doesn't exist or is archived
							channel, exists := agg.channelInfo[cID]
							if !exists || (channel != nil && channel.IsArchived) {
								log.Debug().
									Str("channelID", cID).
									Str("channelName", agg.getChannelDisplayName(cID)).
									Msg("Skipping channel (archived or inaccessible)")
								return
							}

							// Get history since the last check
							params := &slack.GetConversationHistoryParameters{
								ChannelID: cID,
								Oldest:    latestTimestamps[cID],
								Limit:     100, // Get more messages at once
							}

							log.Trace().
								Str("channelID", cID).
								Str("channelName", agg.getChannelDisplayName(cID)).
								Str("oldest", latestTimestamps[cID]).
								Msg("Fetching conversation history")

							history, err := agg.client.GetConversationHistory(params)
							if err != nil {
								// Check if this is a rate limit error
								isRateLimit, retryAfter := IsRateLimitError(err)
								if isRateLimit {
									log.Warn().
										Str("channelID", channelID).
										Dur("retryAfter", retryAfter).
										Msg("Slack rate limit exceeded, waiting as instructed")

									// Use the retry-after value from Slack
									time.Sleep(retryAfter)
								}
								return
							}

							if len(history.Messages) > 0 {
								log.Debug().
									Str("channelID", cID).
									Str("channelName", agg.getChannelDisplayName(cID)).
									Int("message_count", len(history.Messages)).
									Msg("Found new messages")

								// Process messages (newest first)
								// Process messages and collect threads that need checking
								threadsToCheck := make(map[string]bool)

								for i := len(history.Messages) - 1; i >= 0; i-- {
									msg := history.Messages[i]

									// Skip messages created by this app
									if agg.messageFormatter.IsAppMessage(msg.Text) {
										continue
									}

									// Determine channel type
									channelType := "channel"
									isDM := false

									if channel, ok := agg.channelInfo[cID]; ok {
										if channel.IsIM {
											channelType = "direct_message"
											isDM = true
										} else if channel.IsMpIM {
											channelType = "group_dm"
											isDM = true
										} else if channel.IsPrivate {
											channelType = "private_channel"
										}
									}

									message := models.Message{
										User:        msg.User,
										Channel:     cID,
										Text:        msg.Text,
										ThreadTS:    msg.ThreadTimestamp,
										Timestamp:   msg.Timestamp,
										IsThread:    msg.ThreadTimestamp != "",
										IsDM:        isDM,
										ChannelType: channelType,
									}

									agg.addMessage(message)

									// Add to the thread tracking if it has replies
									if msg.ReplyCount > 0 {
										threadsToCheck[msg.Timestamp] = true

										// Also track this thread for future updates
										agg.trackThread(cID, msg.Timestamp)
									}

									// Track this message as recent for future thread detection
									agg.stateManager.TrackRecentMessage(cID, msg.Timestamp)
								}

								// Process all threads in a batch instead of one by one
								if len(threadsToCheck) > 0 {
									agg.batchProcessThreads(cID, threadsToCheck)
								}

								// Update latest timestamp for this channel
								// We take the timestamp of the newest message
								latestTimestamps[cID] = history.Messages[0].Timestamp

								// Save to persistent storage
								agg.stateManager.SetLatestTimestamp(cID, latestTimestamps[cID])

								log.Debug().
									Str("channelID", cID).
									Str("channelName", agg.getChannelDisplayName(cID)).
									Str("newLatestTS", latestTimestamps[cID]).
									Msg("Updated latest timestamp for channel")
							}
						}(channelID)
					}

					// Wait for all channels in this batch to be processed
					wg.Wait()

					// Add delay between batches
					select {
					case <-ctx.Done():
						return
					case <-time.After(batchDelay):
						// Continue to next batch after delay
					}
				}

				log.Debug().Msg("Completed channel polling cycle")
			}()
		}
	}
}

// batchProcessThreads processes all threads in a channel in a single operation
func (agg *Aggregator) batchProcessThreads(channelID string, threadTimestamps map[string]bool) {
	// Get channel info for determining message type
	channel, exists := agg.channelInfo[channelID]
	if !exists {
		log.Error().
			Str("channelID", channelID).
			Msg("Channel not found for batch thread processing")
		return
	}

	// Determine channel type
	channelType := "channel"
	isDM := false
	if channel.IsIM {
		channelType = "direct_message"
		isDM = true
	} else if channel.IsMpIM {
		channelType = "group_dm"
		isDM = true
	} else if channel.IsPrivate {
		channelType = "private_channel"
	}

	// Convert map keys to slice for processing
	threadsList := make([]string, 0, len(threadTimestamps))
	for ts := range threadTimestamps {
		threadsList = append(threadsList, ts)
	}

	log.Debug().
		Str("channelID", channelID).
		Str("channelName", agg.getChannelDisplayName(channelID)).
		Int("threadCount", len(threadsList)).
		Msg("Batch processing threads")

	// Process threads in small batches to avoid rate limiting
	const threadBatchSize = 5

	for i := 0; i < len(threadsList); i += threadBatchSize {
		// Calculate end of this batch
		end := i + threadBatchSize
		if end > len(threadsList) {
			end = len(threadsList)
		}

		// Get batch of thread timestamps
		batchThreads := threadsList[i:end]

		// Process each thread in the batch
		for _, threadTS := range batchThreads {
			log.Debug().
				Str("channelID", channelID).
				Str("channelName", agg.getChannelDisplayName(channelID)).
				Str("threadTS", threadTS).
				Msg("Processing thread in batch")

			// Get thread replies
			replies, hasMore, nextCursor, err := agg.client.GetConversationReplies(&slack.GetConversationRepliesParameters{
				ChannelID: channelID,
				Timestamp: threadTS,
				Limit:     100, // Get more replies at once
			})

			if err != nil {
				log.Error().
					Err(err).
					Str("channelID", channelID).
					Str("channelName", agg.getChannelDisplayName(channelID)).
					Str("threadTS", threadTS).
					Msg("Error getting thread replies")
				continue
			}

			// Process thread replies
			for _, reply := range replies {
				// Skip if it's the parent message or from self
				if reply.Timestamp == threadTS {
					continue
				}

				threadMessage := models.Message{
					User:        reply.User,
					Channel:     channelID,
					Text:        reply.Text,
					ThreadTS:    reply.ThreadTimestamp,
					Timestamp:   reply.Timestamp,
					IsThread:    true,
					IsDM:        isDM,
					ChannelType: channelType,
				}

				agg.addMessage(threadMessage)
			}

			// Handle pagination for large threads if needed
			for hasMore {
				log.Debug().
					Str("threadTS", threadTS).
					Str("channelID", channelID).
					Str("channelName", agg.getChannelDisplayName(channelID)).
					Str("cursor", nextCursor).
					Msg("Fetching more thread replies")

				// Add delay before fetching more replies to avoid rate limiting
				time.Sleep(500 * time.Millisecond)

				moreReplies, moreHasMore, nextCursorNew, err := agg.client.GetConversationReplies(&slack.GetConversationRepliesParameters{
					ChannelID: channelID,
					Timestamp: threadTS,
					Cursor:    nextCursor,
					Limit:     100,
				})

				if err != nil {
					log.Error().
						Err(err).
						Str("channelID", channelID).
						Str("channelName", agg.getChannelDisplayName(channelID)).
						Str("threadTS", threadTS).
						Str("cursor", nextCursor).
						Msg("Error getting additional thread replies")
					break
				}

				// Update for next iteration
				hasMore = moreHasMore
				nextCursor = nextCursorNew

				// Process additional replies
				for _, reply := range moreReplies {
					// Skip if it's the parent message or from self
					if reply.Timestamp == threadTS {
						continue
					}

					threadMessage := models.Message{
						User:        reply.User,
						Channel:     channelID,
						Text:        reply.Text,
						ThreadTS:    reply.ThreadTimestamp,
						Timestamp:   reply.Timestamp,
						IsThread:    true,
						IsDM:        isDM,
						ChannelType: channelType,
					}

					agg.addMessage(threadMessage)
				}
			}

			// Add a short delay between processing threads
			// to avoid hitting rate limits
			time.Sleep(200 * time.Millisecond)
		}

		// Add delay between thread batches
		if end < len(threadsList) {
			time.Sleep(500 * time.Millisecond)
		}
	}

	log.Debug().
		Str("channelID", channelID).
		Str("channelName", agg.getChannelDisplayName(channelID)).
		Int("threadsProcessed", len(threadsList)).
		Msg("Completed batch thread processing")
}

// min returns the smaller of x or y.
func min(x, y int) int {
	if x < y {
		return x
	}
	return y
}
