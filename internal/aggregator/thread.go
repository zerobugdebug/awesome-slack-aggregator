package aggregator

import (
	"context"
	"fmt"
	"math/rand"
	"sort"
	"time"

	"github.com/rs/zerolog/log"
	"github.com/slack-go/slack"

	"github.com/zerobugdebug/awesome-slack-aggregator/internal/models"
)

// batchPollForThreadUpdates periodically checks for updates to active threads in batches
func (agg *Aggregator) batchPollForThreadUpdates(ctx context.Context) {
	// Use persistent storage to initialize active threads
	agg.activeThreads = agg.stateManager.GetActiveThreads()

	// Thread polling with tiered approach
	ticker := time.NewTicker(20 * time.Second) // Increased interval from 10s to 20s
	defer ticker.Stop()

	// Define thread activity tiers for polling frequency
	veryActiveThreshold := 6 * time.Hour    // Threads with activity in the last 6 hours
	activeThreshold := 24 * time.Hour       // Threads with activity in the last 24 hours
	moderateThreshold := 3 * 24 * time.Hour // Threads with activity in the last 3 days
	lowThreshold := 7 * 24 * time.Hour      // Threads with activity in the last week

	// Define delay between batches
	batchDelay := 2 * time.Second

	// Expiry threshold
	expiryThreshold := time.Duration(agg.threadExpiryDays) * 24 * time.Hour

	log.Info().
		Str("baseInterval", "20s").
		Dur("veryActiveThreshold", veryActiveThreshold).
		Dur("activeThreshold", activeThreshold).
		Dur("moderateThreshold", moderateThreshold).
		Dur("lowThreshold", lowThreshold).
		Dur("batchDelay", batchDelay).
		Dur("expiryThreshold", expiryThreshold).
		Msg("Starting batch thread update polling with tiered frequency")

	// Track when each thread was last polled
	lastPolled := make(map[string]map[string]time.Time)

	// Batch size for thread processing
	const threadBatchSize = 10 // Increased from original single thread processing

	for {
		select {
		case <-ctx.Done():
			log.Debug().Msg("Context done, stopping thread polling")
			return
		case <-ticker.C:
			// Get a snapshot of active threads to avoid deadlocks
			activeThreadsCopy := agg.getActiveThreadsSnapshot()

			now := time.Now()

			// Instead of processing threads immediately, collect those that need updating
			type threadCheck struct {
				channelID    string
				threadTS     string
				lastActivity time.Time
			}

			threadsToCheck := make([]threadCheck, 0)
			threadsToRemove := make(map[string][]string) // Map of channelID -> slice of threadTS to remove

			// First pass - identify expired threads and threads that need checking
			for channelID, threads := range activeThreadsCopy {
				// Initialize the inner map if needed
				if _, exists := lastPolled[channelID]; !exists {
					lastPolled[channelID] = make(map[string]time.Time)
				}

				// Initialize the remove slice if needed
				if _, exists := threadsToRemove[channelID]; !exists {
					threadsToRemove[channelID] = make([]string, 0)
				}

				for threadTS, info := range threads {
					// Check if thread has expired (no activity for expiryThreshold)
					timeSinceActivity := now.Sub(info.LastActivity)
					if timeSinceActivity > expiryThreshold {
						// Mark for removal
						threadsToRemove[channelID] = append(threadsToRemove[channelID], threadTS)
						continue
					}

					// Determine polling frequency based on activity
					var pollInterval time.Duration

					if timeSinceActivity <= veryActiveThreshold {
						pollInterval = 1 * time.Minute // Very active: reduced frequency from 30s to 1min
					} else if timeSinceActivity <= activeThreshold {
						pollInterval = 5 * time.Minute // Active: reduced frequency from 3min to 5min
					} else if timeSinceActivity <= moderateThreshold {
						pollInterval = 30 * time.Minute // Moderate: increased from 15min to 30min
					} else {
						pollInterval = 2 * time.Hour // Low: increased from 1h to 2h
					}

					// Check if it's time to poll this thread
					lastThreadPoll, exists := lastPolled[channelID][threadTS]
					if !exists || now.Sub(lastThreadPoll) >= pollInterval {
						// Time to check this thread - add to our queue
						threadsToCheck = append(threadsToCheck, threadCheck{
							channelID:    channelID,
							threadTS:     threadTS,
							lastActivity: info.LastActivity,
						})

						// Update last poll time
						lastPolled[channelID][threadTS] = now
					}
				}
			}

			// Second pass - remove expired threads
			for channelID, threadsList := range threadsToRemove {
				for _, threadTS := range threadsList {
					log.Debug().
						Str("channelID", channelID).
						Str("channelName", agg.getChannelDisplayName(channelID)).
						Str("threadTS", threadTS).
						Msg("Expiring inactive thread")

					// Remove from tracking
					agg.threadMu.Lock()
					if channelThreads, exists := agg.activeThreads[channelID]; exists {
						delete(channelThreads, threadTS)
						if len(channelThreads) == 0 {
							delete(agg.activeThreads, channelID)
						}
					}
					agg.threadMu.Unlock()

					// Remove from persistent storage
					agg.stateManager.RemoveThreadTimestamp(channelID, threadTS)

					// Remove from last polled
					delete(lastPolled[channelID], threadTS)
					if len(lastPolled[channelID]) == 0 {
						delete(lastPolled, channelID)
					}
				}
			}

			// Third pass - process threads that need checking in batches
			if len(threadsToCheck) > 0 {
				go func(threads []threadCheck) {
					log.Debug().
						Int("totalThreadsToCheck", len(threads)).
						Msg("Starting batch thread check")

					// Sort threads by channel ID to optimize batching
					sort.Slice(threads, func(i, j int) bool {
						if threads[i].channelID == threads[j].channelID {
							return threads[i].threadTS < threads[j].threadTS
						}
						return threads[i].channelID < threads[j].channelID
					})

					// Group threads by channel for batch processing
					threadsByChannel := make(map[string][]models.ThreadCheckInfo)
					for _, thread := range threads {
						if _, exists := threadsByChannel[thread.channelID]; !exists {
							threadsByChannel[thread.channelID] = make([]models.ThreadCheckInfo, 0)
						}
						threadsByChannel[thread.channelID] = append(
							threadsByChannel[thread.channelID],
							models.ThreadCheckInfo{
								ThreadTS:     thread.threadTS,
								LastActivity: thread.lastActivity,
							},
						)
					}

					// Process each channel's batch
					channelCount := 0
					for channelID, channelThreads := range threadsByChannel {
						channelCount++
						log.Debug().
							Str("channelID", channelID).
							Str("channelName", agg.getChannelDisplayName(channelID)).
							Int("threadCount", len(channelThreads)).
							Int("channelNum", channelCount).
							Int("totalChannels", len(threadsByChannel)).
							Msg("Processing thread batch for channel")

						// Process this channel's threads in smaller batches
						for i := 0; i < len(channelThreads); i += threadBatchSize {
							end := i + threadBatchSize
							if end > len(channelThreads) {
								end = len(channelThreads)
							}

							currentBatch := channelThreads[i:end]
							agg.batchCheckThreadUpdates(channelID, currentBatch)

							// Add delay between batches within same channel
							if end < len(channelThreads) {
								select {
								case <-ctx.Done():
									return
								case <-time.After(500 * time.Millisecond): // Short delay between batches
								}
							}
						}

						// Add delay between checking different channels
						if channelCount < len(threadsByChannel) {
							select {
							case <-ctx.Done():
								return
							case <-time.After(batchDelay):
							}
						}
					}

					log.Debug().
						Int("totalThreadsChecked", len(threads)).
						Int("channelsProcessed", len(threadsByChannel)).
						Msg("Completed thread check cycle")
				}(threadsToCheck)
			}
		}
	}
}

func IsRateLimitError(err error) (bool, time.Duration) {
	// Check if this is a Slack rate limit error (HTTP 429)
	if rateLimitErr, ok := err.(*slack.RateLimitedError); ok {
		src := rand.NewSource(time.Now().UnixNano())
		r := rand.New(src)
		retryAfter := rateLimitErr.RetryAfter + time.Duration(r.Intn(5000))*time.Millisecond
		// Extract the retry duration from the error
		return true, retryAfter
	}
	return false, 0
}

// batchCheckThreadUpdates checks a batch of threads in a single channel for updates
func (agg *Aggregator) batchCheckThreadUpdates(channelID string, threadsToCheck []models.ThreadCheckInfo) {
	// Skip if the channel doesn't exist or is archived
	channel, exists := agg.channelInfo[channelID]
	if !exists || (channel != nil && channel.IsArchived) {
		log.Debug().
			Str("channelID", channelID).
			Str("channelName", agg.getChannelDisplayName(channelID)).
			Int("threadCount", len(threadsToCheck)).
			Msg("Skipping threads in archived or inaccessible channel")
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

	// Process each thread in the batch
	for _, threadInfo := range threadsToCheck {
		threadTS := threadInfo.ThreadTS
		lastKnownActivity := threadInfo.LastActivity

		log.Debug().
			Str("channelID", channelID).
			Str("channelName", agg.getChannelDisplayName(channelID)).
			Str("threadTS", threadTS).
			Time("lastActivity", lastKnownActivity).
			Msg("Checking for updates to thread")

		// Get the thread replies
		params := &slack.GetConversationRepliesParameters{
			ChannelID: channelID,
			Timestamp: threadTS,
			Limit:     100, // Increased limit to reduce API calls
		}

		// Only filter by time if we have a valid lastKnownActivity
		if !lastKnownActivity.IsZero() {
			params.Oldest = fmt.Sprintf("%d.000000", lastKnownActivity.Unix()) // Only get newer messages
		}

		// Adding a retry mechanism for API failures
		maxRetries := 3
		retryDelay := 500 * time.Millisecond
		var replies []slack.Message
		var hasMore bool
		var nextCursor string
		var err error

		// Try up to maxRetries times with increasing delay
		for retry := range maxRetries {
			if retry > 0 {
				log.Warn().
					Str("channelID", channelID).
					Str("channelName", agg.getChannelDisplayName(channelID)).
					Str("threadTS", threadTS).
					Int("retry", retry).
					Dur("delay", retryDelay).
					Msg("Retrying thread update API call")

				// Exponential backoff
				time.Sleep(retryDelay)
				retryDelay *= 2
			}

			replies, hasMore, nextCursor, err = agg.client.GetConversationReplies(params)

			if err == nil {
				break
			}

			isRateLimit, retryAfter := IsRateLimitError(err)

			if isRateLimit {
				log.Warn().
					Str("channelID", channelID).
					Dur("retryAfter", retryAfter).
					Msg("Slack rate limit exceeded, waiting as instructed")

				// Use the retry-after value from Slack
				time.Sleep(retryAfter)
			} else {
				// For other errors, use exponential backoff
				retryDelay := time.Duration(retry+1) * 500 * time.Millisecond
				log.Warn().
					Str("channelID", channelID).
					Dur("retryDelay", retryDelay).
					Msg("API error, retrying with backoff")
				time.Sleep(retryDelay)
			}
		}
		// Process replies (if any)
		hasNewActivity := false
		newestActivity := lastKnownActivity
		processedCount := 0

		for _, reply := range replies {
			// Skip the parent message and self messages
			if reply.Timestamp == threadTS {
				continue
			}

			// Convert timestamp to time
			timestampFloat := 0.0
			_, err := fmt.Sscanf(reply.Timestamp, "%f", &timestampFloat)
			if err != nil {
				log.Error().
					Err(err).
					Str("timestamp", reply.Timestamp).
					Msg("Failed to parse timestamp")
				continue
			}

			replyTime := time.Unix(int64(timestampFloat), 0)
			if replyTime.After(lastKnownActivity) {
				hasNewActivity = true
				if replyTime.After(newestActivity) {
					newestActivity = replyTime
				}

				// Process the new reply
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

				// Add to message feed
				agg.tryAddUniqueMessage(threadMessage)
				processedCount++
			}
		}

		// Handle pagination for large threads if needed with batching
		// Define delay between pagination requests - increased to reduce rate limits
		paginationDelay := 300 * time.Millisecond
		paginationCount := 0

		for hasMore {
			// Add delay before fetching more replies
			time.Sleep(paginationDelay)
			paginationCount++

			log.Debug().
				Str("threadTS", threadTS).
				Str("channelID", channelID).
				Str("channelName", agg.getChannelDisplayName(channelID)).
				Str("cursor", nextCursor).
				Int("paginationCount", paginationCount).
				Msg("Fetching more thread replies")

			params := &slack.GetConversationRepliesParameters{
				ChannelID: channelID,
				Timestamp: threadTS,
				Cursor:    nextCursor,
				Limit:     100, // Increased limit
			}

			// Only filter by time if we have a valid lastKnownActivity
			if !lastKnownActivity.IsZero() {
				params.Oldest = fmt.Sprintf("%d.000000", lastKnownActivity.Unix()) // Only get newer messages
			}

			// Add retry logic for pagination as well
			var moreReplies []slack.Message
			var moreHasMore bool
			var nextCursorNew string
			var paginationErr error

			for retry := range maxRetries {
				if retry > 0 {
					log.Warn().
						Str("channelID", channelID).
						Str("channelName", agg.getChannelDisplayName(channelID)).
						Str("threadTS", threadTS).
						Str("cursor", nextCursor).
						Int("retry", retry).
						Dur("delay", retryDelay).
						Msg("Retrying pagination API call")

					// Exponential backoff
					time.Sleep(retryDelay)
					retryDelay *= 2
				}

				moreReplies, moreHasMore, nextCursorNew, paginationErr = agg.client.GetConversationReplies(params)

				if paginationErr == nil {
					break
				}
			}

			if paginationErr != nil {
				log.Error().
					Err(paginationErr).
					Str("channelID", channelID).
					Str("channelName", agg.getChannelDisplayName(channelID)).
					Str("threadTS", threadTS).
					Str("cursor", nextCursor).
					Msg("Error getting additional thread replies after retries")
				break
			}

			nextCursor = nextCursorNew

			// Process additional replies
			for _, reply := range moreReplies {
				// Skip if it's the parent message or from self
				if reply.Timestamp == threadTS {
					continue
				}

				// Convert timestamp to time
				timestampFloat := 0.0
				_, err := fmt.Sscanf(reply.Timestamp, "%f", &timestampFloat)
				if err != nil {
					log.Error().
						Err(err).
						Str("timestamp", reply.Timestamp).
						Msg("Failed to parse timestamp")
					continue
				}

				replyTime := time.Unix(int64(timestampFloat), 0)
				if replyTime.After(lastKnownActivity) {
					hasNewActivity = true
					if replyTime.After(newestActivity) {
						newestActivity = replyTime
					}

					// Process the new reply
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

					// Add to message feed
					agg.tryAddUniqueMessage(threadMessage)
					processedCount++
				}
			}

			hasMore = moreHasMore
		}

		// Update thread activity time if there was new activity
		if hasNewActivity {
			agg.threadMu.Lock()
			if channelThreads, exists := agg.activeThreads[channelID]; exists {
				if info, exists := channelThreads[threadTS]; exists {
					info.LastActivity = newestActivity
					channelThreads[threadTS] = info
				}
			}
			agg.threadMu.Unlock()

			// Update in persistent storage
			agg.stateManager.UpdateThreadActivity(channelID, threadTS, newestActivity)

			log.Debug().
				Str("channelID", channelID).
				Str("channelName", agg.getChannelDisplayName(channelID)).
				Str("threadTS", threadTS).
				Time("newLastActivity", newestActivity).
				Int("newMessagesProcessed", processedCount).
				Msg("Updated thread activity timestamp")
		}

		// Always update the checked time
		now := time.Now()
		agg.threadMu.Lock()
		if channelThreads, exists := agg.activeThreads[channelID]; exists {
			if info, exists := channelThreads[threadTS]; exists {
				info.LastChecked = now
				channelThreads[threadTS] = info
			}
		}
		agg.threadMu.Unlock()

		// Update the last check time in persistent storage
		agg.stateManager.UpdateThreadTimestamp(channelID, threadTS, now)
	}
}

// batchPollForNewThreads periodically checks for new threads on recent messages in batches
func (agg *Aggregator) batchPollForNewThreads(ctx context.Context) {
	// How far back to track recent messages for possible thread creation
	messageTrackingPeriod := 24 * time.Hour // Track messages for 24 hours

	// Increase polling interval to reduce API pressure
	ticker := time.NewTicker(3 * time.Minute) // Increased from 1 minute to 3 minutes
	defer ticker.Stop()

	// Define delay between batches
	batchDelay := 2 * time.Second

	// Define batch size for message checks
	const messageBatchSize = 30 // Process 30 messages at a time

	log.Info().
		Dur("trackingPeriod", messageTrackingPeriod).
		Dur("batchDelay", batchDelay).
		Int("batchSize", messageBatchSize).
		Str("checkInterval", "3m").
		Msg("Starting batched recent message thread detection")

	for {
		select {
		case <-ctx.Done():
			log.Debug().Msg("Context done, stopping recent message polling")
			return
		case <-ticker.C:
			// First, clean up old messages from tracking
			removed := agg.stateManager.CleanupOldRecentMessages(messageTrackingPeriod)
			if removed > 0 {
				log.Debug().Int("removedCount", removed).Msg("Removed old messages from recent tracking")
			}

			// Get a snapshot of recent messages
			recentMessages := agg.stateManager.GetRecentMessages()

			// Collect messages that need checking
			type checkInfo struct {
				channelID string
				messageTS string
			}
			messagesToCheck := make([]checkInfo, 0)

			// Identify which messages need checking
			for channelID, messages := range recentMessages {
				// Skip if the channel doesn't exist or is archived
				channel, exists := agg.channelInfo[channelID]
				if !exists || (channel != nil && channel.IsArchived) {
					continue
				}

				for messageTS := range messages {
					// Check if this message is already being tracked as a thread
					isTrackedThread := false
					agg.threadMu.Lock()
					if threads, exists := agg.activeThreads[channelID]; exists {
						_, isTrackedThread = threads[messageTS]
					}
					agg.threadMu.Unlock()

					if !isTrackedThread {
						messagesToCheck = append(messagesToCheck, checkInfo{
							channelID: channelID,
							messageTS: messageTS,
						})
					}
				}
			}

			// Process messages in batches
			if len(messagesToCheck) > 0 {
				go func(messages []checkInfo) {
					log.Debug().
						Int("messagesToCheck", len(messages)).
						Msg("Starting batch check for new threads")

					// Group messages by channel for more efficient processing
					messagesByChannel := make(map[string][]string)
					for _, msg := range messages {
						if _, exists := messagesByChannel[msg.channelID]; !exists {
							messagesByChannel[msg.channelID] = make([]string, 0)
						}
						messagesByChannel[msg.channelID] = append(messagesByChannel[msg.channelID], msg.messageTS)
					}

					checkedCount := 0
					newThreadsFound := 0
					channelCount := 0

					// Process each channel's messages
					for channelID, messageTSList := range messagesByChannel {
						channelCount++

						// Skip channels that don't exist or are archived
						channel, exists := agg.channelInfo[channelID]
						if !exists || (channel != nil && channel.IsArchived) {
							continue
						}

						log.Debug().
							Str("channelID", channelID).
							Str("channelName", agg.getChannelDisplayName(channelID)).
							Int("messageCount", len(messageTSList)).
							Int("channelNum", channelCount).
							Int("totalChannels", len(messagesByChannel)).
							Msg("Checking messages in channel for new threads")

						// Process in smaller batches to avoid rate limiting
						for i := 0; i < len(messageTSList); i += messageBatchSize {
							end := i + messageBatchSize
							if end > len(messageTSList) {
								end = len(messageTSList)
							}

							currentBatch := messageTSList[i:end]

							// For each batch, we'll need to make separate API calls
							// but we can optimize by processing each result together
							threadsFound := make(map[string]bool)

							for _, messageTS := range currentBatch {
								// Use GetConversationHistory with inclusive oldest/latest to get just this message
								history, err := agg.client.GetConversationHistory(&slack.GetConversationHistoryParameters{
									ChannelID: channelID,
									Oldest:    messageTS,
									Latest:    messageTS,
									Inclusive: true,
									Limit:     1,
								})

								checkedCount++

								if err != nil {
									isRateLimit, retryAfter := IsRateLimitError(err)
									if isRateLimit {
										log.Warn().
											Str("channelID", channelID).
											Str("messageTS", messageTS).
											Dur("retryAfter", retryAfter).
											Msg("Slack rate limit exceeded while checking message for thread activity")
										// Use the retry-after value from Slack
										time.Sleep(retryAfter)
									}
								} else if len(history.Messages) > 0 && history.Messages[0].ReplyCount > 0 {
									log.Info().
										Str("channelID", channelID).
										Str("channelName", agg.getChannelDisplayName(channelID)).
										Str("messageTS", messageTS).
										Int("replyCount", history.Messages[0].ReplyCount).
										Msg("Found new thread on recent message")

									// Add this to thread tracking
									agg.trackThread(channelID, messageTS)
									threadsFound[messageTS] = true
									newThreadsFound++
								}

								// Add a small delay between API calls to avoid rate limiting
								time.Sleep(100 * time.Millisecond)
							}

							// Process all discovered threads as a batch
							if len(threadsFound) > 0 {
								agg.batchProcessThreads(channelID, threadsFound)
							}

							// Add delay between batches within this channel
							if end < len(messageTSList) {
								time.Sleep(500 * time.Millisecond)
							}
						}

						// Add delay between channels
						if channelCount < len(messagesByChannel) {
							time.Sleep(batchDelay)
						}
					}

					log.Debug().
						Int("checkedMessages", checkedCount).
						Int("newThreadsFound", newThreadsFound).
						Int("channelsProcessed", channelCount).
						Msg("Completed batch check for new threads")
				}(messagesToCheck)
			}
		}
	}
}
