package main

import (
	"context"
	"fmt"
	"sort"
	"sync"
	"time"

	"github.com/rs/zerolog/log"
	"github.com/slack-go/slack"
)

// ThreadCheckRequest represents a request to check a thread
type ThreadCheckRequest struct {
	ChannelID    string
	ThreadTS     string
	LastActivity time.Time
	Priority     int // 1 = high, 2 = medium, 3 = low
}

// BatchCheckRequest represents a request to check multiple messages for threads
type BatchCheckRequest struct {
	ChannelID  string
	MessageTSs []string
}

// ThreadProcessor is an interface for components that can process messages
type ThreadProcessor interface {
	addMessage(msg Message)
	getChannelDisplayName(channelID string) string
	getUserDisplayName(userID string) string
}

// ThreadManager handles all thread-related operations with rate limiting
type ThreadManager struct {
	client           *slack.Client
	threadCheckQueue chan ThreadCheckRequest
	batchCheckQueue  chan BatchCheckRequest
	activeThreads    map[string]map[string]ThreadInfo
	channelInfo      map[string]*slack.Channel
	userInfo         map[string]*slack.User
	stateManager     *StateManager
	userID           string
	processor        ThreadProcessor // Interface for forwarding messages to FeedAggregator
	threadMu         sync.RWMutex
	requestDelay     time.Duration
	maxRetries       int
	threadExpiryDays int
}

// NewThreadManager creates a new thread manager
func NewThreadManager(
	client *slack.Client,
	channelInfo map[string]*slack.Channel,
	userInfo map[string]*slack.User,
	stateManager *StateManager,
	userID string,
	processor ThreadProcessor,
	threadExpiryDays int,
) *ThreadManager {
	return &ThreadManager{
		client:           client,
		threadCheckQueue: make(chan ThreadCheckRequest, 1000),
		batchCheckQueue:  make(chan BatchCheckRequest, 100),
		activeThreads:    make(map[string]map[string]ThreadInfo),
		channelInfo:      channelInfo,
		userInfo:         userInfo,
		stateManager:     stateManager,
		userID:           userID,
		processor:        processor,
		requestDelay:     300 * time.Millisecond, // Default request delay
		maxRetries:       3,                      // Default max retries
		threadExpiryDays: threadExpiryDays,
	}
}

// Start begins the thread manager processing
func (tm *ThreadManager) Start(ctx context.Context) {
	// Load active threads from state manager
	tm.activeThreads = tm.stateManager.GetActiveThreads()

	// Start thread check worker
	go tm.processThreadChecks(ctx)

	// Start batch check worker
	go tm.processBatchChecks(ctx)

	// Start thread polling
	go tm.pollForThreadUpdates(ctx)

	log.Info().
		Str("requestDelay", tm.requestDelay.String()).
		Int("maxRetries", tm.maxRetries).
		Int("threadExpiryDays", tm.threadExpiryDays).
		Msg("Thread manager started")
}

// QueueThreadCheck adds a thread to the check queue with a priority
func (tm *ThreadManager) QueueThreadCheck(channelID, threadTS string, lastActivity time.Time, priority int) {
	select {
	case tm.threadCheckQueue <- ThreadCheckRequest{
		ChannelID:    channelID,
		ThreadTS:     threadTS,
		LastActivity: lastActivity,
		Priority:     priority,
	}:
		log.Debug().
			Str("channelID", channelID).
			Str("channelName", tm.getChannelDisplayName(channelID)).
			Str("threadTS", threadTS).
			Int("priority", priority).
			Msg("Thread check request queued")
	default:
		log.Warn().
			Str("channelID", channelID).
			Str("channelName", tm.getChannelDisplayName(channelID)).
			Str("threadTS", threadTS).
			Msg("Thread check queue full, request dropped")
	}
}

// QueueBatchCheck adds a batch of messages to check for threads
func (tm *ThreadManager) QueueBatchCheck(channelID string, messageTSs []string) {
	if len(messageTSs) == 0 {
		return
	}

	select {
	case tm.batchCheckQueue <- BatchCheckRequest{
		ChannelID:  channelID,
		MessageTSs: messageTSs,
	}:
		log.Debug().
			Str("channelID", channelID).
			Str("channelName", tm.getChannelDisplayName(channelID)).
			Int("messageCount", len(messageTSs)).
			Msg("Batch check request queued")
	default:
		log.Warn().
			Str("channelID", channelID).
			Str("channelName", tm.getChannelDisplayName(channelID)).
			Int("messageCount", len(messageTSs)).
			Msg("Batch check queue full, request dropped")
	}
}

// processThreadChecks handles the thread check queue with rate limiting
func (tm *ThreadManager) processThreadChecks(ctx context.Context) {
	// Use ticker for rate limiting
	rateLimiter := time.NewTicker(tm.requestDelay)
	defer rateLimiter.Stop()

	log.Info().
		Str("delay", tm.requestDelay.String()).
		Msg("Starting thread check processor")

	for {
		select {
		case <-ctx.Done():
			log.Debug().Msg("Context done, stopping thread check processor")
			return
		case req := <-tm.threadCheckQueue:
			// Wait for rate limiter before processing
			<-rateLimiter.C
			tm.checkThreadUpdates(req.ChannelID, req.ThreadTS, req.LastActivity)
		}
	}
}

// processBatchChecks handles the batch check queue with rate limiting
func (tm *ThreadManager) processBatchChecks(ctx context.Context) {
	// Use ticker for rate limiting
	rateLimiter := time.NewTicker(tm.requestDelay)
	defer rateLimiter.Stop()

	log.Info().
		Str("delay", tm.requestDelay.String()).
		Msg("Starting batch check processor")

	for {
		select {
		case <-ctx.Done():
			log.Debug().Msg("Context done, stopping batch check processor")
			return
		case req := <-tm.batchCheckQueue:
			// Wait for rate limiter before processing
			<-rateLimiter.C
			tm.batchCheckForThreads(req.ChannelID, req.MessageTSs)
		}
	}
}

// batchCheckForThreads checks multiple messages at once for thread activity
func (tm *ThreadManager) batchCheckForThreads(channelID string, messageTSs []string) {
	if len(messageTSs) == 0 {
		return
	}

	// Skip if the channel doesn't exist or is archived
	channel, exists := tm.channelInfo[channelID]
	if !exists || (channel != nil && channel.IsArchived) {
		log.Debug().
			Str("channelID", channelID).
			Str("channelName", tm.getChannelDisplayName(channelID)).
			Int("messageCount", len(messageTSs)).
			Msg("Skipping batch check for archived or inaccessible channel")
		return
	}

	// Sort timestamps
	sort.Strings(messageTSs)
	oldest := messageTSs[0]
	latest := messageTSs[len(messageTSs)-1]

	log.Debug().
		Str("channelID", channelID).
		Str("channelName", tm.getChannelDisplayName(channelID)).
		Int("messageCount", len(messageTSs)).
		Str("oldest", oldest).
		Str("latest", latest).
		Msg("Batch checking messages for thread activity")

	// Create a map of message timestamps for quick lookup
	messageMap := make(map[string]bool)
	for _, ts := range messageTSs {
		messageMap[ts] = true
	}

	// Get conversation history for the time range
	history, err := tm.client.GetConversationHistory(&slack.GetConversationHistoryParameters{
		ChannelID: channelID,
		Oldest:    oldest,
		Latest:    latest,
		Inclusive: true,
		Limit:     100, // Adjust based on expected message volume
	})

	if err != nil {
		log.Error().
			Err(err).
			Str("channelID", channelID).
			Str("channelName", tm.getChannelDisplayName(channelID)).
			Msg("Error getting batch history")
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

	// Process messages that have thread replies
	threadsFound := 0
	for _, msg := range history.Messages {
		// Only process messages we're interested in
		if !messageMap[msg.Timestamp] {
			continue
		}

		// Check if this message has thread replies
		if msg.ReplyCount > 0 {
			log.Debug().
				Str("channelID", channelID).
				Str("channelName", tm.getChannelDisplayName(channelID)).
				Str("messageTS", msg.Timestamp).
				Int("replyCount", msg.ReplyCount).
				Msg("Found thread in batch check")

			// Track this thread
			tm.trackThread(channelID, msg.Timestamp)

			// If the message has thread replies, fetch the entire thread
			replies, hasMore, nextCursor, err := tm.client.GetConversationReplies(&slack.GetConversationRepliesParameters{
				ChannelID: channelID,
				Timestamp: msg.Timestamp,
				Limit:     100,
			})

			if err != nil {
				log.Error().
					Err(err).
					Str("channelID", channelID).
					Str("channelName", tm.getChannelDisplayName(channelID)).
					Str("messageTS", msg.Timestamp).
					Msg("Error getting thread replies in batch check")
				continue
			}

			// Process all thread replies
			for _, reply := range replies {
				// Skip the parent message and self messages
				if reply.Timestamp == msg.Timestamp || reply.User == tm.userID {
					continue
				}

				// Add to message feed
				threadMessage := Message{
					User:        reply.User,
					Channel:     channelID,
					Text:        reply.Text,
					ThreadTS:    reply.ThreadTimestamp,
					Timestamp:   reply.Timestamp,
					IsThread:    true,
					IsDM:        isDM,
					ChannelType: channelType,
				}

				// Send the message to the processor (FeedAggregator)
				tm.processor.addMessage(threadMessage)
			}

			// Handle pagination for large threads if needed
			for hasMore {
				// Add delay before fetching more replies
				time.Sleep(tm.requestDelay)

				log.Debug().
					Str("messageTS", msg.Timestamp).
					Str("channelID", channelID).
					Str("channelName", tm.getChannelDisplayName(channelID)).
					Str("cursor", nextCursor).
					Msg("Fetching more thread replies in batch check")

				moreReplies, moreHasMore, nextCursorNew, err := tm.client.GetConversationReplies(&slack.GetConversationRepliesParameters{
					ChannelID: channelID,
					Timestamp: msg.Timestamp,
					Cursor:    nextCursor,
				})

				if err != nil {
					log.Error().
						Err(err).
						Str("channelID", channelID).
						Str("channelName", tm.getChannelDisplayName(channelID)).
						Str("messageTS", msg.Timestamp).
						Str("cursor", nextCursor).
						Msg("Error getting additional thread replies in batch check")
					break
				}

				nextCursor = nextCursorNew

				// Process additional replies
				for _, reply := range moreReplies {
					// Skip if it's the parent message or from self
					if reply.Timestamp == msg.Timestamp || reply.User == tm.userID {
						continue
					}

					threadMessage := Message{
						User:        reply.User,
						Channel:     channelID,
						Text:        reply.Text,
						ThreadTS:    reply.ThreadTimestamp,
						Timestamp:   reply.Timestamp,
						IsThread:    true,
						IsDM:        isDM,
						ChannelType: channelType,
					}

					// Send the message to the processor (FeedAggregator)
					tm.processor.addMessage(threadMessage)
				}

				hasMore = moreHasMore
			}

			threadsFound++
		}
	}

	log.Debug().
		Str("channelID", channelID).
		Str("channelName", tm.getChannelDisplayName(channelID)).
		Int("checkedCount", len(history.Messages)).
		Int("threadsFound", threadsFound).
		Msg("Completed batch check for threads")
}

// checkThreadUpdates checks a specific thread for updates
func (tm *ThreadManager) checkThreadUpdates(channelID, threadTS string, lastKnownActivity time.Time) {
	// Skip if the channel doesn't exist or is archived
	channel, exists := tm.channelInfo[channelID]
	if !exists || (channel != nil && channel.IsArchived) {
		log.Debug().
			Str("channelID", channelID).
			Str("channelName", tm.getChannelDisplayName(channelID)).
			Str("threadTS", threadTS).
			Msg("Skipping thread in archived or inaccessible channel")
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

	log.Debug().
		Str("channelID", channelID).
		Str("channelName", tm.getChannelDisplayName(channelID)).
		Str("threadTS", threadTS).
		Time("lastActivity", lastKnownActivity).
		Msg("Checking for updates to thread")

	// Get the thread replies
	params := &slack.GetConversationRepliesParameters{
		ChannelID: channelID,
		Timestamp: threadTS,
		Limit:     100,
	}

	// Only filter by time if we have a valid lastKnownActivity
	if !lastKnownActivity.IsZero() {
		params.Oldest = fmt.Sprintf("%d.000000", lastKnownActivity.Unix()) // Only get newer messages
	}

	// Adding a retry mechanism for API failures
	var replies []slack.Message
	var hasMore bool
	var nextCursor string
	var err error

	// Try up to maxRetries times with increasing delay
	retryDelay := tm.requestDelay
	for retry := 0; retry < tm.maxRetries; retry++ {
		if retry > 0 {
			log.Warn().
				Str("channelID", channelID).
				Str("channelName", tm.getChannelDisplayName(channelID)).
				Str("threadTS", threadTS).
				Int("retry", retry).
				Dur("delay", retryDelay).
				Msg("Retrying thread update API call")

			// Exponential backoff
			time.Sleep(retryDelay)
			retryDelay *= 2
		}

		replies, hasMore, nextCursor, err = tm.client.GetConversationReplies(params)

		if err == nil {
			break
		}

		// Rate limited or temporary error, try again
		log.Error().
			Err(err).
			Str("channelID", channelID).
			Str("channelName", tm.getChannelDisplayName(channelID)).
			Str("threadTS", threadTS).
			Int("retry", retry+1).
			Int("maxRetries", tm.maxRetries).
			Msg("Error getting thread updates, will retry")
	}

	if err != nil {
		log.Error().
			Err(err).
			Str("channelID", channelID).
			Str("channelName", tm.getChannelDisplayName(channelID)).
			Str("threadTS", threadTS).
			Msg("Failed to get thread updates after multiple retries")
		return
	}

	// Process replies (if any)
	hasNewActivity := false
	newestActivity := lastKnownActivity
	processedCount := 0

	for _, reply := range replies {
		// Skip the parent message and self messages
		if reply.Timestamp == threadTS || reply.User == tm.userID {
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
			threadMessage := Message{
				User:        reply.User,
				Channel:     channelID,
				Text:        reply.Text,
				ThreadTS:    reply.ThreadTimestamp,
				Timestamp:   reply.Timestamp,
				IsThread:    true,
				IsDM:        isDM,
				ChannelType: channelType,
			}

			// Send the message to the processor (FeedAggregator)
			tm.processor.addMessage(threadMessage)
			processedCount++
		}
	}

	// Handle pagination for large threads if needed
	// Define delay between pagination requests
	paginationDelay := tm.requestDelay
	paginationCount := 0

	for hasMore {
		// Add delay before fetching more replies
		time.Sleep(paginationDelay)
		paginationCount++

		log.Debug().
			Str("threadTS", threadTS).
			Str("channelID", channelID).
			Str("channelName", tm.getChannelDisplayName(channelID)).
			Str("cursor", nextCursor).
			Int("paginationCount", paginationCount).
			Msg("Fetching more thread replies")

		params := &slack.GetConversationRepliesParameters{
			ChannelID: channelID,
			Timestamp: threadTS,
			Cursor:    nextCursor,
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

		for retry := 0; retry < tm.maxRetries; retry++ {
			if retry > 0 {
				log.Warn().
					Str("channelID", channelID).
					Str("channelName", tm.getChannelDisplayName(channelID)).
					Str("threadTS", threadTS).
					Str("cursor", nextCursor).
					Int("retry", retry).
					Dur("delay", retryDelay).
					Msg("Retrying pagination API call")

				// Exponential backoff
				time.Sleep(retryDelay)
				retryDelay *= 2
			}

			moreReplies, moreHasMore, nextCursorNew, paginationErr = tm.client.GetConversationReplies(params)

			if paginationErr == nil {
				break
			}
		}

		if paginationErr != nil {
			log.Error().
				Err(paginationErr).
				Str("channelID", channelID).
				Str("channelName", tm.getChannelDisplayName(channelID)).
				Str("threadTS", threadTS).
				Str("cursor", nextCursor).
				Msg("Error getting additional thread replies after retries")
			break
		}

		nextCursor = nextCursorNew

		// Process additional replies
		for _, reply := range moreReplies {
			// Skip if it's the parent message or from self
			if reply.Timestamp == threadTS || reply.User == tm.userID {
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
				threadMessage := Message{
					User:        reply.User,
					Channel:     channelID,
					Text:        reply.Text,
					ThreadTS:    reply.ThreadTimestamp,
					Timestamp:   reply.Timestamp,
					IsThread:    true,
					IsDM:        isDM,
					ChannelType: channelType,
				}

				// Send the message to the processor (FeedAggregator)
				tm.processor.addMessage(threadMessage)
				processedCount++
			}
		}

		hasMore = moreHasMore
	}

	// Update thread activity time if there was new activity
	if hasNewActivity {
		tm.threadMu.Lock()
		if channelThreads, exists := tm.activeThreads[channelID]; exists {
			if info, exists := channelThreads[threadTS]; exists {
				info.LastActivity = newestActivity
				channelThreads[threadTS] = info
			}
		}
		tm.threadMu.Unlock()

		// Update in persistent storage
		tm.stateManager.UpdateThreadActivity(channelID, threadTS, newestActivity)

		log.Debug().
			Str("channelID", channelID).
			Str("channelName", tm.getChannelDisplayName(channelID)).
			Str("threadTS", threadTS).
			Time("newLastActivity", newestActivity).
			Int("newMessagesProcessed", processedCount).
			Msg("Updated thread activity timestamp")
	}

	// Also update the checked time
	now := time.Now()
	tm.threadMu.Lock()
	if channelThreads, exists := tm.activeThreads[channelID]; exists {
		if info, exists := channelThreads[threadTS]; exists {
			info.LastChecked = now
			channelThreads[threadTS] = info
		}
	}
	tm.threadMu.Unlock()

	// Update the last check time in persistent storage
	tm.stateManager.UpdateThreadTimestamp(channelID, threadTS, now)
}

// pollForThreadUpdates periodically checks for updates to active threads
func (tm *ThreadManager) pollForThreadUpdates(ctx context.Context) {
	// Thread polling will use a tiered approach
	ticker := time.NewTicker(10 * time.Second) // Base polling interval
	defer ticker.Stop()

	// Define thread activity tiers for polling frequency
	veryActiveThreshold := 6 * time.Hour    // Threads with activity in the last 6 hours
	activeThreshold := 24 * time.Hour       // Threads with activity in the last 24 hours
	moderateThreshold := 3 * 24 * time.Hour // Threads with activity in the last 3 days
	lowThreshold := 7 * 24 * time.Hour      // Threads with activity in the last week

	// Expiry threshold
	expiryThreshold := time.Duration(tm.threadExpiryDays) * 24 * time.Hour

	log.Info().
		Str("baseInterval", "10s").
		Dur("veryActiveThreshold", veryActiveThreshold).
		Dur("activeThreshold", activeThreshold).
		Dur("moderateThreshold", moderateThreshold).
		Dur("lowThreshold", lowThreshold).
		Dur("checkDelay", tm.requestDelay).
		Dur("expiryThreshold", expiryThreshold).
		Msg("Starting thread update polling with tiered frequency")

	// Track when each thread was last polled
	lastPolled := make(map[string]map[string]time.Time)

	for {
		select {
		case <-ctx.Done():
			log.Debug().Msg("Context done, stopping thread polling")
			return
		case <-ticker.C:
			// Get a snapshot of active threads to avoid deadlocks
			activeThreadsCopy := tm.getActiveThreadsSnapshot()

			now := time.Now()
			threadCount := 0
			expiredCount := 0

			// Instead of processing threads immediately, collect those that need updating
			type threadCheck struct {
				channelID    string
				threadTS     string
				lastActivity time.Time
				priority     int
			}

			threadsToCheck := make([]threadCheck, 0)

			for channelID, threads := range activeThreadsCopy {
				// Initialize the inner map if needed
				if _, exists := lastPolled[channelID]; !exists {
					lastPolled[channelID] = make(map[string]time.Time)
				}

				for threadTS, info := range threads {
					threadCount++

					// Check if thread has expired (no activity for expiryThreshold)
					timeSinceActivity := now.Sub(info.LastActivity)
					if timeSinceActivity > expiryThreshold {
						log.Debug().
							Str("channelID", channelID).
							Str("channelName", tm.getChannelDisplayName(channelID)).
							Str("threadTS", threadTS).
							Dur("inactiveDuration", timeSinceActivity).
							Msg("Expiring inactive thread")

						// Remove from tracking
						tm.threadMu.Lock()
						if channelThreads, exists := tm.activeThreads[channelID]; exists {
							delete(channelThreads, threadTS)
							if len(channelThreads) == 0 {
								delete(tm.activeThreads, channelID)
							}
						}
						tm.threadMu.Unlock()

						// Remove from persistent storage
						tm.stateManager.RemoveThreadTimestamp(channelID, threadTS)

						// Remove from last polled
						delete(lastPolled[channelID], threadTS)
						if len(lastPolled[channelID]) == 0 {
							delete(lastPolled, channelID)
						}

						expiredCount++
						continue
					}

					// Determine polling frequency and priority based on activity
					var pollInterval time.Duration
					var priority int

					if timeSinceActivity <= veryActiveThreshold {
						pollInterval = 30 * time.Second // Very active: check every 30 seconds
						priority = 1                    // High priority
					} else if timeSinceActivity <= activeThreshold {
						pollInterval = 3 * time.Minute // Active: check every 3 minutes
						priority = 2                   // Medium priority
					} else if timeSinceActivity <= moderateThreshold {
						pollInterval = 15 * time.Minute // Moderate: check every 15 minutes
						priority = 3                    // Low priority
					} else {
						pollInterval = 1 * time.Hour // Low: check every hour
						priority = 3                 // Low priority
					}

					// Check if it's time to poll this thread
					lastThreadPoll, exists := lastPolled[channelID][threadTS]
					if !exists || now.Sub(lastThreadPoll) >= pollInterval {
						// Time to check this thread - add to our queue
						threadsToCheck = append(threadsToCheck, threadCheck{
							channelID:    channelID,
							threadTS:     threadTS,
							lastActivity: info.LastActivity,
							priority:     priority,
						})

						// Update last poll time
						lastPolled[channelID][threadTS] = now
					}
				}
			}

			// Now queue threads for checking with their priorities
			if len(threadsToCheck) > 0 {
				// Sort by priority (1=high, 3=low)
				sort.Slice(threadsToCheck, func(i, j int) bool {
					return threadsToCheck[i].priority < threadsToCheck[j].priority
				})

				// Queue them in the order of priority
				for _, thread := range threadsToCheck {
					tm.QueueThreadCheck(
						thread.channelID,
						thread.threadTS,
						thread.lastActivity,
						thread.priority,
					)
				}

				log.Debug().
					Int("totalThreads", threadCount).
					Int("queuedThreads", len(threadsToCheck)).
					Int("expiredThreads", expiredCount).
					Msg("Queued threads for checking")
			} else if threadCount > 0 {
				log.Debug().
					Int("totalThreads", threadCount).
					Int("queuedThreads", 0).
					Int("expiredThreads", expiredCount).
					Msg("No threads needed checking in this cycle")
			}
		}
	}
}

// trackThread adds a thread to the active threads map
func (tm *ThreadManager) trackThread(channelID, threadTS string) {
	tm.threadMu.Lock()
	defer tm.threadMu.Unlock()

	// Initialize the inner map if needed
	if _, exists := tm.activeThreads[channelID]; !exists {
		tm.activeThreads[channelID] = make(map[string]ThreadInfo)
	}

	// Update the timestamp to now
	now := time.Now()
	info := ThreadInfo{
		LastChecked:  now,
		LastActivity: now,
	}
	tm.activeThreads[channelID][threadTS] = info

	// Update in persistent storage
	tm.stateManager.UpdateThreadTimestamp(channelID, threadTS, now)

	log.Debug().
		Str("channelID", channelID).
		Str("channelName", tm.getChannelDisplayName(channelID)).
		Str("threadTS", threadTS).
		Msg("Added thread to active tracking")
}

// getActiveThreadsSnapshot returns a copy of the active threads map
func (tm *ThreadManager) getActiveThreadsSnapshot() map[string]map[string]ThreadInfo {
	tm.threadMu.RLock()
	defer tm.threadMu.RUnlock()

	// Create a deep copy of the active threads map
	result := make(map[string]map[string]ThreadInfo)
	for channelID, threads := range tm.activeThreads {
		result[channelID] = make(map[string]ThreadInfo)
		for threadTS, info := range threads {
			result[channelID][threadTS] = info
		}
	}

	return result
}

// getChannelDisplayName returns a human-readable name for a channel
func (tm *ThreadManager) getChannelDisplayName(channelID string) string {
	if channel, ok := tm.channelInfo[channelID]; ok {
		// For direct messages, use the other user's name
		if channel.IsIM {
			if user, ok := tm.userInfo[channel.User]; ok {
				return fmt.Sprintf("DM with %s", user.RealName)
			}
			return fmt.Sprintf("DM with user %s", channel.User)
		}

		// For group DMs or multi-person IMs
		if channel.IsMpIM {
			return fmt.Sprintf("Group DM %s", channel.Name)
		}

		// For other channels, use the channel name
		if channel.Name != "" {
			return channel.Name
		}
	}

	// Fallback
	return channelID
}
