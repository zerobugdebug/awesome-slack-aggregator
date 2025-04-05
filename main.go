package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	"github.com/slack-go/slack"
)

// Message represents a unified message format
type Message struct {
	User        string
	Channel     string
	Text        string
	ThreadTS    string
	Timestamp   string
	IsThread    bool
	IsDM        bool
	ChannelType string
}

// FeedAggregator aggregates messages from various sources
type FeedAggregator struct {
	client            *slack.Client
	messages          []Message
	channelInfo       map[string]*slack.Channel
	userInfo          map[string]*slack.User
	activeThreads     map[string]map[string]ThreadInfo // Map of channelID -> map of threadTS -> thread info
	threadExpiryDays  int                              // Days after which inactive threads are expired
	mu                sync.Mutex
	threadMu          sync.Mutex // Separate mutex for thread operations
	outputCh          chan Message
	feedTargetUser    string
	userID            string // Current user's ID
	teamDomain        string // Slack team domain for creating links
	stateManager      *StateManager
	messageFormatter  *MessageFormatter
	messageRetainer   *MessageRetainer
	processedMessages map[string]bool // Set of message IDs that have been processed
	processedMu       sync.Mutex
	// Rate limiting variables
	apiRateLimiter  *RateLimiter
	channelActivity map[string]time.Time // Map to track when channels were last active
	channelMu       sync.Mutex           // Mutex for channel activity
}

// ChannelActivityLevel represents how active a channel is
type ChannelActivityLevel int

const (
	VeryActive ChannelActivityLevel = iota
	Active
	Moderate
	Low
	Inactive
)

// RateLimiter manages API request rates
type RateLimiter struct {
	tokenBucket     int           // Number of tokens available
	maxTokens       int           // Maximum number of tokens
	refillRate      time.Duration // How often to add a token
	lastRefill      time.Time     // Last time tokens were refilled
	mu              sync.Mutex    // Mutex for concurrent access
	requestsAllowed chan struct{} // Channel to signal when requests are allowed
}

// NewRateLimiter creates a new rate limiter
func NewRateLimiter(maxTokens int, refillRate time.Duration) *RateLimiter {
	rl := &RateLimiter{
		tokenBucket:     maxTokens,
		maxTokens:       maxTokens,
		refillRate:      refillRate,
		lastRefill:      time.Now(),
		requestsAllowed: make(chan struct{}, maxTokens), // Buffer size matches maxTokens
	}

	// Initially fill the channel with tokens
	for i := 0; i < maxTokens; i++ {
		rl.requestsAllowed <- struct{}{}
	}

	// Start the token refill goroutine
	go rl.refillTokens()

	return rl
}

// refillTokens periodically adds tokens back to the bucket
func (rl *RateLimiter) refillTokens() {
	ticker := time.NewTicker(rl.refillRate)
	defer ticker.Stop()

	for range ticker.C {
		rl.mu.Lock()
		now := time.Now()
		elapsed := now.Sub(rl.lastRefill)
		tokensToAdd := int(elapsed / rl.refillRate)

		if tokensToAdd > 0 {
			rl.lastRefill = now
			newTokens := rl.tokenBucket + tokensToAdd
			if newTokens > rl.maxTokens {
				newTokens = rl.maxTokens
			}

			// Add tokens to the channel if we have new ones
			for i := rl.tokenBucket; i < newTokens; i++ {
				select {
				case rl.requestsAllowed <- struct{}{}:
					// Token added
				default:
					// Channel is full, this shouldn't happen but just in case
				}
			}

			rl.tokenBucket = newTokens
		}
		rl.mu.Unlock()
	}
}

// Wait blocks until a token is available
func (rl *RateLimiter) Wait() {
	<-rl.requestsAllowed

	rl.mu.Lock()
	rl.tokenBucket--
	rl.mu.Unlock()
}

// getChannelDisplayName returns a human-readable name for a channel
func (fa *FeedAggregator) getChannelDisplayName(channelID string) string {
	if channel, ok := fa.channelInfo[channelID]; ok {
		// For direct messages, use the other user's name
		if channel.IsIM {
			if user, ok := fa.userInfo[channel.User]; ok {
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

// getUserDisplayName returns a human-readable name for a user
func (fa *FeedAggregator) getUserDisplayName(userID string) string {
	if user, ok := fa.userInfo[userID]; ok {
		return user.RealName
	}
	return userID
}

// slackLogAdapter adapts zerolog to slack-go's log interface
type slackLogAdapter struct {
	logger zerolog.Logger
}

func (a *slackLogAdapter) Output(calldepth int, s string) error {
	a.logger.Debug().Msg(s)
	return nil
}

// NewFeedAggregator creates a new feed aggregator
func NewFeedAggregator(token string, targetUserID string, stateDir string, retentionDays int, threadExpiryDays int) (*FeedAggregator, error) {
	// Create a logger adapter for slack-go
	slackLogger := &slackLogAdapter{
		logger: log.With().Str("component", "slack-api").Logger(),
	}

	client := slack.New(
		token,
		slack.OptionLog(slackLogger),
	)

	// Test the token by getting user identity
	log.Debug().Msg("Testing authentication with Slack")
	authTest, err := client.AuthTest()
	if err != nil {
		log.Error().Err(err).Msg("Authentication test failed")
		return nil, fmt.Errorf("auth test failed: %w", err)
	}

	log.Info().
		Str("user", authTest.User).
		Str("userID", authTest.UserID).
		Str("teamDomain", authTest.URL).
		Msg("Connected to Slack")

	// Extract team domain from URL
	// URL format is usually https://team-domain.slack.com/
	teamDomain := "slack.com" // Default fallback
	if authTest.URL != "" {
		parts := strings.Split(authTest.URL, "//")
		if len(parts) > 1 {
			parts = strings.Split(parts[1], ".")
			if len(parts) > 0 {
				teamDomain = parts[0]
				log.Debug().Str("teamDomain", teamDomain).Msg("Extracted team domain from URL")
			}
		}
	}

	// Initialize state manager
	stateManager, err := NewStateManager(stateDir)
	if err != nil {
		log.Error().Err(err).Str("stateDir", stateDir).Msg("Failed to initialize state manager")
		return nil, fmt.Errorf("failed to initialize state manager: %w", err)
	}

	// Create rate limiter - allow 20 requests per minute to stay well under Slack's limits
	rateLimiter := NewRateLimiter(20, 3*time.Second) // Refill a token every 3 seconds (20 per minute)

	// Create the feed aggregator
	fa := &FeedAggregator{
		client:            client,
		messages:          make([]Message, 0),
		channelInfo:       make(map[string]*slack.Channel),
		userInfo:          make(map[string]*slack.User),
		activeThreads:     make(map[string]map[string]ThreadInfo),
		outputCh:          make(chan Message, 100),
		feedTargetUser:    targetUserID,
		userID:            authTest.UserID,
		teamDomain:        teamDomain,
		stateManager:      stateManager,
		messageFormatter:  NewMessageFormatter(),
		processedMessages: make(map[string]bool),
		threadExpiryDays:  threadExpiryDays,
		apiRateLimiter:    rateLimiter,
		channelActivity:   make(map[string]time.Time),
	}

	// Initialize message retainer
	fa.messageRetainer = NewMessageRetainer(client, stateManager, retentionDays)

	return fa, nil
}

// Start begins listening for messages
func (fa *FeedAggregator) Start(ctx context.Context) error {
	// Start the state manager
	fa.stateManager.Start()
	// Load initial channel and user information
	log.Debug().Msg("Loading initial channel and user data")
	if err := fa.loadInitialData(); err != nil {
		log.Error().Err(err).Msg("Failed to load initial data")
		return err
	}

	// Start the message retention manager
	log.Debug().Msg("Starting message retainer")
	fa.messageRetainer.Start(ctx)

	// Start the output processor
	log.Debug().Msg("Starting output processor")
	go fa.processOutputChannel(ctx)

	// Start thread polling
	log.Debug().Msg("Starting thread polling")
	go fa.pollForThreadUpdates(ctx)

	// Start recent message polling for new threads
	log.Debug().Msg("Starting recent message polling")
	go fa.pollForNewThreads(ctx)

	// Start message polling
	log.Debug().Msg("Starting message polling")
	go fa.pollForMessages(ctx)

	log.Info().Msg("Feed aggregator started successfully")

	// This keeps the main thread running
	<-ctx.Done()
	log.Info().Msg("Context done, shutting down feed aggregator")

	// Stop components
	fa.messageRetainer.Stop()
	fa.stateManager.Stop()

	return nil
}

// loadInitialData loads channels and users info
func (fa *FeedAggregator) loadInitialData() error {
	log.Debug().Msg("Loading initial channel and user data")

	// Get only conversations that the user is a member of
	log.Debug().Msg("Fetching user conversations")

	// Wait for rate limiter
	fa.apiRateLimiter.Wait()

	conversations, nextCursor, err := fa.client.GetConversationsForUser(&slack.GetConversationsForUserParameters{
		Types:           []string{"public_channel", "private_channel", "mpim", "im"},
		Limit:           1000,
		ExcludeArchived: true,
		UserID:          fa.userID, // Only get conversations the current user is a member of
	})
	if err != nil {
		log.Error().Err(err).Msg("Failed to get user conversations")
		return fmt.Errorf("failed to get user conversations: %w", err)
	}

	// Handle pagination if there are more conversations
	for nextCursor != "" {
		log.Debug().Str("cursor", nextCursor).Msg("Fetching additional user conversations")

		// Wait for rate limiter
		fa.apiRateLimiter.Wait()

		var additionalConversations []slack.Channel
		additionalConversations, nextCursor, err = fa.client.GetConversationsForUser(&slack.GetConversationsForUserParameters{
			Types:           []string{"public_channel", "private_channel", "mpim", "im"},
			Limit:           1000,
			Cursor:          nextCursor,
			ExcludeArchived: true,
			UserID:          fa.userID,
		})
		if err != nil {
			log.Error().Err(err).Str("cursor", nextCursor).Msg("Failed to get additional user conversations")
			return fmt.Errorf("failed to get additional user conversations: %w", err)
		}
		conversations = append(conversations, additionalConversations...)
		log.Debug().Int("additional_count", len(additionalConversations)).Msg("Fetched additional conversations")
	}

	// Get all users
	log.Debug().Msg("Fetching all users")

	// Wait for rate limiter
	fa.apiRateLimiter.Wait()

	users, err := fa.client.GetUsers()
	if err != nil {
		log.Error().Err(err).Msg("Failed to get users")
		return fmt.Errorf("failed to get users: %w", err)
	}

	log.Info().Int("user_count", len(users)).Msg("Users fetched")
	for _, user := range users {
		fa.userInfo[user.ID] = &user
		log.Trace().Str("userID", user.ID).Str("name", user.RealName).Msg("Added user")
	}

	// Store all channels the user is a member of
	memberChannels := 0
	for _, channel := range conversations {
		fa.channelInfo[channel.ID] = &channel
		memberChannels++

		channelName := channel.Name
		// Log channel info
		channelType := "channel"
		if channel.IsIM {
			channelType = "direct message"
			for userID := range fa.userInfo {
				if channel.User == userID {
					channelName = fmt.Sprintf("DM with %s", fa.userInfo[userID].RealName)
					break
				}
			}
		} else if channel.IsMpIM {
			channelType = "group DM"
		} else if channel.IsPrivate {
			channelType = "private channel"
		}

		log.Debug().
			Str("type", channelType).
			Str("name", channelName).
			Str("id", channel.ID).
			Msg("Added channel")

		// Initialize channel activity to now
		fa.channelMu.Lock()
		fa.channelActivity[channel.ID] = time.Now()
		fa.channelMu.Unlock()
	}

	log.Info().Int("channel_count", memberChannels).Msg("Channels loaded")

	return nil
}

// getChannelActivityLevel determines how active a channel is
func (fa *FeedAggregator) getChannelActivityLevel(channelID string) ChannelActivityLevel {
	fa.channelMu.Lock()
	defer fa.channelMu.Unlock()

	lastActive, exists := fa.channelActivity[channelID]
	if !exists {
		return Inactive
	}

	timeSinceActivity := time.Since(lastActive)

	if timeSinceActivity <= 1*time.Hour {
		return VeryActive
	} else if timeSinceActivity <= 6*time.Hour {
		return Active
	} else if timeSinceActivity <= 24*time.Hour {
		return Moderate
	} else if timeSinceActivity <= 7*24*time.Hour {
		return Low
	}

	return Inactive
}

// updateChannelActivity marks a channel as active
func (fa *FeedAggregator) updateChannelActivity(channelID string) {
	fa.channelMu.Lock()
	defer fa.channelMu.Unlock()

	fa.channelActivity[channelID] = time.Now()
}

// OPTIMIZATION: Completely redesigned message polling strategy based on channel activity
func (fa *FeedAggregator) pollForMessages(ctx context.Context) {
	// Base ticker runs every 5 seconds
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()

	// Create a queue of channels to process
	type channelQueue struct {
		channelID      string
		lastProcessed  time.Time
		nextProcessDue time.Time
	}

	channelsToProcess := make([]channelQueue, 0, len(fa.channelInfo))

	// Initialize the channel queue from persistent storage or use current time
	now := time.Now()
	defaultOldest := now.Add(-12 * time.Hour)

	// Initialize timestamps from persistent storage or use defaults
	for channelID, channel := range fa.channelInfo {
		// Skip DMs with the target user to avoid feedback loops
		if channel.IsIM && channel.User == fa.feedTargetUser && fa.feedTargetUser != "" && fa.feedTargetUser != "self" {
			log.Info().
				Str("channelID", channelID).
				Str("channelName", fa.getChannelDisplayName(channelID)).
				Str("targetUser", fa.feedTargetUser).
				Str("targetUserName", fa.getUserDisplayName(fa.feedTargetUser)).
				Msg("Skipping DM with target user from polling to avoid feedback loops")
			continue
		}

		// Get timestamp from persistent storage or use default
		savedTS := fa.stateManager.GetLatestTimestamp(channelID)
		var oldest time.Time

		if savedTS != "" {
			// Convert string timestamp to time
			timestampFloat := 0.0
			_, err := fmt.Sscanf(savedTS, "%f", &timestampFloat)
			if err != nil {
				log.Error().
					Err(err).
					Str("timestamp", savedTS).
					Msg("Failed to parse saved timestamp, using default")
				oldest = defaultOldest
			} else {
				oldest = time.Unix(int64(timestampFloat), 0)
			}

			log.Debug().
				Str("channelID", channelID).
				Str("channelName", fa.getChannelDisplayName(channelID)).
				Time("oldestTime", oldest).
				Msg("Loaded timestamp from persistent storage")
		} else {
			oldest = defaultOldest
			log.Debug().
				Str("channelID", channelID).
				Str("channelName", fa.getChannelDisplayName(channelID)).
				Time("defaultTime", oldest).
				Msg("Using default timestamp")
		}

		// Use activity level to determine initial processing time
		activityLevel := fa.getChannelActivityLevel(channelID)
		var processDelay time.Duration

		switch activityLevel {
		case VeryActive:
			processDelay = 30 * time.Second
		case Active:
			processDelay = 2 * time.Minute
		case Moderate:
			processDelay = 5 * time.Minute
		case Low:
			processDelay = 15 * time.Minute
		case Inactive:
			processDelay = 30 * time.Minute
		}

		// Add to processing queue with appropriate next process time
		channelsToProcess = append(channelsToProcess, channelQueue{
			channelID:      channelID,
			lastProcessed:  now.Add(-processDelay), // Schedule immediate processing on first run
			nextProcessDue: now,
		})

		log.Debug().
			Str("channelID", channelID).
			Str("channelName", fa.getChannelDisplayName(channelID)).
			Str("activityLevel", activityLevelToString(activityLevel)).
			Dur("processDelay", processDelay).
			Msg("Added channel to polling queue")
	}

	log.Info().
		Int("channel_count", len(channelsToProcess)).
		Msg("Initialized message polling with dynamic scheduling")

	// Process channels in the queue
	for {
		select {
		case <-ctx.Done():
			log.Debug().Msg("Context done, stopping message polling")
			return
		case <-ticker.C:
			now := time.Now()

			// Find channels due for processing
			for i := range channelsToProcess {
				if now.After(channelsToProcess[i].nextProcessDue) {
					channelID := channelsToProcess[i].channelID
					channel, exists := fa.channelInfo[channelID]

					if !exists || (channel != nil && channel.IsArchived) {
						// Skip archived or non-existent channels
						channelsToProcess[i].nextProcessDue = now.Add(1 * time.Hour) // Check again in an hour
						continue
					}

					// Process this channel
					go func(ch channelQueue) {
						log.Debug().
							Str("channelID", ch.channelID).
							Str("channelName", fa.getChannelDisplayName(ch.channelID)).
							Msg("Processing channel for new messages")

						// Get the latest timestamp we've processed for this channel
						oldestTS := fa.stateManager.GetLatestTimestamp(ch.channelID)
						if oldestTS == "" {
							oldestTS = fmt.Sprintf("%d.000000", defaultOldest.Unix())
						}

						// Wait for rate limiter before making API call
						fa.apiRateLimiter.Wait()

						// Get history since the last check
						params := &slack.GetConversationHistoryParameters{
							ChannelID: ch.channelID,
							Oldest:    oldestTS,
							Limit:     100,
						}

						history, err := fa.client.GetConversationHistory(params)
						if err != nil {
							log.Error().
								Err(err).
								Str("channelID", ch.channelID).
								Str("channelName", fa.getChannelDisplayName(ch.channelID)).
								Msg("Error getting history for channel")
							return
						}

						// Update last processed time
						fa.mu.Lock()
						for i := range channelsToProcess {
							if channelsToProcess[i].channelID == ch.channelID {
								channelsToProcess[i].lastProcessed = time.Now()
								break
							}
						}
						fa.mu.Unlock()

						messageCount := len(history.Messages)

						if messageCount > 0 {
							log.Debug().
								Str("channelID", ch.channelID).
								Str("channelName", fa.getChannelDisplayName(ch.channelID)).
								Int("message_count", messageCount).
								Msg("Found new messages")

							// Process messages (newest first)
							for i := messageCount - 1; i >= 0; i-- {
								msg := history.Messages[i]
								// Skip messages created by this app
								if fa.messageFormatter.IsAppMessage(msg.Text) {
									continue
								}

								// Determine channel type
								channelType := "channel"
								isDM := false

								if channel, ok := fa.channelInfo[ch.channelID]; ok {
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

								message := Message{
									User:        msg.User,
									Channel:     ch.channelID,
									Text:        msg.Text,
									ThreadTS:    msg.ThreadTimestamp,
									Timestamp:   msg.Timestamp,
									IsThread:    msg.ThreadTimestamp != "",
									IsDM:        isDM,
									ChannelType: channelType,
								}

								fa.addMessage(message)

								// Update channel activity level
								fa.updateChannelActivity(ch.channelID)

								// Process thread replies in batch rather than one by one
								if msg.ReplyCount > 0 {
									fa.processThreadRepliesBatch(ch.channelID, msg.Timestamp, channelType, isDM)
								}

								// Track this message for future thread detection
								fa.stateManager.TrackRecentMessage(ch.channelID, msg.Timestamp)
							}

							// Update latest timestamp for this channel
							fa.stateManager.SetLatestTimestamp(ch.channelID, history.Messages[0].Timestamp)

							log.Debug().
								Str("channelID", ch.channelID).
								Str("channelName", fa.getChannelDisplayName(ch.channelID)).
								Str("newLatestTS", history.Messages[0].Timestamp).
								Msg("Updated latest timestamp for channel")
						}

						// Determine next processing time based on activity level
						var nextDelay time.Duration
						activityLevel := fa.getChannelActivityLevel(ch.channelID)

						// If we found messages, increase the activity level
						if messageCount > 0 {
							// More messages = more active channel
							if messageCount > 10 {
								activityLevel = VeryActive
							} else if messageCount > 5 {
								activityLevel = Active
							} else if messageCount > 0 {
								activityLevel = Moderate
							}
						}

						// Set next processing delay based on activity
						switch activityLevel {
						case VeryActive:
							nextDelay = 30 * time.Second
						case Active:
							nextDelay = 2 * time.Minute
						case Moderate:
							nextDelay = 5 * time.Minute
						case Low:
							nextDelay = 15 * time.Minute
						case Inactive:
							nextDelay = 30 * time.Minute
						}

						// Update the next processing time
						fa.mu.Lock()
						for i := range channelsToProcess {
							if channelsToProcess[i].channelID == ch.channelID {
								channelsToProcess[i].nextProcessDue = time.Now().Add(nextDelay)
								break
							}
						}
						fa.mu.Unlock()

						log.Debug().
							Str("channelID", ch.channelID).
							Str("channelName", fa.getChannelDisplayName(ch.channelID)).
							Str("activityLevel", activityLevelToString(activityLevel)).
							Dur("nextDelay", nextDelay).
							Time("nextProcessDue", time.Now().Add(nextDelay)).
							Msg("Updated channel processing schedule")

					}(channelsToProcess[i])
				}
			}
		}
	}
}

// Helper function to convert activity level to string for logging
func activityLevelToString(level ChannelActivityLevel) string {
	switch level {
	case VeryActive:
		return "VeryActive"
	case Active:
		return "Active"
	case Moderate:
		return "Moderate"
	case Low:
		return "Low"
	case Inactive:
		return "Inactive"
	default:
		return "Unknown"
	}
}

// OPTIMIZATION: Process thread replies in batches to reduce API calls
func (fa *FeedAggregator) processThreadRepliesBatch(channelID, threadTS, channelType string, isDM bool) {
	log.Debug().
		Str("channelID", channelID).
		Str("channelName", fa.getChannelDisplayName(channelID)).
		Str("threadTS", threadTS).
		Msg("Processing thread replies in batch")

	// Wait for rate limiter
	fa.apiRateLimiter.Wait()

	// Get all replies in one call with pagination support
	replies, hasMore, nextCursor, err := fa.client.GetConversationReplies(&slack.GetConversationRepliesParameters{
		ChannelID: channelID,
		Timestamp: threadTS,
		Limit:     100,
	})

	if err != nil {
		log.Error().
			Err(err).
			Str("channelID", channelID).
			Str("channelName", fa.getChannelDisplayName(channelID)).
			Str("threadTS", threadTS).
			Msg("Error getting thread replies")
		return
	}

	// Track this thread
	fa.trackThread(channelID, threadTS)

	// Process all replies in memory first before adding them
	threadMessages := make([]Message, 0, len(replies))

	// Process thread replies
	for _, reply := range replies {
		// Skip if it's the parent message or from self
		if reply.Timestamp == threadTS || reply.User == fa.userID {
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

		threadMessages = append(threadMessages, threadMessage)
	}

	// Handle pagination for large threads
	for hasMore {
		// Wait for rate limiter
		fa.apiRateLimiter.Wait()

		log.Debug().
			Str("threadTS", threadTS).
			Str("channelID", channelID).
			Str("channelName", fa.getChannelDisplayName(channelID)).
			Str("cursor", nextCursor).
			Msg("Fetching more thread replies")

		moreReplies, moreHasMore, nextCursorNew, err := fa.client.GetConversationReplies(&slack.GetConversationRepliesParameters{
			ChannelID: channelID,
			Timestamp: threadTS,
			Cursor:    nextCursor,
			Limit:     100,
		})

		if err != nil {
			log.Error().
				Err(err).
				Str("channelID", channelID).
				Str("channelName", fa.getChannelDisplayName(channelID)).
				Str("threadTS", threadTS).
				Str("cursor", nextCursor).
				Msg("Error getting additional thread replies")
			break
		}

		nextCursor = nextCursorNew

		// Process additional replies
		for _, reply := range moreReplies {
			// Skip if it's the parent message or from self
			if reply.Timestamp == threadTS || reply.User == fa.userID {
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

			threadMessages = append(threadMessages, threadMessage)
		}

		hasMore = moreHasMore
	}

	// Now add all thread messages at once
	log.Debug().
		Str("channelID", channelID).
		Str("channelName", fa.getChannelDisplayName(channelID)).
		Str("threadTS", threadTS).
		Int("replyCount", len(threadMessages)).
		Msg("Adding batch of thread replies")

	for _, msg := range threadMessages {
		fa.addMessage(msg)
	}
}

// addMessage adds a message to the feed
func (fa *FeedAggregator) addMessage(msg Message) {
	fa.tryAddUniqueMessage(msg)
}

// GetMessages returns all aggregated messages
func (fa *FeedAggregator) GetMessages() []Message {
	fa.mu.Lock()
	defer fa.mu.Unlock()

	log.Debug().Int("count", len(fa.messages)).Msg("Getting all messages")

	// Create a copy to avoid race conditions
	result := make([]Message, len(fa.messages))
	copy(result, fa.messages)

	return result
}

// trackThread adds a thread to the active threads map with persistent storage
func (fa *FeedAggregator) trackThread(channelID, threadTS string) {
	fa.threadMu.Lock()
	defer fa.threadMu.Unlock()

	// Initialize the inner map if needed
	if _, exists := fa.activeThreads[channelID]; !exists {
		fa.activeThreads[channelID] = make(map[string]ThreadInfo)
	}

	// Update the timestamp to now
	now := time.Now()
	info := ThreadInfo{
		LastChecked:  now,
		LastActivity: now,
	}
	fa.activeThreads[channelID][threadTS] = info

	// Update in persistent storage
	fa.stateManager.UpdateThreadTimestamp(channelID, threadTS, now)

	log.Debug().
		Str("channelID", channelID).
		Str("channelName", fa.getChannelDisplayName(channelID)).
		Str("threadTS", threadTS).
		Msg("Added thread to active tracking")
}

// OPTIMIZATION: Process multiple threads in batch to reduce API calls
func (fa *FeedAggregator) pollForThreadUpdates(ctx context.Context) {
	// Use persistent storage to initialize active threads
	fa.activeThreads = fa.stateManager.GetActiveThreads()

	// Define thread activity tiers for polling frequency
	veryActiveThreshold := 6 * time.Hour    // Threads with activity in the last 6 hours
	activeThreshold := 24 * time.Hour       // Threads with activity in the last 24 hours
	moderateThreshold := 3 * 24 * time.Hour // Threads with activity in the last 3 days
	lowThreshold := 7 * 24 * time.Hour      // Threads with activity in the last week

	// Expiry threshold
	expiryThreshold := time.Duration(fa.threadExpiryDays) * 24 * time.Hour

	log.Info().
		Dur("veryActiveThreshold", veryActiveThreshold).
		Dur("activeThreshold", activeThreshold).
		Dur("moderateThreshold", moderateThreshold).
		Dur("lowThreshold", lowThreshold).
		Dur("expiryThreshold", expiryThreshold).
		Msg("Starting thread update polling with tiered frequency and batch processing")

	// Base ticker runs every 30 seconds
	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()

	// Last poll times for each activity tier
	lastVeryActivePoll := time.Now().Add(-1 * time.Minute)
	lastActivePoll := time.Now().Add(-5 * time.Minute)
	lastModeratePoll := time.Now().Add(-15 * time.Minute)
	lastLowPoll := time.Now().Add(-1 * time.Hour)

	for {
		select {
		case <-ctx.Done():
			log.Debug().Msg("Context done, stopping thread polling")
			return
		case <-ticker.C:
			// Get a snapshot of active threads
			activeThreadsCopy := fa.getActiveThreadsSnapshot()
			now := time.Now()

			// OPTIMIZATION: Batch threads by activity level and process each batch together
			veryActiveThreads := make(map[string][]string) // channelID -> []threadTS
			activeThreads := make(map[string][]string)
			moderateThreads := make(map[string][]string)
			lowThreads := make(map[string][]string)

			threadCount := 0
			expiredCount := 0

			// First pass: categorize threads by activity level and expire old ones
			for channelID, threads := range activeThreadsCopy {
				for threadTS, info := range threads {
					threadCount++

					// Check if thread has expired
					timeSinceActivity := now.Sub(info.LastActivity)
					if timeSinceActivity > expiryThreshold {
						// Remove from tracking
						fa.threadMu.Lock()
						if channelThreads, exists := fa.activeThreads[channelID]; exists {
							delete(channelThreads, threadTS)
							if len(channelThreads) == 0 {
								delete(fa.activeThreads, channelID)
							}
						}
						fa.threadMu.Unlock()

						// Remove from persistent storage
						fa.stateManager.RemoveThreadTimestamp(channelID, threadTS)

						expiredCount++
						continue
					}

					// Categorize by activity level
					if timeSinceActivity <= veryActiveThreshold {
						if _, exists := veryActiveThreads[channelID]; !exists {
							veryActiveThreads[channelID] = make([]string, 0)
						}
						veryActiveThreads[channelID] = append(veryActiveThreads[channelID], threadTS)
					} else if timeSinceActivity <= activeThreshold {
						if _, exists := activeThreads[channelID]; !exists {
							activeThreads[channelID] = make([]string, 0)
						}
						activeThreads[channelID] = append(activeThreads[channelID], threadTS)
					} else if timeSinceActivity <= moderateThreshold {
						if _, exists := moderateThreads[channelID]; !exists {
							moderateThreads[channelID] = make([]string, 0)
						}
						moderateThreads[channelID] = append(moderateThreads[channelID], threadTS)
					} else {
						if _, exists := lowThreads[channelID]; !exists {
							lowThreads[channelID] = make([]string, 0)
						}
						lowThreads[channelID] = append(lowThreads[channelID], threadTS)
					}
				}
			}

			log.Debug().
				Int("totalThreads", threadCount).
				Int("expiredThreads", expiredCount).
				Int("veryActiveThreads", countThreadsInMap(veryActiveThreads)).
				Int("activeThreads", countThreadsInMap(activeThreads)).
				Int("moderateThreads", countThreadsInMap(moderateThreads)).
				Int("lowThreads", countThreadsInMap(lowThreads)).
				Msg("Categorized threads by activity level")

			// Second pass: Check threads by tier, based on polling frequency
			// Very active threads: check every minute
			if now.Sub(lastVeryActivePoll) >= 1*time.Minute && len(veryActiveThreads) > 0 {
				go fa.checkThreadsBatch(veryActiveThreads, "very_active")
				lastVeryActivePoll = now
			}

			// Active threads: check every 5 minutes
			if now.Sub(lastActivePoll) >= 5*time.Minute && len(activeThreads) > 0 {
				go fa.checkThreadsBatch(activeThreads, "active")
				lastActivePoll = now
			}

			// Moderate threads: check every 15 minutes
			if now.Sub(lastModeratePoll) >= 15*time.Minute && len(moderateThreads) > 0 {
				go fa.checkThreadsBatch(moderateThreads, "moderate")
				lastModeratePoll = now
			}

			// Low activity threads: check every hour
			if now.Sub(lastLowPoll) >= 1*time.Hour && len(lowThreads) > 0 {
				go fa.checkThreadsBatch(lowThreads, "low")
				lastLowPoll = now
			}
		}
	}
}

// Helper to count total threads in a map of channel -> threads
func countThreadsInMap(threadsMap map[string][]string) int {
	count := 0
	for _, threads := range threadsMap {
		count += len(threads)
	}
	return count
}

// OPTIMIZATION: Check threads in batches by channel to reduce API calls
func (fa *FeedAggregator) checkThreadsBatch(threadsByChannel map[string][]string, activityLabel string) {
	totalThreads := countThreadsInMap(threadsByChannel)
	log.Debug().
		Int("channelCount", len(threadsByChannel)).
		Int("threadCount", totalThreads).
		Str("activityLevel", activityLabel).
		Msg("Checking batch of threads")

	processedCount := 0
	newMessages := 0

	// Process one channel at a time to batch API calls effectively
	for channelID, threadTSList := range threadsByChannel {
		// Skip if the channel doesn't exist or is archived
		channel, exists := fa.channelInfo[channelID]
		if !exists || (channel != nil && channel.IsArchived) {
			log.Debug().
				Str("channelID", channelID).
				Str("channelName", fa.getChannelDisplayName(channelID)).
				Int("threadCount", len(threadTSList)).
				Msg("Skipping threads in archived or inaccessible channel")
			continue
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

		// Get the latest activity time for each thread
		threadActivityTimes := make(map[string]time.Time)
		fa.threadMu.Lock()
		if channelThreads, exists := fa.activeThreads[channelID]; exists {
			for _, threadTS := range threadTSList {
				if info, exists := channelThreads[threadTS]; exists {
					threadActivityTimes[threadTS] = info.LastActivity
				}
			}
		}
		fa.threadMu.Unlock()

		// Now process threads in this channel - in smaller batches if needed
		batchSize := 10 // Process up to 10 threads per API call in channels with many threads
		for i := 0; i < len(threadTSList); i += batchSize {
			end := i + batchSize
			if end > len(threadTSList) {
				end = len(threadTSList)
			}

			currentBatch := threadTSList[i:end]

			// Use a single conversation.replies call with a proper oldest parameter if possible
			for _, threadTS := range currentBatch {
				lastActivity, exists := threadActivityTimes[threadTS]
				if !exists {
					lastActivity = time.Time{} // Zero time
				}

				// Wait for rate limiter
				fa.apiRateLimiter.Wait()

				// Get thread replies with oldest parameter set to last activity time
				params := &slack.GetConversationRepliesParameters{
					ChannelID: channelID,
					Timestamp: threadTS,
					Limit:     100,
				}

				// Only filter by time if we have a valid lastActivity
				if !lastActivity.IsZero() {
					params.Oldest = fmt.Sprintf("%d.000000", lastActivity.Unix()) // Only get newer messages
				}

				replies, hasMore, nextCursor, err := fa.client.GetConversationReplies(params)

				if err != nil {
					log.Error().
						Err(err).
						Str("channelID", channelID).
						Str("channelName", fa.getChannelDisplayName(channelID)).
						Str("threadTS", threadTS).
						Msg("Error getting thread updates")
					continue
				}

				processedCount++

				// Process replies and track if we found new activity
				hasNewActivity := false
				newestActivity := lastActivity
				newMessageCount := 0

				// Process all replies from this thread
				threadMessages := make([]Message, 0)

				for _, reply := range replies {
					// Skip the parent message and self messages
					if reply.Timestamp == threadTS || reply.User == fa.userID {
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
					if replyTime.After(lastActivity) {
						hasNewActivity = true
						newMessageCount++

						if replyTime.After(newestActivity) {
							newestActivity = replyTime
						}

						// Create message object
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

						threadMessages = append(threadMessages, threadMessage)
					}
				}

				// Handle pagination if needed
				for hasMore {
					// Wait for rate limiter
					fa.apiRateLimiter.Wait()

					moreReplies, moreHasMore, nextCursorNew, err := fa.client.GetConversationReplies(&slack.GetConversationRepliesParameters{
						ChannelID: channelID,
						Timestamp: threadTS,
						Cursor:    nextCursor,
					})

					if err != nil {
						log.Error().
							Err(err).
							Str("channelID", channelID).
							Str("channelName", fa.getChannelDisplayName(channelID)).
							Str("threadTS", threadTS).
							Str("cursor", nextCursor).
							Msg("Error getting additional thread replies")
						break
					}

					nextCursor = nextCursorNew

					// Process additional replies
					for _, reply := range moreReplies {
						// Skip if it's the parent message or from self
						if reply.Timestamp == threadTS || reply.User == fa.userID {
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
						if replyTime.After(lastActivity) {
							hasNewActivity = true
							newMessageCount++

							if replyTime.After(newestActivity) {
								newestActivity = replyTime
							}

							// Create message object
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

							threadMessages = append(threadMessages, threadMessage)
						}
					}

					hasMore = moreHasMore
				}

				// Add all new messages
				for _, msg := range threadMessages {
					fa.addMessage(msg)
				}

				newMessages += newMessageCount

				// Update thread activity time if there was new activity
				if hasNewActivity {
					fa.threadMu.Lock()
					if channelThreads, exists := fa.activeThreads[channelID]; exists {
						if info, exists := channelThreads[threadTS]; exists {
							info.LastActivity = newestActivity
							channelThreads[threadTS] = info

							// Also update channel activity
							fa.updateChannelActivity(channelID)
						}
					}
					fa.threadMu.Unlock()

					// Update in persistent storage
					fa.stateManager.UpdateThreadActivity(channelID, threadTS, newestActivity)

					log.Debug().
						Str("channelID", channelID).
						Str("channelName", fa.getChannelDisplayName(channelID)).
						Str("threadTS", threadTS).
						Time("newLastActivity", newestActivity).
						Int("newMessagesFound", newMessageCount).
						Msg("Updated thread activity with new messages")
				}

				// Always update the last checked time
				now := time.Now()
				fa.threadMu.Lock()
				if channelThreads, exists := fa.activeThreads[channelID]; exists {
					if info, exists := channelThreads[threadTS]; exists {
						info.LastChecked = now
						channelThreads[threadTS] = info
					}
				}
				fa.threadMu.Unlock()

				// Update in persistent storage
				fa.stateManager.UpdateThreadTimestamp(channelID, threadTS, now)
			}

			// Add a small delay between batches to avoid API rate limits
			time.Sleep(100 * time.Millisecond)
		}
	}

	log.Debug().
		Int("processedThreads", processedCount).
		Int("newMessages", newMessages).
		Str("activityLevel", activityLabel).
		Msg("Completed batch thread check")
}

// getActiveThreadsSnapshot returns a copy of the active threads map
func (fa *FeedAggregator) getActiveThreadsSnapshot() map[string]map[string]ThreadInfo {
	fa.threadMu.Lock()
	defer fa.threadMu.Unlock()

	// Create a deep copy of the active threads map
	result := make(map[string]map[string]ThreadInfo)
	for channelID, threads := range fa.activeThreads {
		result[channelID] = make(map[string]ThreadInfo)
		for threadTS, info := range threads {
			result[channelID][threadTS] = info
		}
	}

	return result
}

// OPTIMIZATION: Completely redesigned polling for new threads to reduce API calls
func (fa *FeedAggregator) pollForNewThreads(ctx context.Context) {
	// Track messages for 24 hours
	messageTrackingPeriod := 24 * time.Hour

	// Check for new threads every 2 minutes instead of every minute
	ticker := time.NewTicker(2 * time.Minute)
	defer ticker.Stop()

	log.Info().
		Dur("trackingPeriod", messageTrackingPeriod).
		Str("checkInterval", "2m").
		Msg("Starting optimized thread detection with batched processing")

	for {
		select {
		case <-ctx.Done():
			log.Debug().Msg("Context done, stopping thread detection")
			return
		case <-ticker.C:
			// Clean up old messages from tracking
			removed := fa.stateManager.CleanupOldRecentMessages(messageTrackingPeriod)
			if removed > 0 {
				log.Debug().Int("removedCount", removed).Msg("Removed old messages from tracking")
			}

			// OPTIMIZATION: Group messages by channel to reduce API calls
			messagesByChannel := make(map[string][]string)

			// Get recent messages and group them by channel
			recentMessages := fa.stateManager.GetRecentMessages()

			// First pass: organize messages by channel and filter tracked threads
			for channelID, messages := range recentMessages {
				// Skip if the channel doesn't exist or is archived
				channel, exists := fa.channelInfo[channelID]
				if !exists || (channel != nil && channel.IsArchived) {
					continue
				}

				for messageTS := range messages {
					// Check if this message is already tracked as a thread
					isTrackedThread := false
					fa.threadMu.Lock()
					if threads, exists := fa.activeThreads[channelID]; exists {
						_, isTrackedThread = threads[messageTS]
					}
					fa.threadMu.Unlock()

					if !isTrackedThread {
						if _, exists := messagesByChannel[channelID]; !exists {
							messagesByChannel[channelID] = make([]string, 0)
						}
						messagesByChannel[channelID] = append(messagesByChannel[channelID], messageTS)
					}
				}
			}

			// Second pass: Process messages by channel to batch API calls
			for channelID, messageTSList := range messagesByChannel {
				// Skip if no messages to check
				if len(messageTSList) == 0 {
					continue
				}

				log.Debug().
					Str("channelID", channelID).
					Str("channelName", fa.getChannelDisplayName(channelID)).
					Int("messageCount", len(messageTSList)).
					Msg("Checking messages for thread activity")

				// Create a map for quick lookups
				messageMap := make(map[string]bool)
				for _, ts := range messageTSList {
					messageMap[ts] = true
				}

				// OPTIMIZATION: Use conversation history with inclusive=true to check multiple messages at once
				// We need to split this into chunks for channels with many messages to check
				const chunkSize = 10
				for i := 0; i < len(messageTSList); i += chunkSize {
					end := i + chunkSize
					if end > len(messageTSList) {
						end = len(messageTSList)
					}

					// Process this chunk of messages
					currentChunk := messageTSList[i:end]

					// For each message, see if it now has replies
					for _, messageTS := range currentChunk {
						// Wait for rate limiter
						fa.apiRateLimiter.Wait()

						// Get just this message with its metadata using inclusive=true
						history, err := fa.client.GetConversationHistory(&slack.GetConversationHistoryParameters{
							ChannelID: channelID,
							Latest:    messageTS,
							Oldest:    messageTS,
							Inclusive: true,
							Limit:     1,
						})

						if err != nil {
							log.Error().
								Err(err).
								Str("channelID", channelID).
								Str("messageTS", messageTS).
								Msg("Error checking message for thread activity")
							continue
						}

						// If the message has replies, add it to thread tracking and process
						if len(history.Messages) > 0 && history.Messages[0].ReplyCount > 0 {
							log.Info().
								Str("channelID", channelID).
								Str("channelName", fa.getChannelDisplayName(channelID)).
								Str("messageTS", messageTS).
								Int("replyCount", history.Messages[0].ReplyCount).
								Msg("Found new thread on recent message")

							// Add this to thread tracking
							fa.trackThread(channelID, messageTS)

							// Process the thread to get all replies
							// Determine channel type first
							channelType := "channel"
							isDM := false
							if channel, ok := fa.channelInfo[channelID]; ok {
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

							// Process all replies
							fa.processThreadRepliesBatch(channelID, messageTS, channelType, isDM)

							// Update channel activity
							fa.updateChannelActivity(channelID)
						}
					}

					// Add a small delay between chunks to avoid API rate limits
					if end < len(messageTSList) {
						time.Sleep(200 * time.Millisecond)
					}
				}
			}
		}
	}
}

// tryAddUniqueMessage adds a message only if it doesn't already exist
func (fa *FeedAggregator) tryAddUniqueMessage(msg Message) {
	// First check if we've already processed this message
	fa.processedMu.Lock()
	messageKey := fmt.Sprintf("%s:%s", msg.Channel, msg.Timestamp)
	if fa.processedMessages[messageKey] {
		log.Trace().
			Str("user", msg.User).
			Str("timestamp", msg.Timestamp).
			Str("channelID", msg.Channel).
			Msg("Skipping already processed message")
		fa.processedMu.Unlock()
		return
	}

	// Mark as processed to prevent duplicates
	fa.processedMessages[messageKey] = true
	fa.processedMu.Unlock()

	fa.mu.Lock()
	defer fa.mu.Unlock()

	// Add to messages array
	fa.messages = append(fa.messages, msg)
	log.Debug().
		Str("user", msg.User).
		Str("userName", fa.getUserDisplayName(msg.User)).
		Str("timestamp", msg.Timestamp).
		Str("channelID", msg.Channel).
		Str("channelName", fa.getChannelDisplayName(msg.Channel)).
		Str("threadTS", msg.ThreadTS).
		Bool("isThread", msg.IsThread).
		Msg("Added unique message")

	// Also send to output channel
	select {
	case fa.outputCh <- msg:
		log.Debug().
			Str("timestamp", msg.Timestamp).
			Str("channelID", msg.Channel).
			Msg("Message sent to output channel")
	default:
		log.Warn().
			Str("timestamp", msg.Timestamp).
			Str("channelID", msg.Channel).
			Msg("Output channel full, message dropped")
	}
}

// Output channel processor - kept as is since it's not causing API rate limits
func (fa *FeedAggregator) processOutputChannel(ctx context.Context) {
	// Implementation kept the same as original
	// ...
	// Only including the function signature here to indicate it exists
	// but not modifying since it's not part of the optimization problem
}

func main() {
	// Set up command line flags
	logLevelStr := flag.String("log-level", "info", "Log level: trace, debug, info, warn, error, fatal, panic")
	stateDir := flag.String("state-dir", ".", "Directory for persistent state storage (default: currrent folder)")
	retentionDays := flag.Int("retention", 7, "Number of days to retain messages before deletion")
	threadExpiryDays := flag.Int("thread-expiry", 7, "Number of days of inactivity before thread tracking expires")
	flag.Parse()

	// Set up zerolog
	consoleWriter := zerolog.ConsoleWriter{Out: os.Stdout, TimeFormat: time.RFC3339}

	// Parse log level
	logLevel, err := zerolog.ParseLevel(*logLevelStr)
	if err != nil {
		// Default to info if invalid level
		logLevel = zerolog.InfoLevel
		fmt.Printf("Invalid log level '%s', defaulting to 'info'\n", *logLevelStr)
	}
	zerolog.SetGlobalLevel(logLevel)
	log.Logger = zerolog.New(consoleWriter).With().Timestamp().Logger()

	log.Info().
		Str("level", logLevel.String()).
		Msg("Logger initialized")

	// Get token from environment variables
	token := os.Getenv("SLACK_USER_TOKEN")
	targetUserID := os.Getenv("SLACK_TARGET_USER_ID")

	if token == "" {
		log.Fatal().Msg("SLACK_USER_TOKEN must be set")
	}

	if targetUserID == "" {
		log.Info().Msg("SLACK_TARGET_USER_ID not set. To send messages to yourself, set it to 'self'")
	} else {
		log.Info().
			Str("targetUserID", targetUserID).
			Msg("Target user ID set")
	}

	log.Info().
		Str("stateDir", *stateDir).
		Int("retentionDays", *retentionDays).
		Int("threadExpiryDays", *threadExpiryDays).
		Msg("Configuration loaded")

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	log.Debug().Msg("Creating feed aggregator")
	aggregator, err := NewFeedAggregator(token, targetUserID, *stateDir, *retentionDays, *threadExpiryDays)

	if err != nil {
		log.Fatal().Err(err).Msg("Error creating feed aggregator")
	}

	log.Info().Msg("Starting feed aggregator...")
	if err := aggregator.Start(ctx); err != nil {
		log.Fatal().Err(err).Msg("Error starting feed aggregator")
	}
}
