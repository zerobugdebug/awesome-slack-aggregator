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
	}

	log.Info().Int("channel_count", memberChannels).Msg("Channels loaded")

	return nil
}

// pollForMessages periodically checks channels for new messages
func (fa *FeedAggregator) pollForMessages(ctx context.Context) {
	ticker := time.NewTicker(2 * time.Second)
	defer ticker.Stop()

	// Track the latest timestamp we've processed for each channel
	latestTimestamps := make(map[string]string)

	// Initialize from persistent storage or use current time
	now := fmt.Sprintf("%d.000000", time.Now().Add(-12*60*time.Minute).Unix())
	log.Debug().Str("since", now).Msg("Initializing message polling from timestamp")

	// Create a slice of channel IDs to process (only where user is a member)
	channelIDs := make([]string, 0, len(fa.channelInfo))
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

		channelIDs = append(channelIDs, channelID)

		// Get timestamp from persistent storage or use default
		savedTS := fa.stateManager.GetLatestTimestamp(channelID)
		if savedTS != "" {
			latestTimestamps[channelID] = savedTS
			log.Debug().
				Str("channelID", channelID).
				Str("channelName", fa.getChannelDisplayName(channelID)).
				Str("savedTimestamp", savedTS).
				Msg("Loaded timestamp from persistent storage")
		} else {
			latestTimestamps[channelID] = now
			log.Debug().
				Str("channelID", channelID).
				Str("channelName", fa.getChannelDisplayName(channelID)).
				Str("defaultTimestamp", now).
				Msg("Using default timestamp")
		}

		log.Trace().
			Str("channelID", channelID).
			Str("channelName", fa.getChannelDisplayName(channelID)).
			Msg("Added channel to polling list")
	}

	log.Info().
		Int("channel_count", len(channelIDs)).
		Str("interval", "1s").
		Msg("Starting message polling")

	// Track which channel to process next
	currentChannelIndex := 0

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

			// Get the next channel ID to process
			channelID := channelIDs[currentChannelIndex]
			channel, exists := fa.channelInfo[channelID]
			log.Trace().
				Str("channelID", channelID).
				Str("channelName", fa.getChannelDisplayName(channelID)).
				Int("index", currentChannelIndex).
				Int("total", len(channelIDs)).
				Msg("Processing channel")

			// Update the index for next time
			currentChannelIndex = (currentChannelIndex + 1) % len(channelIDs)

			// Skip if the channel doesn't exist or is archived
			if !exists || (channel != nil && channel.IsArchived) {
				log.Debug().
					Str("channelID", channelID).
					Str("channelName", fa.getChannelDisplayName(channelID)).
					Msg("Skipping channel (archived or inaccessible)")
				continue
			}

			// Get history since the last check
			params := &slack.GetConversationHistoryParameters{
				ChannelID: channelID,
				Oldest:    latestTimestamps[channelID],
				Limit:     100,
			}

			log.Trace().
				Str("channelID", channelID).
				Str("channelName", fa.getChannelDisplayName(channelID)).
				Str("oldest", latestTimestamps[channelID]).
				Msg("Fetching conversation history")

			history, err := fa.client.GetConversationHistory(params)
			if err != nil {
				log.Error().
					Err(err).
					Str("channelID", channelID).
					Str("channelName", fa.getChannelDisplayName(channelID)).
					Msg("Error getting history for channel")
				continue
			}

			if len(history.Messages) > 0 {
				log.Debug().
					Str("channelID", channelID).
					Str("channelName", fa.getChannelDisplayName(channelID)).
					Int("message_count", len(history.Messages)).
					Msg("Found new messages")

				// Process messages (newest first)
				for i := len(history.Messages) - 1; i >= 0; i-- {
					msg := history.Messages[i]
					// Skip messages created by this app (look for our marker)
					if fa.messageFormatter.IsAppMessage(msg.Text) {
						log.Debug().
							Str("timestamp", msg.Timestamp).
							Str("channelID", channelID).
							Msg("Skipping message created by this app")
						continue
					}

					// Determine channel type
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

					message := Message{
						User:        msg.User,
						Channel:     channelID,
						Text:        msg.Text,
						ThreadTS:    msg.ThreadTimestamp,
						Timestamp:   msg.Timestamp,
						IsThread:    msg.ThreadTimestamp != "",
						IsDM:        isDM,
						ChannelType: channelType,
					}

					log.Debug().
						Str("user", msg.User).
						Str("userName", fa.getUserDisplayName(msg.User)).
						Str("timestamp", msg.Timestamp).
						Str("channelID", channelID).
						Str("channelName", fa.getChannelDisplayName(channelID)).
						Str("type", channelType).
						Bool("isThread", msg.ThreadTimestamp != "").
						Msg("Adding message")

					fa.addMessage(message)

					// If message has threads, get thread replies
					if msg.ReplyCount > 0 {
						log.Debug().
							Str("messageTS", msg.Timestamp).
							Str("channelID", channelID).
							Str("channelName", fa.getChannelDisplayName(channelID)).
							Int("replyCount", msg.ReplyCount).
							Msg("Fetching thread replies")

						replies, hasMore, nextCursor, err := fa.client.GetConversationReplies(&slack.GetConversationRepliesParameters{
							ChannelID: channelID,
							Timestamp: msg.Timestamp,
						})

						if err != nil {
							log.Error().
								Err(err).
								Str("channelID", channelID).
								Str("channelName", fa.getChannelDisplayName(channelID)).
								Str("messageTS", msg.Timestamp).
								Msg("Error getting thread replies")
							continue
						}

						// Process thread replies
						log.Trace().
							Int("replyCount", len(replies)).
							Str("channelID", channelID).
							Str("channelName", fa.getChannelDisplayName(channelID)).
							Msg("Processing thread replies")
						for _, reply := range replies {
							// Skip if it's the parent message or from self
							if reply.Timestamp == msg.Timestamp || reply.User == fa.userID {
								log.Trace().
									Str("replyTS", reply.Timestamp).
									Msg("Skipping parent message or own reply")
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

							log.Debug().
								Str("user", reply.User).
								Str("userName", fa.getUserDisplayName(reply.User)).
								Str("timestamp", reply.Timestamp).
								Str("channelID", channelID).
								Str("channelName", fa.getChannelDisplayName(channelID)).
								Str("threadTS", reply.ThreadTimestamp).
								Msg("Adding thread reply")

							fa.addMessage(threadMessage)
						}

						// Handle pagination for large threads if needed
						for hasMore {
							log.Debug().
								Str("messageTS", msg.Timestamp).
								Str("channelID", channelID).
								Str("channelName", fa.getChannelDisplayName(channelID)).
								Str("cursor", nextCursor).
								Msg("Fetching more thread replies")

							moreReplies, moreHasMore, nextCursorNew, err := fa.client.GetConversationReplies(&slack.GetConversationRepliesParameters{
								ChannelID: channelID,
								Timestamp: msg.Timestamp,
								Cursor:    nextCursor,
							})

							if err != nil {
								log.Error().
									Err(err).
									Str("channelID", channelID).
									Str("channelName", fa.getChannelDisplayName(channelID)).
									Str("messageTS", msg.Timestamp).
									Str("cursor", nextCursor).
									Msg("Error getting additional thread replies")
								break
							}

							nextCursor = nextCursorNew

							// Process additional replies
							log.Trace().
								Int("additionalReplyCount", len(moreReplies)).
								Str("channelID", channelID).
								Str("channelName", fa.getChannelDisplayName(channelID)).
								Msg("Processing additional thread replies")
							for _, reply := range moreReplies {
								// Skip if it's already processed or from self
								if reply.Timestamp == msg.Timestamp || reply.User == fa.userID {
									log.Trace().
										Str("replyTS", reply.Timestamp).
										Msg("Skipping parent message or own reply")
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

								log.Debug().
									Str("user", reply.User).
									Str("userName", fa.getUserDisplayName(reply.User)).
									Str("timestamp", reply.Timestamp).
									Str("channelID", channelID).
									Str("channelName", fa.getChannelDisplayName(channelID)).
									Str("threadTS", reply.ThreadTimestamp).
									Msg("Adding additional thread reply")

								fa.addMessage(threadMessage)
							}

							hasMore = moreHasMore
						}

						// Track this thread for future updates
						fa.trackThread(channelID, msg.Timestamp)
					}
					// Also track this message as recent for future thread detection
					fa.stateManager.TrackRecentMessage(channelID, msg.Timestamp)
				}

				// Update latest timestamp for this channel
				// We take the timestamp of the newest message
				latestTimestamps[channelID] = history.Messages[0].Timestamp

				// Save to persistent storage
				fa.stateManager.SetLatestTimestamp(channelID, latestTimestamps[channelID])

				log.Debug().
					Str("channelID", channelID).
					Str("channelName", fa.getChannelDisplayName(channelID)).
					Str("newLatestTS", latestTimestamps[channelID]).
					Msg("Updated latest timestamp for channel")
			}
		}
	}
}

// addMessage adds a message to the feed
func (fa *FeedAggregator) addMessage(msg Message) {
	fa.tryAddUniqueMessage(msg)

	// fa.mu.Lock()
	// defer fa.mu.Unlock()

	// fa.messages = append(fa.messages, msg)
	// log.Trace().
	// 	Str("user", msg.User).
	// 	Str("userName", fa.getUserDisplayName(msg.User)).
	// 	Str("channel", msg.Channel).
	// 	Str("channelName", fa.getChannelDisplayName(msg.Channel)).
	// 	Str("timestamp", msg.Timestamp).
	// 	Msg("Message added to internal store")

	// // Also send to output channel
	// select {
	// case fa.outputCh <- msg:
	// 	log.Debug().
	// 		Str("timestamp", msg.Timestamp).
	// 		Str("channelID", msg.Channel).
	// 		Str("channelName", fa.getChannelDisplayName(msg.Channel)).
	// 		Msg("Message sent to output channel")
	// default:
	// 	log.Warn().
	// 		Str("timestamp", msg.Timestamp).
	// 		Str("channelID", msg.Channel).
	// 		Str("channelName", fa.getChannelDisplayName(msg.Channel)).
	// 		Msg("Output channel full, message dropped")
	// }
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

// Helper method to check a specific thread for updates
func (fa *FeedAggregator) checkThreadUpdates(channelID, threadTS string, lastKnownActivity time.Time) {
	// Skip if the channel doesn't exist or is archived
	channel, exists := fa.channelInfo[channelID]
	if !exists || (channel != nil && channel.IsArchived) {
		log.Debug().
			Str("channelID", channelID).
			Str("channelName", fa.getChannelDisplayName(channelID)).
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
		Str("channelName", fa.getChannelDisplayName(channelID)).
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

	replies, hasMore, nextCursor, err := fa.client.GetConversationReplies(params)

	if err != nil {
		log.Error().
			Err(err).
			Str("channelID", channelID).
			Str("channelName", fa.getChannelDisplayName(channelID)).
			Str("threadTS", threadTS).
			Msg("Error getting thread updates")
		return
	}

	// Process replies (if any)
	hasNewActivity := false
	newestActivity := lastKnownActivity

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

			// Add to message feed
			fa.tryAddUniqueMessage(threadMessage)
		}
	}

	// Handle pagination for large threads if needed
	for hasMore {
		log.Debug().
			Str("threadTS", threadTS).
			Str("channelID", channelID).
			Str("channelName", fa.getChannelDisplayName(channelID)).
			Str("cursor", nextCursor).
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

		moreReplies, moreHasMore, nextCursorNew, err := fa.client.GetConversationReplies(params)

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

				// Add to message feed
				fa.tryAddUniqueMessage(threadMessage)
			}
		}

		hasMore = moreHasMore
	}

	// Update thread activity time if there was new activity
	if hasNewActivity {
		fa.threadMu.Lock()
		if channelThreads, exists := fa.activeThreads[channelID]; exists {
			if info, exists := channelThreads[threadTS]; exists {
				info.LastActivity = newestActivity
				channelThreads[threadTS] = info
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
			Msg("Updated thread activity timestamp")
	}

	// Also update the checked time
	now := time.Now()
	fa.threadMu.Lock()
	if channelThreads, exists := fa.activeThreads[channelID]; exists {
		if info, exists := channelThreads[threadTS]; exists {
			info.LastChecked = now
			channelThreads[threadTS] = info
		}
	}
	fa.threadMu.Unlock()

	// Update the last check time in persistent storage
	fa.stateManager.UpdateThreadTimestamp(channelID, threadTS, now)
}

// pollForThreadUpdates periodically checks for updates to active threads
func (fa *FeedAggregator) pollForThreadUpdates(ctx context.Context) {
	// Use persistent storage to initialize active threads
	fa.activeThreads = fa.stateManager.GetActiveThreads()

	// Thread polling will now use a tiered approach
	ticker := time.NewTicker(10 * time.Second) // Base polling interval
	defer ticker.Stop()

	// Define thread activity tiers for polling frequency
	veryActiveThreshold := 6 * time.Hour    // Threads with activity in the last 6 hours
	activeThreshold := 24 * time.Hour       // Threads with activity in the last 24 hours
	moderateThreshold := 3 * 24 * time.Hour // Threads with activity in the last 3 days
	lowThreshold := 7 * 24 * time.Hour      // Threads with activity in the last week

	// Expiry threshold
	expiryThreshold := time.Duration(fa.threadExpiryDays) * 24 * time.Hour

	log.Info().
		Str("baseInterval", "10s").
		Dur("veryActiveThreshold", veryActiveThreshold).
		Dur("activeThreshold", activeThreshold).
		Dur("moderateThreshold", moderateThreshold).
		Dur("lowThreshold", lowThreshold).
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
			activeThreadsCopy := fa.getActiveThreadsSnapshot()

			now := time.Now()
			threadCount := 0
			processedCount := 0
			expiredCount := 0

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
							Str("channelName", fa.getChannelDisplayName(channelID)).
							Str("threadTS", threadTS).
							Dur("inactiveDuration", timeSinceActivity).
							Msg("Expiring inactive thread")

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

						// Remove from last polled
						delete(lastPolled[channelID], threadTS)
						if len(lastPolled[channelID]) == 0 {
							delete(lastPolled, channelID)
						}

						expiredCount++
						continue
					}

					// Determine polling frequency based on activity
					var pollInterval time.Duration

					if timeSinceActivity <= veryActiveThreshold {
						pollInterval = 30 * time.Second // Very active: check every minute
					} else if timeSinceActivity <= activeThreshold {
						pollInterval = 3 * time.Minute // Active: check every 5 minutes
					} else if timeSinceActivity <= moderateThreshold {
						pollInterval = 15 * time.Minute // Moderate: check every 15 minutes
					} else {
						pollInterval = 1 * time.Hour // Low: check every hour
					}

					// Check if it's time to poll this thread
					lastThreadPoll, exists := lastPolled[channelID][threadTS]
					if !exists || now.Sub(lastThreadPoll) >= pollInterval {
						// Time to check this thread
						fa.checkThreadUpdates(channelID, threadTS, info.LastActivity)

						// Update last poll time
						lastPolled[channelID][threadTS] = now
						processedCount++
					}
				}
			}

			if threadCount > 0 {
				log.Debug().
					Int("totalThreads", threadCount).
					Int("processedThreads", processedCount).
					Int("expiredThreads", expiredCount).
					Msg("Checked for updates to active threads")
			}
		}
	}
}

// processOutputChannel handles messages sent to the output channel
func (fa *FeedAggregator) processOutputChannel(ctx context.Context) {
	log.Debug().Msg("Starting output channel processor")

	// Target for feed messages
	var targetChannelID string

	// If a specific target user/channel was specified
	if fa.feedTargetUser != "" {
		log.Debug().
			Str("targetUser", fa.feedTargetUser).
			Msg("Target user specified, looking for appropriate channel")

		// Special case: if the target is "self", find the user's own DM with Slackbot
		// This is a workaround if we don't have permission to open DMs
		if fa.feedTargetUser == "self" {
			log.Debug().Msg("Target user is 'self', looking for Slackbot DM")

			// Look for the slackbot DM as a fallback
			for channelID, channel := range fa.channelInfo {
				if channel.IsIM && channel.User == "USLACKBOT" {
					targetChannelID = channelID
					log.Info().
						Str("channelID", targetChannelID).
						Str("channelName", fa.getChannelDisplayName(targetChannelID)).
						Msg("Found Slackbot DM channel for feed messages")

					// Send welcome message
					welcomeText := "👋 *Feed Aggregator is now active!*\nI'll send all aggregated messages to this conversation with Slackbot."
					_, _, err := fa.client.PostMessage(targetChannelID, slack.MsgOptionText(welcomeText, false))
					if err != nil {
						log.Error().
							Err(err).
							Str("channelID", targetChannelID).
							Str("channelName", fa.getChannelDisplayName(targetChannelID)).
							Msg("Error sending welcome message")
					} else {
						log.Debug().
							Str("channelID", targetChannelID).
							Str("channelName", fa.getChannelDisplayName(targetChannelID)).
							Msg("Welcome message sent successfully")
					}
					break
				}
			}

			if targetChannelID == "" {
				log.Warn().Msg("Couldn't find Slackbot DM, will only log to console")
			}
		} else {
			log.Debug().
				Str("targetUser", fa.feedTargetUser).
				Str("targetUserName", fa.getUserDisplayName(fa.feedTargetUser)).
				Msg("Looking for existing DM with target user")

			// Try to find an existing DM with the target user
			for channelID, channel := range fa.channelInfo {
				if channel.IsIM && channel.User == fa.feedTargetUser {
					targetChannelID = channelID
					log.Info().
						Str("channelID", targetChannelID).
						Str("channelName", fa.getChannelDisplayName(targetChannelID)).
						Str("targetUser", fa.feedTargetUser).
						Str("targetUserName", fa.getUserDisplayName(fa.feedTargetUser)).
						Msg("Found existing DM channel with user for feed messages")

					// Send welcome message
					welcomeText := "👋 *Feed Aggregator is now active!*\nI'll send all aggregated messages to this conversation."
					_, _, err := fa.client.PostMessage(targetChannelID, slack.MsgOptionText(welcomeText, false))
					if err != nil {
						log.Error().
							Err(err).
							Str("channelID", targetChannelID).
							Str("channelName", fa.getChannelDisplayName(targetChannelID)).
							Msg("Error sending welcome message")
					} else {
						log.Debug().
							Str("channelID", targetChannelID).
							Str("channelName", fa.getChannelDisplayName(targetChannelID)).
							Msg("Welcome message sent successfully")
					}
					break
				}
			}

			if targetChannelID == "" {
				log.Info().
					Str("targetUser", fa.feedTargetUser).
					Str("targetUserName", fa.getUserDisplayName(fa.feedTargetUser)).
					Msg("Couldn't find existing DM with user, attempting to open one")

				// Attempt to open a DM channel with the target user
				try, _, _, err := fa.client.OpenConversation(&slack.OpenConversationParameters{
					Users: []string{fa.feedTargetUser},
				})
				if err != nil {
					log.Error().
						Err(err).
						Str("targetUser", fa.feedTargetUser).
						Str("targetUserName", fa.getUserDisplayName(fa.feedTargetUser)).
						Msg("Error opening DM with user")
					log.Warn().Msg("Will only log messages to console. To enable DM functionality, add im:write scope to your token.")
				} else {
					targetChannelID = try.ID
					log.Info().
						Str("channelID", targetChannelID).
						Str("channelName", fa.getChannelDisplayName(targetChannelID)).
						Str("targetUser", fa.feedTargetUser).
						Str("targetUserName", fa.getUserDisplayName(fa.feedTargetUser)).
						Msg("Opened DM channel with user for feed messages")

					// Send welcome message
					welcomeText := "👋 *Feed Aggregator is now active!*\nI'll send all aggregated messages to this conversation."
					_, _, err := fa.client.PostMessage(targetChannelID, slack.MsgOptionText(welcomeText, false))
					if err != nil {
						log.Error().
							Err(err).
							Str("channelID", targetChannelID).
							Str("channelName", fa.getChannelDisplayName(targetChannelID)).
							Msg("Error sending welcome message")
					} else {
						log.Debug().
							Str("channelID", targetChannelID).
							Str("channelName", fa.getChannelDisplayName(targetChannelID)).
							Msg("Welcome message sent successfully")
					}
				}
			}
		}
	} else {
		log.Info().Msg("No target user specified, will only log messages to console")
	}

	// Keep track of when we last sent a message to avoid flooding
	lastMessageTime := time.Now()
	batchedMessages := make([]Message, 0)
	const batchThreshold = 5 // Number of messages to collect before sending
	const minTimeBetweenBatches = 10 * time.Second

	// Add a timer for flushing based on time
	flushTicker := time.NewTicker(minTimeBetweenBatches / 2) // Check every 5 seconds
	defer flushTicker.Stop()

	log.Debug().
		Int("batchThreshold", batchThreshold).
		Str("minTimeBetweenBatches", minTimeBetweenBatches.String()).
		Msg("Configured message batching")

	for {
		select {
		case <-ctx.Done():
			// Flush any remaining messages before exiting
			if len(batchedMessages) > 0 {
				sendBatch(fa, batchedMessages, targetChannelID)
			}
			log.Debug().Msg("Context done, stopping output processor")
			return
		case <-flushTicker.C:
			// Check if we need to flush due to time
			if len(batchedMessages) > 0 && time.Since(lastMessageTime) > minTimeBetweenBatches {
				log.Debug().
					Int("batchSize", len(batchedMessages)).
					Str("timeSinceLastBatch", time.Since(lastMessageTime).String()).
					Msg("Flushing message batch due to time")

				sendBatch(fa, batchedMessages, targetChannelID)
				batchedMessages = make([]Message, 0)
				lastMessageTime = time.Now()
			}
		case msg := <-fa.outputCh:
			log.Debug().
				Str("user", msg.User).
				Str("userName", fa.getUserDisplayName(msg.User)).
				Str("channel", msg.Channel).
				Str("channelName", fa.getChannelDisplayName(msg.Channel)).
				Str("timestamp", msg.Timestamp).
				Msg("Processing output message")

			// Format the message
			//userName := msg.User
			if user, ok := fa.userInfo[msg.User]; ok {
				userName := user.RealName
				log.Trace().
					Str("userID", msg.User).
					Str("userName", userName).
					Msg("Resolved user name")
			} else {
				log.Trace().
					Str("userID", msg.User).
					Msg("Could not resolve user name")
			}

			//channelName := msg.Channel
			if channel, ok := fa.channelInfo[msg.Channel]; ok {
				channelName := channel.Name
				log.Trace().
					Str("channelID", msg.Channel).
					Str("channelName", channelName).
					Msg("Resolved channel name")

				// For DMs, use the other user's name
				if channel.IsIM {
					for userID := range fa.userInfo {
						if channel.User == userID {
							log.Trace().
								Str("channelID", msg.Channel).
								Str("dmWithUser", fa.userInfo[userID].RealName).
								Msg("Resolved DM channel name")
							break
						}
					}
				}
			} else {
				log.Trace().
					Str("channelID", msg.Channel).
					Msg("Could not resolve channel name")
			}

			// Create message link - format: https://team-domain.slack.com/archives/CHANNEL_ID/p{TIMESTAMP_WITHOUT_DOT}
			// Need to replace the dot in timestamp with empty string
			linkTimestamp := strings.Replace(msg.Timestamp, ".", "", 1)
			messageLink := fmt.Sprintf("https://%s.slack.com/archives/%s/p%s",
				fa.teamDomain,
				msg.Channel,
				linkTimestamp)

			log.Debug().
				Str("timestamp", msg.Timestamp).
				Str("linkTimestamp", linkTimestamp).
				Str("messageLink", messageLink).
				Msg("Created message link")

			// Always log to console
			log.Info().
				Str("timestamp", msg.Timestamp).
				Str("user", fa.getUserDisplayName(msg.User)).
				Str("channel", fa.getChannelDisplayName(msg.Channel)).
				Str("channelID", msg.Channel).
				Str("type", func() string {
					if msg.IsThread {
						return "thread reply"
					}
					return "message"
				}()).
				Str("text", msg.Text).
				Msg("Received message")

			// If we have a target channel, send there too
			if targetChannelID != "" {
				log.Debug().
					Str("targetChannelID", targetChannelID).
					Str("targetChannelName", fa.getChannelDisplayName(targetChannelID)).
					Msg("Target channel found for message")

				// Add to batch
				batchedMessages = append(batchedMessages, msg)
				log.Trace().
					Int("batchSize", len(batchedMessages)).
					Msg("Added message to batch")

				// Send batch if we have enough messages
				if len(batchedMessages) >= batchThreshold {
					log.Debug().
						Int("batchSize", len(batchedMessages)).
						Msg("Sending message batch due to size threshold")

					sendBatch(fa, batchedMessages, targetChannelID)
					batchedMessages = make([]Message, 0)
					lastMessageTime = time.Now()
				}

			} else {
				log.Debug().Msg("No target channel found, skipping message send")
			}
		}
	}
}

// Extract the batch sending logic to a helper function to avoid code duplication
func sendBatch(fa *FeedAggregator, batchMessages []Message, targetChannelID string) {
	// Process each message in the batch
	for _, batchMsg := range batchMessages {
		userName := fa.getUserDisplayName(batchMsg.User)
		channelName := fa.getChannelDisplayName(batchMsg.Channel)

		// Format message with our marker
		messageText := fa.messageFormatter.FormatMessage(
			fa.teamDomain,
			batchMsg.Channel,
			batchMsg.Timestamp,
			userName,
			channelName,
		)

		// Send to target channel
		_, timestamp, err := fa.client.PostMessage(
			targetChannelID,
			slack.MsgOptionText(messageText, false),
		)

		if err != nil {
			log.Error().
				Err(err).
				Str("targetChannelID", targetChannelID).
				Str("targetChannelName", fa.getChannelDisplayName(targetChannelID)).
				Msg("Error sending message to channel")
		} else {
			log.Debug().
				Str("targetChannelID", targetChannelID).
				Str("targetChannelName", fa.getChannelDisplayName(targetChannelID)).
				Str("timestamp", timestamp).
				Msg("Message sent successfully")

			// Track this message for retention
			fa.stateManager.TrackSentMessage(timestamp, targetChannelID)
		}
	}
}

// getActiveThreadsSnapshot returns a copy of the active threads map
// to avoid deadlocks when iterating over it
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

// Add a new method to the FeedAggregator to periodically check recent messages for thread creation
func (fa *FeedAggregator) pollForNewThreads(ctx context.Context) {
	// How far back to track recent messages for possible thread creation
	messageTrackingPeriod := 24 * time.Hour // Track messages for 24 hours

	// Check recent messages every minute
	ticker := time.NewTicker(1 * time.Minute)
	defer ticker.Stop()

	log.Info().
		Dur("trackingPeriod", messageTrackingPeriod).
		Str("checkInterval", "1m").
		Msg("Starting recent message thread detection")

	for {
		select {
		case <-ctx.Done():
			log.Debug().Msg("Context done, stopping recent message polling")
			return
		case <-ticker.C:
			// First, clean up old messages from tracking
			removed := fa.stateManager.CleanupOldRecentMessages(messageTrackingPeriod)
			if removed > 0 {
				log.Debug().Int("removedCount", removed).Msg("Removed old messages from recent tracking")
			}

			// Get a snapshot of recent messages
			recentMessages := fa.stateManager.GetRecentMessages()

			// Track stats
			checkedCount := 0
			newThreadsFound := 0

			// Check each recent message for threads
			for channelID, messages := range recentMessages {
				// Skip if the channel doesn't exist or is archived
				channel, exists := fa.channelInfo[channelID]
				if !exists || (channel != nil && channel.IsArchived) {
					continue
				}

				for messageTS := range messages {
					checkedCount++

					// Check if this message is already being tracked as a thread
					isTrackedThread := false
					fa.threadMu.Lock()
					if threads, exists := fa.activeThreads[channelID]; exists {
						_, isTrackedThread = threads[messageTS]
					}
					fa.threadMu.Unlock()

					if isTrackedThread {
						// Already tracking this thread, skip
						continue
					}

					// Get message info from Slack to check for thread replies
					log.Debug().
						Str("channelID", channelID).
						Str("channelName", fa.getChannelDisplayName(channelID)).
						Str("messageTS", messageTS).
						Msg("Checking recent message for new thread activity")

					// Use GetConversationHistory with inclusive oldest/latest to get just this message
					history, err := fa.client.GetConversationHistory(&slack.GetConversationHistoryParameters{
						ChannelID: channelID,
						Oldest:    messageTS,
						Latest:    messageTS,
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

					// If we got the message and it now has thread replies
					if len(history.Messages) > 0 && history.Messages[0].ReplyCount > 0 {
						log.Info().
							Str("channelID", channelID).
							Str("channelName", fa.getChannelDisplayName(channelID)).
							Str("messageTS", messageTS).
							Int("replyCount", history.Messages[0].ReplyCount).
							Msg("Found new thread on recent message")

						// Add this to thread tracking
						fa.trackThread(channelID, messageTS)

						// Get all the thread replies and process them
						fa.checkThreadUpdates(channelID, messageTS, time.Time{}) // Empty time means get all replies

						newThreadsFound++
					}
				}
			}

			if checkedCount > 0 {
				log.Debug().
					Int("checkedMessages", checkedCount).
					Int("newThreadsFound", newThreadsFound).
					Msg("Completed check for new threads on recent messages")
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
			Str("userName", fa.getUserDisplayName(msg.User)).
			Str("timestamp", msg.Timestamp).
			Str("channelID", msg.Channel).
			Str("channelName", fa.getChannelDisplayName(msg.Channel)).
			Msg("Skipping already processed message")
		fa.processedMu.Unlock()
		return
	}

	// Mark as processed to prevent duplicates
	fa.processedMessages[messageKey] = true
	fa.processedMu.Unlock()

	fa.mu.Lock()
	defer fa.mu.Unlock()

	// Also check the messages array as a secondary precaution
	for _, existingMsg := range fa.messages {
		if existingMsg.Timestamp == msg.Timestamp && existingMsg.Channel == msg.Channel {
			// Message already exists, don't add it again
			log.Trace().
				Str("user", msg.User).
				Str("userName", fa.getUserDisplayName(msg.User)).
				Str("timestamp", msg.Timestamp).
				Str("channelID", msg.Channel).
				Str("channelName", fa.getChannelDisplayName(msg.Channel)).
				Msg("Skipping duplicate message")
			return
		}
	}

	// Message doesn't exist yet, add it
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
			Str("channelName", fa.getChannelDisplayName(msg.Channel)).
			Msg("Message sent to output channel")
	default:
		log.Warn().
			Str("timestamp", msg.Timestamp).
			Str("channelID", msg.Channel).
			Str("channelName", fa.getChannelDisplayName(msg.Channel)).
			Msg("Output channel full, message dropped")
	}
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

	// Set up state directory
	// if *stateDir == "" {
	// 	homeDir, err := os.UserHomeDir()
	// 	if err != nil {
	// 		log.Error().Err(err).Msg("Could not determine home directory")
	// 		*stateDir = ".slack-feed" // Use current directory as fallback
	// 	} else {
	// 		*stateDir = filepath.Join(homeDir, ".slack-feed")
	// 	}
	// }

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
