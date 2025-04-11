package aggregator

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/rs/zerolog/log"
	"github.com/slack-go/slack"

	"github.com/zerobugdebug/awesome-slack-aggregator/internal/message"
	"github.com/zerobugdebug/awesome-slack-aggregator/internal/models"
	slackClient "github.com/zerobugdebug/awesome-slack-aggregator/internal/slack"
	"github.com/zerobugdebug/awesome-slack-aggregator/internal/state"
)

// Aggregator aggregates messages from various sources
type Aggregator struct {
	client            *slack.Client
	messages          []models.Message
	channelInfo       map[string]*slack.Channel
	userInfo          map[string]*slack.User
	activeThreads     map[string]map[string]models.ThreadInfo // Map of channelID -> map of threadTS -> thread info
	threadExpiryDays  int                                     // Days after which inactive threads are expired
	mu                sync.Mutex
	threadMu          sync.Mutex // Separate mutex for thread operations
	outputCh          chan models.Message
	feedTargetUser    string
	userID            string // Current user's ID
	teamDomain        string // Slack team domain for creating links
	stateManager      *state.Manager
	messageFormatter  *message.Formatter
	messageRetainer   *message.Retainer
	processedMessages map[string]bool // Set of message IDs that have been processed
	processedMu       sync.Mutex
}

// New creates a new feed aggregator
func New(token string, targetUserID string, stateDir string, retentionDays int, threadExpiryDays int) (*Aggregator, error) {
	// Create Slack client
	client, err := slackClient.NewClient(token)
	if err != nil {
		return nil, fmt.Errorf("failed to create Slack client: %w", err)
	}

	// Initialize state manager
	stateManager, err := state.NewManager(stateDir)
	if err != nil {
		log.Error().Err(err).Str("stateDir", stateDir).Msg("Failed to initialize state manager")
		return nil, fmt.Errorf("failed to initialize state manager: %w", err)
	}

	// Create the feed aggregator
	agg := &Aggregator{
		client:            client.GetClient(),
		messages:          make([]models.Message, 0),
		channelInfo:       make(map[string]*slack.Channel),
		userInfo:          make(map[string]*slack.User),
		activeThreads:     make(map[string]map[string]models.ThreadInfo),
		outputCh:          make(chan models.Message, 100),
		feedTargetUser:    targetUserID,
		userID:            client.GetUserID(),
		teamDomain:        client.GetTeamDomain(),
		stateManager:      stateManager,
		messageFormatter:  message.NewFormatter(),
		processedMessages: make(map[string]bool),
		threadExpiryDays:  threadExpiryDays,
	}

	// Initialize message retainer
	agg.messageRetainer = message.NewRetainer(agg.client, stateManager, retentionDays)

	return agg, nil
}

// Start begins listening for messages
func (agg *Aggregator) Start(ctx context.Context) error {
	// Start the state manager
	agg.stateManager.Start()

	// Load initial channel and user information
	log.Debug().Msg("Loading initial channel and user data")
	if err := agg.loadInitialData(); err != nil {
		log.Error().Err(err).Msg("Failed to load initial data")
		return err
	}

	// Start the message retention manager
	log.Debug().Msg("Starting message retainer")
	agg.messageRetainer.Start(ctx)

	// Start the output processor
	log.Debug().Msg("Starting output processor")
	go agg.processOutputChannel(ctx)

	// Start thread polling with batch processing
	log.Debug().Msg("Starting batch thread polling")
	go agg.batchPollForThreadUpdates(ctx)

	// Start recent message polling for new threads with batching
	log.Debug().Msg("Starting batch recent message polling")
	go agg.batchPollForNewThreads(ctx)

	// Start message polling with optimized batching
	log.Debug().Msg("Starting optimized message polling")
	go agg.optimizedPollForMessages(ctx)

	log.Info().Msg("Feed aggregator started successfully")

	// This keeps the main thread running
	<-ctx.Done()
	log.Info().Msg("Context done, shutting down feed aggregator")

	// Stop components
	agg.messageRetainer.Stop()
	agg.stateManager.Stop()

	return nil
}

// loadInitialData loads channels and users info
func (agg *Aggregator) loadInitialData() error {
	log.Debug().Msg("Loading initial channel and user data")

	// Get only conversations that the user is a member of
	log.Debug().Msg("Fetching user conversations")
	conversations, nextCursor, err := agg.client.GetConversationsForUser(&slack.GetConversationsForUserParameters{
		Types:           []string{"public_channel", "private_channel", "mpim", "im"},
		Limit:           1000,
		ExcludeArchived: true,
		UserID:          agg.userID, // Only get conversations the current user is a member of
	})
	if err != nil {
		log.Error().Err(err).Msg("Failed to get user conversations")
		return fmt.Errorf("failed to get user conversations: %w", err)
	}

	// Handle pagination if there are more conversations
	for nextCursor != "" {
		log.Debug().Str("cursor", nextCursor).Msg("Fetching additional user conversations")
		var additionalConversations []slack.Channel
		additionalConversations, nextCursor, err = agg.client.GetConversationsForUser(&slack.GetConversationsForUserParameters{
			Types:           []string{"public_channel", "private_channel", "mpim", "im"},
			Limit:           1000,
			Cursor:          nextCursor,
			ExcludeArchived: true,
			UserID:          agg.userID,
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
	users, err := agg.client.GetUsers()
	if err != nil {
		log.Error().Err(err).Msg("Failed to get users")
		return fmt.Errorf("failed to get users: %w", err)
	}

	log.Info().Int("user_count", len(users)).Msg("Users fetched")
	for _, user := range users {
		agg.userInfo[user.ID] = &user
		log.Trace().Str("userID", user.ID).Str("name", user.RealName).Msg("Added user")
	}

	// Store all channels the user is a member of
	memberChannels := 0
	for _, channel := range conversations {
		agg.channelInfo[channel.ID] = &channel
		memberChannels++

		channelName := channel.Name
		// Log channel info
		channelType := "channel"
		if channel.IsIM {
			channelType = "direct message"
			for userID := range agg.userInfo {
				if channel.User == userID {
					channelName = fmt.Sprintf("DM with %s", agg.userInfo[userID].RealName)
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

// getChannelDisplayName returns a human-readable name for a channel
func (agg *Aggregator) getChannelDisplayName(channelID string) string {
	if channel, ok := agg.channelInfo[channelID]; ok {
		// For direct messages, use the other user's name
		if channel.IsIM {
			if user, ok := agg.userInfo[channel.User]; ok {
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
func (agg *Aggregator) getUserDisplayName(userID string) string {
	if user, ok := agg.userInfo[userID]; ok {
		return user.RealName
	}
	return userID
}

// GetMessages returns all aggregated messages
func (agg *Aggregator) GetMessages() []models.Message {
	agg.mu.Lock()
	defer agg.mu.Unlock()

	log.Debug().Int("count", len(agg.messages)).Msg("Getting all messages")

	// Create a copy to avoid race conditions
	result := make([]models.Message, len(agg.messages))
	copy(result, agg.messages)

	return result
}

// addMessage adds a message to the feed using the optimized method
func (agg *Aggregator) addMessage(msg models.Message) {
	agg.tryAddUniqueMessage(msg)
}

// tryAddUniqueMessage adds a message only if it doesn't already exist
func (agg *Aggregator) tryAddUniqueMessage(msg models.Message) {
	// First check if we've already processed this message
	agg.processedMu.Lock()
	messageKey := fmt.Sprintf("%s:%s", msg.Channel, msg.Timestamp)
	if agg.processedMessages[messageKey] {
		log.Trace().
			Str("user", msg.User).
			Str("userName", agg.getUserDisplayName(msg.User)).
			Str("timestamp", msg.Timestamp).
			Str("channelID", msg.Channel).
			Str("channelName", agg.getChannelDisplayName(msg.Channel)).
			Msg("Skipping already processed message")
		agg.processedMu.Unlock()
		return
	}

	// Mark as processed to prevent duplicates
	agg.processedMessages[messageKey] = true
	agg.processedMu.Unlock()

	agg.mu.Lock()
	defer agg.mu.Unlock()

	// Also check the messages array as a secondary precaution
	for _, existingMsg := range agg.messages {
		if existingMsg.Timestamp == msg.Timestamp && existingMsg.Channel == msg.Channel {
			// Message already exists, don't add it again
			log.Trace().
				Str("user", msg.User).
				Str("userName", agg.getUserDisplayName(msg.User)).
				Str("timestamp", msg.Timestamp).
				Str("channelID", msg.Channel).
				Str("channelName", agg.getChannelDisplayName(msg.Channel)).
				Msg("Skipping duplicate message")
			return
		}
	}

	// Message doesn't exist yet, add it
	agg.messages = append(agg.messages, msg)
	log.Debug().
		Str("user", msg.User).
		Str("userName", agg.getUserDisplayName(msg.User)).
		Str("timestamp", msg.Timestamp).
		Str("channelID", msg.Channel).
		Str("channelName", agg.getChannelDisplayName(msg.Channel)).
		Str("threadTS", msg.ThreadTS).
		Bool("isThread", msg.IsThread).
		Msg("Added unique message")

	// Also send to output channel
	select {
	case agg.outputCh <- msg:
		log.Debug().
			Str("timestamp", msg.Timestamp).
			Str("channelID", msg.Channel).
			Str("channelName", agg.getChannelDisplayName(msg.Channel)).
			Msg("Message sent to output channel")
	default:
		log.Warn().
			Str("timestamp", msg.Timestamp).
			Str("channelID", msg.Channel).
			Str("channelName", agg.getChannelDisplayName(msg.Channel)).
			Msg("Output channel full, message dropped")
	}
}

// trackThread adds a thread to the active threads map with persistent storage
func (agg *Aggregator) trackThread(channelID, threadTS string) {
	agg.threadMu.Lock()
	defer agg.threadMu.Unlock()

	// Initialize the inner map if needed
	if _, exists := agg.activeThreads[channelID]; !exists {
		agg.activeThreads[channelID] = make(map[string]models.ThreadInfo)
	}

	// Update the timestamp to now
	now := time.Now()
	info := models.ThreadInfo{
		LastChecked:  now,
		LastActivity: now,
	}
	agg.activeThreads[channelID][threadTS] = info

	// Update in persistent storage
	agg.stateManager.UpdateThreadTimestamp(channelID, threadTS, now)

	log.Debug().
		Str("channelID", channelID).
		Str("channelName", agg.getChannelDisplayName(channelID)).
		Str("threadTS", threadTS).
		Msg("Added thread to active tracking")
}

// getActiveThreadsSnapshot returns a copy of the active threads map
// to avoid deadlocks when iterating over it
func (agg *Aggregator) getActiveThreadsSnapshot() map[string]map[string]models.ThreadInfo {
	agg.threadMu.Lock()
	defer agg.threadMu.Unlock()

	// Create a deep copy of the active threads map
	result := make(map[string]map[string]models.ThreadInfo)
	for channelID, threads := range agg.activeThreads {
		result[channelID] = make(map[string]models.ThreadInfo)
		for threadTS, info := range threads {
			result[channelID][threadTS] = info
		}
	}

	return result
}

// processOutputChannel handles messages sent to the output channel
// This has been optimized for batching to reduce API calls
func (agg *Aggregator) processOutputChannel(ctx context.Context) {
	log.Debug().Msg("Starting output channel processor")

	// Target for feed messages
	var targetChannelID string

	// If a specific target user/channel was specified
	if agg.feedTargetUser != "" {
		log.Debug().
			Str("targetUser", agg.feedTargetUser).
			Msg("Target user specified, looking for appropriate channel")

		// Special case: if the target is "self", find the user's own DM with Slackbot
		// This is a workaround if we don't have permission to open DMs
		if agg.feedTargetUser == "self" {
			log.Debug().Msg("Target user is 'self', looking for Slackbot DM")

			// Look for the slackbot DM as a fallback
			for channelID, channel := range agg.channelInfo {
				if channel.IsIM && channel.User == "USLACKBOT" {
					targetChannelID = channelID
					log.Info().
						Str("channelID", targetChannelID).
						Str("channelName", agg.getChannelDisplayName(targetChannelID)).
						Msg("Found Slackbot DM channel for feed messages")

					// Send welcome message
					welcomeText := "👋 *Feed Aggregator is now active!*\nI'll send all aggregated messages to this conversation with Slackbot."
					_, _, err := agg.client.PostMessage(targetChannelID, slack.MsgOptionText(welcomeText, false))
					if err != nil {
						log.Error().
							Err(err).
							Str("channelID", targetChannelID).
							Str("channelName", agg.getChannelDisplayName(targetChannelID)).
							Msg("Error sending welcome message")
					} else {
						log.Debug().
							Str("channelID", targetChannelID).
							Str("channelName", agg.getChannelDisplayName(targetChannelID)).
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
				Str("targetUser", agg.feedTargetUser).
				Str("targetUserName", agg.getUserDisplayName(agg.feedTargetUser)).
				Msg("Looking for existing DM with target user")

			// Try to find an existing DM with the target user
			for channelID, channel := range agg.channelInfo {
				if channel.IsIM && channel.User == agg.feedTargetUser {
					targetChannelID = channelID
					log.Info().
						Str("channelID", targetChannelID).
						Str("channelName", agg.getChannelDisplayName(targetChannelID)).
						Str("targetUser", agg.feedTargetUser).
						Str("targetUserName", agg.getUserDisplayName(agg.feedTargetUser)).
						Msg("Found existing DM channel with user for feed messages")

					// Send welcome message
					welcomeText := "👋 *Feed Aggregator is now active!*\nI'll send all aggregated messages to this conversation."
					_, _, err := agg.client.PostMessage(targetChannelID, slack.MsgOptionText(welcomeText, false))
					if err != nil {
						log.Error().
							Err(err).
							Str("channelID", targetChannelID).
							Str("channelName", agg.getChannelDisplayName(targetChannelID)).
							Msg("Error sending welcome message")
					} else {
						log.Debug().
							Str("channelID", targetChannelID).
							Str("channelName", agg.getChannelDisplayName(targetChannelID)).
							Msg("Welcome message sent successfully")
					}
					break
				}
			}

			if targetChannelID == "" {
				log.Info().
					Str("targetUser", agg.feedTargetUser).
					Str("targetUserName", agg.getUserDisplayName(agg.feedTargetUser)).
					Msg("Couldn't find existing DM with user, attempting to open one")

				// Attempt to open a DM channel with the target user
				try, _, _, err := agg.client.OpenConversation(&slack.OpenConversationParameters{
					Users: []string{agg.feedTargetUser},
				})
				if err != nil {
					log.Error().
						Err(err).
						Str("targetUser", agg.feedTargetUser).
						Str("targetUserName", agg.getUserDisplayName(agg.feedTargetUser)).
						Msg("Error opening DM with user")
					log.Warn().Msg("Will only log messages to console. To enable DM functionality, add im:write scope to your token.")
				} else {
					targetChannelID = try.ID
					log.Info().
						Str("channelID", targetChannelID).
						Str("channelName", agg.getChannelDisplayName(targetChannelID)).
						Str("targetUser", agg.feedTargetUser).
						Str("targetUserName", agg.getUserDisplayName(agg.feedTargetUser)).
						Msg("Opened DM channel with user for feed messages")

					// Send welcome message
					welcomeText := "👋 *Feed Aggregator is now active!*\nI'll send all aggregated messages to this conversation."
					_, _, err := agg.client.PostMessage(targetChannelID, slack.MsgOptionText(welcomeText, false))
					if err != nil {
						log.Error().
							Err(err).
							Str("channelID", targetChannelID).
							Str("channelName", agg.getChannelDisplayName(targetChannelID)).
							Msg("Error sending welcome message")
					} else {
						log.Debug().
							Str("channelID", targetChannelID).
							Str("channelName", agg.getChannelDisplayName(targetChannelID)).
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
	batchedMessages := make([]models.Message, 0)

	// Increased batch size to reduce API calls
	const batchThreshold = 5

	// Increased time between batches to reduce API pressure
	const minTimeBetweenBatches = 20 * time.Second // Increased from 10s to 20s

	// Add a timer for flushing based on time
	flushTicker := time.NewTicker(minTimeBetweenBatches / 2) // Check every 10 seconds
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
				sendBatch(agg, batchedMessages, targetChannelID)
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

				sendBatch(agg, batchedMessages, targetChannelID)
				batchedMessages = make([]models.Message, 0)
				lastMessageTime = time.Now()
			}
		case msg := <-agg.outputCh:
			log.Debug().
				Str("user", msg.User).
				Str("userName", agg.getUserDisplayName(msg.User)).
				Str("channel", msg.Channel).
				Str("channelName", agg.getChannelDisplayName(msg.Channel)).
				Str("timestamp", msg.Timestamp).
				Msg("Processing output message")

			// Always log to console
			log.Info().
				Str("timestamp", msg.Timestamp).
				Str("user", agg.getUserDisplayName(msg.User)).
				Str("channel", agg.getChannelDisplayName(msg.Channel)).
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
					Str("targetChannelName", agg.getChannelDisplayName(targetChannelID)).
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

					sendBatch(agg, batchedMessages, targetChannelID)
					batchedMessages = make([]models.Message, 0)
					lastMessageTime = time.Now()
				}

			} else {
				log.Debug().Msg("No target channel found, skipping message send")
			}
		}
	}
}

// Extract the batch sending logic to a helper function
func sendBatch(agg *Aggregator, batchMessages []models.Message, targetChannelID string) {
	// For larger batches, use a single API call with blocks
	// if len(batchMessages) > 1 {
	// 	// Combine messages into one post with blocks
	// 	//blocks := []slack.Block{}
	// 	messageText := ""

	// 	for _, batchMsg := range batchMessages {
	// 		userName := agg.getUserDisplayName(batchMsg.User)
	// 		channelName := agg.getChannelDisplayName(batchMsg.Channel)

	// 		// Create message link
	// 		//linkTimestamp := strings.Replace(batchMsg.Timestamp, ".", "", 1)
	// 		//messageLink := fmt.Sprintf("https://%s.slack.com/archives/%s/p%s",
	// 		//	agg.teamDomain, batchMsg.Channel, linkTimestamp)

	// 		// Format message with our marker
	// 		messageText += agg.messageFormatter.FormatMessage(
	// 			agg.teamDomain,
	// 			batchMsg.Channel,
	// 			batchMsg.Timestamp,
	// 			userName,
	// 			channelName,
	// 		)

	// 		// // Create a section block for this message
	// 		// headerText := slack.NewTextBlockObject(
	// 		// 	"mrkdwn",
	// 		// 	fmt.Sprintf("*%s* in #%s - <%s|View Message>",
	// 		// 		userName, channelName, messageLink),
	// 		// 	false, false)

	// 		// // Add a divider if it's not the first message
	// 		// if len(blocks) > 0 {
	// 		// 	blocks = append(blocks, slack.NewDividerBlock())
	// 		// }

	// 		// // Add the header section
	// 		// blocks = append(blocks, slack.NewSectionBlock(headerText, nil, nil))

	// 		// // Add the app tag as context
	// 		// appTag := slack.NewTextBlockObject(
	// 		// 	"mrkdwn",
	// 		// 	agg.messageFormatter.GetAppTag(),
	// 		// 	false, false)
	// 		// blocks = append(blocks, slack.NewContextBlock(
	// 		// 	"",
	// 		// 	slack.MixedElement(appTag),
	// 		// ))
	// 	}

	// 	// Send the batch as a single message with blocks
	// 	_, timestamp, err := agg.client.PostMessage(
	// 		targetChannelID,
	// 		slack.MsgOptionBlocks(blocks...),
	// 	)

	// 	if err != nil {
	// 		log.Error().
	// 			Err(err).
	// 			Str("targetChannelID", targetChannelID).
	// 			Int("batchSize", len(batchMessages)).
	// 			Msg("Error sending batch message")
	// 	} else {
	// 		log.Debug().
	// 			Str("targetChannelID", targetChannelID).
	// 			Int("batchSize", len(batchMessages)).
	// 			Str("timestamp", timestamp).
	// 			Msg("Batch message sent successfully")

	// 		// Track this message for retention
	// 		agg.stateManager.TrackSentMessage(timestamp, targetChannelID)
	// 	}
	// } else {
	// For smaller batches, use the original approach
	// Process each message in the batch
	messageText := ""
	for _, batchMsg := range batchMessages {
		userName := agg.getUserDisplayName(batchMsg.User)
		channelName := agg.getChannelDisplayName(batchMsg.Channel)

		// Format message with our marker
		messageText += agg.messageFormatter.FormatMessage(
			agg.teamDomain,
			batchMsg.Channel,
			batchMsg.Timestamp,
			userName,
			channelName,
		)
	}

	// Send to target channel
	_, timestamp, err := agg.client.PostMessage(
		targetChannelID,
		slack.MsgOptionText(messageText, false),
	)

	if err != nil {
		log.Error().
			Err(err).
			Str("targetChannelID", targetChannelID).
			Str("targetChannelName", agg.getChannelDisplayName(targetChannelID)).
			Msg("Error sending message to channel")
	} else {
		log.Debug().
			Str("targetChannelID", targetChannelID).
			Str("targetChannelName", agg.getChannelDisplayName(targetChannelID)).
			Str("timestamp", timestamp).
			Msg("Message sent successfully")

		// Track this message for retention
		agg.stateManager.TrackSentMessage(timestamp, targetChannelID)
	}

}
