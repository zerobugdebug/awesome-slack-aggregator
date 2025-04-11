package slack

import (
	"strings"

	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	"github.com/slack-go/slack"
)

// SlackClient wraps the slack-go client
type SlackClient struct {
	client     *slack.Client
	userID     string
	teamDomain string
}

// NewClient creates a new Slack client wrapper
func NewClient(token string) (*SlackClient, error) {
	// Create a logger adapter for slack-go
	slackLogger := &slackLogAdapter{
		logger: log.With().Str("component", "slack-api").Logger(),
	}

	// Create the slack client
	client := slack.New(
		token,
		slack.OptionLog(slackLogger),
	)

	// Test the token by getting user identity
	log.Debug().Msg("Testing authentication with Slack")
	authTest, err := client.AuthTest()
	if err != nil {
		log.Error().Err(err).Msg("Authentication test failed")
		return nil, err
	}

	log.Info().
		Str("user", authTest.User).
		Str("userID", authTest.UserID).
		Str("teamDomain", authTest.URL).
		Msg("Connected to Slack")

	// Extract team domain from URL
	teamDomain := extractTeamDomain(authTest.URL)

	return &SlackClient{
		client:     client,
		userID:     authTest.UserID,
		teamDomain: teamDomain,
	}, nil
}

// extractTeamDomain extracts the team domain from the Slack URL
func extractTeamDomain(url string) string {

	teamDomain := "slack.com" // Default fallback
	if url != "" {
		parts := strings.Split(url, "//")
		if len(parts) > 1 {
			parts = strings.Split(parts[1], ".")
			if len(parts) > 0 {
				teamDomain = parts[0]
				log.Debug().Str("teamDomain", teamDomain).Msg("Extracted team domain from URL")
			}
		}
	}
	return teamDomain // Default fallback, should be replaced with actual logic
}

// GetClient returns the underlying slack client
func (sc *SlackClient) GetClient() *slack.Client {
	return sc.client
}

// GetUserID returns the authenticated user's ID
func (sc *SlackClient) GetUserID() string {
	return sc.userID
}

// GetTeamDomain returns the team domain
func (sc *SlackClient) GetTeamDomain() string {
	return sc.teamDomain
}

// slackLogAdapter adapts zerolog to slack-go's log interface
type slackLogAdapter struct {
	logger zerolog.Logger
}

// Output implements the slack-go logger interface
func (a *slackLogAdapter) Output(calldepth int, s string) error {
	a.logger.Debug().Msg(s)
	return nil
}
