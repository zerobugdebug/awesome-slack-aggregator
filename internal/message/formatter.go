package message

import (
	"fmt"
	"strings"
)

// Formatter handles formatting of messages sent by the app
type Formatter struct {
	appTag string // The identifier tag for app messages
}

// NewFormatter creates a new formatter with a unique ID
func NewFormatter() *Formatter {
	// Create a unique identifier for this instance
	uniqueID := "FpHZFpdW"

	return &Formatter{
		appTag: uniqueID,
	}
}

// FormatMessage formats a message link for sending
func (mf *Formatter) FormatMessage(teamDomain, channelID, timestamp, userRealName, channelName string) string {
	// Create message link
	linkTimestamp := strings.Replace(timestamp, ".", "", 1)
	messageLink := fmt.Sprintf("https://%s.slack.com/archives/%s/p%s",
		teamDomain, channelID, linkTimestamp)

	// Format with app identifier
	return fmt.Sprintf("`%s` <%s|*#%s*>",
		mf.appTag, messageLink, channelName)
}

// GetAppTag returns the app tag for identification
func (mf *Formatter) GetAppTag() string {
	return fmt.Sprintf("`%s`", mf.appTag)
}

// IsAppMessage checks if a message was created by the app
func (mf *Formatter) IsAppMessage(text string) bool {
	// Look for our special comment marker
	return text != "" && (len(text) >= len(mf.appTag) && strings.Contains(text, mf.appTag))
}
