package config

import (
	"flag"
	"os"

	"github.com/rs/zerolog/log"
)

// Config holds the application configuration
type Config struct {
	LogLevel         string
	StateDir         string
	RetentionDays    int
	ThreadExpiryDays int
	Token            string
	TargetUserID     string
}

// Parse reads the configuration from command line flags and environment variables
func Parse() *Config {
	// Set up command line flags
	logLevelStr := flag.String("log-level", "info", "Log level: trace, debug, info, warn, error, fatal, panic")
	stateDir := flag.String("state-dir", ".", "Directory for persistent state storage (default: current folder)")
	retentionDays := flag.Int("retention", 7, "Number of days to retain messages before deletion")
	threadExpiryDays := flag.Int("thread-expiry", 7, "Number of days of inactivity before thread tracking expires")
	flag.Parse()

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

	return &Config{
		LogLevel:         *logLevelStr,
		StateDir:         *stateDir,
		RetentionDays:    *retentionDays,
		ThreadExpiryDays: *threadExpiryDays,
		Token:            token,
		TargetUserID:     targetUserID,
	}
}
