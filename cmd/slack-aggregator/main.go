package main

import (
	"context"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"

	"github.com/zerobugdebug/awesome-slack-aggregator/internal/aggregator"
	"github.com/zerobugdebug/awesome-slack-aggregator/internal/config"
)

func main() {
	// Set up zerolog
	setupLogging("info")

	// Parse command line flags
	cfg := config.Parse()

	// Set up zerolog
	setupLogging(cfg.LogLevel)

	// Create context with cancellation
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Handle OS signals for graceful shutdown
	setupSignalHandling(cancel)

	// Create and start the aggregator
	log.Debug().Msg("Creating feed aggregator")
	agg, err := aggregator.New(
		cfg.Token,
		cfg.TargetUserID,
		cfg.StateDir,
		cfg.RetentionDays,
		cfg.ThreadExpiryDays,
	)

	if err != nil {
		log.Fatal().Err(err).Msg("Error creating feed aggregator")
	}

	log.Info().Msg("Starting optimized feed aggregator...")
	if err := agg.Start(ctx); err != nil {
		log.Fatal().Err(err).Msg("Error starting feed aggregator")
	}
}

// setupLogging configures the zerolog logger
func setupLogging(logLevelStr string) {
	consoleWriter := zerolog.ConsoleWriter{Out: os.Stdout, TimeFormat: time.RFC3339}

	// Parse log level
	logLevel, err := zerolog.ParseLevel(logLevelStr)
	if err != nil {
		// Default to info if invalid level
		logLevel = zerolog.InfoLevel
		log.Warn().Str("level", logLevelStr).Msg("Invalid log level, defaulting to 'info'")
	}
	zerolog.SetGlobalLevel(logLevel)
	log.Logger = zerolog.New(consoleWriter).With().Timestamp().Logger()

	log.Info().Str("level", logLevel.String()).Msg("Logger initialized")
}

// setupSignalHandling sets up signal handling for graceful shutdown
func setupSignalHandling(cancel context.CancelFunc) {
	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt, syscall.SIGTERM)

	go func() {
		<-c
		log.Info().Msg("Received shutdown signal")
		cancel()
	}()
}
