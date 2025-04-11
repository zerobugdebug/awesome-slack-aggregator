package models

import (
	"time"
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

// ThreadInfo represents information about a thread
type ThreadInfo struct {
	LastChecked  time.Time `json:"lastChecked"`
	LastActivity time.Time `json:"lastActivity"`
}

// Used for batch thread checking
type ThreadCheckInfo struct {
	ThreadTS     string
	LastActivity time.Time
}
