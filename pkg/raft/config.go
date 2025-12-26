package raft

import (
	"math/rand"
	"os"
	"strconv"
	"strings"
	"time"
)

// StartupMode defines the strategy for starting the Raft node.
type StartupMode int

const (
	// StartAlways immediately begins the election and heartbeat loops.
	StartAlways StartupMode = iota
	// WaitForQuorum waits for a majority of nodes to be available before starting.
	WaitForQuorum
	// WaitForAnyPeer waits for at least one peer to be available before starting.
	WaitForAnyPeer
)

// RetryConfig holds parameters for exponential backoff during reconnection attempts.
type RetryConfig struct {
	// InitialBackoff is the duration to wait after the first failure.
	InitialBackoff time.Duration
	// MaxBackoff is the upper limit for the wait duration.
	MaxBackoff time.Duration
	// Multiplier is the factor by which the backoff increases after each attempt.
	Multiplier float64
	// MaxRetries is the number of attempts before giving up (-1 for infinite).
	MaxRetries int
}

// Config contains all parameters for a Raft node, typically loaded from environment variables.
type Config struct {
	logsFilename      string
	metadataFilename  string
	snapshotFilename  string
	totalNodes        int
	raftId            int
	raftAddrs         []string
	snapshotThreshold int

	connectionRetryConfig RetryConfig
	connectionTimeout     time.Duration
	healthCheckInterval   time.Duration
	startupMode           StartupMode
	artificialDelay       time.Duration
}

func getEnvDuration(key string, defaultVal time.Duration) time.Duration {
	if val := os.Getenv(key); val != "" {
		if d, err := time.ParseDuration(val); err == nil {
			return d
		}
	}
	return defaultVal
}

func getEnvFloat(key string, defaultVal float64) float64 {
	if val := os.Getenv(key); val != "" {
		if f, err := strconv.ParseFloat(val, 64); err == nil {
			return f
		}
	}
	return defaultVal
}

func getEnvInt(key string, defaultVal int) int {
	if val := os.Getenv(key); val != "" {
		if i, err := strconv.Atoi(val); err == nil {
			return i
		}
	}
	return defaultVal
}

// LoadConfig reads Raft configuration from environment variables (e.g., FILENAME, RAFT_ID, RAFT_ADDRS).
// It panics if essential variables are missing or malformed.
func LoadConfig() *Config {
	filename, exists := os.LookupEnv("FILENAME")
	if !exists || filename == "" {
		panic("Specify the name of FILENAME environment variable")
	}

	totalNodesStr, exists := os.LookupEnv("TOTAL_NODES")
	if !exists || totalNodesStr == "" {
		panic("TOTAL NODES NOT DEFINED!")
	}
	totalNodes, err := strconv.Atoi(totalNodesStr)
	if err != nil {
		panic("TOTAL NODES IS NOT A NUMBER!")
	}

	snapshotThresholdStr, exists := os.LookupEnv("SNAPSHOT_THRESHOLD")
	if !exists || snapshotThresholdStr == "" {
		panic("TOTAL NODES NOT DEFINED!")
	}
	snapshotThreshold, err := strconv.Atoi(snapshotThresholdStr)
	if err != nil {
		panic("SNAPSHOT THRESHOLD IS NOT A NUMBER!")
	}
	snapshotThreshold = snapshotThreshold + rand.Intn(snapshotThreshold/10) - snapshotThreshold/20

	raftIdStr, exists := os.LookupEnv("RAFT_ID")
	if !exists || raftIdStr == "" {
		panic("RAFT ID NOT DEFINED!")
	}
	raftId, err := strconv.Atoi(raftIdStr)
	if err != nil {
		panic("RAFT ID IS NOT A NUMBER!")
	}

	addrs := os.Getenv("RAFT_ADDRS")
	if addrs == "" {
		panic("Raft addresses not defined in environment")
	}
	raftAddrs := strings.Split(addrs, ",")
	if len(raftAddrs) != totalNodes {
		panic("Number of nodes must be equal to number of Raft addresses")
	}

	cfg := &Config{
		logsFilename:      filename + ".logs",
		metadataFilename:  filename + ".meta",
		snapshotFilename:  filename + ".snap",
		totalNodes:        totalNodes,
		raftId:            raftId,
		raftAddrs:         raftAddrs,
		snapshotThreshold: snapshotThreshold,

		connectionRetryConfig: RetryConfig{
			InitialBackoff: getEnvDuration("RAFT_INITIAL_BACKOFF", 1*time.Second),
			MaxBackoff:     getEnvDuration("RAFT_MAX_BACKOFF", 30*time.Second),
			Multiplier:     getEnvFloat("RAFT_BACKOFF_MULTIPLIER", 2.0),
			MaxRetries:     getEnvInt("RAFT_MAX_RETRIES", -1),
		},
		connectionTimeout:   getEnvDuration("RAFT_CONN_TIMEOUT", 5*time.Second),
		healthCheckInterval: getEnvDuration("RAFT_HEALTH_CHECK_INTERVAL", 10*time.Second),
		startupMode:         StartAlways,
		artificialDelay:     getEnvDuration("RAFT_ARTIFICIAL_DELAY", 0),
	}

	return cfg
}
