package raft

import (
	"os"
	"strconv"
	"strings"
)

type Config struct {
	logsFilename      string
	metadataFilename  string
	snapshotFilename  string
	totalNodes        int
	raftId            int
	raftAddrs         []string
	snapshotThreshold int
}

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

	return &Config{
		logsFilename:      filename + ".logs",
		metadataFilename:  filename + ".meta",
		snapshotFilename:  filename + ".snap",
		totalNodes:        totalNodes,
		raftId:            raftId,
		raftAddrs:         raftAddrs,
		snapshotThreshold: snapshotThreshold,
	}
}
