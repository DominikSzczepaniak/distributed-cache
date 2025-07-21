package raft

import (
	"os"
	"strconv"
	"strings"
)

type Config struct {
	valuesFilename string
	totalNodes     int
	raftId         int
	raftAddrs      []string
}

func LoadConfig() *Config {
	valuesFilename, exists := os.LookupEnv("VALUES_FILENAME")
	if !exists || valuesFilename == "" {
		panic("Specify the name of VALUES_FILENAME environment variable")
	}

	totalNodesStr, exists := os.LookupEnv("TOTAL_NODES")
	if !exists || totalNodesStr == "" {
		panic("TOTAL NODES NOT DEFINED!")
	}
	totalNodes, err := strconv.Atoi(totalNodesStr)
	if err != nil {
		panic("TOTAL NODES IS NOT A NUMBER!")
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
		panic("Number of nodes must be equal to number of Raft adressess")
	}

	return &Config{
		valuesFilename: valuesFilename,
		totalNodes:     totalNodes,
		raftId:         raftId,
		raftAddrs:      raftAddrs,
	}
}
