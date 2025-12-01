package worker

import (
	"os"
	"strconv"
	"strings"

	"github.com/dominikszczepaniak/distributed-cache/pkg/sharding"
)

// Config holds worker node configuration
type Config struct {
	WorkerID           int                   // Unique worker identifier
	HTTPAddr           string                // HTTP API listen address
	GRPCAddr           string                // gRPC replication listen address
	RaftAddrs          []string              // Raft cluster addresses for registration (Stage 2)
	AssignedPartitions []sharding.PartitionID // Partitions assigned to this worker (for testing)
}

// LoadConfig loads worker configuration from environment variables
func LoadConfig() *Config {
	// WORKER_ID: Unique identifier (0, 1, 2, ...)
	workerIDStr := os.Getenv("WORKER_ID")
	if workerIDStr == "" {
		panic("WORKER_ID environment variable is required")
	}
	workerID, err := strconv.Atoi(workerIDStr)
	if err != nil {
		panic("WORKER_ID must be a valid integer")
	}

	// HTTP_ADDR: HTTP API listen address (e.g., ":7000")
	httpAddr := os.Getenv("HTTP_ADDR")
	if httpAddr == "" {
		httpAddr = ":7000"
	}

	// GRPC_ADDR: gRPC replication listen address (e.g., ":7100")
	grpcAddr := os.Getenv("GRPC_ADDR")
	if grpcAddr == "" {
		// Default: HTTP port + 100
		grpcAddr = ":7100"
	}

	// RAFT_ADDRS: Comma-separated Raft node addresses (for Stage 2)
	// Example: "raft-node-0:9000,raft-node-1:9000,raft-node-2:9000"
	raftAddrsStr := os.Getenv("RAFT_ADDRS")
	var raftAddrs []string
	if raftAddrsStr != "" {
		raftAddrs = strings.Split(raftAddrsStr, ",")
	}

	// ASSIGNED_PARTITIONS: Comma-separated partition IDs or ranges (for testing)
	// Examples:
	//   "0,1,2,3"            -> partitions 0, 1, 2, 3
	//   "0-99"               -> partitions 0 through 99
	//   "0-99,200-299"       -> partitions 0-99 and 200-299
	assignedPartitionsStr := os.Getenv("ASSIGNED_PARTITIONS")
	var assignedPartitions []sharding.PartitionID
	if assignedPartitionsStr != "" {
		assignedPartitions = parsePartitionList(assignedPartitionsStr)
	}

	return &Config{
		WorkerID:           workerID,
		HTTPAddr:           httpAddr,
		GRPCAddr:           grpcAddr,
		RaftAddrs:          raftAddrs,
		AssignedPartitions: assignedPartitions,
	}
}

// parsePartitionList parses partition specification string
// Supports: "0,1,2", "0-99", "0-99,200-299"
func parsePartitionList(spec string) []sharding.PartitionID {
	var partitions []sharding.PartitionID

	parts := strings.Split(spec, ",")
	for _, part := range parts {
		part = strings.TrimSpace(part)
		if strings.Contains(part, "-") {
			// Range: "0-99"
			rangeParts := strings.Split(part, "-")
			if len(rangeParts) != 2 {
				continue
			}
			start, err1 := strconv.Atoi(strings.TrimSpace(rangeParts[0]))
			end, err2 := strconv.Atoi(strings.TrimSpace(rangeParts[1]))
			if err1 != nil || err2 != nil {
				continue
			}
			for i := start; i <= end; i++ {
				partitions = append(partitions, sharding.PartitionID(i))
			}
		} else {
			// Single partition: "0"
			pid, err := strconv.Atoi(part)
			if err != nil {
				continue
			}
			partitions = append(partitions, sharding.PartitionID(pid))
		}
	}

	return partitions
}
