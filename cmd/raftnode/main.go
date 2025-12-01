package main

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/dominikszczepaniak/distributed-cache/pkg/api"
	"github.com/dominikszczepaniak/distributed-cache/pkg/raft"
	"github.com/dominikszczepaniak/distributed-cache/pkg/replication"
	"github.com/dominikszczepaniak/distributed-cache/pkg/sharding"
)

type SimpleKVStore struct {
	mu             sync.RWMutex
	data           map[int]int
	partitionTable *sharding.PartitionTable
}

func NewSimpleKVStore() *SimpleKVStore {
	return &SimpleKVStore{
		data:           make(map[int]int),
		partitionTable: sharding.NewPartitionTable(),
	}
}

func (s *SimpleKVStore) AppendMessage(msg raft.Message) (bool, int) {
	s.mu.Lock()
	defer s.mu.Unlock()

	switch msg.MsgType {
	case "UPDATE_PARTITION_TABLE":
		if msg.PartitionTableUpdate == nil {
			slog.Warn("Received UPDATE_PARTITION_TABLE with nil payload")
			return false, 0
		}
		s.partitionTable.ApplyUpdate(
			msg.PartitionTableUpdate.Assignments,
			msg.PartitionTableUpdate.Version,
		)
		slog.Info(fmt.Sprintf("Updated partition table to version %d (%d assignments)",
			msg.PartitionTableUpdate.Version,
			len(msg.PartitionTableUpdate.Assignments)))
		return true, 0
	case "PUT":
		if msg.Value == nil {
			return false, 0
		}
		s.data[msg.Key] = *msg.Value
		slog.Info(fmt.Sprintf("PUT key=%d value=%d", msg.Key, *msg.Value))
		return true, 0
	case "GET":
		val := s.data[msg.Key]
		slog.Info(fmt.Sprintf("GET key=%d value=%d", msg.Key, val))
		return true, val
	case "DELETE":
		delete(s.data, msg.Key)
		slog.Info(fmt.Sprintf("DELETE key=%d", msg.Key))
		return true, 0
	default:
		slog.Warn(fmt.Sprintf("Unknown message type: %s", msg.MsgType))
		return false, 0
	}
}

func (s *SimpleKVStore) GetSnapshot() ([]byte, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	// Serialize data map (existing logic)
	var dataBuf bytes.Buffer
	byteOrder := binary.LittleEndian

	mapLen := int32(len(s.data))
	if err := binary.Write(&dataBuf, byteOrder, mapLen); err != nil {
		return nil, fmt.Errorf("failed to write map length: %w", err)
	}

	for k, v := range s.data {
		if err := binary.Write(&dataBuf, byteOrder, int64(k)); err != nil {
			return nil, fmt.Errorf("failed to write key %d: %w", k, err)
		}
		if err := binary.Write(&dataBuf, byteOrder, int64(v)); err != nil {
			return nil, fmt.Errorf("failed to write value for key %d: %w", k, err)
		}
	}

	// Serialize partition table
	partitionTableData, err := s.partitionTable.Serialize()
	if err != nil {
		return nil, fmt.Errorf("failed to serialize partition table: %w", err)
	}

	// Combine both into a single snapshot
	combined, err := sharding.CombineSnapshot(partitionTableData, dataBuf.Bytes())
	if err != nil {
		return nil, fmt.Errorf("failed to combine snapshot: %w", err)
	}

	slog.Info(fmt.Sprintf("Created snapshot with %d data entries and %d partition assignments",
		len(s.data), s.partitionTable.GetAssignmentCount()))

	return combined, nil
}

func (s *SimpleKVStore) RestoreFromSnapshot(data []byte) (error, int) {
	s.mu.Lock()
	defer s.mu.Unlock()

	// Split combined snapshot into partition table and data
	partitionTableData, dataSnapshot, err := sharding.SplitSnapshot(data)
	if err != nil {
		return fmt.Errorf("failed to split snapshot: %w", err), 0
	}

	// Restore partition table
	if err := s.partitionTable.Deserialize(partitionTableData); err != nil {
		return fmt.Errorf("failed to deserialize partition table: %w", err), 0
	}

	// Restore data map (existing logic)
	reader := bytes.NewReader(dataSnapshot)
	byteOrder := binary.LittleEndian

	var mapLen int32
	if err := binary.Read(reader, byteOrder, &mapLen); err != nil {
		return fmt.Errorf("failed to read map length: %w", err), 0
	}

	newMap := make(map[int]int)
	var lastKey int

	for i := 0; i < int(mapLen); i++ {
		var k64, v64 int64
		if err := binary.Read(reader, byteOrder, &k64); err != nil {
			return fmt.Errorf("failed to read key at index %d: %w", i, err), 0
		}
		if err := binary.Read(reader, byteOrder, &v64); err != nil {
			return fmt.Errorf("failed to read value at index %d: %w", i, err), 0
		}
		lastKey = int(k64)
		newMap[int(k64)] = int(v64)
	}

	s.data = newMap
	slog.Info(fmt.Sprintf("Restored snapshot with %d data entries and %d partition assignments",
		len(s.data), s.partitionTable.GetAssignmentCount()))

	return nil, lastKey
}

func (s *SimpleKVStore) GetValue(key int) int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.data[key]
}

// convertRaftAddrToHTTP converts a Raft gRPC address to an HTTP API address
// Example: "localhost:9000" → "http://localhost:10000"
// Assumes HTTP port = gRPC port + 1000
func convertRaftAddrToHTTP(raftAddr string) string {
	parts := strings.Split(raftAddr, ":")
	if len(parts) != 2 {
		return fmt.Sprintf("http://%s", raftAddr)
	}

	host := parts[0]
	port, err := strconv.Atoi(parts[1])
	if err != nil {
		return fmt.Sprintf("http://%s", raftAddr)
	}

	// Convert gRPC port to HTTP port (add 1000)
	httpPort := port + 1000
	return fmt.Sprintf("http://%s:%d", host, httpPort)
}

func autoInitializePartitions(r *raft.Raft, app *SimpleKVStore, cfg *raft.Config) {
	// Check if partition table is already initialized
	if app.partitionTable.GetAssignmentCount() > 0 {
		slog.Info("Partition table already initialized, skipping auto-initialization",
			"assignments", app.partitionTable.GetAssignmentCount())
		return
	}

	slog.Info("Partition table is empty, waiting for leader election...")

	// Wait for leader election with timeout
	maxWait := 10 * time.Second
	start := time.Now()
	checkInterval := 100 * time.Millisecond

	for time.Since(start) < maxWait {
		if r.IsLeader() {
			slog.Info("This node is the leader, initializing partition table")
			break
		}
		time.Sleep(checkInterval)
	}

	// If not leader after timeout, wait for replication from leader
	if !r.IsLeader() {
		slog.Info("Not the leader, waiting for partition table from leader")
		return
	}

	// This node is the leader - initialize partition table
	totalNodes := cfg.GetTotalNodes()
	nodeIDs := make([]sharding.NodeID, totalNodes)
	for i := 0; i < totalNodes; i++ {
		nodeIDs[i] = sharding.NodeID(i)
	}

	// Create even distribution across all nodes
	pt := sharding.InitializeEvenDistribution(sharding.TOTAL_PARTITIONS, nodeIDs)

	slog.Info("Initialized partition table",
		"total_partitions", sharding.TOTAL_PARTITIONS,
		"nodes", totalNodes,
		"version", pt.GetVersion())

	// Replicate partition table via Raft consensus
	msg := raft.Message{
		MsgType: "UPDATE_PARTITION_TABLE",
		PartitionTableUpdate: &raft.PartitionTableUpdate{
			Assignments: pt.GetAssignments(),
			Version:     pt.GetVersion(),
		},
	}

	// Use Broadcast to replicate to all nodes
	r.Broadcast(msg)

	slog.Info("Partition table update proposed to Raft cluster")
}

func main() {
	opts := &slog.HandlerOptions{
		Level: slog.LevelInfo,
	}
	logger := slog.New(slog.NewTextHandler(os.Stdout, opts))
	slog.SetDefault(logger)

	slog.Info("Starting Raft node...")

	cfg := raft.LoadConfig()

	slog.Info("Node configuration loaded from environment")

	app := NewSimpleKVStore()

	r := raft.NewRaft(app, cfg)

	slog.Info("Raft node started successfully")

	// Auto-initialize partition table after Raft startup
	go autoInitializePartitions(r, app, cfg)

	// Initialize ShardManager for data plane routing
	partitioner := sharding.NewPartitioner()
	nodeID := sharding.NodeID(cfg.GetRaftID())
	shardManager := sharding.NewShardManager(nodeID, app.partitionTable, partitioner)

	// Register peer addresses (convert gRPC addresses to HTTP addresses)
	// RAFT_ADDRS format: "localhost:9000,localhost:9001,localhost:9002"
	// HTTP addresses will be derived by adding 1000 to port (9000 → 10000)
	raftAddrs := cfg.GetRaftAddrs()
	for i, raftAddr := range raftAddrs {
		peerNodeID := sharding.NodeID(i)
		// Convert gRPC address to HTTP address
		// Example: "localhost:9000" → "http://localhost:10000"
		httpAddr := convertRaftAddrToHTTP(raftAddr)
		shardManager.UpdatePeerAddress(peerNodeID, httpAddr)
	}

	// Initialize ReplicationClient for primary-backup replication
	replicationClient := replication.NewClient(nodeID, 1*time.Second)

	// Register peer RaftClients with the replication client
	// Wait a bit for connections to establish
	time.Sleep(2 * time.Second)
	connMgr := r.GetConnectionManager()
	if connMgr != nil {
		peers := connMgr.GetPeers()
		for i, peer := range peers {
			if peer != nil && i != int(nodeID) {
				// Type assert to access the underlying RaftClient
				if grpcPeer, ok := peer.(*raft.GRPCPeerClient); ok {
					raftClient := grpcPeer.GetRaftClient()
					replicationClient.RegisterPeer(sharding.NodeID(i), raftClient)
				}
			}
		}
	}

	apiAddr := os.Getenv("API_ADDR")
	if apiAddr == "" {
		apiAddr = ":8080"
	}

	apiServer := api.NewServer(r, apiAddr, shardManager, replicationClient)
	go func() {
		if err := apiServer.Start(); err != nil && err != http.ErrServerClosed {
			slog.Error(fmt.Sprintf("API server error: %v", err))
		}
	}()

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)

	sig := <-sigCh
	slog.Info(fmt.Sprintf("Received signal %v, shutting down...", sig))

	if err := apiServer.Stop(); err != nil {
		slog.Error(fmt.Sprintf("Error stopping API server: %v", err))
	}

	slog.Info("Raft node stopped")
}
