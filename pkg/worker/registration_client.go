package worker

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/dominikszczepaniak/distributed-cache/pkg/raft/raftpb"
	"github.com/dominikszczepaniak/distributed-cache/pkg/sharding"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// RegistrationClient handles worker registration with the Raft cluster
// and maintains heartbeat connection
type RegistrationClient struct {
	workerID           int
	grpcAddr           string
	httpAddr           string
	raftAddrs          []string
	partitionTable     *sharding.PartitionTable
	partitionTableVersion uint64

	mu             sync.RWMutex
	raftClient     raftpb.RaftClient
	conn           *grpc.ClientConn
	registered     bool
	heartbeatStop  chan struct{}
	heartbeatDone  chan struct{}
}

// NewRegistrationClient creates a new registration client
func NewRegistrationClient(
	workerID int,
	grpcAddr string,
	httpAddr string,
	raftAddrs []string,
	partitionTable *sharding.PartitionTable,
) *RegistrationClient {
	return &RegistrationClient{
		workerID:       workerID,
		grpcAddr:       grpcAddr,
		httpAddr:       httpAddr,
		raftAddrs:      raftAddrs,
		partitionTable: partitionTable,
		heartbeatStop:  make(chan struct{}),
		heartbeatDone:  make(chan struct{}),
	}
}

// RegisterWithRaft attempts to register this worker with the Raft cluster
// Retries until successful or context is cancelled
func (rc *RegistrationClient) RegisterWithRaft(ctx context.Context) error {
	slog.Info("Attempting to register with Raft cluster",
		"worker_id", rc.workerID,
		"grpc_addr", rc.grpcAddr,
		"http_addr", rc.httpAddr)

	// Try each Raft node until successful
	var lastErr error
	for attempt := 0; attempt < 10; attempt++ {
		for _, raftAddr := range rc.raftAddrs {
			err := rc.tryRegisterWithNode(ctx, raftAddr)
			if err == nil {
				rc.mu.Lock()
				rc.registered = true
				rc.mu.Unlock()

				slog.Info("Successfully registered with Raft cluster",
					"worker_id", rc.workerID,
					"raft_node", raftAddr,
					"partition_table_version", rc.partitionTableVersion)
				return nil
			}

			slog.Warn("Failed to register with Raft node",
				"worker_id", rc.workerID,
				"raft_node", raftAddr,
				"attempt", attempt+1,
				"error", err)
			lastErr = err
		}

		// Wait before retrying
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(2 * time.Second):
			// Continue to next attempt
		}
	}

	return fmt.Errorf("failed to register with Raft cluster after 10 attempts: %w", lastErr)
}

// tryRegisterWithNode attempts registration with a specific Raft node
func (rc *RegistrationClient) tryRegisterWithNode(ctx context.Context, raftAddr string) error {
	// Connect to Raft node
	conn, err := grpc.Dial(raftAddr,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithBlock(),
		grpc.WithTimeout(5*time.Second))
	if err != nil {
		return fmt.Errorf("failed to connect to Raft node %s: %w", raftAddr, err)
	}

	// Store connection for heartbeats
	rc.mu.Lock()
	if rc.conn != nil {
		rc.conn.Close()
	}
	rc.conn = conn
	rc.raftClient = raftpb.NewRaftClient(conn)
	rc.mu.Unlock()

	// Send registration request
	req := &raftpb.RegisterWorkerRequest{
		WorkerId:  int32(rc.workerID),
		GrpcAddr:  rc.grpcAddr,
		HttpAddr:  rc.httpAddr,
	}

	timeoutCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()

	resp, err := rc.raftClient.RegisterWorker(timeoutCtx, req)
	if err != nil {
		return fmt.Errorf("registration RPC failed: %w", err)
	}

	if !resp.Success {
		return fmt.Errorf("registration rejected by Raft: %s", resp.Error)
	}

	// Update local partition table with received assignments
	rc.mu.Lock()
	rc.partitionTableVersion = resp.PartitionTableVersion
	rc.mu.Unlock()

	if resp.PartitionTable != nil {
		rc.updatePartitionTable(resp.PartitionTable)
	}

	slog.Info("Registration successful",
		"worker_id", rc.workerID,
		"partition_table_version", resp.PartitionTableVersion,
		"assignments", len(resp.PartitionTable.GetAssignments()))

	return nil
}

// updatePartitionTable updates the local partition table with data from Raft
func (rc *RegistrationClient) updatePartitionTable(ptProto *raftpb.PartitionTable) {
	if ptProto == nil {
		return
	}

	for _, assignment := range ptProto.Assignments {
		partitionID := sharding.PartitionID(assignment.PartitionId)
		nodeID := sharding.NodeID(assignment.NodeId)

		// For Stage 2, we only have primary assignments (no backup yet)
		rc.partitionTable.SetReplicas(partitionID, nodeID, -1)
	}

	slog.Info("Updated partition table",
		"worker_id", rc.workerID,
		"version", ptProto.Version,
		"total_assignments", len(ptProto.Assignments))
}

// StartHeartbeat starts sending periodic heartbeats to the Raft cluster
// Runs in a goroutine until stopped
func (rc *RegistrationClient) StartHeartbeat(ctx context.Context) {
	defer close(rc.heartbeatDone)

	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()

	slog.Info("Starting heartbeat to Raft cluster",
		"worker_id", rc.workerID,
		"interval", "10s")

	for {
		select {
		case <-ctx.Done():
			slog.Info("Heartbeat stopped due to context cancellation",
				"worker_id", rc.workerID)
			return

		case <-rc.heartbeatStop:
			slog.Info("Heartbeat stopped",
				"worker_id", rc.workerID)
			return

		case <-ticker.C:
			rc.sendHeartbeat()
		}
	}
}

// sendHeartbeat sends a single heartbeat to the Raft cluster
func (rc *RegistrationClient) sendHeartbeat() {
	rc.mu.RLock()
	if !rc.registered || rc.raftClient == nil {
		rc.mu.RUnlock()
		slog.Warn("Cannot send heartbeat - not registered",
			"worker_id", rc.workerID)
		return
	}
	client := rc.raftClient
	rc.mu.RUnlock()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	req := &raftpb.WorkerHeartbeatRequest{
		WorkerId:  int32(rc.workerID),
		Timestamp: time.Now().Unix(),
	}

	resp, err := client.WorkerHeartbeat(ctx, req)
	if err != nil {
		slog.Error("Heartbeat failed",
			"worker_id", rc.workerID,
			"error", err)

		// On heartbeat failure, try to re-register
		go rc.tryReregister()
		return
	}

	if !resp.Success {
		slog.Warn("Heartbeat rejected by Raft",
			"worker_id", rc.workerID)

		// Try to re-register
		go rc.tryReregister()
		return
	}

	// Check if partition table version changed
	rc.mu.RLock()
	localVersion := rc.partitionTableVersion
	rc.mu.RUnlock()

	if resp.PartitionTableVersion > localVersion {
		slog.Info("Partition table version updated",
			"worker_id", rc.workerID,
			"old_version", localVersion,
			"new_version", resp.PartitionTableVersion)

		// TODO: Fetch updated partition table in future stages
		rc.mu.Lock()
		rc.partitionTableVersion = resp.PartitionTableVersion
		rc.mu.Unlock()
	}
}

// tryReregister attempts to re-register with the Raft cluster
// Called when heartbeat fails
func (rc *RegistrationClient) tryReregister() {
	rc.mu.Lock()
	rc.registered = false
	rc.mu.Unlock()

	slog.Info("Attempting to re-register with Raft cluster",
		"worker_id", rc.workerID)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	err := rc.RegisterWithRaft(ctx)
	if err != nil {
		slog.Error("Failed to re-register with Raft cluster",
			"worker_id", rc.workerID,
			"error", err)
	}
}

// StopHeartbeat stops the heartbeat goroutine
func (rc *RegistrationClient) StopHeartbeat() {
	close(rc.heartbeatStop)
	<-rc.heartbeatDone
	slog.Info("Heartbeat goroutine stopped",
		"worker_id", rc.workerID)
}

// Close closes the connection to the Raft cluster
func (rc *RegistrationClient) Close() error {
	rc.StopHeartbeat()

	rc.mu.Lock()
	defer rc.mu.Unlock()

	if rc.conn != nil {
		return rc.conn.Close()
	}
	return nil
}

// IsRegistered returns whether the worker is currently registered
func (rc *RegistrationClient) IsRegistered() bool {
	rc.mu.RLock()
	defer rc.mu.RUnlock()
	return rc.registered
}

// GetPartitionTableVersion returns the current partition table version
func (rc *RegistrationClient) GetPartitionTableVersion() uint64 {
	rc.mu.RLock()
	defer rc.mu.RUnlock()
	return rc.partitionTableVersion
}
