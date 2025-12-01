package main

import (
	"context"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/dominikszczepaniak/distributed-cache/pkg/raft/raftpb"
	"github.com/dominikszczepaniak/distributed-cache/pkg/sharding"
	"github.com/dominikszczepaniak/distributed-cache/pkg/worker"
	"google.golang.org/grpc"
)

func main() {
	opts := &slog.HandlerOptions{
		Level: slog.LevelInfo,
	}
	logger := slog.New(slog.NewTextHandler(os.Stdout, opts))
	slog.SetDefault(logger)

	slog.Info("Starting Worker node...")

	// Load configuration from environment
	cfg := worker.LoadConfig()

	slog.Info("Worker configuration loaded",
		"worker_id", cfg.WorkerID,
		"http_addr", cfg.HTTPAddr,
		"grpc_addr", cfg.GRPCAddr)

	// Create worker data storage
	store := worker.NewWorkerStore(cfg.WorkerID)

	// Initialize partition table (will be fetched from Raft in Stage 2)
	partitionTable := sharding.NewPartitionTable()

	// For Stage 1: Assign all partitions to this worker for standalone testing
	// This will be replaced with Raft-based assignment in Stage 2
	if len(cfg.AssignedPartitions) > 0 {
		slog.Info("Assigning partitions to worker",
			"partition_count", len(cfg.AssignedPartitions))
		for _, pid := range cfg.AssignedPartitions {
			partitionTable.SetReplicas(pid, sharding.NodeID(cfg.WorkerID), -1)
		}
	} else {
		// Standalone mode: assign ALL partitions to this worker
		slog.Info("Standalone mode: assigning all partitions to this worker")
		for i := 0; i < sharding.TOTAL_PARTITIONS; i++ {
			partitionTable.SetReplicas(sharding.PartitionID(i), sharding.NodeID(cfg.WorkerID), -1)
		}
	}

	// Initialize partitioner for consistent hashing
	partitioner := sharding.NewPartitioner()

	// Create shard manager
	nodeID := sharding.NodeID(cfg.WorkerID)
	shardManager := sharding.NewShardManager(nodeID, partitionTable, partitioner)

	// Start gRPC server for replication
	grpcServer := grpc.NewServer()
	replicationHandler := worker.NewReplicationHandler(store)
	raftpb.RegisterRaftServer(grpcServer, replicationHandler)

	grpcListener, err := net.Listen("tcp", cfg.GRPCAddr)
	if err != nil {
		slog.Error("Failed to start gRPC listener", "error", err)
		os.Exit(1)
	}

	go func() {
		slog.Info("Starting Worker gRPC server", "addr", cfg.GRPCAddr)
		if err := grpcServer.Serve(grpcListener); err != nil {
			slog.Error("gRPC server error", "error", err)
		}
	}()

	// Start HTTP API server
	apiServer := worker.NewAPIServer(store, cfg.HTTPAddr, shardManager)

	go func() {
		slog.Info("Starting Worker HTTP API", "addr", cfg.HTTPAddr)
		if err := apiServer.Start(); err != nil && err != http.ErrServerClosed {
			slog.Error("Worker API server error", "error", err)
		}
	}()

	// Stage 2: Register with Raft cluster
	var regClient *worker.RegistrationClient
	if len(cfg.RaftAddrs) > 0 {
		slog.Info("Registering with Raft cluster", "raft_addrs", cfg.RaftAddrs)

		regClient = worker.NewRegistrationClient(
			cfg.WorkerID,
			cfg.GRPCAddr,
			cfg.HTTPAddr,
			cfg.RaftAddrs,
			partitionTable,
		)

		regCtx, regCancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer regCancel()

		if err := regClient.RegisterWithRaft(regCtx); err != nil {
			slog.Error("Failed to register with Raft cluster", "error", err)
			slog.Info("Continuing in standalone mode...")
		} else {
			// Start heartbeat goroutine
			heartbeatCtx, _ := context.WithCancel(context.Background())
			go regClient.StartHeartbeat(heartbeatCtx)

			slog.Info("Worker registered and heartbeat started",
				"worker_id", cfg.WorkerID,
				"partition_table_version", regClient.GetPartitionTableVersion())
		}
	} else {
		slog.Info("No Raft addresses configured, running in standalone mode")
	}

	slog.Info("Worker node ready",
		"worker_id", cfg.WorkerID,
		"http_addr", cfg.HTTPAddr,
		"grpc_addr", cfg.GRPCAddr,
		"registered_with_raft", regClient != nil && regClient.IsRegistered())

	// Wait for shutdown signal
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)

	sig := <-sigCh
	slog.Info("Received shutdown signal", "signal", sig)

	// Graceful shutdown
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// Stop registration client and heartbeat
	if regClient != nil {
		if err := regClient.Close(); err != nil {
			slog.Error("Error closing registration client", "error", err)
		}
	}

	// Stop API server
	if err := apiServer.Stop(ctx); err != nil {
		slog.Error("Error stopping API server", "error", err)
	}

	// Stop gRPC server
	grpcServer.GracefulStop()

	slog.Info("Worker node stopped")
}
