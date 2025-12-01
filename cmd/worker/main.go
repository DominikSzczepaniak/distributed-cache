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

	// TODO Stage 2: Register with Raft cluster
	// regClient := worker.NewRegistrationClient(nodeID, cfg.HTTPAddr, cfg.RaftAddrs, partitionTable)
	// ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	// defer cancel()
	// if err := regClient.Register(ctx); err != nil {
	//     slog.Error("Failed to register with Raft", "error", err)
	//     os.Exit(1)
	// }
	// go regClient.StartHeartbeat(5 * time.Second)

	slog.Info("Worker node ready",
		"worker_id", cfg.WorkerID,
		"http_addr", cfg.HTTPAddr,
		"grpc_addr", cfg.GRPCAddr)

	// Wait for shutdown signal
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)

	sig := <-sigCh
	slog.Info("Received shutdown signal", "signal", sig)

	// Graceful shutdown
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := apiServer.Stop(ctx); err != nil {
		slog.Error("Error stopping API server", "error", err)
	}

	grpcServer.GracefulStop()

	slog.Info("Worker node stopped")
}
