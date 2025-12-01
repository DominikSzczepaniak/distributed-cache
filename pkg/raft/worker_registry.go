package raft

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/dominikszczepaniak/distributed-cache/pkg/sharding"
)

// WorkerStatus represents the health status of a worker
type WorkerStatus int

const (
	WorkerStatusActive WorkerStatus = iota
	WorkerStatusInactive
)

// WorkerInfo tracks metadata about a registered worker
type WorkerInfo struct {
	WorkerID      sharding.NodeID
	GRPCAddr      string
	HTTPAddr      string
	Status        WorkerStatus
	LastHeartbeat time.Time
	RegisteredAt  time.Time
}

// WorkerRegistry maintains the registry of all workers and their health status
// Thread-safe for concurrent access
type WorkerRegistry struct {
	mu              sync.RWMutex
	workers         map[sharding.NodeID]*WorkerInfo
	partitionTable  *sharding.PartitionTable
	heartbeatTimeout time.Duration
}

// NewWorkerRegistry creates a new worker registry
func NewWorkerRegistry(partitionTable *sharding.PartitionTable) *WorkerRegistry {
	return &WorkerRegistry{
		workers:          make(map[sharding.NodeID]*WorkerInfo),
		partitionTable:   partitionTable,
		heartbeatTimeout: 30 * time.Second, // Mark inactive after 30s
	}
}

// RegisterWorker registers a new worker or updates an existing worker
// Returns error if worker is already registered with different addresses
func (wr *WorkerRegistry) RegisterWorker(id sharding.NodeID, grpcAddr, httpAddr string) error {
	wr.mu.Lock()
	defer wr.mu.Unlock()

	now := time.Now()

	// Check if worker already registered
	if existing, exists := wr.workers[id]; exists {
		// Allow re-registration (worker restart)
		slog.Info("Worker re-registering",
			"worker_id", id,
			"grpc_addr", grpcAddr,
			"http_addr", httpAddr,
			"previous_status", existing.Status)

		existing.GRPCAddr = grpcAddr
		existing.HTTPAddr = httpAddr
		existing.Status = WorkerStatusActive
		existing.LastHeartbeat = now
		return nil
	}

	// New worker registration
	wr.workers[id] = &WorkerInfo{
		WorkerID:      id,
		GRPCAddr:      grpcAddr,
		HTTPAddr:      httpAddr,
		Status:        WorkerStatusActive,
		LastHeartbeat: now,
		RegisteredAt:  now,
	}

	slog.Info("Worker registered",
		"worker_id", id,
		"grpc_addr", grpcAddr,
		"http_addr", httpAddr,
		"total_workers", len(wr.workers))

	return nil
}

// RecordHeartbeat updates the last heartbeat timestamp for a worker
// Returns error if worker is not registered
func (wr *WorkerRegistry) RecordHeartbeat(id sharding.NodeID) error {
	wr.mu.Lock()
	defer wr.mu.Unlock()

	worker, exists := wr.workers[id]
	if !exists {
		return fmt.Errorf("worker %d not registered", id)
	}

	worker.LastHeartbeat = time.Now()

	// Reactivate if was inactive
	if worker.Status == WorkerStatusInactive {
		slog.Info("Worker reactivated",
			"worker_id", id,
			"was_inactive_for", time.Since(worker.LastHeartbeat))
		worker.Status = WorkerStatusActive
	}

	return nil
}

// GetWorker returns worker info for a specific worker ID
func (wr *WorkerRegistry) GetWorker(id sharding.NodeID) (*WorkerInfo, bool) {
	wr.mu.RLock()
	defer wr.mu.RUnlock()

	worker, exists := wr.workers[id]
	if !exists {
		return nil, false
	}

	// Return a copy to prevent external modification
	workerCopy := *worker
	return &workerCopy, true
}

// GetActiveWorkers returns a list of all active workers
func (wr *WorkerRegistry) GetActiveWorkers() []WorkerInfo {
	wr.mu.RLock()
	defer wr.mu.RUnlock()

	active := make([]WorkerInfo, 0, len(wr.workers))
	for _, worker := range wr.workers {
		if worker.Status == WorkerStatusActive {
			active = append(active, *worker)
		}
	}

	return active
}

// GetAllWorkers returns all workers regardless of status
func (wr *WorkerRegistry) GetAllWorkers() []WorkerInfo {
	wr.mu.RLock()
	defer wr.mu.RUnlock()

	all := make([]WorkerInfo, 0, len(wr.workers))
	for _, worker := range wr.workers {
		all = append(all, *worker)
	}

	return all
}

// MarkInactive marks a worker as inactive
func (wr *WorkerRegistry) MarkInactive(id sharding.NodeID) {
	wr.mu.Lock()
	defer wr.mu.Unlock()

	worker, exists := wr.workers[id]
	if !exists {
		return
	}

	if worker.Status != WorkerStatusInactive {
		slog.Warn("Marking worker inactive",
			"worker_id", id,
			"last_heartbeat", worker.LastHeartbeat,
			"inactive_for", time.Since(worker.LastHeartbeat))
		worker.Status = WorkerStatusInactive
	}
}

// RemoveWorker removes a worker from the registry
func (wr *WorkerRegistry) RemoveWorker(id sharding.NodeID) {
	wr.mu.Lock()
	defer wr.mu.Unlock()

	if _, exists := wr.workers[id]; exists {
		delete(wr.workers, id)
		slog.Info("Worker removed from registry",
			"worker_id", id,
			"remaining_workers", len(wr.workers))
	}
}

// CheckStaleWorkers identifies workers that haven't sent heartbeats within timeout
// and marks them as inactive. Returns list of newly inactive worker IDs
func (wr *WorkerRegistry) CheckStaleWorkers() []sharding.NodeID {
	wr.mu.Lock()
	defer wr.mu.Unlock()

	now := time.Now()
	stale := []sharding.NodeID{}

	for id, worker := range wr.workers {
		if worker.Status == WorkerStatusActive {
			timeSinceHeartbeat := now.Sub(worker.LastHeartbeat)
			if timeSinceHeartbeat > wr.heartbeatTimeout {
				slog.Warn("Worker missed heartbeat",
					"worker_id", id,
					"last_heartbeat", worker.LastHeartbeat,
					"timeout", wr.heartbeatTimeout,
					"elapsed", timeSinceHeartbeat)

				worker.Status = WorkerStatusInactive
				stale = append(stale, id)
			}
		}
	}

	return stale
}

// MonitorWorkers runs a background goroutine that periodically checks for stale workers
// Context cancellation stops the monitoring
func (wr *WorkerRegistry) MonitorWorkers(ctx context.Context) {
	ticker := time.NewTicker(10 * time.Second) // Check every 10 seconds
	defer ticker.Stop()

	slog.Info("Worker health monitoring started",
		"check_interval", "10s",
		"heartbeat_timeout", wr.heartbeatTimeout)

	for {
		select {
		case <-ctx.Done():
			slog.Info("Worker health monitoring stopped")
			return
		case <-ticker.C:
			staleWorkers := wr.CheckStaleWorkers()
			if len(staleWorkers) > 0 {
				slog.Warn("Found stale workers",
					"count", len(staleWorkers),
					"workers", staleWorkers)

				// TODO: Trigger partition table rebalancing when workers become inactive
				// This will be implemented in later stages
			}
		}
	}
}

// GetWorkerCount returns the total number of registered workers
func (wr *WorkerRegistry) GetWorkerCount() int {
	wr.mu.RLock()
	defer wr.mu.RUnlock()
	return len(wr.workers)
}

// GetActiveWorkerCount returns the number of active workers
func (wr *WorkerRegistry) GetActiveWorkerCount() int {
	wr.mu.RLock()
	defer wr.mu.RUnlock()

	count := 0
	for _, worker := range wr.workers {
		if worker.Status == WorkerStatusActive {
			count++
		}
	}
	return count
}
