package worker

import (
	"fmt"
	"log/slog"
	"sync"

	"github.com/dominikszczepaniak/distributed-cache/pkg/sharding"
)

// WorkerStore is the worker's local data storage
// It does NOT use Raft consensus, only stores assigned partition data
// Thread-safe for concurrent access
type WorkerStore struct {
	workerID int
	mu       sync.RWMutex
	data     map[int]int // key -> value (in-memory cache)
}

// NewWorkerStore creates a new worker data store
func NewWorkerStore(workerID int) *WorkerStore {
	return &WorkerStore{
		workerID: workerID,
		data:     make(map[int]int),
	}
}

// Put stores a key-value pair
// Called by primary worker for client requests
func (ws *WorkerStore) Put(key, value int) error {
	ws.mu.Lock()
	defer ws.mu.Unlock()

	ws.data[key] = value
	slog.Info("Worker PUT",
		"worker_id", ws.workerID,
		"key", key,
		"value", value)
	return nil
}

// Get retrieves a value by key
// Returns (value, exists)
func (ws *WorkerStore) Get(key int) (int, bool) {
	ws.mu.RLock()
	defer ws.mu.RUnlock()

	value, exists := ws.data[key]
	slog.Info("Worker GET",
		"worker_id", ws.workerID,
		"key", key,
		"value", value,
		"exists", exists)
	return value, exists
}

// Delete removes a key
func (ws *WorkerStore) Delete(key int) error {
	ws.mu.Lock()
	defer ws.mu.Unlock()

	delete(ws.data, key)
	slog.Info("Worker DELETE",
		"worker_id", ws.workerID,
		"key", key)
	return nil
}

// Replicate handles replication from primary (called on backup worker)
// This is called via gRPC from the primary worker
func (ws *WorkerStore) Replicate(key, value int, operation string) error {
	ws.mu.Lock()
	defer ws.mu.Unlock()

	switch operation {
	case "PUT":
		ws.data[key] = value
		slog.Info("Worker REPLICATE PUT",
			"worker_id", ws.workerID,
			"key", key,
			"value", value)
		return nil
	case "DELETE":
		delete(ws.data, key)
		slog.Info("Worker REPLICATE DELETE",
			"worker_id", ws.workerID,
			"key", key)
		return nil
	default:
		return fmt.Errorf("unknown replication operation: %s", operation)
	}
}

// GetStats returns storage statistics
func (ws *WorkerStore) GetStats() map[string]interface{} {
	ws.mu.RLock()
	defer ws.mu.RUnlock()

	return map[string]interface{}{
		"worker_id": ws.workerID,
		"key_count": len(ws.data),
	}
}

// GetWorkerID returns the worker's ID
func (ws *WorkerStore) GetWorkerID() int {
	return ws.workerID
}

// IsOwner checks if this worker owns a partition
// This is a helper for validation logic
func (ws *WorkerStore) IsOwner(partitionID sharding.PartitionID, partitionTable *sharding.PartitionTable) bool {
	primary, _, ok := partitionTable.GetReplicas(partitionID)
	if !ok {
		return false
	}
	return primary == sharding.NodeID(ws.workerID)
}
