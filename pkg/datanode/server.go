package datanode

import (
	"bytes"
	"encoding/json"
	"fmt"
	"hash/fnv"
	"log/slog"
	"net/http"
	"time"

	"github.com/dominikszczepaniak/distributed-cache/pkg/cache"
	"github.com/dominikszczepaniak/distributed-cache/pkg/metadata"
)

type WriteRequest struct {
	Key   string `json:"key"`
	Value string `json:"value"`
	Epoch uint64 `json:"epoch"` // Client MUST provide this
}

type Server struct {
	cache      *cache.ConcurrentMapCache
	lease      *LeaseManager
	stateMgr   *StateManager
	nodeID     string
	httpClient *http.Client
}

func NewServer(c *cache.ConcurrentMapCache, l *LeaseManager, s *StateManager, nodeID string) *Server {
	return &Server{
		cache:    c,
		lease:    l,
		stateMgr: s,
		nodeID:   nodeID,
		httpClient: &http.Client{
			Timeout: 5 * time.Second,
		},
	}
}

// sendReplication sends a replication request to a replica
func (s *Server) sendReplication(replicaURL string, req WriteRequest) error {
	body, err := json.Marshal(req)
	if err != nil {
		return fmt.Errorf("failed to marshal request: %w", err)
	}

	resp, err := s.httpClient.Post(replicaURL+"/internal/replicate", "application/json", bytes.NewBuffer(body))
	if err != nil {
		return fmt.Errorf("failed to send replication: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("replica returned status %d", resp.StatusCode)
	}

	return nil
}

func (s *Server) HandlePut(w http.ResponseWriter, r *http.Request) {
	// 1. LEASE CHECK (Safety against Split Brain)
	if !s.lease.IsActive() {
		http.Error(w, "Node is Fenced (Lease Expired)", http.StatusServiceUnavailable) // 503
		return
	}

	var req WriteRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "Invalid request body", http.StatusBadRequest)
		return
	}

	// 2. EPOCH CHECK (Safety against Stale Clients)
	localEpoch := s.stateMgr.GetEpoch()
	if req.Epoch < localEpoch {
		http.Error(w, "Client Epoch Stale", http.StatusPreconditionFailed) // 412
		return
	}

	// 3. OWNERSHIP CHECK
	config := s.stateMgr.Get()
	var shardID int
	if config.TotalShards > 0 {
		h := fnv.New32a()
		h.Write([]byte(req.Key))
		shardID = int(h.Sum32()) % config.TotalShards
		if shardID < 0 {
			shardID = -shardID
		}

		shard, exists := config.Shards[shardID]
		if !exists || shard.PrimaryID != s.nodeID {
			http.Error(w, "Not Primary for Shard", http.StatusBadRequest) // 400
			return
		}

		if shard.Status == metadata.ShardStatusLocked || shard.Status == metadata.ShardStatusMigrating {
			http.Error(w, "Shard is Locked/Migrating", http.StatusLocked) // 423
			return
		}
	}

	// 4. SYNCHRONOUS REPLICATION
	replicaURLs := s.stateMgr.GetReplicaURLs(shardID)
	for _, replicaURL := range replicaURLs {
		if err := s.sendReplication(replicaURL, req); err != nil {
			slog.Error("Replication failed", "replica", replicaURL, "err", err)
			http.Error(w, "Replication Failed", http.StatusInternalServerError) // 500
			return
		}
	}

	// 5. LOCAL COMMIT (only after successful replication)
	s.cache.Put(req.Key, req.Value)
	w.WriteHeader(http.StatusOK)
}

func (s *Server) HandleGet(w http.ResponseWriter, r *http.Request) {
	// 1. LEASE CHECK
	if !s.lease.IsActive() {
		http.Error(w, "Node is Fenced (Lease Expired)", http.StatusServiceUnavailable) // 503
		return
	}

	// Parse key from query param "key"
	key := r.URL.Query().Get("key")
	if key == "" {
		// Fallback: try path if needed, but query param is standard for simple GET
		// Or maybe /data/key?
		// Let's stick to query param ?key=... for now based on typical patterns unless specified.
		// Prompt didn't specify URL structure for GET, just "GET /data".
		http.Error(w, "Missing key parameter", http.StatusBadRequest)
		return
	}

	val := s.cache.Get(key)
	if val == "" {
		http.Error(w, "Key not found", http.StatusNotFound)
		return
	}

	w.Write([]byte(val))
}

// HandleReplicate handles internal replication requests from the Primary
func (s *Server) HandleReplicate(w http.ResponseWriter, r *http.Request) {
	var req WriteRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "Invalid request body", http.StatusBadRequest)
		return
	}

	// EPOCH CHECK (Critical Fencing)
	// If Primary thinks it's Epoch 10, but I know it's Epoch 11,
	// the Primary has been demoted and doesn't know it yet.
	localEpoch := s.stateMgr.GetEpoch()
	if req.Epoch < localEpoch {
		http.Error(w, "Primary is Stale", http.StatusPreconditionFailed) // 412
		return
	}

	// WRITE (direct to cache, no ownership check for replicas)
	s.cache.Put(req.Key, req.Value)
	w.WriteHeader(http.StatusOK)
}

// HandleExport streams all keys belonging to a specific shard
func (s *Server) HandleExport(w http.ResponseWriter, r *http.Request) {
	// TODO: Add authorization check (e.g., token from Controller)

	shardIDStr := r.URL.Query().Get("shard")
	if shardIDStr == "" {
		http.Error(w, "Missing shard parameter", http.StatusBadRequest)
		return
	}

	var shardID int
	if _, err := fmt.Sscanf(shardIDStr, "%d", &shardID); err != nil {
		http.Error(w, "Invalid shard parameter", http.StatusBadRequest)
		return
	}

	config := s.stateMgr.Get()
	if config.TotalShards == 0 {
		http.Error(w, "Cluster not configured", http.StatusServiceUnavailable)
		return
	}

	data := s.cache.ExportShard(shardID, config.TotalShards)

	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(data); err != nil {
		slog.Error("Failed to encode export data", "err", err)
		return
	}
}

// HandleImport accepts a stream of keys and inserts them into the cache
func (s *Server) HandleImport(w http.ResponseWriter, r *http.Request) {
	// TODO: Add authorization check

	var data map[string]string
	if err := json.NewDecoder(r.Body).Decode(&data); err != nil {
		http.Error(w, "Invalid request body", http.StatusBadRequest)
		return
	}

	s.cache.Import(data)
	w.WriteHeader(http.StatusOK)
}

// HandlePull triggers this node to pull data from a source node
func (s *Server) HandlePull(w http.ResponseWriter, r *http.Request) {
	sourceURL := r.URL.Query().Get("source")
	shardIDStr := r.URL.Query().Get("shard")

	if sourceURL == "" || shardIDStr == "" {
		http.Error(w, "Missing source or shard parameter", http.StatusBadRequest)
		return
	}

	// Call Export on Source
	resp, err := s.httpClient.Get(fmt.Sprintf("%s/internal/export?shard=%s", sourceURL, shardIDStr))
	if err != nil {
		slog.Error("Failed to pull data", "source", sourceURL, "err", err)
		http.Error(w, "Failed to pull data", http.StatusBadGateway)
		return
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		slog.Error("Source returned error", "status", resp.StatusCode)
		http.Error(w, "Source returned error", http.StatusBadGateway)
		return
	}

	// Decode and Import
	var data map[string]string
	if err := json.NewDecoder(resp.Body).Decode(&data); err != nil {
		slog.Error("Failed to decode pulled data", "err", err)
		http.Error(w, "Failed to decode data", http.StatusInternalServerError)
		return
	}

	s.cache.Import(data)
	slog.Info("Successfully pulled data", "shard", shardIDStr, "keys", len(data))
	w.WriteHeader(http.StatusOK)
}
