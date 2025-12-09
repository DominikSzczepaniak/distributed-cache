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
	Epoch uint64 `json:"epoch"`
}

type DeleteRequest struct {
	Key   string `json:"key"`
	Epoch uint64 `json:"epoch"`
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

func (s *Server) sendDeleteReplication(replicaURL string, req DeleteRequest) error {
	body, err := json.Marshal(req)
	if err != nil {
		return fmt.Errorf("failed to marshal delete request: %w", err)
	}

	resp, err := s.httpClient.Post(replicaURL+"/internal/replicate-delete", "application/json", bytes.NewBuffer(body))
	if err != nil {
		return fmt.Errorf("failed to send delete replication: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("replica returned status %d", resp.StatusCode)
	}

	return nil
}

func (s *Server) HandlePut(w http.ResponseWriter, r *http.Request) {
	if !s.lease.IsActive() {
		http.Error(w, "Node is Fenced (Lease Expired)", http.StatusServiceUnavailable)
		return
	}

	var req WriteRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "Invalid request body", http.StatusBadRequest)
		return
	}

	localEpoch := s.stateMgr.GetEpoch()
	if req.Epoch < localEpoch {
		http.Error(w, "Client Epoch Stale", http.StatusPreconditionFailed)
		return
	}

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
			http.Error(w, "Not Primary for Shard", http.StatusBadRequest)
			return
		}

		if shard.Status == metadata.ShardStatusLocked || shard.Status == metadata.ShardStatusMigrating {
			http.Error(w, "Shard is Locked/Migrating", http.StatusLocked)
			return
		}
	}

	replicaURLs := s.stateMgr.GetReplicaURLs(shardID)
	for _, replicaURL := range replicaURLs {
		if err := s.sendReplication(replicaURL, req); err != nil {
			slog.Error("Replication failed", "replica", replicaURL, "err", err)
			http.Error(w, "Replication Failed", http.StatusInternalServerError)
			return
		}
	}

	s.cache.Put(req.Key, req.Value)
	w.WriteHeader(http.StatusOK)
}
func (s *Server) HandleDelete(w http.ResponseWriter, r *http.Request) {
	if !s.lease.IsActive() {
		http.Error(w, "Node is Fenced (Lease Expired)", http.StatusServiceUnavailable)
		return
	}

	var req DeleteRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "Invalid request body", http.StatusBadRequest)
		return
	}

	localEpoch := s.stateMgr.GetEpoch()
	if req.Epoch < localEpoch {
		http.Error(w, "Client Epoch Stale", http.StatusPreconditionFailed)
		return
	}

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
			http.Error(w, "Not Primary for Shard", http.StatusBadRequest)
			return
		}

		if shard.Status == metadata.ShardStatusLocked || shard.Status == metadata.ShardStatusMigrating {
			http.Error(w, "Shard is Locked/Migrating", http.StatusLocked)
			return
		}
	}

	replicaURLs := s.stateMgr.GetReplicaURLs(shardID)
	for _, replicaURL := range replicaURLs {
		if err := s.sendDeleteReplication(replicaURL, req); err != nil {
			slog.Error("Delete replication failed", "replica", replicaURL, "err", err)
			http.Error(w, "Replication Failed", http.StatusInternalServerError)
			return
		}
	}

	s.cache.Delete(req.Key)
	w.WriteHeader(http.StatusOK)
}

func (s *Server) HandleGet(w http.ResponseWriter, r *http.Request) {
	if !s.lease.IsActive() {
		http.Error(w, "Node is Fenced (Lease Expired)", http.StatusServiceUnavailable)
		return
	}

	key := r.URL.Query().Get("key")
	if key == "" {
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

func (s *Server) HandleReplicate(w http.ResponseWriter, r *http.Request) {
	var req WriteRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "Invalid request body", http.StatusBadRequest)
		return
	}

	localEpoch := s.stateMgr.GetEpoch()
	if req.Epoch < localEpoch {
		http.Error(w, "Primary is Stale", http.StatusPreconditionFailed)
		return
	}

	s.cache.Put(req.Key, req.Value)
	w.WriteHeader(http.StatusOK)
}
func (s *Server) HandleDeleteReplicate(w http.ResponseWriter, r *http.Request) {
	var req DeleteRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "Invalid request body", http.StatusBadRequest)
		return
	}

	localEpoch := s.stateMgr.GetEpoch()
	if req.Epoch < localEpoch {
		http.Error(w, "Primary is Stale", http.StatusPreconditionFailed)
		return
	}

	s.cache.Delete(req.Key)
	w.WriteHeader(http.StatusOK)
}

func (s *Server) HandleExport(w http.ResponseWriter, r *http.Request) {
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

func (s *Server) HandleImport(w http.ResponseWriter, r *http.Request) {
	var data map[string]string
	if err := json.NewDecoder(r.Body).Decode(&data); err != nil {
		http.Error(w, "Invalid request body", http.StatusBadRequest)
		return
	}

	s.cache.Import(data)
	w.WriteHeader(http.StatusOK)
}

func (s *Server) HandlePull(w http.ResponseWriter, r *http.Request) {
	sourceURL := r.URL.Query().Get("source")
	shardIDStr := r.URL.Query().Get("shard")

	if sourceURL == "" || shardIDStr == "" {
		http.Error(w, "Missing source or shard parameter", http.StatusBadRequest)
		return
	}

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
