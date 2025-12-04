package datanode

import (
	"encoding/json"
	"hash/fnv"
	"net/http"

	"github.com/dominikszczepaniak/distributed-cache/pkg/cache"
)

type WriteRequest struct {
	Key   string `json:"key"`
	Value string `json:"value"`
	Epoch uint64 `json:"epoch"` // Client MUST provide this
}

type Server struct {
	cache    *cache.ConcurrentMapCache
	lease    *LeaseManager
	stateMgr *StateManager
	nodeID   string
}

func NewServer(c *cache.ConcurrentMapCache, l *LeaseManager, s *StateManager, nodeID string) *Server {
	return &Server{
		cache:    c,
		lease:    l,
		stateMgr: s,
		nodeID:   nodeID,
	}
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
	if config.TotalShards > 0 {
		h := fnv.New32a()
		h.Write([]byte(req.Key))
		shardID := int(h.Sum32()) % config.TotalShards
		// Handle negative result from modulo if int is signed (though Sum32 is uint32, casting to int might be safe for small TotalShards, but let's be safe)
		if shardID < 0 {
			shardID = -shardID
		}

		shard, exists := config.Shards[shardID]
		if !exists || shard.PrimaryID != s.nodeID {
			http.Error(w, "Not Primary for Shard", http.StatusBadRequest) // 400
			return
		}
	}

	// 4. WRITE
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
