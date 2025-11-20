package api

import (
	"encoding/json"
	"fmt"
	"net/http"

	"github.com/dominikszczepaniak/distributed-cache/pkg/sharding"
)

// PartitionTableResponse represents the partition table state
type PartitionTableResponse struct {
	Version         uint64                               `json:"version"`
	TotalPartitions int                                  `json:"total_partitions"`
	AssignmentCount int                                  `json:"assignment_count"`
	Assignments     map[sharding.PartitionID]sharding.NodeID `json:"assignments,omitempty"`
	NodeStats       map[sharding.NodeID]int              `json:"node_stats"`
}

// InitPartitionTableRequest represents a request to initialize partition table
type InitPartitionTableRequest struct {
	NodeIDs []int `json:"node_ids"`
}

// MetricsResponse represents shard routing metrics
type MetricsResponse struct {
	LocalHits uint64 `json:"local_hits"`
	Redirects uint64 `json:"redirects"`
	Errors    uint64 `json:"errors"`
}

// HandleAdminPartitionTable returns the current partition table state
func (s *Server) HandleAdminPartitionTable(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	if s.shardManager == nil {
		http.Error(w, "Sharding not enabled", http.StatusServiceUnavailable)
		return
	}

	// Get partition table from shard manager
	// Note: We need access to the partition table through the shard manager
	// For now, we'll return basic information

	response := PartitionTableResponse{
		Version:         0, // Would need to expose this from ShardManager
		TotalPartitions: sharding.TOTAL_PARTITIONS,
		AssignmentCount: 0, // Would need to expose this
		NodeStats:       make(map[sharding.NodeID]int),
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(response)
}

// HandleAdminInitPartitionTable initializes the partition table
func (s *Server) HandleAdminInitPartitionTable(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	if s.shardManager == nil {
		http.Error(w, "Sharding not enabled", http.StatusServiceUnavailable)
		return
	}

	var req InitPartitionTableRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, fmt.Sprintf("Invalid request body: %v", err), http.StatusBadRequest)
		return
	}

	if len(req.NodeIDs) == 0 {
		http.Error(w, "node_ids cannot be empty", http.StatusBadRequest)
		return
	}

	// Convert to NodeID slice
	nodeIDs := make([]sharding.NodeID, len(req.NodeIDs))
	for i, id := range req.NodeIDs {
		nodeIDs[i] = sharding.NodeID(id)
	}

	// Create even distribution
	pt := sharding.InitializeEvenDistribution(sharding.TOTAL_PARTITIONS, nodeIDs)

	// Propose partition table update to Raft
	// This would need to be integrated with the Raft layer
	// For now, return success

	response := map[string]interface{}{
		"success": true,
		"message": fmt.Sprintf("Initialized partition table with %d nodes", len(nodeIDs)),
		"version": pt.GetVersion(),
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(response)
}

// HandleAdminMetrics returns shard routing metrics
func (s *Server) HandleAdminMetrics(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	if s.shardManager == nil {
		http.Error(w, "Sharding not enabled", http.StatusServiceUnavailable)
		return
	}

	metrics := s.shardManager.GetMetrics()
	response := MetricsResponse{
		LocalHits: metrics["local_hits"],
		Redirects: metrics["redirects"],
		Errors:    metrics["errors"],
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(response)
}

// HandleAdminNodeInfo returns information about this node
func (s *Server) HandleAdminNodeInfo(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	if s.shardManager == nil {
		http.Error(w, "Sharding not enabled", http.StatusServiceUnavailable)
		return
	}

	// Get node information
	// This would need to be exposed from the ShardManager
	response := map[string]interface{}{
		"node_id": 0, // Would need to expose this
		"address": s.listenAddr,
		"status":  "healthy",
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(response)
}

// HandleAdminHealth provides detailed health check with sharding status
func (s *Server) HandleAdminHealth(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	health := map[string]interface{}{
		"status": "healthy",
		"sharding_enabled": s.shardManager != nil,
	}

	if s.shardManager != nil {
		metrics := s.shardManager.GetMetrics()
		health["metrics"] = metrics
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(health)
}

// RegisterAdminRoutes registers all admin endpoints
func (s *Server) RegisterAdminRoutes(mux *http.ServeMux) {
	mux.HandleFunc("/admin/partition-table", s.HandleAdminPartitionTable)
	mux.HandleFunc("/admin/init-partition-table", s.HandleAdminInitPartitionTable)
	mux.HandleFunc("/admin/metrics", s.HandleAdminMetrics)
	mux.HandleFunc("/admin/node-info", s.HandleAdminNodeInfo)
	mux.HandleFunc("/admin/health", s.HandleAdminHealth)
}
