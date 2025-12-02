package api

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/dominikszczepaniak/distributed-cache/pkg/raft"
	"github.com/dominikszczepaniak/distributed-cache/pkg/raft/raftpb"
	"github.com/dominikszczepaniak/distributed-cache/pkg/replication"
	"github.com/dominikszczepaniak/distributed-cache/pkg/sharding"
	"google.golang.org/protobuf/types/known/wrapperspb"
)

type Server struct {
	raft              *raft.Raft
	listenAddr        string
	httpServer        *http.Server
	retrier           *Retrier
	idempotencyCache  *IdempotencyCache
	leaderCache       *LeaderCache
	shardManager      *sharding.ShardManager
	replicationClient *replication.Client
	workerRegistry    *raft.WorkerRegistry
}

func NewServer(r *raft.Raft, listenAddr string, shardManager *sharding.ShardManager, replClient *replication.Client, workerRegistry *raft.WorkerRegistry) *Server {
	return &Server{
		raft:              r,
		listenAddr:        listenAddr,
		retrier:           NewRetrier(DefaultRetryConfigs["PUT"]),
		idempotencyCache:  NewIdempotencyCache(5 * time.Minute),
		leaderCache:       NewLeaderCache(1 * time.Second),
		shardManager:      shardManager,
		replicationClient: replClient,
		workerRegistry:    workerRegistry,
	}
}

func (s *Server) Start() error {
	mux := http.NewServeMux()

	mux.HandleFunc("/kv", s.handleKV)
	mux.HandleFunc("/kv/", s.handleKVWithKey)

	mux.HandleFunc("/health", s.handleHealth)
	mux.HandleFunc("/status", s.handleStatus)
	mux.HandleFunc("/leader", s.handleLeader)

	// Register admin endpoints
	s.RegisterAdminRoutes(mux)

	s.httpServer = &http.Server{
		Addr:    s.listenAddr,
		Handler: mux,
	}

	slog.Info(fmt.Sprintf("Starting HTTP API server on %s", s.listenAddr))
	return s.httpServer.ListenAndServe()
}

func (s *Server) Stop() error {
	if s.httpServer != nil {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		return s.httpServer.Shutdown(ctx)
	}
	return nil
}

func (s *Server) handleKV(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodPost:
		s.handlePut(w, r)
	default:
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
	}
}

func (s *Server) handleKVWithKey(w http.ResponseWriter, r *http.Request) {
	keyStr := strings.TrimPrefix(r.URL.Path, "/kv/")
	key, err := strconv.Atoi(keyStr)
	if err != nil {
		http.Error(w, "Invalid key", http.StatusBadRequest)
		return
	}

	switch r.Method {
	case http.MethodGet:
		s.handleGet(w, r, key)
	case http.MethodDelete:
		s.handleDelete(w, r, key)
	default:
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
	}
}

func (s *Server) handlePut(w http.ResponseWriter, r *http.Request) {
	var req PutRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "Invalid request body", http.StatusBadRequest)
		return
	}

	// STAGE 3: ROUTE TO WORKER INSTEAD OF HANDLING LOCALLY
	// Determine which worker should handle this key
	if s.workerRegistry != nil && s.shardManager != nil {
		keyStr := fmt.Sprintf("%d", req.Key)
		partitionID := s.shardManager.GetPartitionID(keyStr)

		// Get primary worker for this partition
		primaryWorker, _, ok := s.shardManager.GetReplicas(partitionID)
		if !ok {
			http.Error(w, "Partition not assigned to any worker", http.StatusServiceUnavailable)
			return
		}

		// Look up worker HTTP address from registry
		workerAddr, err := s.workerRegistry.GetWorkerHTTPAddr(primaryWorker)
		if err != nil {
			slog.Error("Failed to get worker address",
				"worker_id", primaryWorker,
				"partition_id", partitionID,
				"key", req.Key,
				"error", err)
			http.Error(w, fmt.Sprintf("Worker unavailable: %v", err), http.StatusServiceUnavailable)
			return
		}

		// Return HTTP 307 Temporary Redirect to worker
		redirectURL := fmt.Sprintf("%s/kv", workerAddr)
		w.Header().Set("Location", redirectURL)
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusTemporaryRedirect)

		json.NewEncoder(w).Encode(map[string]interface{}{
			"message":      "Request should be sent to worker node",
			"worker_id":    int(primaryWorker),
			"worker_address": workerAddr,
			"partition_id": int(partitionID),
			"redirect_url": redirectURL,
		})

		slog.Info("Redirecting PUT request to worker",
			"key", req.Key,
			"partition_id", partitionID,
			"worker_id", primaryWorker,
			"worker_addr", workerAddr)
		return
	}

	// FALLBACK: If no worker registry (backward compatibility), handle via Raft
	// This path will be deprecated in Stage 4
	if s.shardManager != nil {
		keyStr := fmt.Sprintf("%d", req.Key)
		if err := s.shardManager.ValidateKey(keyStr); err != nil {
			if wrongNodeErr, ok := err.(*sharding.WrongNodeError); ok {
				// Return redirect response
				w.Header().Set("Location", wrongNodeErr.RedirectLocation())
				w.Header().Set("Content-Type", "application/json")
				w.WriteHeader(wrongNodeErr.HTTPStatusCode())
				json.NewEncoder(w).Encode(RedirectResponse{
					Error:       "MOVED",
					Message:     wrongNodeErr.Error(),
					NodeID:      fmt.Sprintf("%d", wrongNodeErr.CorrectNode),
					Address:     wrongNodeErr.CorrectAddr,
					PartitionID: uint16(wrongNodeErr.PartitionID),
				})
				return
			}
			// Other validation errors
			http.Error(w, fmt.Sprintf("Shard validation error: %v", err), http.StatusInternalServerError)
			return
		}

		// SYNCHRONOUS REPLICATION TO BACKUP (PRIMARY-BACKUP PATTERN)
		// After validating this is the primary, replicate to backup before Raft commit
		partitionID := s.shardManager.GetPartitionID(keyStr)
		_, backupNode, ok := s.shardManager.GetReplicas(partitionID)

		if ok && backupNode >= 0 && s.replicationClient != nil {
			replicationCtx, replicationCancel := context.WithTimeout(r.Context(), 1*time.Second)
			defer replicationCancel()

			err := s.replicationClient.Replicate(replicationCtx, backupNode, req.Key, req.Value)
			if err != nil {
				slog.Error(fmt.Sprintf("Replication to backup %d failed for key %d: %v", backupNode, req.Key, err))
				// FAIL THE WRITE - Strong consistency guarantee
				http.Error(w, fmt.Sprintf("Replication failed: %v", err), http.StatusServiceUnavailable)
				return
			}

			slog.Info(fmt.Sprintf("Successfully replicated PUT key=%d to backup node %d", req.Key, backupNode))
		}
	}

	idempotencyToken := r.Header.Get("Idempotency-Key")
	if idempotencyToken == "" {
		idempotencyToken = s.generateIdempotencyToken(&req, getClientID(r))
	}

	if cachedResp, found := s.idempotencyCache.Get(idempotencyToken); found {
		slog.Info(fmt.Sprintf("Returning cached response for token: %s", idempotencyToken))
		resp := PutResponse{
			Success: cachedResp.Success,
			Message: "Key-value pair stored successfully (cached)",
		}
		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("X-Cache", "HIT")
		json.NewEncoder(w).Encode(resp)
		return
	}

	var success bool
	var broadcastErr error

	retryFunc := func(ctx context.Context) error {
		msg := &raftpb.Message{
			Type:             raftpb.Message_PUT,
			Key:              int32(req.Key),
			Value:            wrapperspb.Int32(int32(req.Value)),
			IdempotencyToken: idempotencyToken,
			ClientId:         getClientID(r),
		}

		resp, err := s.raft.Forward(ctx, msg)
		if err != nil {
			broadcastErr = err
			return err
		}

		success = resp.Success
		return nil
	}

	ctx, cancel := context.WithTimeout(r.Context(), 15*time.Second)
	defer cancel()

	err := s.retrier.ExecuteWithRetry(ctx, retryFunc)

	if err != nil {
		if ctx.Err() == context.DeadlineExceeded {
			http.Error(w, "Request timeout after retries", http.StatusGatewayTimeout)
			return
		}
		if broadcastErr != nil && strings.Contains(broadcastErr.Error(), "no leader") {
			http.Error(w, "No leader available", http.StatusServiceUnavailable)
			return
		}
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	s.idempotencyCache.Set(idempotencyToken, raft.BroadcastResponse{
		Success: success,
		Value:   0,
		Error:   nil,
	})

	resp := PutResponse{
		Success: success,
		Message: "Key-value pair stored successfully",
	}

	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("X-Cache", "MISS")
	json.NewEncoder(w).Encode(resp)
}

func (s *Server) handleGet(w http.ResponseWriter, r *http.Request, key int) {
	// STAGE 3: ROUTE TO WORKER INSTEAD OF HANDLING LOCALLY
	// Determine which worker should handle this key
	if s.workerRegistry != nil && s.shardManager != nil {
		keyStr := fmt.Sprintf("%d", key)
		partitionID := s.shardManager.GetPartitionID(keyStr)

		// Get primary worker for this partition
		primaryWorker, _, ok := s.shardManager.GetReplicas(partitionID)
		if !ok {
			http.Error(w, "Partition not assigned to any worker", http.StatusServiceUnavailable)
			return
		}

		// Look up worker HTTP address from registry
		workerAddr, err := s.workerRegistry.GetWorkerHTTPAddr(primaryWorker)
		if err != nil {
			slog.Error("Failed to get worker address",
				"worker_id", primaryWorker,
				"partition_id", partitionID,
				"key", key,
				"error", err)
			http.Error(w, fmt.Sprintf("Worker unavailable: %v", err), http.StatusServiceUnavailable)
			return
		}

		// Return HTTP 307 Temporary Redirect to worker
		redirectURL := fmt.Sprintf("%s/kv/%d", workerAddr, key)
		w.Header().Set("Location", redirectURL)
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusTemporaryRedirect)

		json.NewEncoder(w).Encode(map[string]interface{}{
			"message":        "Request should be sent to worker node",
			"worker_id":      int(primaryWorker),
			"worker_address": workerAddr,
			"partition_id":   int(partitionID),
			"redirect_url":   redirectURL,
		})

		slog.Info("Redirecting GET request to worker",
			"key", key,
			"partition_id", partitionID,
			"worker_id", primaryWorker,
			"worker_addr", workerAddr)
		return
	}

	// FALLBACK: If no worker registry (backward compatibility), handle via Raft
	// This path will be deprecated in Stage 4
	if s.shardManager != nil {
		keyStr := fmt.Sprintf("%d", key)
		if err := s.shardManager.ValidateKey(keyStr); err != nil {
			if wrongNodeErr, ok := err.(*sharding.WrongNodeError); ok {
				// Return redirect response
				w.Header().Set("Location", wrongNodeErr.RedirectLocation())
				w.Header().Set("Content-Type", "application/json")
				w.WriteHeader(wrongNodeErr.HTTPStatusCode())
				json.NewEncoder(w).Encode(RedirectResponse{
					Error:       "MOVED",
					Message:     wrongNodeErr.Error(),
					NodeID:      fmt.Sprintf("%d", wrongNodeErr.CorrectNode),
					Address:     wrongNodeErr.CorrectAddr,
					PartitionID: uint16(wrongNodeErr.PartitionID),
				})
				return
			}
			// Other validation errors
			http.Error(w, fmt.Sprintf("Shard validation error: %v", err), http.StatusInternalServerError)
			return
		}
	}

	stale := r.URL.Query().Get("stale") == "true"

	var value int
	var found bool

	if stale {
		value = s.raft.GetApplication().GetValue(key)
		found = true
	} else {
		ctx, cancel := context.WithTimeout(r.Context(), 2*time.Second)
		defer cancel()

		resp, err := s.raft.ForwardGet(ctx, &raftpb.GetRequest{
			Key: int32(key),
		})

		if err != nil {
			if ctx.Err() == context.DeadlineExceeded {
				http.Error(w, "Request timeout", http.StatusGatewayTimeout)
			} else {
				http.Error(w, err.Error(), http.StatusInternalServerError)
			}
			return
		}

		value = int(resp.Value)
		found = resp.Found
	}

	respBody := GetResponse{
		Key:   key,
		Value: value,
		Found: found,
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(respBody)
}

func (s *Server) handleDelete(w http.ResponseWriter, r *http.Request, key int) {
	// STAGE 3: ROUTE TO WORKER INSTEAD OF HANDLING LOCALLY
	// Determine which worker should handle this key
	if s.workerRegistry != nil && s.shardManager != nil {
		keyStr := fmt.Sprintf("%d", key)
		partitionID := s.shardManager.GetPartitionID(keyStr)

		// Get primary worker for this partition
		primaryWorker, _, ok := s.shardManager.GetReplicas(partitionID)
		if !ok {
			http.Error(w, "Partition not assigned to any worker", http.StatusServiceUnavailable)
			return
		}

		// Look up worker HTTP address from registry
		workerAddr, err := s.workerRegistry.GetWorkerHTTPAddr(primaryWorker)
		if err != nil {
			slog.Error("Failed to get worker address",
				"worker_id", primaryWorker,
				"partition_id", partitionID,
				"key", key,
				"error", err)
			http.Error(w, fmt.Sprintf("Worker unavailable: %v", err), http.StatusServiceUnavailable)
			return
		}

		// Return HTTP 307 Temporary Redirect to worker
		redirectURL := fmt.Sprintf("%s/kv/%d", workerAddr, key)
		w.Header().Set("Location", redirectURL)
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusTemporaryRedirect)

		json.NewEncoder(w).Encode(map[string]interface{}{
			"message":        "Request should be sent to worker node",
			"worker_id":      int(primaryWorker),
			"worker_address": workerAddr,
			"partition_id":   int(partitionID),
			"redirect_url":   redirectURL,
		})

		slog.Info("Redirecting DELETE request to worker",
			"key", key,
			"partition_id", partitionID,
			"worker_id", primaryWorker,
			"worker_addr", workerAddr)
		return
	}

	// FALLBACK: If no worker registry (backward compatibility), handle via Raft
	// This path will be deprecated in Stage 4
	if s.shardManager != nil {
		keyStr := fmt.Sprintf("%d", key)
		if err := s.shardManager.ValidateKey(keyStr); err != nil {
			if wrongNodeErr, ok := err.(*sharding.WrongNodeError); ok {
				// Return redirect response
				w.Header().Set("Location", wrongNodeErr.RedirectLocation())
				w.Header().Set("Content-Type", "application/json")
				w.WriteHeader(wrongNodeErr.HTTPStatusCode())
				json.NewEncoder(w).Encode(RedirectResponse{
					Error:       "MOVED",
					Message:     wrongNodeErr.Error(),
					NodeID:      fmt.Sprintf("%d", wrongNodeErr.CorrectNode),
					Address:     wrongNodeErr.CorrectAddr,
					PartitionID: uint16(wrongNodeErr.PartitionID),
				})
				return
			}
			// Other validation errors
			http.Error(w, fmt.Sprintf("Shard validation error: %v", err), http.StatusInternalServerError)
			return
		}

		// SYNCHRONOUS REPLICATION TO BACKUP (PRIMARY-BACKUP PATTERN)
		// After validating this is the primary, replicate to backup before Raft commit
		partitionID := s.shardManager.GetPartitionID(keyStr)
		_, backupNode, ok := s.shardManager.GetReplicas(partitionID)

		if ok && backupNode >= 0 && s.replicationClient != nil {
			replicationCtx, replicationCancel := context.WithTimeout(r.Context(), 1*time.Second)
			defer replicationCancel()

			err := s.replicationClient.DeleteReplicate(replicationCtx, backupNode, key)
			if err != nil {
				slog.Error(fmt.Sprintf("Replication to backup %d failed for DELETE key %d: %v", backupNode, key, err))
				// FAIL THE DELETE - Strong consistency guarantee
				http.Error(w, fmt.Sprintf("Replication failed: %v", err), http.StatusServiceUnavailable)
				return
			}

			slog.Info(fmt.Sprintf("Successfully replicated DELETE key=%d to backup node %d", key, backupNode))
		}
	}

	idempotencyToken := r.Header.Get("Idempotency-Key")
	if idempotencyToken == "" {
		h := sha256.New()
		h.Write([]byte(fmt.Sprintf("DELETE:%s:%d", getClientID(r), key)))
		idempotencyToken = hex.EncodeToString(h.Sum(nil))
	}

	if cachedResp, found := s.idempotencyCache.Get(idempotencyToken); found {
		slog.Info(fmt.Sprintf("Returning cached DELETE response for token: %s", idempotencyToken))
		resp := DeleteResponse{
			Success: cachedResp.Success,
			Message: "Key deleted successfully (cached)",
		}
		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("X-Cache", "HIT")
		json.NewEncoder(w).Encode(resp)
		return
	}

	var success bool
	var broadcastErr error

	retryFunc := func(ctx context.Context) error {
		msg := &raftpb.Message{
			Type:             raftpb.Message_DELETE,
			Key:              int32(key),
			Value:            nil,
			IdempotencyToken: idempotencyToken,
			ClientId:         getClientID(r),
		}

		resp, err := s.raft.Forward(ctx, msg)
		if err != nil {
			broadcastErr = err
			return err
		}

		success = resp.Success
		return nil
	}

	ctx, cancel := context.WithTimeout(r.Context(), 15*time.Second)
	defer cancel()

	err := s.retrier.ExecuteWithRetry(ctx, retryFunc)

	if err != nil {
		if ctx.Err() == context.DeadlineExceeded {
			http.Error(w, "Request timeout after retries", http.StatusGatewayTimeout)
			return
		}
		if broadcastErr != nil && strings.Contains(broadcastErr.Error(), "no leader") {
			http.Error(w, "No leader available", http.StatusServiceUnavailable)
			return
		}
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	s.idempotencyCache.Set(idempotencyToken, raft.BroadcastResponse{
		Success: success,
		Value:   0,
		Error:   nil,
	})

	resp := DeleteResponse{
		Success: success,
		Message: "Key deleted successfully",
	}

	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("X-Cache", "MISS")
	json.NewEncoder(w).Encode(resp)
}

func (s *Server) handleHealth(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(HealthResponse{
		Status: "healthy",
	})
}

func (s *Server) handleStatus(w http.ResponseWriter, r *http.Request) {
	status := s.raft.GetStatus()

	resp := StatusResponse{
		NodeID:     status.NodeID,
		Role:       status.Role,
		Term:       status.Term,
		LeaderID:   status.LeaderID,
		TotalNodes: status.TotalNodes,
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(resp)
}

func (s *Server) handleLeader(w http.ResponseWriter, r *http.Request) {
	status := s.raft.GetStatus()

	resp := LeaderResponse{
		IsLeader: s.raft.IsLeader(),
		LeaderID: status.LeaderID,
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(resp)
}

func (s *Server) generateIdempotencyToken(req *PutRequest, clientID string) string {
	h := sha256.New()
	h.Write([]byte(fmt.Sprintf("%s:%d:%d", clientID, req.Key, req.Value)))
	return hex.EncodeToString(h.Sum(nil))
}

func getClientID(r *http.Request) string {
	clientID := r.Header.Get("X-Client-ID")
	if clientID == "" {
		clientID = r.RemoteAddr
	}
	return clientID
}
