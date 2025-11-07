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
)

// Server provides HTTP API for the Raft cluster
type Server struct {
	raft             *raft.Raft
	listenAddr       string
	httpServer       *http.Server
	retrier          *Retrier
	idempotencyCache *IdempotencyCache
	leaderCache      *LeaderCache
}

// NewServer creates a new API server
func NewServer(r *raft.Raft, listenAddr string) *Server {
	return &Server{
		raft:             r,
		listenAddr:       listenAddr,
		retrier:          NewRetrier(DefaultRetryConfigs["PUT"]),
		idempotencyCache: NewIdempotencyCache(5 * time.Minute),
		leaderCache:      NewLeaderCache(1 * time.Second),
	}
}

// Start starts the HTTP server
func (s *Server) Start() error {
	mux := http.NewServeMux()

	// KV endpoints
	mux.HandleFunc("/kv", s.handleKV)
	mux.HandleFunc("/kv/", s.handleKVWithKey)

	// Status endpoints
	mux.HandleFunc("/health", s.handleHealth)
	mux.HandleFunc("/status", s.handleStatus)
	mux.HandleFunc("/leader", s.handleLeader)

	s.httpServer = &http.Server{
		Addr:    s.listenAddr,
		Handler: mux,
	}

	slog.Info(fmt.Sprintf("Starting HTTP API server on %s", s.listenAddr))
	return s.httpServer.ListenAndServe()
}

// Stop stops the HTTP server gracefully
func (s *Server) Stop() error {
	if s.httpServer != nil {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		return s.httpServer.Shutdown(ctx)
	}
	return nil
}

// handleKV routes POST requests to handlePut
func (s *Server) handleKV(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodPost:
		s.handlePut(w, r)
	default:
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
	}
}

// handleKVWithKey routes GET/DELETE requests to respective handlers
func (s *Server) handleKVWithKey(w http.ResponseWriter, r *http.Request) {
	// Extract key from URL path
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

// handlePut handles PUT operations with retry and idempotency
func (s *Server) handlePut(w http.ResponseWriter, r *http.Request) {
	var req PutRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "Invalid request body", http.StatusBadRequest)
		return
	}

	// Get or generate idempotency token
	idempotencyToken := r.Header.Get("Idempotency-Key")
	if idempotencyToken == "" {
		// Generate from request content
		idempotencyToken = s.generateIdempotencyToken(&req, getClientID(r))
	}

	// Check idempotency cache
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

	// Execute with retry
	var success bool
	var broadcastErr error

	retryFunc := func(ctx context.Context) error {
		msg := raft.Message{
			MsgType:          "PUT",
			Key:              req.Key,
			Value:            &req.Value,
			IdempotencyToken: idempotencyToken,
			ClientID:         getClientID(r),
		}

		var err error
		success, _, err = s.raft.BroadcastSync(msg, 5*time.Second)
		broadcastErr = err
		return err
	}

	ctx, cancel := context.WithTimeout(r.Context(), 15*time.Second)
	defer cancel()

	err := s.retrier.ExecuteWithRetry(ctx, retryFunc)

	if err != nil {
		// Check specific error types
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

	// Cache successful response
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

// handleGet handles GET operations with optional stale reads
func (s *Server) handleGet(w http.ResponseWriter, r *http.Request, key int) {
	// Check if stale reads allowed
	stale := r.URL.Query().Get("stale") == "true"

	var value int
	var found bool

	if stale {
		// Read directly from local application (may be stale)
		value = s.raft.GetApplication().GetValue(key)
		found = true // Simplification - assume exists
	} else {
		// Linearizable read: verify leadership first
		if !s.raft.IsLeader() {
			// Check leader cache
			leaderID, leaderAddr, cached := s.leaderCache.Get()
			if !cached {
				// Query current leader
				status := s.raft.GetStatus()
				leaderID = status.LeaderID
				// TODO: Map leaderID to address (would need address mapping)
				// For now, return error
				http.Error(w, fmt.Sprintf("Not leader. Leader is node %d", leaderID), http.StatusServiceUnavailable)
				return
			}

			// Redirect to leader
			if leaderAddr != "" {
				http.Redirect(w, r, fmt.Sprintf("http://%s/kv/%d", leaderAddr, key), http.StatusTemporaryRedirect)
				return
			}

			http.Error(w, "No leader available", http.StatusServiceUnavailable)
			return
		}

		// Leader serves read
		value = s.raft.GetApplication().GetValue(key)
		found = true
	}

	resp := GetResponse{
		Key:   key,
		Value: value,
		Found: found,
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(resp)
}

// handleDelete handles DELETE operations with retry and idempotency
func (s *Server) handleDelete(w http.ResponseWriter, r *http.Request, key int) {
	// Get or generate idempotency token
	idempotencyToken := r.Header.Get("Idempotency-Key")
	if idempotencyToken == "" {
		// Generate from request content
		h := sha256.New()
		h.Write([]byte(fmt.Sprintf("DELETE:%s:%d", getClientID(r), key)))
		idempotencyToken = hex.EncodeToString(h.Sum(nil))
	}

	// Check idempotency cache
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

	// Execute with retry
	var success bool
	var broadcastErr error

	retryFunc := func(ctx context.Context) error {
		msg := raft.Message{
			MsgType:          "DELETE",
			Key:              key,
			Value:            nil,
			IdempotencyToken: idempotencyToken,
			ClientID:         getClientID(r),
		}

		var err error
		success, _, err = s.raft.BroadcastSync(msg, 5*time.Second)
		broadcastErr = err
		return err
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

	// Cache successful response
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

// handleHealth returns simple health check
func (s *Server) handleHealth(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(HealthResponse{
		Status: "healthy",
	})
}

// handleStatus returns cluster status
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

// handleLeader returns leader information
func (s *Server) handleLeader(w http.ResponseWriter, r *http.Request) {
	status := s.raft.GetStatus()

	resp := LeaderResponse{
		IsLeader: s.raft.IsLeader(),
		LeaderID: status.LeaderID,
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(resp)
}

// Helper functions

func (s *Server) generateIdempotencyToken(req *PutRequest, clientID string) string {
	h := sha256.New()
	h.Write([]byte(fmt.Sprintf("%s:%d:%d", clientID, req.Key, req.Value)))
	return hex.EncodeToString(h.Sum(nil))
}

func getClientID(r *http.Request) string {
	// Use IP address or client-provided ID
	clientID := r.Header.Get("X-Client-ID")
	if clientID == "" {
		clientID = r.RemoteAddr
	}
	return clientID
}
