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
	"google.golang.org/protobuf/types/known/wrapperspb"
)

// Server implements the HTTP API for the distributed cache.
// It coordinates requests with the Raft controller and handles
// client-side idempotency and retries.
type Server struct {
	raft             *raft.Raft
	listenAddr       string
	httpServer       *http.Server
	retrier          *Retrier
	idempotencyCache *IdempotencyCache
	leaderCache      *LeaderCache
}

// NewServer initializes an API server with the provided Raft node and listen address.
func NewServer(r *raft.Raft, listenAddr string) *Server {
	return &Server{
		raft:             r,
		listenAddr:       listenAddr,
		retrier:          NewRetrier(DefaultRetryConfigs["PUT"]),
		idempotencyCache: NewIdempotencyCache(5 * time.Minute),
		leaderCache:      NewLeaderCache(1 * time.Second),
	}
}

// Start begins the HTTP server and listens for incoming requests.
func (s *Server) Start() error {
	mux := http.NewServeMux()

	mux.HandleFunc("/kv", s.handleKV)
	mux.HandleFunc("/kv/", s.handleKVWithKey)

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

// Stop gracefully shuts down the HTTP server.
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

// handlePut processes a client's request to store a value.
// It generates an idempotency token based on the request content and uses the
// IdempotencyCache to ensure that duplicate requests (due to retries) are not
// reapplied to the Raft log. If the node is not the leader, the Retrier logic
// handles routing the request to the correct Controller replica.
func (s *Server) handlePut(w http.ResponseWriter, r *http.Request) {
	var req PutRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "Invalid request body", http.StatusBadRequest)
		return
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

// generateIdempotencyToken creates a unique SHA-256 hash for a PutRequest.
// This token is used to identify and deduplicate retried requests from clients.
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
