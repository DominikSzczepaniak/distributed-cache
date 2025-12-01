package worker

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/dominikszczepaniak/distributed-cache/pkg/sharding"
)

// APIServer provides HTTP API for client requests
type APIServer struct {
	store        *WorkerStore
	listenAddr   string
	httpServer   *http.Server
	shardManager *sharding.ShardManager
}

// NewAPIServer creates a new API server for the worker
func NewAPIServer(store *WorkerStore, listenAddr string, shardManager *sharding.ShardManager) *APIServer {
	return &APIServer{
		store:        store,
		listenAddr:   listenAddr,
		shardManager: shardManager,
	}
}

// Start starts the HTTP API server
func (s *APIServer) Start() error {
	mux := http.NewServeMux()

	mux.HandleFunc("/kv", s.handleKV)
	mux.HandleFunc("/kv/", s.handleKVWithKey)
	mux.HandleFunc("/health", s.handleHealth)
	mux.HandleFunc("/stats", s.handleStats)

	s.httpServer = &http.Server{
		Addr:         s.listenAddr,
		Handler:      mux,
		ReadTimeout:  10 * time.Second,
		WriteTimeout: 10 * time.Second,
	}

	slog.Info("Starting Worker HTTP API", "addr", s.listenAddr)
	return s.httpServer.ListenAndServe()
}

// Stop gracefully stops the HTTP API server
func (s *APIServer) Stop(ctx context.Context) error {
	if s.httpServer != nil {
		return s.httpServer.Shutdown(ctx)
	}
	return nil
}

// handleKV handles POST requests to /kv (PUT operations)
func (s *APIServer) handleKV(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodPost:
		s.handlePut(w, r)
	default:
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
	}
}

// handleKVWithKey handles GET and DELETE requests to /kv/{key}
func (s *APIServer) handleKVWithKey(w http.ResponseWriter, r *http.Request) {
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

// handlePut processes PUT requests
func (s *APIServer) handlePut(w http.ResponseWriter, r *http.Request) {
	var req struct {
		Key   int `json:"key"`
		Value int `json:"value"`
	}

	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "Invalid request body", http.StatusBadRequest)
		return
	}

	// STEP 1: Validate shard ownership (am I the owner?)
	if s.shardManager != nil {
		keyStr := fmt.Sprintf("%d", req.Key)
		if err := s.shardManager.ValidateKey(keyStr); err != nil {
			// Wrong worker → redirect
			if wrongNodeErr, ok := err.(*sharding.WrongNodeError); ok {
				w.Header().Set("Location", wrongNodeErr.CorrectAddr)
				w.WriteHeader(http.StatusTemporaryRedirect)
				json.NewEncoder(w).Encode(map[string]interface{}{
					"error":       "WRONG_NODE",
					"message":     "Not owner for this key",
					"redirect_to": wrongNodeErr.CorrectAddr,
				})
				return
			}
			// Other error (partition not assigned, etc.)
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
	}

	// STEP 2: Write to local storage (NO Raft consensus)
	// NOTE: In Stage 1, we skip replication. Stage 2+ will add backup replication
	if err := s.store.Put(req.Key, req.Value); err != nil {
		http.Error(w, "Storage error", http.StatusInternalServerError)
		return
	}

	// STEP 3: Return success
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(map[string]interface{}{
		"success": true,
		"message": "Key-value pair stored",
	})
}

// handleGet processes GET requests
func (s *APIServer) handleGet(w http.ResponseWriter, r *http.Request, key int) {
	// Validate shard ownership
	if s.shardManager != nil {
		keyStr := fmt.Sprintf("%d", key)
		if err := s.shardManager.ValidateKey(keyStr); err != nil {
			// Wrong worker → redirect
			if wrongNodeErr, ok := err.(*sharding.WrongNodeError); ok {
				w.Header().Set("Location", wrongNodeErr.CorrectAddr)
				w.WriteHeader(http.StatusTemporaryRedirect)
				json.NewEncoder(w).Encode(map[string]interface{}{
					"error":       "WRONG_NODE",
					"redirect_to": wrongNodeErr.CorrectAddr,
				})
				return
			}
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
	}

	// Read from local storage
	value, exists := s.store.Get(key)
	if !exists {
		http.Error(w, "Key not found", http.StatusNotFound)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(map[string]interface{}{
		"key":   key,
		"value": value,
	})
}

// handleDelete processes DELETE requests
func (s *APIServer) handleDelete(w http.ResponseWriter, r *http.Request, key int) {
	// Validate shard ownership
	if s.shardManager != nil {
		keyStr := fmt.Sprintf("%d", key)
		if err := s.shardManager.ValidateKey(keyStr); err != nil {
			// Wrong worker → redirect
			if wrongNodeErr, ok := err.(*sharding.WrongNodeError); ok {
				w.Header().Set("Location", wrongNodeErr.CorrectAddr)
				w.WriteHeader(http.StatusTemporaryRedirect)
				return
			}
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
	}

	// Delete from local storage
	if err := s.store.Delete(key); err != nil {
		http.Error(w, "Storage error", http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusNoContent)
}

// handleHealth returns health status
func (s *APIServer) handleHealth(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(map[string]interface{}{
		"status": "healthy",
	})
}

// handleStats returns storage statistics
func (s *APIServer) handleStats(w http.ResponseWriter, r *http.Request) {
	stats := s.store.GetStats()
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(stats)
}
