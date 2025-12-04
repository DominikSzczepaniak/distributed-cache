package main

import (
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"time"

	"github.com/dominikszczepaniak/distributed-cache/pkg/controller"
	"github.com/dominikszczepaniak/distributed-cache/pkg/metadata"
	"github.com/dominikszczepaniak/distributed-cache/pkg/raft"
)

// DummyApp implements raft.Application
type DummyApp struct{}

func (a *DummyApp) AppendMessage(message raft.Message) (bool, int) { return true, 0 }
func (a *DummyApp) GetSnapshot() ([]byte, error)                   { return []byte("{}"), nil }
func (a *DummyApp) RestoreFromSnapshot(data []byte) (error, int)   { return nil, 0 }
func (a *DummyApp) GetValue(key int) int                           { return 0 }

func main() {
	// Logger setup
	opts := &slog.HandlerOptions{
		Level: slog.LevelInfo,
	}
	logger := slog.New(slog.NewTextHandler(os.Stdout, opts))
	slog.SetDefault(logger)

	slog.Info("Starting Controller...")

	// Load Config
	cfg := raft.LoadConfig()

	// Initialize Application (Dummy for now, as Controller logic handles state separately)
	app := &DummyApp{}

	// Initialize Raft
	r := raft.NewRaft(app, cfg)

	// Initialize Controller
	// Grace period 5s to match lease duration in docker-compose
	ctrl := controller.NewController(r, 5*time.Second)

	// Start HTTP API
	apiAddr := os.Getenv("API_ADDR")
	if apiAddr == "" {
		apiAddr = ":8080"
	}

	mux := http.NewServeMux()

	// Heartbeat endpoint
	mux.HandleFunc("/cluster/heartbeat", func(w http.ResponseWriter, req *http.Request) {
		if req.Method != http.MethodPost {
			http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
			return
		}
		// Parse NodeID from request body or query param?
		// Simple implementation: Expect {"node_id": "..."}
		var body struct {
			NodeID string `json:"node_id"`
		}
		if err := json.NewDecoder(req.Body).Decode(&body); err != nil {
			http.Error(w, "Invalid request body", http.StatusBadRequest)
			return
		}

		ctrl.Heartbeat(body.NodeID)

		// Return current epoch
		config := ctrl.GetConfig()
		resp := map[string]interface{}{
			"epoch": config.Epoch,
		}
		json.NewEncoder(w).Encode(resp)
	})

	// Topology endpoint
	mux.HandleFunc("/topology", func(w http.ResponseWriter, req *http.Request) {
		config := ctrl.GetConfig()
		json.NewEncoder(w).Encode(config)
	})

	// Debug endpoint to set topology
	mux.HandleFunc("/debug/config", func(w http.ResponseWriter, req *http.Request) {
		if req.Method != http.MethodPost {
			http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
			return
		}
		var config metadata.ClusterConfig
		if err := json.NewDecoder(req.Body).Decode(&config); err != nil {
			http.Error(w, "Invalid request body", http.StatusBadRequest)
			return
		}
		ctrl.SetTopology(&config)
		w.WriteHeader(http.StatusOK)
	})

	slog.Info(fmt.Sprintf("Starting API server on %s", apiAddr))
	if err := http.ListenAndServe(apiAddr, mux); err != nil {
		slog.Error("Failed to start API server", "error", err)
		os.Exit(1)
	}
}
