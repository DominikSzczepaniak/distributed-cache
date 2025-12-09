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

func main() {
	opts := &slog.HandlerOptions{
		Level: slog.LevelInfo,
	}
	logger := slog.New(slog.NewTextHandler(os.Stdout, opts))
	slog.SetDefault(logger)

	slog.Info("Starting Controller...")

	cfg := raft.LoadConfig()
	app := controller.NewControllerApp()
	r := raft.NewRaft(app, cfg)

	// Grace period 5s to match lease duration in docker-compose
	ctrl := controller.NewController(r, 5*time.Second)

	app.SetController(ctrl)
	apiAddr := os.Getenv("API_ADDR")
	if apiAddr == "" {
		apiAddr = ":8080"
	}

	mux := http.NewServeMux()

	mux.HandleFunc("/cluster/heartbeat", func(w http.ResponseWriter, req *http.Request) {
		if req.Method != http.MethodPost {
			http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
			return
		}
		var body struct {
			NodeID string `json:"node_id"`
		}
		if err := json.NewDecoder(req.Body).Decode(&body); err != nil {
			http.Error(w, "Invalid request body", http.StatusBadRequest)
			return
		}

		ctrl.Heartbeat(body.NodeID)

		config := ctrl.GetConfig()
		resp := map[string]interface{}{
			"epoch": config.Epoch,
		}
		json.NewEncoder(w).Encode(resp)
	})

	mux.HandleFunc("/cluster/register", func(w http.ResponseWriter, req *http.Request) {
		if req.Method != http.MethodPost {
			http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
			return
		}
		var body struct {
			NodeID  string `json:"node_id"`
			Address string `json:"address"`
		}
		if err := json.NewDecoder(req.Body).Decode(&body); err != nil {
			http.Error(w, "Invalid request body", http.StatusBadRequest)
			return
		}
		if body.NodeID == "" || body.Address == "" {
			http.Error(w, "Missing node_id or address", http.StatusBadRequest)
			return
		}

		if err := ctrl.RegisterNode(body.NodeID, body.Address); err != nil {
			slog.Error("Failed to register node", "error", err)
			http.Error(w, "Failed to register node: "+err.Error(), http.StatusInternalServerError)
			return
		}
		w.WriteHeader(http.StatusOK)
	})

	mux.HandleFunc("/topology", func(w http.ResponseWriter, req *http.Request) {
		config := ctrl.GetConfig()
		json.NewEncoder(w).Encode(config)
	})

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
