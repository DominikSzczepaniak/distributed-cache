package main

import (
	"flag"
	"log"
	"net/http"
	"os"
	"time"

	"github.com/dominikszczepaniak/distributed-cache/pkg/cache"
	"github.com/dominikszczepaniak/distributed-cache/pkg/datanode"
)

// DataNodeConfig holds the configuration for the DataNode
type DataNodeConfig struct {
	ControllerURL string
	NodeID        string
	LeaseDuration time.Duration
}

func main() {
	// 1. Parse Configuration
	config := parseConfig()

	// 2. Validate Configuration
	if config.ControllerURL == "" {
		log.Fatal("Error: CONTROLLER_URL is required")
	}
	if config.NodeID == "" {
		log.Fatal("Error: NODE_ID is required")
	}

	log.Printf("Starting DataNode with Config: %+v", config)

	// 3. Initialize State Manager
	stateMgr := datanode.NewStateManager()

	// 4. Start Lease Manager
	leaseMgr := datanode.NewLeaseManager(config.ControllerURL, config.NodeID, config.LeaseDuration, stateMgr)
	leaseMgr.Start()

	// 5. Initialize Cache and Server
	c := cache.NewConcurrentMapCache()
	srv := datanode.NewServer(c, leaseMgr, stateMgr, config.NodeID)

	// 6. Start HTTP Server
	// We assume NodeID contains the port (e.g., "10.0.1.5:9000") or we just listen on the port part.
	// For simplicity, if NodeID is just an address, we listen on it.
	// In a real scenario, we might want to separate BindAddress from NodeID (public address).
	// But for this stage, we'll use NodeID as the bind address if it looks like one,
	// or just take a separate bind flag if needed.
	// The prompt examples "10.0.1.5:9000" suggest NodeID is the address.

	mux := http.NewServeMux()
	mux.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("OK"))
	})
	mux.HandleFunc("/data", func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodPost || r.Method == http.MethodPut {
			srv.HandlePut(w, r)
		} else if r.Method == http.MethodGet {
			srv.HandleGet(w, r)
		} else {
			http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		}
	})

	server := &http.Server{
		Addr:    config.NodeID, // Using NodeID as bind address for now
		Handler: mux,
	}

	log.Printf("DataNode listening on %s", config.NodeID)
	if err := server.ListenAndServe(); err != nil {
		log.Fatalf("DataNode failed: %v", err)
	}
}

func parseConfig() DataNodeConfig {
	var config DataNodeConfig

	// Flags
	flag.StringVar(&config.ControllerURL, "controller", "", "URL of the Controller (Raft Leader)")
	flag.StringVar(&config.NodeID, "node-id", "", "Unique ID for this node (e.g., ip:port)")
	flag.DurationVar(&config.LeaseDuration, "lease", 5*time.Second, "Lease duration")
	flag.Parse()

	// Environment Variables (override flags if set, or serve as defaults)
	if url := os.Getenv("CONTROLLER_URL"); url != "" {
		config.ControllerURL = url
	}
	if id := os.Getenv("NODE_ID"); id != "" {
		config.NodeID = id
	}
	if lease := os.Getenv("LEASE_DURATION"); lease != "" {
		if d, err := time.ParseDuration(lease); err == nil {
			config.LeaseDuration = d
		}
	}

	return config
}
