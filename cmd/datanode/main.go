package main

import (
	"bytes"
	"flag"
	"fmt"
	"log"
	"net"
	"net/http"
	"os"
	"time"

	"github.com/dominikszczepaniak/distributed-cache/pkg/cache"
	"github.com/dominikszczepaniak/distributed-cache/pkg/datanode"
)

type DataNodeConfig struct {
	ControllerURL string
	NodeID        string
	LeaseDuration time.Duration
}

func main() {
	config := parseConfig()

	if config.ControllerURL == "" {
		log.Fatal("Error: CONTROLLER_URL is required")
	}
	if config.NodeID == "" {
		log.Fatal("Error: NODE_ID is required")
	}

	log.Printf("Starting DataNode with Config: %+v", config)

	stateMgr := datanode.NewStateManager()

	leaseMgr := datanode.NewLeaseManager(config.ControllerURL, config.NodeID, config.LeaseDuration, stateMgr)
	leaseMgr.Start()

	c := cache.NewConcurrentMapCache()
	srv := datanode.NewServer(c, leaseMgr, stateMgr, config.NodeID)

	// NodeID contains the port (e.g., "10.0.1.5:9000") - we listen on it

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
		} else if r.Method == http.MethodDelete {
			srv.HandleDelete(w, r)
		} else {
			http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		}
	})
	mux.HandleFunc("/internal/replicate", func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodPost {
			srv.HandleReplicate(w, r)
		} else {
			http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		}
	})
	mux.HandleFunc("/internal/replicate-delete", func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodPost {
			srv.HandleDeleteReplicate(w, r)
		} else {
			http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		}
	})

	mux.HandleFunc("/internal/export", func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodGet {
			srv.HandleExport(w, r)
		} else {
			http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		}
	})
	mux.HandleFunc("/internal/import", func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodPost {
			srv.HandleImport(w, r)
		} else {
			http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		}
	})

	mux.HandleFunc("/internal/pull", func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodPost {
			srv.HandlePull(w, r)
		} else {
			http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		}
	})

	_, port, err := net.SplitHostPort(config.NodeID)
	if err != nil {
		port = "9000"
	}

	server := &http.Server{
		Addr:    ":" + port,
		Handler: mux,
	}

	go func() {
		time.Sleep(1 * time.Second)
		registerWithController(config)
	}()

	log.Printf("DataNode listening on %s", config.NodeID)
	if err := server.ListenAndServe(); err != nil {
		log.Fatalf("DataNode failed: %v", err)
	}
}

func registerWithController(config DataNodeConfig) {
	for {
		body := fmt.Sprintf(`{"node_id": "%s", "address": "%s"}`, config.NodeID, config.NodeID)
		resp, err := http.Post(config.ControllerURL+"/cluster/register", "application/json", bytes.NewBuffer([]byte(body)))
		if err == nil && resp.StatusCode == http.StatusOK {
			log.Printf("Successfully registered with Controller at %s", config.ControllerURL)
			resp.Body.Close()
			return
		}

		if err != nil {
			log.Printf("Failed to register: %v", err)
		} else {
			log.Printf("Failed to register: status %d", resp.StatusCode)
			resp.Body.Close()
		}

		time.Sleep(2 * time.Second)
	}
}

func parseConfig() DataNodeConfig {
	var config DataNodeConfig

	flag.StringVar(&config.ControllerURL, "controller", "", "URL of the Controller (Raft Leader)")
	flag.StringVar(&config.NodeID, "node-id", "", "Unique ID for this node (e.g., ip:port)")
	flag.DurationVar(&config.LeaseDuration, "lease", 5*time.Second, "Lease duration")
	flag.Parse()

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
