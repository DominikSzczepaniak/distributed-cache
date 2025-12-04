package datanode

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"sync"
	"time"

	"github.com/dominikszczepaniak/distributed-cache/pkg/metadata"
)

// LeaseManager handles the "License to Write" for a DataNode
type LeaseManager struct {
	mu            sync.RWMutex
	validUntil    time.Time
	duration      time.Duration
	controllerURL string
	nodeID        string
	client        *http.Client
	stateMgr      *StateManager
}

// NewLeaseManager creates a new LeaseManager
func NewLeaseManager(controllerURL, nodeID string, duration time.Duration, stateMgr *StateManager) *LeaseManager {
	return &LeaseManager{
		duration:      duration,
		controllerURL: controllerURL,
		nodeID:        nodeID,
		client:        &http.Client{Timeout: 2 * time.Second},
		stateMgr:      stateMgr,
	}
}

// Start runs the background renewal loop
func (l *LeaseManager) Start() {
	// Renew immediately on start
	if l.renew() {
		l.extendLease()
	}

	ticker := time.NewTicker(l.duration / 3)
	go func() {
		for range ticker.C {
			if l.renew() {
				l.extendLease()
			} else {
				log.Printf("Lease renewal failed for node %s", l.nodeID)
				// Do NOT update validUntil. It will naturally expire.
			}
		}
	}()
}

func (l *LeaseManager) extendLease() {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.validUntil = time.Now().Add(l.duration)
	log.Printf("Lease renewed. Valid until: %s", l.validUntil.Format(time.RFC3339))
}

// renew sends a heartbeat to the Controller
func (l *LeaseManager) renew() bool {
	// Payload for heartbeat - simple map for now as per prompt, or we could use a struct
	payload := map[string]string{"node_id": l.nodeID}
	data, err := json.Marshal(payload)
	if err != nil {
		log.Printf("Error marshaling heartbeat payload: %v", err)
		return false
	}

	url := fmt.Sprintf("%s/cluster/heartbeat", l.controllerURL)
	resp, err := l.client.Post(url, "application/json", bytes.NewBuffer(data))
	if err != nil {
		log.Printf("Heartbeat request failed: %v", err)
		return false
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		log.Printf("Heartbeat returned status: %d", resp.StatusCode)
		return false
	}

	// Parse response for Epoch
	var response struct {
		Epoch uint64 `json:"epoch"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&response); err != nil {
		log.Printf("Failed to decode heartbeat response: %v", err)
		// We still consider this a success for lease renewal purposes,
		// but we won't be able to check epoch.
		return true
	}

	// Check Epoch
	localEpoch := l.stateMgr.GetEpoch()
	if response.Epoch > localEpoch {
		log.Printf("Local Epoch %d < Remote Epoch %d. Fetching topology...", localEpoch, response.Epoch)
		l.fetchTopology()
	}

	return true
}

func (l *LeaseManager) fetchTopology() {
	url := fmt.Sprintf("%s/topology", l.controllerURL)
	resp, err := l.client.Get(url)
	if err != nil {
		log.Printf("Failed to fetch topology: %v", err)
		return
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		log.Printf("Topology fetch returned status: %d", resp.StatusCode)
		return
	}

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		log.Printf("Failed to read topology body: %v", err)
		return
	}

	var config metadata.ClusterConfig
	if err := json.Unmarshal(body, &config); err != nil {
		log.Printf("Failed to unmarshal topology: %v", err)
		return
	}

	l.stateMgr.Update(&config)
	log.Printf("Updated topology to Epoch %d", config.Epoch)
}

// IsActive checks if the current lease is valid
func (l *LeaseManager) IsActive() bool {
	l.mu.RLock()
	defer l.mu.RUnlock()
	return time.Now().Before(l.validUntil)
}
