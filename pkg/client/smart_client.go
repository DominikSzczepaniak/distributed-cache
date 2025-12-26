package client

import (
	"bytes"
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io"
	"log/slog"
	"net/http"
	"sync"
	"time"

	"github.com/dominikszczepaniak/distributed-cache/pkg/metadata"
)

type SmartClient struct {
	mu          sync.RWMutex
	config      *metadata.ClusterConfig
	controllers []string
	httpClient  *http.Client
	maxRetries  int
}

// NewSmartClient initializes a client that can automatically route requests to the
// correct DataNode by tracking the cluster topology.
func NewSmartClient(controllers []string) (*SmartClient, error) {
	if len(controllers) == 0 {
		return nil, fmt.Errorf("at least one controller URL is required")
	}

	c := &SmartClient{
		controllers: controllers,
		httpClient: &http.Client{
			Timeout: 5 * time.Second,
		},
		maxRetries: 5,
	}

	if err := c.FetchTopology(); err != nil {
		return nil, fmt.Errorf("failed to fetch initial topology: %w", err)
	}

	return c, nil
}

// FetchTopology retrieves the latest cluster configuration from one of the controllers.
func (c *SmartClient) FetchTopology() error {
	var lastErr error

	for _, controller := range c.controllers {
		resp, err := c.httpClient.Get(controller + "/topology")
		if err != nil {
			lastErr = err
			continue
		}
		defer resp.Body.Close()

		if resp.StatusCode != http.StatusOK {
			lastErr = fmt.Errorf("controller returned status %d", resp.StatusCode)
			continue
		}

		var config metadata.ClusterConfig
		if err := json.NewDecoder(resp.Body).Decode(&config); err != nil {
			lastErr = fmt.Errorf("failed to decode topology: %w", err)
			continue
		}

		c.mu.Lock()
		c.config = &config
		c.mu.Unlock()

		slog.Info("Client updated topology", "epoch", config.Epoch)
		return nil
	}

	return fmt.Errorf("failed to fetch topology from any controller: %w", lastErr)
}

// GetConfig returns the current cluster configuration held by the client.
func (c *SmartClient) GetConfig() *metadata.ClusterConfig {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.config
}

// GetEpoch returns the version (epoch) of the current cluster configuration.
func (c *SmartClient) GetEpoch() uint64 {
	c.mu.RLock()
	defer c.mu.RUnlock()
	if c.config == nil {
		return 0
	}
	return c.config.Epoch
}

type RouteResult struct {
	TargetURL string
	Epoch     uint64
	ShardID   int
}

// Route determines which DataNode is responsible for a given key.
// it uses the FNV-1a hashing algorithm to map the key to a shard, and then
// looks up the current primary for that shard in the topology config.
func (c *SmartClient) Route(key string) (*RouteResult, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	if c.config == nil {
		return nil, fmt.Errorf("no topology available")
	}

	if c.config.TotalShards == 0 {
		return nil, fmt.Errorf("cluster has no shards configured")
	}

	h := fnv.New32a()
	h.Write([]byte(key))
	shardID := int(h.Sum32()) % c.config.TotalShards
	if shardID < 0 {
		shardID = -shardID
	}

	shard, exists := c.config.Shards[shardID]
	if !exists {
		return nil, fmt.Errorf("shard %d not found in config", shardID)
	}

	node, exists := c.config.Nodes[shard.PrimaryID]
	if !exists {
		return nil, fmt.Errorf("primary node %s not found in config", shard.PrimaryID)
	}

	return &RouteResult{
		TargetURL: "http://" + node.Address,
		Epoch:     c.config.Epoch,
		ShardID:   shardID,
	}, nil
}

type writeRequest struct {
	Key   string `json:"key"`
	Value string `json:"value"`
	Epoch uint64 `json:"epoch"`
}

// Put stores a value in the distributed cache.
// It automatically routes the request to the correct primary DataNode.
// If the node is not the primary, the shard is locked (migration), or the Epoch is stale,
// the client will refresh the topology and retry the operation.
func (c *SmartClient) Put(key, value string) error {
	for attempt := 0; attempt < c.maxRetries; attempt++ {
		route, err := c.Route(key)
		if err != nil {
			c.FetchTopology()
			continue
		}

		reqBody := writeRequest{Key: key, Value: value, Epoch: route.Epoch}
		body, _ := json.Marshal(reqBody)

		resp, err := c.httpClient.Post(route.TargetURL+"/data", "application/json", bytes.NewBuffer(body))
		if err != nil {
			slog.Warn("Put failed, refreshing topology", "attempt", attempt, "err", err)
			c.FetchTopology()
			continue
		}
		resp.Body.Close()

		switch resp.StatusCode {
		case http.StatusOK:
			return nil

		case http.StatusBadRequest:
			slog.Info("Not primary for shard, refreshing topology", "attempt", attempt)
			c.FetchTopology()
			continue

		case http.StatusPreconditionFailed:
			slog.Info("Epoch stale, refreshing topology", "attempt", attempt)
			c.FetchTopology()
			continue

		case http.StatusLocked:
			slog.Info("Shard locked (migrating), waiting...", "attempt", attempt)
			time.Sleep(100 * time.Millisecond)
			c.FetchTopology()
			continue

		case http.StatusServiceUnavailable:
			slog.Info("Node fenced, waiting for failover", "attempt", attempt)
			time.Sleep(200 * time.Millisecond)
			c.FetchTopology()
			continue

		case http.StatusInternalServerError:
			slog.Warn("Replication failed, retrying", "attempt", attempt)
			time.Sleep(100 * time.Millisecond)
			continue

		default:
			slog.Warn("Put received unexpected status", "status", resp.StatusCode, "attempt", attempt)
			return fmt.Errorf("unexpected error: status %d", resp.StatusCode)
		}
	}
	return fmt.Errorf("max retries exceeded for Put(%s)", key)
}

// Get retrieves a value from the distributed cache.
// It hashes the key to find the responsible node and performs an HTTP GET.
// Like Put, it handles retries and topology refreshes if the cluster state changes.
func (c *SmartClient) Get(key string) (string, error) {
	for attempt := 0; attempt < c.maxRetries; attempt++ {
		route, err := c.Route(key)
		if err != nil {
			c.FetchTopology()
			continue
		}

		resp, err := c.httpClient.Get(route.TargetURL + "/data?key=" + key)
		if err != nil {
			slog.Warn("Get failed, refreshing topology", "attempt", attempt, "err", err)
			c.FetchTopology()
			continue
		}

		body, _ := io.ReadAll(resp.Body)
		resp.Body.Close()

		switch resp.StatusCode {
		case http.StatusOK:
			return string(body), nil

		case http.StatusNotFound:
			return "", fmt.Errorf("key not found: %s", key)

		case http.StatusLocked:
			slog.Info("Shard locked (migrating), waiting...", "attempt", attempt)
			time.Sleep(100 * time.Millisecond)
			c.FetchTopology()
			continue

		case http.StatusServiceUnavailable:
			slog.Info("Node fenced, waiting for failover", "attempt", attempt)
			time.Sleep(200 * time.Millisecond)
			continue

		default:
			return "", fmt.Errorf("unexpected error: status %d", resp.StatusCode)
		}
	}
	return "", fmt.Errorf("max retries exceeded for Get(%s)", key)
}

type deleteRequest struct {
	Key   string `json:"key"`
	Epoch uint64 `json:"epoch"`
}

// Delete removes a key-value pair from the distributed cache.
// It uses the same routing and retry logic as Put and Get to ensure consistency
// even during shard migrations or node failures.
func (c *SmartClient) Delete(key string) error {
	for attempt := 0; attempt < c.maxRetries; attempt++ {
		route, err := c.Route(key)
		if err != nil {
			c.FetchTopology()
			continue
		}

		reqBody := deleteRequest{Key: key, Epoch: route.Epoch}
		body, _ := json.Marshal(reqBody)

		req, err := http.NewRequest(http.MethodDelete, route.TargetURL+"/data", bytes.NewBuffer(body))
		if err != nil {
			return fmt.Errorf("failed to create request: %w", err)
		}
		req.Header.Set("Content-Type", "application/json")

		resp, err := c.httpClient.Do(req)
		if err != nil {
			slog.Warn("Delete failed, refreshing topology", "attempt", attempt, "err", err)
			c.FetchTopology()
			continue
		}
		resp.Body.Close()

		switch resp.StatusCode {
		case http.StatusOK:
			return nil

		case http.StatusPreconditionFailed:
			slog.Info("Epoch stale, refreshing topology", "attempt", attempt)
			c.FetchTopology()
			continue

		case http.StatusLocked:
			slog.Info("Shard locked (migrating), waiting...", "attempt", attempt)
			time.Sleep(100 * time.Millisecond)
			c.FetchTopology()
			continue

		case http.StatusServiceUnavailable:
			slog.Info("Node fenced, waiting for failover", "attempt", attempt)
			time.Sleep(200 * time.Millisecond)
			continue

		case http.StatusInternalServerError:
			slog.Warn("Delete replication failed, retrying", "attempt", attempt)
			time.Sleep(100 * time.Millisecond)
			continue

		default:
			return fmt.Errorf("unexpected error: status %d", resp.StatusCode)
		}
	}
	return fmt.Errorf("max retries exceeded for Delete(%s)", key)
}
