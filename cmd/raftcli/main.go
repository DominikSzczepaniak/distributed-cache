package main

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/dominikszczepaniak/distributed-cache/pkg/api"
	"github.com/google/uuid"
)

// CLI provides an interactive command-line interface for the Raft cluster
type CLI struct {
	client       *http.Client
	baseURL      string
	retrier      *api.Retrier
	maxRedirects int
}

// NewCLI creates a new CLI instance
func NewCLI(baseURL string) *CLI {
	return &CLI{
		client: &http.Client{
			Timeout: 10 * time.Second,
			CheckRedirect: func(req *http.Request, via []*http.Request) error {
				// Don't auto-follow redirects, handle manually
				return http.ErrUseLastResponse
			},
		},
		baseURL:      baseURL,
		retrier:      api.NewRetrier(api.DefaultRetryConfigs["PUT"]),
		maxRedirects: 3,
	}
}

func main() {
	if len(os.Args) < 2 {
		fmt.Println("Usage: raftcli <node-address>")
		fmt.Println("Example: raftcli localhost:8080")
		os.Exit(1)
	}

	baseURL := os.Args[1]
	if !strings.HasPrefix(baseURL, "http://") && !strings.HasPrefix(baseURL, "https://") {
		baseURL = "http://" + baseURL
	}

	cli := NewCLI(baseURL)

	fmt.Println("==========================================")
	fmt.Println("  Raft Distributed Cache - Interactive CLI")
	fmt.Println("==========================================")
	fmt.Printf("Connected to: %s\n", baseURL)
	fmt.Println("Type 'help' for available commands")
	fmt.Println()

	cli.Run()
}

// Run starts the interactive CLI loop
func (c *CLI) Run() {
	scanner := bufio.NewScanner(os.Stdin)

	for {
		fmt.Print("> ")

		if !scanner.Scan() {
			break
		}

		line := strings.TrimSpace(scanner.Text())
		if line == "" {
			continue
		}

		if err := c.executeCommand(line); err != nil {
			fmt.Printf("Error: %v\n", err)
		}
	}
}

// executeCommand parses and executes a command
func (c *CLI) executeCommand(line string) error {
	parts := strings.Fields(line)
	if len(parts) == 0 {
		return nil
	}

	cmd := strings.ToLower(parts[0])
	args := parts[1:]

	switch cmd {
	case "put":
		return c.cmdPut(args)
	case "get":
		return c.cmdGet(args)
	case "delete", "del":
		return c.cmdDelete(args)
	case "status":
		return c.cmdStatus(args)
	case "leader":
		return c.cmdLeader(args)
	case "health":
		return c.cmdHealth(args)
	case "help":
		c.cmdHelp()
		return nil
	case "exit", "quit":
		fmt.Println("Goodbye!")
		os.Exit(0)
	default:
		return fmt.Errorf("unknown command: %s (type 'help' for available commands)", cmd)
	}

	return nil
}

// cmdPut handles PUT operations
func (c *CLI) cmdPut(args []string) error {
	if len(args) != 2 {
		return fmt.Errorf("usage: put <key> <value>")
	}

	key, err := strconv.Atoi(args[0])
	if err != nil {
		return fmt.Errorf("invalid key: %v", err)
	}

	value, err := strconv.Atoi(args[1])
	if err != nil {
		return fmt.Errorf("invalid value: %v", err)
	}

	// Generate idempotency token
	idempotencyToken := uuid.New().String()

	// Execute with retry
	retryFunc := func(ctx context.Context) error {
		req := map[string]int{"key": key, "value": value}
		body, _ := json.Marshal(req)

		httpReq, _ := http.NewRequestWithContext(ctx, "POST", c.baseURL+"/kv", bytes.NewBuffer(body))
		httpReq.Header.Set("Content-Type", "application/json")
		httpReq.Header.Set("Idempotency-Key", idempotencyToken)

		resp, err := c.client.Do(httpReq)
		if err != nil {
			return err
		}
		defer resp.Body.Close()

		// Handle redirects to leader
		if resp.StatusCode == http.StatusTemporaryRedirect {
			location := resp.Header.Get("Location")
			if location != "" {
				// Update baseURL and retry
				lastSlash := strings.LastIndex(location, "/")
				if lastSlash > 0 {
					c.baseURL = location[:lastSlash]
				}
				return fmt.Errorf("redirecting to leader") // Trigger retry
			}
		}

		if resp.StatusCode != http.StatusOK {
			bodyBytes, _ := io.ReadAll(resp.Body)
			return fmt.Errorf("server error (status %d): %s", resp.StatusCode, string(bodyBytes))
		}

		// Check for cache hit
		cacheStatus := resp.Header.Get("X-Cache")
		if cacheStatus == "HIT" {
			fmt.Printf("  (idempotent retry detected - returned cached result)\n")
		}

		return nil
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	err = c.retrier.ExecuteWithRetry(ctx, retryFunc)
	if err != nil {
		return fmt.Errorf("PUT failed after retries: %v", err)
	}

	fmt.Printf("✓ PUT successful: key=%d, value=%d\n", key, value)
	return nil
}

// cmdGet handles GET operations
func (c *CLI) cmdGet(args []string) error {
	if len(args) < 1 {
		return fmt.Errorf("usage: get <key> [--stale]")
	}

	key, err := strconv.Atoi(args[0])
	if err != nil {
		return fmt.Errorf("invalid key: %v", err)
	}

	// Check for --stale flag
	stale := false
	if len(args) > 1 && args[1] == "--stale" {
		stale = true
	}

	url := fmt.Sprintf("%s/kv/%d", c.baseURL, key)
	if stale {
		url += "?stale=true"
	}

	resp, err := c.client.Get(url)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		bodyBytes, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("server error (status %d): %s", resp.StatusCode, string(bodyBytes))
	}

	var result map[string]interface{}
	json.NewDecoder(resp.Body).Decode(&result)

	if found, ok := result["found"].(bool); ok && found {
		fmt.Printf("✓ GET successful: key=%d, value=%v", key, result["value"])
		if stale {
			fmt.Printf(" (stale read)")
		}
		fmt.Println()
	} else {
		fmt.Printf("✗ Key not found: %d\n", key)
	}

	return nil
}

// cmdDelete handles DELETE operations
func (c *CLI) cmdDelete(args []string) error {
	if len(args) != 1 {
		return fmt.Errorf("usage: delete <key>")
	}

	key, err := strconv.Atoi(args[0])
	if err != nil {
		return fmt.Errorf("invalid key: %v", err)
	}

	// Generate idempotency token
	idempotencyToken := uuid.New().String()

	// Execute with retry
	retryFunc := func(ctx context.Context) error {
		httpReq, _ := http.NewRequestWithContext(ctx, http.MethodDelete, fmt.Sprintf("%s/kv/%d", c.baseURL, key), nil)
		httpReq.Header.Set("Idempotency-Key", idempotencyToken)

		resp, err := c.client.Do(httpReq)
		if err != nil {
			return err
		}
		defer resp.Body.Close()

		if resp.StatusCode != http.StatusOK {
			bodyBytes, _ := io.ReadAll(resp.Body)
			return fmt.Errorf("server error (status %d): %s", resp.StatusCode, string(bodyBytes))
		}

		// Check for cache hit
		cacheStatus := resp.Header.Get("X-Cache")
		if cacheStatus == "HIT" {
			fmt.Printf("  (idempotent retry detected - returned cached result)\n")
		}

		return nil
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	err = c.retrier.ExecuteWithRetry(ctx, retryFunc)
	if err != nil {
		return fmt.Errorf("DELETE failed after retries: %v", err)
	}

	fmt.Printf("✓ DELETE successful: key=%d\n", key)
	return nil
}

// cmdStatus shows cluster status
func (c *CLI) cmdStatus(args []string) error {
	resp, err := c.client.Get(c.baseURL + "/status")
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	var status map[string]interface{}
	json.NewDecoder(resp.Body).Decode(&status)

	fmt.Println("Cluster Status:")
	fmt.Println("----------------------------------------")
	fmt.Printf("  Node ID:     %v\n", status["node_id"])
	fmt.Printf("  Role:        %v\n", status["role"])
	fmt.Printf("  Term:        %v\n", status["term"])
	fmt.Printf("  Leader ID:   %v\n", status["leader_id"])
	fmt.Printf("  Total Nodes: %v\n", status["total_nodes"])
	fmt.Println("----------------------------------------")

	return nil
}

// cmdLeader shows leader information
func (c *CLI) cmdLeader(args []string) error {
	resp, err := c.client.Get(c.baseURL + "/leader")
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	var result map[string]interface{}
	json.NewDecoder(resp.Body).Decode(&result)

	if isLeader, ok := result["is_leader"].(bool); ok && isLeader {
		fmt.Println("✓ This node is the leader")
	} else {
		fmt.Printf("Leader: Node %v\n", result["leader_id"])
	}

	return nil
}

// cmdHealth checks node health
func (c *CLI) cmdHealth(args []string) error {
	resp, err := c.client.Get(c.baseURL + "/health")
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusOK {
		fmt.Println("✓ Node is healthy")
	} else {
		fmt.Println("✗ Node is unhealthy")
	}

	return nil
}

// cmdHelp shows available commands
func (c *CLI) cmdHelp() {
	fmt.Println("Available Commands:")
	fmt.Println("----------------------------------------")
	fmt.Println("  put <key> <value>  - Store a key-value pair")
	fmt.Println("  get <key> [--stale] - Retrieve value for key")
	fmt.Println("                        --stale: Allow stale reads (faster)")
	fmt.Println("  delete <key>       - Delete a key")
	fmt.Println("  status             - Show cluster status")
	fmt.Println("  leader             - Show current leader")
	fmt.Println("  health             - Check node health")
	fmt.Println("  help               - Show this help message")
	fmt.Println("  exit               - Exit the CLI")
	fmt.Println("----------------------------------------")
	fmt.Println("Examples:")
	fmt.Println("  > put 1 100")
	fmt.Println("  > get 1")
	fmt.Println("  > get 1 --stale")
	fmt.Println("  > delete 1")
	fmt.Println("----------------------------------------")
}
