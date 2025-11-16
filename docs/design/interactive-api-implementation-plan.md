# Interactive API Implementation Plan

**Created**: 2025-11-06
**Purpose**: Add interactive terminal and HTTP API for user interaction with the distributed cache

---

## Table of Contents

1. [Overview](#overview)
2. [Architecture Design](#architecture-design)
3. [Implementation Phases](#implementation-phases)
4. [Detailed Specifications](#detailed-specifications)
5. [API Reference](#api-reference)
6. [CLI Reference](#cli-reference)
7. [Implementation Checklist](#implementation-checklist)
8. [Testing Strategy](#testing-strategy)

---

## Overview

### Goals

1. **HTTP REST API**: Programmatic access to the cache from any language/tool
2. **Interactive CLI**: Human-friendly terminal interface for testing and debugging
3. **Any-Node Access**: Send requests to any node, automatic forwarding to leader
4. **Synchronous Responses**: Wait for consensus before returning results
5. **Cluster Observability**: Health checks, status, and metrics endpoints

### User Stories

**As a developer**, I want to:
- PUT/GET/DELETE key-value pairs via HTTP API
- Use curl or Postman to interact with the cache
- Send requests to any node in the cluster
- Get confirmation when my write is committed

**As an operator**, I want to:
- Use an interactive terminal to test the cluster
- Check cluster health and status
- See which node is the leader
- Monitor replication lag

### Non-Goals (For Now)

- ❌ Authentication/authorization (future enhancement)
- ❌ TLS/SSL encryption (future enhancement)
- ❌ Complex query language (simple key-value only)
- ❌ Batch operations (one operation at a time)

---

## Architecture Design

### High-Level Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                         Client                              │
│                                                             │
│  ┌──────────────┐         ┌──────────────────────┐        │
│  │  HTTP Client │         │  Interactive CLI     │        │
│  │  (curl, app) │         │  (raftcli)           │        │
│  └──────┬───────┘         └──────────┬───────────┘        │
│         │                            │                     │
└─────────┼────────────────────────────┼─────────────────────┘
          │                            │
          │ HTTP                       │ HTTP
          ▼                            ▼
┌─────────────────────────────────────────────────────────────┐
│                      Raft Node                              │
│  ┌──────────────────────────────────────────────────────┐  │
│  │              HTTP API Server (:8080)                 │  │
│  │  ┌────────────┐  ┌─────────┐  ┌──────────────┐     │  │
│  │  │ PUT /kv    │  │ GET /kv │  │ DELETE /kv   │     │  │
│  │  │ POST       │  │         │  │              │     │  │
│  │  └─────┬──────┘  └────┬────┘  └──────┬───────┘     │  │
│  └────────┼──────────────┼───────────────┼─────────────┘  │
│           │              │               │                 │
│           └──────────────┴───────────────┘                 │
│                          │                                 │
│  ┌───────────────────────▼──────────────────────────────┐ │
│  │         Request Handler / Orchestrator               │ │
│  │  - Validates request                                 │ │
│  │  - Checks if leader                                  │ │
│  │  - Calls Raft.Broadcast() or forwards               │ │
│  │  - Waits for response via channel                   │ │
│  └───────────────────────┬──────────────────────────────┘ │
│                          │                                 │
│  ┌───────────────────────▼──────────────────────────────┐ │
│  │            Raft Core (pkg/raft)                      │ │
│  │  - Broadcast() for writes                            │ │
│  │  - Direct read from Application for GETs            │ │
│  │  - Response channels for sync replies               │ │
│  └──────────────────────────────────────────────────────┘ │
│                          │                                 │
│  ┌───────────────────────▼──────────────────────────────┐ │
│  │         Application (SimpleKVStore)                  │ │
│  │  - Stores key-value pairs                            │ │
│  │  - Returns results                                   │ │
│  └──────────────────────────────────────────────────────┘ │
└─────────────────────────────────────────────────────────────┘
```

### Request Flow

#### Write Operations (PUT/DELETE)

```
1. Client → HTTP POST /kv {key: "foo", value: 42}
2. HTTP Handler → Validate request
3. Handler → Check if local node is leader
   ├─ If Leader: Process locally
   └─ If Follower: Forward to leader via Broadcast()
4. Raft.Broadcast() → Create log entry
5. Raft → Replicate to followers via gRPC
6. Raft → Wait for quorum acknowledgment
7. Raft → Commit to state machine (Application)
8. Application → Apply operation, return result
9. Handler → Receive result via response channel
10. HTTP Response → Return 200 OK with result
```

#### Read Operations (GET)

**Option 1: Linearizable Reads** (Strongly Consistent)
```
1. Client → HTTP GET /kv/foo
2. HTTP Handler → Forward to leader
3. Leader → Verify leadership (heartbeat round)
4. Leader → Read from local Application
5. HTTP Response → Return value
```

**Option 2: Stale Reads** (Eventually Consistent, Faster)
```
1. Client → HTTP GET /kv/foo?stale=true
2. HTTP Handler → Read from local Application
3. HTTP Response → Return value (may be stale)
```

### Component Responsibilities

| Component | Responsibility |
|-----------|---------------|
| **HTTP Server** | Accept client requests, validate input, return responses |
| **Request Handler** | Orchestrate Raft operations, manage response channels |
| **Raft Core** | Consensus, replication, log management |
| **Application** | State machine, key-value storage |
| **CLI Client** | Human-friendly terminal interface |

---

## Implementation Phases

### Phase 1: Response Channel Mechanism (Core Change)
**Effort**: 3-4 hours

Modify Raft to support synchronous responses:

**Tasks**:
1. Add response channel to `Message` struct
2. Modify `Broadcast()` to accept response channel
3. Update `deliverToApplication()` to send response
4. Modify `Application` interface for response support

**Key Changes**:
- `Message` struct gets optional `responseChan chan<- BroadcastResponse`
- `BroadcastResponse` type with `Success bool`, `Value int`, `Error error`
- Timeout handling (5 seconds default)

---

### Phase 2: HTTP API Server
**Effort**: 4-5 hours

Create REST API endpoints on each Raft node:

**Tasks**:
1. Create `pkg/api/server.go` with HTTP handlers
2. Implement PUT, GET, DELETE endpoints
3. Add health and status endpoints
4. Integrate with Raft node

**Endpoints**:
- `POST /kv` - Create/update key-value pair
- `GET /kv/{key}` - Read value for key
- `DELETE /kv/{key}` - Delete key
- `GET /health` - Health check
- `GET /status` - Cluster status
- `GET /leader` - Get current leader info

---

### Phase 3: Interactive CLI Tool
**Effort**: 3-4 hours

Build standalone CLI for interactive use:

**Tasks**:
1. Create `cmd/raftcli/main.go`
2. Implement interactive shell with readline
3. Add command parser (PUT/GET/DELETE/STATUS/EXIT)
4. HTTP client to communicate with nodes

**Commands**:
- `put <key> <value>` - Store key-value
- `get <key>` - Retrieve value
- `delete <key>` - Remove key
- `status` - Show cluster status
- `leader` - Show current leader
- `help` - Show commands
- `exit` - Quit

---

### Phase 4: Enhanced Observability
**Effort**: 2-3 hours

Add monitoring and debugging endpoints:

**Tasks**:
1. Prometheus metrics endpoint
2. Detailed cluster state endpoint
3. Log streaming endpoint (optional)
4. Performance metrics

---

## Detailed Specifications

### 1. Response Channel Mechanism

#### Update Message Struct

**File**: `pkg/raft/types.go`

```go
type BroadcastResponse struct {
    Success bool
    Value   int
    Error   error
}

type Message struct {
    MsgType MessageType
    Key     int
    Value   *int

    // NEW: Response channel for synchronous operations
    ResponseChan chan<- BroadcastResponse
}
```

#### Update Broadcast Method

**File**: `pkg/raft/raft.go`

```go
// BroadcastSync sends a message and waits for response
func (r *Raft) BroadcastSync(message Message, timeout time.Duration) (bool, int, error) {
    responseChan := make(chan BroadcastResponse, 1)
    message.ResponseChan = responseChan

    r.Broadcast(message)

    select {
    case resp := <-responseChan:
        return resp.Success, resp.Value, resp.Error
    case <-time.After(timeout):
        return false, 0, fmt.Errorf("timeout waiting for response")
    }
}

// Broadcast remains for backward compatibility (async)
func (r *Raft) Broadcast(message Message) {
    // ... existing implementation ...
    // If message has ResponseChan, don't close it here
}
```

#### Update deliverToApplication

**File**: `pkg/raft/raft.go`

```go
func (r *Raft) deliverToApplication(message Message) (success bool, value int) {
    success, value = r.application.AppendMessage(message)

    // NEW: Send response if channel provided
    if message.ResponseChan != nil {
        select {
        case message.ResponseChan <- BroadcastResponse{
            Success: success,
            Value:   value,
            Error:   nil,
        }:
        default:
            // Channel closed or full, ignore
        }
    }

    return success, value
}
```

---

### 2. HTTP API Server

#### API Server Structure

**File**: `pkg/api/server.go`

```go
package api

import (
    "encoding/json"
    "fmt"
    "log/slog"
    "net/http"
    "strconv"
    "time"

    "github.com/dominikszczepaniak/distributed-cache/pkg/raft"
)

type Server struct {
    raft       *raft.Raft
    listenAddr string
    httpServer *http.Server
}

func NewServer(r *raft.Raft, listenAddr string) *Server {
    return &Server{
        raft:       r,
        listenAddr: listenAddr,
    }
}

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

func (s *Server) Stop() error {
    if s.httpServer != nil {
        return s.httpServer.Close()
    }
    return nil
}
```

#### Request/Response Types

```go
type PutRequest struct {
    Key   int `json:"key"`
    Value int `json:"value"`
}

type PutResponse struct {
    Success bool   `json:"success"`
    Message string `json:"message,omitempty"`
}

type GetResponse struct {
    Key   int  `json:"key"`
    Value int  `json:"value"`
    Found bool `json:"found"`
}

type DeleteRequest struct {
    Key int `json:"key"`
}

type DeleteResponse struct {
    Success bool   `json:"success"`
    Message string `json:"message,omitempty"`
}

type StatusResponse struct {
    NodeID      int      `json:"node_id"`
    Role        string   `json:"role"`
    Term        int      `json:"term"`
    LeaderID    int      `json:"leader_id"`
    TotalNodes  int      `json:"total_nodes"`
    PeersStatus []string `json:"peers_status"`
}
```

#### PUT Handler

```go
func (s *Server) handleKV(w http.ResponseWriter, r *http.Request) {
    switch r.Method {
    case http.MethodPost:
        s.handlePut(w, r)
    default:
        http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
    }
}

func (s *Server) handlePut(w http.ResponseWriter, r *http.Request) {
    var req PutRequest
    if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
        http.Error(w, "Invalid request body", http.StatusBadRequest)
        return
    }

    msg := raft.Message{
        MsgType: "PUT",
        Key:     req.Key,
        Value:   &req.Value,
    }

    success, _, err := s.raft.BroadcastSync(msg, 5*time.Second)
    if err != nil {
        http.Error(w, err.Error(), http.StatusInternalServerError)
        return
    }

    resp := PutResponse{
        Success: success,
        Message: "Key-value pair stored successfully",
    }

    w.Header().Set("Content-Type", "application/json")
    json.NewEncoder(w).Encode(resp)
}
```

#### GET Handler

```go
func (s *Server) handleKVWithKey(w http.ResponseWriter, r *http.Request) {
    // Extract key from URL path
    keyStr := r.URL.Path[len("/kv/"):]
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

func (s *Server) handleGet(w http.ResponseWriter, r *http.Request, key int) {
    // Option 1: Stale reads (fast, may be stale)
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
            // Redirect to leader
            leaderAddr := s.getLeaderAddr()
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
```

#### DELETE Handler

```go
func (s *Server) handleDelete(w http.ResponseWriter, r *http.Request, key int) {
    msg := raft.Message{
        MsgType: "DELETE",
        Key:     key,
        Value:   nil,
    }

    success, _, err := s.raft.BroadcastSync(msg, 5*time.Second)
    if err != nil {
        http.Error(w, err.Error(), http.StatusInternalServerError)
        return
    }

    resp := DeleteResponse{
        Success: success,
        Message: "Key deleted successfully",
    }

    w.Header().Set("Content-Type", "application/json")
    json.NewEncoder(w).Encode(resp)
}
```

#### Status Handlers

```go
func (s *Server) handleHealth(w http.ResponseWriter, r *http.Request) {
    // Simple health check
    w.Header().Set("Content-Type", "application/json")
    json.NewEncoder(w).Encode(map[string]string{
        "status": "healthy",
    })
}

func (s *Server) handleStatus(w http.ResponseWriter, r *http.Request) {
    status := s.raft.GetStatus()

    resp := StatusResponse{
        NodeID:     status.NodeID,
        Role:       string(status.Role),
        Term:       status.Term,
        LeaderID:   status.LeaderID,
        TotalNodes: status.TotalNodes,
        // Add peer availability
    }

    w.Header().Set("Content-Type", "application/json")
    json.NewEncoder(w).Encode(resp)
}

func (s *Server) handleLeader(w http.ResponseWriter, r *http.Request) {
    isLeader, leaderID := s.raft.GetLeaderData()

    w.Header().Set("Content-Type", "application/json")
    json.NewEncoder(w).Encode(map[string]interface{}{
        "is_leader": isLeader,
        "leader_id": leaderID,
    })
}
```

---

### 3. Interactive CLI

#### CLI Structure

**File**: `cmd/raftcli/main.go`

```go
package main

import (
    "bufio"
    "bytes"
    "encoding/json"
    "fmt"
    "io"
    "net/http"
    "os"
    "strconv"
    "strings"
)

type CLI struct {
    client  *http.Client
    baseURL string
}

func NewCLI(baseURL string) *CLI {
    return &CLI{
        client:  &http.Client{},
        baseURL: baseURL,
    }
}

func main() {
    if len(os.Args) < 2 {
        fmt.Println("Usage: raftcli <node-address>")
        fmt.Println("Example: raftcli localhost:8080")
        os.Exit(1)
    }

    baseURL := "http://" + os.Args[1]
    cli := NewCLI(baseURL)

    fmt.Println("Raft Interactive CLI")
    fmt.Println("Type 'help' for available commands")
    fmt.Println()

    cli.Run()
}

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
```

#### CLI Commands

```go
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

    req := map[string]int{"key": key, "value": value}
    body, _ := json.Marshal(req)

    resp, err := c.client.Post(c.baseURL+"/kv", "application/json", bytes.NewBuffer(body))
    if err != nil {
        return err
    }
    defer resp.Body.Close()

    if resp.StatusCode != http.StatusOK {
        body, _ := io.ReadAll(resp.Body)
        return fmt.Errorf("server error: %s", body)
    }

    var result map[string]interface{}
    json.NewDecoder(resp.Body).Decode(&result)

    fmt.Printf("✓ PUT successful: key=%d, value=%d\n", key, value)
    return nil
}

func (c *CLI) cmdGet(args []string) error {
    if len(args) != 1 {
        return fmt.Errorf("usage: get <key>")
    }

    key, err := strconv.Atoi(args[0])
    if err != nil {
        return fmt.Errorf("invalid key: %v", err)
    }

    resp, err := c.client.Get(fmt.Sprintf("%s/kv/%d", c.baseURL, key))
    if err != nil {
        return err
    }
    defer resp.Body.Close()

    var result map[string]interface{}
    json.NewDecoder(resp.Body).Decode(&result)

    if found, ok := result["found"].(bool); ok && found {
        fmt.Printf("✓ GET successful: key=%d, value=%v\n", key, result["value"])
    } else {
        fmt.Printf("✗ Key not found: %d\n", key)
    }

    return nil
}

func (c *CLI) cmdDelete(args []string) error {
    if len(args) != 1 {
        return fmt.Errorf("usage: delete <key>")
    }

    key, err := strconv.Atoi(args[0])
    if err != nil {
        return fmt.Errorf("invalid key: %v", err)
    }

    req, _ := http.NewRequest(http.MethodDelete, fmt.Sprintf("%s/kv/%d", c.baseURL, key), nil)
    resp, err := c.client.Do(req)
    if err != nil {
        return err
    }
    defer resp.Body.Close()

    fmt.Printf("✓ DELETE successful: key=%d\n", key)
    return nil
}

func (c *CLI) cmdStatus(args []string) error {
    resp, err := c.client.Get(c.baseURL + "/status")
    if err != nil {
        return err
    }
    defer resp.Body.Close()

    var status map[string]interface{}
    json.NewDecoder(resp.Body).Decode(&status)

    fmt.Println("Cluster Status:")
    fmt.Printf("  Node ID:    %v\n", status["node_id"])
    fmt.Printf("  Role:       %v\n", status["role"])
    fmt.Printf("  Term:       %v\n", status["term"])
    fmt.Printf("  Leader ID:  %v\n", status["leader_id"])
    fmt.Printf("  Total Nodes: %v\n", status["total_nodes"])

    return nil
}

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

func (c *CLI) cmdHelp() {
    fmt.Println("Available Commands:")
    fmt.Println("  put <key> <value>  - Store a key-value pair")
    fmt.Println("  get <key>          - Retrieve value for key")
    fmt.Println("  delete <key>       - Delete a key")
    fmt.Println("  status             - Show cluster status")
    fmt.Println("  leader             - Show current leader")
    fmt.Println("  health             - Check node health")
    fmt.Println("  help               - Show this help message")
    fmt.Println("  exit               - Exit the CLI")
}
```

---

### 4. Integration with Main Application

#### Update cmd/raftnode/main.go

```go
func main() {
    // ... existing setup ...

    // Create Raft instance
    r := raft.NewRaft(app, cfg)

    slog.Info("Raft node started successfully")

    // NEW: Start HTTP API server
    apiAddr := os.Getenv("API_ADDR")
    if apiAddr == "" {
        apiAddr = ":8080"
    }

    apiServer := api.NewServer(r, apiAddr)
    go func() {
        if err := apiServer.Start(); err != nil {
            slog.Error(fmt.Sprintf("API server error: %v", err))
        }
    }()

    // Wait for shutdown signal
    sigCh := make(chan os.Signal, 1)
    signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)

    sig := <-sigCh
    slog.Info(fmt.Sprintf("Received signal %v, shutting down...", sig))

    // Cleanup
    apiServer.Stop()

    slog.Info("Raft node stopped")
}
```

---

## API Reference

### Endpoints

#### `POST /kv`
Store or update a key-value pair.

**Request**:
```json
{
  "key": 42,
  "value": 100
}
```

**Response** (200 OK):
```json
{
  "success": true,
  "message": "Key-value pair stored successfully"
}
```

**Response** (500 Internal Server Error):
```json
{
  "error": "timeout waiting for response"
}
```

---

#### `GET /kv/{key}`
Retrieve value for a key.

**Query Parameters**:
- `stale=true` (optional): Allow stale reads for better performance

**Response** (200 OK):
```json
{
  "key": 42,
  "value": 100,
  "found": true
}
```

**Response** (404 Not Found):
```json
{
  "key": 42,
  "found": false
}
```

---

#### `DELETE /kv/{key}`
Delete a key-value pair.

**Response** (200 OK):
```json
{
  "success": true,
  "message": "Key deleted successfully"
}
```

---

#### `GET /health`
Health check endpoint.

**Response** (200 OK):
```json
{
  "status": "healthy"
}
```

---

#### `GET /status`
Get cluster status.

**Response** (200 OK):
```json
{
  "node_id": 0,
  "role": "leader",
  "term": 5,
  "leader_id": 0,
  "total_nodes": 3,
  "peers_status": ["healthy", "healthy", "unhealthy"]
}
```

---

#### `GET /leader`
Get current leader information.

**Response** (200 OK):
```json
{
  "is_leader": false,
  "leader_id": 1
}
```

---

## CLI Reference

### Commands

| Command | Arguments | Description | Example |
|---------|-----------|-------------|---------|
| `put` | `<key> <value>` | Store key-value pair | `put 10 100` |
| `get` | `<key>` | Retrieve value | `get 10` |
| `delete` | `<key>` | Delete key | `delete 10` |
| `status` | - | Show cluster status | `status` |
| `leader` | - | Show current leader | `leader` |
| `health` | - | Check node health | `health` |
| `help` | - | Show help | `help` |
| `exit` | - | Exit CLI | `exit` |

### Usage Example

```bash
# Start CLI
./raftcli localhost:8080

# Interactive session
> put 1 100
✓ PUT successful: key=1, value=100

> get 1
✓ GET successful: key=1, value=100

> status
Cluster Status:
  Node ID:     0
  Role:        follower
  Term:        3
  Leader ID:   1
  Total Nodes: 3

> delete 1
✓ DELETE successful: key=1

> exit
Goodbye!
```

---

## Implementation Checklist

### Phase 1: Response Mechanism (3-4 hours)
- [ ] Add `BroadcastResponse` type to `pkg/raft/types.go`
- [ ] Add `ResponseChan` field to `Message` struct
- [ ] Implement `BroadcastSync()` method in `pkg/raft/raft.go`
- [ ] Update `deliverToApplication()` to send responses
- [ ] Add `GetApplication()` method to Raft for direct reads
- [ ] Add `IsLeader()` helper method
- [ ] Add `GetStatus()` method returning cluster state
- [ ] Test response mechanism with unit tests

### Phase 2: HTTP API Server (4-5 hours)
- [ ] Create `pkg/api/` directory
- [ ] Create `pkg/api/types.go` with request/response structs
- [ ] Create `pkg/api/server.go` with HTTP server
- [ ] Implement `handlePut()` handler
- [ ] Implement `handleGet()` handler
- [ ] Implement `handleDelete()` handler
- [ ] Implement `handleHealth()` handler
- [ ] Implement `handleStatus()` handler
- [ ] Implement `handleLeader()` handler
- [ ] Add CORS headers (optional)
- [ ] Add request logging middleware
- [ ] Test API with curl

### Phase 3: CLI Tool (3-4 hours)
- [ ] Create `cmd/raftcli/` directory
- [ ] Create `cmd/raftcli/main.go` with CLI framework
- [ ] Implement `cmdPut()` command
- [ ] Implement `cmdGet()` command
- [ ] Implement `cmdDelete()` command
- [ ] Implement `cmdStatus()` command
- [ ] Implement `cmdLeader()` command
- [ ] Implement `cmdHealth()` command
- [ ] Implement `cmdHelp()` command
- [ ] Add command history (optional, using readline library)
- [ ] Add tab completion (optional)
- [ ] Test CLI interactively

### Phase 4: Integration (2-3 hours)
- [ ] Update `cmd/raftnode/main.go` to start API server
- [ ] Add `API_ADDR` environment variable support
- [ ] Update `docker-compose.yml` with API ports (8080-8082)
- [ ] Update Dockerfile to build raftcli
- [ ] Create example scripts for testing
- [ ] Update documentation

### Phase 5: Testing (2-3 hours)
- [ ] Create `pkg/api/server_test.go` with HTTP tests
- [ ] Test PUT with leader node
- [ ] Test PUT with follower node (forwarding)
- [ ] Test GET with stale reads
- [ ] Test DELETE operations
- [ ] Test concurrent requests
- [ ] Test timeout scenarios
- [ ] Integration test with Docker Compose cluster

---

## Testing Strategy

### Unit Tests

**File**: `pkg/api/server_test.go`

```go
func TestPutHandler(t *testing.T) {
    // Create mock Raft
    // Send PUT request
    // Verify response
}

func TestGetHandler(t *testing.T) {
    // Test found case
    // Test not found case
    // Test stale reads
}

func TestStatusHandler(t *testing.T) {
    // Verify status fields
}
```

### Integration Tests

**Manual Test Script**: `scripts/test-api.sh`

```bash
#!/bin/bash

API_URL="http://localhost:8080"

echo "Testing PUT..."
curl -X POST $API_URL/kv -d '{"key":1,"value":100}' -H "Content-Type: application/json"

echo "Testing GET..."
curl $API_URL/kv/1

echo "Testing DELETE..."
curl -X DELETE $API_URL/kv/1

echo "Testing Status..."
curl $API_URL/status
```

### CLI Testing

```bash
# Start cluster
docker-compose up -d

# Run CLI
./raftcli localhost:8080

# Execute test commands
put 1 100
get 1
status
delete 1
exit
```

---

## Docker Compose Updates

### Update docker-compose.yml

Add API port mappings:

```yaml
services:
  raft-node-0:
    # ... existing config ...
    environment:
      # ... existing env vars ...
      API_ADDR: ":8080"
    ports:
      - "9000:9000"  # Raft
      - "8080:8080"  # API

  raft-node-1:
    # ... existing config ...
    environment:
      API_ADDR: ":8080"
    ports:
      - "9001:9000"  # Raft
      - "8081:8080"  # API

  raft-node-2:
    # ... existing config ...
    environment:
      API_ADDR: ":8080"
    ports:
      - "9002:9000"  # Raft
      - "8082:8080"  # API
```

---

## Performance Considerations

### Synchronous vs Asynchronous

**Synchronous Writes** (Implemented):
- ✅ Strong consistency guarantees
- ✅ Simple client logic
- ❌ Higher latency (waits for quorum)
- **Use case**: Financial transactions, critical data

**Asynchronous Writes** (Future):
- ✅ Lower latency (immediate response)
- ❌ Eventual consistency
- ❌ Complex client logic (need to poll)
- **Use case**: Analytics, logging, non-critical data

### Read Strategies

**Linearizable Reads** (Default):
- Leader serves after verifying leadership
- Strong consistency
- Higher latency

**Stale Reads** (`?stale=true`):
- Any node serves from local state
- May return old data
- Lower latency

### Timeouts

| Operation | Default Timeout | Rationale |
|-----------|----------------|-----------|
| Write (PUT/DELETE) | 5s | Consensus + replication |
| Read (GET) | 1s | Local read only |
| Health check | 500ms | Quick liveness probe |

---

## Security Considerations (Future)

Not implemented in this phase, but important for production:

1. **Authentication**: JWT tokens, API keys
2. **Authorization**: Role-based access control
3. **TLS**: Encrypt HTTP traffic
4. **Rate Limiting**: Prevent abuse
5. **Input Validation**: Sanitize all inputs
6. **Audit Logging**: Track all operations

---

## Monitoring & Observability (Phase 4)

### Metrics to Expose

```go
// Prometheus metrics
var (
    requestsTotal = prometheus.NewCounterVec(
        prometheus.CounterOpts{
            Name: "raft_api_requests_total",
            Help: "Total API requests",
        },
        []string{"method", "endpoint", "status"},
    )

    requestDuration = prometheus.NewHistogramVec(
        prometheus.HistogramOpts{
            Name: "raft_api_request_duration_seconds",
            Help: "API request duration",
        },
        []string{"method", "endpoint"},
    )
)
```

### Metrics Endpoint

```go
func (s *Server) handleMetrics(w http.ResponseWriter, r *http.Request) {
    promhttp.Handler().ServeHTTP(w, r)
}
```

---

## Example Usage Scenarios

### Scenario 1: Store and Retrieve Data

```bash
# Using curl
curl -X POST http://localhost:8080/kv \
  -H "Content-Type: application/json" \
  -d '{"key":42,"value":100}'

curl http://localhost:8080/kv/42

# Using CLI
./raftcli localhost:8080
> put 42 100
> get 42
```

### Scenario 2: Test Failover

```bash
# Find leader
curl http://localhost:8080/leader

# Stop leader node
docker-compose stop raft-node-1

# Send write to follower (auto-forwards to new leader)
curl -X POST http://localhost:8081/kv \
  -d '{"key":1,"value":200}'
```

### Scenario 3: Monitoring

```bash
# Check health of all nodes
for port in 8080 8081 8082; do
  curl http://localhost:$port/health
done

# Get cluster status
curl http://localhost:8080/status | jq
```

---

## Migration Path

### Backward Compatibility

Existing code continues to work:
- Old `Broadcast()` method remains (async)
- New `BroadcastSync()` for API layer
- Tests using `setPeers()` unaffected

### Rollout Strategy

1. **Phase 1**: Deploy response mechanism (no API yet)
2. **Phase 2**: Deploy API on one node, test thoroughly
3. **Phase 3**: Roll out API to all nodes
4. **Phase 4**: Deploy CLI tool
5. **Phase 5**: Add monitoring/metrics

---

## Future Enhancements

1. **WebSocket Support**: Real-time updates for clients
2. **GraphQL API**: More flexible querying
3. **Batch Operations**: Multiple PUT/GET/DELETE in one request
4. **Pagination**: For large key ranges
5. **Query Language**: Filter/sort operations
6. **Backup/Restore API**: Snapshot management
7. **Admin API**: Cluster membership changes

---

**Last Updated**: 2025-11-06
**Status**: Implementation plan ready for execution

---

## Retry & Fault Tolerance

⚠️ **IMPORTANT**: This plan has been extended with comprehensive retry logic and fault tolerance mechanisms.

**See**: `retry-and-fault-tolerance-addendum.md` for complete specifications on:

1. **Multi-layer retry strategy** - Client, API, and Raft layers
2. **Idempotency design** - Prevent duplicate operations on retry
3. **Exponential backoff** - Configurable retry delays with jitter
4. **Leader redirection** - Automatic discovery and caching
5. **Error classification** - Retryable vs non-retryable errors
6. **Timeout management** - Per-layer timeout configurations

### Key Retry Features

```
┌─────────────────────────────────────────────┐
│         Retry Architecture                  │
├─────────────────────────────────────────────┤
│                                             │
│  Client Layer (CLI/SDK)                     │
│  ├─ Circuit breaker                         │
│  ├─ Exponential backoff (100ms → 5s)       │
│  └─ Max 3 attempts                          │
│                                             │
│  API Layer (HTTP Server)                    │
│  ├─ Idempotency cache (5min TTL)           │
│  ├─ Leader redirection                      │
│  ├─ Request deduplication                   │
│  └─ Timeout: 15s total                      │
│                                             │
│  Raft Layer                                 │
│  ├─ BroadcastSync with timeout              │
│  └─ Quorum wait: 5s                         │
│                                             │
└─────────────────────────────────────────────┘
```

### Failure Scenarios Handled

| Scenario | Retry Strategy | Max Time |
|----------|----------------|----------|
| Timeout waiting for consensus | 3 attempts with backoff | ~15s |
| Leader election in progress | Retry until new leader | ~15s |
| Network partition | Retry until timeout | ~15s |
| Duplicate request | Return cached response | < 1ms |
| Invalid input | No retry, immediate 400 | ~1ms |

### Implementation Changes

The retry addendum adds these components to the original plan:

**New Files**:
- `pkg/api/retry.go` - Retry helper with exponential backoff
- `pkg/api/idempotency.go` - Idempotency cache implementation
- `pkg/api/leader.go` - Leader discovery and caching

**Updated Files**:
- `pkg/api/server.go` - Add retry logic to all handlers
- `pkg/raft/types.go` - Add `IdempotencyToken` to `Message`
- `cmd/raftcli/main.go` - Client-side retry in CLI

**New Environment Variables**:
```bash
API_RETRY_MAX_ATTEMPTS=3
API_RETRY_INITIAL_DELAY=100ms
API_RETRY_MAX_DELAY=5s
API_IDEMPOTENCY_TTL=5m
API_IDEMPOTENCY_ENABLED=true
```

### Testing Additions

**Unit Tests**:
- `pkg/api/retry_test.go` - Test retry logic
- `pkg/api/idempotency_test.go` - Test cache behavior

**Integration Tests**:
- Test retry on timeout
- Test idempotent retry (duplicate detection)
- Test leader redirection
- Test concurrent requests with same idempotency token

---

