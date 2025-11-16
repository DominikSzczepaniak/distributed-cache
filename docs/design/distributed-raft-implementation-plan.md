# Distributed Raft Implementation Plan

**Created**: 2025-11-06
**Purpose**: Enable Raft algorithm to run across multiple physical computers with resilient connection management

---

## Table of Contents

1. [Overview](#overview)
2. [Current Implementation Analysis](#current-implementation-analysis)
3. [Key Questions & Answers](#key-questions--answers)
4. [Architecture Overview](#architecture-overview)
5. [Implementation Stages](#implementation-stages)
6. [Detailed Specifications](#detailed-specifications)
7. [Deployment Configurations](#deployment-configurations)
8. [Implementation Checklist](#implementation-checklist)
9. [Testing Strategy](#testing-strategy)

---

## Overview

### Goal
Enable the Raft consensus algorithm implementation in `pkg/raft` to run across multiple physical computers, not just multiple threads on the same machine.

### Key Requirements
- **Non-blocking startup**: Raft nodes can start independently without waiting for all peers
- **Cluster size awareness**: Each node knows total cluster size (totalNodes) for quorum calculations
- **Automatic reconnection**: Failed connections retry automatically with configurable backoff
- **Flexible deployment**: Support Docker Compose, Kubernetes, and bare metal deployments
- **Resilient to failures**: Handle network partitions, rolling deployments, and peer failures

---

## Current Implementation Analysis

### Current State
- **File**: `pkg/raft/raft.go:52-92` (NewRaft constructor)
- **Issue**: Constructor calls `initGRPC()` which synchronously establishes connections to all peers
- **Limitation**: Works for multi-threaded same-process testing, fails for distributed deployment

### Key Findings

#### Good News
- `grpc.NewClient()` at `grpc.go:23-25` is **already non-blocking** by default
- Connections are established lazily on first RPC call
- Config system already supports `RAFT_ADDRS` environment variable

#### Problem Areas
1. **Constructor dependency**: Raft expects all peers to be set at construction time
2. **No retry logic**: Failed connections have no automatic retry mechanism
3. **Panic on failure**: Code panics if peer connection fails during init (line 28)
4. **No health monitoring**: No background process to detect and handle connection failures

---

## Key Questions & Answers

### Q1: Do we need to use TCP handshake?
**A**: TCP handshake is **already handled automatically** by gRPC. gRPC uses HTTP/2 over TCP, which includes the standard TCP three-way handshake. No additional work needed.

### Q2: Do we wait before starting a raft service for connection to be done?
**A**: **No, you should NOT wait**. Key insights:
- Raft algorithm is designed to handle temporary network partitions
- Starting Raft before all connections are established is more robust
- Connections should be established asynchronously with retry logic
- Missing peers just can't participate in elections/replication until they're reachable

### Q3: How does the node know the total cluster size?
**A**: The node maintains the `totalNodes` variable from configuration (already implemented). This is critical for:
- Calculating quorum: `majority = ceil((totalNodes + 1) / 2)`
- Knowing when cluster is under-replicated
- Making correct consensus decisions even with partial connectivity

---

## Architecture Overview

### Design Principles

1. **Separation of Concerns**
   - Connection management separate from Raft logic
   - Health monitoring in background goroutines
   - Retry logic independent of RPC calls

2. **Graceful Degradation**
   - Raft starts immediately, even if no peers available
   - Elections proceed with available peers
   - Log replication skips unavailable peers

3. **Automatic Recovery**
   - Background health checks detect failures
   - Exponential backoff prevents network flooding
   - Connections automatically re-established when peers return

### Component Architecture

```
┌─────────────────────────────────────────────────────────┐
│                      Raft Node                          │
├─────────────────────────────────────────────────────────┤
│                                                         │
│  ┌──────────────┐         ┌──────────────────────┐    │
│  │   Raft Core  │────────▶│ ConnectionManager    │    │
│  │              │         │                      │    │
│  │  - Election  │         │ - Async Connections  │    │
│  │  - Log Rep   │         │ - Health Monitoring  │    │
│  │  - Snapshot  │         │ - Auto Reconnect     │    │
│  └──────────────┘         │ - Peer Availability  │    │
│         │                 └──────────────────────┘    │
│         │                           │                  │
│         │                           ▼                  │
│         │                 ┌──────────────────────┐    │
│         └────────────────▶│   gRPC Clients       │    │
│                           │                      │    │
│                           │  Peer 0, 1, 2, ...   │    │
│                           └──────────────────────┘    │
│                                     │                  │
└─────────────────────────────────────┼──────────────────┘
                                      │
                            Network (TCP/gRPC)
                                      │
                    ┌─────────────────┼─────────────────┐
                    ▼                 ▼                 ▼
              ┌─────────┐       ┌─────────┐       ┌─────────┐
              │ Peer 0  │       │ Peer 1  │       │ Peer 2  │
              └─────────┘       └─────────┘       └─────────┘
```

---

## Implementation Stages

### Stage 1: Configuration & Deployment Infrastructure
**Goal**: Support flexible deployment with configurable retry/timeout parameters

**Components**:
- Enhanced Config struct with retry parameters
- Environment variable parsing for connection settings
- Docker Compose configuration for multi-node testing
- Kubernetes StatefulSet configuration (optional)

**Effort**: 2-3 hours

---

### Stage 2: Connection Management Refactoring
**Goal**: Resilient, non-blocking connection establishment

**Components**:
- New `ConnectionManager` type
- Async connection establishment with retry
- Background health monitoring
- Peer availability tracking

**Effort**: 4-6 hours

---

### Stage 3: RPC Resilience & Error Handling
**Goal**: Make Raft algorithm robust to peer unavailability

**Components**:
- Peer availability checks before RPC calls
- Graceful handling of connection failures
- Updated quorum calculations considering availability
- Enhanced logging for distributed debugging

**Effort**: 3-4 hours

---

### Stage 4: Testing & Validation Framework
**Goal**: Validate distributed deployment works correctly

**Components**:
- Multi-node integration tests
- Network partition simulation
- Rolling deployment tests
- Health check endpoints

**Effort**: 4-5 hours

---

## Detailed Specifications

### 1. Configuration Enhancement

**File**: `pkg/raft/config.go`

#### New Structs

```go
type Config struct {
    // Existing fields
    logsFilename      string
    metadataFilename  string
    snapshotFilename  string
    totalNodes        int
    raftId            int
    raftAddrs         []string
    snapshotThreshold int

    // NEW: Connection management fields
    connectionRetryConfig RetryConfig
    connectionTimeout     time.Duration
    healthCheckInterval   time.Duration
    startupMode          StartupMode
}

type RetryConfig struct {
    InitialBackoff time.Duration // Default: 1s
    MaxBackoff     time.Duration // Default: 30s
    Multiplier     float64       // Default: 2.0
    MaxRetries     int           // Default: -1 (infinite)
}

type StartupMode int
const (
    StartAlways StartupMode = iota  // Start immediately
    WaitForQuorum                    // Wait for majority
    WaitForAnyPeer                   // Wait for at least one
)
```

#### Environment Variables

| Variable | Type | Default | Description |
|----------|------|---------|-------------|
| `RAFT_INITIAL_BACKOFF` | duration | `1s` | Initial retry delay |
| `RAFT_MAX_BACKOFF` | duration | `30s` | Maximum retry delay |
| `RAFT_BACKOFF_MULTIPLIER` | float | `2.0` | Backoff multiplier |
| `RAFT_MAX_RETRIES` | int | `-1` | Max retry attempts (-1 = infinite) |
| `RAFT_CONN_TIMEOUT` | duration | `5s` | Connection timeout |
| `RAFT_HEALTH_CHECK_INTERVAL` | duration | `10s` | Health check frequency |

#### Implementation

```go
func LoadConfig() *Config {
    // ... existing code ...

    // NEW: Load connection config with sensible defaults
    cfg.connectionRetryConfig = RetryConfig{
        InitialBackoff: getEnvDuration("RAFT_INITIAL_BACKOFF", 1*time.Second),
        MaxBackoff:     getEnvDuration("RAFT_MAX_BACKOFF", 30*time.Second),
        Multiplier:     getEnvFloat("RAFT_BACKOFF_MULTIPLIER", 2.0),
        MaxRetries:     getEnvInt("RAFT_MAX_RETRIES", -1),
    }

    cfg.connectionTimeout = getEnvDuration("RAFT_CONN_TIMEOUT", 5*time.Second)
    cfg.healthCheckInterval = getEnvDuration("RAFT_HEALTH_CHECK_INTERVAL", 10*time.Second)
    cfg.startupMode = StartAlways

    return cfg
}

// Helper functions
func getEnvDuration(key string, defaultVal time.Duration) time.Duration {
    if val := os.Getenv(key); val != "" {
        if d, err := time.ParseDuration(val); err == nil {
            return d
        }
    }
    return defaultVal
}

func getEnvFloat(key string, defaultVal float64) float64 {
    if val := os.Getenv(key); val != "" {
        if f, err := strconv.ParseFloat(val, 64); err == nil {
            return f
        }
    }
    return defaultVal
}

func getEnvInt(key string, defaultVal int) int {
    if val := os.Getenv(key); val != "" {
        if i, err := strconv.Atoi(val); err == nil {
            return i
        }
    }
    return defaultVal
}
```

---

### 2. Connection Manager

**File**: `pkg/raft/connection_manager.go` (NEW)

#### Purpose
Manage peer connections with health monitoring and automatic reconnection.

#### Key Features
- Async connection establishment per peer
- Exponential backoff retry logic
- Background health monitoring via gRPC connectivity states
- Automatic reconnection on failure
- Thread-safe peer access

#### Interface

```go
type ConnectionManager struct {
    mu sync.RWMutex

    // Connection state
    conns         []*grpc.ClientConn
    peers         []PeerClient
    peerAvailable []atomic.Bool
    lastContact   []time.Time

    // Configuration
    selfID      int
    totalNodes  int
    addrs       []string
    retryCfg    RetryConfig
    connTimeout time.Duration

    // Control
    ctx    context.Context
    cancel context.CancelFunc
    wg     sync.WaitGroup
}

// Constructor
func NewConnectionManager(selfID, totalNodes int, addrs []string, cfg *Config) *ConnectionManager

// Public methods
func (cm *ConnectionManager) GetPeer(peerID int) PeerClient
func (cm *ConnectionManager) GetPeers() []PeerClient
func (cm *ConnectionManager) IsPeerAvailable(peerID int) bool
func (cm *ConnectionManager) GetAvailablePeerCount() int
func (cm *ConnectionManager) Close()
```

#### Core Logic

**Connection Establishment** (`connectPeerAsync`):
1. Attempt connection via `dialPeer()`
2. If failure: wait with exponential backoff
3. Retry until success or max retries reached
4. Update peer availability atomically

**Health Monitoring** (`healthCheckLoop`):
1. Run on ticker (default: 10s intervals)
2. Check gRPC connection state for each peer
3. Detect transitions: healthy ↔ unhealthy
4. Trigger reconnection on failures

**Reconnection** (`reconnectPeer`):
1. Close old connection
2. Clear peer client reference
3. Launch new async connection attempt

#### Connection States (gRPC)

| State | Meaning | Action |
|-------|---------|--------|
| `Ready` | Healthy, active connection | Mark available |
| `Idle` | Healthy, inactive | Mark available |
| `Connecting` | Establishing connection | Wait |
| `TransientFailure` | Temporary failure | Mark unavailable, retry |
| `Shutdown` | Closed | Mark unavailable, reconnect |

---

### 3. Raft Integration

**File**: `pkg/raft/raft.go`

#### Struct Changes

```go
type Raft struct {
    mu sync.RWMutex

    id         int
    totalNodes int
    // ... existing fields ...

    application Application

    // REMOVE: peers atomic.Value
    // REMOVE: conns []*grpc.ClientConn

    // ADD: Connection manager
    connMgr *ConnectionManager

    raftElector *Elector
    logSaver    *DataSaver
    heartbeat   *Heartbeat
    replicators []*Replicator
    snapshotter *Snapshot

    raftpb.UnimplementedRaftServer
}
```

#### Constructor Changes

```go
func NewRaft(application Application, cfg *Config) *Raft {
    r := &Raft{
        id:             cfg.raftId,
        totalNodes:     cfg.totalNodes,
        // ... existing initialization ...
    }

    // ... persistence loading ...

    r.heartbeat = newHeartbeat(r)
    r.raftElector = NewRaftElector(r)

    // NEW: Initialize connection manager
    r.connMgr = NewConnectionManager(r.id, r.totalNodes, cfg.raftAddrs, cfg)

    // Start gRPC server (non-blocking)
    go r.serveGRPC(cfg.raftAddrs[r.id])

    return r
}
```

#### Method Updates

```go
// REPLACE getPeer
func (r *Raft) getPeer(nodeId int) PeerClient {
    return r.connMgr.GetPeer(nodeId)
}

// REPLACE setPeers - no longer needed, remove

// ADD: New helper methods
func (r *Raft) getPeers() []PeerClient {
    return r.connMgr.GetPeers()
}

func (r *Raft) isPeerAvailable(nodeId int) bool {
    return r.connMgr.IsPeerAvailable(nodeId)
}

func (r *Raft) getAvailablePeerCount() int {
    return r.connMgr.GetAvailablePeerCount()
}
```

---

### 4. RPC Resilience Updates

#### A. Election Logic

**File**: `pkg/raft/election.go`

**Changes**: Add peer availability checks before sending vote requests

```go
func (e *Elector) sendVoteRequests() {
    // ... prepare vote request ...

    for i := 0; i < e.r.totalNodes; i++ {
        if i == e.r.id {
            continue
        }

        // NEW: Check availability
        if !e.r.isPeerAvailable(i) {
            slog.Debug(fmt.Sprintf("Skipping vote request to unavailable peer %d", i))
            continue
        }

        peer := e.r.getPeer(i)
        if peer == nil {
            continue
        }

        go func(peerID int, p PeerClient) {
            ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
            defer cancel()

            resp, err := p.VoteRequest(ctx, req)
            if err != nil {
                slog.Warn(fmt.Sprintf("VoteRequest to peer %d failed: %v", peerID, err))
                return
            }

            e.r.receiveVote(VoteResponse{
                nodeId:      peerID,
                currentTerm: int(resp.CurrentTerm),
                granted:     resp.Granted,
            })
        }(i, peer)
    }
}
```

#### B. Log Replication

**File**: `pkg/raft/replicator.go`

**Changes**: Skip unavailable peers during replication

```go
func (rep *Replicator) replicateLog() {
    // ... prepare log request ...

    // NEW: Check availability
    if !rep.r.isPeerAvailable(rep.followerId) {
        slog.Debug(fmt.Sprintf("Skipping replication to unavailable peer %d", rep.followerId))
        return
    }

    peer := rep.r.getPeer(rep.followerId)
    if peer == nil {
        return
    }

    // ... send RPC ...

    resp, err := peer.LogRequest(ctx, req)
    if err != nil {
        slog.Warn(fmt.Sprintf("LogRequest to peer %d failed: %v", rep.followerId, err))
        return
    }

    // ... handle response ...
}
```

#### C. Quorum Calculations

**File**: `pkg/raft/raft.go`

**Changes**: Maintain quorum based on totalNodes, but log warnings

```go
func (r *Raft) becomeLeader() {
    r.currentRole = Leader
    r.currentLeaderId = r.id

    // Calculate quorum from totalNodes (not just available)
    majority := int(math.Ceil(float64(r.totalNodes+1) / 2))

    availableCount := r.getAvailablePeerCount()
    if availableCount < majority {
        slog.Warn(fmt.Sprintf(
            "Became leader with only %d/%d peers available (need %d for quorum)",
            availableCount, r.totalNodes, majority))
    }

    // ... existing leader initialization ...
}
```

---

### 5. Remove Old Code

**File**: `pkg/raft/grpc.go`

**DELETE** the `initGRPC` function (lines 18-36):
```go
// DELETE THIS ENTIRE FUNCTION
// func (r *Raft) initGRPC(cfg *Config) { ... }
```

**KEEP** the `serveGRPC` function (lines 38-48) - still needed for server.

---

## Deployment Configurations

### Docker Compose

**File**: `docker-compose.yml`

```yaml
version: '3.8'

services:
  raft-node-0:
    build: .
    environment:
      RAFT_ID: "0"
      TOTAL_NODES: "3"
      RAFT_ADDRS: "raft-node-0:9000,raft-node-1:9000,raft-node-2:9000"
      FILENAME: "/data/node0"
      SNAPSHOT_THRESHOLD: "100"

      # Connection configuration
      RAFT_INITIAL_BACKOFF: "1s"
      RAFT_MAX_BACKOFF: "30s"
      RAFT_BACKOFF_MULTIPLIER: "2.0"
      RAFT_CONN_TIMEOUT: "5s"
      RAFT_HEALTH_CHECK_INTERVAL: "10s"
    volumes:
      - ./data/node0:/data
    networks:
      - raft-network
    ports:
      - "9000:9000"

  raft-node-1:
    build: .
    environment:
      RAFT_ID: "1"
      TOTAL_NODES: "3"
      RAFT_ADDRS: "raft-node-0:9000,raft-node-1:9000,raft-node-2:9000"
      FILENAME: "/data/node1"
      SNAPSHOT_THRESHOLD: "100"

      RAFT_INITIAL_BACKOFF: "1s"
      RAFT_MAX_BACKOFF: "30s"
      RAFT_BACKOFF_MULTIPLIER: "2.0"
      RAFT_CONN_TIMEOUT: "5s"
      RAFT_HEALTH_CHECK_INTERVAL: "10s"
    volumes:
      - ./data/node1:/data
    networks:
      - raft-network
    ports:
      - "9001:9000"

  raft-node-2:
    build: .
    environment:
      RAFT_ID: "2"
      TOTAL_NODES: "3"
      RAFT_ADDRS: "raft-node-0:9000,raft-node-1:9000,raft-node-2:9000"
      FILENAME: "/data/node2"
      SNAPSHOT_THRESHOLD: "100"

      RAFT_INITIAL_BACKOFF: "1s"
      RAFT_MAX_BACKOFF: "30s"
      RAFT_BACKOFF_MULTIPLIER: "2.0"
      RAFT_CONN_TIMEOUT: "5s"
      RAFT_HEALTH_CHECK_INTERVAL: "10s"
    volumes:
      - ./data/node2:/data
    networks:
      - raft-network
    ports:
      - "9002:9000"

networks:
  raft-network:
    driver: bridge
```

### Kubernetes StatefulSet

**File**: `deploy/k8s-raft.yaml`

```yaml
apiVersion: v1
kind: Service
metadata:
  name: raft-service
spec:
  clusterIP: None  # Headless service
  selector:
    app: raft
  ports:
    - port: 9000
      name: raft

---

apiVersion: apps/v1
kind: StatefulSet
metadata:
  name: raft
spec:
  serviceName: "raft-service"
  replicas: 3
  selector:
    matchLabels:
      app: raft
  template:
    metadata:
      labels:
        app: raft
    spec:
      containers:
      - name: raft
        image: your-raft-image:latest
        ports:
        - containerPort: 9000
          name: raft
        env:
        - name: POD_NAME
          valueFrom:
            fieldRef:
              fieldPath: metadata.name
        - name: RAFT_ID
          # Extract ID from pod name (e.g., raft-0 -> 0)
          value: "$(echo $POD_NAME | sed 's/raft-//')"
        - name: TOTAL_NODES
          value: "3"
        - name: RAFT_ADDRS
          value: "raft-0.raft-service:9000,raft-1.raft-service:9000,raft-2.raft-service:9000"
        - name: FILENAME
          value: "/data/raft"
        - name: SNAPSHOT_THRESHOLD
          value: "100"
        - name: RAFT_INITIAL_BACKOFF
          value: "2s"
        - name: RAFT_MAX_BACKOFF
          value: "60s"
        volumeMounts:
        - name: data
          mountPath: /data
  volumeClaimTemplates:
  - metadata:
      name: data
    spec:
      accessModes: [ "ReadWriteOnce" ]
      resources:
        requests:
          storage: 1Gi
```

### Bare Metal / VM Deployment

**File**: `scripts/start-node.sh`

```bash
#!/bin/bash

# Usage: ./start-node.sh <node-id> <total-nodes> <addr1,addr2,addr3>

NODE_ID=$1
TOTAL_NODES=$2
RAFT_ADDRS=$3

export RAFT_ID=$NODE_ID
export TOTAL_NODES=$TOTAL_NODES
export RAFT_ADDRS=$RAFT_ADDRS
export FILENAME="/var/lib/raft/node${NODE_ID}"
export SNAPSHOT_THRESHOLD=100

# Connection config
export RAFT_INITIAL_BACKOFF="1s"
export RAFT_MAX_BACKOFF="30s"
export RAFT_BACKOFF_MULTIPLIER="2.0"
export RAFT_CONN_TIMEOUT="5s"
export RAFT_HEALTH_CHECK_INTERVAL="10s"

# Start the Raft service
./distributed-cache
```

**Example usage**:
```bash
# On machine 1 (10.0.0.1):
./start-node.sh 0 3 "10.0.0.1:9000,10.0.0.2:9000,10.0.0.3:9000"

# On machine 2 (10.0.0.2):
./start-node.sh 1 3 "10.0.0.1:9000,10.0.0.2:9000,10.0.0.3:9000"

# On machine 3 (10.0.0.3):
./start-node.sh 2 3 "10.0.0.1:9000,10.0.0.2:9000,10.0.0.3:9000"
```

---

## Implementation Checklist

### Phase 1: Configuration (2-3 hours)
- [ ] Add `RetryConfig` struct to `config.go`
- [ ] Add `StartupMode` enum to `config.go`
- [ ] Update `Config` struct with new fields
- [ ] Implement environment variable helpers (`getEnvDuration`, `getEnvFloat`, `getEnvInt`)
- [ ] Update `LoadConfig()` to load new settings
- [ ] Test configuration loading with various env combinations

### Phase 2: Connection Manager (4-6 hours)
- [ ] Create `pkg/raft/connection_manager.go`
- [ ] Implement `ConnectionManager` struct
- [ ] Implement `NewConnectionManager` constructor
- [ ] Implement `connectPeerAsync` with retry logic
- [ ] Implement `dialPeer` with gRPC options
- [ ] Implement `healthCheckLoop` for monitoring
- [ ] Implement `reconnectPeer` for recovery
- [ ] Implement public methods: `GetPeer`, `GetPeers`, `IsPeerAvailable`, `GetAvailablePeerCount`
- [ ] Implement `Close` for graceful shutdown
- [ ] Add comprehensive logging throughout
- [ ] Test connection manager in isolation

### Phase 3: Raft Integration (2-3 hours)
- [ ] Update `Raft` struct in `raft.go` (remove `peers`, `conns`, add `connMgr`)
- [ ] Update `NewRaft` constructor to use `ConnectionManager`
- [ ] Remove old `initGRPC` function from `grpc.go`
- [ ] Update `getPeer` method to use `connMgr`
- [ ] Remove `setPeers` method (no longer needed)
- [ ] Add `getPeers` method
- [ ] Add `isPeerAvailable` method
- [ ] Add `getAvailablePeerCount` method
- [ ] Update `serveGRPC` to run in goroutine from constructor
- [ ] Test Raft initialization

### Phase 4: RPC Resilience (3-4 hours)
- [ ] Update `election.go` to check peer availability before vote requests
- [ ] Add error logging for failed vote requests
- [ ] Update `replicator.go` to check peer availability before replication
- [ ] Add error logging for failed log requests
- [ ] Update `heartbeat.go` if needed
- [ ] Update `becomeLeader` to log warnings about quorum
- [ ] Update `commitLogEntries` if needed
- [ ] Test election with partial connectivity
- [ ] Test log replication with failing peers

### Phase 5: Deployment Setup (2-3 hours)
- [ ] Create `docker-compose.yml` for local testing
- [ ] Create `Dockerfile` if not exists
- [ ] Create `deploy/k8s-raft.yaml` for Kubernetes
- [ ] Create `scripts/start-node.sh` for bare metal
- [ ] Create `scripts/cluster-deploy.sh` helper
- [ ] Test Docker Compose deployment
- [ ] Test Kubernetes deployment (if applicable)
- [ ] Document deployment procedures

### Phase 6: Testing (4-5 hours)
- [ ] Create `pkg/raft/distributed_test.go`
- [ ] Implement `TestDistributedDeployment`
- [ ] Implement `TestRollingDeployment`
- [ ] Implement `TestNetworkPartition`
- [ ] Implement `TestPeerRecovery`
- [ ] Add health check endpoints (optional)
- [ ] Add metrics/monitoring (optional)
- [ ] Run full integration test suite
- [ ] Document test procedures

### Phase 7: Documentation (1-2 hours)
- [ ] Update `README.md` with distributed deployment instructions
- [ ] Create troubleshooting guide
- [ ] Document configuration options
- [ ] Create architecture diagrams
- [ ] Add example scenarios

---

## Testing Strategy

### Unit Tests

**File**: `pkg/raft/connection_manager_test.go`

```go
func TestConnectionManagerAsyncConnect(t *testing.T)
func TestConnectionManagerHealthCheck(t *testing.T)
func TestConnectionManagerReconnect(t *testing.T)
func TestConnectionManagerPeerAvailability(t *testing.T)
```

### Integration Tests

**File**: `pkg/raft/distributed_test.go`

```go
// Test nodes can start independently
func TestDistributedDeployment(t *testing.T) {
    // Start node 0
    // Start node 1 after 2s delay
    // Start node 2 after 2s delay
    // Verify leader election works
    // Verify log replication across all nodes
}

// Test rolling deployment scenario
func TestRollingDeployment(t *testing.T) {
    // Start all nodes
    // Stop node 1
    // Verify cluster still works (has quorum)
    // Restart node 1
    // Verify node 1 catches up
}

// Test network partition handling
func TestNetworkPartition(t *testing.T) {
    // Start 5 nodes
    // Partition into {0,1} and {2,3,4}
    // Verify partition with 3 nodes elects leader
    // Verify partition with 2 nodes doesn't make progress
    // Heal partition
    // Verify cluster converges
}

// Test peer recovery
func TestPeerRecovery(t *testing.T) {
    // Start 3 nodes
    // Kill node 2 (hard stop)
    // Send writes to cluster
    // Restart node 2
    // Verify node 2 catches up via log replication or snapshot
}
```

### Manual Testing Checklist

- [ ] Deploy 3 nodes via Docker Compose
- [ ] Verify all nodes connect to each other
- [ ] Verify leader election occurs
- [ ] Send write requests, verify replication
- [ ] Kill one node, verify cluster continues
- [ ] Restart killed node, verify it rejoins
- [ ] Kill leader, verify new leader elected
- [ ] Network partition test (block traffic between nodes)
- [ ] Monitor logs for connection/reconnection messages
- [ ] Test with 5 nodes for more complex scenarios

---

## Key Benefits

1. **Non-Blocking Startup**: Nodes start immediately, don't wait for peers
2. **Cluster Size Awareness**: Maintains `totalNodes` for correct quorum calculations
3. **Automatic Reconnection**: Background processes handle failures transparently
4. **Configurable**: All parameters adjustable via environment variables
5. **Deployment Flexible**: Works with Docker, Kubernetes, bare metal
6. **Resilient**: Handles network partitions, rolling updates, peer failures gracefully
7. **Production Ready**: Proper error handling, logging, and monitoring hooks

---

## Common Issues & Solutions

### Issue: Nodes can't connect on startup
**Cause**: Nodes starting simultaneously, none listening yet
**Solution**: Connection manager retries automatically, cluster forms within seconds

### Issue: Split brain scenario
**Cause**: Network partition with nodes in both partitions
**Solution**: Raft's quorum mechanism prevents split brain (only partition with majority can elect leader)

### Issue: Slow reconnection after failure
**Cause**: Backoff settings too conservative
**Solution**: Adjust `RAFT_MAX_BACKOFF` to smaller value (e.g., 10s instead of 30s)

### Issue: Connection flapping (repeated connect/disconnect)
**Cause**: Health check interval too aggressive
**Solution**: Increase `RAFT_HEALTH_CHECK_INTERVAL` (e.g., 30s instead of 10s)

### Issue: Node never rejoins cluster after restart
**Cause**: Incorrect RAFT_ADDRS or firewall blocking
**Solution**: Verify addresses are correct and reachable, check firewall rules

---

## Performance Considerations

### Network Overhead
- Each node maintains N-1 gRPC connections (for N total nodes)
- Health checks every 10s per peer (low overhead)
- Retry attempts with exponential backoff (minimizes flood)

### Memory Usage
- Connection manager: ~100KB per peer
- gRPC connections: ~50KB per connection
- Total overhead: < 1MB for typical 3-5 node cluster

### Latency Impact
- First RPC to peer: may incur connection establishment (~5-50ms)
- Subsequent RPCs: normal gRPC latency (< 1ms local, varies for WAN)
- Retry delays: controlled by backoff settings

### Tuning Recommendations

**Low Latency Network (datacenter)**:
```
RAFT_INITIAL_BACKOFF=100ms
RAFT_MAX_BACKOFF=5s
RAFT_CONN_TIMEOUT=2s
RAFT_HEALTH_CHECK_INTERVAL=5s
```

**High Latency Network (WAN)**:
```
RAFT_INITIAL_BACKOFF=2s
RAFT_MAX_BACKOFF=60s
RAFT_CONN_TIMEOUT=10s
RAFT_HEALTH_CHECK_INTERVAL=30s
```

---

## Future Enhancements

### Potential Improvements
1. **Dynamic Cluster Membership**: Add/remove nodes without restart
2. **TLS Support**: Secure communication between nodes
3. **Observability**: Prometheus metrics, Jaeger tracing
4. **Discovery Service**: Use etcd/Consul for peer discovery
5. **Connection Pooling**: Multiple connections per peer for parallelism
6. **Adaptive Timeouts**: Adjust timeouts based on observed latency

### Not in Scope (Current Plan)
- Authentication/authorization between nodes
- Encryption at rest for log files
- Advanced failure detection (phi accrual, SWIM)
- Multi-region deployment optimizations

---

## References

- [Raft Paper](https://raft.github.io/raft.pdf) - Original consensus algorithm
- [gRPC Go Documentation](https://grpc.io/docs/languages/go/) - gRPC client/server patterns
- [Go Context Package](https://pkg.go.dev/context) - Cancellation and timeouts
- [Kubernetes StatefulSets](https://kubernetes.io/docs/concepts/workloads/controllers/statefulset/) - Stateful application deployment

---

**Last Updated**: 2025-11-06
**Status**: Implementation plan ready for execution
