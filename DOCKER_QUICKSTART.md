# Docker Compose Quick Start Guide

Run a 3-node Raft cluster locally using Docker Compose.

## Prerequisites

- Docker Engine 20.10+
- Docker Compose 2.0+
- Ports 9000-9002 available

## Quick Start (3 Commands)

```bash
# 1. Start the cluster
./scripts/start-cluster.sh

# 2. Watch the logs
docker-compose logs -f

# 3. Stop when done
./scripts/stop-cluster.sh
```

## What You'll See

### 1. Nodes Start and Connect (0-5 seconds)
```
raft-node-0 | INFO Starting Raft node...
raft-node-1 | INFO Starting Raft node...
raft-node-2 | INFO Starting Raft node...
raft-node-0 | INFO Successfully connected to peer 1 after 1 attempts
raft-node-0 | INFO Successfully connected to peer 2 after 1 attempts
```

### 2. Leader Election (5-10 seconds)
```
raft-node-1 | INFO Starting election for term 1
raft-node-1 | INFO Node 1 became leader with 3/3 peers available
```

### 3. Continuous Operation
```
raft-node-1 | DEBUG Health check: all peers healthy
raft-node-0 | DEBUG Received heartbeat from leader 1
```

## Manual Commands

### Start Cluster
```bash
docker-compose up -d --build
```

### View Logs
```bash
# All nodes
docker-compose logs -f

# Specific node
docker-compose logs -f raft-node-0
docker-compose logs -f raft-node-1
docker-compose logs -f raft-node-2

# Last 50 lines
docker-compose logs --tail=50
```

### Check Status
```bash
docker-compose ps
```

### Stop Cluster
```bash
docker-compose down
```

### Clean All Data
```bash
./scripts/clean-data.sh
# Or manually:
docker-compose down -v
rm -rf data/
```

## Testing Scenarios

### Scenario 1: Network Partition (Minority Offline)

```bash
# Stop one node (cluster keeps working with 2/3)
docker-compose stop raft-node-2

# Watch logs - cluster should continue operating
docker-compose logs -f raft-node-0

# Restart the node
docker-compose start raft-node-2

# Watch it reconnect automatically
docker-compose logs -f raft-node-2
```

**Expected**: Cluster continues, node 2 reconnects and syncs.

### Scenario 2: Leader Failure

```bash
# Find current leader
docker-compose logs | grep "became leader" | tail -1

# Stop the leader (e.g., if it's node-1)
docker-compose stop raft-node-1

# Watch new election
docker-compose logs -f

# Restart old leader
docker-compose start raft-node-1
```

**Expected**: New leader elected within ~5 seconds, old leader becomes follower.

### Scenario 3: Full Cluster Restart

```bash
# Restart all nodes
docker-compose restart

# Watch re-election
docker-compose logs -f
```

**Expected**: New election, cluster recovers automatically.

### Scenario 4: Rolling Restart (Zero Downtime)

```bash
# Restart one at a time with delays
docker-compose restart raft-node-0 && sleep 10
docker-compose restart raft-node-1 && sleep 10
docker-compose restart raft-node-2
```

**Expected**: Cluster maintains quorum throughout, no election needed.

## Architecture

```
┌─────────────────┐      ┌─────────────────┐      ┌─────────────────┐
│  raft-node-0    │      │  raft-node-1    │      │  raft-node-2    │
│  :9000          │◄────►│  :9000          │◄────►│  :9000          │
│                 │      │  (Leader)       │      │                 │
│  Follower       │      │                 │      │  Follower       │
│                 │      │                 │      │                 │
│  ./data/node0/  │      │  ./data/node1/  │      │  ./data/node2/  │
└─────────────────┘      └─────────────────┘      └─────────────────┘
         │                        │                        │
         └────────────────────────┴────────────────────────┘
                    raft-network (Docker bridge)
```

## Configuration

Default settings in `docker-compose.yml`:

```yaml
TOTAL_NODES: "3"
RAFT_ADDRS: "raft-node-0:9000,raft-node-1:9000,raft-node-2:9000"
SNAPSHOT_THRESHOLD: "100"
RAFT_INITIAL_BACKOFF: "1s"
RAFT_MAX_BACKOFF: "30s"
RAFT_CONN_TIMEOUT: "5s"
RAFT_HEALTH_CHECK_INTERVAL: "10s"
```

### Customize Retry Behavior

For **faster reconnection** (good for testing):
```yaml
RAFT_INITIAL_BACKOFF: "500ms"
RAFT_MAX_BACKOFF: "5s"
RAFT_HEALTH_CHECK_INTERVAL: "3s"
```

For **production** (more conservative):
```yaml
RAFT_INITIAL_BACKOFF: "2s"
RAFT_MAX_BACKOFF: "60s"
RAFT_HEALTH_CHECK_INTERVAL: "30s"
```

## Troubleshooting

### Nodes Won't Connect

**Symptom**: Continuous "Failed to connect to peer X" messages

**Check**:
```bash
# Verify all containers running
docker-compose ps

# Check network
docker network inspect raft-cluster

# Restart cluster
docker-compose down && docker-compose up -d
```

### No Leader Elected

**Symptom**: All nodes stuck as followers

**Solutions**:
1. Check if majority (2/3) nodes are running: `docker-compose ps`
2. Look for election timeouts in logs: `docker-compose logs | grep election`
3. Restart cluster: `./scripts/clean-data.sh && ./scripts/start-cluster.sh`

### Port Already in Use

**Symptom**: `bind: address already in use`

**Solutions**:
```bash
# Find process using port
lsof -i :9000
lsof -i :9001
lsof -i :9002

# Kill the process or change ports in docker-compose.yml
# Change "9000:9000" to "9100:9000" etc.
```

### High CPU Usage

**Symptom**: Docker consuming high CPU

**Causes**:
1. Election storm (nodes repeatedly re-electing)
2. Network issues causing connection retries
3. Health check interval too aggressive

**Solutions**:
1. Check logs for continuous elections: `docker-compose logs | grep election`
2. Increase health check interval to 30s
3. Ensure stable network connection

### Data Corruption

**Symptom**: Nodes fail to start or panic on startup

**Solution**:
```bash
# Nuclear option - clean everything
./scripts/clean-data.sh
docker-compose up -d --build
```

## Next Steps

- **Add more nodes**: See `deploy/README.md` for 5+ node clusters
- **Kubernetes deployment**: See `docs/design/distributed-raft-implementation-plan.md`
- **Production setup**: Consider TLS, monitoring, backups
- **Performance tuning**: Adjust snapshot threshold, health check intervals

## Files Created

```
distributed-cache/
├── docker-compose.yml           # Multi-node cluster config
├── cmd/raftnode/main.go         # Application entry point
├── deploy/
│   ├── Dockerfile               # Container image
│   └── README.md                # Detailed deployment guide
├── scripts/
│   ├── start-cluster.sh         # Quick start helper
│   ├── stop-cluster.sh          # Quick stop helper
│   └── clean-data.sh            # Data cleanup helper
└── data/                        # Persistent storage (created on first run)
    ├── node0/
    ├── node1/
    └── node2/
```

## Support

- **Full Implementation Plan**: `docs/design/distributed-raft-implementation-plan.md`
- **Deployment Guide**: `deploy/README.md`
- **Issues**: Check logs with `docker-compose logs -f`
