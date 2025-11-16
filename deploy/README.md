# Distributed Raft Deployment Guide

This directory contains deployment configurations for running Raft across multiple machines.

## Quick Start with Docker Compose

### Prerequisites
- Docker and Docker Compose installed
- Ports 9000-9002 available on your machine

### Start the Cluster

From the project root directory:

```bash
# Build and start all 3 nodes
docker-compose up --build

# Or run in background
docker-compose up -d --build
```

### Monitor Logs

```bash
# View all nodes
docker-compose logs -f

# View specific node
docker-compose logs -f raft-node-0
docker-compose logs -f raft-node-1
docker-compose logs -f raft-node-2
```

### Stop the Cluster

```bash
# Stop all nodes
docker-compose down

# Stop and remove data
docker-compose down -v
rm -rf data/
```

## What to Expect

When you start the cluster, you should see:

1. **Connection Attempts**: Nodes will try to connect to each other
   ```
   INFO Successfully connected to peer 1 after 1 attempts
   INFO Successfully connected to peer 2 after 1 attempts
   ```

2. **Leader Election**: Within a few seconds, one node will become leader
   ```
   INFO Starting election for term 1
   INFO Node 1 became leader with 2/3 peers available (need 2 for quorum)
   ```

3. **Health Monitoring**: Every 10 seconds, nodes check connection health
   ```
   DEBUG Health check: peer 0=healthy, peer 2=healthy
   ```

## Cluster Configuration

The default setup runs 3 nodes:

| Node | Container Name | Port | Data Directory |
|------|---------------|------|----------------|
| 0 | raft-node-0 | 9000 | ./data/node0 |
| 1 | raft-node-1 | 9001 | ./data/node1 |
| 2 | raft-node-2 | 9002 | ./data/node2 |

## Customization

### Change Retry Behavior

Edit `docker-compose.yml` environment variables:

```yaml
RAFT_INITIAL_BACKOFF: "500ms"      # Faster retry
RAFT_MAX_BACKOFF: "10s"            # Lower max backoff
RAFT_HEALTH_CHECK_INTERVAL: "5s"   # More frequent checks
```

### Add More Nodes

1. Copy a node service in `docker-compose.yml`
2. Update: `RAFT_ID`, container name, port, data volume
3. Update `TOTAL_NODES` and `RAFT_ADDRS` in **all** nodes
4. Example for 5 nodes:
   ```yaml
   TOTAL_NODES: "5"
   RAFT_ADDRS: "raft-node-0:9000,raft-node-1:9000,raft-node-2:9000,raft-node-3:9000,raft-node-4:9000"
   ```

## Testing Scenarios

### Test Network Partition

```bash
# Stop one node
docker-compose stop raft-node-2

# Cluster should continue with 2/3 nodes
docker-compose logs -f raft-node-0

# Restart the node
docker-compose start raft-node-2

# Node should reconnect automatically
```

### Test Leader Failure

```bash
# Find the leader (check logs for "became leader")
docker-compose logs | grep "became leader"

# Stop the leader
docker-compose stop raft-node-<leader-id>

# Watch new election in remaining nodes
docker-compose logs -f
```

### Test Rolling Restart

```bash
# Restart nodes one at a time
docker-compose restart raft-node-0
sleep 5
docker-compose restart raft-node-1
sleep 5
docker-compose restart raft-node-2
```

## Troubleshooting

### Nodes can't connect

**Symptom**: `Failed to connect to peer X` continuously

**Solutions**:
1. Check all nodes are running: `docker-compose ps`
2. Verify network: `docker network inspect raft-cluster`
3. Check firewall isn't blocking internal Docker network
4. Ensure `RAFT_ADDRS` matches container names exactly

### No leader elected

**Symptom**: All nodes remain followers

**Solutions**:
1. Check logs for vote request failures
2. Verify quorum: need (N+1)/2 nodes available
3. Ensure clocks are relatively synchronized
4. Check for network partition between nodes

### Connection flapping

**Symptom**: Repeated connect/disconnect messages

**Solutions**:
1. Increase `RAFT_HEALTH_CHECK_INTERVAL` (e.g., `30s`)
2. Increase `RAFT_MAX_BACKOFF` (e.g., `60s`)
3. Check for DNS resolution issues
4. Verify resource constraints (CPU/memory)

### Data corruption

**Symptom**: Nodes fail to start or consensus breaks

**Solutions**:
1. Stop all nodes: `docker-compose down`
2. Remove corrupted data: `rm -rf data/`
3. Restart cluster: `docker-compose up -d`

## Production Deployment

For production deployment, see:
- `docs/design/distributed-raft-implementation-plan.md` - Full implementation details
- Kubernetes deployment example in the plan document
- Security considerations (TLS, authentication)

## Environment Variables Reference

| Variable | Default | Description |
|----------|---------|-------------|
| `RAFT_ID` | required | Unique node ID (0 to N-1) |
| `TOTAL_NODES` | required | Total cluster size |
| `RAFT_ADDRS` | required | Comma-separated list of all node addresses |
| `FILENAME` | required | Base path for data files |
| `SNAPSHOT_THRESHOLD` | required | Number of log entries before snapshot |
| `RAFT_INITIAL_BACKOFF` | `1s` | Initial connection retry delay |
| `RAFT_MAX_BACKOFF` | `30s` | Maximum retry delay |
| `RAFT_BACKOFF_MULTIPLIER` | `2.0` | Backoff growth rate |
| `RAFT_MAX_RETRIES` | `-1` | Max retry attempts (-1 = infinite) |
| `RAFT_CONN_TIMEOUT` | `5s` | Connection establishment timeout |
| `RAFT_HEALTH_CHECK_INTERVAL` | `10s` | Health monitoring frequency |
