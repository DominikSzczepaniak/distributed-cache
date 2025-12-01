# Local Cluster Scripts

Comprehensive set of scripts for running the distributed cache cluster locally (non-Docker).

## Quick Start

```bash
# Start the cluster
./scripts/local/start-local.sh

# Check cluster status
./scripts/local/status-local.sh

# Run functionality tests
./scripts/local/test-local.sh

# Stop the cluster
./scripts/local/stop-local.sh

# Clean all data (fresh start)
./scripts/local/clean-local.sh
```

## Scripts Overview

### start-local.sh

**Purpose**: Start a local distributed cache cluster

**Features**:
- Builds raftnode binary
- Cleans old data and processes
- Starts N nodes (default: 3) with proper environment configuration
- Waits for cluster health and auto-initialization
- Verifies partition table is initialized (16384 partitions)
- Shows cluster status and helpful usage info

**Configuration** (via environment variables):
```bash
TOTAL_NODES=3           # Number of nodes (default: 3)
BASE_RAFT_PORT=9000     # Starting Raft port (default: 9000)
BASE_API_PORT=10000     # Starting API port (default: 10000)
STARTUP_WAIT=3          # Initial wait time (default: 3s)
HEALTH_TIMEOUT=30       # Max wait for health (default: 30s)
```

**Example**:
```bash
# Start with defaults (3 nodes)
./scripts/local/start-local.sh

# Start with 5 nodes
TOTAL_NODES=5 ./scripts/local/start-local.sh
```

**Output**:
- Color-coded progress indicators
- Health check verification
- Leader election confirmation
- Partition table initialization status
- Node endpoints and quick commands

**What happens**:
1. Builds the raftnode binary
2. Stops any running processes
3. Cleans old data directories
4. Starts N nodes with:
   - Raft ports: 9000, 9001, 9002
   - API ports: 10000, 10001, 10002
   - Data dirs: data/node0, data/node1, data/node2
   - Logs: data/nodeX/node.log
5. Waits for all nodes to respond on health endpoints
6. Verifies leader election
7. Confirms partition table auto-initialization (16384 partitions)

**No manual initialization needed**: The cluster now auto-initializes the partition table via the leader.

---

### stop-local.sh

**Purpose**: Stop all local distributed cache nodes

**Features**:
- Gracefully stops all raftnode processes
- Uses PID files for clean shutdown
- Fallback to pkill if needed
- Cleans up PID files
- Shows what was stopped

**Example**:
```bash
./scripts/local/stop-local.sh
```

**What happens**:
1. Checks for running raftnode processes
2. Attempts graceful shutdown using saved PIDs
3. Falls back to pkill if needed (TERM, then KILL)
4. Cleans up PID files
5. Preserves data directories (use clean-local.sh to remove)

---

### status-local.sh

**Purpose**: Check distributed cache cluster status

**Features**:
- Shows running processes and PIDs
- Checks node health endpoints
- Displays partition table status
- Shows leader information
- Shows partition distribution across nodes
- Displays recent log summaries

**Example**:
```bash
./scripts/local/status-local.sh
```

**What it shows**:
- Process status (PID, running/stopped)
- Node health (OK/Failed)
- Leader/Follower role
- Current Raft term
- Total partitions (expected: 16384)
- Partition distribution per node
- Partition table version
- Last 3 log entries per node (with error highlighting)
- Quick action suggestions

---

### clean-local.sh

**Purpose**: Clean all local distributed cache data

**Features**:
- Stops all nodes first (if running)
- Removes all data directories
- Cleans log files and PID files
- Optional: remove binaries with `--binaries` flag
- Provides fresh slate for restart

**Example**:
```bash
# Clean data only
./scripts/local/clean-local.sh

# Clean data AND binaries
./scripts/local/clean-local.sh --binaries
```

**What happens**:
1. Stops any running processes
2. Removes data/node0, data/node1, data/node2 directories
3. Cleans PID files
4. Optionally removes raftnode and raftcli binaries

**Use cases**:
- Fresh cluster restart
- Testing from clean state
- Removing all local data before pull/rebase

---

### test-local.sh

**Purpose**: Quick functionality test for local cluster

**Features**:
- Waits for cluster to be ready
- Tests PUT operation
- Tests GET operation
- Tests DELETE operation
- Tests multiple nodes
- Color-coded results

**Example**:
```bash
./scripts/local/test-local.sh
```

**Tests performed**:
1. **Cluster Health**: Verifies all nodes healthy
2. **Partition Table**: Verifies 16384 partitions initialized
3. **PUT Test**: Writes key-value pair
4. **GET Test**: Reads value back
5. **DELETE Test**: Deletes key
6. **GET Deleted**: Verifies deletion
7. **Multi-Node Test**: Tests all nodes independently

**Exit codes**:
- `0`: All tests passed
- `1`: One or more tests failed

---

## Typical Workflows

### Development Workflow

```bash
# Start cluster
./scripts/local/start-local.sh

# Make code changes...

# Test changes
./scripts/local/test-local.sh

# Check status
./scripts/local/status-local.sh

# View logs
tail -f data/node0/node.log

# Restart cluster
./scripts/local/stop-local.sh
./scripts/local/start-local.sh
```

### Fresh Start Workflow

```bash
# Complete clean slate
./scripts/local/clean-local.sh --binaries

# Rebuild and start
go build -o raftnode ./cmd/raftnode/main.go
./scripts/local/start-local.sh

# Verify
./scripts/local/test-local.sh
```

### Debugging Workflow

```bash
# Check status
./scripts/local/status-local.sh

# View logs
tail -100 data/node0/node.log
tail -100 data/node1/node.log
tail -100 data/node2/node.log

# Or follow live
tail -f data/node0/node.log

# Clean restart if needed
./scripts/local/stop-local.sh
./scripts/local/clean-local.sh
./scripts/local/start-local.sh
```

---

## Configuration

All scripts use environment variables for configuration:

| Variable | Default | Description |
|----------|---------|-------------|
| `TOTAL_NODES` | 3 | Number of nodes in cluster |
| `BASE_RAFT_PORT` | 9000 | Starting port for Raft (9000, 9001, 9002...) |
| `BASE_API_PORT` | 10000 | Starting port for API (10000, 10001, 10002...) |
| `STARTUP_WAIT` | 3 | Seconds to wait after process start |
| `HEALTH_TIMEOUT` | 30 | Max seconds to wait for cluster health |
| `HEALTH_INTERVAL` | 1 | Seconds between health checks |

### Example: 5-Node Cluster

```bash
# Start 5-node cluster
TOTAL_NODES=5 ./scripts/local/start-local.sh

# Nodes will use:
# - Raft ports: 9000-9004
# - API ports: 10000-10004
# - Data dirs: data/node0-4

# Other scripts auto-detect from running processes or use same env var
TOTAL_NODES=5 ./scripts/local/status-local.sh
TOTAL_NODES=5 ./scripts/local/test-local.sh
```

---

## Auto-Initialization

**Important**: Manual partition table initialization is NO LONGER NEEDED.

The cluster now automatically initializes the partition table when:
1. The leader is elected
2. The partition table is empty (first startup)
3. The leader creates an even distribution across all nodes
4. The partition table is replicated via Raft consensus

**What this means**:
- No manual `curl` commands needed
- Cluster is ready immediately after startup
- Scripts verify auto-initialization succeeded
- 16384 partitions distributed evenly across nodes

**Verification**:
```bash
# start-local.sh verifies:
# - Leader election occurred
# - Partition table has 16384 assignments
# - Distribution is complete

# status-local.sh shows:
# - Total partitions: 16384
# - Partitions per node
# - Partition table version
```

---

## Troubleshooting

### Cluster won't start

```bash
# Check for port conflicts
lsof -i :9000
lsof -i :10000

# Clean everything and retry
./scripts/local/clean-local.sh --binaries
go build -o raftnode ./cmd/raftnode/main.go
./scripts/local/start-local.sh
```

### Auto-initialization fails

```bash
# Check logs for leader election
grep -i "leader" data/node*/node.log

# Check logs for partition table
grep -i "partition" data/node*/node.log

# Manually verify leader
curl http://localhost:10000/admin/status
curl http://localhost:10001/admin/status
curl http://localhost:10002/admin/status

# Check partition table
curl http://localhost:10000/admin/partition-table | jq '.assignments | length'
```

### Tests fail

```bash
# Check cluster status first
./scripts/local/status-local.sh

# View logs
tail -50 data/node0/node.log

# Restart cluster
./scripts/local/stop-local.sh
./scripts/local/start-local.sh

# Retry tests
./scripts/local/test-local.sh
```

### Processes won't stop

```bash
# Force kill all
pkill -KILL -f "./raftnode"

# Clean PID files
rm -f data/node*/node.pid

# Verify stopped
pgrep -f "./raftnode"  # Should return nothing
```

---

## API Reference

### Node Endpoints

Each node exposes:

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/health` | GET | Health check (returns 200 OK) |
| `/kv` | POST | Store key-value pair (PUT) |
| `/kv/{key}` | GET | Retrieve value for key |
| `/kv/{key}` | DELETE | Delete key |
| `/status` | GET | Node status (role, term, leader, etc.) |
| `/leader` | GET | Leader information |

### Examples

```bash
# Health check
curl http://localhost:10000/health

# PUT (JSON body)
curl -X POST http://localhost:10000/kv \
  -H "Content-Type: application/json" \
  -d '{"key": 123, "value": 456}'

# GET
curl http://localhost:10000/kv/123

# DELETE
curl -X DELETE http://localhost:10000/kv/123

# Node status
curl http://localhost:10000/status | jq

# Leader info
curl http://localhost:10000/leader | jq
```

---

## File Structure

```
scripts/local/
├── README.md           # This file
├── start-local.sh      # Start cluster
├── stop-local.sh       # Stop cluster
├── status-local.sh     # Check status
├── clean-local.sh      # Clean data
└── test-local.sh       # Run tests

data/
├── node0/
│   ├── raft/          # Raft log and snapshots
│   ├── node.log       # Node logs
│   └── node.pid       # Process ID
├── node1/
│   └── ...
└── node2/
    └── ...

raftnode                # Compiled binary
raftcli                 # CLI binary
```

---

## Comparison with Docker Scripts

| Feature | Local Scripts | Docker Scripts |
|---------|---------------|----------------|
| Build process | `go build` | `docker build` |
| Process management | PIDs, pkill | Docker containers |
| Data persistence | data/nodeX/ | Docker volumes |
| Network | localhost ports | Docker network |
| Logs | data/nodeX/node.log | `docker logs` |
| Start time | ~5 seconds | ~10-15 seconds |
| Debugging | Direct log access | `docker exec`, logs |
| Resource usage | Lower | Higher (container overhead) |

**When to use local scripts**:
- Fast iteration during development
- Debugging with direct log access
- Lower resource usage
- No Docker dependencies

**When to use Docker**:
- Production-like environment
- Testing containerized deployment
- Network isolation testing
- Multi-host simulation

---

## Tips

1. **Always check status first**: Run `./scripts/local/status-local.sh` before debugging
2. **Use test script for verification**: Quick sanity check with `./scripts/local/test-local.sh`
3. **Clean data between tests**: Use `./scripts/local/clean-local.sh` for fresh state
4. **Monitor logs during development**: `tail -f data/node0/node.log`
5. **Set environment variables once**: Export them to avoid repetition
   ```bash
   export TOTAL_NODES=5
   ./scripts/local/start-local.sh
   ./scripts/local/test-local.sh
   ```

---

## Advanced Usage

### Custom Port Ranges

```bash
# Avoid port conflicts
BASE_RAFT_PORT=9100 BASE_API_PORT=11000 ./scripts/local/start-local.sh
```

### Different Node Counts

```bash
# Single node (testing)
TOTAL_NODES=1 ./scripts/local/start-local.sh

# 5-node cluster (production-like)
TOTAL_NODES=5 ./scripts/local/start-local.sh
```

### Shorter Timeouts (CI/CD)

```bash
# Faster startup for CI
STARTUP_WAIT=1 HEALTH_TIMEOUT=15 ./scripts/local/start-local.sh
```

### Integration with CI

```bash
#!/bin/bash
set -e

# Start cluster
./scripts/local/start-local.sh

# Run tests
./scripts/local/test-local.sh

# Additional integration tests
go test ./... -v

# Cleanup
./scripts/local/stop-local.sh
./scripts/local/clean-local.sh
```

---

## Support

For issues or questions:
1. Check logs: `data/nodeX/node.log`
2. Run status check: `./scripts/local/status-local.sh`
3. Try clean restart: `./scripts/local/clean-local.sh && ./scripts/local/start-local.sh`
4. Review troubleshooting section above
