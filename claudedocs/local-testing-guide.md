# Local Testing Guide - Complete Cluster with Workers

Quick guide for testing the Stage 3 implementation with Raft nodes + Worker nodes.

## Architecture

```
Control Plane (3 Raft Nodes):          Data Plane (3 Workers):
┌─────────────────────────┐            ┌─────────────────────────┐
│ Raft 0: :9000 → :8080   │            │ Worker 0: :7100 → :7000 │
│ Raft 1: :9001 → :8081   │────────────│ Worker 1: :7101 → :7001 │
│ Raft 2: :9002 → :8082   │  manages   │ Worker 2: :7102 → :7002 │
│                         │            │                         │
│ Stores: Partition Table │            │ Stores: Partitioned Data│
└─────────────────────────┘            └─────────────────────────┘
```

**Request Flow**:
```
Client → Raft Node (:8080)
         ↓ Lookup partition
         ↓ Find worker from registry
         ↓ Return HTTP 307 redirect
Client → Worker Node (:7000)
         ↓ Serve from local storage
         Response
```

## Quick Start

### 1. Start the Cluster

```bash
# Start 3 Raft nodes + 3 Workers
./scripts/local/start-cluster-with-workers.sh
```

**What it does**:
- Builds `bin/raftnode` and `bin/worker` binaries
- Cleans old data and processes
- Starts 3 Raft nodes (ports 8080-8082)
- Starts 3 Worker nodes (ports 7000-7002)
- Waits for health checks
- Shows cluster status and usage examples

**Expected output**:
```
=== Building Binaries ===
✓ Raft node binary built: bin/raftnode
✓ Worker binary built: bin/worker

=== Starting Raft Cluster (3 nodes) ===
✓ All Raft nodes started
✓ Raft cluster is healthy (3 nodes)

=== Starting Worker Nodes (3 workers) ===
✓ All worker nodes started
✓ All workers are healthy (3 workers)

=== Cluster Ready! 🚀 ===
```

### 2. Test the Routing

```bash
# Run comprehensive routing tests
./scripts/local/test-routing.sh
```

**What it tests**:
- PUT via Raft (expect HTTP 307 redirect)
- PUT with redirect following (end-to-end)
- GET via Raft with redirect
- Direct PUT to worker (no redirect)
- Multiple keys across different partitions
- DELETE operations

**Expected output**:
```
=== Test 1: PUT via Raft (without following redirect) ===
✓ Received HTTP 307 redirect
→ Redirect location: http://localhost:7000/kv

=== Test 2: PUT via Raft (following redirect) ===
✓ Successfully PUT key=123 via redirect

=== Test 3: GET via Raft (following redirect) ===
✓ Successfully GET key=123 via redirect
✓ Data matches expected values
```

### 3. Manual Testing

```bash
# PUT via Raft (see redirect)
curl -v -X POST http://localhost:8080/kv \
  -H 'Content-Type: application/json' \
  -d '{"key": 123, "value": 456}'

# Expected response:
# < HTTP/1.1 307 Temporary Redirect
# < Location: http://localhost:7000/kv
# {"message":"Request should be sent to worker node","worker_id":0,...}

# PUT via Raft (follow redirect automatically)
curl -L -X POST http://localhost:8080/kv \
  -H 'Content-Type: application/json' \
  -d '{"key": 123, "value": 456}'

# Expected response:
# HTTP/1.1 200 OK (from worker)
# {"message":"Key stored successfully","key":123,"value":456}

# GET via Raft (follow redirect)
curl -L http://localhost:8080/kv/123

# Expected response:
# {"key":123,"value":456}

# DELETE via Raft (follow redirect)
curl -L -X DELETE http://localhost:8080/kv/123

# Expected response:
# {"message":"Key deleted successfully","key":123}
```

### 4. Check Cluster Status

```bash
# Check Raft health
curl http://localhost:8080/health
curl http://localhost:8081/health
curl http://localhost:8082/health

# Check Worker health
curl http://localhost:7000/health
curl http://localhost:7001/health
curl http://localhost:7002/health

# Check Worker stats (partition count, operations)
curl http://localhost:7000/stats
```

### 5. View Logs

```bash
# Raft logs (partition table management, worker registration)
tail -f data/node0/node.log
tail -f data/node1/node.log
tail -f data/node2/node.log

# Worker logs (data operations, replication)
tail -f data/worker0/worker.log
tail -f data/worker1/worker.log
tail -f data/worker2/worker.log

# Watch all logs simultaneously
tail -f data/node*/node.log data/worker*/worker.log
```

### 6. Stop the Cluster

```bash
# Stop all nodes
./scripts/local/stop-cluster-with-workers.sh
```

**What it does**:
- Stops all Raft nodes
- Stops all Worker nodes
- Preserves logs for debugging
- Shows cleanup commands

### 7. Clean Data

```bash
# Remove all data (start fresh)
rm -rf data/
```

## Port Reference

| Component | Instance | gRPC Port | HTTP Port |
|-----------|----------|-----------|-----------|
| Raft Node | 0 | 9000 | 8080 |
| Raft Node | 1 | 9001 | 8081 |
| Raft Node | 2 | 9002 | 8082 |
| Worker | 0 | 7100 | 7000 |
| Worker | 1 | 7101 | 7001 |
| Worker | 2 | 7102 | 7002 |

## Configuration

Customize the cluster by setting environment variables:

```bash
# Start with more workers
TOTAL_WORKERS=6 ./scripts/local/start-cluster-with-workers.sh

# Change ports (if needed)
BASE_RAFT_PORT=9000 \
BASE_RAFT_API_PORT=8080 \
BASE_WORKER_GRPC_PORT=7100 \
BASE_WORKER_HTTP_PORT=7000 \
./scripts/local/start-cluster-with-workers.sh
```

## Troubleshooting

### Cluster won't start

**Symptom**: Health check timeouts

**Solutions**:
```bash
# Check if ports are already in use
lsof -i :8080 -i :8081 -i :8082 -i :7000 -i :7001 -i :7002

# Kill existing processes
pkill -f bin/raftnode
pkill -f bin/worker

# Clean data and restart
rm -rf data/
./scripts/local/start-cluster-with-workers.sh
```

### Workers not registering

**Symptom**: Workers start but don't register with Raft

**Solutions**:
```bash
# Check worker logs
tail -f data/worker0/worker.log

# Look for registration messages:
# "Registering with Raft cluster..."
# "Successfully registered with Raft"

# Check Raft logs
tail -f data/node0/node.log

# Look for:
# "Worker X registered successfully"
```

### Redirects not working

**Symptom**: Getting 200 instead of 307 from Raft

**Possible causes**:
1. Stage 3 not fully implemented
2. Worker registry not initialized
3. Workers not registered

**Check**:
```bash
# Verify workers are registered
curl http://localhost:8080/status

# Test redirect explicitly
curl -v -X POST http://localhost:8080/kv \
  -H 'Content-Type: application/json' \
  -d '{"key": 123, "value": 456}'

# Should see: HTTP/1.1 307 Temporary Redirect
```

### Data not persisting

**Symptom**: Data lost after restart

**Note**: This is expected in Stage 3. Workers store data in-memory only.

**For persistence**: Wait for production deployment with persistent volumes.

## Testing Scenarios

### Scenario 1: Basic CRUD Operations

```bash
# Create
curl -L -X POST http://localhost:8080/kv \
  -H 'Content-Type: application/json' \
  -d '{"key": 1, "value": 100}'

# Read
curl -L http://localhost:8080/kv/1

# Update
curl -L -X POST http://localhost:8080/kv \
  -H 'Content-Type: application/json' \
  -d '{"key": 1, "value": 200}'

# Delete
curl -L -X DELETE http://localhost:8080/kv/1
```

### Scenario 2: Multiple Keys (Partition Distribution)

```bash
# Insert keys that hash to different partitions
for i in {1..20}; do
  curl -L -X POST http://localhost:8080/kv \
    -H 'Content-Type: application/json' \
    -d "{\"key\": $i, \"value\": $((i * 10))}"
done

# Check worker stats to see partition distribution
curl http://localhost:7000/stats
curl http://localhost:7001/stats
curl http://localhost:7002/stats
```

### Scenario 3: Direct Worker Access

```bash
# Find which worker owns key=100
# (Use partition table or trial-and-error)

# Try Worker 0
curl -X POST http://localhost:7000/kv \
  -H 'Content-Type: application/json' \
  -d '{"key": 100, "value": 500}'

# If rejected (HTTP 400):
# "Partition X not owned by this worker"

# Try Worker 1
curl -X POST http://localhost:7001/kv \
  -H 'Content-Type: application/json' \
  -d '{"key": 100, "value": 500}'

# Success (HTTP 200)
```

### Scenario 4: Failure Simulation

```bash
# Kill a worker
pkill -f "WORKER_ID=0"

# Try accessing data on that worker
curl -L http://localhost:8080/kv/123

# Expected: HTTP 503 (Service Unavailable)

# Restart worker
WORKER_ID=0 HTTP_ADDR=:7000 GRPC_ADDR=:7100 \
RAFT_ADDRS="localhost:9000,localhost:9001,localhost:9002" \
./bin/worker &

# Wait for registration (~10s)
# Try again - should work
```

## Next Steps

After verifying Stage 3 works:

1. **Stage 4**: Remove data storage from Raft nodes
   - Raft becomes pure control plane
   - Complete architecture separation

2. **Production Deployment**:
   - Docker Compose with persistent volumes
   - Kubernetes manifests
   - Monitoring and observability

3. **Advanced Features**:
   - Dynamic worker registration/deregistration
   - Partition rebalancing
   - Backup node activation
   - Read replicas

## Quick Reference

```bash
# Start cluster
./scripts/local/start-cluster-with-workers.sh

# Test routing
./scripts/local/test-routing.sh

# Stop cluster
./scripts/local/stop-cluster-with-workers.sh

# Clean data
rm -rf data/

# View logs
tail -f data/node0/node.log data/worker0/worker.log
```
