# Distributed Cache - Run Guide

## Quick Start with Primary-Backup Replication

This guide shows how to run the distributed cache system with the newly implemented Synchronous Primary-Backup replication.

## Prerequisites

### Required Software
- Go 1.21 or higher
- Protocol Buffers compiler (protoc)
- curl or httpie for testing

### Install Dependencies
```bash
# Install Go dependencies
go mod download

# Verify protoc is installed
protoc --version

# If protoc is missing, install it:
# macOS:
brew install protobuf

# Linux:
apt-get install protobuf-compiler

# Windows:
# Download from https://github.com/protocolbuffers/protobuf/releases
```

## Building the Application

```bash
# Build the raftnode binary
go build -o raftnode ./cmd/raftnode

# Or build and install
go install ./cmd/raftnode

# Verify build
./raftnode --help
```

## Architecture Overview

```
┌─────────────────────────────────────────────────────────┐
│                    CONTROL PLANE                        │
│              3-Node Raft Cluster                        │
│   Node 0        Node 1        Node 2                    │
│  :9000         :9001          :9002  (gRPC)             │
│  Leader    ←→  Follower   ←→  Follower                  │
│  Manages partition table topology                       │
└─────────────────────────────────────────────────────────┘
                         ↓
┌─────────────────────────────────────────────────────────┐
│                     DATA PLANE                          │
│              Worker Nodes with Replication              │
│   Node 0        Node 1        Node 2                    │
│  :10000        :10001         :10002  (HTTP)            │
│                                                          │
│  Partition 0: Primary=0, Backup=1                       │
│  Partition 1: Primary=1, Backup=2                       │
│  Partition 2: Primary=2, Backup=0                       │
│  ... (16,384 partitions total)                          │
└─────────────────────────────────────────────────────────┘
```

## Running a 3-Node Cluster

### Option 1: Using Separate Terminals (Recommended for Development)

**Terminal 1 - Node 0 (Leader candidate):**
```bash
export RAFT_ID=0
export RAFT_ADDRS="localhost:9000,localhost:9001,localhost:9002"
export RAFT_DATA_DIR="./data/node0"
export API_ADDR=":10000"
mkdir -p $RAFT_DATA_DIR

./raftnode
```

**Terminal 2 - Node 1:**
```bash
export RAFT_ID=1
export RAFT_ADDRS="localhost:9000,localhost:9001,localhost:9002"
export RAFT_DATA_DIR="./data/node1"
export API_ADDR=":10001"
mkdir -p $RAFT_DATA_DIR

./raftnode
```

**Terminal 3 - Node 2:**
```bash
export RAFT_ID=2
export RAFT_ADDRS="localhost:9000,localhost:9001,localhost:9002"
export RAFT_DATA_DIR="./data/node2"
export API_ADDR=":10002"
mkdir -p $RAFT_DATA_DIR

./raftnode
```

### Option 2: Using Background Processes

```bash
# Create data directories
mkdir -p data/node{0,1,2}

# Start Node 0
RAFT_ID=0 RAFT_ADDRS="localhost:9000,localhost:9001,localhost:9002" \
RAFT_DATA_DIR="./data/node0" API_ADDR=":10000" \
./raftnode > logs/node0.log 2>&1 &

# Start Node 1
RAFT_ID=1 RAFT_ADDRS="localhost:9000,localhost:9001,localhost:9002" \
RAFT_DATA_DIR="./data/node1" API_ADDR=":10001" \
./raftnode > logs/node1.log 2>&1 &

# Start Node 2
RAFT_ID=2 RAFT_ADDRS="localhost:9000,localhost:9001,localhost:9002" \
RAFT_DATA_DIR="./data/node2" API_ADDR=":10002" \
./raftnode > logs/node2.log 2>&1 &

# Check processes
ps aux | grep raftnode

# Tail logs
tail -f logs/node0.log
```

### Option 3: Using Docker Compose (if docker-compose.yml exists)

```bash
# Start all nodes
docker-compose up -d

# View logs
docker-compose logs -f

# Stop cluster
docker-compose down
```

## Verifying Cluster is Running

### Check Health Endpoints
```bash
# Check all nodes are healthy
curl http://localhost:10000/health
curl http://localhost:10001/health
curl http://localhost:10002/health

# Expected response from each:
# {"status":"healthy"}
```

### Check Node Status
```bash
# Check node 0 status
curl http://localhost:10000/status

# Expected response:
# {
#   "node_id": 0,
#   "role": "leader",
#   "term": 1,
#   "leader_id": 0,
#   "total_nodes": 3
# }
```

### Find the Leader
```bash
# Check leader status on each node
curl http://localhost:10000/leader
curl http://localhost:10001/leader
curl http://localhost:10002/leader

# Expected response from leader:
# {"is_leader":true,"leader_id":0}

# Expected response from follower:
# {"is_leader":false,"leader_id":0}
```

## Testing Primary-Backup Replication

### 1. Basic PUT Operation
```bash
# Write a key-value pair (key=100, value=42)
curl -X POST http://localhost:10000/kv \
  -H "Content-Type: application/json" \
  -d '{"key": 100, "value": 42}'

# Expected response:
# {
#   "success": true,
#   "message": "Key-value pair stored successfully"
# }

# Check logs to verify replication:
# Node logs should show:
# "Successfully replicated PUT key=100 to backup node X"
```

### 2. Verify Data on Both Primary and Backup
```bash
# GET from primary
curl http://localhost:10000/kv/100

# GET from backup (may need to find which node is backup)
# The partition table assigns backups round-robin
# For key 100, check nodes to find which has it

# Expected response:
# {
#   "key": 100,
#   "value": 42,
#   "found": true
# }
```

### 3. Test DELETE Operation
```bash
# Delete a key
curl -X DELETE http://localhost:10000/kv/100

# Expected response:
# {
#   "success": true,
#   "message": "Key deleted successfully"
# }

# Check logs to verify replication:
# "Successfully replicated DELETE key=100 to backup node X"
```

### 4. Test Wrong Node Redirect
```bash
# Try to write to a node that doesn't own the key
# The system will return a 307 redirect

curl -i -X POST http://localhost:10001/kv \
  -H "Content-Type: application/json" \
  -d '{"key": 100, "value": 999}'

# Expected response (if node 1 doesn't own key 100):
# HTTP/1.1 307 Temporary Redirect
# Location: http://localhost:10000/kv
# {
#   "error": "MOVED",
#   "message": "Key belongs to node 0",
#   "node_id": "0",
#   "address": "http://localhost:10000",
#   "partition_id": 1234
# }
```

### 5. Test Replication Failure (Strong Consistency)
```bash
# Stop the backup node (e.g., node 1)
kill $(pgrep -f "RAFT_ID=1")

# Try to write a key that has node 1 as backup
# The write should FAIL with 503
curl -X POST http://localhost:10000/kv \
  -H "Content-Type: application/json" \
  -d '{"key": 200, "value": 99}'

# Expected response:
# HTTP/1.1 503 Service Unavailable
# Replication failed: connection refused

# Restart node 1 to restore normal operation
RAFT_ID=1 RAFT_ADDRS="localhost:9000,localhost:9001,localhost:9002" \
RAFT_DATA_DIR="./data/node1" API_ADDR=":10001" \
./raftnode > logs/node1.log 2>&1 &
```

### 6. Test Circuit Breaker
```bash
# After 3 consecutive replication failures, circuit breaker opens
# Try 4 writes in rapid succession while backup is down:

for i in {1..4}; do
  curl -X POST http://localhost:10000/kv \
    -H "Content-Type: application/json" \
    -d "{\"key\": $((300+i)), \"value\": $i}"
  echo ""
done

# First 3 will fail with "replication failed"
# 4th will fail with "circuit breaker is open"
```

## Comprehensive Testing Script

Create a file `test-replication.sh`:

```bash
#!/bin/bash

set -e

BASE_URL="http://localhost:10000"

echo "=== Testing Primary-Backup Replication ==="

# Test 1: Health check
echo -e "\n[1] Health Check"
curl -s $BASE_URL/health | jq .

# Test 2: Node status
echo -e "\n[2] Node Status"
curl -s $BASE_URL/status | jq .

# Test 3: Write data
echo -e "\n[3] Writing key=1000, value=777"
curl -s -X POST $BASE_URL/kv \
  -H "Content-Type: application/json" \
  -d '{"key": 1000, "value": 777}' | jq .

# Test 4: Read data
echo -e "\n[4] Reading key=1000"
curl -s $BASE_URL/kv/1000 | jq .

# Test 5: Update data
echo -e "\n[5] Updating key=1000, value=888"
curl -s -X POST $BASE_URL/kv \
  -H "Content-Type: application/json" \
  -d '{"key": 1000, "value": 888}' | jq .

# Test 6: Read updated data
echo -e "\n[6] Reading updated key=1000"
curl -s $BASE_URL/kv/1000 | jq .

# Test 7: Delete data
echo -e "\n[7] Deleting key=1000"
curl -s -X DELETE $BASE_URL/kv/1000 | jq .

# Test 8: Read deleted data (should return found=false or 0 value)
echo -e "\n[8] Reading deleted key=1000"
curl -s $BASE_URL/kv/1000 | jq .

# Test 9: Batch writes
echo -e "\n[9] Batch writing 10 keys"
for i in {1..10}; do
  curl -s -X POST $BASE_URL/kv \
    -H "Content-Type: application/json" \
    -d "{\"key\": $((2000+i)), \"value\": $((i*10))}" > /dev/null
  echo "  Written key=$((2000+i)), value=$((i*10))"
done

# Test 10: Batch reads
echo -e "\n[10] Batch reading 10 keys"
for i in {1..10}; do
  result=$(curl -s $BASE_URL/kv/$((2000+i)) | jq -r '.value')
  echo "  Read key=$((2000+i)), value=$result"
done

echo -e "\n=== All tests completed ==="
```

Run it:
```bash
chmod +x test-replication.sh
./test-replication.sh
```

## Monitoring and Debugging

### View Real-Time Logs
```bash
# Node 0 logs
tail -f logs/node0.log

# Filter for replication events
tail -f logs/node0.log | grep -i "replicate"

# Filter for errors
tail -f logs/node0.log | grep -i "error"
```

### Check Partition Distribution
```bash
# Create a script to see partition distribution
cat > check-partitions.sh << 'EOF'
#!/bin/bash
for key in {0..100}; do
  result=$(curl -s -X POST http://localhost:10000/kv \
    -H "Content-Type: application/json" \
    -d "{\"key\": $key, \"value\": 1}" 2>&1)

  if echo "$result" | grep -q "MOVED"; then
    node=$(echo "$result" | jq -r '.node_id')
    echo "Key $key -> Node $node"
  else
    echo "Key $key -> Node 0 (local)"
  fi
done
EOF

chmod +x check-partitions.sh
./check-partitions.sh
```

### Check Raft State
```bash
# Check Raft log files
ls -lh data/node0/
cat data/node0/raft.log  # If using file-based persistence
```

### Performance Testing
```bash
# Install Apache Bench (if not already installed)
# brew install apache2  # macOS
# apt-get install apache2-utils  # Linux

# Benchmark PUT operations
ab -n 1000 -c 10 -p data.json -T application/json \
  http://localhost:10000/kv

# Where data.json contains:
echo '{"key": 5000, "value": 123}' > data.json

# Benchmark GET operations
ab -n 1000 -c 10 http://localhost:10000/kv/5000
```

## Cleanup

### Stop All Nodes
```bash
# If using background processes
pkill -f raftnode

# Or kill specific nodes
kill $(pgrep -f "RAFT_ID=0")
kill $(pgrep -f "RAFT_ID=1")
kill $(pgrep -f "RAFT_ID=2")
```

### Clean Data and Logs
```bash
# Remove all data directories
rm -rf data/

# Remove log files
rm -rf logs/

# Start fresh
mkdir -p data/node{0,1,2} logs/
```

## Troubleshooting

### Node Won't Start
```bash
# Check if port is already in use
lsof -i :9000  # gRPC port
lsof -i :10000  # HTTP API port

# Kill process using the port
kill -9 $(lsof -t -i :9000)

# Check data directory permissions
ls -la data/node0/
```

### No Leader Elected
```bash
# Check if all nodes are running
curl http://localhost:10000/status
curl http://localhost:10001/status
curl http://localhost:10002/status

# Check logs for election messages
grep -i "election" logs/node*.log

# Restart all nodes if needed
pkill -f raftnode
# Then restart all nodes
```

### Replication Failures
```bash
# Check backup node is running
curl http://localhost:10001/health

# Check circuit breaker status in logs
grep -i "circuit breaker" logs/node0.log

# Check network connectivity
nc -zv localhost 9001  # gRPC port of backup
```

### Data Inconsistency
```bash
# Read from multiple nodes
curl http://localhost:10000/kv/100
curl http://localhost:10001/kv/100
curl http://localhost:10002/kv/100

# Check Raft commit logs
grep -i "commit" logs/node*.log

# Verify partition assignment
# (requires admin endpoint to query partition table)
```

## Configuration Reference

### Environment Variables

| Variable | Description | Example | Default |
|----------|-------------|---------|---------|
| `RAFT_ID` | Node ID (0-based) | `0`, `1`, `2` | Required |
| `RAFT_ADDRS` | Comma-separated gRPC addresses | `localhost:9000,localhost:9001,localhost:9002` | Required |
| `RAFT_DATA_DIR` | Data persistence directory | `./data/node0` | Required |
| `API_ADDR` | HTTP API listen address | `:10000` | `:8080` |

### Port Mapping
- **gRPC Ports**: 9000, 9001, 9002 (Raft communication)
- **HTTP API Ports**: 10000, 10001, 10002 (Client requests)
- **Rule**: HTTP port = gRPC port + 1000

## Advanced Usage

### Running with Different Node Counts

**5-Node Cluster:**
```bash
export RAFT_ADDRS="localhost:9000,localhost:9001,localhost:9002,localhost:9003,localhost:9004"

# Start 5 nodes with RAFT_ID=0,1,2,3,4
# API ports: 10000,10001,10002,10003,10004
```

**Single-Node (Testing Only):**
```bash
export RAFT_ID=0
export RAFT_ADDRS="localhost:9000"
export RAFT_DATA_DIR="./data/node0"
export API_ADDR=":10000"

./raftnode
# Note: Backup replication won't work with single node
```

### Production Deployment Considerations

1. **Use Persistent Volumes**: Mount `RAFT_DATA_DIR` to persistent storage
2. **Set Proper Log Levels**: Configure slog level in code or via flag
3. **Enable Metrics**: Add Prometheus endpoints for monitoring
4. **Use Load Balancer**: Place HTTP API behind load balancer
5. **Configure Firewalls**: Allow gRPC and HTTP ports between nodes
6. **Set Resource Limits**: Use Docker/K8s resource limits
7. **Enable TLS**: Add TLS for gRPC and HTTPS for API

## Next Steps

1. **Load Testing**: Run comprehensive load tests with realistic workloads
2. **Failure Testing**: Test various failure scenarios (node crashes, network partitions)
3. **Monitoring Setup**: Add Prometheus metrics and Grafana dashboards
4. **Client Libraries**: Create client SDKs with automatic redirect handling
5. **Admin Tools**: Build CLI tools for cluster management

## Support

For issues and questions:
- Check logs first: `tail -f logs/node*.log`
- Review implementation summary: `claudedocs/implementation-summary.md`
- Read original plan: `claudedocs/worker-primary-backup-plan.md`
