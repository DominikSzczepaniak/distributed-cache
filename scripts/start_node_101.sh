#!/bin/bash
# Script to run on computer 192.168.1.101
# Components: Controller 1, Controller 2, DataNode 3, DataNode 4

set -e

# Build binaries if needed
echo "Building binaries..."
go build -o controller ./cmd/controller
go build -o datanode ./cmd/datanode
go build -o raftcli ./cmd/raftcli

# Create data directories
mkdir -p ./data/node1
mkdir -p ./data/node2
mkdir -p ./data/datanode3
mkdir -p ./data/datanode4

# Controller addresses (all 3 controllers)
# Controller 0: 192.168.1.100:9000
# Controller 1: 192.168.1.101:9000
# Controller 2: 192.168.1.101:9001
RAFT_ADDRS="192.168.1.100:9000,192.168.1.101:9000,192.168.1.101:9001"

echo "========================================"
echo " Starting Controller 1 on 192.168.1.101"
echo "========================================"

# Start Controller 1 (port 9000 for Raft, 8080 for API)
RAFT_ID=1 \
TOTAL_NODES=3 \
RAFT_ADDRS="$RAFT_ADDRS" \
API_ADDR=":8080" \
FILENAME="./data/node1" \
./controller &

CONTROLLER1_PID=$!
echo "Controller 1 started (PID: $CONTROLLER1_PID)"

echo "========================================"
echo " Starting Controller 2 on 192.168.1.101"
echo "========================================"

# Start Controller 2 (port 9001 for Raft, 8081 for API)
RAFT_ID=2 \
TOTAL_NODES=3 \
RAFT_ADDRS="$RAFT_ADDRS" \
API_ADDR=":8081" \
FILENAME="./data/node2" \
./controller &

CONTROLLER2_PID=$!
echo "Controller 2 started (PID: $CONTROLLER2_PID)"

# Wait for controllers to initialize
sleep 3

echo "========================================"
echo " Starting DataNodes on 192.168.1.101"
echo "========================================"

# Start DataNode 3
CONTROLLER_URL="http://192.168.1.101:8080" \
NODE_ID="192.168.1.101:9100" \
LEASE_DURATION="5s" \
./datanode &

DATANODE3_PID=$!
echo "DataNode 3 started (PID: $DATANODE3_PID) - Node ID: 192.168.1.101:9100"

# Start DataNode 4
CONTROLLER_URL="http://192.168.1.101:8081" \
NODE_ID="192.168.1.101:9101" \
LEASE_DURATION="5s" \
./datanode &

DATANODE4_PID=$!
echo "DataNode 4 started (PID: $DATANODE4_PID) - Node ID: 192.168.1.101:9101"

echo ""
echo "========================================"
echo " All components started on 192.168.1.101"
echo "========================================"
echo "Controller 1: PID $CONTROLLER1_PID (Raft: :9000, API: :8080)"
echo "Controller 2: PID $CONTROLLER2_PID (Raft: :9001, API: :8081)"
echo "DataNode 3:   PID $DATANODE3_PID (192.168.1.101:9100)"
echo "DataNode 4:   PID $DATANODE4_PID (192.168.1.101:9101)"
echo ""
echo "To connect with raftcli:"
echo "./raftcli 192.168.1.100:8080,192.168.1.101:8080,192.168.1.101:8081"
echo ""
echo "Press Ctrl+C to stop all processes..."

# Handle graceful shutdown
cleanup() {
    echo ""
    echo "Shutting down..."
    kill $CONTROLLER1_PID $CONTROLLER2_PID $DATANODE3_PID $DATANODE4_PID 2>/dev/null || true
    exit 0
}

trap cleanup SIGINT SIGTERM

# Keep script running
wait
