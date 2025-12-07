#!/bin/bash
# Script to run on computer 192.168.1.100
# Components: Controller 0, DataNode 1, DataNode 2

set -e

# Build binaries if needed
echo "Building binaries..."
go build -o controller ./cmd/controller
go build -o datanode ./cmd/datanode
go build -o raftcli ./cmd/raftcli

# Create data directories
mkdir -p ./data/node0
mkdir -p ./data/datanode1
mkdir -p ./data/datanode2

# Controller addresses (all 3 controllers)
RAFT_ADDRS="192.168.1.100:9000,192.168.1.101:9000,192.168.1.101:9001"

echo "========================================"
echo " Starting Controller 0 on 192.168.1.100"
echo "========================================"

# Start Controller 0
RAFT_ID=0 \
TOTAL_NODES=3 \
RAFT_ADDRS="$RAFT_ADDRS" \
API_ADDR=":8080" \
FILENAME="./data/node0" \
./controller &

CONTROLLER0_PID=$!
echo "Controller 0 started (PID: $CONTROLLER0_PID)"

# Wait for controller to initialize
sleep 3

echo "========================================"
echo " Starting DataNodes on 192.168.1.100"
echo "========================================"

# Start DataNode 1
CONTROLLER_URL="http://192.168.1.100:8080" \
NODE_ID="192.168.1.100:9100" \
LEASE_DURATION="5s" \
./datanode &

DATANODE1_PID=$!
echo "DataNode 1 started (PID: $DATANODE1_PID) - Node ID: 192.168.1.100:9100"

# Start DataNode 2
CONTROLLER_URL="http://192.168.1.100:8080" \
NODE_ID="192.168.1.100:9101" \
LEASE_DURATION="5s" \
./datanode &

DATANODE2_PID=$!
echo "DataNode 2 started (PID: $DATANODE2_PID) - Node ID: 192.168.1.100:9101"

echo ""
echo "========================================"
echo " All components started on 192.168.1.100"
echo "========================================"
echo "Controller 0: PID $CONTROLLER0_PID (Raft: :9000, API: :8080)"
echo "DataNode 1:   PID $DATANODE1_PID (192.168.1.100:9100)"
echo "DataNode 2:   PID $DATANODE2_PID (192.168.1.100:9101)"
echo ""
echo "To connect with raftcli:"
echo "./raftcli 192.168.1.100:8080,192.168.1.101:8080,192.168.1.101:8081"
echo ""
echo "Press Ctrl+C to stop all processes..."

# Handle graceful shutdown
cleanup() {
    echo ""
    echo "Shutting down..."
    kill $CONTROLLER0_PID $DATANODE1_PID $DATANODE2_PID 2>/dev/null || true
    exit 0
}

trap cleanup SIGINT SIGTERM

# Keep script running
wait
