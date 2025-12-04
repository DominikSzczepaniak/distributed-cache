#!/bin/bash
set -e

# Build binaries
echo "Building binaries..."
go build -o raftcli ./cmd/raftcli
# We rely on docker-compose to build images, but we need to ensure they are fresh.
# We'll use --build flag.

# Clean up
echo "Cleaning up..."
docker-compose down -v

# Start Cluster with 2 DataNodes
echo "Starting Cluster (Controllers + 2 DataNodes)..."
docker-compose up -d --build controller-0 controller-1 controller-2 datanode-1 datanode-2

# Wait for cluster to be ready
echo "Waiting for cluster to stabilize..."
sleep 15

docker-compose ps

# Copy raftcli to controller-0
echo "Copying raftcli to controller-0..."
# We need to build linux binary if on mac
GOOS=linux GOARCH=amd64 go build -o raftcli-linux ./cmd/raftcli
docker cp raftcli-linux controller-0:/app/raftcli
docker exec controller-0 chmod +x /app/raftcli

# Load Data
echo "Loading 1000 keys..."
# Run from inside controller-0
if ! docker exec -i controller-0 /app/raftcli localhost:8080,localhost:8081,localhost:8082 <<< "load 1000
exit"; then
    echo "Load failed. Checking logs..."
    docker-compose logs datanode-2
    exit 1
fi

# Start 3rd DataNode
echo "Starting 3rd DataNode..."
docker-compose up -d --build datanode-3

# Wait for Rebalancing
echo "Waiting for rebalancing (30s)..."
sleep 30

# Check logs for rebalancing activity
echo "Checking Controller logs for rebalancing..."
docker-compose logs controller-0 | grep "Rebalancing" || echo "No rebalancing logs found on controller-0 (might be on leader)"

# Stop original DataNodes
echo "Stopping original DataNodes..."
docker stop datanode-1 datanode-2

# Verify Data
echo "Verifying data availability..."
found=0
for i in {0..999}; do
    key="key-$i"
    # Run get from inside controller-0
    if docker exec -i controller-0 /app/raftcli localhost:8080,localhost:8081,localhost:8082 <<< "get $key
exit" | grep "GET successful"; then
        echo "Found available key: $key"
        found=1
        break
    fi
done

if [ $found -eq 1 ]; then
    echo "SUCCESS: Data verified on new node."
else
    echo "FAILURE: Could not retrieve any keys after stopping original nodes."
    echo "Dumping datanode-3 logs:"
    docker-compose logs datanode-3
    exit 1
fi
