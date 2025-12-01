#!/bin/bash

# Configuration
TOTAL_NODES=3
BASE_RAFT_PORT=9000
BASE_API_PORT=10000
RAFT_ADDRS="localhost:9000,localhost:9001,localhost:9002"

# Colors for output
GREEN='\033[0;32m'
RED='\033[0;31m'
NC='\033[0m' # No Color

echo -e "${GREEN}--- Building Raft Node and CLI ---${NC}"
go build -o raftnode ./cmd/raftnode/main.go
go build -o raftcli ./cmd/raftcli/main.go

if [ ! -f ./raftnode ]; then
    echo -e "${RED}Build failed!${NC}"
    exit 1
fi

echo -e "${GREEN}--- Cleaning up old data and processes ---${NC}"
pkill -f "./raftnode" || true
rm -rf data/node*

echo -e "${GREEN}--- Starting $TOTAL_NODES Node Cluster ---${NC}"

for (( i=0; i<$TOTAL_NODES; i++ ))
do
    RAFT_PORT=$((BASE_RAFT_PORT + i))
    API_PORT=$((BASE_API_PORT + i))
    DATA_DIR="data/node$i"
    mkdir -p $DATA_DIR

    echo "Starting Node $i (Raft: $RAFT_PORT, API: $API_PORT)"
    
    # Start node in background
    RAFT_ID=$i \
    TOTAL_NODES=$TOTAL_NODES \
    RAFT_ADDRS=$RAFT_ADDRS \
    API_ADDR=":$API_PORT" \
    FILENAME="$DATA_DIR/raft" \
    SNAPSHOT_THRESHOLD=100 \
    RAFT_INITIAL_BACKOFF="500ms" \
    RAFT_MAX_BACKOFF="5s" \
    ./raftnode > "$DATA_DIR/node.log" 2>&1 &
done

echo -e "${GREEN}--- Waiting 5 seconds for cluster to bootstrap ---${NC}"
sleep 5

echo -e "${GREEN}--- Initializing Partition Table ---${NC}"
# We construct the JSON array [0, 1, 2] dynamically or hardcode for 3 nodes
curl -s -X POST http://localhost:10000/admin/init-partition-table \
  -H "Content-Type: application/json" \
  -d "{\"node_ids\": [0, 1, 2]}"

echo -e "\n\n${GREEN}--- Cluster is Ready! ---${NC}"
echo "You can use the CLI to interact with the cluster:"
echo "./raftcli localhost:10000"
echo ""
echo "To stop the cluster, run: pkill -f ./raftnode"
echo "Logs are located in data/nodeX/node.log"

# Optional: Tail the logs of the first node
# tail -f data/node0/node.log
