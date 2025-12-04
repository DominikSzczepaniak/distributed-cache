#!/bin/bash

# End-to-End Consistency Verification Script
# Usage: ./verify_consistency.sh

set -e

echo "=================================================="
echo "      End-to-End Consistency Verification"
echo "=================================================="

# 1. Build raftcli for Linux (to run inside Docker)
echo "[1/5] Building raftcli for Linux..."
GOOS=linux GOARCH=amd64 go build -o raftcli-linux ./cmd/raftcli

# 2. Ensure Cluster is Running
echo "[2/5] Checking if cluster is running..."
if ! docker ps | grep -q "controller-0"; then
    echo "Cluster not running. Starting it..."
    docker-compose up -d
    echo "Waiting 10s for cluster to stabilize..."
    sleep 10
fi

# 3. Install raftcli in controller-0
echo "[3/5] Installing raftcli in controller-0..."
docker cp raftcli-linux controller-0:/bin/raftcli
docker exec controller-0 chmod +x /bin/raftcli

# 4. Run Write Loop
echo "[4/5] Starting Write Loop (50 keys)..."
echo "--------------------------------------------------"
echo "!!! ACTION REQUIRED !!!"
echo "While this loop runs, open another terminal and KILL the Primary DataNode."
echo "Command: docker stop datanode-1"
echo "--------------------------------------------------"
sleep 2

for i in $(seq 1 50); do
    echo -n "Writing key-$i... "
    # Run raftcli inside controller-0, pointing to itself (localhost:8080)
    # We use '|| true' to continue script even if raftcli fails (though it should retry)
    docker exec controller-0 /bin/raftcli localhost:8080 put "key-$i" "val-$i" || echo "FAILED"
    sleep 0.5
done

# 5. Verification
echo "--------------------------------------------------"
echo "[5/5] Verifying Data Consistency..."
echo "Reading back all keys..."

MISSING=0
for i in $(seq 1 50); do
    if ! docker exec controller-0 /bin/raftcli localhost:8080 get "key-$i" > /dev/null 2>&1; then
        echo "❌ Missing: key-$i"
        MISSING=$((MISSING+1))
    else
        echo "✓ Found: key-$i"
    fi
done

echo "--------------------------------------------------"
if [ $MISSING -eq 0 ]; then
    echo "SUCCESS: All keys found! Consistency verified."
else
    echo "FAILURE: $MISSING keys missing."
    exit 1
fi
