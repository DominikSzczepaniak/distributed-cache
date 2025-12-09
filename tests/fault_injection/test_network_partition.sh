#!/bin/bash
set -e

NETWORK_NAME="raft-cluster"
CONTROLLERS=("controller-0" "controller-1" "controller-2")

cleanup() {
    echo ""
    echo "--- 🧹 Cleaning up cluster ---"
    docker-compose down -v
}

trap cleanup EXIT

echo "--- 🏗️  Building binaries ---"
CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -o raftcli-linux ./cmd/raftcli

echo "--- �️  Cleaning data directories ---"
rm -rf data/node0 data/node1 data/node2
mkdir -p data/node0 data/node1 data/node2

echo "--- �🚀 Starting cluster ---"
docker-compose up -d --build controller-0 controller-1 controller-2 datanode-1 datanode-2
echo "Waiting for cluster to stabilize (15s)..."
sleep 15

find_leader() {
    echo "--- 🕵️ Finding leader by checking logs ---" >&2
    for i in {1..10}; do
        for node in "${CONTROLLERS[@]}"; do
            if docker-compose logs "$node" | grep -q "became leader"; then
                echo "--- ✅ Leader found: $node ---" >&2
                echo "$node"
                return
            fi
        done
        sleep 2
    done
    echo "--- ❌ ERROR: Could not find a leader. ---" >&2
    exit 1
}

initial_leader=$(find_leader)

echo "--- 🔌 Isolating leader '$initial_leader' from network '$NETWORK_NAME' ---"

LEADER_COUNT_BEFORE=$(docker-compose logs 2>&1 | grep -c "became leader" || echo "0")
echo "Leader elections before partition: $LEADER_COUNT_BEFORE"

docker network disconnect "$NETWORK_NAME" "$initial_leader"

echo "Waiting for new leader election (up to 60s)..."
NEW_LEADER=""
for i in {1..30}; do
    sleep 2
    LEADER_COUNT_NOW=$(docker-compose logs 2>&1 | grep -c "became leader" || echo "0")
    
    if [ "$LEADER_COUNT_NOW" -gt "$LEADER_COUNT_BEFORE" ]; then
        for node in "${CONTROLLERS[@]}"; do
            if [ "$node" == "$initial_leader" ]; then
                continue
            fi
            if docker-compose logs "$node" 2>&1 | grep -q "became leader"; then
                NEW_LEADER="$node"
                echo "Found new leader: $NEW_LEADER" >&2
                break 2
            fi
        done
    fi
    echo "  Waiting for new leader... ($((i*2))s elapsed, elections: $LEADER_COUNT_NOW)"
done


if [ -z "$NEW_LEADER" ]; then
    echo "--- ❌ ERROR: No new leader elected after partition ---"
    echo "Dumping logs:"
    docker-compose logs
    exit 1
fi

echo "--- ✅ New leader found: $NEW_LEADER ---"

echo "--- ✍️  Registering 'partition-test-node' to new leader to change Raft state ---"

PORT="8080"
if [ "$NEW_LEADER" == "controller-1" ]; then PORT="8081"; fi
if [ "$NEW_LEADER" == "controller-2" ]; then PORT="8082"; fi

echo "Attempting to register to $NEW_LEADER:$PORT..."
REGISTERED=false
for attempt in {1..5}; do
    HTTP_CODE=$(curl -s -o /dev/null -w "%{http_code}" --max-time 10 -X POST -H "Content-Type: application/json" \
        -d '{"node_id": "partition-test-node", "address": "1.2.3.4:9000"}' \
        "http://localhost:$PORT/cluster/register" || echo "FAILED")
    
    if [ "$HTTP_CODE" == "200" ]; then
        echo "--- ✅ Registration successful on $NEW_LEADER ---"
        REGISTERED=true
        break
    else
        echo "--- ⚠️  Registration attempt $attempt failed with status $HTTP_CODE, retrying in 2s... ---"
        sleep 2
    fi
done

if [ "$REGISTERED" = false ]; then
    echo "--- ❌ Failed to register node during partition ---"
    echo "Dumping logs:"
    docker-compose logs
    exit 1
fi

echo "--- 🔗 Reconnecting original leader '$initial_leader' to network '$NETWORK_NAME' ---"
docker network connect "$NETWORK_NAME" "$initial_leader"

echo "Waiting for state reconciliation (30s)..."
sleep 30

echo "--- 📖 Verifying state on original leader '$initial_leader' ---"
LEADER_PORT="8080"
if [ "$initial_leader" == "controller-1" ]; then LEADER_PORT="8081"; fi
if [ "$initial_leader" == "controller-2" ]; then LEADER_PORT="8082"; fi

TOPOLOGY=$(curl -s "http://localhost:$LEADER_PORT/topology")
echo "Topology on $initial_leader: $TOPOLOGY"

if echo "$TOPOLOGY" | grep -q "partition-test-node"; then
    echo "--- ✅ State reconciled! Old leader has the new node. ---"
else
    echo "--- ❌ State reconciliation failed. Old leader missing new node. ---"
    echo "Dumping logs for $initial_leader:"
    docker-compose logs "$initial_leader"
    exit 1
fi

echo ""
echo "--- 🎉 SUCCESS: Network partition test passed! ---"
