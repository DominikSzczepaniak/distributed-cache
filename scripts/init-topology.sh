#!/bin/sh
# init-topology.sh
# Waits for datanodes to register, then configures the shard topology

set -e

CONTROLLER_URL="${CONTROLLER_URL:-http://localhost:8080}"
EXPECTED_NODES="${EXPECTED_NODES:-4}"
TOTAL_SHARDS="${TOTAL_SHARDS:-1}"

echo "=== Topology Init Script ==="
echo "Controller: $CONTROLLER_URL"
echo "Expected nodes: $EXPECTED_NODES"
echo "Total shards: $TOTAL_SHARDS"

# Wait for controller to be ready
echo "Waiting for controller to be ready..."
until wget -qO- "${CONTROLLER_URL}/topology" > /dev/null 2>&1; do
    echo "  Controller not ready, waiting..."
    sleep 2
done
echo "Controller is ready!"

# Wait for all datanodes to register
echo "Waiting for $EXPECTED_NODES datanodes to register..."
while true; do
    TOPOLOGY=$(wget -qO- "${CONTROLLER_URL}/topology" 2>/dev/null || echo '{}')
    
    # Count registered nodes (look for "ACTIVE" status in nodes)
    NODE_COUNT=$(echo "$TOPOLOGY" | grep -o '"status":"ACTIVE"' | wc -l | tr -d ' ')
    
    echo "  Registered nodes: $NODE_COUNT / $EXPECTED_NODES"
    
    if [ "$NODE_COUNT" -ge "$EXPECTED_NODES" ]; then
        echo "All datanodes registered!"
        break
    fi
    
    sleep 2
done

# Extract node IDs from the topology
echo "Extracting node IDs..."
NODE_IDS=$(echo "$TOPOLOGY" | grep -o '"id":"[^"]*"' | sed 's/"id":"//g' | sed 's/"//g' | sort)
echo "Found nodes: $NODE_IDS"

# Build the nodes JSON
NODES_JSON="{"
FIRST=true
for NODE_ID in $NODE_IDS; do
    if [ "$FIRST" = true ]; then
        FIRST=false
    else
        NODES_JSON="${NODES_JSON},"
    fi
    NODES_JSON="${NODES_JSON}\"${NODE_ID}\":{\"id\":\"${NODE_ID}\",\"address\":\"${NODE_ID}\",\"status\":\"ACTIVE\"}"
done
NODES_JSON="${NODES_JSON}}"

# Select primary and replicas for each shard
# Simple strategy: first node is primary, rest are replicas for shard 0
# For multiple shards, we'd distribute across nodes
NODE_ARRAY=""
for NODE_ID in $NODE_IDS; do
    NODE_ARRAY="${NODE_ARRAY} ${NODE_ID}"
done

# Convert to array-like handling in shell
set -- $NODE_ARRAY
PRIMARY_ID="$1"
shift

REPLICAS_JSON="["
FIRST=true
for REPLICA_ID in "$@"; do
    if [ "$FIRST" = true ]; then
        FIRST=false
    else
        REPLICAS_JSON="${REPLICAS_JSON},"
    fi
    REPLICAS_JSON="${REPLICAS_JSON}\"${REPLICA_ID}\""
done
REPLICAS_JSON="${REPLICAS_JSON}]"

# Build shards JSON (for now, single shard)
SHARDS_JSON="{\"0\":{\"id\":0,\"primary_id\":\"${PRIMARY_ID}\",\"replica_ids\":${REPLICAS_JSON},\"status\":\"ACTIVE\"}}"

# Get current epoch and increment
CURRENT_EPOCH=$(echo "$TOPOLOGY" | grep -o '"epoch":[0-9]*' | head -1 | sed 's/"epoch"://')
NEW_EPOCH=$((CURRENT_EPOCH + 1))

# Build final config
CONFIG="{\"epoch\":${NEW_EPOCH},\"total_shards\":${TOTAL_SHARDS},\"nodes\":${NODES_JSON},\"shards\":${SHARDS_JSON}}"

echo ""
echo "Setting topology with epoch $NEW_EPOCH..."
echo "Primary: $PRIMARY_ID"
echo "Replicas: $REPLICAS_JSON"

# Apply the configuration
RESULT=$(wget -qO- --post-data="${CONFIG}" --header="Content-Type: application/json" "${CONTROLLER_URL}/debug/config" 2>&1 || echo "failed")

if echo "$RESULT" | grep -q "failed"; then
    echo "Failed to set topology!"
    exit 1
fi

echo ""
echo "=== Topology configured successfully! ==="
echo ""

# Verify
sleep 2
echo "Final topology:"
wget -qO- "${CONTROLLER_URL}/topology" | head -c 500
echo ""
echo ""
echo "Init complete. Cluster is ready for use."
