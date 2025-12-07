#!/bin/sh
# init-topology.sh
# Waits for datanodes to register, then configures the shard topology

set -e

CONTROLLER_URL="${CONTROLLER_URL:-http://localhost:8080}"
EXPECTED_NODES="${EXPECTED_NODES:-4}"
TOTAL_SHARDS="${TOTAL_SHARDS:-1}"

# Define the node IDs we expect (passed as env var or default to these)
# Format: comma-separated list of node IDs
NODE_LIST="${NODE_LIST:-192.168.1.100:9010,192.168.1.100:9011,192.168.1.101:9010,192.168.1.101:9011}"

echo "=== Topology Init Script ==="
echo "Controller: $CONTROLLER_URL"
echo "Expected nodes: $EXPECTED_NODES"
echo "Total shards: $TOTAL_SHARDS"
echo "Node list: $NODE_LIST"

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
    
    # Count how many of our expected nodes are in the topology
    NODE_COUNT=0
    for NODE_ID in $(echo "$NODE_LIST" | tr ',' ' '); do
        if echo "$TOPOLOGY" | grep -q "\"$NODE_ID\""; then
            NODE_COUNT=$((NODE_COUNT + 1))
        fi
    done
    
    echo "  Registered nodes: $NODE_COUNT / $EXPECTED_NODES"
    
    if [ "$NODE_COUNT" -ge "$EXPECTED_NODES" ]; then
        echo "All datanodes registered!"
        break
    fi
    
    sleep 2
done

# Parse node list into primary and replicas
PRIMARY_ID=""
REPLICAS=""
NODES_JSON="{"
FIRST_NODE=true
FIRST_REPLICA=true

for NODE_ID in $(echo "$NODE_LIST" | tr ',' ' '); do
    # Build nodes map
    if [ "$FIRST_NODE" = true ]; then
        FIRST_NODE=false
        PRIMARY_ID="$NODE_ID"
    else
        if [ "$FIRST_REPLICA" = true ]; then
            FIRST_REPLICA=false
            REPLICAS="\"$NODE_ID\""
        else
            REPLICAS="$REPLICAS,\"$NODE_ID\""
        fi
        NODES_JSON="$NODES_JSON,"
    fi
    NODES_JSON="$NODES_JSON\"$NODE_ID\":{\"id\":\"$NODE_ID\",\"address\":\"$NODE_ID\",\"status\":\"ACTIVE\"}"
done
NODES_JSON="$NODES_JSON}"

# Get current epoch and increment significantly
CURRENT_EPOCH=$(echo "$TOPOLOGY" | sed 's/.*"epoch":\([0-9]*\).*/\1/' | head -1)
if [ -z "$CURRENT_EPOCH" ] || [ "$CURRENT_EPOCH" = "$TOPOLOGY" ]; then
    CURRENT_EPOCH=0
fi
NEW_EPOCH=$((CURRENT_EPOCH + 100))

# Build shards JSON
SHARDS_JSON="{\"0\":{\"id\":0,\"primary_id\":\"$PRIMARY_ID\",\"replica_ids\":[$REPLICAS],\"status\":\"ACTIVE\"}}"

# Build final config
CONFIG="{\"epoch\":$NEW_EPOCH,\"total_shards\":$TOTAL_SHARDS,\"nodes\":$NODES_JSON,\"shards\":$SHARDS_JSON}"

echo ""
echo "Setting topology with epoch $NEW_EPOCH..."
echo "Primary: $PRIMARY_ID"
echo "Replicas: [$REPLICAS]"
echo ""
echo "Config: $CONFIG"
echo ""

# Apply the configuration
HTTP_CODE=$(wget -qO- --post-data="$CONFIG" --header="Content-Type: application/json" \
    -S "${CONTROLLER_URL}/debug/config" 2>&1 | grep "HTTP/" | tail -1 | awk '{print $2}')

if [ "$HTTP_CODE" != "200" ] && [ -n "$HTTP_CODE" ]; then
    echo "Warning: Got HTTP $HTTP_CODE when setting topology"
fi

echo ""
echo "=== Topology configured! ==="
echo ""

# Wait a moment for propagation
sleep 3

# Verify
echo "Final topology:"
wget -qO- "${CONTROLLER_URL}/topology"
echo ""
echo ""
echo "Init complete. Cluster is ready for use."
