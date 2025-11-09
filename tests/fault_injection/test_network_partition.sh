#!/bin/bash

# Exit immediately if a command exits with a non-zero status.
set -e

# --- Configuration ---
NETWORK_NAME="raft-cluster"
NODES=("raft-node-0" "raft-node-1" "raft-node-2")
NODE_URLS=("http://localhost:8080" "http://localhost:8081" "http://localhost:8082")
TEST_KEY=303
TEST_VALUE=3003

# --- Cleanup Function ---
cleanup() {
    echo ""
    echo "--- 🧹 Cleaning up cluster ---"
    docker compose down -v
}

# Register the cleanup function to be called on script exit
trap cleanup EXIT

# --- Helper Functions ---

# Function to find the current leader
# Usage: find_leader [exclude_node]
find_leader() {
    local exclude_node=$1
    echo "--- 🕵️ Finding leader (excluding: ${exclude_node:-none}) ---" >&2
    for i in {1..30}; do
        for idx in "${!NODES[@]}"; do
            local node_name=${NODES[$idx]}
            local node_url=${NODE_URLS[$idx]}

            if [ "$node_name" == "$exclude_node" ]; then
                continue
            fi

            # Use curl to get leader status, suppress output, handle errors
            local response
            response=$(curl --silent --fail --max-time 2 "${node_url}/leader" || true)

            if [[ -n "$response" ]] && [[ $(echo "$response" | jq -r '.is_leader') == "true" ]]; then
                echo "--- ✅ Leader found: $node_name ---" >&2
                echo "$node_name"
                return
            fi
        done
        sleep 1
    done
    echo "--- ❌ ERROR: Could not find a leader after 30 seconds. ---" >&2
    exit 1
}

# --- Main Script ---

# 1. Start Cluster
echo "--- 🚀 Starting cluster ---"
docker compose up -d --build
# Give nodes a moment to initialize
sleep 5

# 2. Identify Initial Leader
initial_leader=$(find_leader)
initial_leader_url=""
other_nodes=()
for idx in "${!NODES[@]}"; do
    if [ "${NODES[$idx]}" == "$initial_leader" ]; then
        initial_leader_url=${NODE_URLS[$idx]}
    else
        other_nodes+=("${NODES[$idx]}")
    fi
done

# 3. Isolate Leader
echo "--- 🔌 Isolating leader '$initial_leader' from network '$NETWORK_NAME' ---"
docker network disconnect "$NETWORK_NAME" "$initial_leader"

# 4. Elect New Leader
new_leader=$(find_leader "$initial_leader")
new_leader_url=""
for idx in "${!NODES[@]}"; do
    if [ "${NODES[$idx]}" == "$new_leader" ]; then
        new_leader_url=${NODE_URLS[$idx]}
    fi
done

# 5. Write Data to New Leader
echo "--- ✍️ Writing key=$TEST_KEY, value=$TEST_VALUE to new leader '$new_leader' ---"
curl --silent --fail -X POST -H "Content-Type: application/json" \
    -d "{\"key\": $TEST_KEY, \"value\": $TEST_VALUE}" \
    "${new_leader_url}/kv"
echo ""
echo "--- ✅ Write successful ---"

# 6. Heal Partition
echo "--- 🔗 Reconnecting original leader '$initial_leader' to network '$NETWORK_NAME' ---"
docker network connect "$NETWORK_NAME" "$initial_leader"

# 7. Verify State Reconciliation
echo "--- 🔄 Verifying state sync on original leader '$initial_leader' ---"
for i in {1..30}; do
    echo "Attempt $i: Checking for key=$TEST_KEY on $initial_leader..."
    response=$(curl --silent --fail "${initial_leader_url}/kv/${TEST_KEY}" || true)
    
    if [[ -n "$response" ]]; then
        retrieved_value=$(echo "$response" | jq -r '.value' || echo "null")
        if [[ "$retrieved_value" == "$TEST_VALUE" ]]; then
            echo ""
            echo "--- 🎉 SUCCESS: State reconciled on '$initial_leader'! ---"
            exit 0
        fi
    fi
    sleep 1
done

echo ""
echo "--- 🔥 FAILURE: Timed out waiting for state reconciliation on '$initial_leader'. ---"
exit 1
