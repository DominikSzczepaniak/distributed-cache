#!/bin/bash

# start-local.sh - Start distributed cache cluster locally
# Features:
# - Builds raftnode binary
# - Cleans old data and processes
# - Starts N nodes with proper configuration
# - Waits for cluster health (auto-init verification)
# - Shows cluster status and usage info

set -e

# =============================================================================
# Configuration
# =============================================================================

# Cluster configuration
TOTAL_NODES=${TOTAL_NODES:-3}
BASE_RAFT_PORT=${BASE_RAFT_PORT:-9000}
BASE_API_PORT=${BASE_API_PORT:-10000}

# Timing configuration
STARTUP_WAIT=${STARTUP_WAIT:-3}       # Initial wait for processes to start
HEALTH_TIMEOUT=${HEALTH_TIMEOUT:-30}  # Max wait for cluster health
HEALTH_INTERVAL=${HEALTH_INTERVAL:-1} # Interval between health checks

# Expected partition count (16384 partitions in total)
EXPECTED_PARTITIONS=16384

# Colors for output
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
BLUE='\033[0;34m'
CYAN='\033[0;36m'
NC='\033[0m' # No Color

# =============================================================================
# Helper Functions
# =============================================================================

print_header() {
    echo -e "\n${BLUE}===${NC} ${CYAN}$1${NC} ${BLUE}===${NC}"
}

print_success() {
    echo -e "${GREEN}✓${NC} $1"
}

print_error() {
    echo -e "${RED}✗${NC} $1"
}

print_warning() {
    echo -e "${YELLOW}⚠${NC} $1"
}

print_info() {
    echo -e "${CYAN}→${NC} $1"
}

check_node_health() {
    local port=$1
    curl -s -f "http://localhost:${port}/health" > /dev/null 2>&1
    return $?
}

get_leader_id() {
    for ((i=0; i<TOTAL_NODES; i++)); do
        local port=$((BASE_API_PORT + i))
        local response=$(curl -s "http://localhost:${port}/status" 2>/dev/null)
        if echo "$response" | grep -q '"role":"leader"'; then
            echo "$i"
            return 0
        fi
    done
    echo ""
    return 1
}

check_partition_table_initialized() {
    # Check logs for partition table initialization message
    for ((i=0; i<TOTAL_NODES; i++)); do
        if grep -q "Updated partition table to version" "data/node${i}/node.log" 2>/dev/null; then
            return 0
        fi
        if grep -q "Restored snapshot with.*partition assignments" "data/node${i}/node.log" 2>/dev/null; then
            return 0
        fi
    done
    return 1
}

# =============================================================================
# Main Script
# =============================================================================

print_header "Building Raft Node Binary"

# Build the binary
if go build -o raftnode ./cmd/raftnode/main.go 2>&1; then
    print_success "Binary built successfully"
else
    print_error "Build failed!"
    exit 1
fi

# Verify binary exists
if [ ! -f ./raftnode ]; then
    print_error "Binary not found after build!"
    exit 1
fi

print_header "Cleaning Up Old Data and Processes"

# Stop any running raftnode processes
if pkill -f "./raftnode" 2>/dev/null; then
    print_success "Stopped old processes"
    sleep 1
else
    print_info "No old processes to stop"
fi

# Remove old data directories
if rm -rf data/node* 2>/dev/null; then
    print_success "Cleaned old data directories"
else
    print_info "No old data to clean"
fi

print_header "Starting ${TOTAL_NODES}-Node Cluster"

# Generate RAFT_ADDRS dynamically
RAFT_ADDRS=""
for ((i=0; i<TOTAL_NODES; i++)); do
    RAFT_PORT=$((BASE_RAFT_PORT + i))
    if [ $i -eq 0 ]; then
        RAFT_ADDRS="localhost:${RAFT_PORT}"
    else
        RAFT_ADDRS="${RAFT_ADDRS},localhost:${RAFT_PORT}"
    fi
done

print_info "Raft addresses: ${RAFT_ADDRS}"
echo ""

# Start each node
for ((i=0; i<TOTAL_NODES; i++)); do
    RAFT_PORT=$((BASE_RAFT_PORT + i))
    API_PORT=$((BASE_API_PORT + i))
    DATA_DIR="data/node${i}"
    mkdir -p "${DATA_DIR}"

    print_info "Starting Node ${i} (Raft: ${RAFT_PORT}, API: ${API_PORT})"

    # Start node in background
    RAFT_ID=$i \
    TOTAL_NODES=$TOTAL_NODES \
    RAFT_ADDRS=$RAFT_ADDRS \
    API_ADDR=":${API_PORT}" \
    FILENAME="${DATA_DIR}/raft" \
    SNAPSHOT_THRESHOLD=100 \
    RAFT_INITIAL_BACKOFF="500ms" \
    RAFT_MAX_BACKOFF="5s" \
    ./raftnode > "${DATA_DIR}/node.log" 2>&1 &

    # Save PID for later reference
    echo $! > "${DATA_DIR}/node.pid"
done

print_success "All nodes started"

print_header "Waiting for Cluster to Bootstrap"

# Initial wait for processes to start
print_info "Waiting ${STARTUP_WAIT}s for processes to initialize..."
sleep $STARTUP_WAIT

# Wait for all nodes to be healthy
print_info "Checking node health..."
start_time=$SECONDS
all_healthy=false

while [ $((SECONDS - start_time)) -lt $HEALTH_TIMEOUT ]; do
    healthy_count=0

    for ((i=0; i<TOTAL_NODES; i++)); do
        port=$((BASE_API_PORT + i))
        if check_node_health $port; then
            ((healthy_count++))
        fi
    done

    if [ $healthy_count -eq $TOTAL_NODES ]; then
        all_healthy=true
        break
    fi

    echo -ne "\r${CYAN}→${NC} Healthy nodes: ${healthy_count}/${TOTAL_NODES}... "
    sleep $HEALTH_INTERVAL
done

echo ""

if [ "$all_healthy" = false ]; then
    print_error "Timeout waiting for cluster health after ${HEALTH_TIMEOUT}s"
    print_warning "Some nodes may not have started properly"
    print_info "Check logs in data/nodeX/node.log for details"
    exit 1
fi

print_success "All nodes are healthy"

# Wait for auto-initialization and leader election
print_header "Verifying Auto-Initialization"
print_info "Waiting for leader election and partition table auto-init..."

leader_elected=false
partitions_initialized=false
start_time=$SECONDS

while [ $((SECONDS - start_time)) -lt $HEALTH_TIMEOUT ]; do
    # Check for leader
    if [ "$leader_elected" = false ]; then
        leader_id=$(get_leader_id)
        if [ -n "$leader_id" ]; then
            leader_elected=true
            leader_port=$((BASE_API_PORT + leader_id))
            print_success "Leader elected: Node ${leader_id} (port ${leader_port})"
        fi
    fi

    # Check partition table initialization via logs
    if [ "$partitions_initialized" = false ]; then
        if check_partition_table_initialized; then
            partitions_initialized=true
            print_success "Partition table initialized: ${EXPECTED_PARTITIONS} partitions"
        fi
    fi

    # Break if both conditions met
    if [ "$leader_elected" = true ] && [ "$partitions_initialized" = true ]; then
        break
    fi

    # Show progress
    if [ "$leader_elected" = false ]; then
        echo -ne "\r${CYAN}→${NC} Waiting for leader election... "
    elif [ "$partitions_initialized" = false ]; then
        echo -ne "\r${CYAN}→${NC} Waiting for partition table initialization... "
    fi

    sleep $HEALTH_INTERVAL
done

echo ""

# Check final status
if [ "$leader_elected" = false ]; then
    print_error "Leader election timeout after ${HEALTH_TIMEOUT}s"
    print_warning "Cluster may not be functioning properly"
    exit 1
fi

if [ "$partitions_initialized" = false ]; then
    print_error "Partition table auto-initialization timeout after ${HEALTH_TIMEOUT}s"
    print_info "Check logs in data/nodeX/node.log for details"
    exit 1
fi

# =============================================================================
# Success - Show Cluster Information
# =============================================================================

print_header "Cluster Ready!"

echo ""
echo -e "${GREEN}✓ Cluster Status:${NC}"
echo "  • Nodes: ${TOTAL_NODES}"
echo "  • Leader: Node ${leader_id} (http://localhost:${leader_port})"
echo "  • Partitions: ${EXPECTED_PARTITIONS}"
echo ""

echo -e "${CYAN}→ Node Endpoints:${NC}"
for ((i=0; i<TOTAL_NODES; i++)); do
    api_port=$((BASE_API_PORT + i))
    raft_port=$((BASE_RAFT_PORT + i))
    echo "  Node ${i}: http://localhost:${api_port} (Raft: ${raft_port})"
done

echo ""
echo -e "${CYAN}→ Quick Commands:${NC}"
echo "  # Check cluster status"
echo "  ./scripts/local/status-local.sh"
echo ""
echo "  # Run functionality tests"
echo "  ./scripts/local/test-local.sh"
echo ""
echo "  # Stop cluster"
echo "  ./scripts/local/stop-local.sh"
echo ""
echo "  # Clean all data"
echo "  ./scripts/local/clean-local.sh"
echo ""

echo -e "${CYAN}→ API Examples:${NC}"
echo "  # PUT key-value"
echo "  curl -X POST http://localhost:${BASE_API_PORT}/kv -H 'Content-Type: application/json' -d '{\"key\": 123, \"value\": 456}'"
echo ""
echo "  # GET value"
echo "  curl http://localhost:${BASE_API_PORT}/kv/123"
echo ""
echo "  # DELETE key"
echo "  curl -X DELETE http://localhost:${BASE_API_PORT}/kv/123"
echo ""

echo -e "${CYAN}→ Logs:${NC}"
echo "  Logs are located in: data/node{0,1,2}/node.log"
echo ""
echo "  # Tail first node log"
echo "  tail -f data/node0/node.log"
echo ""
