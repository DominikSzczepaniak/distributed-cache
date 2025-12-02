#!/bin/bash

# start-cluster-with-workers.sh - Start complete distributed cache cluster
#
# Architecture:
# - 3 Raft nodes (control plane - partition table management)
# - 3 Worker nodes (data plane - partitioned storage)
#
# Features:
# - Builds both binaries (raftnode + worker)
# - Cleans old data and processes
# - Starts Raft cluster with auto-initialization
# - Starts worker nodes with registration
# - Validates cluster health
# - Shows usage examples

set -e

# =============================================================================
# Configuration
# =============================================================================

# Cluster configuration
TOTAL_RAFT_NODES=${TOTAL_RAFT_NODES:-3}
TOTAL_WORKERS=${TOTAL_WORKERS:-3}

# Port configuration
BASE_RAFT_PORT=9000         # Raft gRPC ports: 9000, 9001, 9002
BASE_RAFT_API_PORT=8080     # Raft HTTP API ports: 8080, 8081, 8082
BASE_WORKER_GRPC_PORT=17100 # Worker gRPC ports: 17100, 17101, 17102
BASE_WORKER_HTTP_PORT=17000 # Worker HTTP ports: 17000, 17001, 17002 (avoid macOS port 7000)

# Timing configuration
STARTUP_WAIT=3
HEALTH_TIMEOUT=30
HEALTH_INTERVAL=1

# Colors
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
BLUE='\033[0;34m'
CYAN='\033[0;36m'
NC='\033[0m'

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

check_health() {
    local port=$1
    curl -s -f "http://localhost:${port}/health" > /dev/null 2>&1
    return $?
}

# =============================================================================
# Main Script
# =============================================================================

print_header "Building Binaries"

# Build raftnode binary
if go build -o bin/raftnode ./cmd/raftnode/main.go 2>&1; then
    print_success "Raft node binary built: bin/raftnode"
else
    print_error "Raft node build failed!"
    exit 1
fi

# Build worker binary
if go build -o bin/worker ./cmd/worker/main.go 2>&1; then
    print_success "Worker binary built: bin/worker"
else
    print_error "Worker build failed!"
    exit 1
fi

print_header "Cleaning Up Old Data and Processes"

# Stop old processes
pkill -f "bin/raftnode" 2>/dev/null && print_success "Stopped old raftnode processes" || print_info "No old raftnode processes"
pkill -f "bin/worker" 2>/dev/null && print_success "Stopped old worker processes" || print_info "No old worker processes"
sleep 1

# Remove old data
rm -rf data/node* 2>/dev/null && print_success "Cleaned Raft data directories" || print_info "No old Raft data"
rm -rf data/worker* 2>/dev/null && print_success "Cleaned worker data directories" || print_info "No old worker data"

print_header "Starting Raft Cluster (${TOTAL_RAFT_NODES} nodes)"

# Generate RAFT_ADDRS
RAFT_ADDRS=""
for ((i=0; i<TOTAL_RAFT_NODES; i++)); do
    RAFT_PORT=$((BASE_RAFT_PORT + i))
    if [ $i -eq 0 ]; then
        RAFT_ADDRS="localhost:${RAFT_PORT}"
    else
        RAFT_ADDRS="${RAFT_ADDRS},localhost:${RAFT_PORT}"
    fi
done

print_info "Raft addresses: ${RAFT_ADDRS}"
echo ""

# Start each Raft node
for ((i=0; i<TOTAL_RAFT_NODES; i++)); do
    RAFT_PORT=$((BASE_RAFT_PORT + i))
    API_PORT=$((BASE_RAFT_API_PORT + i))
    DATA_DIR="data/node${i}"
    mkdir -p "${DATA_DIR}"

    print_info "Starting Raft Node ${i} (Raft: ${RAFT_PORT}, HTTP: ${API_PORT})"

    RAFT_ID=$i \
    TOTAL_NODES=$TOTAL_RAFT_NODES \
    RAFT_ADDRS=$RAFT_ADDRS \
    API_ADDR=":${API_PORT}" \
    FILENAME="${DATA_DIR}/raft" \
    SNAPSHOT_THRESHOLD=100 \
    ./bin/raftnode > "${DATA_DIR}/node.log" 2>&1 &

    echo $! > "${DATA_DIR}/node.pid"
done

print_success "All Raft nodes started"

# Wait for Raft cluster health
print_header "Waiting for Raft Cluster Health"
print_info "Waiting ${STARTUP_WAIT}s for Raft initialization..."
sleep $STARTUP_WAIT

all_healthy=false
start_time=$SECONDS

while [ $((SECONDS - start_time)) -lt $HEALTH_TIMEOUT ]; do
    healthy_count=0

    for ((i=0; i<TOTAL_RAFT_NODES; i++)); do
        port=$((BASE_RAFT_API_PORT + i))
        if check_health $port; then
            ((healthy_count++))
        fi
    done

    if [ $healthy_count -eq $TOTAL_RAFT_NODES ]; then
        all_healthy=true
        break
    fi

    echo -ne "\r${CYAN}→${NC} Healthy Raft nodes: ${healthy_count}/${TOTAL_RAFT_NODES}... "
    sleep $HEALTH_INTERVAL
done

echo ""

if [ "$all_healthy" = false ]; then
    print_error "Raft cluster health timeout after ${HEALTH_TIMEOUT}s"
    print_info "Check logs in data/node*/node.log"
    exit 1
fi

print_success "Raft cluster is healthy (${TOTAL_RAFT_NODES} nodes)"

print_header "Starting Worker Nodes (${TOTAL_WORKERS} workers)"

# Start each worker
for ((i=0; i<TOTAL_WORKERS; i++)); do
    WORKER_HTTP_PORT=$((BASE_WORKER_HTTP_PORT + i))
    WORKER_GRPC_PORT=$((BASE_WORKER_GRPC_PORT + i))
    WORKER_DATA_DIR="data/worker${i}"
    mkdir -p "${WORKER_DATA_DIR}"

    print_info "Starting Worker ${i} (HTTP: ${WORKER_HTTP_PORT}, gRPC: ${WORKER_GRPC_PORT})"

    WORKER_ID=$i \
    HTTP_ADDR=":${WORKER_HTTP_PORT}" \
    GRPC_ADDR=":${WORKER_GRPC_PORT}" \
    RAFT_ADDRS=$RAFT_ADDRS \
    ./bin/worker > "${WORKER_DATA_DIR}/worker.log" 2>&1 &

    echo $! > "${WORKER_DATA_DIR}/worker.pid"
done

print_success "All worker nodes started"

# Wait for worker health
print_header "Waiting for Worker Health & Registration"
print_info "Waiting ${STARTUP_WAIT}s for worker initialization..."
sleep $STARTUP_WAIT

all_workers_healthy=false
start_time=$SECONDS

while [ $((SECONDS - start_time)) -lt $HEALTH_TIMEOUT ]; do
    healthy_count=0

    for ((i=0; i<TOTAL_WORKERS; i++)); do
        port=$((BASE_WORKER_HTTP_PORT + i))
        if check_health $port; then
            ((healthy_count++))
        fi
    done

    if [ $healthy_count -eq $TOTAL_WORKERS ]; then
        all_workers_healthy=true
        break
    fi

    echo -ne "\r${CYAN}→${NC} Healthy workers: ${healthy_count}/${TOTAL_WORKERS}... "
    sleep $HEALTH_INTERVAL
done

echo ""

if [ "$all_workers_healthy" = false ]; then
    print_error "Worker health timeout after ${HEALTH_TIMEOUT}s"
    print_warning "Check logs in data/worker*/worker.log"
fi

print_success "All workers are healthy (${TOTAL_WORKERS} workers)"

# =============================================================================
# Success - Show Cluster Information
# =============================================================================

print_header "Cluster Ready! 🚀"

echo ""
echo -e "${GREEN}✓ Cluster Status:${NC}"
echo "  • Raft Nodes: ${TOTAL_RAFT_NODES} (control plane)"
echo "  • Worker Nodes: ${TOTAL_WORKERS} (data plane)"
echo "  • Partitions: 16,384"
echo ""

echo -e "${CYAN}→ Raft Node Endpoints (Control Plane):${NC}"
for ((i=0; i<TOTAL_RAFT_NODES; i++)); do
    api_port=$((BASE_RAFT_API_PORT + i))
    raft_port=$((BASE_RAFT_PORT + i))
    echo "  Raft Node ${i}: http://localhost:${api_port} (gRPC: ${raft_port})"
done

echo ""
echo -e "${CYAN}→ Worker Node Endpoints (Data Plane):${NC}"
for ((i=0; i<TOTAL_WORKERS; i++)); do
    http_port=$((BASE_WORKER_HTTP_PORT + i))
    grpc_port=$((BASE_WORKER_GRPC_PORT + i))
    echo "  Worker ${i}: http://localhost:${http_port} (gRPC: ${grpc_port})"
done

echo ""
echo -e "${CYAN}→ Testing the Cluster:${NC}"
echo ""
echo "  # PUT via Raft (will redirect to worker)"
echo "  curl -v -L -X POST http://localhost:${BASE_RAFT_API_PORT}/kv \\"
echo "    -H 'Content-Type: application/json' \\"
echo "    -d '{\"key\": 123, \"value\": 456}'"
echo ""
echo "  # GET via Raft (will redirect to worker)"
echo "  curl -L http://localhost:${BASE_RAFT_API_PORT}/kv/123"
echo ""
echo "  # DELETE via Raft (will redirect to worker)"
echo "  curl -L -X DELETE http://localhost:${BASE_RAFT_API_PORT}/kv/123"
echo ""
echo "  # Direct PUT to worker (no redirect)"
echo "  curl -X POST http://localhost:${BASE_WORKER_HTTP_PORT}/kv \\"
echo "    -H 'Content-Type: application/json' \\"
echo "    -d '{\"key\": 456, \"value\": 789}'"
echo ""
echo "  # Check worker stats"
echo "  curl http://localhost:${BASE_WORKER_HTTP_PORT}/stats"
echo ""

echo -e "${CYAN}→ Management Commands:${NC}"
echo "  # Stop cluster"
echo "  pkill -f bin/raftnode && pkill -f bin/worker"
echo ""
echo "  # View Raft logs"
echo "  tail -f data/node0/node.log"
echo ""
echo "  # View Worker logs"
echo "  tail -f data/worker0/worker.log"
echo ""
echo "  # Clean all data"
echo "  rm -rf data/"
echo ""

echo -e "${CYAN}→ Logs:${NC}"
echo "  Raft logs: data/node{0,1,2}/node.log"
echo "  Worker logs: data/worker{0,1,2}/worker.log"
echo ""

echo -e "${GREEN}🎉 Ready to test Stage 3 routing!${NC}"
echo "   Try the test commands above to see HTTP 307 redirects in action."
echo ""
