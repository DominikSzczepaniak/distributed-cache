#!/bin/bash

# status-local.sh - Check distributed cache cluster status
# Features:
# - Checks which nodes are running
# - Queries health endpoints
# - Shows partition table status
# - Shows leader information
# - Displays recent log summaries

set -e

# =============================================================================
# Configuration
# =============================================================================

TOTAL_NODES=${TOTAL_NODES:-3}
BASE_RAFT_PORT=${BASE_RAFT_PORT:-9000}
BASE_API_PORT=${BASE_API_PORT:-10000}
EXPECTED_PARTITIONS=16384

# Colors for output
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
BLUE='\033[0;34m'
CYAN='\033[0;36m'
GRAY='\033[0;90m'
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

print_gray() {
    echo -e "${GRAY}$1${NC}"
}

check_node_health() {
    local port=$1
    curl -s -f "http://localhost:${port}/health" > /dev/null 2>&1
    return $?
}

get_node_status() {
    local port=$1
    curl -s "http://localhost:${port}/status" 2>/dev/null
}

check_partition_table_initialized() {
    # Check logs for partition table initialization
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

print_header "Cluster Process Status"

# Check for running raftnode processes
running_pids=$(pgrep -f "./raftnode" 2>/dev/null || echo "")

if [ -z "$running_pids" ]; then
    print_error "No raftnode processes running"
    echo ""
    print_info "Start cluster with: ./scripts/local/start-local.sh"
    echo ""
    exit 1
fi

running_count=$(echo "$running_pids" | wc -l | tr -d ' ')
print_success "${running_count} raftnode process(es) running"

# Show PIDs
echo ""
echo -e "${CYAN}Process Details:${NC}"
ps aux | grep "./raftnode" | grep -v grep | while read line; do
    echo -e "  ${GRAY}${line}${NC}"
done

print_header "Node Health Status"

healthy_nodes=0
leader_id=""
leader_port=""

for ((i=0; i<TOTAL_NODES; i++)); do
    api_port=$((BASE_API_PORT + i))
    raft_port=$((BASE_RAFT_PORT + i))

    echo -e "\n${CYAN}Node ${i}${NC} (API: ${api_port}, Raft: ${raft_port})"

    # Check if process exists
    pid_file="data/node${i}/node.pid"
    if [ -f "$pid_file" ]; then
        pid=$(cat "$pid_file")
        if kill -0 "$pid" 2>/dev/null; then
            echo -e "  ${GREEN}✓${NC} Process: Running (PID: ${pid})"
        else
            echo -e "  ${RED}✗${NC} Process: Not running (stale PID file)"
            continue
        fi
    else
        echo -e "  ${YELLOW}⚠${NC} Process: No PID file found"
    fi

    # Check health endpoint
    if check_node_health $api_port; then
        echo -e "  ${GREEN}✓${NC} Health: OK"
        ((healthy_nodes++))
    else
        echo -e "  ${RED}✗${NC} Health: Failed to connect"
        continue
    fi

    # Get node status
    status=$(get_node_status $api_port)
    if [ -n "$status" ]; then
        # Check role
        role=$(echo "$status" | grep -o '"role":"[^"]*"' | cut -d':' -f2 | tr -d '"')
        if [ "$role" = "leader" ]; then
            echo -e "  ${GREEN}✓${NC} Role: ${GREEN}LEADER${NC}"
            leader_id=$i
            leader_port=$api_port
        else
            echo -e "  ${CYAN}→${NC} Role: $role"
        fi

        # Show term if available
        term=$(echo "$status" | grep -o '"term":[0-9]*' | cut -d':' -f2)
        if [ -n "$term" ]; then
            echo -e "  ${CYAN}→${NC} Term: ${term}"
        fi
    fi

    # Check log file
    log_file="data/node${i}/node.log"
    if [ -f "$log_file" ]; then
        log_size=$(du -h "$log_file" | cut -f1)
        echo -e "  ${CYAN}→${NC} Log: ${log_file} (${log_size})"
    fi
done

print_header "Cluster Summary"

echo ""
echo -e "${CYAN}Overall Status:${NC}"
echo "  • Total nodes: ${TOTAL_NODES}"
echo "  • Healthy nodes: ${healthy_nodes}/${TOTAL_NODES}"

if [ -n "$leader_id" ]; then
    echo -e "  • Leader: ${GREEN}Node ${leader_id}${NC} (http://localhost:${leader_port})"
else
    echo -e "  • Leader: ${RED}No leader detected${NC}"
fi

print_header "Partition Table Status"

echo ""
# Check if partition table is initialized via logs
if check_partition_table_initialized; then
    print_success "Partition table initialized (${EXPECTED_PARTITIONS} partitions)"

    # Try to get version from logs
    for ((i=0; i<TOTAL_NODES; i++)); do
        log_file="data/node${i}/node.log"
        if [ -f "$log_file" ]; then
            version_line=$(grep "Updated partition table to version" "$log_file" 2>/dev/null | tail -1)
            if [ -n "$version_line" ]; then
                version=$(echo "$version_line" | grep -o "version [0-9]*" | cut -d' ' -f2)
                if [ -n "$version" ]; then
                    echo -e "${CYAN}→${NC} Partition table version: ${version}"
                    break
                fi
            fi
        fi
    done

    echo ""
    echo -e "${CYAN}Distribution:${NC}"
    echo "  • Even distribution across ${TOTAL_NODES} nodes"
    partitions_per_node=$((EXPECTED_PARTITIONS / TOTAL_NODES))
    echo "  • Approximately ${partitions_per_node} partitions per node"
else
    print_error "Partition table not initialized"
    print_info "Check logs for initialization errors"
fi

print_header "Recent Log Activity"

echo ""
for ((i=0; i<TOTAL_NODES; i++)); do
    log_file="data/node${i}/node.log"
    if [ -f "$log_file" ]; then
        echo -e "${CYAN}Node ${i} - Last 3 log entries:${NC}"
        tail -3 "$log_file" | while read line; do
            # Highlight errors and warnings
            if echo "$line" | grep -qi "error"; then
                echo -e "  ${RED}${line}${NC}"
            elif echo "$line" | grep -qi "warn"; then
                echo -e "  ${YELLOW}${line}${NC}"
            else
                echo -e "  ${GRAY}${line}${NC}"
            fi
        done
        echo ""
    fi
done

print_header "Quick Actions"

echo ""
echo -e "${CYAN}→ Run functionality tests:${NC}"
echo "  ./scripts/local/test-local.sh"
echo ""
echo -e "${CYAN}→ Stop cluster:${NC}"
echo "  ./scripts/local/stop-local.sh"
echo ""
echo -e "${CYAN}→ View live logs:${NC}"
echo "  tail -f data/node0/node.log"
echo ""
echo -e "${CYAN}→ Manual API test:${NC}"
if [ -n "$leader_port" ]; then
    echo "  curl -X POST http://localhost:${leader_port}/kv -H 'Content-Type: application/json' -d '{\"key\": 123, \"value\": 456}'"
    echo "  curl http://localhost:${leader_port}/kv/123"
else
    echo "  curl -X POST http://localhost:${BASE_API_PORT}/kv -H 'Content-Type: application/json' -d '{\"key\": 123, \"value\": 456}'"
    echo "  curl http://localhost:${BASE_API_PORT}/kv/123"
fi
echo ""
