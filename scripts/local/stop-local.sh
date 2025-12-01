#!/bin/bash

# stop-local.sh - Stop all local distributed cache nodes
# Features:
# - Gracefully stops all raftnode processes
# - Shows what was stopped
# - Cleans up PID files

set -e

# =============================================================================
# Configuration
# =============================================================================

TOTAL_NODES=${TOTAL_NODES:-3}

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

# =============================================================================
# Main Script
# =============================================================================

print_header "Stopping Distributed Cache Cluster"

# Check if any raftnode processes are running
running_count=$(pgrep -f "./raftnode" | wc -l | tr -d ' ')

if [ "$running_count" -eq "0" ]; then
    print_warning "No raftnode processes found running"
    exit 0
fi

print_info "Found ${running_count} raftnode process(es) running"

# Try to stop processes gracefully using PID files first
stopped_from_pid=0
for ((i=0; i<TOTAL_NODES; i++)); do
    pid_file="data/node${i}/node.pid"
    if [ -f "$pid_file" ]; then
        pid=$(cat "$pid_file")
        if kill -0 "$pid" 2>/dev/null; then
            print_info "Stopping Node ${i} (PID: ${pid})..."
            if kill "$pid" 2>/dev/null; then
                ((stopped_from_pid++))
            fi
        fi
        rm -f "$pid_file"
    fi
done

if [ $stopped_from_pid -gt 0 ]; then
    print_success "Stopped ${stopped_from_pid} node(s) gracefully"
    # Give processes time to shut down
    sleep 2
fi

# Check if any processes still running
still_running=$(pgrep -f "./raftnode" | wc -l | tr -d ' ')

if [ "$still_running" -gt "0" ]; then
    print_warning "Some processes still running, using pkill..."
    if pkill -TERM -f "./raftnode" 2>/dev/null; then
        sleep 2
        # Force kill if still running
        if pgrep -f "./raftnode" > /dev/null 2>&1; then
            print_warning "Force killing remaining processes..."
            pkill -KILL -f "./raftnode" 2>/dev/null || true
        fi
    fi
fi

# Clean up any remaining PID files
for ((i=0; i<TOTAL_NODES; i++)); do
    rm -f "data/node${i}/node.pid" 2>/dev/null || true
done

# Final check
final_count=$(pgrep -f "./raftnode" 2>/dev/null | wc -l | tr -d ' ')

if [ "$final_count" -eq "0" ]; then
    print_success "All raftnode processes stopped successfully"
    echo ""
    print_info "Data preserved in data/nodeX/ directories"
    print_info "Use './scripts/local/clean-local.sh' to remove all data"
    print_info "Use './scripts/local/start-local.sh' to restart cluster"
    echo ""
else
    print_error "Failed to stop some processes"
    print_info "Remaining processes:"
    pgrep -af "./raftnode" || true
    exit 1
fi
