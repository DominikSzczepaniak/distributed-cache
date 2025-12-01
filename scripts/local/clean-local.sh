#!/bin/bash

# clean-local.sh - Clean all local distributed cache data
# Features:
# - Stops all nodes first (if running)
# - Removes data directories
# - Cleans log files
# - Removes binaries (optional)
# - Provides fresh slate for restart

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
# Parse Arguments
# =============================================================================

CLEAN_BINARIES=false

while [[ $# -gt 0 ]]; do
    case $1 in
        --binaries)
            CLEAN_BINARIES=true
            shift
            ;;
        -h|--help)
            echo "Usage: $0 [OPTIONS]"
            echo ""
            echo "Clean all local distributed cache data"
            echo ""
            echo "Options:"
            echo "  --binaries    Also remove compiled binaries"
            echo "  -h, --help    Show this help message"
            echo ""
            exit 0
            ;;
        *)
            echo "Unknown option: $1"
            echo "Use --help for usage information"
            exit 1
            ;;
    esac
done

# =============================================================================
# Main Script
# =============================================================================

print_header "Cleaning Local Distributed Cache"

# Check if any processes are running
running_count=$(pgrep -f "./raftnode" 2>/dev/null | wc -l | tr -d ' ')

if [ "$running_count" -gt "0" ]; then
    print_warning "${running_count} raftnode process(es) running"
    print_info "Stopping nodes first..."

    # Run stop script if it exists
    if [ -f "./scripts/local/stop-local.sh" ]; then
        ./scripts/local/stop-local.sh
    else
        # Fallback to pkill
        pkill -TERM -f "./raftnode" 2>/dev/null || true
        sleep 2
        pkill -KILL -f "./raftnode" 2>/dev/null || true
    fi

    print_success "All processes stopped"
fi

# Clean data directories
print_header "Removing Data Directories"

removed_count=0
for ((i=0; i<TOTAL_NODES; i++)); do
    data_dir="data/node${i}"
    if [ -d "$data_dir" ]; then
        rm -rf "$data_dir"
        print_success "Removed: ${data_dir}"
        ((removed_count++))
    fi
done

# Also clean any other data directory remnants
if [ -d "data" ]; then
    # Remove any remaining files in data directory
    if [ -n "$(ls -A data 2>/dev/null)" ]; then
        print_info "Cleaning additional data directory contents..."
        rm -rf data/*
    fi
else
    print_info "No data directory found"
fi

if [ $removed_count -gt 0 ]; then
    print_success "Removed ${removed_count} data director(ies)"
else
    print_info "No data directories to remove"
fi

# Clean binaries if requested
if [ "$CLEAN_BINARIES" = true ]; then
    print_header "Removing Binaries"

    binaries_removed=0

    if [ -f "./raftnode" ]; then
        rm -f ./raftnode
        print_success "Removed: raftnode"
        ((binaries_removed++))
    fi

    if [ -f "./raftcli" ]; then
        rm -f ./raftcli
        print_success "Removed: raftcli"
        ((binaries_removed++))
    fi

    if [ $binaries_removed -gt 0 ]; then
        print_success "Removed ${binaries_removed} binar(ies)"
    else
        print_info "No binaries to remove"
    fi
fi

# Clean PID files
print_header "Cleaning PID Files"

pid_files_removed=0
for ((i=0; i<TOTAL_NODES; i++)); do
    pid_file="data/node${i}/node.pid"
    if [ -f "$pid_file" ]; then
        rm -f "$pid_file"
        ((pid_files_removed++))
    fi
done

if [ $pid_files_removed -gt 0 ]; then
    print_success "Removed ${pid_files_removed} PID file(s)"
else
    print_info "No PID files to remove"
fi

# Summary
print_header "Cleanup Complete"

echo ""
print_success "All local data cleaned"
echo ""

if [ "$CLEAN_BINARIES" = true ]; then
    print_info "Binaries removed - rebuild with: go build -o raftnode ./cmd/raftnode/main.go"
    echo ""
fi

echo -e "${CYAN}→ Next Steps:${NC}"
echo "  # Start fresh cluster"
echo "  ./scripts/local/start-local.sh"
echo ""
echo "  # Or rebuild binaries first"
echo "  go build -o raftnode ./cmd/raftnode/main.go"
echo "  go build -o raftcli ./cmd/raftcli/main.go"
echo ""

# Show what's left
if [ -d "data" ] && [ -z "$(ls -A data 2>/dev/null)" ]; then
    # Remove empty data directory
    rmdir data 2>/dev/null || true
fi

remaining=$(find . -maxdepth 2 -name "node*.log" -o -name "node*.pid" 2>/dev/null | wc -l | tr -d ' ')
if [ "$remaining" -gt "0" ]; then
    print_warning "Some files may remain:"
    find . -maxdepth 2 -name "node*.log" -o -name "node*.pid" 2>/dev/null || true
fi
