#!/bin/bash

# stop-cluster-with-workers.sh - Stop distributed cache cluster

set -e

# Colors
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
CYAN='\033[0;36m'
NC='\033[0m'

echo -e "${CYAN}Stopping distributed cache cluster...${NC}\n"

# Stop Raft nodes
if pkill -f "bin/raftnode" 2>/dev/null; then
    echo -e "${GREEN}✓${NC} Stopped Raft nodes"
else
    echo -e "${YELLOW}⚠${NC} No Raft nodes running"
fi

# Stop Worker nodes
if pkill -f "bin/worker" 2>/dev/null; then
    echo -e "${GREEN}✓${NC} Stopped Worker nodes"
else
    echo -e "${YELLOW}⚠${NC} No Worker nodes running"
fi

# Wait for processes to terminate
sleep 1

# Check if any processes still running
RAFT_COUNT=$(pgrep -f "bin/raftnode" | wc -l)
WORKER_COUNT=$(pgrep -f "bin/worker" | wc -l)

if [ $RAFT_COUNT -eq 0 ] && [ $WORKER_COUNT -eq 0 ]; then
    echo -e "\n${GREEN}✓ All processes stopped successfully${NC}"
else
    echo -e "\n${YELLOW}⚠ Some processes may still be running:${NC}"
    [ $RAFT_COUNT -gt 0 ] && echo "  Raft nodes: $RAFT_COUNT"
    [ $WORKER_COUNT -gt 0 ] && echo "  Workers: $WORKER_COUNT"
    echo ""
    echo "To force kill: pkill -9 -f bin/raftnode && pkill -9 -f bin/worker"
fi

echo ""
echo -e "${CYAN}→ Logs preserved in:${NC}"
echo "  data/node*/node.log"
echo "  data/worker*/worker.log"
echo ""
echo -e "${CYAN}→ To clean data:${NC}"
echo "  rm -rf data/"
echo ""
