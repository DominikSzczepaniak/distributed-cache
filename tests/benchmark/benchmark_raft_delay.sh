#!/bin/bash

# This script analyzes the impact of Raft delay on controller consensus performance.

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(cd "$SCRIPT_DIR/../.." && pwd)"
cd "$PROJECT_DIR"

CONTROLLER_URL="http://localhost:8080"

DELAYS=("0ms" "10ms" "25ms" "50ms")

cleanup() {
    echo ""
    echo "Cleaning up..."
    docker-compose down -v 2>/dev/null || true
    rm -rf data/node0 data/node1 data/node2 2>/dev/null || true
}
trap cleanup EXIT

echo "╔═══════════════════════════════════════════════════════════════╗"
echo "║        Raft Consensus Performance Impact Analysis             ║"
echo "╚═══════════════════════════════════════════════════════════════╝"
echo ""

RESULTS=()

for DELAY in "${DELAYS[@]}"; do
    echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
    echo "  Testing with Raft Artificial Delay: $DELAY"
    echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
    
    export RAFT_ARTIFICIAL_DELAY=$DELAY
    docker-compose down -v 2>/dev/null || true
    rm -rf data/node0 data/node1 data/node2 2>/dev/null || true
    mkdir -p data/node0 data/node1 data/node2
    
    docker-compose up -d --build controller-0 controller-1 controller-2 2>&1 | tail -3
    
    echo "Waiting for cluster to stabilize..."
    for i in {1..30}; do
        if curl -s "$CONTROLLER_URL/topology" >/dev/null 2>&1; then
            echo "Controller ready!"
            break
        fi
        sleep 1
    done
    sleep 15
    
    RPS_OUTPUT=$(go run "$SCRIPT_DIR/benchmark_raft_noop.go" 2>&1)
    echo "$RPS_OUTPUT"
    RPS=$(echo "$RPS_OUTPUT" | grep "RPS:" | awk '{print $2}')
    
    echo "Result for $DELAY: $RPS RPS"
    RESULTS+=("$DELAY|$RPS")
    echo ""
done

echo "╔═══════════════════════════════════════════════════════════════╗"
echo "║                    FINAL ANALYSIS RESULTS                     ║"
echo "╠═══════════════════════════════════════════════════════════════╣"
printf "║  %-20s %20s          ║\n" "Artificial Delay" "Consensus RPS"
for entry in "${RESULTS[@]}"; do
    IFS='|' read -r d r <<< "$entry"
    printf "║  %-20s %20s          ║\n" "$d" "$r"
done
echo "╚═══════════════════════════════════════════════════════════════╝"
echo ""
echo "Note: Consensus RPS measures how many Raft commands per second"
echo "      can be committed with the given artificial delay."
