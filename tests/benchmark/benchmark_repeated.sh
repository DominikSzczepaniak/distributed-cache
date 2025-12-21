#!/bin/bash

# This script runs benchmark_comparison.sh multiple times and calculates average RPS.

set -e

ITERATIONS=${1:-10}
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(cd "$SCRIPT_DIR/../.." && pwd)"
COMPARISON_SCRIPT="$SCRIPT_DIR/benchmark_comparison.sh"

cd "$PROJECT_DIR"

echo "╔═══════════════════════════════════════════════════════════════╗"
echo "║         Repeated Distributed Cache Benchmark                  ║"
echo "║   Running $ITERATIONS iterations of benchmark_comparison.sh      ║"
echo "╚═══════════════════════════════════════════════════════════════╝"
echo ""

TOTAL_YOUR_RPS=0
TOTAL_REDIS_RPS=0
TOTAL_ETCD_RPS=0
VALID_RUNS=0

for ((i=1; i<=ITERATIONS; i++)); do
    echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
    echo "  RUN $i of $ITERATIONS"
    echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
    
    OUTPUT=$(bash "$COMPARISON_SCRIPT" 2>&1)
    
    YOUR_RPS=$(echo "$OUTPUT" | grep "My Distributed Cache:" | sed 's/.*: *\([0-9]*\).*/\1/')
    REDIS_RPS=$(echo "$OUTPUT" | grep "Redis (in-memory):" | sed 's/.*: *\([0-9]*\).*/\1/')
    ETCD_RPS=$(echo "$OUTPUT" | grep "etcd (durable KV):" | sed 's/.*: *\([0-9]*\).*/\1/')

    echo ""
    echo "Run $i Results:"
    echo "  My Cache: $YOUR_RPS RPS"
    echo "  Redis:    $REDIS_RPS RPS"
    echo "  etcd:     $ETCD_RPS RPS"
    
    if [[ -n "$YOUR_RPS" && -n "$REDIS_RPS" && -n "$ETCD_RPS" ]]; then
        TOTAL_YOUR_RPS=$(echo "$TOTAL_YOUR_RPS + $YOUR_RPS" | bc)
        TOTAL_REDIS_RPS=$(echo "$TOTAL_REDIS_RPS + $REDIS_RPS" | bc)
        TOTAL_ETCD_RPS=$(echo "$TOTAL_ETCD_RPS + $ETCD_RPS" | bc)
        VALID_RUNS=$((VALID_RUNS + 1))
    else
        echo "Warning: Could not parse all RPS values for run $i. Skipping from average."
    fi
    echo ""
done

if [ "$VALID_RUNS" -gt 0 ]; then
    AVG_YOUR_RPS=$(echo "scale=2; $TOTAL_YOUR_RPS / $VALID_RUNS" | bc)
    AVG_REDIS_RPS=$(echo "scale=2; $TOTAL_REDIS_RPS / $VALID_RUNS" | bc)
    AVG_ETCD_RPS=$(echo "scale=2; $TOTAL_ETCD_RPS / $VALID_RUNS" | bc)

    echo "╔═══════════════════════════════════════════════════════════════╗"
    echo "║                    AVERAGE RESULTS ($VALID_RUNS runs)              ║"
    echo "╠═══════════════════════════════════════════════════════════════╣"
    printf "║  %-30s %10s RPS          ║\n" "My Distributed Cache:" "$AVG_YOUR_RPS"
    printf "║  %-30s %10s RPS          ║\n" "Redis (in-memory):" "$AVG_REDIS_RPS"
    printf "║  %-30s %10s RPS          ║\n" "etcd (durable KV):" "$AVG_ETCD_RPS"
    echo "╚═══════════════════════════════════════════════════════════════╝"
else
    echo "No valid runs completed."
fi
