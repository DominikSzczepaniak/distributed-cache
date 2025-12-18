#!/bin/bash

# This script tests how RPS changes based on number of datanodes with and without replication

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(cd "$SCRIPT_DIR/../.." && pwd)"
cd "$PROJECT_DIR"

CONTROLLER_URL="http://localhost:8080"

cleanup() {
    echo ""
    echo "Cleaning up..."
    docker-compose down -v 2>/dev/null || true
    rm -rf data/node0 data/node1 data/node2 2>/dev/null || true
}
trap cleanup EXIT

echo "╔═══════════════════════════════════════════════════════════════╗"
echo "║         Multi-Datanode Scaling Benchmark                      ║"
echo "╚═══════════════════════════════════════════════════════════════╝"
echo ""

# Store results
RESULTS_NO_REPL=""
RESULTS_WITH_REPL=""

run_benchmark() {
    local NUM_DATANODES=$1
    local WITH_REPLICATION=$2
    
    if [ "$WITH_REPLICATION" = "true" ]; then
        REPL_TEXT="WITH REPLICATION"
    else
        REPL_TEXT="NO REPLICATION"
    fi
    
    echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
    echo "  Testing with $NUM_DATANODES datanode(s) - $REPL_TEXT"
    echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
    
    docker-compose down -v 2>/dev/null || true
    rm -rf data/node0 data/node1 data/node2 2>/dev/null || true
    mkdir -p data/node0 data/node1 data/node2
    
    # Start services
    SERVICES="controller-0 controller-1 controller-2"
    for i in $(seq 1 $NUM_DATANODES); do
        SERVICES="$SERVICES datanode-$i"
    done
    docker-compose up -d --build $SERVICES 2>&1 | tail -3
    
    echo "Waiting for datanodes..."
    for i in $(seq 1 $NUM_DATANODES); do
        PORT=$((9009 + i))
        for j in {1..30}; do
            if curl -s "http://localhost:$PORT/health" >/dev/null 2>&1; then
                echo "Datanode $i (port $PORT) ready!"
                break
            fi
            sleep 1
        done
    done
    sleep 5
    
    # Build topology JSON
    NODES_JSON=""
    for i in $(seq 1 $NUM_DATANODES); do
        NODE_ID="datanode-$i:9000"
        if [ $i -gt 1 ]; then NODES_JSON="$NODES_JSON,"; fi
        NODES_JSON="$NODES_JSON\"$NODE_ID\":{\"ID\":\"$NODE_ID\",\"Address\":\"$NODE_ID\",\"Status\":\"Active\"}"
    done
    
    # Build shards - distribute 10 shards across datanodes
    SHARDS_JSON=""
    for s in $(seq 0 9); do
        NODE_IDX=$(( (s % NUM_DATANODES) + 1 ))
        NODE_ID="datanode-$NODE_IDX:9000"
        
        # Build replica list if replication is enabled
        REPLICAS="[]"
        if [ "$WITH_REPLICATION" = "true" ] && [ $NUM_DATANODES -gt 1 ]; then
            REPLICA_LIST=""
            for r in $(seq 1 $NUM_DATANODES); do
                if [ $r -ne $NODE_IDX ]; then
                    if [ -n "$REPLICA_LIST" ]; then REPLICA_LIST="$REPLICA_LIST,"; fi
                    REPLICA_LIST="$REPLICA_LIST\"datanode-$r:9000\""
                fi
            done
            REPLICAS="[$REPLICA_LIST]"
        fi
        
        if [ $s -gt 0 ]; then SHARDS_JSON="$SHARDS_JSON,"; fi
        SHARDS_JSON="$SHARDS_JSON\"$s\":{\"ID\":$s,\"PrimaryID\":\"$NODE_ID\",\"ReplicaIDs\":$REPLICAS,\"Status\":\"Active\"}"
    done
    
    CONFIG="{\"Epoch\":100,\"TotalShards\":10,\"Nodes\":{$NODES_JSON},\"Shards\":{$SHARDS_JSON}}"
    
    echo "Configuring topology ($REPL_TEXT)..."
    curl -s -X POST -H "Content-Type: application/json" -d "$CONFIG" "$CONTROLLER_URL/debug/config" >/dev/null 2>&1
    sleep 3
    
    # Run benchmark
    echo "Running benchmark..."
    RPS_OUT=$(go run "$SCRIPT_DIR/benchmark_rps.go" 2>&1 | grep "RPS:" | awk '{print $2}')
    
    if [ -n "$RPS_OUT" ]; then
        RPS_INT=$(printf "%.0f" "$RPS_OUT")
        echo "Result: $RPS_INT RPS"
        echo "$RPS_INT"
    else
        echo "0"
    fi
}

# Test WITHOUT replication
echo "========== PHASE 1: NO REPLICATION =========="
RPS_1_NO=$(run_benchmark 1 "false" | tail -1)
RPS_2_NO=$(run_benchmark 2 "false" | tail -1)
RPS_3_NO=$(run_benchmark 3 "false" | tail -1)

# Test WITH replication
echo ""
echo "========== PHASE 2: WITH REPLICATION =========="
RPS_2_WITH=$(run_benchmark 2 "true" | tail -1)
RPS_3_WITH=$(run_benchmark 3 "true" | tail -1)

echo ""
echo "╔═══════════════════════════════════════════════════════════════╗"
echo "║                    FINAL RESULTS SUMMARY                      ║"
echo "╠═══════════════════════════════════════════════════════════════╣"
printf "║  %-35s %12s RPS  ║\n" "1 datanode (no replication):" "$RPS_1_NO"
printf "║  %-35s %12s RPS  ║\n" "2 datanodes (no replication):" "$RPS_2_NO"
printf "║  %-35s %12s RPS  ║\n" "3 datanodes (no replication):" "$RPS_3_NO"
printf "║  %-35s %12s RPS  ║\n" "2 datanodes (WITH replication):" "$RPS_2_WITH"
printf "║  %-35s %12s RPS  ║\n" "3 datanodes (WITH replication):" "$RPS_3_WITH"
echo "╚═══════════════════════════════════════════════════════════════╝"
