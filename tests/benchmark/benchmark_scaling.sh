#!/bin/bash

# This script tests how RPS changes based on number of concurrent clients

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(cd "$SCRIPT_DIR/../.." && pwd)"
cd "$PROJECT_DIR"

CONTROLLER_URL="http://localhost:8080"
DATANODE_URL="http://localhost:9010"

cleanup() {
    echo ""
    echo "Cleaning up..."
    docker-compose down -v 2>/dev/null || true
    rm -rf data/node0 data/node1 data/node2 2>/dev/null || true
}
trap cleanup EXIT

echo "╔═══════════════════════════════════════════════════════════════╗"
echo "║            Client Scaling Benchmark                           ║"
echo "╚═══════════════════════════════════════════════════════════════╝"
echo ""

echo "Starting cluster..."
docker-compose down -v 2>/dev/null || true
rm -rf data/node0 data/node1 data/node2 2>/dev/null || true
mkdir -p data/node0 data/node1 data/node2
docker-compose up -d --build controller-0 controller-1 controller-2 datanode-1 2>&1 | tail -3

echo "Waiting for datanode..."
for i in {1..60}; do
    if curl -s "$DATANODE_URL/health" >/dev/null 2>&1; then
        echo "Datanode ready!"
        break
    fi
    sleep 1
done

echo "Configuring topology..."
curl -s -X POST -H "Content-Type: application/json" \
    -d '{"Epoch":100,"TotalShards":1,"Nodes":{"datanode-1:9000":{"ID":"datanode-1:9000","Address":"datanode-1:9000","Status":"Active"}},"Shards":{"0":{"ID":0,"PrimaryID":"datanode-1:9000","ReplicaIDs":[],"Status":"Active"}}}' \
    "$CONTROLLER_URL/debug/config" >/dev/null 2>&1
sleep 3

echo ""
go run "$SCRIPT_DIR/benchmark_scaling.go"
