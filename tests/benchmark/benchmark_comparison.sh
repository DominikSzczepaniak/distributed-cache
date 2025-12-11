#!/bin/bash

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(cd "$SCRIPT_DIR/../.." && pwd)"
cd "$PROJECT_DIR"

cleanup() {
    echo ""
    echo "Cleaning up..."
    docker-compose down -v 2>/dev/null || true
    docker stop etcd-benchmark redis-benchmark 2>/dev/null || true
    docker rm etcd-benchmark redis-benchmark 2>/dev/null || true
    rm -rf data/node0 data/node1 data/node2 2>/dev/null || true
}
trap cleanup EXIT

echo "╔═══════════════════════════════════════════════════════════════╗"
echo "║         Distributed Cache Comparison Benchmark                ║"
echo "║   My Cache vs Redis (in-memory) vs etcd (durable KV store)   ║"
echo "╚═══════════════════════════════════════════════════════════════╝"
echo ""

echo "Installing dependencies..."
go get go.etcd.io/etcd/client/v3@v3.5.11 2>/dev/null || true
go get github.com/redis/go-redis/v9 2>/dev/null || true

echo ""
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "  BENCHMARK 1: My Distributed Cache (in-memory)"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo ""

echo "Starting my cluster..."
docker-compose down -v 2>/dev/null || true
rm -rf data/node0 data/node1 data/node2 2>/dev/null || true
mkdir -p data/node0 data/node1 data/node2
docker-compose up -d --build controller-0 controller-1 controller-2 datanode-1 2>&1 | tail -3

echo "Waiting for datanode..."
for i in {1..60}; do
    if curl -s "http://localhost:9010/health" >/dev/null 2>&1; then
        echo "Datanode ready!"
        break
    fi
    sleep 1
done

curl -s -X POST -H "Content-Type: application/json" \
    -d '{"Epoch":100,"TotalShards":1,"Nodes":{"datanode-1:9000":{"ID":"datanode-1:9000","Address":"datanode-1:9000","Status":"Active"}},"Shards":{"0":{"ID":0,"PrimaryID":"datanode-1:9000","ReplicaIDs":[],"Status":"Active"}}}' \
    "http://localhost:8080/debug/config" >/dev/null 2>&1
sleep 3

echo "Running benchmark..."
YOUR_RESULT=$(go run "$SCRIPT_DIR/benchmark_rps.go" 2>&1)
echo "$YOUR_RESULT"
YOUR_RPS=$(echo "$YOUR_RESULT" | grep "RPS:" | awk '{print $2}')

echo "Stopping my cluster..."
docker-compose down -v 2>/dev/null || true

echo ""
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "  BENCHMARK 2: Redis (in-memory cache)"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo ""

echo "Starting Redis..."
docker run -d --name redis-benchmark -p 6379:6379 redis:7-alpine >/dev/null 2>&1

echo "Waiting for Redis..."
for i in {1..30}; do
    if docker exec redis-benchmark redis-cli ping 2>/dev/null | grep -q "PONG"; then
        echo "Redis ready!"
        break
    fi
    sleep 1
done

REDIS_RESULT=$(go run "$SCRIPT_DIR/benchmark_redis.go" 2>&1)
echo "$REDIS_RESULT"
REDIS_RPS=$(echo "$REDIS_RESULT" | grep "RPS:" | awk '{print $2}')

docker stop redis-benchmark >/dev/null 2>&1 || true
docker rm redis-benchmark >/dev/null 2>&1 || true

echo ""
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "  BENCHMARK 3: etcd (durable key-value store)"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo ""

echo "Starting etcd..."
docker run -d --name etcd-benchmark \
    -p 12379:2379 \
    gcr.io/etcd-development/etcd:v3.5.11 \
    /usr/local/bin/etcd \
    --advertise-client-urls http://0.0.0.0:2379 \
    --listen-client-urls http://0.0.0.0:2379 \
    >/dev/null 2>&1

echo "Waiting for etcd..."
for i in {1..30}; do
    if curl -s "http://localhost:12379/health" 2>/dev/null | grep -q "true"; then
        echo "etcd ready!"
        break
    fi
    sleep 1
done

ETCD_RESULT=$(go run "$SCRIPT_DIR/benchmark_etcd.go" 2>&1)
echo "$ETCD_RESULT"
ETCD_RPS=$(echo "$ETCD_RESULT" | grep "RPS:" | awk '{print $2}')

echo ""
echo "╔═══════════════════════════════════════════════════════════════╗"
echo "║                    COMPARISON RESULTS                         ║"
echo "╠═══════════════════════════════════════════════════════════════╣"
printf "║  %-30s %10s RPS          ║\n" "My Distributed Cache:" "$YOUR_RPS"
printf "║  %-30s %10s RPS          ║\n" "Redis (in-memory):" "$REDIS_RPS"
printf "║  %-30s %10s RPS          ║\n" "etcd (durable KV):" "$ETCD_RPS"
echo "╠═══════════════════════════════════════════════════════════════╣"
echo "║  Note: Redis & My Cache = in-memory (no durability)          ║"
echo "║        etcd = Raft consensus + disk sync (durable)           ║"
echo "╚═══════════════════════════════════════════════════════════════╝"
echo ""
