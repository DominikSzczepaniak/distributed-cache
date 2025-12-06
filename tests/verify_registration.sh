#!/bin/bash
set -e

echo "--- 🚀 Starting Cluster ---"
docker-compose up -d --build controller-0 controller-1 controller-2

echo "Waiting for cluster to stabilize (15s)..."
sleep 15

echo "--- ✍️  Registering 'test-node' ---"
curl -v -f -X POST -H "Content-Type: application/json" \
    -d '{"node_id": "test-node", "address": "1.2.3.4:9000"}' \
    "http://localhost:8080/cluster/register"

echo "--- ✅ Registration successful ---"

# echo "--- 🧹 Cleaning up ---"
# docker-compose down -v
