#!/bin/bash
set -e

echo "🧹 Cleaning Raft Data..."
echo ""

if docker compose ps | grep -q "Up"; then
    echo "⚠️  Cluster is still running. Stopping first..."
    docker compose down
    echo ""
fi

echo "Removing data directories..."
rm -rf data/

echo "✅ Data cleaned successfully"
echo ""
echo "💡 To start fresh cluster: ./scripts/start-cluster.sh"
