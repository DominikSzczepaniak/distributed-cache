#!/bin/bash
set -e

echo "🛑 Stopping Raft Cluster..."
echo ""

echo "📊 Current cluster status:"
docker compose ps

echo ""
echo "Stopping all nodes..."
docker compose down

echo ""
echo "✅ Cluster stopped successfully"
echo ""
echo "💡 To remove all data: rm -rf data/"
echo "💡 To start cluster: ./scripts/start-cluster.sh"
