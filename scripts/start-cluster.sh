#!/bin/bash
set -e

echo "🚀 Starting Raft Cluster..."
echo ""

# Create data directories if they don't exist
mkdir -p data/node0 data/node1 data/node2

echo "📦 Building Docker images..."
docker-compose build

echo ""
echo "🏃 Starting nodes..."
docker-compose up -d

echo ""
echo "⏳ Waiting for nodes to start..."
sleep 3

echo ""
echo "✅ Cluster Status:"
docker-compose ps

echo ""
echo "📊 To view logs:"
echo "  All nodes:     docker-compose logs -f"
echo "  Specific node: docker-compose logs -f raft-node-0"
echo ""
echo "🛑 To stop cluster:"
echo "  ./scripts/stop-cluster.sh"
echo ""
echo "🔍 Monitoring connections in 5 seconds..."
sleep 5

echo ""
echo "Recent logs from all nodes:"
docker-compose logs --tail=20
