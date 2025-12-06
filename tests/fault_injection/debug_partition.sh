#!/bin/bash
# Debug script for network partition test

cd "$(dirname "$0")/../.."
LOG_FILE="./partition_debug.log"

echo "=== Network Partition Debug Test ===" > "$LOG_FILE"
echo "Started at: $(date)" >> "$LOG_FILE"
echo "Working dir: $(pwd)" >> "$LOG_FILE"

# Cleanup
echo "" >> "$LOG_FILE"
echo "=== Step 1: Cleanup ===" >> "$LOG_FILE"
docker-compose down -v >> "$LOG_FILE" 2>&1
rm -rf data/node0 data/node1 data/node2
mkdir -p data/node0 data/node1 data/node2
echo "Cleanup complete" >> "$LOG_FILE"

# Start cluster
echo "" >> "$LOG_FILE"
echo "=== Step 2: Starting cluster ===" >> "$LOG_FILE"
docker-compose up -d --build controller-0 controller-1 controller-2 >> "$LOG_FILE" 2>&1
echo "Waiting 20 seconds for cluster to stabilize..." >> "$LOG_FILE"
sleep 20

# Check election timer started
echo "" >> "$LOG_FILE"
echo "=== Step 3: Check election timer started ===" >> "$LOG_FILE"
docker-compose logs 2>&1 | grep "Election timer loop started" >> "$LOG_FILE"

# Find and show current leader
echo "" >> "$LOG_FILE"
echo "=== Step 4: Find current leader ===" >> "$LOG_FILE"
docker-compose logs 2>&1 | grep "became leader" >> "$LOG_FILE"

# Check recent LogRequest activity
echo "" >> "$LOG_FILE"
echo "=== Step 5: Recent LogRequest activity ===" >> "$LOG_FILE"
docker-compose logs 2>&1 | grep "LogRequest" | tail -10 >> "$LOG_FILE"

# Isolate leader (assuming controller-0 is leader based on patterns)
LEADER=$(docker-compose logs 2>&1 | grep "became leader" | tail -1 | awk '{print $1}' | tr -d ' ' | cut -d'|' -f1)
echo "" >> "$LOG_FILE"
echo "=== Step 6: Isolating leader: $LEADER ===" >> "$LOG_FILE"
docker network disconnect raft-cluster "$LEADER" >> "$LOG_FILE" 2>&1
echo "Leader isolated at: $(date)" >> "$LOG_FILE"

# Wait and monitor
echo "" >> "$LOG_FILE"
echo "=== Step 7: Waiting 30 seconds for new election ===" >> "$LOG_FILE"
sleep 30

# Check for election timer activity on remaining nodes
echo "" >> "$LOG_FILE"
echo "=== Step 8: Election timer activity after partition ===" >> "$LOG_FILE"
for node in controller-0 controller-1 controller-2; do
    echo "--- $node ---" >> "$LOG_FILE"
    docker-compose logs "$node" 2>&1 | grep -E "Election timer|Starting election" | tail -10 >> "$LOG_FILE"
done

# Check if new leader was elected
echo "" >> "$LOG_FILE"
echo "=== Step 9: All 'became leader' messages ===" >> "$LOG_FILE"
docker-compose logs 2>&1 | grep "became leader" >> "$LOG_FILE"

# Count elections
echo "" >> "$LOG_FILE"
echo "=== Step 10: Election count summary ===" >> "$LOG_FILE"
ELECTION_COUNT=$(docker-compose logs 2>&1 | grep -c "became leader")
echo "Total 'became leader' events: $ELECTION_COUNT" >> "$LOG_FILE"

# Cleanup
echo "" >> "$LOG_FILE"
echo "=== Step 11: Cleanup ===" >> "$LOG_FILE"
docker-compose down -v >> "$LOG_FILE" 2>&1

echo "" >> "$LOG_FILE"
echo "=== Debug complete at: $(date) ===" >> "$LOG_FILE"
echo "Log written to: $LOG_FILE"
