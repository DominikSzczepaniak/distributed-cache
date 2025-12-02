#!/bin/bash
# Quick test script for the distributed cache cluster

set -e

echo "🧪 Testing Distributed Cache Cluster"
echo ""

# Test 1: Check Raft nodes
echo "1. Checking Raft nodes..."
for port in 8080 8081 8082; do
    if curl -s http://localhost:$port/health > /dev/null 2>&1; then
        echo "   ✓ Raft node on :$port is healthy"
    else
        echo "   ✗ Raft node on :$port is down"
    fi
done

# Test 2: Check Workers
echo ""
echo "2. Checking Worker nodes..."
for port in 7000 7001 7002; do
    if curl -s http://localhost:$port/health > /dev/null 2>&1; then
        echo "   ✓ Worker on :$port is healthy"
    else
        echo "   ✗ Worker on :$port is down"
    fi
done

# Test 3: Check leader
echo ""
echo "3. Checking Raft leader..."
LEADER_STATUS=$(curl -s http://localhost:8080/status)
echo "   Node 0: $LEADER_STATUS"

# Test 4: PUT request
echo ""
echo "4. Testing PUT request (key=123, value=456)..."
RESULT=$(curl -v -L -X POST http://localhost:8080/kv \
  -H 'Content-Type: application/json' \
  -d '{"key": 123, "value": 456}' 2>&1)

if echo "$RESULT" | grep -q "307"; then
    echo "   ✓ Received HTTP 307 redirect (routing works!)"
    LOCATION=$(echo "$RESULT" | grep -i "< Location:" | cut -d' ' -f3 | tr -d '\r')
    echo "   → Redirect to: $LOCATION"
elif echo "$RESULT" | grep -q "200"; then
    echo "   ✓ Request completed successfully"
else
    echo "   ✗ Request failed"
    echo "$RESULT" | grep -E "(< HTTP|Partition)"
fi

# Test 5: GET request
echo ""
echo "5. Testing GET request (key=123)..."
GET_RESULT=$(curl -L http://localhost:8080/kv/123 2>&1)
if echo "$GET_RESULT" | grep -q "123"; then
    echo "   ✓ GET successful: $GET_RESULT"
else
    echo "   ✗ GET failed: $GET_RESULT"
fi

echo ""
echo "Testing complete!"
