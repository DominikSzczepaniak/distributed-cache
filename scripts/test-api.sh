#!/bin/bash
set -e

API_URL="${1:-http://localhost:8080}"

echo "🧪 Testing Raft HTTP API at $API_URL"
echo ""

echo "1️⃣ Testing Health Check..."
curl -s $API_URL/health | jq . || echo "Health check failed"
echo ""

echo "2️⃣ Testing Status..."
curl -s $API_URL/status | jq . || echo "Status check failed"
echo ""

echo "3️⃣ Testing Leader Info..."
curl -s $API_URL/leader | jq . || echo "Leader check failed"
echo ""

echo "4️⃣ Testing PUT operation..."
curl -s -X POST $API_URL/kv \
  -H "Content-Type: application/json" \
  -H "Idempotency-Key: test-$(date +%s)" \
  -d '{"key":1,"value":100}' | jq . || echo "PUT failed"
echo ""

echo "5️⃣ Testing GET operation..."
curl -s $API_URL/kv/1 | jq . || echo "GET failed"
echo ""

echo "6️⃣ Testing PUT with another key..."
curl -s -X POST $API_URL/kv \
  -H "Content-Type: application/json" \
  -H "Idempotency-Key: test-$(date +%s)" \
  -d '{"key":42,"value":999}' | jq . || echo "PUT failed"
echo ""

echo "7️⃣ Testing GET for second key..."
curl -s $API_URL/kv/42 | jq . || echo "GET failed"
echo ""

echo "8️⃣ Testing stale read..."
curl -s "$API_URL/kv/1?stale=true" | jq . || echo "Stale read failed"
echo ""

echo "9️⃣ Testing DELETE operation..."
curl -s -X DELETE $API_URL/kv/1 | jq . || echo "DELETE failed"
echo ""

echo "🔟 Verifying DELETE (should return not found or value 0)..."
curl -s $API_URL/kv/1 | jq . || echo "Verification failed"
echo ""

echo "✅ All tests completed!"
echo ""
echo "💡 To test other nodes:"
echo "  ./scripts/test-api.sh http://localhost:8081"
echo "  ./scripts/test-api.sh http://localhost:8082"
