#!/bin/bash

# test-routing.sh - Test Stage 3 request routing

set -e

# Colors
GREEN='\033[0;32m'
RED='\033[0;31m'
YELLOW='\033[1;33m'
CYAN='\033[0;36m'
BLUE='\033[0;34m'
NC='\033[0m'

RAFT_API_PORT=8080
WORKER_HTTP_PORT=7000

print_test() {
    echo -e "\n${BLUE}===${NC} ${CYAN}$1${NC} ${BLUE}===${NC}"
}

print_success() {
    echo -e "${GREEN}✓${NC} $1"
}

print_error() {
    echo -e "${RED}✗${NC} $1"
}

print_info() {
    echo -e "${CYAN}→${NC} $1"
}

# Check if cluster is running
if ! curl -s http://localhost:${RAFT_API_PORT}/health > /dev/null 2>&1; then
    print_error "Raft cluster not running on port ${RAFT_API_PORT}"
    echo "Start cluster first: ./scripts/local/start-cluster-with-workers.sh"
    exit 1
fi

if ! curl -s http://localhost:${WORKER_HTTP_PORT}/health > /dev/null 2>&1; then
    print_error "Workers not running on port ${WORKER_HTTP_PORT}"
    echo "Start cluster first: ./scripts/local/start-cluster-with-workers.sh"
    exit 1
fi

print_success "Cluster is running"

# =============================================================================
# Test 1: PUT via Raft (expect redirect)
# =============================================================================

print_test "Test 1: PUT via Raft (without following redirect)"

RESPONSE=$(curl -s -w "\n%{http_code}" -X POST http://localhost:${RAFT_API_PORT}/kv \
  -H 'Content-Type: application/json' \
  -d '{"key": 123, "value": 456}')

HTTP_CODE=$(echo "$RESPONSE" | tail -n1)
BODY=$(echo "$RESPONSE" | head -n-1)

if [ "$HTTP_CODE" = "307" ]; then
    print_success "Received HTTP 307 redirect"
    LOCATION=$(curl -s -I -X POST http://localhost:${RAFT_API_PORT}/kv \
      -H 'Content-Type: application/json' \
      -d '{"key": 123, "value": 456}' | grep -i "Location:" | cut -d' ' -f2 | tr -d '\r')
    print_info "Redirect location: ${LOCATION}"
elif [ "$HTTP_CODE" = "200" ]; then
    print_error "Expected 307 redirect, got 200 (Stage 3 not working?)"
    echo "Body: $BODY"
else
    print_error "Unexpected HTTP code: $HTTP_CODE"
    echo "Body: $BODY"
fi

# =============================================================================
# Test 2: PUT via Raft (follow redirect)
# =============================================================================

print_test "Test 2: PUT via Raft (following redirect)"

RESPONSE=$(curl -s -w "\n%{http_code}" -L -X POST http://localhost:${RAFT_API_PORT}/kv \
  -H 'Content-Type: application/json' \
  -d '{"key": 123, "value": 456}')

HTTP_CODE=$(echo "$RESPONSE" | tail -n1)
BODY=$(echo "$RESPONSE" | head -n-1)

if [ "$HTTP_CODE" = "200" ]; then
    print_success "Successfully PUT key=123 via redirect"
    print_info "Response: $BODY"
else
    print_error "PUT failed with HTTP $HTTP_CODE"
    echo "Body: $BODY"
fi

# =============================================================================
# Test 3: GET via Raft (follow redirect)
# =============================================================================

print_test "Test 3: GET via Raft (following redirect)"

RESPONSE=$(curl -s -w "\n%{http_code}" -L http://localhost:${RAFT_API_PORT}/kv/123)

HTTP_CODE=$(echo "$RESPONSE" | tail -n1)
BODY=$(echo "$RESPONSE" | head -n-1)

if [ "$HTTP_CODE" = "200" ]; then
    print_success "Successfully GET key=123 via redirect"
    print_info "Response: $BODY"

    # Validate response
    if echo "$BODY" | grep -q '"key":123' && echo "$BODY" | grep -q '"value":456'; then
        print_success "Data matches expected values"
    else
        print_error "Data mismatch!"
        echo "Expected: {\"key\":123,\"value\":456}"
        echo "Got: $BODY"
    fi
else
    print_error "GET failed with HTTP $HTTP_CODE"
    echo "Body: $BODY"
fi

# =============================================================================
# Test 4: Direct PUT to Worker (no redirect)
# =============================================================================

print_test "Test 4: Direct PUT to Worker (no redirect)"

RESPONSE=$(curl -s -w "\n%{http_code}" -X POST http://localhost:${WORKER_HTTP_PORT}/kv \
  -H 'Content-Type: application/json' \
  -d '{"key": 789, "value": 999}')

HTTP_CODE=$(echo "$RESPONSE" | tail -n1)
BODY=$(echo "$RESPONSE" | head -n-1)

if [ "$HTTP_CODE" = "200" ]; then
    print_success "Successfully PUT key=789 directly to worker"
    print_info "Response: $BODY"
elif [ "$HTTP_CODE" = "400" ]; then
    print_info "Worker rejected (partition not owned) - this is expected"
    print_info "Response: $BODY"
else
    print_error "Unexpected HTTP code: $HTTP_CODE"
    echo "Body: $BODY"
fi

# =============================================================================
# Test 5: Multiple keys (different partitions)
# =============================================================================

print_test "Test 5: Multiple keys (testing partition distribution)"

for KEY in 100 200 300 400 500; do
    RESPONSE=$(curl -s -w "\n%{http_code}" -L -X POST http://localhost:${RAFT_API_PORT}/kv \
      -H 'Content-Type: application/json' \
      -d "{\"key\": ${KEY}, \"value\": $((KEY * 2))}")

    HTTP_CODE=$(echo "$RESPONSE" | tail -n1)

    if [ "$HTTP_CODE" = "200" ]; then
        print_success "PUT key=${KEY} successful"
    else
        print_error "PUT key=${KEY} failed with HTTP ${HTTP_CODE}"
    fi
done

# =============================================================================
# Test 6: DELETE via Raft
# =============================================================================

print_test "Test 6: DELETE via Raft (following redirect)"

RESPONSE=$(curl -s -w "\n%{http_code}" -L -X DELETE http://localhost:${RAFT_API_PORT}/kv/123)

HTTP_CODE=$(echo "$RESPONSE" | tail -n1)
BODY=$(echo "$RESPONSE" | head -n-1)

if [ "$HTTP_CODE" = "200" ]; then
    print_success "Successfully DELETE key=123 via redirect"
    print_info "Response: $BODY"
else
    print_error "DELETE failed with HTTP $HTTP_CODE"
    echo "Body: $BODY"
fi

# Verify deletion
RESPONSE=$(curl -s -w "\n%{http_code}" -L http://localhost:${RAFT_API_PORT}/kv/123)
HTTP_CODE=$(echo "$RESPONSE" | tail -n1)

if [ "$HTTP_CODE" = "404" ]; then
    print_success "Verified: key=123 no longer exists"
else
    print_error "Key still exists after deletion (HTTP $HTTP_CODE)"
fi

# =============================================================================
# Summary
# =============================================================================

print_test "Test Summary"

echo ""
echo -e "${GREEN}✓ Stage 3 routing tests complete!${NC}"
echo ""
echo -e "${CYAN}→ Architecture:${NC}"
echo "  Client → Raft Node (HTTP 307) → Worker Node → Response"
echo ""
echo -e "${CYAN}→ Check worker stats:${NC}"
echo "  curl http://localhost:${WORKER_HTTP_PORT}/stats"
echo ""
echo -e "${CYAN}→ View logs:${NC}"
echo "  tail -f data/node0/node.log    # Raft logs"
echo "  tail -f data/worker0/worker.log # Worker logs"
echo ""
