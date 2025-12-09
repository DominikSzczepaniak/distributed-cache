#!/bin/bash

set -e

RED='\033[0;31m'
GREEN='\033[0;32m'
NC='\033[0m'

log_info() { echo -e "${GREEN}[INFO]${NC} $1"; }
log_error() { echo -e "${RED}[ERROR]${NC} $1"; }

cleanup() {
    log_info "Cleaning up..."
    docker compose down
}
trap cleanup EXIT

echo "=================================================="
echo "      Basic E2E Test (PUT, GET, DELETE)"
echo "=================================================="

log_info "Cleaning up previous state..."
docker compose down -v 2>/dev/null || true
rm -rf data/node0 data/node1 data/node2 2>/dev/null || true
mkdir -p data/node0 data/node1 data/node2

log_info "Building raftcli for Linux..."
CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -o raftcli-linux ./cmd/raftcli

log_info "Starting cluster..."
docker compose up -d

log_info "Waiting 20s for cluster to stabilize..."
sleep 20

log_info "Installing raftcli in controller-0..."
docker cp raftcli-linux controller-0:/bin/raftcli
docker exec controller-0 chmod +x /bin/raftcli

run_raftcli() {
    echo "$*" | docker exec -i controller-0 /bin/raftcli localhost:8080
}

TEST_KEY="e2e-test-key"
TEST_VALUE="e2e-test-value"

log_info "Testing PUT..."
if run_raftcli put "$TEST_KEY" "$TEST_VALUE"; then
    log_info "PUT successful"
else
    log_error "PUT failed"
    exit 1
fi

sleep 1

log_info "Testing GET..."
GET_RESULT=$(run_raftcli get "$TEST_KEY")
echo "$GET_RESULT"
if echo "$GET_RESULT" | grep -q "$TEST_VALUE"; then
    log_info "GET verified successfully"
else
    log_error "GET failed: Expected '$TEST_VALUE', got output: $GET_RESULT"
    exit 1
fi

sleep 1

log_info "Testing DELETE..."
if run_raftcli delete "$TEST_KEY"; then
    log_info "DELETE successful"
else
    log_error "DELETE failed"
    exit 1
fi

sleep 1

log_info "Verifying deletion..."
if run_raftcli get "$TEST_KEY" 2>&1 | grep -q "key not found"; then
    log_info "Confirmation: Key not found (as expected)"
elif ! run_raftcli get "$TEST_KEY" > /dev/null 2>&1; then
     log_info "Confirmation: GET failed (as expected for deleted key)"
else
    GET_AGAIN=$(run_raftcli get "$TEST_KEY")
    if echo "$GET_AGAIN" | grep -q "$TEST_VALUE"; then
        log_error "Deletion failed! Key still exists with value: $TEST_VALUE"
        exit 1
    else
         log_info "Confirmation: Key verification weird but value apparently gone."
    fi
fi

echo ""
echo "=================================================="
echo -e "${GREEN}      SUCCESS: Basic E2E Test Passed!${NC}"
echo "=================================================="
