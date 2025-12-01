#!/bin/bash

# Test script for partition table auto-initialization
# This script verifies that the auto-init fix works correctly

set -e  # Exit on error

SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
PROJECT_ROOT="$( cd "$SCRIPT_DIR/.." && pwd )"

cd "$PROJECT_ROOT"

echo "=========================================="
echo "Partition Table Auto-Init Test"
echo "=========================================="
echo ""

# Colors for output
GREEN='\033[0;32m'
RED='\033[0;31m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Helper functions
success() {
    echo -e "${GREEN}✓ $1${NC}"
}

error() {
    echo -e "${RED}✗ $1${NC}"
    exit 1
}

info() {
    echo -e "${YELLOW}→ $1${NC}"
}

# Check if Docker is running
info "Checking Docker daemon..."
if ! docker info > /dev/null 2>&1; then
    error "Docker daemon is not running. Please start Docker Desktop."
fi
success "Docker is running"

# Stop any existing containers
info "Stopping existing containers..."
docker compose down -v > /dev/null 2>&1 || true
success "Cleaned up existing containers"

# Build fresh images
info "Building Docker images..."
docker compose build > /dev/null 2>&1
success "Images built successfully"

# Start cluster
info "Starting 3-node cluster..."
docker compose up -d
success "Cluster started"

# Wait for initialization
info "Waiting for cluster initialization (10 seconds)..."
sleep 10

# Check if containers are running
info "Verifying containers are running..."
RUNNING=$(docker compose ps --filter "status=running" -q | wc -l | tr -d ' ')
if [ "$RUNNING" != "3" ]; then
    error "Expected 3 running containers, found $RUNNING"
fi
success "All 3 nodes are running"

# Check leader logs for initialization
info "Checking leader initialization logs..."
LEADER_INIT=$(docker compose logs | grep -c "This node is the leader, initializing partition table" || true)
if [ "$LEADER_INIT" -lt 1 ]; then
    error "Leader did not initialize partition table"
fi
success "Leader initialized partition table"

# Check for partition table updates
info "Checking partition table updates..."
PT_UPDATES=$(docker compose logs | grep -c "Updated partition table to version 1" || true)
if [ "$PT_UPDATES" -lt 3 ]; then
    error "Expected 3 nodes to update partition table, found $PT_UPDATES"
fi
success "All nodes received partition table update"

# Test admin endpoint
info "Querying partition table status..."
PT_RESPONSE=$(curl -s http://localhost:8080/admin/partition-table)
ASSIGNMENT_COUNT=$(echo "$PT_RESPONSE" | grep -o '"assignment_count":[0-9]*' | grep -o '[0-9]*')

if [ "$ASSIGNMENT_COUNT" != "16384" ]; then
    error "Expected 16384 assignments, found $ASSIGNMENT_COUNT"
fi
success "Partition table has 16,384 assignments"

# Test PUT operation
info "Testing PUT operation..."
PUT_RESPONSE=$(curl -s -X PUT http://localhost:8080/cache/123 \
    -H "Content-Type: application/json" \
    -d '{"value": 42}')

if ! echo "$PUT_RESPONSE" | grep -q '"success":true'; then
    error "PUT operation failed: $PUT_RESPONSE"
fi
success "PUT operation succeeded"

# Test GET operation
info "Testing GET operation..."
GET_RESPONSE=$(curl -s http://localhost:8080/cache/123)

if ! echo "$GET_RESPONSE" | grep -q '"value":42'; then
    error "GET operation failed: $GET_RESPONSE"
fi
success "GET operation succeeded"

# Test DELETE operation
info "Testing DELETE operation..."
DELETE_RESPONSE=$(curl -s -X DELETE http://localhost:8080/cache/123)

if ! echo "$DELETE_RESPONSE" | grep -q '"success":true'; then
    error "DELETE operation failed: $DELETE_RESPONSE"
fi
success "DELETE operation succeeded"

# Verify deletion
info "Verifying deletion..."
VERIFY_RESPONSE=$(curl -s http://localhost:8080/cache/123)

if ! echo "$VERIFY_RESPONSE" | grep -q '"error"'; then
    error "Key should not exist after deletion"
fi
success "Deletion verified"

# Test cluster restart (persistence)
info "Testing cluster restart..."
docker compose restart > /dev/null 2>&1
sleep 10

info "Checking partition table after restart..."
PT_RESPONSE_AFTER=$(curl -s http://localhost:8080/admin/partition-table)
ASSIGNMENT_COUNT_AFTER=$(echo "$PT_RESPONSE_AFTER" | grep -o '"assignment_count":[0-9]*' | grep -o '[0-9]*')

if [ "$ASSIGNMENT_COUNT_AFTER" != "16384" ]; then
    error "Partition table not persisted after restart"
fi
success "Partition table persisted correctly"

# Test operation after restart
info "Testing operation after restart..."
PUT_RESPONSE_AFTER=$(curl -s -X PUT http://localhost:8080/cache/456 \
    -H "Content-Type: application/json" \
    -d '{"value": 99}')

if ! echo "$PUT_RESPONSE_AFTER" | grep -q '"success":true'; then
    error "Operations failed after restart"
fi
success "Operations work after restart"

# Show cluster status
echo ""
echo "=========================================="
echo "Cluster Status"
echo "=========================================="
curl -s http://localhost:8080/admin/partition-table | python3 -m json.tool || true

# Cleanup (optional)
echo ""
read -p "Stop and remove containers? (y/n) " -n 1 -r
echo ""
if [[ $REPLY =~ ^[Yy]$ ]]; then
    info "Cleaning up..."
    docker compose down -v > /dev/null 2>&1
    success "Cleanup complete"
fi

echo ""
echo "=========================================="
echo -e "${GREEN}ALL TESTS PASSED ✓${NC}"
echo "=========================================="
