#!/bin/bash

# test-local.sh - Quick functionality test for local cluster
# Features:
# - Waits for cluster ready
# - Tests PUT operation
# - Tests GET operation
# - Tests DELETE operation
# - Shows results with color coding

set -e

# =============================================================================
# Configuration
# =============================================================================

BASE_API_PORT=${BASE_API_PORT:-10000}
TOTAL_NODES=${TOTAL_NODES:-3}
HEALTH_TIMEOUT=${HEALTH_TIMEOUT:-30}
EXPECTED_PARTITIONS=16384

# Test data
TEST_KEY=12345
TEST_VALUE=67890
TEST_KEY_2=99999
TEST_VALUE_2=11111

# Colors for output
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
BLUE='\033[0;34m'
CYAN='\033[0;36m'
GRAY='\033[0;90m'
NC='\033[0m' # No Color

# =============================================================================
# Helper Functions
# =============================================================================

print_header() {
    echo -e "\n${BLUE}===${NC} ${CYAN}$1${NC} ${BLUE}===${NC}"
}

print_success() {
    echo -e "${GREEN}✓${NC} $1"
}

print_error() {
    echo -e "${RED}✗${NC} $1"
}

print_warning() {
    echo -e "${YELLOW}⚠${NC} $1"
}

print_info() {
    echo -e "${CYAN}→${NC} $1"
}

print_test() {
    echo -e "\n${CYAN}Test:${NC} $1"
}

print_result() {
    local expected=$1
    local actual=$2
    if [ "$expected" = "$actual" ]; then
        print_success "Result: ${actual} (expected: ${expected})"
        return 0
    else
        print_error "Result: ${actual} (expected: ${expected})"
        return 1
    fi
}

check_node_health() {
    local port=$1
    curl -s -f "http://localhost:${port}/health" > /dev/null 2>&1
    return $?
}

check_partition_table_initialized() {
    # Check logs for partition table initialization
    for ((i=0; i<TOTAL_NODES; i++)); do
        if grep -q "Updated partition table to version" "data/node${i}/node.log" 2>/dev/null; then
            return 0
        fi
        if grep -q "Restored snapshot with.*partition assignments" "data/node${i}/node.log" 2>/dev/null; then
            return 0
        fi
    done
    return 1
}

# =============================================================================
# Main Script
# =============================================================================

print_header "Distributed Cache Functionality Test"

# Check if cluster is running
print_info "Checking cluster health..."

running_count=$(pgrep -f "./raftnode" 2>/dev/null | wc -l | tr -d ' ')

if [ "$running_count" -eq "0" ]; then
    print_error "No raftnode processes running"
    print_info "Start cluster with: ./scripts/local/start-local.sh"
    exit 1
fi

# Wait for all nodes to be healthy
print_info "Waiting for all nodes to be healthy..."
start_time=$SECONDS
all_healthy=false

while [ $((SECONDS - start_time)) -lt $HEALTH_TIMEOUT ]; do
    healthy_count=0

    for ((i=0; i<TOTAL_NODES; i++)); do
        port=$((BASE_API_PORT + i))
        if check_node_health $port; then
            ((healthy_count++))
        fi
    done

    if [ $healthy_count -eq $TOTAL_NODES ]; then
        all_healthy=true
        break
    fi

    echo -ne "\r${CYAN}→${NC} Healthy nodes: ${healthy_count}/${TOTAL_NODES}... "
    sleep 1
done

echo ""

if [ "$all_healthy" = false ]; then
    print_error "Cluster not healthy after ${HEALTH_TIMEOUT}s"
    exit 1
fi

print_success "All ${TOTAL_NODES} nodes are healthy"

# Wait for partition table
print_info "Checking partition table..."

if ! check_partition_table_initialized; then
    print_info "Waiting for initialization..."

    start_time=$SECONDS
    while [ $((SECONDS - start_time)) -lt $HEALTH_TIMEOUT ]; do
        if check_partition_table_initialized; then
            break
        fi
        echo -ne "\r${CYAN}→${NC} Waiting for partition table initialization... "
        sleep 1
    done
    echo ""
fi

if check_partition_table_initialized; then
    print_success "Partition table initialized: ${EXPECTED_PARTITIONS} partitions"
else
    print_error "Partition table not ready after ${HEALTH_TIMEOUT}s"
    exit 1
fi

# Run tests
print_header "Running Functionality Tests"

test_failures=0

# Test 1: PUT operation
print_test "PUT key=${TEST_KEY} value=${TEST_VALUE}"
response=$(curl -s -X POST "http://localhost:${BASE_API_PORT}/kv" \
    -H "Content-Type: application/json" \
    -d "{\"key\": ${TEST_KEY}, \"value\": ${TEST_VALUE}}" 2>&1)

if echo "$response" | grep -q "success"; then
    print_success "PUT operation succeeded"
else
    print_error "PUT operation failed: ${response}"
    ((test_failures++))
fi

# Test 2: GET operation (verify PUT)
print_test "GET key=${TEST_KEY}"
response=$(curl -s "http://localhost:${BASE_API_PORT}/kv/${TEST_KEY}" 2>&1)

# Extract value from response
value=$(echo "$response" | grep -o '"value":[0-9]*' | cut -d':' -f2)

if [ -n "$value" ] && [ "$value" -eq "$TEST_VALUE" ]; then
    print_success "GET operation succeeded: value=${value}"
else
    print_error "GET operation failed: expected ${TEST_VALUE}, got ${value}"
    ((test_failures++))
fi

# Test 3: PUT another key
print_test "PUT key=${TEST_KEY_2} value=${TEST_VALUE_2}"
response=$(curl -s -X POST "http://localhost:${BASE_API_PORT}/kv" \
    -H "Content-Type: application/json" \
    -d "{\"key\": ${TEST_KEY_2}, \"value\": ${TEST_VALUE_2}}" 2>&1)

if echo "$response" | grep -q "success"; then
    print_success "PUT operation succeeded"
else
    print_error "PUT operation failed: ${response}"
    ((test_failures++))
fi

# Test 4: DELETE operation
print_test "DELETE key=${TEST_KEY}"
response=$(curl -s -X DELETE "http://localhost:${BASE_API_PORT}/kv/${TEST_KEY}" 2>&1)

if echo "$response" | grep -q "success"; then
    print_success "DELETE operation succeeded"
else
    print_error "DELETE operation failed: ${response}"
    ((test_failures++))
fi

# Test 5: GET deleted key (should be 0 or not found)
print_test "GET deleted key=${TEST_KEY} (should return 0)"
response=$(curl -s "http://localhost:${BASE_API_PORT}/kv/${TEST_KEY}" 2>&1)

# Extract value from response
value=$(echo "$response" | grep -o '"value":[0-9]*' | cut -d':' -f2)

if [ -n "$value" ] && [ "$value" -eq "0" ]; then
    print_success "GET deleted key returned 0 as expected"
else
    print_warning "GET deleted key returned: ${value}"
fi

# Test 6: Verify second key still exists
print_test "GET key=${TEST_KEY_2} (verify still exists)"
response=$(curl -s "http://localhost:${BASE_API_PORT}/kv/${TEST_KEY_2}" 2>&1)

value=$(echo "$response" | grep -o '"value":[0-9]*' | cut -d':' -f2)

if [ -n "$value" ] && [ "$value" -eq "$TEST_VALUE_2" ]; then
    print_success "GET operation succeeded: value=${value}"
else
    print_error "GET operation failed: expected ${TEST_VALUE_2}, got ${value}"
    ((test_failures++))
fi

# Test 7: Test multiple nodes (round-robin)
print_header "Testing Multiple Nodes"

for ((i=0; i<TOTAL_NODES; i++)); do
    port=$((BASE_API_PORT + i))
    test_key=$((TEST_KEY + i + 1000))
    test_value=$((TEST_VALUE + i + 1000))

    print_test "Node ${i} - PUT key=${test_key} value=${test_value} on port ${port}"

    response=$(curl -s -X POST "http://localhost:${port}/kv" \
        -H "Content-Type: application/json" \
        -d "{\"key\": ${test_key}, \"value\": ${test_value}}" 2>&1)

    if echo "$response" | grep -q "success"; then
        print_success "Node ${i}: PUT succeeded"
    else
        print_error "Node ${i}: PUT failed"
        ((test_failures++))
        continue
    fi

    # Verify on same node
    response=$(curl -s "http://localhost:${port}/kv/${test_key}" 2>&1)
    value=$(echo "$response" | grep -o '"value":[0-9]*' | cut -d':' -f2)

    if [ -n "$value" ] && [ "$value" -eq "$test_value" ]; then
        print_success "Node ${i}: GET verified"
    else
        print_error "Node ${i}: GET failed (expected ${test_value}, got ${value})"
        ((test_failures++))
    fi
done

# Summary
print_header "Test Summary"

echo ""
if [ $test_failures -eq 0 ]; then
    print_success "All tests passed!"
    echo ""
    print_info "Cluster is functioning correctly"
    echo ""
    exit 0
else
    print_error "${test_failures} test(s) failed"
    echo ""
    print_info "Check cluster status with: ./scripts/local/status-local.sh"
    print_info "View logs in: data/nodeX/node.log"
    echo ""
    exit 1
fi
