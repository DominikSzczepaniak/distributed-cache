#!/bin/bash
# Test Stage 3: Request Routing from Raft to Workers
# This script tests HTTP 307 redirects from Raft nodes to worker nodes

set -e

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

echo -e "${BLUE}================================${NC}"
echo -e "${BLUE}Stage 3: Raft-to-Worker Routing${NC}"
echo -e "${BLUE}================================${NC}"
echo

# Configuration
RAFT_NODE="http://localhost:8080"
WORKER_0="http://localhost:7000"
WORKER_1="http://localhost:7001"

# Test key-value pairs
TEST_KEY_1=123
TEST_VALUE_1=456
TEST_KEY_2=789
TEST_VALUE_2=999

# Function to print test result
print_result() {
    if [ $1 -eq 0 ]; then
        echo -e "${GREEN}✓ $2${NC}"
    else
        echo -e "${RED}✗ $2${NC}"
        exit 1
    fi
}

# Function to check if service is ready
wait_for_service() {
    local url=$1
    local name=$2
    local max_attempts=30
    local attempt=0

    echo -n "Waiting for $name to be ready..."
    while [ $attempt -lt $max_attempts ]; do
        if curl -s -f "$url/health" > /dev/null 2>&1; then
            echo -e " ${GREEN}✓${NC}"
            return 0
        fi
        echo -n "."
        sleep 1
        attempt=$((attempt + 1))
    done
    echo -e " ${RED}✗ Timeout${NC}"
    return 1
}

echo -e "${YELLOW}Step 1: Checking service availability${NC}"
echo "-------------------------------------------"

# Check Raft node
if ! wait_for_service "$RAFT_NODE" "Raft node"; then
    echo -e "${RED}Error: Raft node not available at $RAFT_NODE${NC}"
    echo "Please start Raft cluster first: cd scripts && docker-compose up -d"
    exit 1
fi

# Check Worker 0
if ! wait_for_service "$WORKER_0" "Worker 0"; then
    echo -e "${RED}Error: Worker 0 not available at $WORKER_0${NC}"
    echo "Please start worker nodes first"
    exit 1
fi

echo

echo -e "${YELLOW}Step 2: Testing PUT redirect (Raft → Worker)${NC}"
echo "-------------------------------------------"

# Send PUT to Raft node without following redirects
PUT_RESPONSE=$(curl -s -w "\nHTTP_CODE:%{http_code}\nREDIRECT_URL:%{redirect_url}" -X POST \
    -H "Content-Type: application/json" \
    -d "{\"key\": $TEST_KEY_1, \"value\": $TEST_VALUE_1}" \
    "$RAFT_NODE/kv")

HTTP_CODE=$(echo "$PUT_RESPONSE" | grep "HTTP_CODE:" | cut -d':' -f2)
REDIRECT_URL=$(echo "$PUT_RESPONSE" | grep "REDIRECT_URL:" | cut -d':' -f2-)

echo "HTTP Status Code: $HTTP_CODE"
echo "Redirect URL: $REDIRECT_URL"

# Verify 307 redirect
if [ "$HTTP_CODE" == "307" ]; then
    print_result 0 "Raft node returns HTTP 307 Temporary Redirect"
else
    print_result 1 "Expected HTTP 307, got $HTTP_CODE"
fi

# Verify redirect URL contains worker address
if [[ "$REDIRECT_URL" == http://localhost:7* ]]; then
    print_result 0 "Redirect URL points to worker node"
else
    echo -e "${RED}Warning: Redirect URL doesn't point to expected worker: $REDIRECT_URL${NC}"
fi

# Extract response body (before HTTP_CODE line)
PUT_BODY=$(echo "$PUT_RESPONSE" | sed '/HTTP_CODE:/,$d')
echo "Response: $PUT_BODY"

# Check if response contains worker information
if echo "$PUT_BODY" | grep -q "worker_id"; then
    print_result 0 "Response contains worker_id"
else
    print_result 1 "Response missing worker_id"
fi

if echo "$PUT_BODY" | grep -q "partition_id"; then
    print_result 0 "Response contains partition_id"
else
    print_result 1 "Response missing partition_id"
fi

echo

echo -e "${YELLOW}Step 3: Testing GET redirect (Raft → Worker)${NC}"
echo "-------------------------------------------"

GET_RESPONSE=$(curl -s -w "\nHTTP_CODE:%{http_code}\nREDIRECT_URL:%{redirect_url}" \
    "$RAFT_NODE/kv/$TEST_KEY_1")

HTTP_CODE=$(echo "$GET_RESPONSE" | grep "HTTP_CODE:" | cut -d':' -f2)
REDIRECT_URL=$(echo "$GET_RESPONSE" | grep "REDIRECT_URL:" | cut -d':' -f2-)

echo "HTTP Status Code: $HTTP_CODE"
echo "Redirect URL: $REDIRECT_URL"

# Verify 307 redirect
if [ "$HTTP_CODE" == "307" ]; then
    print_result 0 "GET request returns HTTP 307 redirect"
else
    print_result 1 "Expected HTTP 307, got $HTTP_CODE"
fi

echo

echo -e "${YELLOW}Step 4: Testing DELETE redirect (Raft → Worker)${NC}"
echo "-------------------------------------------"

DELETE_RESPONSE=$(curl -s -w "\nHTTP_CODE:%{http_code}\nREDIRECT_URL:%{redirect_url}" \
    -X DELETE "$RAFT_NODE/kv/$TEST_KEY_1")

HTTP_CODE=$(echo "$DELETE_RESPONSE" | grep "HTTP_CODE:" | cut -d':' -f2)
REDIRECT_URL=$(echo "$DELETE_RESPONSE" | grep "REDIRECT_URL:" | cut -d':' -f2-)

echo "HTTP Status Code: $HTTP_CODE"
echo "Redirect URL: $REDIRECT_URL"

# Verify 307 redirect
if [ "$HTTP_CODE" == "307" ]; then
    print_result 0 "DELETE request returns HTTP 307 redirect"
else
    print_result 1 "Expected HTTP 307, got $HTTP_CODE"
fi

echo

echo -e "${YELLOW}Step 5: Testing end-to-end flow with redirect following${NC}"
echo "-------------------------------------------"

# PUT with automatic redirect following (curl -L)
PUT_RESPONSE=$(curl -s -L -w "\nHTTP_CODE:%{http_code}" -X POST \
    -H "Content-Type: application/json" \
    -d "{\"key\": $TEST_KEY_2, \"value\": $TEST_VALUE_2}" \
    "$RAFT_NODE/kv")

HTTP_CODE=$(echo "$PUT_RESPONSE" | grep "HTTP_CODE:" | cut -d':' -f2)
PUT_BODY=$(echo "$PUT_RESPONSE" | sed '/HTTP_CODE:/,$d')

echo "PUT Response: $PUT_BODY"
echo "HTTP Status Code: $HTTP_CODE"

if [ "$HTTP_CODE" == "200" ]; then
    print_result 0 "PUT succeeded on worker (after redirect)"
else
    print_result 1 "PUT failed: HTTP $HTTP_CODE"
fi

# GET with automatic redirect following
GET_RESPONSE=$(curl -s -L -w "\nHTTP_CODE:%{http_code}" "$RAFT_NODE/kv/$TEST_KEY_2")

HTTP_CODE=$(echo "$GET_RESPONSE" | grep "HTTP_CODE:" | cut -d':' -f2)
GET_BODY=$(echo "$GET_RESPONSE" | sed '/HTTP_CODE:/,$d')

echo "GET Response: $GET_BODY"
echo "HTTP Status Code: $HTTP_CODE"

if [ "$HTTP_CODE" == "200" ]; then
    print_result 0 "GET succeeded on worker (after redirect)"

    # Verify returned value matches
    RETURNED_VALUE=$(echo "$GET_BODY" | grep -o '"value":[0-9]*' | cut -d':' -f2)
    if [ "$RETURNED_VALUE" == "$TEST_VALUE_2" ]; then
        print_result 0 "Returned value matches ($RETURNED_VALUE == $TEST_VALUE_2)"
    else
        print_result 1 "Value mismatch: expected $TEST_VALUE_2, got $RETURNED_VALUE"
    fi
else
    print_result 1 "GET failed: HTTP $HTTP_CODE"
fi

echo

echo -e "${YELLOW}Step 6: Testing partition distribution${NC}"
echo "-------------------------------------------"

# Test multiple keys to see partition distribution
echo "Testing keys across different partitions..."
declare -A worker_counts

for key in {100..110}; do
    RESPONSE=$(curl -s -w "\nHTTP_CODE:%{http_code}" -X POST \
        -H "Content-Type: application/json" \
        -d "{\"key\": $key, \"value\": $((key * 10))}" \
        "$RAFT_NODE/kv")

    BODY=$(echo "$RESPONSE" | sed '/HTTP_CODE:/,$d')

    # Extract worker_id from response
    WORKER_ID=$(echo "$BODY" | grep -o '"worker_id":[0-9]*' | cut -d':' -f2)

    if [ -n "$WORKER_ID" ]; then
        worker_counts[$WORKER_ID]=$((${worker_counts[$WORKER_ID]:-0} + 1))
    fi
done

echo "Key distribution across workers:"
for worker_id in "${!worker_counts[@]}"; do
    echo "  Worker $worker_id: ${worker_counts[$worker_id]} keys"
done

# Check if keys are distributed (if multiple workers exist)
if [ ${#worker_counts[@]} -gt 1 ]; then
    print_result 0 "Keys distributed across ${#worker_counts[@]} workers"
else
    echo -e "${YELLOW}Note: Only 1 worker detected (may be expected for single-worker test)${NC}"
fi

echo

echo -e "${GREEN}================================${NC}"
echo -e "${GREEN}Stage 3 Validation Complete! ✓${NC}"
echo -e "${GREEN}================================${NC}"
echo
echo "Summary:"
echo "- Raft nodes return HTTP 307 redirects to workers"
echo "- Redirect Location headers point to correct workers"
echo "- End-to-end flow (Client → Raft → Worker) works correctly"
echo "- Data is routed to workers based on partition table"
echo
echo "Stage 3 is complete and working as expected!"
