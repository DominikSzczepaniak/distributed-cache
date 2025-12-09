#!/bin/bash

cd "$(dirname "$0")/../.."
LOG_FILE="./partition_test_result.log"

echo "Running network partition test..." 
echo "Log file: $LOG_FILE"

./tests/fault_injection/test_network_partition.sh > "$LOG_FILE" 2>&1
EXIT_CODE=$?

echo ""
echo "Test completed with exit code: $EXIT_CODE"
echo "See $LOG_FILE for details"

if [ $EXIT_CODE -eq 0 ]; then
    echo "✅ TEST PASSED!"
else
    echo "❌ TEST FAILED"
    echo ""
    echo "=== Last 30 lines of log ==="
    tail -30 "$LOG_FILE"
fi
