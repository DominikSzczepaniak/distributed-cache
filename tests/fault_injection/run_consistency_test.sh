#!/bin/bash

# Runner script for strong consistency test
# Captures output to a log file and reports pass/fail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
LOG_FILE="./consistency_test_result.log"

echo "Running strong consistency tests..."
echo "Log file: $LOG_FILE"
echo ""

"$SCRIPT_DIR/test_strong_consistency.sh" > "$LOG_FILE" 2>&1
EXIT_CODE=$?

echo ""
echo "Test completed with exit code: $EXIT_CODE"
echo "See $LOG_FILE for details"

if [ $EXIT_CODE -eq 0 ]; then
    echo "✅ ALL CONSISTENCY TESTS PASSED!"
else
    echo "❌ CONSISTENCY TESTS FAILED"
    echo ""
    echo "=== Last 30 lines of log ==="
    tail -30 "$LOG_FILE"
fi

exit $EXIT_CODE
