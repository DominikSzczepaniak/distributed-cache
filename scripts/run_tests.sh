#!/bin/bash

# Exit immediately if a command exits with a non-zero status.
set -e

# Make sure we are in the root of the project
cd "$(dirname "$0")/.."

echo "Running e2e tests..."
if ! tests/e2e/test-api.sh; then
    echo "e2e tests failed"
    exit 1
fi
echo "e2e tests passed"

echo "Running fault injection tests..."
if ! go test ./tests/fault_injection; then
    echo "Fault injection Go tests failed"
    exit 1
fi
echo "Fault injection Go tests passed"

echo "Running network partition tests..."
if ! tests/fault_injection/test_network_partition.sh; then
    echo "Network partition tests failed"
    exit 1
fi
echo "Network partition tests passed"


echo "All tests passed successfully!"
