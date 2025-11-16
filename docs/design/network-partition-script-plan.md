# Plan for Network Partition Test Script

This document outlines the plan to create a standalone shell script for testing network partition fault tolerance. This script will replace the `TestNetworkPartitionAndReconciliation` Go test.

## Script Details

*   **Name:** `test_network_partition.sh`
*   **Location:** `scripts/` directory
*   **Objective:** To verify that the cluster can:
    1.  Survive the network isolation of its leader.
    2.  Elect a new leader in the remaining majority partition.
    3.  Process new writes on the new leader.
    4.  Allow the original leader to rejoin and correctly synchronize its state after the partition is healed.

## Execution Steps

The script will perform the following sequence of actions:

1.  **Cleanup:** Start by ensuring a clean environment by running `docker compose down -v` to stop any running containers and remove volumes.
2.  **Start Cluster:** Launch the 3-node Raft cluster using `docker compose up -d --build`.
3.  **Identify Initial Leader:** Poll the `/leader` API endpoint of each node using `curl` in a loop until the initial leader container is identified.
4.  **Isolate Leader:** Disconnect the leader container from the `raft-cluster` Docker network using the `docker network disconnect` command.
5.  **Elect New Leader:** Wait for a new leader to be elected by polling the other two nodes.
6.  **Write Data:** Use `curl` to send `POST` requests to the new leader's `/kv` endpoint to store a new key-value pair.
7.  **Heal Partition:** Reconnect the original leader container to the `raft-cluster` network using `docker network connect`.
8.  **Verify State Reconciliation:** In a loop, poll the `/kv/{key}` endpoint on the original (now reconnected) leader. The script will wait until this node returns the value that was written to the new leader during the partition.
9.  **Report Result:** Print a clear "SUCCESS" or "FAILURE" message based on the outcome of the verification step.
10. **Guaranteed Cleanup:** Use a `trap` command to ensure that `docker compose down -v` is always executed when the script exits, whether it succeeds, fails, or is interrupted.

## Go Test Modification

The existing `TestNetworkPartitionAndReconciliation` function will be removed from the `tests/integration/fault_tolerance_test.go` file, as this new script will supersede it. The `TestLeaderFailureAndRecovery` test will remain.
