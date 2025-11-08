# Fault Tolerance Test Plan

This document outlines the plan for two fault tolerance tests for the Raft-based distributed cache.

## Test 1: Leader Failure and Recovery

**Objective:** To ensure the system can withstand a leader failure, elect a new leader, and that the failed node can successfully rejoin the cluster and synchronize its state.

**Steps:**

1.  **Start Cluster:** Initialize a 3-node Raft cluster.
2.  **Identify Leader:** Determine the current leader node.
3.  **Simulate Failure:** Terminate the process of the leader node.
4.  **Verify New Leader:** Confirm that one of the remaining two nodes is elected as the new leader.
5.  **Write Data:** Send a write request (e.g., SET `key1`=`value1`) to the new leader and verify it is successful.
6.  **Restart Node:** Bring the failed node back online.
7.  **Verify State Sync:** After the node has rejoined the cluster, query it to confirm that it has the data (`key1`=`value1`) that was written while it was offline.

## Test 2: Network Partition and Reconciliation

**Objective:** To verify that the cluster can handle a network partition that isolates the leader, elect a new leader in the majority partition, and that the old leader reconciles its state after the partition is resolved.

**Steps:**

1.  **Start Cluster:** Initialize a 3-node Raft cluster.
2.  **Identify Leader:** Determine the current leader node.
3.  **Create Partition:** Isolate the leader node from the other two nodes by blocking its network traffic to them.
4.  **Verify New Leader:** Confirm that a new leader is elected from the two-node majority partition.
5.  **Write Data to New Leader:** Send a write request (e.g., SET `key2`=`value2`) to the new leader and verify it is successful.
6.  **Attempt Write to Old Leader:** Send a write request to the isolated old leader. This request is expected to fail or time out, as it cannot achieve a quorum.
7.  **Resolve Partition:** Remove the network block, allowing all nodes to communicate again.
8.  **Verify State Reconciliation:** Query the original leader node to ensure that it has stepped down, accepted the new leader's authority, and has the data (`key2`=`value2`) that was written during the partition.

## Implementation Strategy

*   **Test Framework:** The tests will be implemented as Go integration tests within the `tests/integration` directory.
*   **Node Control:** The tests will manage the lifecycle of the Raft node processes directly, using commands to start and stop them.
*   **Network Control:** The network partition will be simulated using `iptables` rules to block traffic between specific node ports.
*   **API Interaction:** A Go HTTP client will be used to send `SET` and `GET` requests to the nodes' APIs to write and verify data.
