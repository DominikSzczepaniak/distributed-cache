# Distributed Cache with Raft Consensus

A strongly consistent, distributed key-value store (cache) implemented in Go, built on top of the Raft consensus algorithm. This system is designed to handle node failures and network partitions while maintaining data integrity.

## Table of Contents
- [Overview](#overview)
- [Why Strong Consistency?](#why-strong-consistency)
- [CAP Theorem Trade-offs (CP)](#cap-theorem-trade-offs-cp)
- [Getting Started (Local)](#getting-started-local)
- [Multi-Node Deployment](#multi-node-deployment)
- [Project Structure](#project-structure)
- [Future Work](#future-work)

## Overview
This project demonstrates a distributed system capable of automatic sharding, replication, and failover. It uses a Controller cluster (Raft-based) to manage the topology and metadata, while DataNodes store the actual key-value pairs.

Key features:
- **Raft Consensus**: For cluster coordination and metadata management.
- **Linearizable Semantics**: Guarantees that once a write commited, subsequent reads will see that value.
- **Automatic Failover**: Detects node failures and rebalances shards.
- **Dynamic Scaling**: Supports adding/removing nodes at runtime.

## Why Strong Consistency?
In many distributed systems, "eventual consistency" is chosen for performance, but it introduces complexity for the application developer (e.g., handling stale data, read-after-write inconsistencies).

We chose **Strong Consistency (Linearizability)** to ensure that the system behaves like a single entity.
- **Predictability**: If you `PUT x=5`, a subsequent `GET x` will return `5`.
- **Simplicty for Clients**: No need to handle version vectors or conflict resolution logic in the client application.
- **Data Integrity**: Critical for use-cases like locking, counters, or financial transactions where stale reads are unacceptable.

## CAP Theorem Trade-offs (CP)
According to the CAP Theorem, a distributed system can only provide two of the following three guarantees in the event of a network partition: **C**onsistency, **A**vailability, and **P**artition Tolerance.

**This system is designed as a CP system.**

- **Consistency (C)**: We prioritize data correctness. If a partition occurs and a quorum cannot be reached, the system will reject writes rather than accepting potentially conflicting data.
- **Partition Tolerance (P)**: The system continues to operate correctly on the side of the partition that holds the majority (quorum) of nodes.
- **Availability (A - Sacrificed)**: During a partition, the minority side of the cluster will become unavailable for writes (and potentially strong reads) to prevent split-brain scenarios and data divergence.

## Getting Started (Local)

### Prerequisites
- Docker & Docker Compose
- Go 1.21+ (for building CLI)

### Quick Start
Use the helper script to spin up a local cluster:

```bash
chmod +x run_local.sh
./run_local.sh
```

This interactive script allows you to:
1.  **Start Cluster**: Boots up 3 Controllers and 1 DataNode.
2.  **Add/Remove Nodes**: Simulate scaling out/in.
3.  **Run CLI**: Interact with the cache (`put`, `get`, `delete`).
4.  **View Logs**: Monitor system behavior.

### Manual Run
Alternatively, use standard Docker Compose commands:
```bash
# Start cluster
docker-compose up -d --build

# View status
docker-compose ps
```

See [LOCAL_RUN.md](LOCAL_RUN.md) for detailed instructions.

## Multi-Node Deployment
To deploy across multiple physical machines or VMs:

1.  **Build Binaries**: Compile `controller` and `datanode` for your target OS.
2.  **Configure Network**: Ensure ports 9000 (Raft) and 8080 (HTTP) are open.
3.  **Set Environment Variables**: Define `RAFT_ADDRS` and `NODE_ID` for each instance.

Detailed step-by-step instructions are available in [MULTI_NODE_SETUP.md](MULTI_NODE_SETUP.md).

## Project Structure

```text
├── cmd/                # Entry points for binaries
│   ├── controller/     # The Raft-based cluster manager
│   ├── datanode/       # The storage node server
│   └── raftcli/        # Command-line interface client
├── pkg/                # Core library code
│   ├── consensus/      # Raft implementation details
│   ├── controller/     # Topology and shard management
│   ├── datanode/       # Storage engine and replication logic
│   └── network/        # RPC and HTTP communication layer
├── deploy/             # Dockerfiles and deployment configs
├── scripts/            # Helper scripts (start/stop/clean)
├── tests/              # Test suites
│   ├── e2e/            # End-to-End tests
│   └── fault_injection/ # Chaos engineering tests
└── data/               # Persistent storage for local runs (git-ignored)
.
```

## Future Work
-   **Security**: Add TLS/SSL for inter-node communication and API endpoints.
-   **Storage Engine**: Replace in-memory map with a persistent LSM-tree (e.g., Badger or LevelDB) for DataNodes.
-   **Observability**: Add Prometheus metrics and comprehensive tracing.
-   **Performance**: Optimize data transfer and reduce latency.
