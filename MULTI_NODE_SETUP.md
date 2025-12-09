# Multi-Node Setup Instructions

This guide explains how to run the Distributed Cache system across multiple computers on the same network.

## Prerequisites
-   All computers must be on the same network and able to reach each other via TCP.
-   Go 1.21+ installed on all machines (or build binaries for the target OS).
-   Firewall ports 9000-9002 (Raft/Data) and 8080-8082 (API) must be open.

## Architecture Overview
You will need at least:
-   **3 Controllers** (for Raft consensus). Can be on the same or different machines.
-   **N DataNodes**.

## Step 1: Build Binaries
On your development machine, build the binaries for your target OS (e.g., Linux or macOS).

```bash
# For Linux
GOOS=linux GOARCH=amd64 go build -o controller ./cmd/controller
GOOS=linux GOARCH=amd64 go build -o datanode ./cmd/datanode
GOOS=linux GOARCH=amd64 go build -o raftcli ./cmd/raftcli

# For macOS (ARM64)
GOOS=darwin GOARCH=arm64 go build -o controller ./cmd/controller
# ... etc
```
Copy the respective binaries to the target machines.

## Step 2: Configure Controllers
Decide on the IP addresses for your 3 controllers.
Example:
-   Controller 0: `192.168.1.10`
-   Controller 1: `192.168.1.11`
-   Controller 2: `192.168.1.12`

**On Machine 1 (Controller 0):**
```bash
export RAFT_ID=0
export TOTAL_NODES=3
export RAFT_ADDRS="192.168.1.10:9000,192.168.1.11:9000,192.168.1.12:9000"
export API_ADDR=":8080"
export FILENAME="./data/node0"
./controller
```

**On Machine 2 (Controller 1):**
```bash
export RAFT_ID=1
export TOTAL_NODES=3
export RAFT_ADDRS="192.168.1.10:9000,192.168.1.11:9000,192.168.1.12:9000"
export API_ADDR=":8080"
export FILENAME="./data/node1"
./controller
```

**On Machine 3 (Controller 2):**
```bash
export RAFT_ID=2
export TOTAL_NODES=3
export RAFT_ADDRS="192.168.1.10:9000,192.168.1.11:9000,192.168.1.12:9000"
export API_ADDR=":8080"
export FILENAME="./data/node2"
./controller
```

## Step 3: Start DataNodes
You can run DataNodes on any machine (including the controller machines).

**On Machine 4 (DataNode 1):**
```bash
# Point to ANY of the controllers (or use a load balancer)
export CONTROLLER_URL="http://192.168.1.10:8080"
# Unique ID for this node (IP:Port)
export NODE_ID="192.168.1.13:9000"
export LEASE_DURATION="5s"
./datanode
```

**On Machine 5 (DataNode 2):**
```bash
export CONTROLLER_URL="http://192.168.1.10:8080"
export NODE_ID="192.168.1.14:9000"
export LEASE_DURATION="5s"
./datanode
```

## Step 4: Run Client
From any machine:
```bash
./raftcli 192.168.1.10:8080,192.168.1.11:8080,192.168.1.12:8080
```

## Troubleshooting
-   **Connection Refused**: Check firewalls and ensure `RAFT_ADDRS` and `NODE_ID` use accessible IP addresses (not localhost/127.0.0.1 if across machines).
-   **Leader Election Fails**: Ensure all 3 controllers are running and can reach each other on port 9000.
