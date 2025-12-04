# Local Run Instructions

This guide explains how to run the Distributed Cache system locally using Docker Compose and the provided helper script.

## Prerequisites
- Docker and Docker Compose installed.
- Go 1.21+ installed (to build the CLI).

## Using the Helper Script
The easiest way to run the system is using the `run_local.sh` script.

1.  **Make the script executable**:
    ```bash
    chmod +x run_local.sh
    ```

2.  **Run the script**:
    ```bash
    ./run_local.sh
    ```

3.  **Menu Options**:
    -   **1. Start Cluster**: Starts 3 Controllers and 1 DataNode (`datanode-1`).
    -   **2. Add DataNode**: Allows you to start additional nodes (`datanode-2` to `datanode-5`). This simulates scaling out.
    -   **3. Remove DataNode**: Stops a specific DataNode. This simulates node failure or scaling in.
    -   **4. Run CLI**: Launches the interactive `raftcli` connected to the local cluster.
    -   **5. View Logs**: Tail logs for a specific service (e.g., `controller-0`, `datanode-3`).
    -   **6. Stop Cluster**: Stops and removes all containers.

## Manual Execution
If you prefer running commands manually:

### 1. Start the Cluster
```bash
docker-compose up -d --build controller-0 controller-1 controller-2 datanode-1
```

### 2. Scale Out (Add Node)
To add a second node:
```bash
docker-compose up -d --build datanode-2
```
The Controller will detect the new node and automatically trigger rebalancing.

### 3. Scale In (Remove Node)
To remove a node:
```bash
docker stop datanode-2
```
**Note**: If the replication factor is 1 (default), data on this node will be unavailable until it restarts.

### 4. Run CLI
Build the CLI:
```bash
go build -o raftcli ./cmd/raftcli
```
Run it:
```bash
./raftcli localhost:8080,localhost:8081,localhost:8082
```
Commands:
-   `put <key> <value>`
-   `get <key>`
-   `load <count>` (Bulk load N keys)

## Monitoring Rebalancing
To see rebalancing in action:
1.  Start with 1 node.
2.  Load data: `load 1000`.
3.  Add `datanode-2`.
4.  Check Controller logs:
    ```bash
    docker-compose logs -f controller-0
    ```
    Look for "Rebalancing: Moving shard..." messages.
