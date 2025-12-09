#!/bin/bash

check_docker() {
    if ! docker info > /dev/null 2>&1; then
        echo "Error: Docker is not running. Please start Docker."
        exit 1
    fi
}

build_binaries() {
    echo "Building raftcli (Linux)..."
    CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -o raftcli-linux ./cmd/raftcli
}

start_cluster() {
    echo "Starting Controllers and DataNode 1..."
    docker-compose up -d --build controller-0 controller-1 controller-2 datanode-1
    echo "Cluster started. Waiting for stabilization..."
    sleep 10
    echo "Cluster ready."
}

add_node() {
    echo "Available nodes to add: datanode-2, datanode-3, datanode-4, datanode-5"
    read -p "Enter node name to add (e.g., datanode-2): " node_name
    if [[ "$node_name" =~ ^datanode-[2-5]$ ]]; then
        echo "Starting $node_name..."
        docker-compose up -d --build $node_name
        echo "$node_name started. Rebalancing should trigger automatically."
    else
        echo "Invalid node name. Please use datanode-2 to datanode-5."
    fi
}

remove_node() {
    read -p "Enter node name to remove (e.g., datanode-2): " node_name
    if [[ "$node_name" =~ ^datanode-[1-5]$ ]]; then
        echo "Stopping $node_name..."
        docker stop $node_name
        echo "$node_name stopped. Note: Data might be unavailable if replication factor is 1."
    else
        echo "Invalid node name."
    fi
}

run_cli() {
    echo "Starting CLI (inside Docker network)..."
    docker run -it --rm \
        --network raft-cluster \
        -v "$(pwd)/raftcli-linux":/usr/local/bin/raftcli \
        alpine:latest \
        /usr/local/bin/raftcli controller-0:8080,controller-1:8080,controller-2:8080
}

while true; do
    echo ""
    echo "=== Distributed Cache Manager ==="
    echo "1. Start Cluster (Controllers + DataNode 1)"
    echo "2. Add DataNode (Scale Out)"
    echo "3. Remove DataNode (Scale In)"
    echo "4. Run CLI (Put/Get/Load)"
    echo "5. View Logs"
    echo "6. Stop Cluster"
    echo "7. Exit"
    read -p "Select option: " choice

    case $choice in
        1)
            check_docker
            build_binaries
            start_cluster
            ;;
        2)
            add_node
            ;;
        3)
            remove_node
            ;;
        4)
            run_cli
            ;;
        5)
            read -p "Enter service name (e.g., controller-0, datanode-1): " service
            docker-compose logs -f $service
            ;;
        6)
            echo "Stopping cluster..."
            docker-compose down
            ;;
        7)
            exit 0
            ;;
        *)
            echo "Invalid option."
            ;;
    esac
done
