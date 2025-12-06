#!/bin/bash
echo "--- 🧪 Verifying Inter-Container Connectivity ---"

# Start cluster if not running
docker-compose up -d controller-0 controller-1 controller-2
sleep 5

echo "--- Checking connectivity from controller-1 to controller-0 ---"
docker exec controller-1 wget -qO- http://controller-0:8080/topology
docker exec controller-1 nc -z -v controller-0 9000

echo "--- Checking connectivity from controller-2 to controller-0 ---"
docker exec controller-2 wget -qO- http://controller-0:8080/topology
docker exec controller-2 nc -z -v controller-0 9000

echo "--- Checking connectivity from controller-0 to controller-1 ---"
docker exec controller-0 wget -qO- http://controller-1:8080/topology
docker exec controller-0 nc -z -v controller-1 9000
