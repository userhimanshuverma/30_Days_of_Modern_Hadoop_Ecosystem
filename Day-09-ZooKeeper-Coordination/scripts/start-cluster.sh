#!/usr/bin/env bash
# start-cluster.sh
# Starts the 3-node ZooKeeper Docker ensemble and waits for all nodes to become healthy.

set -euo pipefail

# Color codes
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m'

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
DOCKER_DIR="${SCRIPT_DIR}/../docker"

echo -e "${YELLOW}=== Starting Apache ZooKeeper 3-Node Ensemble ===${NC}"

# Navigate to docker folder and run docker-compose
cd "${DOCKER_DIR}"
docker compose up -d --build

echo -e "\n${YELLOW}=== Waiting for ZooKeeper instances to pass healthchecks... ===${NC}"

# Wait for zookeeper1, zookeeper2, zookeeper3 to be healthy
for container in zookeeper1 zookeeper2 zookeeper3; do
  echo -n "Checking ${container} health... "
  while true; do
    STATUS=$(docker inspect --format='{{.State.Health.Status}}' "${container}" 2>/dev/null || echo "missing")
    if [ "${STATUS}" = "healthy" ]; then
      echo -e "${GREEN}HEALTHY${NC}"
      break
    elif [ "${STATUS}" = "unhealthy" ] || [ "${STATUS}" = "missing" ]; then
      # If missing or unhealthy, wait a bit and check
      sleep 2
    else
      # status is "starting"
      echo -n "."
      sleep 2
    fi
  done
done

echo -e "\n${GREEN}[SUCCESS] All ZooKeeper nodes are healthy and running!${NC}"
echo -e "Port mappings:"
echo -e " - zookeeper1: client=2181, admin=8081, metrics=7001"
echo -e " - zookeeper2: client=2182, admin=8082, metrics=7002"
echo -e " - zookeeper3: client=2183, admin=8083, metrics=7003"
