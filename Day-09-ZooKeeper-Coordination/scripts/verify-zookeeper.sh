#!/usr/bin/env bash
# verify-zookeeper.sh
# Audits the running state of the three ZooKeeper containers.

set -euo pipefail

GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m'

echo -e "${YELLOW}=== Auditing ZooKeeper Cluster State ===${NC}"

CONTAINERS=("zookeeper1" "zookeeper2" "zookeeper3")
ALL_OK=true

for container in "${CONTAINERS[@]}"; do
  # Check if container is running
  if docker ps --format '{{.Names}}' | grep -q "^${container}$"; then
    STATUS=$(docker inspect --format='{{.State.Status}}' "${container}")
    HEALTH=$(docker inspect --format='{{.State.Health.Status}}' "${container}" 2>/dev/null || echo "N/A")
    echo -e "Container ${GREEN}${container}${NC} status: ${GREEN}${STATUS}${NC} (health: ${GREEN}${HEALTH}${NC})"
  else
    echo -e "Container ${RED}${container}${NC} is ${RED}NOT RUNNING${NC}!"
    ALL_OK=false
  fi
done

if [ "$ALL_OK" = true ]; then
  echo -e "\n${GREEN}[SUCCESS] All containers are running and healthy.${NC}"
  exit 0
else
  echo -e "\n${RED}[ERROR] One or more ZooKeeper containers are not running. Run ./start-cluster.sh to start them.${NC}"
  exit 1
fi
