#!/usr/bin/env bash
# verify-containers.sh
# Verifies container allocations on YARN by querying the YARN CLI and NodeManager.

set -euo pipefail

# Colors for output
GREEN='\033[0;32m'
RED='\033[0;31m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

RM_CONTAINER="resourcemanager-day06"
NM_CONTAINER="nodemanager-day06"

echo -e "${YELLOW}=== YARN Container Allocation Diagnostics ===${NC}"

# Check if containers are running
if ! docker ps --format '{{.Names}}' | grep -q "^${RM_CONTAINER}$"; then
  echo -e "${RED}[ERROR] Container '${RM_CONTAINER}' is not running.${NC}"
  exit 1
fi
if ! docker ps --format '{{.Names}}' | grep -q "^${NM_CONTAINER}$"; then
  echo -e "${RED}[ERROR] Container '${NM_CONTAINER}' is not running.${NC}"
  exit 1
fi

# 1. Query Active YARN Applications
echo -e "\nQuerying Active Applications..."
echo -e "${YELLOW}Command: yarn application -list${NC}"
docker exec "${RM_CONTAINER}" yarn application -list

# 2. Query Container allocations on NodeManager
echo -e "\nQuerying Active Containers on NodeManager..."
NM_CONTAINERS_URL="http://localhost:8042/ws/v1/node/containers"

CONTAINERS_JSON=$(docker exec "${NM_CONTAINER}" curl -s -f --connect-timeout 5 "$NM_CONTAINERS_URL" || echo "")

if [ -z "$CONTAINERS_JSON" ]; then
  echo -e "${YELLOW}No active container endpoints returned or NodeManager is idling (no jobs running).${NC}"
else
  echo -e "${GREEN}[OK] Container metadata found!${NC}"
  echo "$CONTAINERS_JSON"
fi

echo -e "\n${GREEN}[SUCCESS] Container verification checks completed.${NC}"
exit 0
