#!/usr/bin/env bash
# verify-yarn.sh
# Verifies YARN cluster status using YARN CLI tools inside the ResourceManager.

set -euo pipefail

# Colors for output
GREEN='\033[0;32m'
RED='\033[0;31m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

RM_CONTAINER="resourcemanager-day06"

echo -e "${YELLOW}=== YARN Cluster CLI Verification ===${NC}"

# Check if RM container is running
if ! docker ps --format '{{.Names}}' | grep -q "^${RM_CONTAINER}$"; then
  echo -e "${RED}[ERROR] Container '${RM_CONTAINER}' is not running. Please start the cluster first.${NC}"
  exit 1
fi

# 1. Print Active and Inactive Nodes List
echo -e "\nQuerying registered NodeManagers via CLI..."
echo -e "${YELLOW}Command: yarn node -list -all${NC}"
docker exec "${RM_CONTAINER}" yarn node -list -all

# 2. Print Queue Information
echo -e "\nQuerying Queue Details..."
echo -e "${YELLOW}Command: yarn queue -status default${NC}"
docker exec "${RM_CONTAINER}" yarn queue -status default

# 3. Print ResourceManager Health States and CPU info
echo -e "\nEvaluating RM ResourceManager stats..."
ACTIVE_COUNT=$(docker exec "${RM_CONTAINER}" yarn node -list | grep -c "RUNNING" || echo "0")
if [ "${ACTIVE_COUNT}" -gt 0 ]; then
  echo -e "${GREEN}[OK] YARN Cluster has ${ACTIVE_COUNT} active running NodeManagers.${NC}"
  echo -e "\n${GREEN}[SUCCESS] YARN Cluster CLI checks completed successfully.${NC}"
  exit 0
else
  echo -e "${RED}[ERROR] No running NodeManagers found in active list. Cluster is not ready.${NC}"
  exit 1
fi
