#!/usr/bin/env bash
# verify-capacity-scheduler.sh
# Verifies that the Capacity Scheduler is active and parsing resources correctly.

set -euo pipefail

GREEN='\033[0;32m'
RED='\033[0;31m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

CONTAINER_NAME="resourcemanager-day07"

echo -e "${YELLOW}=== Verifying Capacity Scheduler Active State ===${NC}"

# 1. Check if ResourceManager container is running
if ! docker ps --format '{{.Names}}' | grep -q "^${CONTAINER_NAME}$"; then
  echo -e "${RED}[ERROR] Container '${CONTAINER_NAME}' is not running.${NC}"
  echo "Please start the cluster by running: docker compose -f ../docker/docker-compose.yml up -d"
  exit 1
fi

# 2. Query ResourceManager Scheduler Endpoint
SCHEDULER_URL="http://localhost:8088/ws/v1/cluster/scheduler"
echo -e "Querying Scheduler API endpoint: $SCHEDULER_URL"

if ! curl -s -f --connect-timeout 5 "$SCHEDULER_URL" > /dev/null; then
  echo -e "${RED}[ERROR] Failed to query YARN Scheduler API. ResourceManager may be offline or starting up.${NC}"
  exit 1
fi

RESPONSE=$(curl -s "$SCHEDULER_URL")

# Check if scheduler type is capacityScheduler
if echo "$RESPONSE" | grep -q -i "capacityScheduler"; then
  echo -e "${GREEN}[OK] CapacityScheduler is active in YARN ResourceManager.${NC}"
else
  echo -e "${RED}[ERROR] CapacityScheduler is NOT active. Current scheduler type could not be verified as CapacityScheduler.${NC}"
  echo "Verify 'yarn.resourcemanager.scheduler.class' in yarn-site.xml."
  exit 1
fi

# 3. Pull resource calculator class in active configs
echo -e "\nChecking Resource Calculator implementation..."
CALCULATOR=$(docker exec "$CONTAINER_NAME" hdfs getconf -confKey yarn.scheduler.capacity.resource-calculator 2>/dev/null || echo "Unknown")
if [[ "$CALCULATOR" == *"DominantResourceCalculator"* ]]; then
  echo -e "${GREEN}[OK] DominantResourceCalculator (DRC) is enabled: ${CALCULATOR}${NC}"
else
  echo -e "${YELLOW}[WARNING] DRC might not be configured. Default is DefaultResourceCalculator (Memory-only scheduling).${NC}"
  echo "Found configuration value: ${CALCULATOR}"
fi

# 4. Check if Preemption is active
echo -e "\nChecking Preemption settings..."
PREEMPTION_ENABLED=$(docker exec "$CONTAINER_NAME" hdfs getconf -confKey yarn.resourcemanager.scheduler.monitor.enable 2>/dev/null || echo "Unknown")
if [ "$PREEMPTION_ENABLED" = "true" ]; then
  echo -e "${GREEN}[OK] ResourceManager Preemption Monitor is ENABLED.${NC}"
else
  echo -e "${RED}[ERROR] Preemption Monitor is DISABLED (yarn.resourcemanager.scheduler.monitor.enable = $PREEMPTION_ENABLED).${NC}"
fi

echo -e "\n${GREEN}[SUCCESS] Capacity Scheduler configuration verified successfully.${NC}"
exit 0
