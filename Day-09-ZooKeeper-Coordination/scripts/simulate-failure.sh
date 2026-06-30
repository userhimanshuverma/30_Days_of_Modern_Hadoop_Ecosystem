#!/usr/bin/env bash
# simulate-failure.sh
# Simulates a leader failure to test leader election and ensemble recovery.

set -euo pipefail

GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m'

echo -e "${YELLOW}=== Simulating ZooKeeper Leader Failure & Recovery ===${NC}"

# 1. Identify the current leader
echo -e "\n1. Identifying active leader..."
LEADER=""
CONTAINERS=("zookeeper1" "zookeeper2" "zookeeper3")

for container in "${CONTAINERS[@]}"; do
  STAT_OUT=$(docker exec "${container}" bash -c "echo stat | nc localhost 2181" 2>/dev/null || echo "failed")
  if echo "${STAT_OUT}" | grep -q "Mode: leader"; then
    LEADER="${container}"
    break
  fi
done

if [ -z "${LEADER}" ]; then
  echo -e "${RED}[ERROR] No leader is currently active in the cluster! Aborting failover test.${NC}"
  exit 1
fi

echo -e "Found Leader: ${GREEN}${LEADER}${NC}"

# 2. Kill the leader container
echo -e "\n2. Stopping leader container: ${RED}${LEADER}${NC}..."
docker stop "${LEADER}"

# 3. Wait for election and verify quorum status of survivors
echo -e "\n3. Waiting 5 seconds for leader election to complete..."
sleep 5

NEW_LEADER=""
SURVIVORS=()
for container in "${CONTAINERS[@]}"; do
  if [ "${container}" != "${LEADER}" ]; then
    SURVIVORS+=("${container}")
  fi
done

echo -e "Checking survivors: ${SURVIVORS[*]}"
for survivor in "${SURVIVORS[@]}"; do
  STAT_OUT=$(docker exec "${survivor}" bash -c "echo stat | nc localhost 2181" 2>/dev/null || echo "failed")
  if echo "${STAT_OUT}" | grep -q "Mode: leader"; then
    NEW_LEADER="${survivor}"
  fi
done

if [ -n "${NEW_LEADER}" ]; then
  echo -e "${GREEN}[SUCCESS] A new leader has been elected: ${NEW_LEADER}${NC}"
else
  echo -e "${RED}[ERROR] Failover failed! No new leader was elected among the survivors.${NC}"
  exit 1
fi

# Show status of new cluster configuration
echo -e "\n4. Verifying degraded quorum state..."
for survivor in "${SURVIVORS[@]}"; do
  STAT_OUT=$(docker exec "${survivor}" bash -c "echo stat | nc localhost 2181" 2>/dev/null || echo "failed")
  MODE=$(echo "${STAT_OUT}" | grep "Mode:" | awk '{print $2}' || echo "offline")
  echo -e "  - ${survivor} mode: ${GREEN}${MODE}${NC}"
done

# 5. Restart the old leader
echo -e "\n5. Restarting the stopped node (${LEADER}) to verify recovery..."
docker start "${LEADER}"

# Wait for container to start up and join
echo -e "Waiting for ${LEADER} to sync with the new leader..."
sleep 5

RECOVERED_STATUS=$(docker exec "${LEADER}" bash -c "echo stat | nc localhost 2181" 2>/dev/null || echo "failed")
if echo "${RECOVERED_STATUS}" | grep -q "Mode: follower"; then
  echo -e "${GREEN}[SUCCESS] Node ${LEADER} rejoined the cluster successfully as a follower.${NC}"
  exit 0
else
  echo -e "${YELLOW}[WARNING] Node ${LEADER} started but did not join as follower yet. Status:${NC}"
  echo "${RECOVERED_STATUS}"
  exit 1
fi
