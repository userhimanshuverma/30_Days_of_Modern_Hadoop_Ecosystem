#!/usr/bin/env bash
# verify-failover.sh
# Automated simulation of NameNode failover to test ZKFC-driven standby promotion.

set -euo pipefail

# Colors
GREEN='\033[0;32m'
RED='\033[0;31m'
YELLOW='\033[1;33m'
NC='\033[0m'

echo -e "${YELLOW}=== HDFS Automatic Failover Simulation ===${NC}"

get_state() {
  local target_node=$1
  docker exec namenode1-day03 hdfs haadmin -getServiceState "$target_node" 2>/dev/null | tr -d '\r\n ' || echo "unknown"
}

# 1. Identify which node is currently active
echo "Auditing current state..."
STATE_NN1=$(get_state "nn1")
STATE_NN2=$(get_state "nn2")

ACTIVE_CONTAINER=""
STANDBY_CONTAINER=""
ACTIVE_ID=""
STANDBY_ID=""

if [ "${STATE_NN1}" = "active" ] && [ "${STATE_NN2}" = "standby" ]; then
  ACTIVE_CONTAINER="namenode1-day03"
  ACTIVE_ID="nn1"
  STANDBY_CONTAINER="namenode2-day03"
  STANDBY_ID="nn2"
elif [ "${STATE_NN1}" = "standby" ] && [ "${STATE_NN2}" = "active" ]; then
  ACTIVE_CONTAINER="namenode2-day03"
  ACTIVE_ID="nn2"
  STANDBY_CONTAINER="namenode1-day03"
  STANDBY_ID="nn1"
else
  echo -e "${RED}[ERROR] Invalid starting states. One must be active and one standby. NN1: ${STATE_NN1}, NN2: ${STATE_NN2}${NC}"
  exit 1
fi

echo -e "Current Active Node: ${GREEN}${ACTIVE_CONTAINER} (${ACTIVE_ID})${NC}"
echo -e "Current Standby Node: ${GREEN}${STANDBY_CONTAINER} (${STANDBY_ID})${NC}"

# 2. Simulate failure by stopping the Active NameNode
echo -e "\n${YELLOW}[ACTION] Stopping active container '${ACTIVE_CONTAINER}' to simulate crash...${NC}"
docker stop "${ACTIVE_CONTAINER}" >/dev/null

echo "Waiting 10 seconds for ZKFC health check failure & failover..."
sleep 10

# 3. Verify Standby Node is promoted to Active
echo -e "\nChecking if standby '${STANDBY_CONTAINER}' has transitioned..."
NEW_STANDBY_STATE=""

# Since namenode1 might be stopped, we run the CLI from the other running namenode (standby_container)
STANDBY_STATE=$(docker exec "${STANDBY_CONTAINER}" hdfs haadmin -getServiceState "${STANDBY_ID}" 2>/dev/null | tr -d '\r\n ' || echo "unknown")

echo -e "Status of '${STANDBY_CONTAINER}' (${STANDBY_ID}): ${GREEN}${STANDBY_STATE}${NC}"

if [ "${STANDBY_STATE}" = "active" ]; then
  echo -e "${GREEN}[SUCCESS] Automatic failover triggered! Standby node has successfully transitioned to ACTIVE.${NC}"
else
  echo -e "${RED}[FAIL] Automatic failover failed! Node '${STANDBY_CONTAINER}' is still '${STANDBY_STATE}'.${NC}"
  echo "Restarting stopped node before exit..."
  docker start "${ACTIVE_CONTAINER}" >/dev/null
  exit 1
fi

# 4. Restart the stopped NameNode and verify it joins as Standby
echo -e "\n${YELLOW}[ACTION] Restarting former active container '${ACTIVE_CONTAINER}'...${NC}"
docker start "${ACTIVE_CONTAINER}" >/dev/null

echo "Waiting 15 seconds for startup, synchronization, and registration..."
sleep 15

# Verify new service states
STATE_NN1_FINAL=$(docker exec "${STANDBY_CONTAINER}" hdfs haadmin -getServiceState "nn1" 2>/dev/null | tr -d '\r\n ' || echo "unknown")
STATE_NN2_FINAL=$(docker exec "${STANDBY_CONTAINER}" hdfs haadmin -getServiceState "nn2" 2>/dev/null | tr -d '\r\n ' || echo "unknown")

echo -e "\nPost-Failover Service States:"
echo -e "NameNode 1 (nn1): ${GREEN}${STATE_NN1_FINAL}${NC}"
echo -e "NameNode 2 (nn2): ${GREEN}${STATE_NN2_FINAL}${NC}"

if [ "${STATE_NN1_FINAL}" = "active" ] && [ "${STATE_NN2_FINAL}" = "active" ]; then
  echo -e "${RED}[FATAL] SPLIT-BRAIN DETECTED after restarting the node!${NC}"
  exit 2
fi

# Check if the restarted node is now in standby
RESTARTED_STATE=""
if [ "${ACTIVE_ID}" = "nn1" ]; then
  RESTARTED_STATE="${STATE_NN1_FINAL}"
else
  RESTARTED_STATE="${STATE_NN2_FINAL}"
fi

if [ "${RESTARTED_STATE}" = "standby" ]; then
  echo -e "${GREEN}[SUCCESS] The restarted NameNode joined the cluster correctly as STANDBY. No split-brain occurred.${NC}"
  exit 0
else
  echo -e "${RED}[FAIL] Restarted NameNode failed to join as standby. Status is ${RESTARTED_STATE}.${NC}"
  exit 3
fi
