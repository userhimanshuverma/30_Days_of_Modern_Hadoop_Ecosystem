#!/usr/bin/env bash
# verify-active-standby.sh
# Diagnostic script to determine the active/standby status of NameNodes.

set -euo pipefail

# Colors
GREEN='\033[0;32m'
RED='\033[0;31m'
YELLOW='\033[1;33m'
NC='\033[0m'

echo -e "${YELLOW}=== HDFS Active/Standby State Diagnostics ===${NC}"

# Check if containers are running
for container in namenode1-day03 namenode2-day03; do
  if ! docker ps --format '{{.Names}}' | grep -q "^${container}$"; then
    echo -e "${RED}[ERROR] Container '${container}' is not running!${NC}"
    exit 1
  fi
done

get_state() {
  local target_node=$1
  # Run haadmin to get service state (nn1 or nn2)
  docker exec namenode1-day03 hdfs haadmin -getServiceState "$target_node" 2>/dev/null | tr -d '\r\n '
}

echo -e "\nQuerying NameNode HA service states..."

STATE_NN1=$(get_state "nn1")
STATE_NN2=$(get_state "nn2")

echo -e "NameNode 1 (namenode1-day03 / nn1): ${GREEN}${STATE_NN1}${NC}"
echo -e "NameNode 2 (namenode2-day03 / nn2): ${GREEN}${STATE_NN2}${NC}"

# Validation Rules
if [ -z "${STATE_NN1}" ] || [ -z "${STATE_NN2}" ]; then
  echo -e "${RED}[FAIL] Could not query states. Cluster might still be starting up.${NC}"
  exit 1
fi

if [ "${STATE_NN1}" = "active" ] && [ "${STATE_NN2}" = "active" ]; then
  echo -e "${RED}[FATAL] SPLIT-BRAIN DETECTED! Both NameNodes report ACTIVE status!${NC}"
  exit 2
fi

if [ "${STATE_NN1}" = "standby" ] && [ "${STATE_NN2}" = "standby" ]; then
  echo -e "${YELLOW}[WARNING] Both NameNodes are in STANDBY status. Failover or election has not triggered yet.${NC}"
  exit 3
fi

if { [ "${STATE_NN1}" = "active" ] && [ "${STATE_NN2}" = "standby" ]; } || { [ "${STATE_NN1}" = "standby" ] && [ "${STATE_NN2}" = "active" ]; }; then
  echo -e "\n${GREEN}[SUCCESS] HA Topology is correct. Exactly one NameNode is Active, and one is Standby.${NC}"
  exit 0
else
  echo -e "${RED}[FAIL] Unexpected topology states. NN1: ${STATE_NN1}, NN2: ${STATE_NN2}${NC}"
  exit 4
fi
