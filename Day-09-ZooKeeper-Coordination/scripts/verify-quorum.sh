#!/usr/bin/env bash
# verify-quorum.sh
# Verifies the quorum status, active connections, and replication status of the ZooKeeper ensemble.

set -euo pipefail

GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m'

echo -e "${YELLOW}=== Auditing ZooKeeper Ensemble Quorum Status ===${NC}"

NODES=("zookeeper1:2181" "zookeeper2:2181" "zookeeper3:2181")
LEADER_COUNT=0
FOLLOWER_COUNT=0
UNRESPONSIVE_COUNT=0

for node in "${NODES[@]}"; do
  container=$(echo "${node}" | cut -d: -f1)
  port=$(echo "${node}" | cut -d: -f2)
  
  echo -e "\nQuerying Node: ${YELLOW}${container}${NC}..."
  
  # Send 'stat' 4-letter word command
  STAT_OUT=$(docker exec "${container}" bash -c "echo stat | nc localhost ${port}" 2>&1 || echo "failed")
  
  if echo "${STAT_OUT}" | grep -q "Mode:"; then
    MODE=$(echo "${STAT_OUT}" | grep "Mode:" | awk '{print $2}')
    VERSION=$(echo "${STAT_OUT}" | head -n 1)
    CONNECTIONS=$(echo "${STAT_OUT}" | grep "Connections:" | awk '{print $2}' || echo "0")
    ZXID=$(echo "${STAT_OUT}" | grep "Zxid:" | awk '{print $2}' || echo "unknown")
    
    echo -e "  - Status: ${GREEN}Online${NC}"
    echo -e "  - Version: ${VERSION}"
    echo -e "  - Mode: ${GREEN}${MODE}${NC}"
    echo -e "  - Connections: ${CONNECTIONS}"
    echo -e "  - Last Zxid: ${ZXID}"
    
    if [ "${MODE}" = "leader" ]; then
      LEADER_COUNT=$((LEADER_COUNT + 1))
    elif [ "${MODE}" = "follower" ]; then
      FOLLOWER_COUNT=$((FOLLOWER_COUNT + 1))
    fi
  else
    echo -e "  - Status: ${RED}Unresponsive/Error${NC} (Ensure 4LW is whitelisted & container is running)"
    UNRESPONSIVE_COUNT=$((UNRESPONSIVE_COUNT + 1))
  fi
done

echo -e "\n=== Quorum Verification Summary ==="
echo -e "  - Total Active Leaders: ${GREEN}${LEADER_COUNT}${NC} (Expected: 1)"
echo -e "  - Total Active Followers: ${GREEN}${FOLLOWER_COUNT}${NC} (Expected: 2)"
echo -e "  - Unresponsive Nodes: ${RED}${UNRESPONSIVE_COUNT}${NC} (Expected: 0)"

if [ "${LEADER_COUNT}" -eq 1 ] && [ "${UNRESPONSIVE_COUNT}" -eq 0 ]; then
  echo -e "\n${GREEN}[SUCCESS] Quorum is fully established and healthy!${NC}"
  exit 0
elif [ "${LEADER_COUNT}" -eq 1 ] && [ "${UNRESPONSIVE_COUNT}" -le 1 ]; then
  echo -e "\n${YELLOW}[WARNING] Quorum is maintained but cluster is degraded (1 node offline).${NC}"
  exit 0
else
  echo -e "\n${RED}[ERROR] Quorum is lost! No leader or too many offline nodes.${NC}"
  exit 1
fi
