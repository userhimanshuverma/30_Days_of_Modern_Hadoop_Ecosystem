#!/usr/bin/env bash
# verify-journalnodes.sh
# Diagnostic script to audit HDFS JournalNode cluster and log synchronization status.

set -euo pipefail

# Colors
GREEN='\033[0;32m'
RED='\033[0;31m'
YELLOW='\033[1;33m'
NC='\033[0m'

echo -e "${YELLOW}=== HDFS JournalNode Quorum Diagnostics ===${NC}"

JNS=("journalnode1-day03" "journalnode2-day03" "journalnode3-day03")
SUCCESS_COUNT=0

check_port() {
  local container=$1
  local port=8485
  local port_hex
  port_hex=$(printf '%04X' "$port")
  if docker exec "${container}" which ss >/dev/null 2>&1; then
    docker exec "${container}" ss -tln | grep -q ":$port "
  elif docker exec "${container}" which netstat >/dev/null 2>&1; then
    docker exec "${container}" netstat -tln | grep -q ":$port "
  else
    docker exec "${container}" cat /proc/net/tcp /proc/net/tcp6 2>/dev/null | grep -i -q ":$port_hex "
  fi
}

for jn in "${JNS[@]}"; do
  echo -e "\nChecking ${jn}..."
  
  # 1. Container Running State
  if ! docker ps --format '{{.Names}}' | grep -q "^${jn}$"; then
    echo -e "${RED}[ERROR] Container '${jn}' is not running!${NC}"
    continue
  fi
  echo -e "${GREEN}[OK] Container is running.${NC}"
  
  # 2. Port Binding check
  if check_port "${jn}"; then
    echo -e "${GREEN}[OK] Daemon is listening on QJM RPC port 8485.${NC}"
  else
    echo -e "${RED}[ERROR] Daemon is NOT listening on RPC port 8485.${NC}"
    continue
  fi
  
  # 3. Journal Storage Directory inspection
  if docker exec "${jn}" [ -d "/hadoop/dfs/journal/mycluster/current" ]; then
    echo -e "${GREEN}[OK] Journal storage directories are initialized for nameservice 'mycluster'.${NC}"
    
    # Extract latest layout version and transaction IDs
    LAYOUT=$(docker exec "${jn}" cat /hadoop/dfs/journal/mycluster/current/VERSION 2>/dev/null | grep layoutVersion | cut -d'=' -f2 || echo "N/A")
    CID=$(docker exec "${jn}" cat /hadoop/dfs/journal/mycluster/current/VERSION 2>/dev/null | grep clusterID | cut -d'=' -f2 || echo "N/A")
    echo -e "   -> Layout Version: ${LAYOUT}"
    echo -e "   -> Cluster ID:      ${CID}"
    SUCCESS_COUNT=$((SUCCESS_COUNT + 1))
  else
    echo -e "${YELLOW}[WARNING] Storage directory not yet initialized. Waiting for NameNode format or write activity.${NC}"
  fi
done

echo -e "\n${YELLOW}=== Quorum Audit Summary ===${NC}"
echo -e "Active/Healthy JournalNodes: ${GREEN}${SUCCESS_COUNT} / 3${NC}"

if [ "${SUCCESS_COUNT}" -ge 2 ]; then
  echo -e "${GREEN}[SUCCESS] JournalNode quorum is healthy. Majorities can be established (Quorum size: 2/3).${NC}"
  exit 0
else
  echo -e "${RED}[FAIL] JournalNode cluster is unhealthy. Quorum cannot be established (less than 2 online).${NC}"
  exit 1
fi
