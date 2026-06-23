#!/usr/bin/env bash
# verify-datanodes.sh
# Verifies that all HDFS DataNodes are running, healthy, and communicating with the NameNode.

set -euo pipefail

GREEN='\033[0;32m'
RED='\033[0;31m'
YELLOW='\033[1;33m'
NC='\033[0m'

echo -e "${YELLOW}=== HDFS DataNodes Diagnostics ===${NC}"

DATANODES=("datanode1-day02" "datanode2-day02" "datanode3-day02")
HOST_PORTS=("9864" "9865" "9866")

# 1. Check if DataNode containers are running
echo "Checking DataNode container statuses..."
all_running=true
for i in "${!DATANODES[@]}"; do
  DN="${DATANODES[$i]}"
  PORT="${HOST_PORTS[$i]}"
  
  if ! docker ps --format '{{.Names}}' | grep -q "^${DN}$"; then
    echo -e "${RED}[ERROR] DataNode container '${DN}' is NOT running.${NC}"
    all_running=false
  else
    echo -e "${GREEN}[OK] DataNode container '${DN}' is running (Web UI port exposed on host: ${PORT}).${NC}"
  fi
done

if [ "$all_running" = false ]; then
  echo -e "${RED}[ERROR] One or more DataNode containers are down. Please check docker status.${NC}"
  exit 1
fi

# 2. Check DataNode heartbeats in logs
echo -e "\nChecking DataNode heartbeat activity..."
for DN in "${DATANODES[@]}"; do
  echo -n "Checking logs for $DN... "
  HEARTBEAT_LOGS=$(docker logs --tail 200 "$DN" 2>&1 | grep -i -E "HeartbeatManager|Successfully sent heartbeat" || true)
  if [ -n "$HEARTBEAT_LOGS" ]; then
    echo -e "${GREEN}Active logs found (Heartbeats OK).${NC}"
  else
    # Some builds might log differently, check connection logs
    REG_LOGS=$(docker logs --tail 200 "$DN" 2>&1 | grep -i "Successfully registered" || true)
    if [ -n "$REG_LOGS" ]; then
      echo -e "${GREEN}Registered successfully (Active).${NC}"
    else
      echo -e "${YELLOW}No registration/heartbeat logs in last 200 lines. Inspecting...${NC}"
    fi
  fi
done

# 3. Retrieve HDFS DFSAdmin Report
echo -e "\nQuerying NameNode for DataNode Reports (hdfs dfsadmin -report)..."
REPORT=$(docker exec namenode-day02 hdfs dfsadmin -report 2>/dev/null || echo "FAILED")

if [ "$REPORT" = "FAILED" ]; then
  echo -e "${RED}[ERROR] Failed to run 'hdfs dfsadmin -report' inside NameNode container.${NC}"
  exit 1
fi

echo -e "\n${YELLOW}=== Registered DataNodes from NameNode Perspective ===${NC}"
echo "$REPORT" | grep -E "Name:|Hostname:|Decommission Status|Configured Capacity:|DFS Used:|Non DFS Used:|Remaining:|Block Pool Used:" || echo "No DataNodes registered."

# Parse registered nodes count
REGISTERED_NODES=$(echo "$REPORT" | grep -c -i "Live datanodes" || echo "0")
LIVE_NODES_COUNT=$(echo "$REPORT" | grep -i "Live datanodes" | sed -n -E 's/.*Live datanodes[[:space:]]*\(([0-9]+)\).*/\1/p')
LIVE_NODES_COUNT=${LIVE_NODES_COUNT:-0}

echo -e "\nSummary:"
echo -e "Reported Live DataNodes: ${GREEN}${LIVE_NODES_COUNT}${NC}"

if [ "${LIVE_NODES_COUNT}" -eq 3 ]; then
  echo -e "${GREEN}[SUCCESS] All 3 DataNodes are registered and reports indicate healthy statuses.${NC}"
  exit 0
else
  echo -e "${RED}[ERROR] Cluster does not have the expected 3 registered DataNodes.${NC}"
  exit 1
fi
