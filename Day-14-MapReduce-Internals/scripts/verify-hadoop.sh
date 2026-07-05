#!/usr/bin/env bash
# verify-hadoop.sh
# Verifies the health and status of the HDFS NameNode, DataNode, and YARN ResourceManager/NodeManager.

set -euo pipefail

GREEN='\033[0;32m'
RED='\033[0;31m'
YELLOW='\033[1;33m'
NC='\033[0m'

echo -e "${YELLOW}=== Hadoop HDFS & YARN Cluster Diagnostics ===${NC}"

CONTAINERS=("namenode-day14" "datanode-day14" "resourcemanager-day14" "nodemanager-day14" "historyserver-day14")

# 1. Check if docker containers are running
echo "Checking container statuses..."
all_running=true
for container in "${CONTAINERS[@]}"; do
  if ! docker ps --format '{{.Names}}' | grep -q "^${container}$"; then
    echo -e "${RED}[ERROR] Container '${container}' is NOT running.${NC}"
    all_running=false
  else
    echo -e "${GREEN}[OK] Container '${container}' is running.${NC}"
  fi
done

if [ "$all_running" = false ]; then
  echo -e "${RED}[ERROR] One or more Hadoop services are down. Execute 'docker compose up -d' first.${NC}"
  exit 1
fi

# 2. Check NameNode Web UI / API
echo -e "\nChecking HDFS NameNode status via Web UI..."
if curl -s -f -o /dev/null http://localhost:9870/; then
  echo -e "${GREEN}[OK] NameNode Web UI is active on port 9870.${NC}"
else
  echo -e "${RED}[ERROR] NameNode Web UI is unreachable.${NC}"
  exit 1
fi

# 3. Check ResourceManager YARN Web UI
echo -e "\nChecking YARN ResourceManager status via Web UI..."
if curl -s -f -o /dev/null http://localhost:8088/; then
  echo -e "${GREEN}[OK] YARN ResourceManager Web UI is active on port 8088.${NC}"
else
  echo -e "${RED}[ERROR] YARN ResourceManager Web UI is unreachable.${NC}"
  exit 1
fi

# 4. Check DataNode registration inside NameNode
echo -e "\nChecking DataNode registration inside NameNode (hdfs dfsadmin -report)..."
REPORT=$(docker exec namenode-day14 hdfs dfsadmin -report 2>/dev/null || echo "FAILED")

if [ "$REPORT" = "FAILED" ]; then
  echo -e "${RED}[ERROR] Failed to run 'hdfs dfsadmin -report' inside NameNode container.${NC}"
  exit 1
fi

LIVE_NODES=$(echo "$REPORT" | grep -i "Live datanodes" | sed -n -E 's/.*Live datanodes[[:space:]]*\(([0-9]+)\).*/\1/p')
LIVE_NODES=${LIVE_NODES:-0}

echo -e "Reported Live DataNodes: ${GREEN}${LIVE_NODES}${NC}"

if [ "${LIVE_NODES}" -gt 0 ]; then
  echo -e "${GREEN}[SUCCESS] NameNode reports ${LIVE_NODES} live registered DataNodes.${NC}"
else
  echo -e "${RED}[ERROR] No registered live DataNodes detected. Check datanode logs.${NC}"
  exit 1
fi

echo -e "\n${GREEN}[SUCCESS] Hadoop HDFS and YARN environment is fully healthy!${NC}"
exit 0
