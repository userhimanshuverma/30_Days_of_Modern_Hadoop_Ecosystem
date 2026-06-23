#!/usr/bin/env bash
# verify-namenode.sh
# Verifies that the HDFS NameNode is healthy, running, and accessible.

set -euo pipefail

# Colors for output
GREEN='\033[0;32m'
RED='\033[0;31m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

echo -e "${YELLOW}=== HDFS NameNode Diagnostics ===${NC}"

# 1. Check if the Docker container is running
CONTAINER_NAME="namenode-day02"
if ! docker ps --format '{{.Names}}' | grep -q "^${CONTAINER_NAME}$"; then
  echo -e "${RED}[ERROR] Container '${CONTAINER_NAME}' is not running.${NC}"
  echo "Please start the cluster by running: docker compose -f ../docker/docker-compose.yml up -d"
  exit 1
else
  echo -e "${GREEN}[OK] Container '${CONTAINER_NAME}' is running.${NC}"
fi

# 2. Check if the NameNode is listening on RPC port 9000 and HTTP port 9870
echo -e "\nChecking port bindings..."

check_port() {
  local port=$1
  local port_hex
  port_hex=$(printf '%04X' "$port")
  if docker exec "${CONTAINER_NAME}" which ss >/dev/null 2>&1; then
    docker exec "${CONTAINER_NAME}" ss -tln | grep -q ":$port "
  elif docker exec "${CONTAINER_NAME}" which netstat >/dev/null 2>&1; then
    docker exec "${CONTAINER_NAME}" netstat -tln | grep -q ":$port "
  else
    docker exec "${CONTAINER_NAME}" cat /proc/net/tcp /proc/net/tcp6 2>/dev/null | grep -i -q ":$port_hex "
  fi
}

if check_port 9000; then
  echo -e "${GREEN}[OK] NameNode is listening on RPC port 9000 (Internal IPC)${NC}"
else
  echo -e "${RED}[ERROR] NameNode is NOT listening on RPC port 9000${NC}"
  exit 1
fi

if check_port 9870; then
  echo -e "${GREEN}[OK] NameNode is listening on Web UI port 9870 (HTTP)${NC}"
else
  echo -e "${RED}[ERROR] NameNode is NOT listening on Web UI port 9870${NC}"
  exit 1
fi

# 3. Retrieve JMX Metrics from NameNode Web API
echo -e "\nQuerying NameNode JMX Endpoint..."
JMX_URL="http://localhost:9870/jmx?qry=Hadoop:service=NameNode,name=NameNodeInfo"

if ! curl -s -f --connect-timeout 5 "$JMX_URL" > /dev/null; then
  echo -e "${RED}[ERROR] Failed to query NameNode Web UI. Port 9870 might not be exposed or NameNode is still initializing.${NC}"
  exit 1
fi

JMX_RESPONSE=$(curl -s "$JMX_URL")

# Extract properties using grep/sed (to avoid dependency on jq for users who don't have it installed)
extract_jmx_value() {
  local key=$1
  echo "$JMX_RESPONSE" | grep "\"$key\"" | head -n1 | sed -E 's/.*:[[:space:]]*(.*)/\1/' | tr -d '"' | tr -d ',' | tr -d ' ' || echo "N/A"
}

VERSION=$(extract_jmx_value "Version")
TOTAL_CAPACITY=$(extract_jmx_value "Total")
FREE_CAPACITY=$(extract_jmx_value "Free")
USED_CAPACITY=$(extract_jmx_value "Used")
LIVE_NODES=$(extract_jmx_value "NumLiveDataNodes")
DEAD_NODES=$(extract_jmx_value "NumDeadDataNodes")
SAFEMODE_STATUS=$(docker exec "${CONTAINER_NAME}" hdfs dfsadmin -safemode get 2>/dev/null || echo "Unknown")

format_capacity() {
  local val=$1
  if [[ "$val" =~ ^[0-9]+$ ]]; then
    echo "$((val / 1024 / 1024 / 1024)) GB"
  else
    echo "N/A"
  fi
}

# Ensure integer formats for comparisons
if ! [[ "$LIVE_NODES" =~ ^[0-9]+$ ]]; then
  LIVE_NODES=0
fi
if ! [[ "$DEAD_NODES" =~ ^[0-9]+$ ]]; then
  DEAD_NODES=0
fi

echo -e "\n${YELLOW}=== NameNode Info ===${NC}"
echo -e "Hadoop Version:      ${GREEN}${VERSION}${NC}"
echo -e "Safe Mode Status:    ${GREEN}${SAFEMODE_STATUS}${NC}"
echo -e "Total HDFS Capacity: ${GREEN}$(format_capacity "$TOTAL_CAPACITY")${NC}"
echo -e "Used HDFS Capacity:  ${GREEN}$(format_capacity "$USED_CAPACITY")${NC}"
echo -e "Free HDFS Capacity:  ${GREEN}$(format_capacity "$FREE_CAPACITY")${NC}"
echo -e "Live DataNodes:      ${GREEN}${LIVE_NODES}${NC}"
echo -e "Dead DataNodes:      ${RED}${DEAD_NODES}${NC}"

if [ "${LIVE_NODES}" -ge 3 ]; then
  echo -e "\n${GREEN}[SUCCESS] NameNode health verification completed successfully. All 3 DataNodes are registered and active.${NC}"
  exit 0
else
  echo -e "\n${RED}[WARNING] Only ${LIVE_NODES} DataNodes are active. Expected 3. Check DataNode logs for registration issues.${NC}"
  exit 2
fi
