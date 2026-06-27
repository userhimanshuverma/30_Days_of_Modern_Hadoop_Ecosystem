#!/usr/bin/env bash
# verify-nm.sh
# Verifies that the YARN NodeManager is healthy, running, and registered.

set -euo pipefail

# Colors for output
GREEN='\033[0;32m'
RED='\033[0;31m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

echo -e "${YELLOW}=== YARN NodeManager Diagnostics ===${NC}"

# 1. Check if the Docker container is running
CONTAINER_NAME="nodemanager-day06"
if ! docker ps --format '{{.Names}}' | grep -q "^${CONTAINER_NAME}$"; then
  echo -e "${RED}[ERROR] Container '${CONTAINER_NAME}' is not running.${NC}"
  echo "Please start the cluster by running: docker compose -f ../docker/docker-compose.yml up -d"
  exit 1
else
  echo -e "${GREEN}[OK] Container '${CONTAINER_NAME}' is running.${NC}"
fi

# 2. Check if the NodeManager is listening on HTTP port 8042
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

if check_port 8042; then
  echo -e "${GREEN}[OK] NodeManager is listening on Web UI port 8042 (HTTP)${NC}"
else
  echo -e "${RED}[ERROR] NodeManager is NOT listening on Web UI port 8042${NC}"
  exit 1
fi

# 3. Retrieve Node Details from NodeManager REST API
echo -e "\nQuerying NodeManager Info Endpoint..."
NM_INFO_URL="http://localhost:8042/ws/v1/node/info"

if ! curl -s -f --connect-timeout 5 "$NM_INFO_URL" > /dev/null; then
  echo -e "${RED}[ERROR] Failed to query NodeManager Web API. Port 8042 might not be exposed or NM is still initializing.${NC}"
  exit 1
fi

NM_RESPONSE=$(curl -s "$NM_INFO_URL")

# Extract properties using grep/sed
extract_nm_value() {
  local key=$1
  echo "$NM_RESPONSE" | grep -o "\"$key\":\"[^\"]*\"" | cut -d':' -f2 | tr -d '"' || echo "N/A"
}

extract_nm_num() {
  local key=$1
  echo "$NM_RESPONSE" | grep -o "\"$key\":[0-9]*" | cut -d':' -f2 || echo "N/A"
}

NM_HOST=$(extract_nm_value "nodeHostName")
HADOOP_VER=$(extract_nm_value "nodeVersion")
TOTAL_MEM=$(extract_nm_num "totalMemoryAllocatedMB")
TOTAL_VCORES=$(extract_nm_num "totalVCoresAllocated")
NM_HEALTH=$(extract_nm_value "nodeHealthy")

echo -e "\n${YELLOW}=== NodeManager Info ===${NC}"
echo -e "Node Hostname:         ${GREEN}${NM_HOST}${NC}"
echo -e "Hadoop Version:        ${GREEN}${HADOOP_VER}${NC}"
echo -e "Total Memory Capacity: ${GREEN}${TOTAL_MEM} MB${NC}"
echo -e "Total vCores Capacity: ${GREEN}${TOTAL_VCORES}${NC}"
echo -e "Node Health Status:    ${GREEN}${NM_HEALTH}${NC}"

# Check connection to ResourceManager
echo -e "\nChecking NodeManager to ResourceManager connectivity..."
if docker exec "${CONTAINER_NAME}" ping -c 1 resourcemanager > /dev/null 2>&1; then
  echo -e "${GREEN}[OK] NodeManager can resolve and ping 'resourcemanager'${NC}"
else
  echo -e "${RED}[ERROR] NodeManager CANNOT reach 'resourcemanager'. Check docker network configuration.${NC}"
  exit 1
fi

if [ "${NM_HEALTH}" = "true" ]; then
  echo -e "\n${GREEN}[SUCCESS] NodeManager health verification completed successfully. Node is healthy.${NC}"
  exit 0
else
  echo -e "\n${RED}[WARNING] NodeManager reports unhealthy state. Check nodemanager logs using: docker logs ${CONTAINER_NAME}${NC}"
  exit 2
fi
