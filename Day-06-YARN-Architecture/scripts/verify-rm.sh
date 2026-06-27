#!/usr/bin/env bash
# verify-rm.sh
# Verifies that the YARN ResourceManager is healthy, running, and accessible.

set -euo pipefail

# Colors for output
GREEN='\033[0;32m'
RED='\033[0;31m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

echo -e "${YELLOW}=== YARN ResourceManager Diagnostics ===${NC}"

# 1. Check if the Docker container is running
CONTAINER_NAME="resourcemanager-day06"
if ! docker ps --format '{{.Names}}' | grep -q "^${CONTAINER_NAME}$"; then
  echo -e "${RED}[ERROR] Container '${CONTAINER_NAME}' is not running.${NC}"
  echo "Please start the cluster by running: docker compose -f ../docker/docker-compose.yml up -d"
  exit 1
else
  echo -e "${GREEN}[OK] Container '${CONTAINER_NAME}' is running.${NC}"
fi

# 2. Check if the ResourceManager is listening on RPC port 8032 and HTTP port 8088
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

if check_port 8032; then
  echo -e "${GREEN}[OK] ResourceManager is listening on Applications Manager RPC port 8032${NC}"
else
  echo -e "${RED}[ERROR] ResourceManager is NOT listening on Applications Manager RPC port 8032${NC}"
  exit 1
fi

if check_port 8088; then
  echo -e "${GREEN}[OK] ResourceManager is listening on Web UI port 8088 (HTTP)${NC}"
else
  echo -e "${RED}[ERROR] ResourceManager is NOT listening on Web UI port 8088${NC}"
  exit 1
fi

# 3. Retrieve Cluster Metrics from ResourceManager REST API
echo -e "\nQuerying ResourceManager Metrics Endpoint..."
METRICS_URL="http://localhost:8088/ws/v1/cluster/metrics"

if ! curl -s -f --connect-timeout 5 "$METRICS_URL" > /dev/null; then
  echo -e "${RED}[ERROR] Failed to query ResourceManager Web API. Port 8088 might not be exposed or RM is still initializing.${NC}"
  exit 1
fi

METRICS_RESPONSE=$(curl -s "$METRICS_URL")

# Extract properties using grep/sed (to avoid dependency on jq)
extract_metric_value() {
  local key=$1
  echo "$METRICS_RESPONSE" | grep -o "\"$key\":[0-9]*" | cut -d':' -f2 || echo "N/A"
}

ACTIVE_NODES=$(extract_metric_value "activeNodes")
LOST_NODES=$(extract_metric_value "lostNodes")
UNHEALTHY_NODES=$(extract_metric_value "unhealthyNodes")
SUBMITTED_APPS=$(extract_metric_value "appsSubmitted")
RUNNING_APPS=$(extract_metric_value "appsRunning")
COMPLETED_APPS=$(extract_metric_value "appsCompleted")
FAILED_APPS=$(extract_metric_value "appsFailed")
ALLOCATED_MB=$(extract_metric_value "allocatedMB")
AVAILABLE_MB=$(extract_metric_value "availableMB")

echo -e "\n${YELLOW}=== Cluster Status Summary ===${NC}"
echo -e "Active NodeManagers:   ${GREEN}${ACTIVE_NODES}${NC}"
echo -e "Lost NodeManagers:     ${RED}${LOST_NODES}${NC}"
echo -e "Unhealthy NodeManagers: ${RED}${UNHEALTHY_NODES}${NC}"
echo -e "Applications Submitted: ${GREEN}${SUBMITTED_APPS}${NC}"
echo -e "Applications Running:   ${GREEN}${RUNNING_APPS}${NC}"
echo -e "Applications Completed: ${GREEN}${COMPLETED_APPS}${NC}"
echo -e "Allocated Memory:      ${GREEN}$((ALLOCATED_MB)) MB${NC}"
echo -e "Available Memory:      ${GREEN}$((AVAILABLE_MB)) MB${NC}"

# Check JMX for HaState
echo -e "\nQuerying JMX for High Availability State..."
JMX_URL="http://localhost:8088/jmx?qry=Hadoop:service=ResourceManager,name=RMInfo"
JMX_RESPONSE=$(curl -s "$JMX_URL" || echo "")
HA_STATE="Unknown"
if [ -n "$JMX_RESPONSE" ]; then
  HA_STATE=$(echo "$JMX_RESPONSE" | grep "\"State\"" | head -n1 | sed -E 's/.*:[[:space:]]*(.*)/\1/' | tr -d '"' | tr -d ',' | tr -d ' ' || echo "Standby")
fi
echo -e "ResourceManager HA State: ${GREEN}${HA_STATE}${NC}"

if [ "${ACTIVE_NODES}" -ge 1 ]; then
  echo -e "\n${GREEN}[SUCCESS] YARN ResourceManager is healthy and active. Connected to ${ACTIVE_NODES} active NodeManager(s).${NC}"
  exit 0
else
  echo -e "\n${RED}[WARNING] ResourceManager is running but no active NodeManagers are registered yet. Check NodeManager logs.${NC}"
  exit 2
fi
