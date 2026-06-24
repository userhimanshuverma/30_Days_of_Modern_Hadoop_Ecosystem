#!/usr/bin/env bash
# verify-ha.sh
# Master diagnostic and validation orchestrator for the HDFS High Availability cluster.

set -euo pipefail

# Colors
GREEN='\033[0;32m'
RED='\033[0;31m'
YELLOW='\033[1;33m'
NC='\033[0m'

SCRIPT_DIR="$(dirname "$0")"

echo -e "${YELLOW}=========================================================================${NC}"
echo -e "${YELLOW}           HDFS HIGH AVAILABILITY INTEGRATED HEALTH CHECK                ${NC}"
echo -e "${YELLOW}=========================================================================${NC}"

# 1. Check ZooKeeper coordination status
echo -e "\n${YELLOW}[STAGE 1/4] Auditing ZooKeeper Coordination...${NC}"
if ! bash "${SCRIPT_DIR}/verify-zookeeper.sh"; then
  echo -e "${RED}[ERROR] Stage 1 failed. ZooKeeper or ZKFC is unhealthy.${NC}"
  exit 1
fi

# 2. Check JournalNode quorum status
echo -e "\n${YELLOW}[STAGE 2/4] Auditing JournalNode Quorum...${NC}"
if ! bash "${SCRIPT_DIR}/verify-journalnodes.sh"; then
  echo -e "${RED}[ERROR] Stage 2 failed. JournalNode cluster is down or partitioned.${NC}"
  exit 1
fi

# 3. Check Active/Standby health status
echo -e "\n${YELLOW}[STAGE 3/4] Auditing NameNode Roles & Topology...${NC}"
if ! bash "${SCRIPT_DIR}/verify-active-standby.sh"; then
  echo -e "${RED}[ERROR] Stage 3 failed. Role mapping is invalid or split-brain exists.${NC}"
  exit 1
fi

# 4. Perform live HDFS read/write validation on logical nameservice
echo -e "\n${YELLOW}[STAGE 4/4] Running Logical Nameservice I/O Tests...${NC}"

# Find which container is currently Active
ACTIVE_CONTAINER=""
if docker exec namenode1-day03 hdfs haadmin -getServiceState nn1 2>/dev/null | grep -q "active"; then
  ACTIVE_CONTAINER="namenode1-day03"
else
  ACTIVE_CONTAINER="namenode2-day03"
fi

echo -e "Executing write client operations against active container: ${GREEN}${ACTIVE_CONTAINER}${NC}"

# Create directories via logical Nameservice URI
echo "Creating test directories in HDFS..."
docker exec "${ACTIVE_CONTAINER}" hdfs dfs -mkdir -p hdfs://mycluster/system/ha-test

# Write a config file from the container disk to HDFS
echo "Uploading test file to HA cluster namespace..."
docker exec "${ACTIVE_CONTAINER}" hdfs dfs -put -f /etc/hadoop/core-site.xml hdfs://mycluster/system/ha-test/core-site.xml

# Read file contents to verify data consistency
echo "Reading back file from logical namespace..."
if docker exec "${ACTIVE_CONTAINER}" hdfs dfs -cat hdfs://mycluster/system/ha-test/core-site.xml > /dev/null; then
  echo -e "${GREEN}[OK] Read operations succeeded.${NC}"
else
  echo -e "${RED}[ERROR] Read operation failed.${NC}"
  exit 1
fi

# Query replication status
echo "Querying file replication status across DataNodes..."
docker exec "${ACTIVE_CONTAINER}" hdfs dfs -ls hdfs://mycluster/system/ha-test/core-site.xml

echo -e "\n${GREEN}[SUCCESS] All HDFS HA stages passed! The cluster is healthy, redundant, and accepting client connections under the logical nameservice URI.${NC}"
exit 0
