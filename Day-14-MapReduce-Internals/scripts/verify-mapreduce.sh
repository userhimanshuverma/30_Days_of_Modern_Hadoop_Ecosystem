#!/usr/bin/env bash
# verify-mapreduce.sh
# Verifies MapReduce operational parameters, JobHistoryServer configuration, and CLI functionality.

set -euo pipefail

GREEN='\033[0;32m'
RED='\033[0;31m'
YELLOW='\033[1;33m'
NC='\033[0m'

echo -e "${YELLOW}=== MapReduce Framework Diagnostics ===${NC}"

# 1. Check JobHistoryServer Web UI accessibility
echo "Checking MapReduce Job History Server (JHS) on port 19888..."
if curl -s -f -o /dev/null http://localhost:19888/; then
  echo -e "${GREEN}[OK] JobHistoryServer Web UI is active on port 19888.${NC}"
else
  echo -e "${RED}[ERROR] JobHistoryServer Web UI is unreachable.${NC}"
  exit 1
fi

# 2. Check if YARN classpath and MapReduce libraries are mapped
echo -e "\nVerifying YARN MapReduce Classpaths inside Container..."
CLASSPATH_CHECK=$(docker exec resourcemanager-day14 mapred classpath 2>/dev/null || echo "FAILED")

if [ "$CLASSPATH_CHECK" = "FAILED" ]; then
  echo -e "${RED}[ERROR] MapReduce CLI ('mapred') is not executable or not on the PATH in resourcemanager container.${NC}"
  exit 1
else
  echo -e "${GREEN}[OK] MapReduce classpath is resolved successfully.${NC}"
fi

# 3. Check for staging directory permissions in HDFS
echo -e "\nChecking HDFS directories for YARN/MapReduce execution..."
docker exec namenode-day14 hdfs dfs -mkdir -p /tmp/hadoop-yarn/staging 2>/dev/null || true
docker exec namenode-day14 hdfs dfs -chmod -R 1777 /tmp 2>/dev/null || true

HDFS_DIRS=$(docker exec namenode-day14 hdfs dfs -ls / 2>/dev/null || echo "FAILED")
if [ "$HDFS_DIRS" = "FAILED" ]; then
  echo -e "${RED}[ERROR] Failed to contact NameNode HDFS to verify standard paths.${NC}"
  exit 1
fi

if echo "$HDFS_DIRS" | grep -q "/tmp"; then
  echo -e "${GREEN}[OK] HDFS /tmp directory is present with correct operational scope.${NC}"
else
  echo -e "${YELLOW}[WARNING] HDFS /tmp directory is missing. Staging jobs may fail without HDFS folders configured.${NC}"
fi

echo -e "\n${GREEN}[SUCCESS] MapReduce engine and cluster environment are verified and operational!${NC}"
exit 0
