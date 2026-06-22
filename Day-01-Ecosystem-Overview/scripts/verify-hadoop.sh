#!/usr/bin/env bash
# =========================================================================
# Hadoop HDFS Validation Script - Day 1
# Checks HDFS NameNode health and performs a read/write integration test.
# =========================================================================

set -euo pipefail

# ANSI color codes
GREEN='\033[0;32m'
RED='\033[0;31m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

CONTAINER_NAME="namenode-day01"
TEST_FILE="temp_hadoop_test.txt"
HDFS_TEST_DIR="/tmp/day01_test"

echo -e "${YELLOW}=== Starting Hadoop HDFS Validation ===${NC}"

# 1. Check if Container is Running
if ! docker ps --filter "name=${CONTAINER_NAME}" --filter "status=running" | grep -q "${CONTAINER_NAME}"; then
  echo -e "${RED}[ERROR] NameNode container '${CONTAINER_NAME}' is not running.${NC}"
  echo -e "${YELLOW}Please start it using: docker compose up -d namenode${NC}"
  exit 1
fi
echo -e "${GREEN}[OK] NameNode container is running.${NC}"

# 2. Wait for HDFS to be ready (out of safe mode)
echo "Waiting for HDFS to respond and leave safe mode..."
for i in {1..30}; do
  if docker exec "${CONTAINER_NAME}" hdfs dfsadmin -report >/dev/null 2>&1; then
    echo -e "${GREEN}[OK] HDFS is online and responding.${NC}"
    break
  fi
  if [ "$i" -eq 30 ]; then
    echo -e "${RED}[ERROR] HDFS failed to respond after 30 seconds.${NC}"
    exit 1
  fi
  sleep 2
done

# 3. Create a Local Test File
echo "Hadoop Day 1: Modern Data Platforms Validation Successful!" > "${TEST_FILE}"

# 4. Perform Write Test
echo "Creating HDFS test directory '${HDFS_TEST_DIR}'..."
docker exec "${CONTAINER_NAME}" hdfs dfs -mkdir -p "${HDFS_TEST_DIR}"

echo "Copying test file to NameNode container..."
docker cp "${TEST_FILE}" "${CONTAINER_NAME}:/tmp/${TEST_FILE}"

echo "Uploading file to HDFS..."
docker exec "${CONTAINER_NAME}" hdfs dfs -put -f "/tmp/${TEST_FILE}" "${HDFS_TEST_DIR}/${TEST_FILE}"
echo -e "${GREEN}[OK] Write test successful. File uploaded to HDFS.${NC}"

# 5. Perform Read Test
echo "Reading file back from HDFS..."
HDFS_CONTENT=$(docker exec "${CONTAINER_NAME}" hdfs dfs -cat "${HDFS_TEST_DIR}/${TEST_FILE}")

if [[ "$HDFS_CONTENT" == *"Hadoop Day 1: Modern Data Platforms Validation Successful!"* ]]; then
  echo -e "${GREEN}[OK] Read test successful. Retained identical content.${NC}"
else
  echo -e "${RED}[ERROR] Read test failed. Content mismatch! Got: ${HDFS_CONTENT}${NC}"
  exit 1
fi

# 6. Cleanup
echo "Cleaning up test assets..."
docker exec "${CONTAINER_NAME}" hdfs dfs -rm -r -f "${HDFS_TEST_DIR}"
docker exec "${CONTAINER_NAME}" rm -f "/tmp/${TEST_FILE}"
rm -f "${TEST_FILE}"

echo -e "${GREEN}=== Hadoop HDFS Validation PASSED successfully! ===${NC}"
