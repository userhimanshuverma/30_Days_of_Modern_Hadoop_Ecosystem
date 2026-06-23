#!/usr/bin/env bash
# verify-replication.sh
# Uploads a test file to HDFS and analyzes its block replication and placement.

set -euo pipefail

GREEN='\033[0;32m'
RED='\033[0;31m'
YELLOW='\033[1;33m'
NC='\033[0m'

CONTAINER="namenode-day02"
HDFS_PATH="/tmp/test_replication_$(date +%s).txt"
LOCAL_TEMP="temp_hdfs_rep_test.txt"

echo -e "${YELLOW}=== HDFS Replication & Block Placement Test ===${NC}"

# 1. Ensure NameNode is reachable
if ! docker ps --format '{{.Names}}' | grep -q "^${CONTAINER}$"; then
  echo -e "${RED}[ERROR] Container '${CONTAINER}' is not running.${NC}"
  exit 1
fi

# 2. Create a dummy test file (size ~10KB)
echo "Generating local test file (${LOCAL_TEMP})..."
dd if=/dev/urandom of="${LOCAL_TEMP}" bs=1024 count=10 status=none

# Copy local file into NameNode container for upload
docker cp "${LOCAL_TEMP}" "${CONTAINER}:/tmp/${LOCAL_TEMP}"

# 3. Upload file to HDFS with replication factor = 3
echo -e "\nUploading file to HDFS at '${HDFS_PATH}' with Replication Factor = 3..."
docker exec "${CONTAINER}" hdfs dfs -Ddfs.replication=3 -put "/tmp/${LOCAL_TEMP}" "${HDFS_PATH}"

# Check metadata
echo -e "\nHDFS File Metadata:"
docker exec "${CONTAINER}" hdfs dfs -ls "${HDFS_PATH}"

# 4. Check block locations via fsck
echo -e "\nRunning HDFS fsck to discover block locations..."
FSCK_OUTPUT=$(docker exec "${CONTAINER}" hdfs fsck "${HDFS_PATH}" -files -blocks -locations)
echo "$FSCK_OUTPUT"

# Parse block count and target hosts
echo -e "\n${YELLOW}=== Block Location Summary ===${NC}"
echo "$FSCK_OUTPUT" | grep -A2 "0. len=" || echo "No block details retrieved."

# 5. Dynamically change replication factor to 2
echo -e "\nReducing Replication Factor to 2 dynamically..."
docker exec "${CONTAINER}" hdfs dfs -setrep -w 2 "${HDFS_PATH}"

echo -e "\nRunning HDFS fsck post-reduction..."
docker exec "${CONTAINER}" hdfs fsck "${HDFS_PATH}" -files -blocks -locations | grep -A2 "0. len=" || true

# 6. Dynamically change replication factor to 1
echo -e "\nReducing Replication Factor to 1 dynamically..."
docker exec "${CONTAINER}" hdfs dfs -setrep -w 1 "${HDFS_PATH}"

echo -e "\nRunning HDFS fsck post-reduction..."
docker exec "${CONTAINER}" hdfs fsck "${HDFS_PATH}" -files -blocks -locations | grep -A2 "0. len=" || true

# 7. Cleanup
echo -e "\nCleaning up files..."
docker exec "${CONTAINER}" hdfs dfs -rm -f "${HDFS_PATH}"
docker exec "${CONTAINER}" rm -f "/tmp/${LOCAL_TEMP}"
rm -f "${LOCAL_TEMP}"

echo -e "\n${GREEN}[SUCCESS] Dynamic replication and block placement verification completed successfully!${NC}"
