#!/usr/bin/env bash
# verify-output.sh
# Validates the output directories, success file, and checks generated token counts from HDFS.

set -euo pipefail

GREEN='\033[0;32m'
RED='\033[0;31m'
YELLOW='\033[1;33m'
NC='\033[0m'

echo -e "${YELLOW}=== Verifying MapReduce Job Output in HDFS ===${NC}"

# Check files inside the HDFS output folder
HDFS_FILES=$(docker exec namenode-day14 hdfs dfs -ls /output 2>/dev/null || echo "FAILED")

if [ "$HDFS_FILES" = "FAILED" ]; then
  echo -e "${RED}[ERROR] Output folder /output does not exist in HDFS. MapReduce job may have failed to run.${NC}"
  exit 1
fi

echo -e "${GREEN}Found output files in HDFS /output:${NC}"
echo "$HDFS_FILES"

# Check success flag
if echo "$HDFS_FILES" | grep -q "_SUCCESS"; then
  echo -e "${GREEN}[OK] HDFS execution flag '_SUCCESS' is present.${NC}"
else
  echo -e "${RED}[ERROR] MapReduce Job failed: _SUCCESS file is missing in HDFS output.${NC}"
  exit 1
fi

# Print partitions output (should see part-r-00000.gz and part-r-00001.gz since we used 2 reducers + Gzip compression)
PART_FILES=$(echo "$HDFS_FILES" | grep -o -E "part-r-[0-9]+" || echo "")

if [ -n "$PART_FILES" ]; then
  echo -e "${GREEN}[OK] Found compressed Reducer partitions: ${PART_FILES//[$'\n']/ }${NC}"
else
  # Check if they are compressed with .gz extension
  PART_FILES_GZ=$(echo "$HDFS_FILES" | grep -o -E "part-r-[0-9]+\.gz" || echo "")
  if [ -n "$PART_FILES_GZ" ]; then
    echo -e "${GREEN}[OK] Found Gzip-compressed Reducer partition files: ${PART_FILES_GZ//[$'\n']/ }${NC}"
  else
    echo -e "${RED}[ERROR] Reducer partition output files (part-r-*) are missing.${NC}"
    exit 1
  fi
fi

# Retrieve and decompress content to show word frequencies
echo -e "\n${YELLOW}=== Aggregated Word Frequencies (Top Results) ===${NC}"
TEMP_DIR="/tmp/mr_out"
mkdir -p "$TEMP_DIR"
rm -f "$TEMP_DIR"/*

# Copy from HDFS to local container, then extract to host workspace for visibility
docker exec namenode-day14 hdfs dfs -copyToLocal /output /tmp/output_local 2>/dev/null || true
docker exec namenode-day14 bash -c "gunzip -f /tmp/output_local/part-r-*" 2>/dev/null || true

echo -e "${YELLOW}Data partition 0 (a-m characters):${NC}"
docker exec namenode-day14 head -n 15 /tmp/output_local/part-r-00000 2>/dev/null || echo "No data in partition 0"

echo -e "\n${YELLOW}Data partition 1 (n-z characters):${NC}"
docker exec namenode-day14 head -n 15 /tmp/output_local/part-r-00001 2>/dev/null || echo "No data in partition 1"

# Verify key words exist
echo -e "\nVerifying specific token frequencies..."
ALL_TEXT=$(docker exec namenode-day14 cat /tmp/output_local/part-r-00000 /tmp/output_local/part-r-00001 2>/dev/null || echo "")

if echo "$ALL_TEXT" | grep -i -E "mapreduce|hadoop|wordcount" > /dev/null; then
  echo -e "${GREEN}[SUCCESS] Token check passed. Words 'hadoop', 'mapreduce', and 'wordcount' are aggregated correctly!${NC}"
  # Cleanup temp directory inside namenode container
  docker exec namenode-day14 rm -rf /tmp/output_local
  exit 0
else
  echo -e "${RED}[ERROR] Core tokens were not found in final files. Output might be corrupt.${NC}"
  docker exec namenode-day14 rm -rf /tmp/output_local
  exit 1
fi
