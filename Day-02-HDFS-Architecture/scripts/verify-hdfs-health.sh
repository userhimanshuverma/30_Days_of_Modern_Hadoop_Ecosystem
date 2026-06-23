#!/usr/bin/env bash
# verify-hdfs-health.sh
# Comprehensive diagnostic script to evaluate HDFS cluster health, Safe Mode status, and block integrity.

set -euo pipefail

GREEN='\033[0;32m'
RED='\033[0;31m'
YELLOW='\033[1;33m'
NC='\033[0m'

CONTAINER="namenode-day02"

echo -e "${YELLOW}=== HDFS Global Health Audit ===${NC}"

# 1. Container check
if ! docker ps --format '{{.Names}}' | grep -q "^${CONTAINER}$"; then
  echo -e "${RED}[ERROR] NameNode container '${CONTAINER}' is not running.${NC}"
  exit 2
fi

# 2. Check Safe Mode status
echo "Checking Safe Mode state..."
SAFEMODE_STATUS=$(docker exec "${CONTAINER}" hdfs dfsadmin -safemode get 2>/dev/null || echo "FAILED")

if [ "$SAFEMODE_STATUS" = "FAILED" ]; then
  echo -e "${RED}[ERROR] Failed to query Safe Mode status from NameNode.${NC}"
  exit 2
fi

echo -e "Safe Mode Status: ${GREEN}${SAFEMODE_STATUS}${NC}"

if [[ "$SAFEMODE_STATUS" == *"ON"* ]]; then
  echo -e "${YELLOW}[WARNING] NameNode is in SAFE MODE. HDFS is read-only. Safe mode will deactivate once block reports are processed.${NC}"
fi

# 3. Check Disk Capacity
echo -e "\nChecking DFS Disk space allocations..."
REPORT=$(docker exec "${CONTAINER}" hdfs dfsadmin -report 2>/dev/null)
CAPACITY_LINE=$(echo "$REPORT" | grep "Configured Capacity" | head -n1)
DFS_USED_LINE=$(echo "$REPORT" | grep "DFS Used" | head -n1)
DFS_REMAINING_LINE=$(echo "$REPORT" | grep "Remaining" | head -n1)

echo "$CAPACITY_LINE"
echo "$DFS_USED_LINE"
echo "$DFS_REMAINING_LINE"

# 4. Check Block Integrity (fsck /)
echo -e "\nAuditing HDFS block filesystem integrity (hdfs fsck /)..."
FSCK_REPORT=$(docker exec "${CONTAINER}" hdfs fsck / 2>/dev/null || true)

# Parse stats
CORRUPT_BLOCKS=$(echo "$FSCK_REPORT" | grep -i "corrupt blocks" | awk -F: '{print $2}' | tr -d '[:space:]' || echo "0")
MISSING_BLOCKS=$(echo "$FSCK_REPORT" | grep -i -E "missing blocks|missing replicas" | awk -F: '{print $2}' | awk '{print $1}' | tr -d '[:space:]' || echo "0")
UNDER_REPLICATED=$(echo "$FSCK_REPORT" | grep -i "under-replicated blocks" | awk -F: '{print $2}' | tr -d '[:space:]' || echo "0")
MIS_REPLICATED=$(echo "$FSCK_REPORT" | grep -i "mis-replicated blocks" | awk -F: '{print $2}' | tr -d '[:space:]' || echo "0")

# Fallback to zero if empty
CORRUPT_BLOCKS=${CORRUPT_BLOCKS:-0}
MISSING_BLOCKS=${MISSING_BLOCKS:-0}
UNDER_REPLICATED=${UNDER_REPLICATED:-0}
MIS_REPLICATED=${MIS_REPLICATED:-0}

echo -e "Corrupt Blocks:     ${GREEN}${CORRUPT_BLOCKS}${NC}"
echo -e "Missing Blocks:     ${GREEN}${MISSING_BLOCKS}${NC}"
echo -e "Under-replicated:   ${GREEN}${UNDER_REPLICATED}${NC}"
echo -e "Mis-replicated:     ${GREEN}${MIS_REPLICATED}${NC}"

# 5. Evaluate overall health status and exit codes
HEALTHY=true

if [ "$CORRUPT_BLOCKS" != "0" ] && [ "$CORRUPT_BLOCKS" != "" ]; then
  echo -e "${RED}[CRITICAL] Corrupt blocks found. Data loss may have occurred! Run 'hdfs fsck -list-corruptfileblocks' to inspect.${NC}"
  HEALTHY=false
fi

if [ "$MISSING_BLOCKS" != "0" ] && [ "$MISSING_BLOCKS" != "" ]; then
  echo -e "${RED}[CRITICAL] Missing blocks found! Some files cannot be read.${NC}"
  HEALTHY=false
fi

if [ "$UNDER_REPLICATED" != "0" ] && [ "$UNDER_REPLICATED" != "" ]; then
  echo -e "${YELLOW}[WARNING] There are under-replicated blocks. HDFS will re-replicate blocks automatically once resources are free.${NC}"
fi

if [ "$HEALTHY" = true ]; then
  echo -e "\n${GREEN}[SUCCESS] HDFS is fully healthy. No corrupt or missing blocks detected.${NC}"
  exit 0
else
  echo -e "\n${RED}[FAILURE] HDFS filesystem is degraded or corrupt.${NC}"
  exit 1
fi
