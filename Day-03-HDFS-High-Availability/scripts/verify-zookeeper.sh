#!/usr/bin/env bash
# verify-zookeeper.sh
# Diagnostic script to verify ZooKeeper cluster connection and ZKFC election lock paths.

set -euo pipefail

# Colors
GREEN='\033[0;32m'
RED='\033[0;31m'
YELLOW='\033[1;33m'
NC='\033[0m'

echo -e "${YELLOW}=== ZooKeeper Coordination & HA Locks Audit ===${NC}"

# 1. Check if ZooKeeper container is running
ZK_CONTAINER="zookeeper-day03"
if ! docker ps --format '{{.Names}}' | grep -q "^${ZK_CONTAINER}$"; then
  echo -e "${RED}[ERROR] Container '${ZK_CONTAINER}' is not running!${NC}"
  exit 1
fi
echo -e "${GREEN}[OK] ZooKeeper container is running.${NC}"

# 2. Check if Zookeeper is accepting connections
echo -e "\nSending 'ruok' ping to ZooKeeper..."
# Send 'ruok' using bash dev/tcp or netcat if available, otherwise check container logs or status
# A common method in docker is using echo stat | nc localhost 2181 or equivalent
if docker exec "${ZK_CONTAINER}" bash -c "echo ruok | nc localhost 2181" 2>/dev/null | grep -q "imok"; then
  echo -e "${GREEN}[OK] ZooKeeper responded with 'imok' status.${NC}"
else
  # Fallback check using zkCli
  echo "Ping failed. Attempting zkCli connection check..."
  if docker exec "${ZK_CONTAINER}" zkServer.sh status 2>/dev/null | grep -E -q "(mode: standalone|mode: leader|mode: follower)"; then
    echo -e "${GREEN}[OK] ZooKeeper is active and healthy.${NC}"
  else
    echo -e "${RED}[ERROR] ZooKeeper service is unresponsive.${NC}"
    exit 1
  fi
fi

# 3. Query ZKFC Election Node
echo -e "\nQuerying ZooKeeper for active NameNode locks..."
LOCK_PATH="/hadoop-ha/mycluster"

# Check if directory exists in ZooKeeper
ZK_LS_OUT=$(docker exec "${ZK_CONTAINER}" zkCli.sh -server localhost:2181 ls "${LOCK_PATH}" 2>&1 || echo "Error")

if echo "$ZK_LS_OUT" | grep -q "NoNodeException"; then
  echo -e "${RED}[ERROR] HA Lock paths do not exist in ZooKeeper. Ensure ZKFC was formatted via 'hdfs zkfc -formatZK'.${NC}"
  exit 1
elif echo "$ZK_LS_OUT" | grep -q "ActiveStandbyElectorLock"; then
  echo -e "${GREEN}[OK] ActiveStandbyElectorLock path is registered in ZooKeeper.${NC}"
  
  # Get details about the active lock holder
  echo -e "\nFetching election lock details..."
  LOCK_DETAILS=$(docker exec "${ZK_CONTAINER}" zkCli.sh -server localhost:2181 get "${LOCK_PATH}/ActiveStandbyElectorLock" 2>&1 || echo "")
  
  # ZKFC stores NameNode addresses inside the lock node. Let's inspect
  if echo "$LOCK_DETAILS" | grep -q "mycluster"; then
    echo -e "${GREEN}[OK] Lock holder information found.${NC}"
  else
    echo -e "${YELLOW}[NOTE] Lock holder binary contents could not be fully decoded in plain text, which is normal for ZKFC serialized payloads.${NC}"
  fi
  
  echo -e "\n${GREEN}[SUCCESS] ZooKeeper is actively coordinating HDFS failover election.${NC}"
  exit 0
else
  echo -e "${RED}[FAIL] HA node locks are empty or lock was not acquired. ActiveStandbyElectorLock missing.${NC}"
  exit 2
fi
