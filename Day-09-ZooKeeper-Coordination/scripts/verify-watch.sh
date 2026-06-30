#!/usr/bin/env bash
# verify-watch.sh
# Verifies that ZooKeeper watches are triggered and delivered to clients on data change.

set -euo pipefail

GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m'

WATCH_NODE="/watch-test-node"
LOG_FILE="/tmp/zk_watch_test.log"

echo -e "${YELLOW}=== Testing ZNode Watch Notifications ===${NC}"

# Clean up any leftover log
rm -f "${LOG_FILE}"

# 1. Create the znode first
echo -e "\n1. Creating ZNode ${YELLOW}${WATCH_NODE}${NC}..."
docker exec zookeeper1 zkCli.sh -server localhost:2181 create "${WATCH_NODE}" "initial-value" > /dev/null 2>&1 || true
docker exec zookeeper1 zkCli.sh -server localhost:2181 set "${WATCH_NODE}" "initial-value" > /dev/null 2>&1

# 2. Start a background CLI that registers a watch
echo -e "2. Launching background client to set a Watch on ${YELLOW}${WATCH_NODE}${NC}..."

# We execute zkCli in background, write "get -w /watch-test-node" followed by a sleep to keep it alive
(
  echo "get -w ${WATCH_NODE}"
  sleep 10
  echo "quit"
) | docker exec -i zookeeper2 zkCli.sh -server localhost:2181 > "${LOG_FILE}" 2>&1 &

CLI_PID=$!

# Wait for client to start and connect
sleep 3

# 3. Update the ZNode from another node
echo -e "3. Updating ZNode value from zookeeper1 to trigger the Watch..."
docker exec zookeeper1 zkCli.sh -server localhost:2181 set "${WATCH_NODE}" "triggered-value" > /dev/null

# Wait for event to trigger and propagate
sleep 3

# Kill background client script
kill $CLI_PID 2>/dev/null || true
wait $CLI_PID 2>/dev/null || true

# 4. Read the log file and check for the NodeDataChanged event
echo -e "\n4. Analyzing client logs for WatchedEvent..."

if grep -q -E "(NodeDataChanged|WatchedEvent)" "${LOG_FILE}"; then
  echo -e "${GREEN}[OK] Watch notification found in client logs:${NC}"
  grep --color -E "(NodeDataChanged|WatchedEvent.*path:${WATCH_NODE})" "${LOG_FILE}"
  
  # Clean up
  docker exec zookeeper1 zkCli.sh -server localhost:2181 delete "${WATCH_NODE}" > /dev/null 2>&1
  rm -f "${LOG_FILE}"
  
  echo -e "\n${GREEN}[SUCCESS] Watch mechanism is working correctly!${NC}"
  exit 0
else
  echo -e "${RED}[ERROR] Watch notification not found. Output logs:${NC}"
  cat "${LOG_FILE}"
  
  # Clean up
  docker exec zookeeper1 zkCli.sh -server localhost:2181 delete "${WATCH_NODE}" > /dev/null 2>&1
  rm -f "${LOG_FILE}"
  exit 1
fi
