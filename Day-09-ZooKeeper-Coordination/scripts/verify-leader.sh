#!/usr/bin/env bash
# verify-leader.sh
# Identifies which ZooKeeper node is currently the leader.

set -euo pipefail

GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m'

CONTAINERS=("zookeeper1" "zookeeper2" "zookeeper3")
LEADER=""

echo -e "${YELLOW}=== Locating Active ZooKeeper Leader ===${NC}"

for container in "${CONTAINERS[@]}"; do
  STAT_OUT=$(docker exec "${container}" bash -c "echo stat | nc localhost 2181" 2>/dev/null || echo "failed")
  if echo "${STAT_OUT}" | grep -q "Mode: leader"; then
    LEADER="${container}"
    break
  fi
done

if [ -n "${LEADER}" ]; then
  echo -e "${GREEN}[OK] Current Leader: ${LEADER}${NC}"
  echo "${LEADER}" > /tmp/zk_leader_name.tmp 2>/dev/null || true
  exit 0
else
  echo -e "${RED}[ERROR] No leader is currently active in the cluster!${NC}"
  exit 1
fi
