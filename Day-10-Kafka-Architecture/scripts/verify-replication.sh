#!/usr/bin/env bash
# verify-replication.sh
# Checks the replica synchronization state, detecting under-replicated or offline partitions.

set -euo pipefail

# Colors
GREEN='\033[0;32m'
RED='\033[0;31m'
YELLOW='\033[1;33m'
NC='\033[0m'

echo -e "${YELLOW}=== Kafka Replication & ISR Health Diagnostics ===${NC}"

# Check if at least one broker container is running
if ! docker ps --format '{{.Names}}' | grep -q "^kafka1-day10$"; then
  echo -e "${RED}[ERROR] Container 'kafka1-day10' is not running! Cannot query replication status.${NC}"
  exit 1
fi

echo -e "Checking for under-replicated partitions (should be empty in a healthy cluster)..."
under_replicated=$(docker exec kafka1-day10 kafka-topics.sh --bootstrap-server localhost:9092 --describe --under-replicated-partitions)

if [ -n "$under_replicated" ]; then
  echo -e "${RED}[WARN] Under-replicated partitions detected:${NC}"
  echo "$under_replicated"
else
  echo -e "${GREEN}[OK] No under-replicated partitions detected. All replicas are in sync.${NC}"
fi

echo -e "\nChecking for unavailable partitions (no active leader - partition offline)..."
unavailable=$(docker exec kafka1-day10 kafka-topics.sh --bootstrap-server localhost:9092 --describe --unavailable-partitions)

if [ -n "$unavailable" ]; then
  echo -e "${RED}[CRITICAL] Offline (unavailable) partitions detected!${NC}"
  echo "$unavailable"
else
  echo -e "${GREEN}[OK] All partitions have active leaders.${NC}"
fi

echo -e "\nDetailed Replication Layout for Custom Topics:"
# List and describe custom topics details
docker exec kafka1-day10 kafka-topics.sh --bootstrap-server localhost:9092 --describe | grep -v "^__" || true

# Summary check
if [ -z "$under_replicated" ] && [ -z "$unavailable" ]; then
  echo -e "\n${GREEN}[SUCCESS] Replication health checks passed. Zero under-replicated or unavailable partitions. Cluster is healthy and durable.${NC}"
  exit 0
else
  echo -e "\n${RED}[FAIL] Cluster has replication anomalies. Check broker logs and connectivity.${NC}"
  exit 1
fi
