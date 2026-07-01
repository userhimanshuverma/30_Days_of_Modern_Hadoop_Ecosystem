#!/usr/bin/env bash
# verify-brokers.sh
# Checks if the Kafka brokers are active and queries the KRaft Controller quorum status.

set -euo pipefail

# Colors
GREEN='\033[0;32m'
RED='\033[0;31m'
YELLOW='\033[1;33m'
NC='\033[0m'

echo -e "${YELLOW}=== Kafka Broker HA & KRaft Quorum State Diagnostics ===${NC}"

# Check if docker containers are running
required_containers=("kafka1-day10" "kafka2-day10" "kafka3-day10")
missing_containers=0

for container in "${required_containers[@]}"; do
  if ! docker ps --format '{{.Names}}' | grep -q "^${container}$"; then
    echo -e "${RED}[ERROR] Container '${container}' is NOT running!${NC}"
    missing_containers=$((missing_containers + 1))
  else
    echo -e "${GREEN}[OK] Container '${container}' is running.${NC}"
  fi
done

if [ "$missing_containers" -gt 0 ]; then
  echo -e "${RED}[FAIL] Not all required Kafka containers are running. Execute 'docker compose up -d' first.${NC}"
  exit 1
fi

echo -e "\nQuerying KRaft Metadata Quorum Status from kafka1-day10..."

# Execute the kafka-metadata-quorum tool
if ! docker exec kafka1-day10 kafka-metadata-quorum.sh --bootstrap-server localhost:9092 describe --status; then
  echo -e "${RED}[FAIL] Could not query KRaft metadata quorum from kafka1-day10. The brokers might still be starting up or in a split-brain loop.${NC}"
  exit 1
fi

echo -e "\nQuerying Quorum Replication Lag..."
if ! docker exec kafka1-day10 kafka-metadata-quorum.sh --bootstrap-server localhost:9092 describe --replication; then
  echo -e "${RED}[FAIL] Could not query KRaft quorum replication status.${NC}"
  exit 1
fi

echo -e "\n${GREEN}[SUCCESS] All brokers are online and KRaft consensus quorum is active!${NC}"
exit 0
